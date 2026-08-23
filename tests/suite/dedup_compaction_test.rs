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
    database::{Database, scan_metric_names},
    observability::{counter_value, init_local_metrics_for_test},
    support::test_helpers::{BufferMode, TestConfigBuilder, array_get_str, delta_physical_row_count, json_to_batch, test_span_ts},
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

    // two commits did not coalesce by accident).
    let date_str = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string();
    let part_marker = format!("project_id={}/date={}", project_id, date_str);
    let file_count_before = table_ref.read().await.get_file_uris()?.filter(|u| u.contains(&part_marker)).count();
    assert!(file_count_before >= 2, "expected >=2 files in partition before dedup, got {}", file_count_before);

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

/// Does the path a ROLLUP BUILD reads through deduplicate?
///
/// This decides how expensive rollup coverage has to be. `rollup_backfill_tick`
/// certifies a partition duplicate-free — a full `dedup_partition` pass — before
/// it may build, and on 2026-08-15 that gate was what stopped every LARGE tenant
/// from ever getting coverage: certification needs a zero-drop pass over an
/// UNCHANGED file set, which a busy partition never offers while compaction is
/// draining. Small tenants had rollups; shipbubble and the whale had none.
///
/// If `query_delta_only` (what `rebuild_rollup_partition` reads through) already
/// collapsed duplicates, the gate would be redundant and could simply be
/// dropped. `dup_across_flush_is_deduped_on_read` deliberately says "routed scan
/// (NOT query_delta_only)", so pin the actual behaviour rather than infer it —
/// the answer is the difference between a one-line fix and a redesign, and
/// guessing wrong ships silently wrong dashboard numbers.
#[serial]
#[tokio::test]
async fn query_delta_only_deduplicates_so_a_rollup_build_needs_no_certification() -> Result<()> {
    let cfg = TestConfigBuilder::new("rollup_read_dedup_premise").with_buffer_mode(BufferMode::Enabled).build();
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    let row = |name: &str| -> Result<_> { json_to_batch(vec![test_span_ts("dup_id", name, &project_id, ts)]) };
    // Two independent commits → one physical duplicate, no sweep run.
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("first")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("second")?], true, None).await?;

    let count = |sql: String| {
        let db = Arc::clone(&db);
        async move {
            let batches = db.query_delta_only(&sql).await?;
            anyhow::Ok(
                batches
                    .iter()
                    .filter(|b| b.num_rows() > 0)
                    .filter_map(|b| b.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().map(|c| c.value(0)))
                    .next()
                    .unwrap_or(0),
            )
        }
    };

    let via_delta_only = count(format!("SELECT COUNT(*)::BIGINT FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND id = 'dup_id'")).await?;

    // The routed scan is the deduplicated reference.
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let routed_sql = format!("SELECT COUNT(*) AS cnt FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND id = 'dup_id'");
    let routed = ctx.sql(&routed_sql).await?.collect().await?[0].column(0).as_primitive::<Int64Type>().value(0);
    assert_eq!(routed, 1, "precondition: the routed scan deduplicates");

    assert_eq!(
        via_delta_only, routed,
        "query_delta_only must deduplicate exactly like the routed scan. The rollup build reads through it, so \
         this is what makes a build over an UNCERTIFIED partition correct — and therefore what makes the \
         certification gate redundant. If this ever regresses to 2, restore the gate or every rollup over an \
         undeduped partition silently double-counts."
    );
    Ok(())
}

/// END-TO-END proof that dropping the certification gate is safe: a sealed
/// partition that still holds PHYSICAL DUPLICATES and was never certified must
/// still roll up to the deduplicated answer.
///
/// This is the test that licenses the change. `rollup_backfill_tick` used to run
/// a full `dedup_partition` per partition before building, which no large tenant
/// could ever satisfy (see the comment at its call site), so shipbubble and the
/// whale had zero coverage and every 7-day query fell back to raw scans. The
/// gate is redundant because the build reads through `query_delta_only`, which
/// deduplicates — but "redundant" is a claim about NUMBERS, and a wrong rollup
/// is silently wrong, so assert the numbers.
#[serial]
#[tokio::test]
async fn a_rollup_built_over_an_uncertified_duplicated_partition_matches_the_deduped_answer() -> Result<()> {
    let mut cfg = (*TestConfigBuilder::new("rollup_no_cert").with_buffer_mode(BufferMode::Enabled).with_rollups().build()).clone();
    // Pinned to 4 to keep this fixture's horizon small and explicit; the
    // shipped default is 35 (it was 0/disabled until 2026-08-17).
    cfg.maintenance.timefusion_rollup_backfill_days = 4;
    let cfg = Arc::new(cfg);
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    // YESTERDAY, deliberately: it is the boundary of what the backfill claims.
    // It used to be excluded as "the sweep's job", but the sweep only builds at
    // its certification point and large tenants never certify — so yesterday
    // stayed uncovered for exactly them, and a rolling 7-day window (the only
    // shape a dashboard sends) paid a full raw day for it.
    let day = chrono::Utc::now().date_naive() - chrono::Duration::days(1);
    let ts = day.and_hms_opt(9, 0, 0).unwrap().and_utc().timestamp_micros();

    // Three distinct spans, one of which is written TWICE in separate commits →
    // a physical duplicate in a partition nothing ever certifies.
    for (id, name) in [("a", "first"), ("b", "first"), ("dup", "first"), ("dup", "second")] {
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts(id, name, &project_id, ts)])?], true, None).await?;
    }

    let scalar = |sql: String| {
        let db = Arc::clone(&db);
        async move {
            let batches = db.query_delta_only(&sql).await?;
            anyhow::Ok(
                batches
                    .iter()
                    .filter(|b| b.num_rows() > 0)
                    .filter_map(|b| b.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().map(|c| c.value(0)))
                    .next()
                    .unwrap_or(0),
            )
        }
    };

    let deduped_raw = scalar(format!("SELECT COUNT(*)::BIGINT FROM otel_logs_and_spans WHERE project_id = '{project_id}'")).await?;
    assert_eq!(deduped_raw, 3, "precondition: 4 physical rows, 3 distinct ids");

    let built = {
        // Plan, then step past FINALIZATION_DELAY: units are minted with a
        // deadline in the future, so draining immediately claims nothing.
        db.plan_rollup_backfill().await?;
        timefusion::support::advance_micros(16 * 60 * 1_000_000);
        db.drain_coordinator_rollups(64).await?
    };
    assert!(built > 0, "the backfill must build an UNCERTIFIED partition — that is the whole point of dropping the gate");

    let rolled =
        scalar(format!("SELECT COALESCE(SUM(request_count), 0)::BIGINT FROM otel_logs_and_spans_rollup_dashboard_1m_v3 WHERE project_id = '{project_id}'"))
            .await?;
    assert_eq!(rolled, deduped_raw, "the rollup must count the DEDUPLICATED rows; counting the physical 4 is the silent-wrong-number failure");
    Ok(())
}

/// The rollup backfill must not disturb work that is already queued.
///
/// Backfill exists because nothing else queues an untouched sealed day:
/// `reconcile_maintenance_task_cursors` enqueues only partitions named by
/// commits after its durable cursor, and DML invalidation only covers what it
/// touched. Prod 2026-08-17 had rollup rows for exactly two dates against 30+
/// days of source data, so every 7d/14d/30d query was refused with `not_built`.
///
/// But the enqueue path it reuses, `invalidate`, takes
/// `deadline.max(new_deadline)`. So re-invalidating a day that ALREADY has an
/// eligible task pushes that task's deadline out by a full finalization delay,
/// and a planner running every 60s holds the live frontier permanently just out
/// of reach. That is not hypothetical: the first version of this planner did
/// exactly that, and two rollup-parity tests went from correct totals to
/// building nothing at all.
///
/// So the precondition of a backfill — "nothing has touched this day" — has to
/// be literal. This pins it, and pins the horizon switch that turns the whole
/// thing off.
#[serial]
#[tokio::test]
async fn rollup_backfill_leaves_already_queued_days_alone() -> Result<()> {
    let base = TestConfigBuilder::new("rollup_coordinator_backfill").with_buffer_mode(BufferMode::Enabled).with_rollups().build();
    assert!(base.maintenance.timefusion_rollup_backfill_days >= 30, "the shipped default must cover a 30d query; 0 disables the backfill entirely");

    let db = Arc::new(Database::with_config(Arc::clone(&base)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // Writing the day queues rollup work for it through the normal path.
    let day = chrono::Utc::now().date_naive() - chrono::Duration::days(3);
    let ts = day.and_hms_opt(9, 0, 0).unwrap().and_utc().timestamp_micros();
    for id in ["a", "b", "c"] {
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts(id, "op", &project_id, ts)])?], true, None).await?;
    }
    if let Some(layer) = db.buffered_layer() {
        layer.flush_all_now().await?;
    }

    assert_eq!(
        db.plan_rollup_backfill().await?,
        0,
        "a day that already has queued rollup work must not be re-invalidated; doing so pushes its deadline out every pass and starves the live frontier"
    );

    // The horizon switch must still turn it off, or there is no way back.
    let mut off = (*base).clone();
    off.maintenance.timefusion_rollup_backfill_days = 0;
    let db_off = Arc::new(Database::with_config(Arc::new(off)).await?);
    assert_eq!(db_off.plan_rollup_backfill().await?, 0, "horizon 0 must disable the backfill entirely");
    Ok(())
}

/// TODAY may be rolled up to the buffer boundary, and the answer must still
/// equal the raw one.
///
/// Sealed history already answers a 7-day window in ~0.3s on prod, but a ROLLING
/// window pays a raw scan for today at roughly a second per elapsed hour, so by
/// evening today dominates and the window misses its budget. Covering today up
/// to the oldest still-buffered row leaves only the last few minutes raw.
///
/// The danger is claiming MORE than the build read: buckets above the bound
/// would be served from a rollup that never aggregated them, which is the
/// silent-wrong-number failure no read-side guard can catch. So this asserts
/// (a) today really is covered — otherwise the test passes vacuously by falling
/// back to raw — and (b) the aggregate equals the same aggregate over raw rows.
#[serial]
#[tokio::test]
async fn today_is_rolled_up_to_the_buffer_boundary_and_still_matches_the_raw_answer() -> Result<()> {
    let mut cfg = (*TestConfigBuilder::new("rollup_today").with_buffer_mode(BufferMode::Enabled).with_rollups().build()).clone();
    cfg.maintenance.timefusion_rollup_realtime_tail = true;
    cfg.maintenance.timefusion_rollup_backfill_days = 2;
    let cfg = Arc::new(cfg);
    // A REAL buffered layer: without one `min_buffered_micros` is always None,
    // the bound degenerates to the day end, and the partial-day case this test
    // exists for is never exercised.
    let layer = Arc::new(timefusion::support::test_helpers::test_layer(Arc::clone(&cfg))?);
    let db = Arc::new(Database::with_config(cfg).await?.with_buffered_layer(Arc::clone(&layer)));
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let today = chrono::Utc::now().date_naive();
    let midnight = today.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
    let row = |id: &str, duration: i64, ts: i64| -> Result<_> {
        json_to_batch(vec![serde_json::json!({
            "timestamp": ts, "id": id, "name": "op", "project_id": project_id, "hashes": [], "summary": ["today rollup fixture"],
            "date": today.to_string(), "duration": duration, "kind": "server", "status_code": "OK",
            "resource___service___name": "cart",
        })])
    };

    // SETTLED: early rows, flushed to Delta so a build can aggregate them.
    for (i, offset) in [60_000_000i64, 120_000_000, 3_600_000_000].iter().enumerate() {
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row(&format!("s{i}"), 100 + i as i64, midnight + offset)?], true, None).await?;
    }
    layer.flush_all_now().await?;

    // STILL BUFFERED: later rows, deliberately not flushed. These sit at or
    // above the bound, so the build cannot see them and must not claim them.
    for (i, offset) in [7_200_000_000i64, 7_260_000_000].iter().enumerate() {
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row(&format!("b{i}"), 900 + i as i64, midnight + offset)?], true, None).await?;
    }

    let built = {
        // Plan, then step past FINALIZATION_DELAY: units are minted with a
        // deadline in the future, so draining immediately claims nothing.
        db.plan_rollup_backfill().await?;
        timefusion::support::advance_micros(16 * 60 * 1_000_000);
        db.drain_coordinator_rollups(64).await?
    };
    assert!(built > 0, "the backfill must claim TODAY — otherwise this test proves nothing about partial-day coverage");

    let rollup_rows: i64 = {
        let sql = format!("SELECT COUNT(*)::BIGINT FROM otel_logs_and_spans_rollup_dashboard_1m_v3 WHERE project_id = '{project_id}'");
        let batches = db.query_delta_only(&sql).await?;
        batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .filter_map(|b| b.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().map(|c| c.value(0)))
            .next()
            .unwrap_or(0)
    };
    assert!(rollup_rows > 0, "today must actually have rollup buckets, not just an empty partition");

    // The whole of today, through the routed path (rollup interior + raw tail).
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let day_end = midnight + 86_400_000_000i64;
    let agg = format!(
        "SELECT COUNT(*) AS n, SUM(duration) AS total FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
         AND timestamp >= to_timestamp_micros({midnight}) AND timestamp < to_timestamp_micros({day_end})"
    );
    let routed = ctx.sql(&agg).await?.collect().await?;
    let (n, total) = (routed[0].column(0).as_primitive::<Int64Type>().value(0), routed[0].column(1).as_primitive::<Int64Type>().value(0));

    // Every row written, whatever leg served it: 3 settled + 2 buffered.
    assert_eq!(n, 5, "the union must return every row — a bound that over-claims drops the buffered tail");
    assert_eq!(total, 100 + 101 + 102 + 900 + 901, "durations must match exactly; a wrong interior shows up here as a wrong SUM");
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
            (0..b.num_rows()).map(|i| timefusion::support::test_helpers::array_get_str(col.as_ref(), i)).collect::<Vec<_>>()
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
        "UPDATE otel_logs_and_spans SET hashes = make_array(u.name) \
         FROM (VALUES ('dup_id', 'enriched')) AS u(id, name) \
         WHERE project_id = '{project_id}' AND otel_logs_and_spans.id = u.id"
    );
    let updated = ctx.sql(&sql).await?.collect().await?[0].column(0).as_primitive::<datafusion::arrow::datatypes::UInt64Type>().value(0);
    assert_eq!(updated, 1, "the duplicate copies are ONE logical row to merge-on-read: one version appended, superseding both");
    // The point of the test: the MERGE did not abort, and the duplicate is gone
    // from the logical table rather than half-updated.
    let mut ctx2 = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx2)?;
    let rows = ctx2
        .sql(&format!("SELECT COALESCE(array_element(hashes, 1), name) AS name FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND id = 'dup_id'"))
        .await?
        .collect()
        .await?;
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
        "UPDATE otel_logs_and_spans SET hashes = make_array(u.name) \
         FROM (VALUES ('dup_id', 'a'), ('dup_id', 'b')) AS u(id, name) \
         WHERE project_id = '{project_id}' AND otel_logs_and_spans.id = u.id"
    );
    let updated = ctx.sql(&sql).await?.collect().await?[0].column(0).as_primitive::<datafusion::arrow::datatypes::UInt64Type>().value(0);
    assert_eq!(updated, 2, "both rounds applied to the single target row (last-write-wins), not aborted");

    // Last source row wins; the target stays a single logical row.
    let rows = ctx
        .sql(&format!("SELECT COALESCE(array_element(hashes, 1), name) AS name FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND id = 'dup_id'"))
        .await?
        .collect()
        .await?;
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
    use timefusion::support::test_helpers::minio_test_config;
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
            .filter(|&i| timefusion::support::test_helpers::array_get_str(paths.as_ref(), i).contains(&marker))
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

/// A `Database` with a real buffered layer, so writes can land in MemBuffer.
async fn buffered_db(name: &str) -> Result<(Arc<Database>, String)> {
    let cfg = TestConfigBuilder::new(name).with_buffer_mode(BufferMode::Enabled).build();
    let layer = Arc::new(timefusion::support::test_helpers::test_layer(Arc::clone(&cfg))?);
    let db = Arc::new(Database::with_config(cfg).await?.with_buffered_layer(layer));
    Ok((db, format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8])))
}

/// `to_delta = true` commits straight to Delta (the already-flushed base row);
/// `false` goes through the buffered layer into MemBuffer (the new version).
async fn write(db: &Arc<Database>, project_id: &str, rows: Vec<serde_json::Value>, to_delta: bool) -> Result<()> {
    write_to(db, "otel_logs_and_spans", project_id, rows, to_delta).await
}

async fn write_to(db: &Arc<Database>, table: &str, project_id: &str, rows: Vec<serde_json::Value>, to_delta: bool) -> Result<()> {
    let batch = timefusion::support::test_helpers::json_to_batch_for(table, rows)?;
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
        .flat_map(|b| (0..b.num_rows()).map(|i| timefusion::support::test_helpers::array_get_str(b.column(0).as_ref(), i)).collect::<Vec<_>>())
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
    // A real scan must survive. NOT "DedupExec must survive": once the sweep
    // certifies this partition the read-side skip legitimately removes it (there
    // is one winning row per key by then — see
    // `a_swept_mor_partition_holds_one_winning_row_per_key`), and asserting the
    // operator instead of the property made this test pass only for as long as
    // the partition happened never to be certified.
    assert!(text.contains("DeltaScanExec"), "count_pushdown must decline where tombstones can exist — it answered from add-action stats:\n{text}");
    // Whichever way dedup resolves, the tombstone filter is what makes the count
    // right, and it must be in the plan.
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
    let schema = timefusion::schema::get_schema(TABLE).expect("fixture registered");
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
    let cb: timefusion::write::DeltaWriteCallback = Arc::new(move |project, table, batches, wm| {
        let db = db_for_cb.clone();
        Box::pin(async move { db.insert_records_batch(&project, &table, batches, true, Some(&wm)).await })
    });
    let layer = Arc::new(timefusion::support::test_helpers::test_layer(Arc::clone(&cfg))?.with_delta_writer(cb));
    let db = Arc::new(db_inner.with_buffered_layer(Arc::clone(&layer)));
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let ts = chrono::Utc::now().timestamp_micros();

    // The write path now builds batches at the FULL (wider) YAML column set
    // while the loaded Delta table declares the narrower one. This is the exact
    // skew that failed every prod flush.
    for (i, skip_queue) in [(0, true), (1, false)] {
        let rows = vec![mor_row(&format!("k{i}"), "v", &project_id, ts - i * 1000, None)];
        let batch = timefusion::support::test_helpers::json_to_batch_for(TABLE, rows)?;
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
    ctx.sql(&format!("UPDATE otel_logs_and_spans SET hashes = make_array('after') WHERE project_id = '{project_id}' AND id = 'row'")).await?.collect().await?;

    let count = |ctx: datafusion::prelude::SessionContext, pid: String, name: &'static str| async move {
        // Filter the column the UPDATE actually touched. `hashes` is the one
        // declared-mutable column, so its predicate must stay ABOVE the dedup;
        // filtering an immutable column here would prove nothing, since those
        // are pushed to the legs precisely because no UPDATE can change them.
        let rows = ctx
            .sql(&format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{pid}' AND array_element(hashes, 1) = '{name}'"))
            .await?
            .collect()
            .await?;
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
        let rows = ctx
            .sql(&format!("SELECT COALESCE(array_element(hashes, 1), name) AS name FROM {TABLE} WHERE project_id = '{pid}' AND id = 'legacy'"))
            .await?
            .collect()
            .await?;
        anyhow::Ok(
            rows.iter().flat_map(|b| (0..b.num_rows()).map(|i| b.column(0).as_string_view().value(i).to_string()).collect::<Vec<_>>()).collect::<Vec<_>>(),
        )
    };
    assert_eq!(read(ctx.clone(), project_id.clone()).await?, vec!["before"], "precondition: the legacy row reads back");

    ctx.sql(&format!("UPDATE {TABLE} SET hashes = make_array('after') WHERE project_id = '{project_id}' AND id = 'legacy'")).await?.collect().await?;

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

    let schema = timefusion::schema::get_schema(TABLE).expect("fixture registered");
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

/// The hot-tail skip, end to end: with the default ON, a reconcile whose only
/// uncovered files are in TODAY's partition builds nothing — while the census
/// still counts them, so the today/week/older breakdown stays honest.
///
/// The unit case table pins the RULE; this pins the WIRING, which is the half
/// that can silently regress (a flag read in the wrong place looks identical to
/// a flag that works). It matters because this default ships ON: today's files
/// are covered at birth by the flush callback and the two inline-reindex paths,
/// so what is deferred is re-indexing churn, not first coverage.
#[serial]
#[tokio::test]
async fn tantivy_backfill_skips_todays_partition_but_the_census_still_counts_it() -> Result<()> {
    use timefusion::tantivy::search::TantivyIndexService;
    const TABLE: &str = "otel_logs_and_spans";
    let cfg = TestConfigBuilder::new("tantivy_skip_today").with_buffer_mode(BufferMode::Enabled).build();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let svc = Arc::new(TantivyIndexService::new(store, Arc::new(cfg.tantivy.clone())));
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?.with_tantivy_indexer(svc));
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    // Must land in TODAY's partition whatever the hour: `now - 2h` falls into
    // YESTERDAY between 00:00 and 02:00 UTC, which is not skipped, and the test
    // would fail for two hours a day.
    let now = chrono::Utc::now();
    let ts = (now - chrono::Duration::hours(2)).max(now.date_naive().and_hms_opt(0, 0, 1).unwrap().and_utc()).timestamp_micros();
    db.insert_records_batch(&project_id, TABLE, vec![json_to_batch(vec![test_span_ts("hot", "n", &project_id, ts)])?], true, None).await?;

    let (uncovered, _, _) = db.tantivy_coverage_census().await?;
    assert!(uncovered >= 1, "the census must still SEE today's uncovered files, got {uncovered}");
    let (built, _, _) = db.tantivy_reconcile_table(TABLE).await?;
    assert_eq!(built, 0, "today's partition is churn — the backfill must not spend the pass on it, built={built}");
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
    use timefusion::tantivy::{load_manifest, search::TantivyIndexService};
    const TABLE: &str = "otel_logs_and_spans";
    // The rows below land in TODAY's partition, which the backfill skips by
    // default (`timefusion_tantivy_backfill_skip_today`) because today is churn.
    // This test is about the CLI repair path's backfill+GC mechanics, so it opts
    // out — and asserts the default's effect first, so the opt-out cannot hide a
    // regression in either direction.
    let cfg = {
        let mut cfg = (*TestConfigBuilder::new("tantivy_reconcile").with_buffer_mode(BufferMode::Enabled).build()).clone();
        assert!(
            cfg.tantivy.timefusion_tantivy_backfill_skip_today,
            "the hot-tail skip must be ON by default — this test's opt-out is what makes it meaningful"
        );
        cfg.tantivy.timefusion_tantivy_backfill_skip_today = false;
        Arc::new(cfg)
    };
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
    // A reindex has no visible end state without a remaining-work gauge, which
    // is why it was being chased by hand from sibling containers (three
    // OOM-killed on 2026-08-16). Seed a sentinel first: a gauge that is simply
    // never written also reads 0, so asserting 0 alone would pass vacuously.
    let tantivy_stats = timefusion::observability::maintenance_stats();
    let ordering = std::sync::atomic::Ordering::Relaxed;
    tantivy_stats.tantivy_uncovered_files.store(999, ordering);

    // The census must see the uncovered files BEFORE anything is built. The
    // gauge is otherwise only written by a reconcile pass, i.e. once a day, so a
    // freshly deployed process reports "0 remaining" for up to 24h whether or
    // not the reindex is actually finished — the exact blind spot that had this
    // work being chased by hand from sibling containers.
    let (uncovered_before, _, by_age_before) = db.tantivy_coverage_census().await?;
    assert!(uncovered_before >= 2, "census must count uncovered live files before any build, got {uncovered_before}");
    // The age split must partition the total, not merely exist: it is what
    // separates "a rewrite path is minting uncovered files" from "an old
    // backlog", and those want opposite fixes.
    assert_eq!(by_age_before.iter().sum::<u64>(), uncovered_before, "age buckets must account for every uncovered file: {by_age_before:?}");

    let (built, removed, _) = db.tantivy_reconcile_table(TABLE).await?;
    assert!(built >= 2, "expected both uncovered live files indexed, built={built}");
    assert_eq!(removed, 0, "nothing to GC before compaction");
    assert_eq!(
        tantivy_stats.tantivy_uncovered_files.load(ordering),
        0,
        "the pass must publish remaining work (sentinel not overwritten); 'uncovered -> 0' is the definition of a finished reindex"
    );
    let (uncovered_after, _, by_age_after) = db.tantivy_coverage_census().await?;
    assert_eq!(uncovered_after, 0, "after indexing every uncovered file the census must independently agree the reindex is done");
    assert_eq!(by_age_after, [0; 3], "a finished reindex leaves no uncovered file in any age bucket");
    let m = load_manifest(tantivy_store.as_ref(), TABLE, &project_id).await?;
    assert_eq!(m.entries.len(), 2, "per-uuid manifest covers both files");

    // Compact 2 files → 1: the inputs' entries are now stale.
    let table_ref = db.unified_tables().read().await.get(TABLE).expect("table created").clone();
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive();
    db.compact_date_concurrent(&table_ref, TABLE, date, Some(&project_id), None).await?;

    // Reconcile #2: stale entries GC'd (via manifest enumeration — the fixed
    // "default" list would miss this manifest) and the output file covered.
    let (_, removed2, _) = db.tantivy_reconcile_table(TABLE).await?;
    assert!(removed2 >= 2, "pre-compaction entries must be GC'd, removed={removed2}");
    let m = load_manifest(tantivy_store.as_ref(), TABLE, &project_id).await?;
    let live: Vec<String> = table_ref.read().await.get_file_uris()?.filter(|u| u.contains(&project_id)).collect();
    assert!(!live.is_empty());
    let covered: Vec<&String> = m.entries.values().filter(|e| e.error.is_none()).flat_map(|e| e.covered_files.iter()).collect();
    for u in &live {
        assert!(covered.contains(&u), "live file {u} must be index-covered after reconcile");
    }
    assert!(m.entries.values().all(|e| e.covered_files.iter().all(|u| live.contains(u))), "no entry may cover a dead file");
    Ok(())
}

/// The safety precondition for dropping the read-side `DedupExec` on a swept
/// partition — the change that would reclaim the `id` projection (43% of a
/// file's bytes) for charts and `count(*)`. See
/// `docs/plans/2026-08-09-per-date-dedup.md`.
///
/// `dedup_skip_allowed` refuses outright when a table sets `version_append`,
/// on the belief that merge-on-read leaves superseded versions a skip would
/// serve. Inspection says otherwise — the sweep calls `dedup_batches` with the
/// schema tiebreak (keep-greatest), and `filter_tombstones` runs OUTSIDE
/// `match dedup_on` so deletes never depend on dedup. This asserts it by
/// EXECUTION, because inspection cannot rule out an ordering interaction and
/// serving a superseded row is silent corruption.
///
/// Deliberately reads PHYSICAL rows (Delta log stats), never a routed query:
/// the read-side dedup would mask exactly the property under test.
#[serial]
#[tokio::test]
async fn a_swept_mor_partition_holds_one_winning_row_per_key() -> Result<()> {
    let cfg = TestConfigBuilder::new("swept_mor_one_winner").with_buffer_mode(BufferMode::Enabled).build();
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    // Yesterday noon UTC: a sealed prior-day partition the sweep will rewrite.
    let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();

    // Two versions of ONE key, in separate flushes so they land in separate
    // files — the shape merge-on-read produces for an UPDATE.
    // Two versions of ONE key differing only in `hashes` — the column declared
    // `mutable: true`. Differing in an immutable column instead would violate the
    // contract the pushdown rests on, which is what this test exists to police.
    let row = |tag: &str| -> Result<_> {
        let mut value = test_span_ts("mor_key", "span", &project_id, ts);
        value["hashes"] = serde_json::json!([tag]);
        json_to_batch(vec![value])
    };
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("original")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("updated")?], true, None).await?;

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    assert_eq!(delta_physical_row_count(&table_ref).await?, 2, "pre-sweep: the two versions are two physical rows, which is what a skip would wrongly serve");

    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

    // THE claim: after the sweep a certified partition holds exactly one row
    // per key. If this ever fails, the `version_append` bail is load-bearing
    // and the per-date dedup skip must NOT be built on top of it.
    assert_eq!(
        delta_physical_row_count(&table_ref).await?,
        1,
        "a swept merge-on-read partition must hold ONE physical row per key — otherwise skipping DedupExec would serve a superseded version"
    );
    Ok(())
}

/// End-to-end guard for the read-side dedup skip on a merge-on-read table:
/// a certified partition must return the UPDATED value, never the superseded
/// one. See `docs/plans/2026-08-09-per-date-dedup.md`.
///
/// The skip removes `DedupExec` and (now) the dedup-key projection with it —
/// `id` alone is 43% of an otel file's bytes. Its whole safety rests on the
/// sweep having collapsed each key to its winner, so this asserts the winner is
/// what a query actually gets, through the routed scan rather than the log.
///
/// `timefusion_read_dedup_skip_swept` is off by default, so this is the only
/// place the path runs until an operator opts in.
#[serial]
#[tokio::test]
async fn dedup_skip_on_a_swept_mor_partition_returns_the_updated_row() -> Result<()> {
    let mut cfg = (*TestConfigBuilder::new("dedup_skip_mor_winner").with_buffer_mode(BufferMode::Enabled).build()).clone();
    cfg.maintenance.timefusion_read_dedup_skip_swept = true;
    let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();

    // Two versions of ONE key differing only in `hashes` — the column declared
    // `mutable: true`. Differing in an immutable column instead would violate the
    // contract the pushdown rests on, which is what this test exists to police.
    let row = |tag: &str| -> Result<_> {
        let mut value = test_span_ts("mor_key", "span", &project_id, ts);
        value["hashes"] = serde_json::json!([tag]);
        json_to_batch(vec![value])
    };
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("original")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("updated")?], true, None).await?;

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

    // Through the routed scan, which is where a wrong skip would show up.
    let sql = format!(
        "SELECT array_element(hashes, 1) FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
         AND timestamp >= {ts} AND timestamp <= {ts}",
        ts = format_args!("to_timestamp_micros({ts})")
    );
    let batches = db.query_delta_only(&sql).await?;
    // Cast rather than assume the physical string type (otel columns are
    // Utf8View on this path).
    let rows: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let col = datafusion::arrow::compute::kernels::cast::cast(b.column(0), &datafusion::arrow::datatypes::DataType::Utf8).expect("cast to Utf8");
            let col = col.as_string::<i32>();
            (0..b.num_rows()).map(|i| col.value(i).to_string()).collect::<Vec<_>>()
        })
        .collect();

    assert_eq!(rows.len(), 1, "exactly one row survives the sweep, so the skip must return one: {rows:?}");
    assert_eq!(rows[0], "updated", "the skip must return the WINNING version, never the superseded one");
    Ok(())
}

/// Dedup-as-you-compact (docs/plans/2026-08-20 §3): with
/// `timefusion_compact_dedup_merge` on, the on-demand compaction path
/// (`compact_date`) must collapse merge-on-read versions WHILE merging files —
/// one physical row per (timestamp, id) key, the greatest `updated_at` winning —
/// and must RETAIN tombstones (a key whose winning version carries
/// `deleted=true` keeps that row; dropping it would resurrect the base row).
///
/// Physical rows are asserted via the Delta log (`delta_physical_row_count`);
/// the read-side DedupExec would mask exactly the property under test.
#[serial]
#[tokio::test]
async fn compact_dedup_merge_collapses_versions_and_retains_tombstones() -> Result<()> {
    let mut cfg = (*TestConfigBuilder::new("compact_dedup_merge").with_buffer_mode(BufferMode::Enabled).build()).clone();
    cfg.maintenance.timefusion_compact_dedup_merge = true;
    let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    // Yesterday noon UTC: sealed, and never straddles a midnight-UTC date flip.
    let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();

    // Key A: two versions across two files, differing only in `hashes` (the
    // mutable column). `stamp_version` gives the second insert the greater
    // `updated_at`, so it must be the survivor.
    let versioned = |tag: &str| -> Result<_> {
        let mut value = test_span_ts("mor_key", "span", &project_id, ts);
        value["hashes"] = serde_json::json!([tag]);
        json_to_batch(vec![value])
    };
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![versioned("original")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![versioned("updated")?], true, None).await?;
    // Key B: a tombstone — its (single, winning) version marks deletion. The
    // merge must carry it through verbatim, never drop it.
    let tombstone = {
        let mut value = test_span_ts("dead_key", "span", &project_id, ts + 1);
        value["deleted"] = serde_json::json!(true);
        json_to_batch(vec![value])?
    };
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![tombstone], true, None).await?;

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    assert_eq!(delta_physical_row_count(&table_ref).await?, 3, "pre-compact: two versions of key A + the key B tombstone");

    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive();
    db.compact_date(&table_ref, "otel_logs_and_spans", date, Some(&project_id)).await?;

    // THE claim: the merged output is unique-within — one physical row per key.
    // The 2 includes key B's tombstone: dropping it would have left 1.
    assert_eq!(delta_physical_row_count(&table_ref).await?, 2, "post-compact: key A collapsed to its winner, key B's tombstone retained");
    // Through the read path: key A's greatest-updated_at version is what a query
    // gets; key B is invisible — its retained tombstone keeps suppressing it
    // (which is exactly why the row must survive the merge physically).
    let rows = db
        .query_delta_only(&format!("SELECT id, array_element(hashes, 1) AS tag FROM otel_logs_and_spans WHERE project_id = '{project_id}' ORDER BY id"))
        .await?;
    let got: Vec<(String, String)> = rows
        .iter()
        .flat_map(|b| (0..b.num_rows()).map(|i| (array_get_str(b.column(0).as_ref(), i), array_get_str(b.column(1).as_ref(), i))).collect::<Vec<_>>())
        .collect();
    assert_eq!(got, vec![("mor_key".into(), "updated".into())], "the greatest-updated_at version wins and the tombstoned key stays suppressed");
    Ok(())
}

/// Regression, prod 2026-08-09 → 08-11: every 5-minute service-map rollup failed with
///
///     Internal error: DedupExec key `id` not in input schema
///
/// from the day `f1f0b90` enabled the swept-partition dedup skip by default.
///
/// The shape is the rollup's own: a date whose Delta partition the sweep has CERTIFIED,
/// plus fresher rows for that date still sitting in the MemBuffer. `pre_skip_dedup` reads
/// the certified Delta partition and drops the dedup keys from the scan projection — but
/// the mem ∪ delta union path "never grants `skip_dedup`" (see the exclusion-range comment
/// in `ProjectRoutingTable::scan`), so a `DedupExec` is still built, over a scan that no
/// longer carries `id`.
///
/// The query must project neither a dedup key nor the tombstone marker, which is what the
/// dispatcher's `SELECT DISTINCT project_id` does.
#[serial]
#[tokio::test]
async fn a_certified_partition_with_buffered_rows_still_answers_without_the_keys_projected() -> Result<()> {
    let mut cfg = (*TestConfigBuilder::new("dedup_skip_mem_union").with_buffer_mode(BufferMode::Enabled).build()).clone();
    cfg.maintenance.timefusion_read_dedup_skip_swept = true;
    let cfg = Arc::new(cfg);
    let db0 = Database::with_config(Arc::clone(&cfg)).await?;
    let layer = Arc::new(timefusion::support::test_helpers::test_layer(Arc::clone(&cfg))?.with_delta_writer(timefusion::server::delta_write_callback(&db0)));
    let db = Arc::new(db0.with_buffered_layer(Arc::clone(&layer)));
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let day = chrono::Utc::now().date_naive() - chrono::Duration::days(1);
    let ts = day.and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
    let (lo, hi) = (day.and_hms_opt(0, 0, 0).unwrap().and_utc().to_rfc3339(), day.and_hms_opt(23, 59, 59).unwrap().and_utc().to_rfc3339());

    // Delta leg: committed directly, then swept so the partition is certified.
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts("settled", "delta", &project_id, ts)])?], true, None)
        .await?;
    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

    // MemBuffer leg: same date, never flushed — this is what makes it a union.
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts("buffered", "mem", &project_id, ts + 1)])?], false, None)
        .await?;

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    // Warm the fast-resolve cache the way a live process is warm: `pre_skip_dedup` reads it,
    // and a cold cache leaves the skip off — which is why this never reproduced in-harness.
    let _ = ctx.sql(&format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{project_id}'")).await?.collect().await?;

    // `name` is neither a dedup key (timestamp, id) nor the tombstone marker.
    let sql = format!("SELECT name FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND timestamp >= '{lo}' AND timestamp <= '{hi}'");
    let batches = ctx.sql(&sql).await?.collect().await?;

    for b in &batches {
        assert_eq!(b.num_columns(), 1, "only `name` was selected; a dedup key or the tombstone marker leaked through");
    }
    let mut rows: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let col = datafusion::arrow::compute::kernels::cast::cast(b.column(0), &datafusion::arrow::datatypes::DataType::Utf8).expect("cast to Utf8");
            let col = col.as_string::<i32>();
            (0..b.num_rows()).map(|i| col.value(i).to_string()).collect::<Vec<_>>()
        })
        .collect();
    rows.sort();
    assert_eq!(rows, vec!["delta".to_string(), "mem".to_string()], "both legs must answer");
    Ok(())
}

/// A predicate on a version-MUTABLE column, pushed into the Delta leg because
/// the window is sweep-certified. This is the half of the swept-partition work
/// that reclaims ROWS rather than the key projection: prod 2026-08-09 measured
/// monoscope-self decoding 15.97 M rows to keep 35.89 K (445x) because
/// `version_mutable_columns` withheld every dashboard predicate and only
/// `timestamp >=` ever reached Parquet.
///
/// It is also the change most able to corrupt a read. Pushing such a predicate
/// BELOW the dedup normally drops the newer version of a key and leaves
/// keep-greatest serving the superseded one — `WHERE status_code = 'OK'`
/// matching a row already updated to 'ERROR' (2026-08-02,
/// `integration::test_update_operations`). The claim is that a certified
/// partition has no superseded version to serve, so this asserts BOTH
/// directions: the new value is found, and the old value is NOT.
#[serial]
#[tokio::test]
async fn a_pushed_mutable_predicate_on_a_swept_partition_cannot_match_the_superseded_row() -> Result<()> {
    let mut cfg = (*TestConfigBuilder::new("swept_pushdown_mutable").with_buffer_mode(BufferMode::Enabled).build()).clone();
    cfg.maintenance.timefusion_read_dedup_skip_swept = true;
    let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();

    // Two versions of ONE key differing only in `hashes` — the column declared
    // `mutable: true`. Differing in an immutable column instead would violate the
    // contract the pushdown rests on, which is what this test exists to police.
    let row = |tag: &str| -> Result<_> {
        let mut value = test_span_ts("mor_key", "span", &project_id, ts);
        value["hashes"] = serde_json::json!([tag]);
        json_to_batch(vec![value])
    };
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("original")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("updated")?], true, None).await?;

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

    // `name` is version-mutable, so this predicate is exactly the one that may
    let count_where = |val: &str| {
        let sql = format!(
            "SELECT array_element(hashes, 1) FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
             AND timestamp >= to_timestamp_micros({ts}) AND timestamp <= to_timestamp_micros({ts}) \
             AND array_element(hashes, 1) = '{val}'"
        );
        let db = Arc::clone(&db);
        async move { db.query_delta_only(&sql).await.map(|bs| bs.iter().map(|b| b.num_rows()).sum::<usize>()) }
    };

    assert_eq!(count_where("updated").await?, 1, "the winning version must still be found through a pushed predicate");
    assert_eq!(
        count_where("original").await?,
        0,
        "the SUPERSEDED value must match nothing — if it matches, the pushdown resurrected a stale version below the dedup"
    );
    Ok(())
}

/// The guard on the test above: an UNCERTIFIED window must NOT get the
/// pushdown, and must still answer correctly.
///
/// Same two versions, but no sweep — so `dedup_skip_allowed` refuses, the
/// mutable predicate is re-stripped by `leg_safe`, `DedupExec` runs, and the
/// FilterExec above it rejects the stale match. If the pushdown ever escapes
/// its certification gate this is the test that catches it, because here the
/// superseded row really is still on disk.
#[serial]
#[tokio::test]
async fn an_uncertified_window_still_hides_the_superseded_row_from_a_mutable_predicate() -> Result<()> {
    let mut cfg = (*TestConfigBuilder::new("uncertified_no_pushdown").with_buffer_mode(BufferMode::Enabled).build()).clone();
    cfg.maintenance.timefusion_read_dedup_skip_swept = true; // enabled, but the window is not certified
    let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();

    // Two versions of ONE key differing only in `hashes` — the column declared
    // `mutable: true`. Differing in an immutable column instead would violate the
    // contract the pushdown rests on, which is what this test exists to police.
    let row = |tag: &str| -> Result<_> {
        let mut value = test_span_ts("mor_key", "span", &project_id, ts);
        value["hashes"] = serde_json::json!([tag]);
        json_to_batch(vec![value])
    };
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("original")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("updated")?], true, None).await?;

    // Deliberately NO `dedup_today_partitions`: both physical versions remain.
    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    assert_eq!(delta_physical_row_count(&table_ref).await?, 2, "precondition: the superseded row is physically present, so a leaked pushdown could match it");

    let count_where = |val: &str| {
        let sql = format!(
            "SELECT array_element(hashes, 1) FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
             AND timestamp >= to_timestamp_micros({ts}) AND timestamp <= to_timestamp_micros({ts}) \
             AND array_element(hashes, 1) = '{val}'"
        );
        let db = Arc::clone(&db);
        async move { db.query_delta_only(&sql).await.map(|bs| bs.iter().map(|b| b.num_rows()).sum::<usize>()) }
    };

    assert_eq!(count_where("original").await?, 0, "uncertified: the superseded version must stay invisible — a match here means the pushdown escaped its gate");
    assert_eq!(count_where("updated").await?, 1, "uncertified: the winner is still returned");
    Ok(())
}

/// End-to-end for the dashboard rollup: certifying a partition must build its
/// buckets, and those buckets must agree with the raw aggregate EXACTLY.
///
/// A rollup that disagrees is worse than no rollup — it returns a wrong number
/// silently, on the panel people trust most. So this compares the stored
/// measures against the same aggregate computed from raw spans, rather than
/// asserting the rollup merely exists.
///
/// It also pins idempotence: the build is triggered by certification, and a
/// partition can be certified more than once, so a second build must REPLACE
/// its rows rather than double every count.
#[serial]
#[tokio::test]
async fn certifying_a_partition_builds_rollup_buckets_that_match_the_raw_aggregate() -> Result<()> {
    struct ClockGuard;
    impl Drop for ClockGuard {
        fn drop(&mut self) {
            timefusion::support::unfreeze();
        }
    }
    let _clock_guard = ClockGuard;
    let cfg = TestConfigBuilder::new("rollup_build_parity").with_buffer_mode(BufferMode::Enabled).with_rollups().build();
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let day = chrono::Utc::now().date_naive() - chrono::Duration::days(1);
    let ts = day.and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();

    // Several spans inside one minute bucket, plus one in the next.
    for (i, offset) in [0i64, 1_000_000, 2_000_000, 60_000_000].iter().enumerate() {
        let batch = json_to_batch(vec![test_span_ts(&format!("span_{i}"), "op", &project_id, ts + offset)])?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
    }

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
    timefusion::support::advance_micros(
        timefusion::maintenance_coordinator::FINALIZATION_DELAY_MICROS + timefusion::maintenance_coordinator::INVALIDATION_DEADLINE_BUCKET_MICROS + 1,
    );
    assert!(db.run_maintenance_units(1024).await? > 0, "eligible slice tasks must be drained");

    let total_from_rollup = |db: Arc<Database>, project_id: String| async move {
        let sql = format!("SELECT COALESCE(SUM(request_count), 0)::BIGINT FROM otel_logs_and_spans_rollup_dashboard_1m_v3 WHERE project_id = '{project_id}'");
        let batches = db.query_delta_only(&sql).await?;
        let v = batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .filter_map(|b| b.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().map(|c| c.value(0)))
            .next()
            .unwrap_or(0);
        anyhow::Ok(v)
    };

    let raw_total = {
        let sql = format!("SELECT COUNT(*)::BIGINT FROM otel_logs_and_spans WHERE project_id = '{project_id}'");
        let batches = db.query_delta_only(&sql).await?;
        batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .filter_map(|b| b.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().map(|c| c.value(0)))
            .next()
            .unwrap_or(0)
    };

    let rolled = total_from_rollup(Arc::clone(&db), project_id.clone()).await?;
    assert_eq!(rolled, raw_total, "rollup request_count must sum to the raw row count, or every Traffic panel is silently wrong");
    assert!(raw_total > 0, "precondition: the fixture actually wrote rows");

    // The publication counters must move on the COORDINATOR path, not only on
    // the retired cohort path (`stage_rollup_wave`) they were originally wired
    // to. Prod 2026-08-16 reported rollup_output_rows_total = 0 and
    // rollup_staged_projects_total = 0 while this very table held 1,220 rows for
    // a live project — a metric reading zero while the system works is worse
    // than no metric, and it cost a full diagnosis pass chasing a rollup outage
    // that was not happening. nextest gives each test its own process, so these
    // process-global counters are this test's alone.
    let stats = timefusion::observability::maintenance_stats();
    let ordering = std::sync::atomic::Ordering::Relaxed;
    assert!(stats.rollup_output_rows.load(ordering) > 0, "the coordinator published rollup rows but rollup_output_rows_total stayed 0");
    assert!(stats.rollup_staged_projects.load(ordering) > 0, "the coordinator published a slice but rollup_staged_projects_total stayed 0");
    assert!(stats.rollup_commit_actions.load(ordering) > 0, "the coordinator committed Delta actions but rollup_commit_actions_total stayed 0");

    // Certify again: the build must replace, not append.
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
    let after_rebuild = total_from_rollup(Arc::clone(&db), project_id.clone()).await?;
    assert_eq!(after_rebuild, raw_total, "a second certification must REPLACE the buckets, not double every measure");

    // A second independently replaceable slice must leave the first slice
    // untouched. If slice replacement drops or duplicates the earlier state,
    // the total moves and no read-side guard can repair it.
    let late = day.and_hms_opt(20, 30, 0).unwrap().and_utc().timestamp_micros();
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts("span_late", "op", &project_id, late)])?], true, None)
        .await?;
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
    timefusion::support::advance_micros(
        timefusion::maintenance_coordinator::FINALIZATION_DELAY_MICROS + timefusion::maintenance_coordinator::INVALIDATION_DEADLINE_BUCKET_MICROS + 1,
    );
    assert!(db.run_maintenance_units(1024).await? > 0, "late slice tasks must be drained");
    assert_eq!(
        total_from_rollup(Arc::clone(&db), project_id.clone()).await?,
        raw_total + 1,
        "an incremental rebuild must carry the untouched hours forward exactly once"
    );
    Ok(())
}

/// The rollup must answer a window it only PARTLY covers, by unioning its
/// certified interior with raw fringes, and the answer must equal the raw one.
///
//// THE gate for routing `COALESCE(<dimension>, 'null')` — the spelling every
/// monoscope grouped chart emits, and one that declined as `unsupported_shape`
/// until 2026-08-22 (3,237 ms raw against 276 ms routed at 3 days).
///
/// COALESCE folds NULL and the literal string `'null'` into ONE group, so the
/// question this must settle is whether the rollup can reproduce that fold. It
/// can, because `COALESCE(dim, lit)` is a function of `dim`: the tier's
/// partition by `dim` REFINES the partition by `COALESCE(dim, lit)`, and
/// re-aggregating decomposable states over a refinement equals aggregating raw
/// rows. This asserts that claim on data engineered to break it — a NULL
/// service AND a literal-'null' service, both in the rollup-covered day and in
/// the raw tail, so the fold has to survive the union of the two legs.
///
/// Asserts routing as well as equality: a miss also returns the right answer,
/// so equality alone would pass vacuously.
#[serial]
#[tokio::test]
async fn a_coalesced_dimension_folds_null_and_the_literal_identically_through_the_rollup() -> Result<()> {
    struct ClockGuard;
    impl Drop for ClockGuard {
        fn drop(&mut self) {
            timefusion::support::unfreeze();
        }
    }
    let _clock_guard = ClockGuard;
    let mut cfg = (*TestConfigBuilder::new("rollup_coalesce").with_buffer_mode(BufferMode::Enabled).with_rollups().build()).clone();
    cfg.maintenance.timefusion_rollup_realtime_tail = true;
    let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
    db.cancel_maintenance();
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let today = chrono::Utc::now().date_naive();
    let midnight = today.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
    let yesterday_noon = (today - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();

    // `service: None` writes a genuine SQL NULL; "null" is the four-character
    // string. Collapsing those two is the whole point of the test, so they must
    // be distinct in the fixture — asserted below before anything else.
    let row = |id: &str, service: Option<&str>, ts: i64| -> Result<_> {
        json_to_batch(vec![serde_json::json!({
            "timestamp": ts, "id": id, "name": "op", "project_id": project_id, "hashes": [], "summary": ["coalesce fixture"],
            "date": chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string(),
            "duration": 100, "kind": "server", "status_code": "OK",
            "resource___service___name": service,
        })])
    };

    // Yesterday, spread over hours so the interior is worth a union. Each of the
    // three service shapes appears in the COVERED day.
    for (i, (service, offset)) in [
        (None, 17i64),
        (Some("cart"), 61_000_000),
        (Some("null"), 130_000_000),
        (None, 3_661_000_000),
        (Some("null"), 7_330_000_000),
        (Some("cart"), 10_810_000_000),
        (None, 14_410_000_000),
        (Some("cart"), 18_010_000_000),
    ]
    .iter()
    .enumerate()
    {
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row(&format!("y{i}"), *service, yesterday_noon + offset)?], true, None).await?;
    }

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
    timefusion::support::advance_micros(
        timefusion::maintenance_coordinator::FINALIZATION_DELAY_MICROS + timefusion::maintenance_coordinator::INVALIDATION_DEADLINE_BUCKET_MICROS + 1,
    );
    assert!(db.run_maintenance_units(1024).await? > 0, "eligible yesterday slices must be drained");

    // Written AFTER certification, so these reach the query only via the raw
    // leg — and both fold-participants appear here too, which is what forces the
    // outer merge to combine a folded group ACROSS the two legs.
    for (i, (service, offset)) in [(None, 5_000_000i64), (Some("null"), 3_600_000_000), (Some("cart"), 3_700_000_000)].iter().enumerate() {
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row(&format!("t{i}"), *service, midnight + offset)?], true, None).await?;
    }

    let (lo, hi) = (yesterday_noon + 17, midnight + 7_200_000_017);
    let window = format!("project_id = '{project_id}' AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi})");

    // The fixture is only meaningful if a real NULL and a real 'null' both
    // exist. Without this the test could silently degrade into all-strings and
    // still pass, proving nothing about the fold.
    let shapes = db
        .query_delta_only(&format!(
            "SELECT COUNT(*) FILTER (WHERE resource___service___name IS NULL)::BIGINT, \
                    COUNT(*) FILTER (WHERE resource___service___name = 'null')::BIGINT \
             FROM otel_logs_and_spans WHERE {window}"
        ))
        .await?;
    let (nulls, literals) = shapes
        .iter()
        .filter(|b| b.num_rows() > 0)
        .map(|b| (b.column(0).as_primitive::<Int64Type>().value(0), b.column(1).as_primitive::<Int64Type>().value(0)))
        .next()
        .expect("a probe row");
    assert!(nulls > 0 && literals > 0, "the fixture must hold BOTH real NULLs and literal 'null's, got nulls={nulls} literals={literals}");

    // monoscope's real two-key chart shape: bucket AND coalesced dimension.
    let query = format!(
        "SELECT time_bucket('1 hours', timestamp) AS tb, COALESCE(resource___service___name, 'null') AS svc, \
                COUNT(*) AS c, min(duration) AS lo, max(duration) AS hi \
         FROM otel_logs_and_spans WHERE {window} GROUP BY 1, 2 ORDER BY 1, 2"
    );

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;

    let rows_of = |batches: Vec<datafusion::arrow::array::RecordBatch>| {
        batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .flat_map(|b| {
                (0..b.num_rows())
                    .map(|r| {
                        (
                            b.column(0).as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>().value(r),
                            array_get_str(b.column(1).as_ref(), r),
                            b.column(2).as_primitive::<Int64Type>().value(r),
                            b.column(3).as_primitive::<Int64Type>().value(r),
                            b.column(4).as_primitive::<Int64Type>().value(r),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };

    let hits = || timefusion::observability::maintenance_stats().rollup_hits_hybrid.load(std::sync::atomic::Ordering::Relaxed);
    let before = hits();
    let routed = rows_of(ctx.sql(&query).await?.collect().await?);
    assert_eq!(hits(), before + 1, "the coalesced chart must route, or this proves nothing about the rollup's fold");

    // `query_delta_only` bypasses rollup and buffer, so it is the authority.
    let raw = rows_of(db.query_delta_only(&query).await?);
    assert_eq!(routed, raw, "the coalesced group must match the raw aggregate row for row");

    // Every fixture row lands in exactly one group, and the NULLs really did
    // fold in with the literals rather than forming their own group.
    assert_eq!(routed.iter().map(|row| row.2).sum::<i64>(), 11, "every fixture row must be counted exactly once: {routed:?}");
    assert!(routed.iter().all(|row| row.1 == "null" || row.1 == "cart"), "COALESCE must leave only folded labels: {routed:?}");
    Ok(())
}

/// THE gate for dropping `duration IS NOT NULL` — the predicate monoscope emits
/// on EVERY latency chart, and the one 2026-08-22's counter A/B isolated as the
/// whole of `filter_not_eligible` (the same p95 query declined
/// `filter_not_eligible` with it and `stale_coverage` without it, 3 reps each).
///
/// Dropping it is NOT free. `percentile_agg` skips nulls, so the VALUES are
/// unaffected — but the raw query also ELIMINATES a bucket whose every row has a
/// null duration, and the rollup, which aggregated all rows, would resurrect it
/// as a 0. So the predicate is replaced by `HAVING sum(duration_count) > 0`,
/// `duration_count` being `count(duration)` and therefore exactly the count of
/// rows the predicate would have kept.
///
/// The fixture is built to break precisely that: one hour whose rows ALL have a
/// null duration, beside hours that mix null and non-null. If the guard is wrong
/// the routed answer gains a bucket the raw answer does not have.
#[serial]
#[tokio::test]
async fn an_all_null_duration_bucket_is_eliminated_identically_through_the_rollup() -> Result<()> {
    struct ClockGuard;
    impl Drop for ClockGuard {
        fn drop(&mut self) {
            timefusion::support::unfreeze();
        }
    }
    let _clock_guard = ClockGuard;
    let mut cfg = (*TestConfigBuilder::new("rollup_null_guard").with_buffer_mode(BufferMode::Enabled).with_rollups().build()).clone();
    cfg.maintenance.timefusion_rollup_realtime_tail = true;
    let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
    db.cancel_maintenance();
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let today = chrono::Utc::now().date_naive();
    let midnight = today.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
    let yesterday_noon = (today - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();

    let row = |id: &str, duration: Option<i64>, ts: i64| -> Result<_> {
        json_to_batch(vec![serde_json::json!({
            "timestamp": ts, "id": id, "name": "op", "project_id": project_id, "hashes": [], "summary": ["null-guard fixture"],
            "date": chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string(),
            "duration": duration, "kind": "server", "status_code": "OK", "resource___service___name": "cart",
        })])
    };

    // Hour 0 mixes null and non-null; hour 1 is ALL null (the bucket that must
    // vanish); hour 2 is all non-null. Yesterday, so it certifies into the tier.
    for (i, (duration, offset)) in
        [(Some(100i64), 17i64), (None, 61_000_000), (Some(300), 130_000_000), (None, 3_661_000_000), (None, 3_700_000_000), (Some(500), 7_330_000_000)]
            .iter()
            .enumerate()
    {
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row(&format!("y{i}"), *duration, yesterday_noon + offset)?], true, None).await?;
    }

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
    timefusion::support::advance_micros(
        timefusion::maintenance_coordinator::FINALIZATION_DELAY_MICROS + timefusion::maintenance_coordinator::INVALIDATION_DEADLINE_BUCKET_MICROS + 1,
    );
    assert!(db.run_maintenance_units(1024).await? > 0, "eligible yesterday slices must be drained");

    // Today, reachable only through the raw leg — including its own all-null
    // hour, so the guard has to hold on BOTH sides of the union.
    for (i, (duration, offset)) in [(None, 5_000_000i64), (None, 60_000_000), (Some(700), 3_700_000_000)].iter().enumerate() {
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row(&format!("t{i}"), *duration, midnight + offset)?], true, None).await?;
    }

    let (lo, hi) = (yesterday_noon + 17, midnight + 7_200_000_017);
    let window = format!("project_id = '{project_id}' AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi})");

    // Without an all-null bucket the test proves nothing, so assert the fixture
    // really contains one before trusting the comparison below.
    let probe = db
        .query_delta_only(&format!(
            "SELECT COUNT(*)::BIGINT FROM (SELECT time_bucket('1 hours', timestamp) tb FROM otel_logs_and_spans WHERE {window} \
             GROUP BY 1 HAVING COUNT(duration) = 0)"
        ))
        .await?;
    let all_null_buckets = probe.iter().filter(|b| b.num_rows() > 0).map(|b| b.column(0).as_primitive::<Int64Type>().value(0)).next().expect("a probe row");
    assert!(all_null_buckets > 0, "the fixture must contain a bucket whose rows ALL have a null duration, got {all_null_buckets}");

    // monoscope's p95 chart, verbatim down to the IS NOT NULL.
    let query = format!(
        "SELECT time_bucket('1 hours', timestamp) AS tb, \
                COALESCE(approx_percentile(0.95, percentile_agg(CAST(duration AS DOUBLE PRECISION))), 0) AS p95 \
         FROM otel_logs_and_spans WHERE {window} AND duration IS NOT NULL GROUP BY 1 ORDER BY 1"
    );

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;

    let rows_of = |batches: Vec<datafusion::arrow::array::RecordBatch>| {
        batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .flat_map(|b| {
                (0..b.num_rows())
                    .map(|r| {
                        (
                            b.column(0).as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>().value(r),
                            b.column(1).as_primitive::<datafusion::arrow::datatypes::Float64Type>().value(r).round() as i64,
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };

    let hits = || timefusion::observability::maintenance_stats().rollup_hits_hybrid.load(std::sync::atomic::Ordering::Relaxed);
    let before = hits();
    let routed = rows_of(ctx.sql(&query).await?.collect().await?);
    assert_eq!(hits(), before + 1, "the p95 chart must route, or this proves nothing about the null guard");

    let raw = rows_of(db.query_delta_only(&query).await?);
    assert_eq!(routed, raw, "the guarded p95 must match the raw aggregate bucket for bucket");
    assert!(!routed.is_empty(), "the fixture must produce buckets, or equality is vacuous: {routed:?}");
    Ok(())
}

/// `count(*)` alongside the guarded percentile must NOT route: raw counts only the
/// rows with a duration, while `request_count` counted every row. This is real
/// monoscope traffic (the top-K endpoint tables put both in one query), so the
/// disqualifier is load-bearing rather than defensive.
#[serial]
#[tokio::test]
async fn a_count_star_beside_the_null_guard_refuses_to_route() -> Result<()> {
    struct ClockGuard;
    impl Drop for ClockGuard {
        fn drop(&mut self) {
            timefusion::support::unfreeze();
        }
    }
    let _clock_guard = ClockGuard;
    let mut cfg = (*TestConfigBuilder::new("rollup_null_guard_count").with_buffer_mode(BufferMode::Enabled).with_rollups().build()).clone();
    cfg.maintenance.timefusion_rollup_realtime_tail = true;
    let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
    db.cancel_maintenance();
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let today = chrono::Utc::now().date_naive();
    let midnight = today.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
    let yesterday_noon = (today - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();

    for (i, (duration, offset)) in [(Some(100i64), 17i64), (None, 61_000_000), (Some(300), 3_661_000_000)].iter().enumerate() {
        let batch = json_to_batch(vec![serde_json::json!({
            "timestamp": yesterday_noon + offset, "id": format!("y{i}"), "name": "op", "project_id": project_id, "hashes": [],
            "summary": ["count fixture"], "date": chrono::DateTime::<chrono::Utc>::from_timestamp_micros(yesterday_noon + offset).unwrap().date_naive().to_string(),
            "duration": duration, "kind": "server", "status_code": "OK", "resource___service___name": "cart",
        })])?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
    }
    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
    timefusion::support::advance_micros(
        timefusion::maintenance_coordinator::FINALIZATION_DELAY_MICROS + timefusion::maintenance_coordinator::INVALIDATION_DEADLINE_BUCKET_MICROS + 1,
    );
    db.run_maintenance_units(1024).await?;

    let (lo, hi) = (yesterday_noon + 17, midnight + 7_200_000_017);
    let query = format!(
        "SELECT time_bucket('1 hours', timestamp) AS tb, COUNT(*) AS c \
         FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
           AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi}) \
           AND duration IS NOT NULL GROUP BY 1 ORDER BY 1"
    );

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let hits = || {
        let stats = timefusion::observability::maintenance_stats();
        stats.rollup_hits_hybrid.load(std::sync::atomic::Ordering::Relaxed) + stats.rollup_hits_full.load(std::sync::atomic::Ordering::Relaxed)
    };
    let before = hits();
    let routed = ctx.sql(&query).await?.collect().await?;
    assert_eq!(hits(), before, "count(*) under a null guard counts a different row set than the tier's request_count; it must NOT route");

    let total = |batches: &[datafusion::arrow::array::RecordBatch]| {
        batches.iter().filter(|b| b.num_rows() > 0).flat_map(|b| (0..b.num_rows()).map(|r| b.column(1).as_primitive::<Int64Type>().value(r))).sum::<i64>()
    };
    assert_eq!(total(&routed), total(&db.query_delta_only(&query).await?), "the refused query must still answer correctly from raw");
    Ok(())
}

// This is the shape production actually sends: microsecond-precision bounds
/// running up to now. Before the union it was refused outright, so the feature
/// never served a single query. A hybrid that merely *runs* is worthless — the
/// failure mode is a plausible wrong number — so this asserts row-for-row
/// equality against the same aggregate computed from raw spans, AND asserts the
/// query really routed. Without the second assertion the test passes vacuously,
/// because a miss also returns the right answer.
///
/// The fixture makes coverage partial deterministically, with no dependence on
/// flush timing: yesterday is certified, today is written afterwards and never
/// certified. So the interior ends at midnight and today's rows can only be
/// reached through the raw leg.
#[serial]
#[tokio::test]
async fn a_partly_covered_window_unions_the_rollup_with_raw_and_matches_the_raw_answer() -> Result<()> {
    struct ClockGuard;
    impl Drop for ClockGuard {
        fn drop(&mut self) {
            timefusion::support::unfreeze();
        }
    }
    let _clock_guard = ClockGuard;
    let mut cfg = (*TestConfigBuilder::new("rollup_hybrid").with_buffer_mode(BufferMode::Enabled).with_rollups().build()).clone();
    cfg.maintenance.timefusion_rollup_realtime_tail = true;
    let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
    // This test publishes selected coverage explicitly below. A background
    // coordinator can race that fixture and change the routing counters being
    // asserted, so isolate the routing behavior under test.
    db.cancel_maintenance();
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let today = chrono::Utc::now().date_naive();
    let midnight = today.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
    let yesterday_noon = (today - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();

    // Varied duration/service so avg/min/max and the group set are all
    // non-trivial, and one service that appears ONLY in the raw tail — the case
    // a naive merge drops.
    let row = |id: &str, service: &str, duration: i64, ts: i64| -> Result<_> {
        json_to_batch(vec![serde_json::json!({
            "timestamp": ts, "id": id, "name": "op", "project_id": project_id, "hashes": [], "summary": ["rollup hybrid fixture"],
            "date": chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string(),
            "duration": duration, "kind": "server", "status_code": "OK",
            "resource___service___name": service,
        })])
    };
    // Yesterday, spread over six genuinely touched hours and starting
    // mid-bucket so the left fringe is non-empty too. The hybrid cost guard
    // intentionally declines a rollup covering less than 20% of the requested
    // window; keeping these rows in one hour made this test depend on the old
    for (i, offset) in
        [17i64, 61_000_000, 130_000_000, 610_000_000, 3_661_000_000, 7_330_000_000, 10_810_000_000, 14_410_000_000, 18_010_000_000].iter().enumerate()
    {
        db.insert_records_batch(
            &project_id,
            "otel_logs_and_spans",
            vec![row(&format!("y{i}"), "cart", 100 + i as i64 * 10, yesterday_noon + offset)?],
            true,
            None,
        )
        .await?;
    }

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
    timefusion::support::advance_micros(
        timefusion::maintenance_coordinator::FINALIZATION_DELAY_MICROS + timefusion::maintenance_coordinator::INVALIDATION_DEADLINE_BUCKET_MICROS + 1,
    );
    assert!(db.run_maintenance_units(1024).await? > 0, "eligible yesterday slices must be drained");

    // Written AFTER certification: today has no coverage, so these are reachable
    // only through the raw leg.
    for (i, offset) in [5_000_000i64, 3_600_000_000].iter().enumerate() {
        db.insert_records_batch(
            &project_id,
            "otel_logs_and_spans",
            vec![row(&format!("t{i}"), "checkout", 500 + i as i64 * 10, midnight + offset)?],
            true,
            None,
        )
        .await?;
    }

    // Unaligned on both ends, exactly like a dashboard's `now`-relative window.
    let (lo, hi) = (yesterday_noon + 17, midnight + 7_200_000_017);
    let query = format!(
        "SELECT resource___service___name AS svc, COUNT(*) AS c, avg(duration) AS mean, min(duration) AS lo, max(duration) AS hi \
         FROM otel_logs_and_spans \
         WHERE project_id = '{project_id}' AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi}) \
         GROUP BY 1 ORDER BY 1"
    );

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;

    // Precondition: certification really did build yesterday's buckets. Without
    // this the routing assertion below cannot distinguish "the rewrite is broken"
    // from "there was nothing to rewrite to".
    let built: i64 = db
        .query_delta_only(&format!(
            "SELECT COALESCE(SUM(request_count), 0)::BIGINT FROM otel_logs_and_spans_rollup_dashboard_1m_v3 WHERE project_id = '{project_id}'"
        ))
        .await?
        .iter()
        .filter(|b| b.num_rows() > 0)
        .filter_map(|b| b.column(0).as_primitive_opt::<Int64Type>().map(|c| c.value(0)))
        .next()
        .unwrap_or(0);
    // On failure ONLY, print what the tier actually holds. This assertion has
    // failed intermittently in CI (`left: 10, right: 9`) while passing 6/6 locally
    // in isolation and 985/985 in a full local suite — it reproduces only under
    // the concurrency of a loaded shard, so it cannot be caught by re-running it.
    //
    // Nine spans are inserted, so a sum of TEN is a double count, not an extra
    // row. The dump distinguishes the two shapes that produce it — one bucket
    // with `request_count = 2`, versus two live `rollup_generation`s covering the
    // same bucket (the #139 shape, where a wider file and a narrower one both
    // stay live and a SUM counts both). Those want opposite fixes, and the
    // counter alone cannot tell them apart.
    if built != 9 {
        let rows = db
            .query_delta_only(&format!(
                "SELECT CAST(date AS VARCHAR), CAST(timestamp AS VARCHAR), request_count, rollup_generation \
                 FROM otel_logs_and_spans_rollup_dashboard_1m_v3 WHERE project_id = '{project_id}' ORDER BY 2"
            ))
            .await?;
        println!("rollup tier contents at the failing assertion:\n{}", datafusion::arrow::util::pretty::pretty_format_batches(&rows).unwrap());
    }
    assert_eq!(built, 9, "certification must have rolled up all nine of yesterday's spans");
    let base_target = db.unified_tables().read().await.get("otel_logs_and_spans_rollup_dashboard_1m_v3").expect("base rollup table created").clone();
    let tagged_files = base_target
        .read()
        .await
        .snapshot()?
        .log_data()
        .iter()
        .filter(|file| {
            #[allow(deprecated)]
            let action = file.add_action();
            action.tags.as_ref().is_some_and(|tags| tags.contains_key(timefusion::maintenance_coordinator::TAG_SLICE_START))
        })
        .count();
    // Every published file must carry its slice tags — a derived unit selects
    // its input by them, so an untagged file is invisible to the coarse tier.
    // Counted as "all of them", not ">= 6": that literal encoded the
    // pre-coarsening slice granularity, and `coarsen_sealed_slices` (#134) now
    // collapses a sealed day's fine units into one, so a correct run publishes
    // ONE day-wide tagged file. The invariant is that none is untagged.
    let total_files = base_target.read().await.snapshot()?.log_data().iter().count();
    assert!(
        tagged_files > 0 && tagged_files == total_files,
        "every published slice must retain its Delta Add tags (got {tagged_files} tagged of {total_files})"
    );
    let derived_built: i64 = db
        .query_delta_only(&format!(
            "SELECT COALESCE(SUM(request_count), 0)::BIGINT FROM otel_logs_and_spans_rollup_dashboard_1h_v2 WHERE project_id = '{project_id}'"
        ))
        .await?
        .iter()
        .filter(|b| b.num_rows() > 0)
        .filter_map(|b| b.column(0).as_primitive_opt::<Int64Type>().map(|c| c.value(0)))
        .next()
        .unwrap_or(0);
    assert_eq!(derived_built, 9, "derived coverage must merge every independently published base slice");

    let rows_of = |batches: Vec<datafusion::arrow::array::RecordBatch>| {
        batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .flat_map(|b| {
                (0..b.num_rows())
                    .map(|r| {
                        (
                            array_get_str(b.column(0).as_ref(), r),
                            b.column(1).as_primitive::<Int64Type>().value(r),
                            b.column(2).as_primitive::<datafusion::arrow::datatypes::Float64Type>().value(r),
                            b.column(3).as_primitive::<Int64Type>().value(r),
                            b.column(4).as_primitive::<Int64Type>().value(r),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };

    // The routing decision is only observable through this counter. `EXPLAIN`
    // cannot see it: the substitution happens in `DmlQueryPlanner`, and EXPLAIN
    // renders its inner plan with the DEFAULT planner, so an explained query
    // always shows the raw scan even when the real one routes.
    let hits = || timefusion::observability::maintenance_stats().rollup_hits_hybrid.load(std::sync::atomic::Ordering::Relaxed);
    let misses = || timefusion::observability::maintenance_stats().rollup_misses_total.load(std::sync::atomic::Ordering::Relaxed);
    let miss_snapshot = || {
        let stats = timefusion::observability::maintenance_stats();
        [
            stats.rollup_miss_not_built.load(std::sync::atomic::Ordering::Relaxed),
            stats.rollup_miss_stale_coverage.load(std::sync::atomic::Ordering::Relaxed),
            stats.rollup_miss_tiny_interior.load(std::sync::atomic::Ordering::Relaxed),
            stats.rollup_miss_too_many_branches.load(std::sync::atomic::Ordering::Relaxed),
            stats.rollup_miss_incomplete_coverage.load(std::sync::atomic::Ordering::Relaxed),
        ]
    };
    let (before, misses_before) = (hits(), misses());
    let reasons_before = miss_snapshot();
    let hybrid = rows_of(ctx.sql(&query).await?.collect().await?);
    let reasons_after = miss_snapshot();
    let reason_delta = std::array::from_fn::<_, 5, _>(|index| reasons_after[index] - reasons_before[index]);
    assert_eq!(
        hits(),
        before + 1,
        "the query must be served from the rollup as a hybrid union, not a raw scan (misses +{}, reason delta [not_built, stale, tiny, branches, incomplete] = {reason_delta:?})",
        misses() - misses_before
    );

    // `query_delta_only` bypasses both the rollup and the buffer, so it is the
    // authority: every fixture row was written straight to Delta.
    let raw = rows_of(db.query_delta_only(&query).await?);

    assert_eq!(hybrid, raw, "the hybrid rewrite must equal the raw aggregate exactly");
    assert_eq!(hybrid.len(), 2, "both services must survive, including the one that exists only in the raw tail: {hybrid:?}");
    assert_eq!(hybrid.iter().map(|row| row.1).sum::<i64>(), 11, "every fixture row must be counted exactly once: {hybrid:?}");

    // THE SAME WINDOW WITH NO UPPER BOUND — the shape monoscope's 7d and 14d
    // panels actually send. Until 2026-08-15 the matcher demanded both bounds
    // and answered `UnboundedTime`, so a fully built window fell back to a raw
    // scan and timed out at the 60s statement cap.
    //
    // Accepting it introduces the opposite failure: `hi` becomes a plan-time
    // stand-in, and if the trailing raw range closed at it the rewrite would
    // silently drop the newest rows. This row sits PAST the bounded window, so
    // only an open tail can reach it — the bounded assertions above still see 6
    // because it is outside their `hi`.
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("t_open", "checkout", 700, hi + 1_000_000)?], true, None).await?;
    // Keep the open-ended plan-time upper bound deterministic. If this test
    // inherits the wall clock late in the UTC day, the same six-hour covered
    // interior falls below the 20% hybrid cost threshold and the assertion
    // becomes time-of-day dependent.
    timefusion::support::set_micros(hi + 3_600_000_000);
    let open = query.replace(&format!(" AND timestamp < to_timestamp_micros({hi})"), "");
    let before = hits();
    let open_reasons_before = miss_snapshot();
    let open_rows = rows_of(ctx.sql(&open).await?.collect().await?);
    let open_reasons_after = miss_snapshot();
    let open_reason_delta = std::array::from_fn::<_, 5, _>(|index| open_reasons_after[index] - open_reasons_before[index]);
    assert_eq!(
        hits(),
        before + 1,
        "an open-ended window must route, not fall back to a raw scan (misses +{}, reason delta [not_built, stale, tiny, branches, incomplete] = {open_reason_delta:?})",
        misses() - misses_before
    );
    assert_eq!(open_rows, rows_of(db.query_delta_only(&open).await?), "the open-ended rewrite must equal the raw aggregate exactly");
    assert_eq!(open_rows.iter().map(|row| row.1).sum::<i64>(), 12, "the open tail must reach the row past the bounded window: {open_rows:?}");

    // The shape prod's `rollup_declined_shape` log printed for months: a CAST
    // over an aggregate plus `ORDER BY <an aggregate> LIMIT n` optimizes to
    // `Projection(Sort(Projection(Aggregate)))`. Every peeling matcher declined
    // it — and no bare-`SessionContext` unit test could reproduce it, because
    // WHERE the optimizer puts that Sort depends on the session's analyzer
    // rules. This runs on the real `Database` session, which is the only place
    // the difference exists. `LIMIT 1` keeps it deterministic: the top bucket
    // holds 4 rows and the other two hold 1 each.
    let shaped = format!(
        "SELECT COUNT(*) AS c, avg(duration)::BIGINT AS m FROM otel_logs_and_spans \
         WHERE project_id = '{project_id}' AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi}) \
         GROUP BY time_bucket('1 hours', timestamp) ORDER BY 1 DESC LIMIT 1"
    );
    let counts = |batches: Vec<datafusion::arrow::array::RecordBatch>| {
        batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .flat_map(|b| {
                (0..b.num_rows())
                    .map(|r| (b.column(0).as_primitive::<Int64Type>().value(r), b.column(1).as_primitive::<Int64Type>().value(r)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };
    let before = hits();
    let routed = counts(ctx.sql(&shaped).await?.collect().await?);
    assert_eq!(hits(), before + 1, "the shape that defeated every peeling matcher must route (misses +{})", misses() - misses_before);
    assert_eq!(routed, counts(db.query_delta_only(&shaped).await?), "the substituted plan must equal the raw answer exactly");

    // An UPDATE scoped to TODAY must not invalidate YESTERDAY's coverage.
    //
    // Regression, prod 2026-08-11: every DML wiped coverage for the whole
    // (project, table) — all dates, both tiers. monoscope issues ~400 scoped
    // enrichment UPDATEs per 10 minutes, so nine days of built rollups could
    // never survive to serve one read, and `otel_logs_and_spans` reported
    // `not_built` for every date while `otel_metrics` — which takes no DML —
    // routed the same shape in 1s.
    ctx.sql(&format!(
        "UPDATE otel_logs_and_spans SET hashes = make_array('enriched') WHERE project_id = '{project_id}' \
         AND timestamp >= to_timestamp_micros({midnight}) AND timestamp < to_timestamp_micros({})",
        midnight + 7_200_000_000i64
    ))
    .await?
    .collect()
    .await?;
    let before = hits();
    let after_update = rows_of(ctx.sql(&query).await?.collect().await?);
    assert_eq!(hits(), before + 1, "a DML confined to today must leave yesterday's coverage intact (misses +{})", misses() - misses_before);
    assert_eq!(after_update.iter().map(|row| row.1).sum::<i64>(), 11, "the enrichment must not change the counted rows: {after_update:?}");

    // monoscope's Golden Signals row filter, which is exactly the predicate the
    // server_* measures declare. Unit tests pass this on a bare MemTable session;
    // prod declined it, so the canonicalization must be checked through a real
    // Database session where the analyzer rules actually run.
    let golden = format!(
        "SELECT time_bucket('1 hours', timestamp) AS b, COUNT(*) AS c FROM otel_logs_and_spans \
         WHERE project_id = '{project_id}' AND (kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http') \
           AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi}) GROUP BY 1 ORDER BY 2 DESC"
    );
    let before = hits();
    let routed_golden = ctx.sql(&golden).await?.collect().await?;
    assert_eq!(hits(), before + 1, "the promoted row filter must route (misses +{})", misses() - misses_before);
    assert_eq!(
        routed_golden.iter().map(|b| b.num_rows()).sum::<usize>(),
        db.query_delta_only(&golden).await?.iter().map(|b| b.num_rows()).sum::<usize>(),
        "the promoted rewrite must return the same buckets as the raw query"
    );

    // The same, with a predicate that says NOTHING about time. On a merge-on-read
    // table the re-appended row invalidates its own date and no other, so
    // precision here does not depend on the predicate's shape — which is the
    // difference between working and getting lucky, since monoscope's enrichment
    // happens to carry a time range but nothing guarantees the next writer will.
    ctx.sql(&format!("UPDATE otel_logs_and_spans SET hashes = make_array('by-id') WHERE project_id = '{project_id}' AND id = 't0'")).await?.collect().await?;
    let before = hits();
    let after_by_id = rows_of(ctx.sql(&query).await?.collect().await?);
    assert_eq!(hits(), before + 1, "an id-scoped DML on today's row must leave yesterday's coverage intact (misses +{})", misses() - misses_before);
    assert_eq!(after_by_id.iter().map(|row| row.1).sum::<i64>(), 11, "the id-scoped update must not change the counted rows: {after_by_id:?}");

    // LAST, because it writes a new row and would move every count above it.
    //
    // A derived slice that cannot publish — a wider live file already covers it
    // — must REOPEN that covering slice rather than quietly complete.
    // `invalidate` mints derived work at DERIVED_SLICE_MICROS, so late rows for
    // one hour inside an already-published day arrive as an hour-wide unit, and
    // dropping it leaves that hour permanently stale in the coarse tier: a wrong
    // number, not a slow one. Prod confirmed the branch is reachable —
    // `rollup_skipped_covered_by_wider` moved to 5 within an hour of shipping
    // the counter (#145), which is the condition that PR set for doing this.
    let derived_of = |db: Arc<Database>, project_id: String| async move {
        let batches = db
            .query_delta_only(&format!(
                "SELECT COALESCE(SUM(request_count), 0)::BIGINT FROM otel_logs_and_spans_rollup_dashboard_1h_v2 WHERE project_id = '{project_id}'"
            ))
            .await?;
        Ok::<i64, anyhow::Error>(
            batches.iter().filter(|b| b.num_rows() > 0).filter_map(|b| b.column(0).as_primitive_opt::<Int64Type>().map(|c| c.value(0))).next().unwrap_or(0),
        )
    };
    let derived_before = derived_of(Arc::clone(&db), project_id.clone()).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("y-late", "cart", 999, yesterday_noon + 90_000_000)?], true, None).await?;
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
    // Past the live-frontier window too, so the late row's day is SEALED whatever
    // hour the suite runs at. `yesterday_noon` is only `12 + H` hours old at UTC
    // hour H, so before noon it falls INSIDE LIVE_FRONTIER_WINDOW_MICROS and takes
    // the frontier scheduling path rather than the sealed one this asserts on.
    // That made the test pass at 23:00 UTC and fail at 03:00 UTC on the very same
    // commit — it failed CI on 2026-08-18 and reproduced identically on master.
    timefusion::support::advance_micros(
        timefusion::maintenance_coordinator::LIVE_FRONTIER_WINDOW_MICROS
            + timefusion::maintenance_coordinator::FINALIZATION_DELAY_MICROS
            + timefusion::maintenance_coordinator::INVALIDATION_DEADLINE_BUCKET_MICROS
            + 1,
    );
    // Twice: the first drain may escalate, the second rebuilds the covering slice.
    db.run_maintenance_units(1024).await?;
    db.run_maintenance_units(1024).await?;
    let derived_after = derived_of(Arc::clone(&db), project_id.clone()).await?;
    assert!(derived_after > derived_before, "a late row inside an already-published day must reach the 1h tier (was {derived_before}, now {derived_after})");
    Ok(())
}

/// A multi-day window can only route if the OLD days are covered, and the dedup
/// sweep never reaches past its lookback — so before the backfill a 7d or 30d
/// query could never route no matter how long the process ran, while a 24h one
/// could. That asymmetry is the whole reason the expensive queries stayed slow.
///
/// Also pins that untagged whole-day output is not scanned and re-adopted at
/// restart. Only coordinator slice publications carry enough Add-tag identity
/// for metadata-only recovery; legacy output safely falls back to raw reads.
#[serial]
#[tokio::test]
async fn backfill_covers_sealed_days_and_legacy_coverage_falls_back_after_restart() -> Result<()> {
    let mut cfg = (*TestConfigBuilder::new("rollup_backfill").with_buffer_mode(BufferMode::Enabled).with_rollups().build()).clone();
    cfg.maintenance.timefusion_rollup_realtime_tail = true;
    cfg.maintenance.timefusion_rollup_backfill_days = 7;
    let cfg = Arc::new(cfg);
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let today = chrono::Utc::now().date_naive();
    let day = |back: i64| (today - chrono::Duration::days(back)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
    let row = |id: &str, ts: i64| -> Result<_> {
        json_to_batch(vec![serde_json::json!({
            "timestamp": ts, "id": id, "name": "op", "project_id": project_id, "hashes": [], "summary": ["backfill fixture"],
            "date": chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string(),
            "duration": 100, "kind": "server", "status_code": "OK", "resource___service___name": "cart",
        })])
    };
    // D-5 and D-4 are sealed and far outside the dedup lookback (default 1).
    for back in [5i64, 4] {
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row(&format!("d{back}"), day(back))?], true, None).await?;
    }

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

    let covered = |db: Arc<Database>| {
        let project_id = project_id.clone();
        async move {
            anyhow::Ok(
                db.query_delta_only(&format!(
                    "SELECT COALESCE(SUM(request_count), 0)::BIGINT FROM otel_logs_and_spans_rollup_dashboard_1m_v3 WHERE project_id = '{project_id}'"
                ))
                .await?
                .iter()
                .filter(|b| b.num_rows() > 0)
                .filter_map(|b| b.column(0).as_primitive_opt::<Int64Type>().map(|c| c.value(0)))
                .next()
                .unwrap_or(0),
            )
        }
    };
    assert_eq!(covered(Arc::clone(&db)).await?, 0, "the sweep must NOT reach sealed days — that gap is what the backfill exists to close");

    let built = {
        // Plan, then step past FINALIZATION_DELAY: units are minted with a
        // deadline in the future, so draining immediately claims nothing.
        db.plan_rollup_backfill().await?;
        timefusion::support::advance_micros(16 * 60 * 1_000_000);
        db.drain_coordinator_rollups(64).await?
    };
    // Units run, not days built — the coordinator splits a day across slices,
    // so only the coverage assertion below states the property. This one just
    // stops the test passing vacuously on a drain that claimed nothing.
    assert!(built > 0, "the backfill must actually run units");
    assert_eq!(covered(Arc::clone(&db)).await?, 2, "both sealed spans must be rolled up");

    // Restart over the SAME config (same storage prefix). Coverage is proved
    // from the slice identity tags on the Add actions, so what the coordinator
    // published must still be provable in a process that swept nothing — TF
    // deploys several times a day and re-earning coverage from scratch each
    // time is what kept `rollup_min_contiguous_days` pinned near zero.
    //
    // This assertion used to require 0, because the builder it covered was the
    // orphaned `rollup_backfill_tick`, whose output carried no tags. That
    // writer is gone; requiring its weakness of its replacement would assert
    // the opposite of what the system needs.
    let restarted = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let recovered = restarted.recover_rollup_coverage("otel_logs_and_spans").await?;
    assert!(recovered > 0, "tagged coverage must survive a restart without a fresh sweep");

    // Recovery restores SLICE coverage; the date-level map is a separate route
    // through `ProjectRoutingTable::scan`, produced at publish time. Pinned here
    // too so a regression shows up in the test that already builds rollups.
    assert!(db.rollup_coverage_entries() > 0, "the build must have recorded date-level coverage as well as slice coverage");
    Ok(())
}

/// A committed rollup must record DATE-level coverage, not only slice coverage.
///
/// Until 2026-08-22 `rollup_coverage` had **no producer**: every use of the
/// private field was a read (the routing lookup in `mod.rs`) or a removal
/// (`maintain.rs` x2), with no `insert` anywhere, so it could not be non-empty
/// and the date-level lookup returned `None` for every date on every process.
/// Two comments still described the mechanism that had gone.
///
/// It left no runtime trace — the `None` branch deliberately `continue`s WITHOUT
/// setting a miss reason, so queries fell through to slice coverage and were
/// correct, merely unrouted. No miss, no error, no log.
///
/// Why it mattered: slice coverage is the path gated by the per-slice witness
/// rule, and prod 2026-08-22 measured `stale_coverage` as the SOLE miss reason
/// on every bare dashboard shape with `rollup_hits_* = 0` — 95.2% of it
/// witness-less slices. There was no second route to fall back on because the
/// second route was inert. This one does not consult the witness at all.
///
/// Asserts `built > 0` FIRST: an empty drain would satisfy the coverage
/// assertion vacuously, and an earlier revision of this test did exactly that
/// against a bare config that drained zero units.
#[serial]
#[tokio::test]
async fn a_committed_rollup_records_date_level_coverage() -> Result<()> {
    // Same fixture as `backfill_covers_sealed_days_…` above, which is the test
    // that proves these units really commit. Reusing its shape matters: a bare
    // config drains ZERO units and the assertion then passes vacuously.
    let mut cfg = (*TestConfigBuilder::new("rollup_cov_producer").with_buffer_mode(BufferMode::Enabled).with_rollups().build()).clone();
    cfg.maintenance.timefusion_rollup_realtime_tail = true;
    cfg.maintenance.timefusion_rollup_backfill_days = 7;
    let cfg = Arc::new(cfg);
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let today = chrono::Utc::now().date_naive();
    for back in [5i64, 4] {
        let ts = (today - chrono::Duration::days(back)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
        let batch = json_to_batch(vec![serde_json::json!({
            "timestamp": ts, "id": format!("d{back}"), "name": "op", "project_id": project_id, "hashes": [], "summary": ["coverage fixture"],
            "date": chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string(),
            "duration": 100, "kind": "server", "status_code": "OK", "resource___service___name": "cart",
        })])?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
    }
    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

    db.plan_rollup_backfill().await?;
    timefusion::support::advance_micros(16 * 60 * 1_000_000);
    let built = db.drain_coordinator_rollups(64).await?;
    // Guard against a vacuous verdict: an empty drain would "prove" the defect
    // without ever exercising the commit path that should record coverage.
    assert!(built > 0, "no rollup units ran, so this says nothing about coverage recording");
    assert!(db.rollup_coverage_entries() > 0, "a committed rollup must record DATE-level coverage, not only slice coverage");
    Ok(())
}

/// A certification must survive the process (`timefusion_dedup_certification_persist`).
///
/// This is the whole point of persisting them: `dedup_clean_fp` is process-local
/// and TF deploys several times a day, so the read-side skip spends much of its
/// life starting from cold. Nothing covered a restart before, which is exactly
/// how the cache could stay useless without any test noticing.
#[serial]
#[tokio::test]
async fn a_certification_survives_a_restart_and_still_grants_the_skip() -> Result<()> {
    init_local_metrics_for_test();
    let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
    let cfg = {
        let mut cfg = (*TestConfigBuilder::new("cert_restart").with_buffer_mode(BufferMode::Enabled).build()).clone();
        cfg.maintenance.timefusion_read_dedup_skip_swept = true;
        cfg.maintenance.timefusion_dedup_certification_persist = true;
        Arc::new(cfg)
    };
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let sql = format!(
        "SELECT name FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
         AND timestamp >= to_timestamp_micros({}) AND timestamp < to_timestamp_micros({})",
        ts - 1,
        ts + 1_000
    );

    {
        let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
        let batch = json_to_batch((0..40).map(|i| test_span_ts(&format!("k{i}"), "n", &project_id, ts + i as i64)).collect())?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
        let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
        db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
        assert_eq!(counter_value(scan_metric_names::CERT_GRANTED_TOTAL), 1, "the sweep must certify before a restart can carry it");
    }

    // A brand-new Database over the same data dir — a deploy, in miniature. Its
    // `dedup_clean_fp` starts empty and is filled only from what was persisted.
    let db = Arc::new(Database::with_config(cfg).await?);
    db.query_delta_only(&sql).await?; // warm `try_fast_resolve`; cold it declines as Unresolved
    let before = counter_value(scan_metric_names::DEDUP_SKIPPED);
    db.query_delta_only(&sql).await?;
    assert_eq!(counter_value(scan_metric_names::DEDUP_SKIPPED) - before, 1, "the reloaded certification must grant the skip; a fresh process swept nothing");
    Ok(())
}

/// A partition that HAD duplicates must still end up certified, without waiting
/// for an unrelated commit to come along.
///
/// `record_certification` needs a 0-drop pass over an unmoved file set, so the
/// pass that rewrites certifies nothing and the next one is meant to confirm it.
/// The sweep's global version guard used to prevent that: it returns immediately
/// while the table version is unchanged, and the rewriting pass was the last
/// thing to move it. The confirmation then waited on someone else's write — in
/// prod, other projects' ingest; on a quiet table, possibly never. That left the
/// partitions that had duplicates as the ones least likely to be certified,
/// which is precisely backwards.
#[serial]
#[tokio::test]
async fn a_rewriting_sweep_is_confirmed_by_the_next_pass_with_no_other_commit() -> Result<()> {
    init_local_metrics_for_test();
    let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
    let mut cfg = (*TestConfigBuilder::new("dedup_confirming_pass").with_buffer_mode(BufferMode::Enabled).build()).clone();
    cfg.maintenance.timefusion_read_dedup_skip_swept = true;
    // Rollups bypass the version guard entirely (`needs_rollup_retry`), which
    // would hide the regression this test exists for.
    cfg.maintenance.timefusion_rollup_enabled = false;
    let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // The same keys twice, in separate flushes: cross-file duplicates, so the
    let batch = || json_to_batch((0..40).map(|i| test_span_ts(&format!("k{i}"), "n", &project_id, ts + i as i64)).collect());
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch()?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch()?], true, None).await?;

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    let certs = || counter_value(scan_metric_names::CERT_GRANTED_TOTAL);

    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
    assert_eq!(certs(), 0, "a pass that rewrites must not certify what it just rewrote");

    // No write in between — that is the whole point.
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
    assert_eq!(certs(), 1, "the confirming pass must run off the back of the rewrite, not wait for an unrelated commit");
    Ok(())
}

/// The denial split that decides whether persisting certifications could ever
/// pay (`docs/plans/2026-08-11-certification-survival.md`, Phase 0).
///
/// On prod the skip fires on 0.2–0.5% of Delta-reading scans, and a single
/// `dedup_denied_uncertified` counter cannot say why. The two causes have
/// opposite conclusions: `never_certified` is what persisting or warming
/// `dedup_clean_fp` would recover, `fp_moved` is what nothing recovers — the
/// partition genuinely changed. This walks ONE partition through all three
/// states in order, because the counters are only worth reading if each state is
/// reached deliberately.
#[serial]
#[tokio::test]
async fn a_denied_skip_says_whether_it_was_never_certified_or_written_to_since() -> Result<()> {
    init_local_metrics_for_test();
    let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
    let mut cfg = (*TestConfigBuilder::new("dedup_denial_split").with_buffer_mode(BufferMode::Enabled).build()).clone();
    cfg.maintenance.timefusion_read_dedup_skip_swept = true;
    let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let write = |lo: usize, hi: usize| {
        let (db, project_id) = (Arc::clone(&db), project_id.clone());
        async move {
            let batch = json_to_batch((lo..hi).map(|i| test_span_ts(&format!("k{i}"), "n", &project_id, ts + i as i64)).collect())?;
            db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await
        }
    };
    write(0, 40).await?;

    let sql = format!(
        "SELECT name FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
         AND timestamp >= to_timestamp_micros({}) AND timestamp < to_timestamp_micros({})",
        ts - 1,
        ts + 1_000
    );
    let counts = || {
        (
            counter_value(scan_metric_names::DEDUP_DENIED_NEVER_CERTIFIED),
            counter_value(scan_metric_names::DEDUP_DENIED_FP_MOVED),
            counter_value(scan_metric_names::DEDUP_SKIPPED),
            counter_value(scan_metric_names::CERT_DWELL_TOTAL),
        )
    };
    // Warm `try_fast_resolve` first: cold, the skip declines as `Unresolved`
    // before it ever consults a certification, and every assertion below reads
    // zero. That the cold query lands in `Unresolved` rather than in the
    // uncertified bucket is itself part of what makes the split readable.
    db.query_delta_only(&sql).await?;

    let base = counts();
    db.query_delta_only(&sql).await?;
    let never = counts();
    assert_eq!((never.0 - base.0, never.1 - base.1), (1, 0), "an unswept partition must be denied as never-certified, not as a moved fingerprint");

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
    db.query_delta_only(&sql).await?;
    let certified = counts();
    assert_eq!(certified.2 - never.2, 1, "a swept partition must actually grant the skip, or the split below measures nothing");

    // A commit into the certified partition moves its fingerprint. This is the
    // irreducible denial — no persistence layer recovers it.
    write(40, 80).await?;
    db.query_delta_only(&sql).await?;
    let moved = counts();
    assert_eq!((moved.1 - certified.1, moved.0 - certified.0), (1, 0), "a written-to partition must be denied as fp_moved, not as never-certified");
    assert_eq!(moved.3 - certified.3, 1, "observing the moved fingerprint must close the certification's dwell");

    // ...and close it exactly once: the stale entry is dropped when observed, so
    // a second read must not re-report the same certification's lifetime.
    db.query_delta_only(&sql).await?;
    assert_eq!(counts().3, moved.3, "dwell must be recorded on the first observation only");
    Ok(())
}

/// A window with no certified partition must NOT be granted the skip.
///
/// `dedup_window_clean` seeds its verdict with `Granted` and every `continue`
/// leaves it untouched, so a window whose dates all lack Delta files under this
/// project's key returned `Granted` having examined nothing — a
/// "provably duplicate-free" verdict derived from an absence of evidence. The
/// skip it authorises removes `DedupExec` from the WHOLE scan, not just the
/// Delta leg, and the MemBuffer and hot-tier legs unioned in can hold
/// superseded merge-on-read versions of their own.
///
/// Written after prod 2026-08-20, where `count(*)` and `count(distinct id)`
/// disagreed 4x on the same snapshot in the same second (112,595 vs 27,909) —
/// the shape of a count that skipped dedup while a scan did not.
#[serial]
#[tokio::test]
async fn an_uncertified_window_is_never_granted_the_dedup_skip() -> Result<()> {
    init_local_metrics_for_test();
    let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
    let mut cfg = (*TestConfigBuilder::new("dedup_empty_window").with_buffer_mode(BufferMode::Enabled).build()).clone();
    cfg.maintenance.timefusion_read_dedup_skip_swept = true;
    let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // Rows for ONE day, then query a window whose dates are all EARLIER — so
    // every date resolves to no files for this project and the loop skips them
    // all without ever consulting a certification.
    let batch = json_to_batch((0..40).map(|i| test_span_ts(&format!("k{i}"), "n", &project_id, ts + i as i64)).collect())?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;

    let (lo, hi) = (ts - 10 * 86_400_000_000, ts - 7 * 86_400_000_000);
    let sql = format!(
        "SELECT name FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
         AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi})"
    );
    // Warm `try_fast_resolve`: cold, the skip declines as `Unresolved` before it
    // reaches the verdict this test is about.
    db.query_delta_only(&sql).await?;
    let before = counter_value(scan_metric_names::DEDUP_SKIPPED);
    db.query_delta_only(&sql).await?;
    let skipped = counter_value(scan_metric_names::DEDUP_SKIPPED) - before;

    assert_eq!(skipped, 0, "a window that certified nothing must not be granted the skip — that is granting from an absence of evidence");
    Ok(())
}

/// COUNT parity for the per-FILE skip, over a partition that CHURNED after it
/// was certified — the case the whole-partition and per-date skips both refuse
/// and the reason this exists.
///
/// Certification is keyed on a partition's entire file set, so one new file
/// voids it. Recent partitions gain files continuously, which is why prod
/// 2026-08-22 measured `dedup_denied_never_certified` at 100% of eligible
/// scans. Per FILE, a new file costs only the files it OVERLAPS: the proved
/// files stay skippable when no uncertified file could hold another version of
/// their rows (`read::skippable_certified_files`).
///
/// The fixture makes that concrete: certified rows in one timestamp band, then
/// a later batch — itself duplicated across flushes — in a DISJOINT band. The
/// second batch moves the fingerprint, so the old skips must all decline; the
/// per-file skip must still fire for the first band while the second is still
/// deduplicated. Parity against the same data with the feature off is the
/// assertion that matters, because the failure mode is a silent over-count.
#[serial]
#[tokio::test]
async fn count_is_identical_with_and_without_the_per_file_dedup_skip() -> Result<()> {
    init_local_metrics_for_test();
    const DUPLICATED: usize = 200;
    const UNIQUE: usize = 80;
    const CHURN: usize = 60;
    // A second apart, so the churn batch's file span cannot touch the certified
    // band's. Overlapping bands would (correctly) refuse the skip, and the test
    // would pass while proving nothing.
    const CHURN_OFFSET: i64 = 1_000_000;
    let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();

    let run = |per_file: bool, tag: &'static str| async move {
        let mut cfg = (*TestConfigBuilder::new(tag).with_buffer_mode(BufferMode::Enabled).build()).clone();
        cfg.maintenance.timefusion_read_dedup_skip_per_file = per_file;
        let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
        let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

        let batch = |lo: usize, hi: usize, offset: i64| -> Result<_> {
            json_to_batch((lo..hi).map(|i| test_span_ts(&format!("k{i}"), "n", &project_id, ts + offset + i as i64)).collect())
        };
        // Cross-file duplicates plus keys written once, then certified.
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch(0, DUPLICATED, 0)?], true, None).await?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch(0, DUPLICATED, 0)?], true, None).await?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch(DUPLICATED, DUPLICATED + UNIQUE, 0)?], true, None).await?;

        let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
        db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
        db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

        // THE CHURN: written after certification, in a disjoint band, and itself
        // duplicated across two flushes so the uncertified leg genuinely still
        // has work to do. This moves the partition fingerprint, so every
        // all-or-nothing skip must now decline.
        let churn = |lo: usize, hi: usize| batch(lo, hi, CHURN_OFFSET);
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![churn(1000, 1000 + CHURN)?], true, None).await?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![churn(1000, 1000 + CHURN)?], true, None).await?;

        let (lo, hi) = (ts - 1, ts + CHURN_OFFSET + (1000 + CHURN) as i64 + 1);
        // Rows, not `count(*)`: a bare count is answered from Delta statistics
        // without building a scan, so it exercises neither DedupExec nor the
        // skip that removes it.
        let sql = format!(
            "SELECT id FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
             AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi})"
        );
        // Warm the fast-resolve cache; a cold first query declines before it ever
        // reaches the skip.
        db.query_delta_only(&sql).await?;
        let before = counter_value(scan_metric_names::DEDUP_SKIPPED_PER_FILE);
        let batches = db.query_delta_only(&sql).await?;
        let n = batches.iter().map(|b| b.num_rows()).sum::<usize>() as i64;
        anyhow::Ok((n, counter_value(scan_metric_names::DEDUP_SKIPPED_PER_FILE) - before))
    };

    let (authoritative, control_skips) = run(false, "per_file_parity_off").await?;
    let (with_skip, skips) = run(true, "per_file_parity_on").await?;

    assert_eq!(authoritative, (DUPLICATED + UNIQUE + CHURN) as i64, "the control itself must be right: every key counted exactly once");
    assert_eq!(control_skips, 0, "the control must run with the per-file skip genuinely off");
    assert!(skips > 0, "the per-file skip never engaged, so this proves nothing — check the disjoint band and the fast-resolve warm-up");
    assert_eq!(with_skip, authoritative, "the per-file skip changed the answer: it must never over-count");
    Ok(())
}

/// COUNT parity — the precondition `timefusion_read_dedup_skip_swept`'s own doc
/// names ("off by default until COUNT parity is validated on prod-shaped
/// data"). The skip removes `DedupExec` and its key projection, so if the
/// certification is ever wrong a `count(*)` silently over-counts on every
/// dashboard. This runs the SAME data both ways and demands identical answers.
///
/// Prod-shaped means what actually produces duplicates here: many keys, each
/// written twice across separate flushes so the copies land in different Delta
/// files (flush-time dedup is per-bucket and cannot see across files), plus
/// unique keys that must not be collapsed.
#[serial]
#[tokio::test]
async fn count_is_identical_with_and_without_the_dedup_skip() -> Result<()> {
    init_local_metrics_for_test();
    const DUPLICATED: usize = 250;
    const UNIQUE: usize = 120;
    let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();

    // One dataset, two engines: skip disabled (the authority) and enabled.
    let run = |skip: bool, tag: &'static str| async move {
        let mut cfg = (*TestConfigBuilder::new(tag).with_buffer_mode(BufferMode::Enabled).build()).clone();
        cfg.maintenance.timefusion_read_dedup_skip_swept = skip;
        let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
        let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

        let batch =
            |lo: usize, hi: usize| -> Result<_> { json_to_batch((lo..hi).map(|i| test_span_ts(&format!("k{i}"), "n", &project_id, ts + i as i64)).collect()) };
        // Two flushes covering the same keys -> cross-file duplicates.
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch(0, DUPLICATED)?], true, None).await?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch(0, DUPLICATED)?], true, None).await?;
        // ...plus keys written once, which must survive intact.
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch(DUPLICATED, DUPLICATED + UNIQUE)?], true, None).await?;

        let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
        // TWICE. The first pass drops duplicates, and `record_certification`
        // requires `dropped == 0` over a file set that did not move — a pass
        // cannot certify what it just rewrote. The second is the confirming pass,
        // and without it the skip below never engages. (That the second pass runs
        // at all, with nothing committed in between, is
        // `a_rewriting_sweep_is_confirmed_by_the_next_pass_with_no_other_commit`.)
        db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
        db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

        // TIME-BOUNDED. Without a window `dedup_skip_allowed` returns `NoWindow`
        // and refuses before it ever looks at a certification, so the unbounded
        // gate. The `skips` assertion below is what stops that recurring silently.
        let (lo, hi) = (ts - 1, ts + (DUPLICATED + UNIQUE) as i64 + 1);
        // Rows, counted here — NOT `count(*)`. A bare count is answered by
        // `count_pushdown` from Delta statistics without ever building a scan, so
        // it exercises neither `DedupExec` nor the skip that removes it. Counting
        // the rows the scan actually returns is the same assertion about the same
        // hazard (silent over-counting), through the path that can produce it.
        let sql = format!(
            "SELECT id FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
             AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi})"
        );
        // Warm the fast-resolve cache: `pre_skip_dedup` consults `try_fast_resolve`
        // and a miss declines outright, so a cold first query never reaches the
        // skip. Three earlier attempts at a skip test were green against broken
        // code for exactly this reason.
        db.query_delta_only(&sql).await?;
        let before = counter_value(scan_metric_names::DEDUP_SKIPPED);
        let batches = db.query_delta_only(&sql).await?;
        let n = batches.iter().map(|b| b.num_rows()).sum::<usize>() as i64;
        anyhow::Ok((n, counter_value(scan_metric_names::DEDUP_SKIPPED) - before))
    };

    let (authoritative, control_skips) = run(false, "count_parity_dedup_on").await?;
    let (skipped, skips) = run(true, "count_parity_skip_on").await?;

    assert_eq!(authoritative, (DUPLICATED + UNIQUE) as i64, "the control itself must be right: every key counted exactly once");
    assert_eq!(control_skips, 0, "the control must run with the skip genuinely off");
    assert!(skips > 0, "the skip never engaged, so this proves nothing about it — check the window and the fast-resolve warm-up");
    assert_eq!(skipped, authoritative, "the dedup skip must not change COUNT — a mismatch here is silent over-counting on every dashboard");
    Ok(())
}

/// `count(*)` itself must agree with the rows a scan returns, over a window
/// spanning several days.
///
/// The parity test above deliberately counts SCAN ROWS rather than `count(*)`,
/// because a bare count is answered by `count_pushdown` from Delta statistics
/// without building a scan — so nothing in this file ever asserted that the
/// pushdown's answer is right. Prod 2026-08-20 (project 94c5dc1f, measured
/// against `count(distinct id)` as the authority):
///
/// | span   | `count(*)` | truth  |
/// | 1 hour |        360 |    360 |
/// | 1 day  |      8,919 |  8,919 |
/// | 3 days |    112,595 | 27,909 |
/// | 10 days|    173,287 | 88,601 |
///
/// One day is right and three days is 4x wrong, which is why a single-day
/// fixture cannot catch it: the duplicates have to be spread across partitions
/// so the window covers more than one `date=`. Every dashboard tile that counts
/// over a week reads the inflated number.
#[serial]
#[tokio::test]
async fn count_star_matches_the_scan_over_a_multi_day_window() -> Result<()> {
    const DAYS: i64 = 4;
    const PER_DAY: usize = 60;
    let cfg = TestConfigBuilder::new("count_star_multi_day").with_buffer_mode(BufferMode::Enabled).build();
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // Noon on each of the last DAYS days: distinct `date=` partitions, and far
    // enough from midnight that a UTC rollover mid-test cannot move a row.
    let noon = |back: i64| (chrono::Utc::now().date_naive() - chrono::Duration::days(back)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
    for back in 1..=DAYS {
        let base = noon(back);
        let batch = || -> Result<_> { json_to_batch((0..PER_DAY).map(|i| test_span_ts(&format!("d{back}k{i}"), "n", &project_id, base + i as i64)).collect()) };
        // Written twice through separate commits, so the copies land in
        // different Delta files: flush-time dedup is per-bucket and cannot see
        // across files, which is how duplicates survive in production.
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch()?], true, None).await?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch()?], true, None).await?;
    }

    // BOTH branches of `try_count_pushdown`. The logical-count index only
    // answers spans of at most 3 days; a wider span declines and falls back to
    // a real scan. Prod disagreed at 3 days AND at 10, so covering one branch
    // would leave the other free to regress.
    for (label, back) in [("within the logical-count span", 3), ("wider than the logical-count span", DAYS)] {
        let (lo, hi) = (noon(back) - 1, noon(1) + PER_DAY as i64 + 1);
        let window = format!("project_id = '{project_id}' AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi})");

        // The authority is the scan: the rows `SELECT id` actually returns,
        // after DedupExec has collapsed the versions. `count(*)` must equal it.
        let scanned =
            db.query_delta_only(&format!("SELECT id FROM otel_logs_and_spans WHERE {window}")).await?.iter().map(|b| b.num_rows()).sum::<usize>() as i64;
        let counted = db
            .query_delta_only(&format!("SELECT count(*) FROM otel_logs_and_spans WHERE {window}"))
            .await?
            .first()
            .map(|b| b.column(0).as_primitive::<Int64Type>().value(0))
            .unwrap_or_default();

        assert_eq!(scanned, back * PER_DAY as i64, "the control itself must be right {label}: every key counted exactly once");
        assert_eq!(counted, scanned, "count(*) disagreed with the scan {label} — silent over-counting on every dashboard tile");
    }
    Ok(())
}

/// Per-DATE dedup skip (`timefusion_read_dedup_skip_per_date`): a window whose
/// dates are only PARTLY certified must skip `DedupExec` over the certified
/// partitions while the uncertified ones still dedup — and must return exactly
/// what the all-or-nothing path returns.
///
/// This is the shape prod actually has: 2026-08-22 measured 97 live
/// certifications with a longest consecutive run of 5 days, so a 7d window is
/// never wholly certified and the old rule skipped nothing at all
/// (`dedup_skipped_pct = 0.0`).
///
/// Over-counting is the failure mode that matters — this table is
/// `version_append`, so a certified leg unioned above `DedupExec` must not
/// resurrect superseded versions. The assertion is therefore equality between
/// the flag on and off, plus the winning value, over a two-date window where
/// BOTH dates carry merge-on-read duplicates.
#[serial]
#[tokio::test]
async fn per_date_dedup_skip_matches_the_all_or_nothing_result() -> Result<()> {
    // Two dates, each with two versions of its own key.
    let days_ago = |n: i64| (chrono::Utc::now().date_naive() - chrono::Duration::days(n)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
    let (recent_ts, older_ts) = (days_ago(0), days_ago(3));

    let run = |per_date: bool| async move {
        let mut cfg = (*TestConfigBuilder::new(&format!("per_date_skip_{per_date}")).with_buffer_mode(BufferMode::Enabled).build()).clone();
        cfg.maintenance.timefusion_read_dedup_skip_swept = true;
        cfg.maintenance.timefusion_read_dedup_skip_per_date = per_date;
        let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
        let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let row = |key: &str, ts: i64, tag: &str| -> Result<_> {
            let mut value = test_span_ts(key, "span", &project_id, ts);
            value["hashes"] = serde_json::json!([tag]);
            json_to_batch(vec![value])
        };
        for (key, ts) in [("recent_key", recent_ts), ("older_key", older_ts)] {
            for tag in ["original", "updated"] {
                db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row(key, ts, tag)?], true, None).await?;
            }
        }
        // Force PARTIAL certification, which is the case under test. The sweep
        // certifies every partition it proves clean; writing to the older date
        // AFTERWARDS moves that partition's fingerprint, so it is uncertified
        // while the recent one stays certified. Without this the sweep
        // certifies both dates, the plain all-or-nothing skip fires, and the
        // split under test is never reached.
        let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
        db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("older_key", older_ts, "final")?], true, None).await?;

        let sql = format!(
            "SELECT array_element(hashes, 1) AS tag FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
             AND timestamp >= to_timestamp_micros({lo}) AND timestamp <= to_timestamp_micros({hi}) ORDER BY tag",
            lo = older_ts - 1,
            hi = recent_ts + 1,
        );
        let batches = db.query_delta_only(&sql).await?;
        let mut rows: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let col = datafusion::arrow::compute::kernels::cast::cast(b.column(0), &datafusion::arrow::datatypes::DataType::Utf8).expect("cast");
                let col = col.as_string::<i32>();
                (0..b.num_rows()).map(|i| col.value(i).to_string()).collect::<Vec<_>>()
            })
            .collect();
        rows.sort();
        Ok::<Vec<String>, anyhow::Error>(rows)
    };

    let baseline = run(false).await?;
    let split = run(true).await?;

    assert_eq!(split, baseline, "per-date skip changed the result set — over/under-count regression");
    assert_eq!(split.len(), 2, "one winning row per key across the two dates, got {split:?}");
    // recent date (certified, skipped) keeps its swept winner; older date
    // (fingerprint moved, still deduped) must collapse to its LATEST version.
    assert_eq!(split, vec!["final".to_string(), "updated".to_string()], "each date must yield its winning version, got {split:?}");
    Ok(())
}

/// A selective point lookup whose needle is in no file, with the per-date split
/// enabled: the bloom prefilter removes every file and the scan must return
/// nothing without erroring.
///
/// HONEST SCOPE: this does NOT reproduce the `index out of bounds: the len is 0
/// but the index is 0` panic that `wrap_result_split` took in prod on
/// 2026-08-22 — it passes with and without the guard, because it does not
/// manage to empty exactly one side of the split. It is kept as a behavioural
/// test of the pruned-to-nothing shape, not as a regression guard for that
/// panic. The panic itself is now unreachable by construction (`plans[0]` was
/// removed in favour of `plans.first()` plus an explicit error), which is the
/// guarantee a test could not give here.
#[serial]
#[tokio::test]
async fn a_pruned_to_nothing_delta_scan_does_not_panic() -> Result<()> {
    let mut cfg = (*TestConfigBuilder::new("pruned_to_nothing").with_buffer_mode(BufferMode::FlushImmediately).build()).clone();
    cfg.maintenance.timefusion_file_bloom_pruning = true;
    cfg.maintenance.timefusion_read_dedup_skip_swept = true;
    cfg.maintenance.timefusion_read_dedup_skip_per_date = true;
    let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let ts = chrono::Utc::now().timestamp_micros();
    let rows: Vec<_> = (0..8).map(|i| test_span_ts(&format!("k{i}"), "n", &project_id, ts + i)).collect();
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![json_to_batch(rows)?], true, None).await?;
    // Blooms are what prune whole files; without the sidecars the scan keeps
    // every file and the leg is never emptied.
    db.bloom_sidecar_reconcile().await?;

    let sql = format!(
        "SELECT name FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND id = 'no-such-id-anywhere' \
         AND timestamp >= to_timestamp_micros({}) AND timestamp <= to_timestamp_micros({})",
        ts - 1_000,
        ts + 1_000,
    );
    let found: usize = db.query_delta_only(&sql).await?.iter().map(|b| b.num_rows()).sum();
    assert_eq!(found, 0, "the needle is in no file — and getting there must not panic");
    Ok(())
}
