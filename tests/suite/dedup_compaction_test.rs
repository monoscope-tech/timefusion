//! Dedup/compaction tests: cross-file duplicates within a `(project_id, date)`
//! partition, the merge-on-read read path, and schema-skew regressions.

use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::{
    array::{Array, AsArray, RecordBatch},
    datatypes::{Float64Type, Int64Type, TimestampMicrosecondType},
};
use serial_test::serial;
use test_case::test_case;
use timefusion::{
    database::{Database, scan_metric_names},
    observability::{counter_value, init_local_metrics_for_test},
    support::test_helpers::{BufferMode, TestConfigBuilder, array_get_str, delta_physical_row_count, json_to_batch, test_span_ts},
};

/// A unique project id, so tests never share a `(project_id, date)` partition.
fn new_project_id() -> String {
    format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8])
}

/// `hours` back from now. Fixtures use 3h: dedup only rewrites hour chunks
/// sealed for 2h+.
fn hours_ago(hours: i64) -> i64 {
    (chrono::Utc::now() - chrono::Duration::hours(hours)).timestamp_micros()
}

fn date_of(ts: i64) -> chrono::NaiveDate {
    chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive()
}

/// Every row of `batches` projected through `row`, empty batches skipped.
fn rows_of<T>(batches: &[RecordBatch], row: impl Fn(&RecordBatch, usize) -> T) -> Vec<T> {
    batches.iter().filter(|b| b.num_rows() > 0).flat_map(|b| (0..b.num_rows()).map(|i| row(b, i)).collect::<Vec<_>>()).collect()
}

fn i64_at(b: &RecordBatch, col: usize, row: usize) -> i64 {
    b.column(col).as_primitive::<Int64Type>().value(row)
}

fn f64_at(b: &RecordBatch, col: usize, row: usize) -> f64 {
    b.column(col).as_primitive::<Float64Type>().value(row)
}

fn ts_at(b: &RecordBatch, col: usize, row: usize) -> i64 {
    b.column(col).as_primitive::<TimestampMicrosecondType>().value(row)
}

/// otel string columns arrive as `Utf8View` on the routed path and `Utf8`
/// elsewhere; `array_get_str` handles both.
fn str_at(b: &RecordBatch, col: usize, row: usize) -> String {
    array_get_str(b.column(col).as_ref(), row)
}

/// `cfg` with `f` applied.
fn tweak(cfg: Arc<timefusion::config::AppConfig>, f: impl FnOnce(&mut timefusion::config::AppConfig)) -> Arc<timefusion::config::AppConfig> {
    let mut cfg = (*cfg).clone();
    f(&mut cfg);
    Arc::new(cfg)
}

/// The default fixture config (buffered mode, no rollups) with `f` applied.
fn tuned_cfg(name: &str, f: impl FnOnce(&mut timefusion::config::AppConfig)) -> Arc<timefusion::config::AppConfig> {
    tweak(TestConfigBuilder::new(name).with_buffer_mode(BufferMode::Enabled).build(), f)
}

/// `plain_db` over a config `f` tweaked.
async fn tuned_db(name: &str, f: impl FnOnce(&mut timefusion::config::AppConfig)) -> Result<(Arc<Database>, String)> {
    db_of(tuned_cfg(name, f)).await
}

/// A `Database` over `cfg`, plus a fresh project id.
async fn db_of(cfg: Arc<timefusion::config::AppConfig>) -> Result<(Arc<Database>, String)> {
    Ok((Arc::new(Database::with_config(cfg).await?), new_project_id()))
}

/// The default fixture: buffered mode, no rollups, no config tweaks.
async fn plain_db(name: &str) -> Result<(Arc<Database>, String)> {
    db_of(TestConfigBuilder::new(name).with_buffer_mode(BufferMode::Enabled).build()).await
}

/// The live Delta table TF resolved for `table`.
async fn table_of(db: &Arc<Database>, table: &str) -> Arc<tokio::sync::RwLock<deltalake::DeltaTable>> {
    db.unified_tables().read().await.get(table).expect("table created").clone()
}

/// One span committed straight to Delta (`skip_queue = true`) as its own Delta
/// commit, which is what manufactures separate files in one partition.
async fn commit_span(db: &Arc<Database>, project_id: &str, id: &str, name: &str, ts: i64) -> Result<()> {
    db.insert_records_batch(project_id, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts(id, name, project_id, ts)])?], true, None).await?;
    Ok(())
}

/// Two independent Delta commits of the SAME `(id, timestamp)` → a physical
/// duplicate in one partition (the cross-flush scenario).
async fn commit_dup_pair(db: &Arc<Database>, project_id: &str, ts: i64) -> Result<()> {
    commit_span(db, project_id, "dup_id", "first", ts).await?;
    commit_span(db, project_id, "dup_id", "second", ts).await
}

/// Scalar `i64` through `query_delta_only` (the path a rollup build reads through).
async fn delta_scalar(db: &Arc<Database>, sql: &str) -> Result<i64> {
    Ok(db
        .query_delta_only(sql)
        .await?
        .iter()
        .filter(|b| b.num_rows() > 0)
        .filter_map(|b| b.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().map(|c| c.value(0)))
        .next()
        .unwrap_or(0))
}

/// A session context wired exactly as the routed (pgwire) read path is.
fn ctx_for(db: &Arc<Database>) -> Result<datafusion::prelude::SessionContext> {
    let mut ctx = Arc::clone(db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    Ok(ctx)
}

/// Scalar `i64` from an EXISTING session context (keeps the warmed caches).
async fn scalar_in(ctx: &datafusion::prelude::SessionContext, sql: &str) -> Result<i64> {
    Ok(i64_at(&ctx.sql(sql).await?.collect().await?[0], 0, 0))
}

/// Scalar `i64` through the ROUTED scan (MemBuffer ∪ Delta, read-side dedup).
async fn routed_scalar(db: &Arc<Database>, sql: &str) -> Result<i64> {
    scalar_in(&ctx_for(db)?, sql).await
}

#[serial]
#[tokio::test]
async fn dedup_compaction_collapses_cross_flush_duplicates() -> Result<()> {
    let (db, project_id) = plain_db("dedup_compaction").await?;

    // Fixed timestamp so both inserts share (id, timestamp) and date.
    let ts = hours_ago(3);
    commit_dup_pair(&db, &project_id, ts).await?;

    // Measured via Delta log stats, NOT a routed query: read-side DedupExec
    // would otherwise mask the on-disk duplicate.
    let table_ref = table_of(&db, "otel_logs_and_spans").await;
    assert_eq!(delta_physical_row_count(&table_ref).await?, 2, "pre-dedup: cross-flush duplicate should exist as 2 physical rows in Delta");

    let part_marker = format!("project_id={}/date={}", project_id, date_of(ts));
    let file_count_before = table_ref.read().await.get_file_uris()?.filter(|u| u.contains(&part_marker)).count();
    assert!(file_count_before >= 2, "expected >=2 files in partition before dedup, got {}", file_count_before);

    let (dropped, complete) = db.dedup_partition(&table_ref, "otel_logs_and_spans", &project_id, date_of(ts)).await?;
    assert_eq!((dropped, complete), (1, true), "expected exactly one duplicate row dropped in a complete pass");

    assert_eq!(delta_physical_row_count(&table_ref).await?, 1, "post-dedup: duplicate should be physically collapsed to a single row");

    Ok(())
}

/// A cross-flush physical duplicate the sweep has NOT yet collapsed must still
/// read as a single row through the routed scan, including when the dedup keys
/// are projected away (projection augmentation).
#[serial]
#[tokio::test]
async fn dup_across_flush_is_deduped_on_read() -> Result<()> {
    let (db, project_id) = plain_db("read_side_dedup").await?;
    commit_dup_pair(&db, &project_id, hours_ago(3)).await?;

    let count_sql = format!("SELECT COUNT(*) AS cnt FROM otel_logs_and_spans WHERE project_id = '{}' AND id = 'dup_id'", project_id);
    assert_eq!(
        routed_scalar(&db, &count_sql).await?,
        1,
        "read-side dedup must collapse the cross-flush duplicate to a single row (COUNT(*) projects keys away)"
    );

    let name_sql = format!("SELECT name FROM otel_logs_and_spans WHERE project_id = '{}' AND id = 'dup_id'", project_id);
    let rows: usize = ctx_for(&db)?.sql(&name_sql).await?.collect().await?.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "read-side dedup must still collapse when dedup keys are projected away (`SELECT name`)");

    Ok(())
}

/// `query_delta_only` — what a rollup build reads through — must deduplicate
/// exactly like the routed scan. That is what makes building over an
/// uncertified partition correct, and the certification gate redundant.
#[serial]
#[tokio::test]
async fn query_delta_only_deduplicates_so_a_rollup_build_needs_no_certification() -> Result<()> {
    let (db, project_id) = plain_db("rollup_read_dedup_premise").await?;
    commit_dup_pair(&db, &project_id, hours_ago(3)).await?;

    let via_delta_only =
        delta_scalar(&db, &format!("SELECT COUNT(*)::BIGINT FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND id = 'dup_id'")).await?;

    let routed_sql = format!("SELECT COUNT(*) AS cnt FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND id = 'dup_id'");
    let routed = routed_scalar(&db, &routed_sql).await?;
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

/// END-TO-END: a sealed partition that still holds PHYSICAL DUPLICATES and was
/// never certified must still roll up to the deduplicated answer.
#[serial]
#[tokio::test]
async fn a_rollup_built_over_an_uncertified_duplicated_partition_matches_the_deduped_answer() -> Result<()> {
    // Horizon pinned to 4 to keep the fixture small; the shipped default is 35.
    let base = TestConfigBuilder::new("rollup_no_cert").with_buffer_mode(BufferMode::Enabled).with_rollups().build();
    let (db, project_id) = db_of(tweak(base, |c| c.maintenance.timefusion_rollup_backfill_days = 4)).await?;
    // YESTERDAY deliberately: the boundary of what the backfill claims.
    let day = chrono::Utc::now().date_naive() - chrono::Duration::days(1);
    let ts = day.and_hms_opt(9, 0, 0).unwrap().and_utc().timestamp_micros();

    // Three distinct spans, one written TWICE in separate commits → a physical
    // duplicate in a partition nothing ever certifies.
    for (id, name) in [("a", "first"), ("b", "first"), ("dup", "first"), ("dup", "second")] {
        commit_span(&db, &project_id, id, name, ts).await?;
    }

    let deduped_raw = delta_scalar(&db, &format!("SELECT COUNT(*)::BIGINT FROM otel_logs_and_spans WHERE project_id = '{project_id}'")).await?;
    assert_eq!(deduped_raw, 3, "precondition: 4 physical rows, 3 distinct ids");

    assert!(plan_and_drain_backfill(&db).await? > 0, "the backfill must build an UNCERTIFIED partition — that is the whole point of dropping the gate");

    let rolled = delta_scalar(
        &db,
        &format!("SELECT COALESCE(SUM(request_count), 0)::BIGINT FROM otel_logs_and_spans_rollup_dashboard_1m_v3 WHERE project_id = '{project_id}'"),
    )
    .await?;
    assert_eq!(rolled, deduped_raw, "the rollup must count the DEDUPLICATED rows; counting the physical 4 is the silent-wrong-number failure");
    Ok(())
}

/// The rollup backfill must only claim days nothing has touched. Its enqueue
/// path (`invalidate`) takes `deadline.max(new_deadline)`, so re-invalidating a
/// day that already has an eligible task pushes that task's deadline out every
/// pass and starves the live frontier. Also pins the horizon off-switch.
#[serial]
#[tokio::test]
async fn rollup_backfill_leaves_already_queued_days_alone() -> Result<()> {
    let base = TestConfigBuilder::new("rollup_coordinator_backfill").with_buffer_mode(BufferMode::Enabled).with_rollups().build();
    assert!(base.maintenance.timefusion_rollup_backfill_days >= 30, "the shipped default must cover a 30d query; 0 disables the backfill entirely");

    let (db, project_id) = db_of(Arc::clone(&base)).await?;

    let day = chrono::Utc::now().date_naive() - chrono::Duration::days(3);
    let ts = day.and_hms_opt(9, 0, 0).unwrap().and_utc().timestamp_micros();
    for id in ["a", "b", "c"] {
        commit_span(&db, &project_id, id, "op", ts).await?;
    }
    if let Some(layer) = db.buffered_layer() {
        layer.flush_all_now().await?;
    }

    assert_eq!(
        db.plan_rollup_backfill().await?,
        0,
        "a day that already has queued rollup work must not be re-invalidated; doing so pushes its deadline out every pass and starves the live frontier"
    );

    let mut off = (*base).clone();
    off.maintenance.timefusion_rollup_backfill_days = 0;
    let db_off = Arc::new(Database::with_config(Arc::new(off)).await?);
    assert_eq!(db_off.plan_rollup_backfill().await?, 0, "horizon 0 must disable the backfill entirely");
    Ok(())
}

/// TODAY may be rolled up only up to the oldest still-buffered row, and the
/// answer must still equal the raw one. Claiming buckets ABOVE that bound serves
/// them from a rollup that never aggregated them. Asserts both that today really
/// is covered (else the test passes vacuously via raw) and that totals match.
#[serial]
#[tokio::test]
async fn today_is_rolled_up_to_the_buffer_boundary_and_still_matches_the_raw_answer() -> Result<()> {
    let base = TestConfigBuilder::new("rollup_today").with_buffer_mode(BufferMode::Enabled).with_rollups().build();
    let cfg = tweak(base, |c| c.maintenance.timefusion_rollup_backfill_days = 2);
    // A REAL buffered layer: without one `min_buffered_micros` is always None
    // and the bound degenerates to the day end, skipping the partial-day case.
    let layer = Arc::new(timefusion::support::test_helpers::test_layer(Arc::clone(&cfg))?);
    let db = Arc::new(Database::with_config(cfg).await?.with_buffered_layer(Arc::clone(&layer)));
    let project_id = new_project_id();

    let today = chrono::Utc::now().date_naive();
    let midnight = today.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
    let row = |id: &str, duration: i64, ts: i64| -> Result<_> {
        json_to_batch(vec![serde_json::json!({
            "timestamp": ts, "id": id, "name": "op", "project_id": project_id, "hashes": [], "summary": ["today rollup fixture"],
            "date": today.to_string(), "duration": duration, "kind": "server", "status_code": "OK",
            "resource___service___name": "cart",
        })])
    };

    // SETTLED: flushed to Delta so a build can aggregate them.
    for (i, offset) in [60_000_000i64, 120_000_000, 3_600_000_000].iter().enumerate() {
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row(&format!("s{i}"), 100 + i as i64, midnight + offset)?], true, None).await?;
    }
    layer.flush_all_now().await?;

    // STILL BUFFERED: at or above the bound, so the build must not claim them.
    for (i, offset) in [7_200_000_000i64, 7_260_000_000].iter().enumerate() {
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row(&format!("b{i}"), 900 + i as i64, midnight + offset)?], true, None).await?;
    }

    assert!(plan_and_drain_backfill(&db).await? > 0, "the backfill must claim TODAY — otherwise this test proves nothing about partial-day coverage");

    let rollup_rows =
        delta_scalar(&db, &format!("SELECT COUNT(*)::BIGINT FROM otel_logs_and_spans_rollup_dashboard_1m_v3 WHERE project_id = '{project_id}'")).await?;
    assert!(rollup_rows > 0, "today must actually have rollup buckets, not just an empty partition");

    // The whole of today, through the routed path (rollup interior + raw tail).
    let day_end = midnight + 86_400_000_000i64;
    let agg = format!(
        "SELECT COUNT(*) AS n, SUM(duration) AS total FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
         AND timestamp >= to_timestamp_micros({midnight}) AND timestamp < to_timestamp_micros({day_end})"
    );
    let routed = ctx_for(&db)?.sql(&agg).await?.collect().await?;
    let (n, total) = (i64_at(&routed[0], 0, 0), i64_at(&routed[0], 1, 0));

    // 3 settled + 2 buffered.
    assert_eq!(n, 5, "the union must return every row — a bound that over-claims drops the buffered tail");
    assert_eq!(total, 100 + 101 + 102 + 900 + 901, "durations must match exactly; a wrong interior shows up here as a wrong SUM");
    Ok(())
}

/// A pushed `LIMIT N` must not be forwarded into the Delta scan: that truncates
/// *before* DedupExec drops duplicates, so the deduped union can yield < N rows.
#[serial]
#[tokio::test]
async fn limit_query_not_truncated_below_read_dedup() -> Result<()> {
    let (db, project_id) = plain_db("read_dedup_limit").await?;

    let ts = hours_ago(3);
    // 3 physical copies of "a" plus one "b": 4 physical rows, 2 distinct.
    for _ in 0..3 {
        commit_span(&db, &project_id, "a", "a", ts).await?;
    }
    commit_span(&db, &project_id, "b", "b", ts).await?;

    let sql = format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{}' LIMIT 2", project_id);
    let rows: usize = ctx_for(&db)?.sql(&sql).await?.collect().await?.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "LIMIT 2 must return both distinct ids, not a duplicate-truncated single row");

    Ok(())
}

/// The dedup sweep (`dedup_today_partitions`) must cover a recent-day lookback
/// window, not just today — late data can land in a prior-day partition.
#[serial]
#[tokio::test]
async fn dedup_sweep_collapses_prior_day_partition() -> Result<()> {
    let (db, project_id) = plain_db("dedup_sweep_lookback").await?;

    // Yesterday noon UTC: always a prior-day partition and always >2h sealed.
    let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
    commit_dup_pair(&db, &project_id, ts).await?;

    // Physical row count (Delta log stats), so read-side DedupExec can't mask it.
    let table_ref = table_of(&db, "otel_logs_and_spans").await;
    assert_eq!(delta_physical_row_count(&table_ref).await?, 2, "pre-sweep: prior-day cross-flush duplicate should exist as 2 physical rows");

    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

    assert_eq!(delta_physical_row_count(&table_ref).await?, 1, "post-sweep: prior-day duplicate must be physically collapsed to a single row");
    Ok(())
}

/// Dedup must commit under concurrent append fire. The in-process
/// `delta_commit_lock` serializes commits so the OCC rebase never has to
/// evaluate dedup's timestamp predicate, which delta-kernel cannot.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dedup_commits_despite_concurrent_appends() -> Result<()> {
    use std::sync::atomic::Ordering::{Acquire, Release};
    let (db, project_id) = plain_db("dedup_occ_race").await?;

    let ts = hours_ago(3);
    commit_dup_pair(&db, &project_id, ts).await?;

    // Append fire: fresh-timestamp rows committing continuously while dedup
    // rewrites the sealed chunk.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let committed = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let appender = {
        let (db, project_id, stop, committed) = (Arc::clone(&db), project_id.clone(), Arc::clone(&stop), Arc::clone(&committed));
        tokio::spawn(async move {
            let mut i = 0u64;
            while !stop.load(Acquire) {
                let now = chrono::Utc::now().timestamp_micros();
                commit_span(&db, &project_id, &format!("live_{i}"), "live", now).await.unwrap();
                i += 1;
                committed.store(i, Release);
            }
            i
        })
    };

    // Gate dedup on the appender's first commit so the race is guaranteed rather
    // than a scheduling artifact.
    while committed.load(Acquire) == 0 {
        tokio::task::yield_now().await;
    }

    let table_ref = table_of(&db, "otel_logs_and_spans").await;
    let (dropped, _complete) = db.dedup_partition(&table_ref, "otel_logs_and_spans", &project_id, date_of(ts)).await?;
    stop.store(true, Release);
    let appended = appender.await?;
    assert!(appended > 0, "appender must have raced at least one commit");
    assert_eq!(dropped, 1, "dedup must collapse the duplicate despite concurrent appends");

    let count_sql = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}' AND id = 'dup_id'", project_id);
    assert_eq!(delta_scalar(&db, &count_sql).await?, 1, "post-dedup: dup_id row should be collapsed to 1");
    Ok(())
}

/// Light OPTIMIZE (bin-pack compact) must preserve ALL partition values on
/// rewritten files. The kernel narrows `partitionValues_parsed` to the
/// predicate-referenced subset, and using that narrowed map for output silently
/// NULLs `project_id` and hides every compacted row from project-scoped queries.
#[serial]
#[tokio::test]
async fn optimize_preserves_all_partition_values() -> Result<()> {
    let (db, project_id) = plain_db("optimize_partition_preserve").await?;

    let ts = chrono::Utc::now().timestamp_micros();
    // 6 separate commits → 6 small files (>= timefusion_compact_min_files=5, so
    // the optimize commit isn't skipped).
    for i in 0..6 {
        commit_span(&db, &project_id, &format!("opt_id_{i}"), "row", ts + i).await?;
    }

    let count_sql = format!("SELECT COUNT(*) AS cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project_id);
    assert_eq!(delta_scalar(&db, &count_sql).await?, 6, "pre-optimize row count");

    let table_ref = table_of(&db, "otel_logs_and_spans").await;
    db.optimize_table_light(&table_ref, "otel_logs_and_spans", timefusion::database::TailPass::Pack).await?;

    // Compacted files must keep the full (project_id, date) partition path…
    let date_str = date_of(ts).to_string();
    let bad: Vec<String> = table_ref.read().await.get_file_uris()?.filter(|u| u.contains(&format!("/date={date_str}")) && !u.contains("project_id=")).collect();
    assert!(bad.is_empty(), "optimize dropped project_id partition from: {bad:?}");

    // …and project-scoped queries must still see every row.
    assert_eq!(delta_scalar(&db, &count_sql).await?, 6, "post-optimize: project-scoped count must be unchanged");
    Ok(())
}

/// The dedup rewrite is a TARGETED file transaction: a bystander file in the
/// same partition but outside the duplicate's 10-minute window must survive at
/// the same path, while the duplicate-bearing files are replaced.
#[serial]
#[tokio::test]
async fn dedup_rewrite_targets_only_duplicate_files() -> Result<()> {
    // Copy-on-write mode: file REPLACEMENT is a copy-on-write-only property (a
    // DV masks in place, same path). DV equivalent: e2e
    // `dv_dedup_drops_cross_file_duplicate`.
    let (db, project_id) = db_of(TestConfigBuilder::new("dedup_targeted").with_buffer_mode(BufferMode::Enabled).without_deletion_vectors().build()).await?;

    // Duplicate pair 3h back (sealed); bystander 20 min earlier, a different
    // 10-minute chunk.
    let ts = hours_ago(3);
    let ts_bystander = ts - chrono::Duration::minutes(20).num_microseconds().unwrap();

    commit_dup_pair(&db, &project_id, ts).await?;
    let table_ref = table_of(&db, "otel_logs_and_spans").await;
    let files_before_bystander: std::collections::HashSet<String> = table_ref.read().await.get_file_uris()?.collect();

    commit_span(&db, &project_id, "bystander", "witness", ts_bystander).await?;
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

const SHARDABLE: &[(&str, i64)] = &[("a", 0), ("a", 0), ("b", 1), ("c", 2), ("d", 3)];
const ONE_HOT_KEY: &[(&str, i64)] = &[("hot", 0), ("hot", 0), ("hot", 0)];

/// A chunk whose estimated decoded footprint exceeds the budget must SHARD by a
/// hash of the dedup keys rather than be skipped, collapsing the duplicate while
/// every distinct row survives exactly once.
///
/// SKEW SAFETY VALVE (second case): sharding cannot split a single key group, so
/// when one group alone exceeds the budget the chunk is SKIPPED (0, false)
/// rather than materialized into an OOM.
///
/// `bytes_per_row`/`inflation` are pinned to make the estimate deterministic.
/// Inserts are `(id, timestamp offset)`, each its own Delta commit, 3h back
/// (sealed); a repeated `(id, ts)` hashes to the same shard.
#[test_case("dedup_shard_preserve", 5_000_000, SHARDABLE, 5, (1, true), 4, &["a", "b", "c", "d"] ; "shards over budget and preserves rows")]
#[test_case("dedup_hot_key", 4_000_000, ONE_HOT_KEY, 3, (0, false), 3, &["hot"] ; "skips single hot key over budget")]
#[serial]
#[tokio::test]
async fn dedup_over_budget(
    name: &str, max_decoded_bytes: u64, inserts: &[(&str, i64)], physical_before: i64, outcome: (u64, bool), physical_after: i64, ids_after: &[&str],
) -> Result<()> {
    let (db, project_id) = tuned_db(name, |c| {
        c.maintenance.timefusion_dedup_bytes_per_row = 1_000_000;
        c.maintenance.timefusion_dedup_decode_inflation = 1;
        c.maintenance.timefusion_dedup_max_decoded_bytes = max_decoded_bytes;
    })
    .await?;

    let base = hours_ago(3);
    for (id, offset) in inserts {
        commit_span(&db, &project_id, id, id, base + offset).await?;
    }

    let table_ref = table_of(&db, "otel_logs_and_spans").await;
    assert_eq!(delta_physical_row_count(&table_ref).await?, physical_before, "pre-dedup physical rows");

    let (dropped, complete) = db.dedup_partition(&table_ref, "otel_logs_and_spans", &project_id, date_of(base)).await?;
    assert_eq!((dropped, complete), outcome, "over-budget dedup outcome (dropped rows, chunk completed)");
    assert_eq!(delta_physical_row_count(&table_ref).await?, physical_after, "physical rows surviving the pass");

    let got = column_strings(&db, &format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{project_id}' ORDER BY id")).await?;
    assert_eq!(got, ids_after, "every distinct id preserved exactly once");
    Ok(())
}

/// `UPDATE ... FROM` against duplicate keys, on the TARGET and the SOURCE side.
/// Both cases end with one logical row holding the winning value.
///
/// `duplicated target`: a duplicated target row does not break the update. The
/// reported count is 1 here because `otel_logs_and_spans` is merge-on-read — the
/// two physical rows are ONE logical row, and the single appended version
/// supersedes both. (An in-place table would report 2.)
///
/// `duplicate source keys`: two source rows sharing a join key would be a
/// delta-rs cardinality abort. TF splits such a source into successive
/// single-key rounds applied last-write-wins, so count = rounds and the target
/// holds the LAST source row's value.
#[test_case("update_dup_target", &["first", "second"], "('dup_id', 'enriched')", 1, "enriched" ; "duplicated target updates all copies")]
#[test_case("update_dup_source", &["orig"], "('dup_id', 'a'), ('dup_id', 'b')", 2, "b" ; "duplicate source keys apply last write wins")]
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn update_from_resolves_to_one_logical_row(name: &str, target_names: &[&str], source_values: &str, expect_updated: u64, expect_name: &str) -> Result<()> {
    let (db, project_id) = plain_db(name).await?;

    // All at the SAME (id, timestamp) → that many physical rows for 'dup_id'.
    let ts = hours_ago(3);
    for n in target_names {
        commit_span(&db, &project_id, "dup_id", n, ts).await?;
    }
    let table_ref = table_of(&db, "otel_logs_and_spans").await;
    assert_eq!(delta_physical_row_count(&table_ref).await?, target_names.len() as i64, "precondition: the target rows exist physically in Delta");

    let sql = format!(
        "UPDATE otel_logs_and_spans SET hashes = make_array(u.name) \
         FROM (VALUES {source_values}) AS u(id, name) \
         WHERE project_id = '{project_id}' AND otel_logs_and_spans.id = u.id"
    );
    let updated = ctx_for(&db)?.sql(&sql).await?.collect().await?[0].column(0).as_primitive::<datafusion::arrow::datatypes::UInt64Type>().value(0);
    assert_eq!(updated, expect_updated, "rows reported updated: one appended version per source round, never an abort");

    let read = format!("SELECT COALESCE(array_element(hashes, 1), name) AS name FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND id = 'dup_id'");
    assert_eq!(column_strings(&db, &read).await?, vec![expect_name], "one current version survives, holding the winning value");
    Ok(())
}

/// The cold consolidate sweep must produce event-time DISJOINT sorted runs.
/// Binning in snapshot (arrival) order instead yields runs that all overlap the
/// whole day, so a recent-window or ORDER-BY-DESC-LIMIT query opens every file.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn cold_consolidate_produces_event_time_disjoint_runs() -> Result<()> {
    use timefusion::support::test_helpers::minio_test_config;
    let id = format!("cold-consol-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let dir = format!("/tmp/timefusion-{id}");
    let project_id = new_project_id();

    // A sealed, cold date. Arrival order deliberately interleaves event times,
    // so snapshot-order binning would mix early and late hours in every bin.
    let base = (chrono::Utc::now() - chrono::Duration::days(3)).date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc();
    let date = base.date_naive();
    let hours = [0i64, 6, 1, 7, 2, 8];

    // Event-time range from the raw Add stats JSON (timestamps are RFC3339).
    fn ts_range(stats: &str) -> Option<(i64, i64)> {
        let v: serde_json::Value = serde_json::from_str(stats).ok()?;
        let get = |key: &str| v[key]["timestamp"].as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|d| d.timestamp_micros());
        Some((get("minValues")?, get("maxValues")?))
    }

    /// The partition's live files' (min,max) event-time ranges, sorted. A run
    /// without readable stats is mapped to (MIN, MAX) so it fails disjointness —
    /// unreadable stats are as fatal for pruning as an overlap.
    async fn sorted_ranges(table: &Arc<tokio::sync::RwLock<deltalake::DeltaTable>>, filters: &[deltalake::PartitionFilter]) -> Result<Vec<(i64, i64)>> {
        use futures::TryStreamExt;
        let guard = table.read().await;
        let adds: Vec<_> = guard.get_active_add_actions_by_partitions(filters).try_collect().await?;
        let mut r: Vec<_> = adds.iter().map(|a| a.stats().and_then(|s| ts_range(&s)).unwrap_or((i64::MIN, i64::MAX))).collect();
        r.sort();
        Ok(r)
    }

    // Only the flush path writes min/max timestamp stats into the Add actions;
    // flush_immediately lands each insert as its own commit.
    let cfg = tweak(minio_test_config(&id, &dir), |c| c.buffer.timefusion_flush_immediately = true);
    let mut sizes: Vec<i64> = {
        let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
        for (i, h) in hours.iter().enumerate() {
            let ts = (base + chrono::Duration::hours(*h)).timestamp_micros();
            let mut row = test_span_ts(&format!("id-{i}"), &format!("span-{i}"), &project_id, ts);
            // Incompressible ~140KB payload so file size scales with row count;
            // 1-row files would otherwise be pure footer overhead.
            let blob: String = (0..4000).map(|_| uuid::Uuid::new_v4().to_string()).collect();
            row["summary"] = serde_json::json!([blob]);
            let batch = json_to_batch(vec![row])?;
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
    sizes.sort();
    let cold_target = sizes[sizes.len() / 2] * 5 / 2;
    let db = Arc::new(Database::with_config(tweak(cfg, |c| c.parquet.timefusion_cold_optimize_target_size = cold_target)).await?);
    let table_ref = db.get_or_create_unified_table("otel_logs_and_spans").await?;
    db.consolidate_date_binned(&table_ref, "otel_logs_and_spans", date, cold_target, None, usize::MAX).await?;

    let filters = vec![
        deltalake::PartitionFilter::try_from(("project_id", "=", project_id.as_str()))?,
        deltalake::PartitionFilter::try_from(("date", "=", date.to_string().as_str()))?,
    ];
    let sorted = sorted_ranges(&table_ref, &filters).await?;
    assert!(sorted.len() < 6, "consolidation must merge files (got {} of 6)", sorted.len());
    assert!(sorted.len() >= 2, "target must split the day into multiple runs (got {})", sorted.len());
    for w in sorted.windows(2) {
        assert!(w[0].1 < w[1].0, "consolidated runs must be event-time disjoint, got overlapping ranges {:?} and {:?} (all: {:?})", w[0], w[1], sorted);
    }
    assert_eq!(delta_physical_row_count(&table_ref).await?, 6, "consolidation must not lose rows");

    // Idempotence: a second sweep must not rewrite converged runs.
    db.consolidate_date_binned(&table_ref, "otel_logs_and_spans", date, cold_target, None, usize::MAX).await?;
    assert_eq!(sorted_ranges(&table_ref, &filters).await?, sorted, "second sweep must be a no-op on converged runs");
    Ok(())
}

// Merge-on-read read path. Keep-greatest only engages while `DedupExec`'s input
// still DECLARES an ordering on the leading dedup key, so every version of a key
// arrives in one contiguous run: `scan` sorts the cheap in-memory legs up to the
// Delta leg's declared footer ordering so the union can advertise it and
// `EnforceDistribution` picks `SortPreservingMergeExec` over an ordering-erasing
// `CoalescePartitionsExec`.

/// A `Database` with a real buffered layer, so writes can land in MemBuffer.
async fn buffered_db(name: &str) -> Result<(Arc<Database>, String)> {
    let cfg = TestConfigBuilder::new(name).with_buffer_mode(BufferMode::Enabled).build();
    let layer = Arc::new(timefusion::support::test_helpers::test_layer(Arc::clone(&cfg))?);
    let db = Arc::new(Database::with_config(cfg).await?.with_buffered_layer(layer));
    Ok((db, new_project_id()))
}

/// `count` base rows already flushed to Delta (one file, so the footer pushdown
/// declares `timestamp DESC`) plus a newer version of `k0` still in MemBuffer.
async fn seed_mor_versions(db: &Arc<Database>, table: &str, project_id: &str, count: i64) -> Result<()> {
    let ts = chrono::Utc::now().timestamp_micros();
    let rows = (0..count).map(|i| mor_row(&format!("k{i}"), "v", project_id, ts - i * 1000, None)).collect();
    write_to(db, table, project_id, rows, true).await?;
    write_to(db, table, project_id, vec![mor_row("k0", "v2", project_id, ts, None)], false).await
}

/// `to_delta = true` commits straight to Delta; `false` goes through the
/// buffered layer into MemBuffer.
async fn write_to(db: &Arc<Database>, table: &str, project_id: &str, rows: Vec<serde_json::Value>, to_delta: bool) -> Result<()> {
    let batch = timefusion::support::test_helpers::json_to_batch_for(table, rows)?;
    db.insert_records_batch(project_id, table, vec![batch], to_delta, None).await?;
    Ok(())
}

/// A `mor_versioned` row — the fixture table that ships `version_append: true`,
/// so the merge-on-read read path is live on it.
fn mor_row(id: &str, name: &str, project_id: &str, ts: i64, deleted: Option<bool>) -> serde_json::Value {
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string();
    serde_json::json!({ "timestamp": ts, "id": id, "name": name, "project_id": project_id, "date": date, "deleted": deleted })
}

/// Sweep until the window's partitions carry a clean fingerprint — the
/// precondition the read-side dedup skip and `count_pushdown` gate on. Only a
/// 0-drop pass over an UNCHANGED file set certifies, so two passes are needed.
async fn sweep_clean(db: &Arc<Database>, table: &str) -> Result<()> {
    let table_ref = table_of(db, table).await;
    for _ in 0..2 {
        db.dedup_today_partitions(&table_ref, table, table).await?;
    }
    Ok(())
}

async fn physical_plan(db: &Arc<Database>, sql: &str) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    Ok(ctx_for(db)?.sql(sql).await?.create_physical_plan().await?)
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
    col0_in(&ctx_for(db)?, sql).await
}

/// A plain merge-on-read `SELECT` must never sort the DELTA leg — that blocking
/// whole-window SortExec exhausts the query pool. In-memory legs MAY be sorted
/// (bounded, already materialized); mem-leg sort + `SortPreservingMergeExec` is
/// the intended shape, giving `DedupExec` a bounded keep-greatest.
#[serial]
#[tokio::test]
async fn plain_select_dedups_without_sorting_under_mor() -> Result<()> {
    let (db, project_id) = buffered_db("mor_plan_shape").await?;
    seed_mor_versions(&db, "mor_versioned", &project_id, 8).await?;

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

/// While `version_append` is OFF no version can exist, so the ordering
/// machinery (mem/hot `SortExec`, k-way `SortPreservingMergeExec`) must be
/// absent — it would be pure cost, and the merge holds one in-flight batch per
/// Delta partition. `DedupExec` keeps keep-first behind `CoalescePartitionsExec`.
#[serial]
#[tokio::test]
async fn dormant_version_append_table_keeps_coalesce_and_no_injected_sort() -> Result<()> {
    // `mor_dormant`, not otel: otel ships `version_append: true`.
    let (db, project_id) = buffered_db("mor_plan_shape_dormant").await?;
    seed_mor_versions(&db, "mor_dormant", &project_id, 8).await?;

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

/// The merge-on-read contract: two versions of one `(timestamp, id)` differing
/// only in their TF-stamped `updated_at` read back as the NEWER version, through
/// a plain `SELECT` and through an aggregation.
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

/// Merge-on-read `DELETE`: the tombstone must first BEAT the older live version
/// on `updated_at`, and only then remove the row. Filtering below the dedup
/// would drop the tombstone and resurrect the stale row. Covers `SELECT` and
/// `COUNT(*)` (the stats-based pushdown must decline on a tombstone table).
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

    assert_eq!(routed_scalar(&db, &format!("SELECT COUNT(*) FROM mor_versioned {where_}")).await?, 1, "COUNT(*) must not count the tombstoned row");
    Ok(())
}

/// `COUNT(*)` over a Delta-only, flushed, timestamp-bounded window — the shape
/// `count_pushdown` answers from add-action `numRecords`. Those stats count a
/// tombstone and the live version it retires as two rows, so the pushdown must
/// decline wherever tombstones can exist; the answer here is 0, not 2.
#[serial]
#[tokio::test]
async fn count_pushdown_declines_where_tombstones_are_possible() -> Result<()> {
    let (db, project_id) = buffered_db("mor_count_pushdown").await?;
    let ts = hours_ago(3);
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
    // exec; declining leaves the real scan standing. Assert on the scan, NOT on
    // `DedupExec` — a certified partition legitimately drops the dedup.
    let text = rendered(&physical_plan(&db, &sql).await?);
    assert!(text.contains("DeltaScanExec"), "count_pushdown must decline where tombstones can exist — it answered from add-action stats:\n{text}");
    assert!(text.contains("IS DISTINCT FROM true"), "the tombstone filter must be part of the counted plan:\n{text}");

    assert_eq!(routed_scalar(&db, &sql).await?, 0, "the tombstone wins its key and removes the row — stats would have said 2");
    Ok(())
}

/// NULL must read as LIVE, so the tombstone filter is a no-op on pre-existing
/// data — which is what lets a table declare the column at birth.
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
/// applied at the source it drops the tombstone before the dedup, so the older
/// live version wins and a deleted row silently resurrects. Reported
/// `Unsupported`, and stripped again inside `scan`.
#[serial]
#[tokio::test]
async fn tombstone_predicate_is_not_pushed_into_the_scan() -> Result<()> {
    use datafusion::logical_expr::{TableProviderFilterPushDown, col, lit};
    let (db, project_id) = buffered_db("mor_tombstone_pushdown").await?;
    let ts = chrono::Utc::now().timestamp_micros();
    write_to(&db, "mor_versioned", &project_id, vec![mor_row("k", "v1", &project_id, ts, None)], true).await?;

    let provider = ctx_for(&db)?.table_provider("mor_versioned").await?;
    let pred = col("deleted").eq(lit(true));
    assert!(
        matches!(provider.supports_filters_pushdown(&[&pred])?[0], TableProviderFilterPushDown::Unsupported),
        "a tombstone-column predicate must never be pushed to the scan legs"
    );

    // End to end: the dedup still runs under a user predicate on the marker.
    let text = rendered(&physical_plan(&db, &format!("SELECT name FROM mor_versioned WHERE project_id = '{project_id}' AND deleted")).await?);
    assert!(text.contains("DedupExec"), "the dedup must still run under a tombstone predicate:\n{text}");
    Ok(())
}

/// `ORDER BY timestamp DESC LIMIT n` must not regrow the blocking whole-window
/// `SortExec` that `ordered_union_for_topk` removes — every surviving sort
/// carries a fetch (a TopK).
#[serial]
#[tokio::test]
async fn topk_path_still_streams() -> Result<()> {
    let (db, project_id) = buffered_db("mor_topk").await?;
    let ts = chrono::Utc::now().timestamp_micros();
    write_to(&db, "otel_logs_and_spans", &project_id, (0..8).map(|i| test_span_ts(&format!("t{i}"), "n", &project_id, ts - i * 1000)).collect(), true).await?;
    write_to(&db, "otel_logs_and_spans", &project_id, vec![test_span_ts("t9", "n", &project_id, ts + 1000)], false).await?;

    let sql = format!("SELECT name FROM otel_logs_and_spans WHERE project_id = '{project_id}' ORDER BY timestamp DESC LIMIT 2");
    let text = rendered(&physical_plan(&db, &sql).await?);
    for line in text.lines().filter(|l| l.contains("SortExec")) {
        assert!(line.contains("fetch="), "a blocking whole-window SortExec regrew in the top-K plan: {line}\n{text}");
    }
    assert_eq!(column_strings(&db, &sql).await?.len(), 2);
    Ok(())
}

/// The URI + storage options `get_or_create_unified_table` will resolve for
/// `table` under `cfg`, so a test can reach the SAME Delta table out-of-band.
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

/// Pre-create `table`'s unified Delta at the OLD column set (the YAML minus
/// `added`), at exactly the URI `get_or_create_unified_table` will resolve, so
/// TF LOADS this table instead of creating one from the YAML. That skew is what
/// a long-lived table is in; a table built from the current YAML proves nothing.
async fn create_at_old_column_set(
    cfg: &timefusion::config::AppConfig, table: &str, added: &[&str],
) -> Result<(String, std::collections::HashMap<String, String>)> {
    let schema = timefusion::schema::get_schema(table).expect("fixture registered");
    let old_columns: Vec<_> = schema.columns()?.into_iter().filter(|c| !added.contains(&c.name().as_str())).collect();
    assert_eq!(old_columns.len(), schema.columns()?.len() - added.len(), "the fixture must still declare the columns this test removes");
    let (storage_uri, storage_options) = unified_table_location(cfg, table);
    deltalake::operations::create::CreateBuilder::new()
        .with_location(&storage_uri)
        .with_columns(old_columns)
        .with_partition_columns(schema.partitions.clone())
        .with_storage_options(storage_options.clone())
        .await?;
    Ok((storage_uri, storage_options))
}

/// `table`'s Delta loaded OUT OF BAND, so a test can read the COMMITTED schema
/// rather than TF's cached view of it.
async fn load_out_of_band(cfg: &timefusion::config::AppConfig, table: &str) -> Result<deltalake::DeltaTable> {
    let (storage_uri, storage_options) = unified_table_location(cfg, table);
    Ok(deltalake::DeltaTableBuilder::from_url(url::Url::parse(&storage_uri)?)?.with_storage_options(storage_options).load().await?)
}

/// A `SchemaMode::Merge` write straight to `table`'s Delta, bypassing TF — the
/// only way to manufacture on-disk states TF's own casting write path never
/// produces (a nullability-widened file, a row with no `updated_at` stamp).
async fn merge_write_out_of_band(cfg: &timefusion::config::AppConfig, table: &str, batches: Vec<datafusion::arrow::array::RecordBatch>) -> Result<()> {
    load_out_of_band(cfg, table).await?.write(batches).with_schema_mode(deltalake::operations::write::SchemaMode::Merge).await?;
    Ok(())
}

/// Column 0 of every row, as strings.
fn col0_strings(batches: &[RecordBatch]) -> Vec<String> {
    rows_of(batches, |b, i| str_at(b, 0, i))
}

/// `col0_strings` over a query run on an EXISTING context (the warmed one).
async fn col0_in(ctx: &datafusion::prelude::SessionContext, sql: &str) -> Result<Vec<String>> {
    Ok(col0_strings(&ctx.sql(sql).await?.collect().await?))
}

/// Adding a column to a table that ALREADY HAS live Delta data. Ordinary tests
/// create their Delta table FROM the YAML, so the two schemas always agree and
/// the skew is invisible; this manufactures it by creating the table at a
/// trimmed column set and then writing the FULL YAML column set through
/// `insert_records_batch`. Both `skip_queue` variants are exercised.
///
/// DOES NOT COVER: custom (BYO-bucket) project tables, non-nullable or
/// mid-schema insertions, type changes, the reverse skew (Delta wider than the
/// YAML), or the tantivy/dedup/optimize paths, which read the stored schema
/// separately. Green here means the plain write path tolerates the skew, not
/// that a column addition is safe to deploy.
#[serial]
#[tokio::test]
async fn adding_a_column_to_an_existing_table_is_caught() -> Result<()> {
    const TABLE: &str = "mor_versioned";
    let cfg = TestConfigBuilder::new("schema_skew").with_buffer_mode(BufferMode::Enabled).build();

    let added = ["updated_at", "deleted"];
    let (storage_uri, storage_options) = create_at_old_column_set(&cfg, TABLE, &added).await?;

    // Wire the Delta write callback as `bootstrap` does — without it a flush
    // drains the MemBuffer and never reaches Delta.
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

    // Batches are built at the FULL (wider) YAML column set while the loaded
    // Delta table declares the narrower one.
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

    // `flush_all_now` swallows per-bucket errors into `buckets_failed`, so that
    // counter — not the Result — is the assertion that matters.
    let stats = layer.flush_all_now().await.map_err(|e| {
        anyhow::anyhow!(
            "flushing a buffered write into a Delta table created at an OLDER column set failed: {e}\n\
             This is the 7d68f01 prod failure — see the note above `TableSchema`."
        )
    })?;
    assert_eq!(stats.buckets_failed, 0, "a bucket failed to flush against the older Delta schema: {stats:?}");
    assert!(stats.total_rows > 0, "the flush must have moved the buffered row to Delta, got {stats:?}");

    // Read the log FRESH: TF's cached snapshot lags a flush, so only an
    // out-of-band load asserts what durably landed.
    let table = deltalake::DeltaTableBuilder::from_url(url::Url::parse(&storage_uri)?)?.with_storage_options(storage_options).load().await?;
    let snapshot = table.snapshot()?;
    let adds = snapshot.add_actions_table(true)?;
    let nr = adds.column_by_name("num_records").expect("num_records").as_primitive::<Int64Type>();
    let physical: i64 = (0..nr.len()).filter(|&i| !nr.is_null(i)).map(|i| nr.value(i)).sum();
    assert_eq!(physical, 2, "both rows must be durable in Delta after the flush");
    let stored: Vec<String> = snapshot.schema().fields().map(|f| f.name().to_string()).collect();
    for c in added {
        assert!(stored.contains(&c.to_string()), "the write path did not evolve the stored Delta schema — `{c}` is still missing: {stored:?}");
    }
    Ok(())
}

/// Aggregating over a column whose on-disk parquet is NULLABLE while the YAML
/// (and the committed Delta schema) declare it NOT NULL. delta-rs merges
/// nullability by union, so a `SchemaMode::Merge` write permanently widens the
/// physical parquet without changing the Delta schema, and the logical/physical
/// mismatch used to reject `GROUP BY time_bucket(timestamp)` while `GROUP BY`
/// on a nullable-declared column kept working — both are asserted.
///
/// The out-of-band merge write is essential: TF's staged write path casts to the
/// table's arrow schema and does NOT widen, so `insert_records_batch` alone
/// cannot reproduce the state.
///
/// Relates to `datafusion.execution.skip_physical_aggregate_schema_check`, but
/// NOTE: this query passes on this branch with or without that flag, so a green
/// run is not grounds for deleting it.
#[serial]
#[tokio::test]
async fn aggregate_groups_on_a_nullability_widened_column() -> Result<()> {
    use datafusion::arrow::datatypes::Schema;

    const TABLE: &str = "otel_logs_and_spans";
    let cfg = TestConfigBuilder::new("nullability_widened").with_buffer_mode(BufferMode::Enabled).build();
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = new_project_id();
    let ts = hours_ago(3);

    // A normal write first, so TF creates the table with `timestamp` NOT NULL.
    let rows: Vec<_> = (0..2).map(|i| test_span_ts(&format!("n{i}"), "v", &project_id, ts + i * 1000)).collect();
    db.insert_records_batch(&project_id, TABLE, vec![json_to_batch(rows)?], true, None).await?;

    // Write out-of-band with `timestamp`/`id` nullability widened; values untouched.
    let rows: Vec<_> = (2..4).map(|i| test_span_ts(&format!("n{i}"), "v", &project_id, ts + i * 1000)).collect();
    let batch = json_to_batch(rows)?;
    let widened: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| if matches!(f.name().as_str(), "timestamp" | "id") { Arc::new(f.as_ref().clone().with_nullable(true)) } else { f.clone() })
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new_with_metadata(widened, batch.schema().metadata().clone())), batch.columns().to_vec())?;

    merge_write_out_of_band(&cfg, TABLE, vec![batch]).await?;

    // Precondition: the committed schema is untouched while the new file's footer
    // says nullable. Without that mismatch the test is green for the wrong reason.
    let table = load_out_of_band(&cfg, TABLE).await?;
    let ts_field = table.snapshot()?.schema().field("timestamp").expect("timestamp declared").clone();
    assert!(!ts_field.is_nullable(), "the COMMITTED Delta schema must still say NOT NULL — that is the whole mismatch");

    // Without the skip flag this is an
    // `Internal error: Physical input schema should be the same ...`.
    let ctx = ctx_for(&db)?;
    let sql = format!("SELECT time_bucket('1 hour', timestamp) AS b, COUNT(*) AS c FROM {TABLE} WHERE project_id = '{project_id}' GROUP BY b");
    let got = ctx.sql(&sql).await?.collect().await.map_err(|e| {
        anyhow::anyhow!(
            "GROUP BY on a NOT NULL-declared column failed over nullability-widened files: {e}\n\
             Set datafusion.execution.skip_physical_aggregate_schema_check (2026-07-31 dashboard outage)."
        )
    })?;
    assert_eq!(got.iter().map(|b| b.num_rows()).sum::<usize>(), 1, "all four rows fall in one hour bucket");

    // Grouping on an already-nullable column was never affected, so a green
    // result here alone would prove nothing.
    let sql = format!("SELECT status_code, COUNT(*) FROM {TABLE} WHERE project_id = '{project_id}' GROUP BY status_code");
    assert_eq!(ctx.sql(&sql).await?.collect().await?.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    Ok(())
}

/// A predicate on a column an UPDATE can change must never reach a scan leg.
///
/// Applied at the source it selects rows by a value belonging to a SUPERSEDED
/// version AND removes the newer version from `DedupExec`'s input, so
/// keep-greatest returns the stale row. `Inexact` pushdown cannot fix this:
/// re-applying the filter above the scan cannot recover a version the source
/// already dropped.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_filter_on_an_updated_column_never_matches_the_superseded_version() -> Result<()> {
    let (db, project_id) = buffered_db("mor_mutable_filter").await?;
    let ts = chrono::Utc::now().timestamp_micros();
    write_to(&db, "otel_logs_and_spans", &project_id, vec![test_span_ts("row", "before", &project_id, ts)], true).await?;

    let ctx = ctx_for(&db)?;
    ctx.sql(&format!("UPDATE otel_logs_and_spans SET hashes = make_array('after') WHERE project_id = '{project_id}' AND id = 'row'")).await?.collect().await?;

    // `hashes` is the one declared-mutable column, so its predicate must stay
    // ABOVE the dedup; filtering an immutable column here would prove nothing.
    let count = |name: &str| format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND array_element(hashes, 1) = '{name}'");
    assert_eq!(scalar_in(&ctx, &count("after")).await?, 1, "the current version must match its own value");
    assert_eq!(
        scalar_in(&ctx, &count("before")).await?,
        0,
        "the superseded value must match NOTHING — a non-zero count means the filter reached a scan leg and resurrected the old version"
    );
    Ok(())
}

/// A row written before `updated_at` existed has a NULL stamp; a stamped version
/// must beat it, or every UPDATE against such a row is a silent no-op. The row
/// has to be manufactured out-of-band because TF stamps everything it writes.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_legacy_null_stamped_row_loses_to_a_stamped_version() -> Result<()> {
    const TABLE: &str = "otel_logs_and_spans";
    let cfg = TestConfigBuilder::new("mor_null_stamp").with_buffer_mode(BufferMode::Enabled).build();
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = new_project_id();
    let ts = hours_ago(3);

    commit_span(&db, &project_id, "other", "v", ts).await?;

    // An out-of-band write leaves `updated_at` NULL, like a pre-migration row.
    let batch = json_to_batch(vec![test_span_ts("legacy", "before", &project_id, ts)])?;
    assert!(
        batch.column_by_name("updated_at").is_none_or(|c| c.null_count() == c.len()),
        "precondition: the out-of-band row must carry no stamp, or this test proves nothing"
    );
    merge_write_out_of_band(&cfg, TABLE, vec![batch]).await?;

    let ctx = ctx_for(&db)?;
    let sql = format!("SELECT COALESCE(array_element(hashes, 1), name) AS name FROM {TABLE} WHERE project_id = '{project_id}' AND id = 'legacy'");
    assert_eq!(col0_in(&ctx, &sql).await?, vec!["before"], "precondition: the legacy row reads back");

    ctx.sql(&format!("UPDATE {TABLE} SET hashes = make_array('after') WHERE project_id = '{project_id}' AND id = 'legacy'")).await?.collect().await?;

    assert_eq!(
        col0_in(&ctx, &sql).await?,
        vec!["after"],
        "a stamped version must beat a legacy NULL stamp — otherwise UPDATE is a silent no-op after the flip"
    );
    Ok(())
}

/// `migrate_add_columns` widens STORAGE first, so a later YAML change declaring
/// the same columns is a no-op for the stored schema. Widening the YAML alone is
/// not safe. The fixture creates the table at the OLD column set, because a
/// table built from the current YAML already has the columns.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn migrate_add_columns_widens_the_stored_schema_and_is_idempotent() -> Result<()> {
    const TABLE: &str = "mor_versioned";
    let cfg = TestConfigBuilder::new("migrate_cols").with_buffer_mode(BufferMode::Enabled).build();
    create_at_old_column_set(&cfg, TABLE, &["updated_at", "deleted"]).await?;

    let db = Database::with_config(Arc::clone(&cfg)).await?;
    let adds = vec![("updated_at".to_string(), "timestamp".to_string()), ("deleted".to_string(), "boolean".to_string())];

    let dry = db.migrate_add_columns(TABLE, &adds, true).await?;
    assert_eq!(dry.added.len(), 2, "dry run must report both missing columns");
    assert_eq!(dry.stored_after, dry.stored_before, "dry run must not change the stored schema");

    let first = db.migrate_add_columns(TABLE, &adds, false).await?;
    assert_eq!(first.added.len(), 2, "both columns must be added to the STORED schema, got {:?}", first.added);
    assert_eq!(first.stored_after, first.stored_before + 2, "stored column count must grow by exactly the two added");

    // Idempotent: re-running must be a no-op, not a second commit.
    let second = db.migrate_add_columns(TABLE, &adds, false).await?;
    assert!(second.added.is_empty(), "re-running the migration must add nothing, got {:?}", second.added);
    assert_eq!(second.stored_before, first.stored_after, "the second run must observe the widened schema");

    // Rollup measure types must be migratable: counts are Int64, sums/min/max
    // Int64 or Float64, and `tdigest`/`hll` states are Binary.
    let measures =
        vec![("m_count".to_string(), "bigint".to_string()), ("m_ratio".to_string(), "double".to_string()), ("m_digest".to_string(), "binary".to_string())];
    let widened = db.migrate_add_columns(TABLE, &measures, false).await?;
    assert_eq!(widened.added.len(), 3, "every measure type must be migratable, got {:?}", widened.added);
    assert_eq!(widened.stored_after, widened.stored_before + 3);
    assert!(db.migrate_add_columns(TABLE, &measures, false).await?.added.is_empty(), "measure migration must be idempotent too");

    // A promoted OTel attribute that is not a count or timestamp is Utf8, so
    // `text`/`string` must be migratable too.
    let attrs = vec![("a_route".to_string(), "text".to_string()), ("a_alias".to_string(), "string".to_string())];
    let widened_attrs = db.migrate_add_columns(TABLE, &attrs, false).await?;
    assert_eq!(widened_attrs.added.len(), 2, "a Utf8 attribute column must be migratable, got {:?}", widened_attrs.added);
    assert!(db.migrate_add_columns(TABLE, &attrs, false).await?.added.is_empty(), "attribute migration must be idempotent too");
    {
        let t = db.get_or_create_unified_table(TABLE).await?;
        let guard = t.read().await;
        let stored = guard.snapshot()?.schema();
        let f = stored.field("a_route").expect("migrated column is in the stored schema");
        assert!(format!("{:?}", f.data_type()).contains("String"), "a_route must land as a string, got {:?}", f.data_type());
    }

    assert!(db.migrate_add_columns(TABLE, &[("m_bad".to_string(), "decimal".to_string())], true).await.is_err());

    Ok(())
}

/// The hot-tail skip, end to end: with the default ON, a reconcile whose only
/// uncovered files are in TODAY's partition builds nothing — while the census
/// still counts them, so the today/week/older breakdown stays honest.
#[serial]
#[tokio::test]
async fn tantivy_backfill_skips_todays_partition_but_the_census_still_counts_it() -> Result<()> {
    use timefusion::tantivy::search::TantivyIndexService;
    const TABLE: &str = "otel_logs_and_spans";
    let cfg = TestConfigBuilder::new("tantivy_skip_today").with_buffer_mode(BufferMode::Enabled).build();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let svc =
        Arc::new(TantivyIndexService::new(store, Arc::new(cfg.tantivy.clone()), std::env::temp_dir().join(format!("tf-scratch-{}", uuid::Uuid::new_v4()))));
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?.with_tantivy_indexer(svc));
    let project_id = new_project_id();
    // Must land in TODAY's partition whatever the hour: a bare `now - 2h` falls
    // into YESTERDAY between 00:00 and 02:00 UTC, which is not skipped.
    let now = chrono::Utc::now();
    let ts = (now - chrono::Duration::hours(2)).max(now.date_naive().and_hms_opt(0, 0, 1).unwrap().and_utc()).timestamp_micros();
    commit_span(&db, &project_id, "hot", "n", ts).await?;

    let (uncovered, _, _) = db.tantivy_coverage_census().await?;
    assert!(uncovered >= 1, "the census must still SEE today's uncovered files, got {uncovered}");
    let (built, _, _) = db.tantivy_reconcile_table(TABLE).await?;
    assert_eq!(built, 0, "today's partition is churn — the backfill must not spend the pass on it, built={built}");
    Ok(())
}

/// `tantivy_reconcile_table` is the optimize CLI's post-run repair: it must
/// backfill uncovered files AND GC stale entries, including the per-uuid
/// manifests the in-server hook's fixed "default"+customs list never reaches.
#[serial]
#[tokio::test]
async fn tantivy_reconcile_backfills_new_files_and_gcs_orphans() -> Result<()> {
    use timefusion::tantivy::{load_manifest, search::TantivyIndexService};
    const TABLE: &str = "otel_logs_and_spans";
    // The rows below land in TODAY's partition, which the backfill skips by
    // default. This test is about backfill+GC mechanics, so it opts out — after
    // asserting the default, so the opt-out cannot hide a regression.
    let cfg = tuned_cfg("tantivy_reconcile", |c| {
        assert!(c.tantivy.timefusion_tantivy_backfill_skip_today, "the hot-tail skip must be ON by default — this test's opt-out is what makes it meaningful");
        c.tantivy.timefusion_tantivy_backfill_skip_today = false;
    });
    let tantivy_store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let svc = Arc::new(TantivyIndexService::new(
        tantivy_store.clone(),
        Arc::new(cfg.tantivy.clone()),
        std::env::temp_dir().join(format!("tf-scratch-{}", uuid::Uuid::new_v4())),
    ));
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?.with_tantivy_indexer(svc));
    let project_id = new_project_id();
    let ts = hours_ago(3);

    // Two direct commits → two live parquet files, neither indexed (the direct
    // insert path has no flush-time index callback).
    for id in ["id_a", "id_b"] {
        commit_span(&db, &project_id, id, "n", ts).await?;
    }

    // Seed the global gauge so an unwritten zero cannot pass the census checks.
    let tantivy_stats = timefusion::observability::maintenance_stats();
    let ordering = std::sync::atomic::Ordering::Relaxed;
    tantivy_stats.tantivy_uncovered_files.store(999, ordering);

    let (uncovered_before, _, by_age_before) = db.tantivy_coverage_census().await?;
    assert!(uncovered_before >= 2, "census must count uncovered live files before any build, got {uncovered_before}");
    // The age split must partition the total, not merely exist.
    assert_eq!(by_age_before.iter().sum::<u64>(), uncovered_before, "age buckets must account for every uncovered file: {by_age_before:?}");

    assert_eq!(db.tantivy_reconcile_table("mor_versioned").await?, (0, 0, 0));
    assert_eq!(
        tantivy_stats.tantivy_uncovered_files.load(ordering),
        uncovered_before,
        "backfilling an empty table must not erase the all-table coverage census"
    );

    let (built, removed, _) = db.tantivy_reconcile_table(TABLE).await?;
    assert!(built >= 2, "expected both uncovered live files indexed, built={built}");
    assert_eq!(removed, 0, "nothing to GC before compaction");
    let (uncovered_after, _, by_age_after) = db.tantivy_coverage_census().await?;
    assert_eq!(tantivy_stats.tantivy_uncovered_files.load(ordering), 0, "the all-table census must publish zero after all uncovered files have been indexed");
    assert_eq!(uncovered_after, 0, "after indexing every uncovered file the census must independently agree the reindex is done");
    assert_eq!(by_age_after, [0; 3], "a finished reindex leaves no uncovered file in any age bucket");
    let m = load_manifest(tantivy_store.as_ref(), TABLE, &project_id).await?;
    assert_eq!(m.entries.len(), 2, "per-uuid manifest covers both files");

    let table_ref = table_of(&db, TABLE).await;
    db.compact_date_concurrent(&table_ref, TABLE, date_of(ts), Some(&project_id), None).await?;

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

/// Noon UTC `n` days back: a sealed partition instant far enough from midnight
/// that a UTC rollover mid-test cannot move a row into another `date=`.
fn noon_days_ago(n: i64) -> i64 {
    (chrono::Utc::now().date_naive() - chrono::Duration::days(n)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros()
}

/// Yesterday noon UTC: a sealed prior-day partition the sweep will rewrite, and
/// an instant that never straddles a midnight-UTC date flip.
fn yesterday_noon() -> i64 {
    noon_days_ago(1)
}

/// Two versions of ONE key in separate files — the shape merge-on-read produces
/// for an UPDATE. They differ only in `hashes` (the `mutable: true` column);
/// the second commit gets the greater `updated_at`, so `"updated"` wins and
/// `"original"` is the superseded version.
async fn commit_two_hash_versions(db: &Arc<Database>, project_id: &str, ts: i64) -> Result<()> {
    for tag in ["original", "updated"] {
        let mut value = test_span_ts("mor_key", "span", project_id, ts);
        value["hashes"] = serde_json::json!([tag]);
        db.insert_records_batch(project_id, "otel_logs_and_spans", vec![json_to_batch(vec![value])?], true, None).await?;
    }
    Ok(())
}

/// A db with the swept-partition read-dedup skip forced ON.
/// `timefusion_read_dedup_skip_swept` is off by default, so these fixtures are
/// the only place that path runs until an operator opts in.
async fn skip_swept_db(name: &str) -> Result<(Arc<Database>, String)> {
    tuned_db(name, |c| c.maintenance.timefusion_read_dedup_skip_swept = true).await
}

/// Rows whose `hashes[0]` equals `val` in the single-instant window at `ts`,
/// read through `query_delta_only` — the Delta leg alone, where a leaked
/// pushdown would show up.
async fn hash_matches(db: &Arc<Database>, project_id: &str, ts: i64, val: &str) -> Result<usize> {
    let sql = format!(
        "SELECT array_element(hashes, 1) FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
         AND timestamp >= to_timestamp_micros({ts}) AND timestamp <= to_timestamp_micros({ts}) \
         AND array_element(hashes, 1) = '{val}'"
    );
    Ok(db.query_delta_only(&sql).await?.iter().map(|b| b.num_rows()).sum())
}

/// The safety precondition for dropping the read-side `DedupExec` on a swept
/// partition: after the sweep a certified partition holds exactly one row per
/// key, even on a `version_append` table.
///
/// Deliberately reads PHYSICAL rows (Delta log stats), never a routed query:
/// the read-side dedup would mask exactly the property under test.
#[serial]
#[tokio::test]
async fn a_swept_mor_partition_holds_one_winning_row_per_key() -> Result<()> {
    let (db, project_id) = plain_db("swept_mor_one_winner").await?;
    let ts = yesterday_noon();
    commit_two_hash_versions(&db, &project_id, ts).await?;

    let table_ref = table_of(&db, "otel_logs_and_spans").await;
    assert_eq!(delta_physical_row_count(&table_ref).await?, 2, "pre-sweep: the two versions are two physical rows, which is what a skip would wrongly serve");

    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

    assert_eq!(
        delta_physical_row_count(&table_ref).await?,
        1,
        "a swept merge-on-read partition must hold ONE physical row per key — otherwise skipping DedupExec would serve a superseded version"
    );
    Ok(())
}

/// End-to-end guard for the read-side dedup skip on a merge-on-read table:
/// a certified partition must return the UPDATED value, never the superseded
/// one — asserted through the routed scan rather than the Delta log.
#[serial]
#[tokio::test]
async fn dedup_skip_on_a_swept_mor_partition_returns_the_updated_row() -> Result<()> {
    let (db, project_id) = skip_swept_db("dedup_skip_mor_winner").await?;
    let ts = yesterday_noon();
    commit_two_hash_versions(&db, &project_id, ts).await?;

    let table_ref = table_of(&db, "otel_logs_and_spans").await;
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

    let sql = format!(
        "SELECT array_element(hashes, 1) FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
         AND timestamp >= {ts} AND timestamp <= {ts}",
        ts = format_args!("to_timestamp_micros({ts})")
    );
    let rows = col0_strings(&db.query_delta_only(&sql).await?);

    assert_eq!(rows.len(), 1, "exactly one row survives the sweep, so the skip must return one: {rows:?}");
    assert_eq!(rows[0], "updated", "the skip must return the WINNING version, never the superseded one");
    Ok(())
}

/// With `timefusion_compact_dedup_merge` on, `compact_date` must collapse
/// merge-on-read versions while merging files — one physical row per
/// (timestamp, id), greatest `updated_at` winning — and must RETAIN tombstones
/// (dropping a `deleted=true` winner would resurrect the base row).
///
/// Physical rows are asserted via the Delta log; the read-side DedupExec would
/// mask exactly the property under test.
#[serial]
#[tokio::test]
async fn compact_dedup_merge_collapses_versions_and_retains_tombstones() -> Result<()> {
    let (db, project_id) = tuned_db("compact_dedup_merge", |c| c.maintenance.timefusion_compact_dedup_merge = true).await?;
    let ts = yesterday_noon();

    commit_two_hash_versions(&db, &project_id, ts).await?;
    // Key B: a tombstone the merge must carry through verbatim.
    let tombstone = {
        let mut value = test_span_ts("dead_key", "span", &project_id, ts + 1);
        value["deleted"] = serde_json::json!(true);
        json_to_batch(vec![value])?
    };
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![tombstone], true, None).await?;

    let table_ref = table_of(&db, "otel_logs_and_spans").await;
    assert_eq!(delta_physical_row_count(&table_ref).await?, 3, "pre-compact: two versions of key A + the key B tombstone");

    let date = date_of(ts);
    db.compact_date(&table_ref, "otel_logs_and_spans", date, Some(&project_id)).await?;

    // The 2 includes key B's tombstone: dropping it would have left 1.
    assert_eq!(delta_physical_row_count(&table_ref).await?, 2, "post-compact: key A collapsed to its winner, key B's tombstone retained");
    let rows = db
        .query_delta_only(&format!("SELECT id, array_element(hashes, 1) AS tag FROM otel_logs_and_spans WHERE project_id = '{project_id}' ORDER BY id"))
        .await?;
    let got: Vec<(String, String)> = rows_of(&rows, |b, i| (str_at(b, 0, i), str_at(b, 1, i)));
    assert_eq!(got, vec![("mor_key".into(), "updated".into())], "the greatest-updated_at version wins and the tombstoned key stays suppressed");
    Ok(())
}

/// A CERTIFIED Delta partition plus fresher MemBuffer rows for the same date,
/// queried without projecting a dedup key or the tombstone marker. `pre_skip_dedup`
/// drops the dedup keys from the scan projection while the mem ∪ delta union path
/// never grants `skip_dedup`, so a `DedupExec` could be built over a scan that no
/// longer carries `id` (`Internal error: DedupExec key 'id' not in input schema`).
#[serial]
#[tokio::test]
async fn a_certified_partition_with_buffered_rows_still_answers_without_the_keys_projected() -> Result<()> {
    let cfg = tuned_cfg("dedup_skip_mem_union", |c| c.maintenance.timefusion_read_dedup_skip_swept = true);
    let db0 = Database::with_config(Arc::clone(&cfg)).await?;
    let layer = Arc::new(timefusion::support::test_helpers::test_layer(Arc::clone(&cfg))?.with_delta_writer(timefusion::server::delta_write_callback(&db0)));
    let db = Arc::new(db0.with_buffered_layer(Arc::clone(&layer)));
    let project_id = new_project_id();

    let ts = yesterday_noon();
    let day = date_of(ts);
    let (lo, hi) = (day.and_hms_opt(0, 0, 0).unwrap().and_utc().to_rfc3339(), day.and_hms_opt(23, 59, 59).unwrap().and_utc().to_rfc3339());

    // Delta leg: committed directly, then swept so the partition is certified.
    commit_span(&db, &project_id, "settled", "delta", ts).await?;
    let table_ref = table_of(&db, "otel_logs_and_spans").await;
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

    // MemBuffer leg: same date, never flushed — this is what makes it a union.
    write_to(&db, "otel_logs_and_spans", &project_id, vec![test_span_ts("buffered", "mem", &project_id, ts + 1)], false).await?;

    let ctx = ctx_for(&db)?;
    // Warm the fast-resolve cache: `pre_skip_dedup` reads it, and a cold cache
    // leaves the skip off entirely.
    let _ = ctx.sql(&format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{project_id}'")).await?.collect().await?;

    // `name` is neither a dedup key (timestamp, id) nor the tombstone marker.
    let sql = format!("SELECT name FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND timestamp >= '{lo}' AND timestamp <= '{hi}'");
    let batches = ctx.sql(&sql).await?.collect().await?;

    for b in &batches {
        assert_eq!(b.num_columns(), 1, "only `name` was selected; a dedup key or the tombstone marker leaked through");
    }
    let mut rows = col0_strings(&batches);
    rows.sort();
    assert_eq!(rows, vec!["delta".to_string(), "mem".to_string()], "both legs must answer");
    Ok(())
}

/// A predicate on a version-MUTABLE column, pushed into the Delta leg because
/// the window is sweep-certified. Pushing such a predicate BELOW the dedup
/// normally drops the newer version of a key and leaves keep-greatest serving
/// the superseded one; the claim is that a certified partition has no superseded
/// version to serve. Asserts BOTH directions: the new value is found, the old is not.
///
/// The UNCERTIFIED case is the guard: no sweep, so `dedup_skip_allowed` refuses,
/// the predicate is re-stripped by `leg_safe` and `DedupExec` runs. There the
/// superseded row really is still on disk, so an escaped pushdown shows up.
#[test_case("swept_pushdown_mutable", true ; "a certified partition may push the predicate")]
#[test_case("uncertified_no_pushdown", false ; "an uncertified window keeps the dedup above the scan")]
#[serial]
#[tokio::test]
async fn a_mutable_predicate_never_matches_the_superseded_row(name: &str, certified: bool) -> Result<()> {
    // The skip is ON in BOTH cases: the uncertified case must be refused by the
    // certification gate, not by the flag being off.
    let (db, project_id) = skip_swept_db(name).await?;
    let ts = yesterday_noon();
    commit_two_hash_versions(&db, &project_id, ts).await?;

    let table_ref = table_of(&db, "otel_logs_and_spans").await;
    if certified {
        db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
    } else {
        // Deliberately NO `dedup_today_partitions`: both physical versions remain.
        assert_eq!(
            delta_physical_row_count(&table_ref).await?,
            2,
            "precondition: the superseded row is physically present, so a leaked pushdown could match it"
        );
    }

    assert_eq!(hash_matches(&db, &project_id, ts, "updated").await?, 1, "the winning version must still be found through the predicate");
    assert_eq!(
        hash_matches(&db, &project_id, ts, "original").await?,
        0,
        "the SUPERSEDED value must match nothing — a match means the predicate reached a scan leg (escaping the certification gate, in the uncertified case) \
         and resurrected a stale version below the dedup"
    );
    Ok(())
}

/// Restores the real clock when a rollup test ends: every one of them freezes
/// and advances virtual time to clear the finalization deadlines.
struct UnfreezeOnDrop;
impl Drop for UnfreezeOnDrop {
    fn drop(&mut self) {
        timefusion::support::unfreeze();
    }
}

/// The rollup-parity scaffold: a rollups-enabled db plus the two day boundaries
/// every parity fixture writes to — yesterday, which certifies into the tier,
/// and today, which can only be reached through the raw leg.
struct RollupEnv {
    _clock: UnfreezeOnDrop,
    db: Arc<Database>,
    project_id: String,
    midnight: i64,
    yesterday_noon: i64,
}

async fn rollup_env(name: &str) -> Result<RollupEnv> {
    rollup_env_of(TestConfigBuilder::new(name).with_buffer_mode(BufferMode::Enabled).with_rollups().build()).await
}

/// The same scaffold over a caller-tweaked config, which must outlive the
/// `Database` built from it.
async fn rollup_env_of(cfg: Arc<timefusion::config::AppConfig>) -> Result<RollupEnv> {
    let (db, project_id) = db_of(cfg).await?;
    Ok(RollupEnv {
        _clock: UnfreezeOnDrop,
        db,
        project_id,
        midnight: chrono::Utc::now().date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros(),
        yesterday_noon: yesterday_noon(),
    })
}

impl RollupEnv {
    /// One fixture row committed straight to Delta. `service` and `duration` are
    /// the two columns the parity fixtures vary; `None` writes a genuine SQL
    /// NULL, which several of them depend on.
    async fn insert(&self, id: &str, ts: i64, service: Option<&str>, duration: Option<i64>, summary: &str) -> Result<()> {
        self.insert_row(id, ts, service, duration, "OK", summary).await
    }

    /// As `insert`, with `status_code` varied too — the second grouped dimension.
    async fn insert_row(&self, id: &str, ts: i64, service: Option<&str>, duration: Option<i64>, status: &str, summary: &str) -> Result<()> {
        let batch = json_to_batch(vec![serde_json::json!({
            "timestamp": ts, "id": id, "name": "op", "project_id": self.project_id, "hashes": [], "summary": [summary],
            "date": date_of(ts).to_string(),
            "duration": duration, "kind": "server", "status_code": status, "resource___service___name": service,
        })])?;
        self.db.insert_records_batch(&self.project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
        Ok(())
    }

    /// `SUM(request_count)` over one rollup tier for this project — how many raw
    /// spans a tier actually accounts for. Zero when nothing was built.
    async fn tier_rows(&self, table: &str) -> Result<i64> {
        delta_scalar(&self.db, &format!("SELECT COALESCE(SUM(request_count), 0)::BIGINT FROM {table} WHERE project_id = '{}'", self.project_id)).await
    }

    /// Certify the written partitions, clear the finalization and invalidation
    /// deadlines on the virtual clock, then drain the maintenance queue. Returns
    /// the number of units run so each caller keeps its own assertion.
    async fn certify_and_drain(&self) -> Result<usize> {
        self.certify().await?;
        timefusion::support::advance_micros(
            timefusion::maintenance_coordinator::FINALIZATION_DELAY_MICROS + timefusion::maintenance_coordinator::INVALIDATION_DEADLINE_BUCKET_MICROS + 1,
        );
        self.db.run_maintenance_units(1024).await
    }

    async fn certify(&self) -> Result<()> {
        sweep_once(&self.db).await
    }

    /// The window predicate every parity query shares: this project, from the
    /// first fixture row on the certified day to two hours into today.
    fn window(&self) -> String {
        let (lo, hi) = (self.yesterday_noon + 17, self.midnight + 7_200_000_017);
        format!("project_id = '{}' AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi})", self.project_id)
    }
}

/// Hybrid (rollup ∪ raw fringe) routes. `full` counts the windows served
/// entirely from the tier; a shape that may take either is checked with both.
fn hybrid_hits() -> u64 {
    timefusion::observability::maintenance_stats().rollup_hits_hybrid.load(std::sync::atomic::Ordering::Relaxed)
}

fn any_rollup_hits() -> u64 {
    let stats = timefusion::observability::maintenance_stats();
    stats.rollup_hits_hybrid.load(std::sync::atomic::Ordering::Relaxed) + stats.rollup_hits_full.load(std::sync::atomic::Ordering::Relaxed)
}

/// Two scalar `i64`s from one probe row, through `query_delta_only` — the
/// fixture preconditions every parity test asserts before trusting equality.
async fn delta_pair(db: &Arc<Database>, sql: &str) -> Result<(i64, i64)> {
    let batches = db.query_delta_only(sql).await?;
    let b = batches.iter().find(|b| b.num_rows() > 0).expect("a probe row");
    Ok((i64_at(b, 0, 0), i64_at(b, 1, 0)))
}

/// Every rollup miss counter, by name — so a failing route assertion can report
/// WHY the query missed.
fn miss_counters() -> Vec<(&'static str, u64)> {
    let s = timefusion::observability::maintenance_stats();
    let load = |c: &std::sync::atomic::AtomicU64| c.load(std::sync::atomic::Ordering::Relaxed);
    vec![
        ("TOTAL", load(&s.rollup_misses_total)),
        ("not_built", load(&s.rollup_miss_not_built)),
        ("stale_coverage", load(&s.rollup_miss_stale_coverage)),
        ("incomplete_coverage", load(&s.rollup_miss_incomplete_coverage)),
        ("unsupported_shape", load(&s.rollup_miss_unsupported)),
        ("unwalkable_source", load(&s.rollup_miss_unwalkable_source)),
        ("filter_not_eligible", load(&s.rollup_miss_filter_not_eligible)),
        ("missing_measure", load(&s.rollup_miss_missing_measure)),
        ("unknown_group_by", load(&s.rollup_miss_unknown_group_by)),
        ("missing_project", load(&s.rollup_miss_missing_project)),
        ("unaligned_bucket", load(&s.rollup_miss_unaligned_bucket)),
        ("tiny_interior", load(&s.rollup_miss_tiny_interior)),
        ("too_many_branches", load(&s.rollup_miss_too_many_branches)),
        ("unknown_filter", load(&s.rollup_miss_unknown_filter)),
        ("measure_not_stored", load(&s.rollup_miss_measure_not_stored)),
        ("unbounded_time", load(&s.rollup_miss_unbounded_time)),
        ("non_decomposable", load(&s.rollup_miss_non_decomposable)),
        ("rewrite_schema_mismatch", load(&s.rollup_miss_rewrite_schema_mismatch)),
    ]
}

/// The miss reasons that moved since `before`, rendered for a failure message.
fn miss_delta(before: &[(&'static str, u64)]) -> String {
    miss_counters()
        .iter()
        .zip(before)
        .filter(|((_, now), (_, then))| now > then)
        .map(|((name, now), (_, then))| format!("{name}=+{}", now - then))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Run `query` on the routed session and assert it was served as a hybrid
/// (rollup ∪ raw fringe) union, reporting the miss reasons that moved on failure.
///
/// The routing decision is only observable through the counter: the substitution
/// happens in `DmlQueryPlanner`, and EXPLAIN renders its inner plan with the
/// DEFAULT planner, so an explained query always shows the raw scan.
async fn routed_hybrid(ctx: &datafusion::prelude::SessionContext, query: &str, why: &str) -> Result<Vec<RecordBatch>> {
    let (before, misses) = (hybrid_hits(), miss_counters());
    let out = ctx.sql(query).await?.collect().await?;
    assert_eq!(hybrid_hits(), before + 1, "{why} (misses: [{}])", miss_delta(&misses));
    Ok(out)
}

/// As `routed_hybrid`, for a shape that may legitimately be served either wholly
/// from the tier or as a hybrid.
async fn routed_any(ctx: &datafusion::prelude::SessionContext, query: &str, why: &str) -> Result<Vec<RecordBatch>> {
    let (before, misses) = (any_rollup_hits(), miss_counters());
    let out = ctx.sql(query).await?.collect().await?;
    assert!(any_rollup_hits() > before, "{why} (misses: [{}])", miss_delta(&misses));
    Ok(out)
}

/// The backfill fixture: a rollups-enabled db with a 7-day horizon plus spans on
/// D-5 and D-4 — sealed days outside the dedup lookback (default 1), the gap the
/// backfill exists to close. A bare config drains ZERO units and every assertion
/// downstream then passes vacuously. Returns the config so coverage recovery can
/// be proved by a SECOND `Database` over the same storage prefix.
async fn backfill_env(name: &str) -> Result<(Arc<timefusion::config::AppConfig>, RollupEnv)> {
    let base = TestConfigBuilder::new(name).with_buffer_mode(BufferMode::Enabled).with_rollups().build();
    let cfg = tweak(base, |c| c.maintenance.timefusion_rollup_backfill_days = 7);
    let env = rollup_env_of(Arc::clone(&cfg)).await?;
    for back in [5i64, 4] {
        env.insert(&format!("d{back}"), noon_days_ago(back), Some("cart"), Some(100), "backfill fixture").await?;
    }
    env.certify().await?;
    Ok((cfg, env))
}

/// Plan the backfill, step past `FINALIZATION_DELAY` — units are minted with a
/// deadline in the future, so draining immediately claims nothing — then drain.
async fn plan_and_drain_backfill(db: &Arc<Database>) -> Result<usize> {
    db.plan_rollup_backfill().await?;
    timefusion::support::advance_micros(16 * 60 * 1_000_000);
    db.drain_coordinator_rollups(64).await
}

/// Keys `k{lo}`..`k{hi}` one microsecond apart from `ts`, as ONE Delta commit.
/// Committing the same range twice is how cross-file duplicates are made:
/// flush-time dedup is per-bucket and cannot see across files.
async fn commit_span_run(db: &Arc<Database>, project_id: &str, ts: i64, lo: usize, hi: usize) -> Result<()> {
    let batch = json_to_batch((lo..hi).map(|i| test_span_ts(&format!("k{i}"), "n", project_id, ts + i as i64)).collect())?;
    db.insert_records_batch(project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
    Ok(())
}

/// A TIME-BOUNDED select over one project. Bounded on purpose: without a window
/// `dedup_skip_allowed` returns `NoWindow` and refuses before it ever consults a
/// certification, so an unbounded query silently measures nothing.
fn window_select(column: &str, project_id: &str, lo: i64, hi: i64) -> String {
    format!(
        "SELECT {column} FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
         AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi})"
    )
}

/// End-to-end for the dashboard rollup: certifying a partition must build
/// buckets that agree with the raw aggregate EXACTLY. Also pins idempotence —
/// a partition can be certified more than once, so a second build must REPLACE
/// its rows rather than double every count.
#[serial]
#[tokio::test]
async fn certifying_a_partition_builds_rollup_buckets_that_match_the_raw_aggregate() -> Result<()> {
    let env = rollup_env("rollup_build_parity").await?;
    let (db, project_id, ts) = (Arc::clone(&env.db), env.project_id.clone(), env.yesterday_noon);

    for (i, offset) in [0i64, 1_000_000, 2_000_000, 60_000_000].iter().enumerate() {
        commit_span(&db, &project_id, &format!("span_{i}"), "op", ts + offset).await?;
    }

    assert!(env.certify_and_drain().await? > 0, "eligible slice tasks must be drained");

    let rollup_total =
        format!("SELECT COALESCE(SUM(request_count), 0)::BIGINT FROM otel_logs_and_spans_rollup_dashboard_1m_v3 WHERE project_id = '{project_id}'");
    let raw_total = delta_scalar(&db, &format!("SELECT COUNT(*)::BIGINT FROM otel_logs_and_spans WHERE project_id = '{project_id}'")).await?;

    assert_eq!(
        delta_scalar(&db, &rollup_total).await?,
        raw_total,
        "rollup request_count must sum to the raw row count, or every Traffic panel is silently wrong"
    );
    assert!(raw_total > 0, "precondition: the fixture actually wrote rows");

    // The publication counters must move on the COORDINATOR path, not only on the
    // cohort path they were originally wired to. nextest gives each test its own
    // process, so these process-global counters are this test's alone.
    let stats = timefusion::observability::maintenance_stats();
    let ordering = std::sync::atomic::Ordering::Relaxed;
    assert!(stats.rollup_output_rows.load(ordering) > 0, "the coordinator published rollup rows but rollup_output_rows_total stayed 0");
    assert!(stats.rollup_staged_projects.load(ordering) > 0, "the coordinator published a slice but rollup_staged_projects_total stayed 0");
    assert!(stats.rollup_commit_actions.load(ordering) > 0, "the coordinator committed Delta actions but rollup_commit_actions_total stayed 0");

    env.certify().await?;
    assert_eq!(delta_scalar(&db, &rollup_total).await?, raw_total, "a second certification must REPLACE the buckets, not double every measure");

    let late = date_of(ts).and_hms_opt(20, 30, 0).unwrap().and_utc().timestamp_micros();
    commit_span(&db, &project_id, "span_late", "op", late).await?;
    assert!(env.certify_and_drain().await? > 0, "late slice tasks must be drained");
    assert_eq!(delta_scalar(&db, &rollup_total).await?, raw_total + 1, "an incremental rebuild must carry the untouched hours forward exactly once");
    Ok(())
}

/// The rollup must answer a window it only PARTLY covers, by unioning its
/// certified interior with raw fringes, and the answer must equal the raw one.
///
/// The gate for routing `COALESCE(<dimension>, 'null')`, which folds NULL and
/// the literal string `'null'` into ONE group. The tier's partition by `dim`
/// REFINES the partition by `COALESCE(dim, lit)`, so re-aggregating decomposable
/// states over the refinement equals aggregating raw rows. The fixture holds a
/// NULL service AND a literal-'null' one, in both legs, so the fold must survive
/// the union.
///
/// Asserts routing as well as equality: a miss also returns the right answer,
/// so equality alone would pass vacuously.
#[serial]
#[tokio::test]
async fn a_coalesced_dimension_folds_null_and_the_literal_identically_through_the_rollup() -> Result<()> {
    let env = rollup_env("rollup_coalesce").await?;
    let db = Arc::clone(&env.db);
    db.cancel_maintenance();

    // `service: None` is a genuine SQL NULL; "null" is the four-character string.
    // Yesterday, spread over hours so the interior is worth a union.
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
        env.insert(&format!("y{i}"), env.yesterday_noon + offset, *service, Some(100), "coalesce fixture").await?;
    }

    assert!(env.certify_and_drain().await? > 0, "eligible yesterday slices must be drained");

    // Written AFTER certification, so these reach the query only via the raw leg;
    // both fold-participants appear here too, forcing the outer merge to combine
    // a folded group ACROSS the two legs.
    for (i, (service, offset)) in [(None, 5_000_000i64), (Some("null"), 3_600_000_000), (Some("cart"), 3_700_000_000)].iter().enumerate() {
        env.insert(&format!("t{i}"), env.midnight + offset, *service, Some(100), "coalesce fixture").await?;
    }

    let window = env.window();

    // Without both a real NULL and a real 'null' the fold is never exercised.
    let (nulls, literals) = delta_pair(
        &db,
        &format!(
            "SELECT COUNT(*) FILTER (WHERE resource___service___name IS NULL)::BIGINT, \
                    COUNT(*) FILTER (WHERE resource___service___name = 'null')::BIGINT \
             FROM otel_logs_and_spans WHERE {window}"
        ),
    )
    .await?;
    assert!(nulls > 0 && literals > 0, "the fixture must hold BOTH real NULLs and literal 'null's, got nulls={nulls} literals={literals}");

    let query = format!(
        "SELECT time_bucket('1 hours', timestamp) AS tb, COALESCE(resource___service___name, 'null') AS svc, \
                COUNT(*) AS c, min(duration) AS lo, max(duration) AS hi \
         FROM otel_logs_and_spans WHERE {window} GROUP BY 1, 2 ORDER BY 1, 2"
    );

    let ctx = ctx_for(&db)?;

    let row = |b: &RecordBatch, r: usize| (ts_at(b, 0, r), str_at(b, 1, r), i64_at(b, 2, r), i64_at(b, 3, r), i64_at(b, 4, r));

    let routed = rows_of(&routed_hybrid(&ctx, &query, "the coalesced chart must route, or this proves nothing about the rollup's fold").await?, row);

    // `query_delta_only` bypasses rollup and buffer, so it is the authority.
    let raw = rows_of(&db.query_delta_only(&query).await?, row);
    assert_eq!(routed, raw, "the coalesced group must match the raw aggregate row for row");

    assert_eq!(routed.iter().map(|row| row.2).sum::<i64>(), 11, "every fixture row must be counted exactly once: {routed:?}");
    assert!(routed.iter().all(|row| row.1 == "null" || row.1 == "cart"), "COALESCE must leave only folded labels: {routed:?}");
    Ok(())
}

/// The gate for dropping `duration IS NOT NULL` when routing a latency chart.
///
/// Dropping it is NOT free: `percentile_agg` skips nulls so the VALUES are
/// unaffected, but the raw query also ELIMINATES a bucket whose every row has a
/// null duration, which the rollup would resurrect as a 0. So the predicate is
/// replaced by `HAVING sum(duration_count) > 0` — `duration_count` is
/// `count(duration)`, exactly the rows the predicate would have kept.
///
/// The fixture has one hour whose rows are ALL null, beside mixed hours; if the
/// guard is wrong the routed answer gains a bucket the raw answer lacks.
#[serial]
#[tokio::test]
async fn an_all_null_duration_bucket_is_eliminated_identically_through_the_rollup() -> Result<()> {
    let env = rollup_env("rollup_null_guard").await?;
    let db = Arc::clone(&env.db);
    db.cancel_maintenance();

    // Hour 0 mixes null and non-null; hour 1 is ALL null (the bucket that must
    // vanish); hour 2 is all non-null. Yesterday, so it certifies into the tier.
    for (i, (duration, offset)) in
        [(Some(100i64), 17i64), (None, 61_000_000), (Some(300), 130_000_000), (None, 3_661_000_000), (None, 3_700_000_000), (Some(500), 7_330_000_000)]
            .iter()
            .enumerate()
    {
        env.insert(&format!("y{i}"), env.yesterday_noon + offset, Some("cart"), *duration, "null-guard fixture").await?;
    }

    assert!(env.certify_and_drain().await? > 0, "eligible yesterday slices must be drained");

    // Today, reachable only through the raw leg — with its own all-null hour, so
    // the guard has to hold on BOTH sides of the union.
    for (i, (duration, offset)) in [(None, 5_000_000i64), (None, 60_000_000), (Some(700), 3_700_000_000)].iter().enumerate() {
        env.insert(&format!("t{i}"), env.midnight + offset, Some("cart"), *duration, "null-guard fixture").await?;
    }

    let window = env.window();

    // Without an all-null bucket the comparison below proves nothing.
    let all_null_buckets = delta_scalar(
        &db,
        &format!(
            "SELECT COUNT(*)::BIGINT FROM (SELECT time_bucket('1 hours', timestamp) tb FROM otel_logs_and_spans WHERE {window} \
             GROUP BY 1 HAVING COUNT(duration) = 0)"
        ),
    )
    .await?;
    assert!(all_null_buckets > 0, "the fixture must contain a bucket whose rows ALL have a null duration, got {all_null_buckets}");

    let query = format!(
        "SELECT time_bucket('1 hours', timestamp) AS tb, \
                COALESCE(approx_percentile(0.95, percentile_agg(CAST(duration AS DOUBLE PRECISION))), 0) AS p95 \
         FROM otel_logs_and_spans WHERE {window} AND duration IS NOT NULL GROUP BY 1 ORDER BY 1"
    );

    let ctx = ctx_for(&db)?;

    let row = |b: &RecordBatch, r: usize| (ts_at(b, 0, r), f64_at(b, 1, r).round() as i64);

    let routed = rows_of(&routed_hybrid(&ctx, &query, "the p95 chart must route, or this proves nothing about the null guard").await?, row);

    let raw = rows_of(&db.query_delta_only(&query).await?, row);
    assert_eq!(routed, raw, "the guarded p95 must match the raw aggregate bucket for bucket");
    assert!(!routed.is_empty(), "the fixture must produce buckets, or equality is vacuous: {routed:?}");
    Ok(())
}

/// `count(*)` under a null guard ROUTES, and answers from `duration_count`.
///
/// Under `col IS NOT NULL` consumed as a guard rather than pushed, `count(*)` is
/// exactly `count(col)`, which `duration_count` declares. (`count(*)` *alongside*
/// a percentile is a different query and is still refused — see
/// `a_guarded_count_beside_a_percentile_still_declines` in `src/rollup.rs`.)
#[serial]
#[tokio::test]
async fn a_count_star_under_a_null_guard_routes_via_duration_count() -> Result<()> {
    let env = rollup_env("rollup_null_guard_count").await?;
    let (db, project_id) = (Arc::clone(&env.db), env.project_id.clone());
    db.cancel_maintenance();

    // Spread across >= 4h: the interior must clear `MIN_INTERIOR_BUCKETS = 2`
    // grains (2h at the 1h tier) or `TinyInterior` declines first.
    for (i, (duration, offset)) in
        [(Some(100i64), 17i64), (None, 61_000_000), (Some(300), 3_661_000_000), (Some(400), 7_261_000_000), (Some(500), 14_461_000_000)].iter().enumerate()
    {
        env.insert(&format!("y{i}"), env.yesterday_noon + offset, Some("cart"), *duration, "count fixture").await?;
    }
    env.certify_and_drain().await?;

    // Bound to YESTERDAY, where the tier is certified: reaching into the live
    // frontier makes the interior a tiny slice of the window and `TinyInterior`
    // declines before the guarded count is ever considered.
    let (lo, hi) = (env.yesterday_noon + 17, env.midnight);
    let query = format!(
        "SELECT time_bucket('1 hours', timestamp) AS tb, COUNT(*) AS c \
         FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
           AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi}) \
           AND duration IS NOT NULL GROUP BY 1 ORDER BY 1"
    );

    let ctx = ctx_for(&db)?;
    let why = "count(*) under a null guard is exactly count(duration), which duration_count declares; it must route";
    let routed = routed_any(&ctx, &query, why).await?;

    let total = |batches: &[RecordBatch]| rows_of(batches, |b, r| i64_at(b, 1, r)).into_iter().sum::<i64>();
    assert_eq!(
        total(&routed),
        total(&db.query_delta_only(&query).await?),
        "the ROUTED answer must equal raw — this is what proves duration_count is the right measure"
    );
    Ok(())
}

/// `dcount(resource.service.name)`, which lowers to
/// `distinct_count(approx_count_distinct(col))`, must agree with the raw sketch
/// both as a bare scalar and bucketed.
///
/// `resource___service___name` is the one dcount column that could route on the
/// measure alone: it is a declared dimension, so its `IS NOT NULL` becomes a row
/// filter. A dcount over a NON-dimension would additionally need a
/// `{agg: count, column: …}` guard measure.
///
/// Equality is exact, not approximate: HLL merge is register-wise max and so
/// associative, making the hybrid's merged sketch identical to the raw one.
#[serial]
#[tokio::test]
async fn a_distinct_count_over_services_routes_and_matches_the_raw_sketch() -> Result<()> {
    let env = rollup_env("rollup_dcount").await?;
    let db = Arc::clone(&env.db);
    db.cancel_maintenance();

    // Services REPEAT, so a distinct count differs from a row count; one
    // null-service row exercises the `IS NOT NULL` row filter on both legs.
    let fixture = [
        (Some("cart"), 17i64),
        (Some("cart"), 61_000_000),
        (Some("checkout"), 130_000_000),
        (None, 3_661_000_000),
        (Some("cart"), 3_700_000_000),
        (Some("search"), 7_330_000_000),
    ];
    for (i, (service, offset)) in fixture.iter().enumerate() {
        env.insert(&format!("y{i}"), env.yesterday_noon + offset, *service, Some(100), "dcount fixture").await?;
    }

    assert!(env.certify_and_drain().await? > 0, "eligible yesterday slices must be drained");

    for (i, (service, offset)) in [(Some("cart"), 5_000_000i64), (Some("payments"), 3_700_000_000)].iter().enumerate() {
        env.insert(&format!("t{i}"), env.midnight + offset, *service, Some(100), "dcount fixture").await?;
    }

    let window = format!("{} AND resource___service___name IS NOT NULL", env.window());

    let (rows, services) =
        delta_pair(&db, &format!("SELECT COUNT(*)::BIGINT, COUNT(DISTINCT resource___service___name)::BIGINT FROM otel_logs_and_spans WHERE {}", env.window()))
            .await?;
    assert!(services > 1 && services < rows, "the fixture must repeat services and hold a null one, got {services} distinct over {rows} rows");

    let ctx = ctx_for(&db)?;
    let show = |batches: Vec<datafusion::arrow::array::RecordBatch>| {
        let kept = batches.into_iter().filter(|b| b.num_rows() > 0).collect::<Vec<_>>();
        assert!(!kept.is_empty(), "an empty answer makes the parity assertion vacuous");
        datafusion::arrow::util::pretty::pretty_format_batches(&kept).expect("format").to_string()
    };

    for select in [
        "distinct_count(approx_count_distinct(resource___service___name))::float AS dcount FROM otel_logs_and_spans WHERE {window}".to_string(),
        "time_bucket('1 hours', timestamp) AS tb, distinct_count(approx_count_distinct(resource___service___name))::float AS dcount \
         FROM otel_logs_and_spans WHERE {window} GROUP BY 1 ORDER BY 1"
            .to_string(),
    ] {
        let query = format!("SELECT {}", select.replace("{window}", &window));
        let before = any_rollup_hits();
        let routed = show(ctx.sql(&query).await?.collect().await?);
        // `service_name_hll` is declared but on `MEASURES_NOT_YET_SERVABLE`:
        // `distinct_count` of an EMPTY sketch is 0, not NULL, so a routed widget
        // over cells missing the state would silently render 0 services. Until
        // the guard is lifted the query must fall back to raw and still be exact.
        assert_eq!(any_rollup_hits(), before, "the hll measure is on the not-yet-servable list, so this must NOT route: {query}");
        assert_eq!(routed, show(db.query_delta_only(&query).await?), "and the raw answer must still be exact: {query}");
    }
    Ok(())
}

/// The base (1-minute) and derived (1-hour) dashboard tiers.
const TIER_1M: &str = "otel_logs_and_spans_rollup_dashboard_1m_v3";
const TIER_1H: &str = "otel_logs_and_spans_rollup_dashboard_1h_v2";

/// ONE dedup pass over `otel_logs_and_spans`. The pass COUNT is load-bearing —
/// only a 0-drop pass over an unmoved file set certifies — so callers repeat it
/// deliberately rather than getting two passes by default.
async fn sweep_once(db: &Arc<Database>) -> Result<()> {
    let table_ref = table_of(db, "otel_logs_and_spans").await;
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
    Ok(())
}

/// How many rows a `query_delta_only` scan returns — the path where `DedupExec`
/// (and the skip that removes it) actually runs.
async fn delta_row_count(db: &Arc<Database>, sql: &str) -> Result<usize> {
    Ok(db.query_delta_only(sql).await?.iter().map(|b| b.num_rows()).sum())
}

/// One arm of a COUNT-parity run over a read-side dedup skip.
///
/// `configure` sets the flag under test (the control arm sets it OFF).
/// `duplicated` keys are committed TWICE across separate flushes — cross-file
/// duplicates, which flush-time per-bucket dedup cannot see — plus `unique` keys
/// committed once, then two certifying sweeps. A non-zero `churn` adds a later,
/// DISJOINT band written twice AFTER certification, moving the partition
/// fingerprint so an all-or-nothing skip must decline while a per-file skip may
/// still fire for the certified band.
///
/// Returns the scanned rows and how far `metric` moved. Rows, NOT `count(*)`: a
/// bare count is answered by `count_pushdown` from Delta statistics without ever
/// building a scan, so it exercises neither `DedupExec` nor the skip.
async fn dedup_skip_parity_arm(
    tag: &'static str, configure: impl FnOnce(&mut timefusion::config::AppConfig), duplicated: usize, unique: usize, churn: usize, metric: &'static str,
) -> Result<(i64, u64)> {
    // A second apart, so the churn batch's file span cannot touch the certified
    // band's; overlapping bands would (correctly) refuse the skip.
    const CHURN_OFFSET: i64 = 1_000_000;
    const CHURN_KEY_BASE: usize = 1_000;
    let ts = yesterday_noon();
    let (db, project_id) = tuned_db(tag, configure).await?;

    for _ in 0..2 {
        commit_span_run(&db, &project_id, ts, 0, duplicated).await?;
    }
    commit_span_run(&db, &project_id, ts, duplicated, duplicated + unique).await?;

    // TWICE. `record_certification` requires `dropped == 0` over an unmoved file
    // set, so the rewriting first pass cannot certify; the second confirms it and
    // without it the skip never engages.
    sweep_once(&db).await?;
    sweep_once(&db).await?;

    // The churn is itself duplicated across two flushes, so the uncertified leg
    // genuinely still has work to do.
    let churn_flushes = if churn > 0 { 2 } else { 0 };
    for _ in 0..churn_flushes {
        commit_span_run(&db, &project_id, ts + CHURN_OFFSET, CHURN_KEY_BASE, CHURN_KEY_BASE + churn).await?;
    }

    // TIME-BOUNDED: without a window `dedup_skip_allowed` returns `NoWindow` and
    // refuses before consulting any certification.
    let hi = ts + if churn > 0 { CHURN_OFFSET + (CHURN_KEY_BASE + churn) as i64 + 1 } else { (duplicated + unique) as i64 + 1 };
    let sql = window_select("id", &project_id, ts - 1, hi);
    // Warm the fast-resolve cache: `pre_skip_dedup` consults `try_fast_resolve`
    // and a miss declines outright, so a cold first query never reaches the skip.
    db.query_delta_only(&sql).await?;
    let before = counter_value(metric);
    let rows = delta_row_count(&db, &sql).await? as i64;
    Ok((rows, counter_value(metric) - before))
}

/// The same chart wrapped in a derived table ROUTES, and answers what raw does.
///
/// Walking through a `SubqueryAlias` is only sound if the alias's
/// re-qualification is undone on the way back.
#[serial]
#[tokio::test]
async fn a_chart_under_a_derived_table_routes_and_agrees_with_raw() -> Result<()> {
    let env = rollup_env("rollup_derived_table").await?;
    let db = Arc::clone(&env.db);
    db.cancel_maintenance();
    let project_id = env.project_id.clone();

    // Rollup coverage extent is a function of `now`, so pin the clock or the
    // outcome depends on the hour the suite started. The env's guard restores it.
    timefusion::support::set_micros(env.midnight + 12 * 3_600_000_000);

    for (i, (status, offset)) in [("OK", 17i64), ("ERROR", 61_000_000), ("OK", 3_661_000_000)].iter().enumerate() {
        env.insert_row(&format!("y{i}"), env.yesterday_noon + offset, Some("cart"), Some(100 + i as i64), status, "derived table fixture").await?;
    }
    env.certify_and_drain().await?;

    let (lo, hi) = (env.yesterday_noon + 17, env.midnight + 7_200_000_017);
    // Grouped chart under a derived table: the alias qualifies both the bucket's
    // argument and the grouped dimension.
    let query = format!(
        "SELECT time_bucket('1 hours', t.timestamp) AS tb, t.status_code, count(*) AS c \
         FROM (SELECT timestamp, status_code, project_id FROM otel_logs_and_spans \
               WHERE project_id = '{project_id}' \
                 AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi})) t \
         GROUP BY 1, 2 ORDER BY 1, 2"
    );

    let ctx = ctx_for(&db)?;
    let routed = routed_any(&ctx, &query, "a derived table only re-qualifies; the chart under it must route").await?;

    let rows = |batches: &[RecordBatch]| rows_of(batches, |b, r| (str_at(b, 1, r), i64_at(b, 2, r)));
    assert_eq!(rows(&routed), rows(&db.query_delta_only(&query).await?), "the ROUTED answer must equal raw, bucket for bucket and group for group");
    Ok(())
}

/// A partly covered window must union the rollup with raw and match raw exactly,
/// AND must really route — a miss also returns the right answer, so without the
/// routing assertion the test passes vacuously.
///
/// Coverage is made partial deterministically: yesterday is certified, today is
/// written afterwards and never certified.
#[serial]
#[tokio::test]
async fn a_partly_covered_window_unions_the_rollup_with_raw_and_matches_the_raw_answer() -> Result<()> {
    // A background coordinator would race the explicit coverage published below.
    let env = rollup_env("rollup_hybrid").await?;
    let db = Arc::clone(&env.db);
    db.cancel_maintenance();
    let (project_id, midnight, yesterday_noon) = (env.project_id.clone(), env.midnight, env.yesterday_noon);

    // Varied duration/service so avg/min/max and the group set are non-trivial.
    // Spread over six hours and starting mid-bucket: the hybrid cost guard
    // declines a rollup covering less than 20% of the requested window.
    for (i, offset) in
        [17i64, 61_000_000, 130_000_000, 610_000_000, 3_661_000_000, 7_330_000_000, 10_810_000_000, 14_410_000_000, 18_010_000_000].iter().enumerate()
    {
        env.insert(&format!("y{i}"), yesterday_noon + offset, Some("cart"), Some(100 + i as i64 * 10), "rollup hybrid fixture").await?;
    }

    assert!(env.certify_and_drain().await? > 0, "eligible yesterday slices must be drained");

    // Written AFTER certification: today has no coverage, so these are reachable
    // only through the raw leg.
    for (i, offset) in [5_000_000i64, 3_600_000_000].iter().enumerate() {
        env.insert(&format!("t{i}"), midnight + offset, Some("checkout"), Some(500 + i as i64 * 10), "rollup hybrid fixture").await?;
    }

    // Unaligned on both ends, like a dashboard's `now`-relative window.
    let (lo, hi) = (yesterday_noon + 17, midnight + 7_200_000_017);
    let query = format!(
        "SELECT resource___service___name AS svc, COUNT(*) AS c, avg(duration) AS mean, min(duration) AS lo, max(duration) AS hi \
         FROM otel_logs_and_spans WHERE {} GROUP BY 1 ORDER BY 1",
        env.window()
    );

    let ctx = ctx_for(&db)?;

    // Precondition: without this the routing assertion below cannot distinguish
    // "the rewrite is broken" from "there was nothing to rewrite to".
    let built = env.tier_rows(TIER_1M).await?;
    // On failure only, dump the tier: a double count shows either one bucket with
    // `request_count = 2` or two live `rollup_generation`s over the same bucket,
    // and the counter alone cannot tell those apart.
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
    let base_target = db.unified_tables().read().await.get(TIER_1M).expect("base rollup table created").clone();
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
    // Every published file must carry its slice tags — a derived unit selects its
    // input by them, so an untagged file is invisible to the coarse tier. The
    // invariant is "none untagged", not a file count: sealed slices get coarsened.
    let total_files = base_target.read().await.snapshot()?.log_data().iter().count();
    assert!(
        tagged_files > 0 && tagged_files == total_files,
        "every published slice must retain its Delta Add tags (got {tagged_files} tagged of {total_files})"
    );
    let derived_built = env.tier_rows(TIER_1H).await?;
    assert_eq!(derived_built, 9, "derived coverage must merge every independently published base slice");

    let row = |b: &RecordBatch, r: usize| (str_at(b, 0, r), i64_at(b, 1, r), f64_at(b, 2, r), i64_at(b, 3, r), i64_at(b, 4, r));

    let why = "the query must be served from the rollup as a hybrid union, not a raw scan";
    let hybrid = rows_of(&routed_hybrid(&ctx, &query, why).await?, row);

    // `query_delta_only` bypasses both the rollup and the buffer, so it is the
    // authority here.
    let raw = rows_of(&db.query_delta_only(&query).await?, row);

    assert_eq!(hybrid, raw, "the hybrid rewrite must equal the raw aggregate exactly");
    assert_eq!(hybrid.len(), 2, "both services must survive, including the one that exists only in the raw tail: {hybrid:?}");
    assert_eq!(hybrid.iter().map(|row| row.1).sum::<i64>(), 11, "every fixture row must be counted exactly once: {hybrid:?}");

    // THE SAME WINDOW WITH NO UPPER BOUND. With an open tail `hi` is a plan-time
    // stand-in; if the trailing raw range closed at it the rewrite would silently
    // drop the newest rows. This row sits PAST the bounded window, so only an open
    // tail can reach it.
    env.insert("t_open", hi + 1_000_000, Some("checkout"), Some(700), "rollup hybrid fixture").await?;
    // Keep the open-ended plan-time upper bound deterministic: on the wall clock
    // the covered interior can fall below the 20% hybrid cost threshold depending
    // on the hour of day.
    timefusion::support::set_micros(hi + 3_600_000_000);
    let open = query.replace(&format!(" AND timestamp < to_timestamp_micros({hi})"), "");
    let open_rows = rows_of(&routed_hybrid(&ctx, &open, "an open-ended window must route, not fall back to a raw scan").await?, row);
    assert_eq!(open_rows, rows_of(&db.query_delta_only(&open).await?, row), "the open-ended rewrite must equal the raw aggregate exactly");
    assert_eq!(open_rows.iter().map(|row| row.1).sum::<i64>(), 12, "the open tail must reach the row past the bounded window: {open_rows:?}");

    // A CAST over an aggregate plus `ORDER BY <aggregate> LIMIT n` optimizes to
    // `Projection(Sort(Projection(Aggregate)))`. Where the optimizer puts that
    // Sort depends on the session's analyzer rules, so this must run on a real
    // `Database` session; a bare `SessionContext` cannot reproduce it. `LIMIT 1`
    // is deterministic: the top bucket holds 4 rows, the other two hold 1 each.
    let shaped = format!(
        "SELECT COUNT(*) AS c, avg(duration)::BIGINT AS m FROM otel_logs_and_spans \
         WHERE project_id = '{project_id}' AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi}) \
         GROUP BY time_bucket('1 hours', timestamp) ORDER BY 1 DESC LIMIT 1"
    );
    let counts = |b: &RecordBatch, r: usize| (i64_at(b, 0, r), i64_at(b, 1, r));
    let routed = rows_of(&routed_hybrid(&ctx, &shaped, "the shape that defeated every peeling matcher must route").await?, counts);
    assert_eq!(routed, rows_of(&db.query_delta_only(&shaped).await?, counts), "the substituted plan must equal the raw answer exactly");

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
    let after_update = rows_of(&routed_hybrid(&ctx, &query, "a DML confined to today must leave yesterday's coverage intact").await?, row);
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
    let routed_golden = routed_hybrid(&ctx, &golden, "the promoted row filter must route").await?;
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
    let after_by_id = rows_of(&routed_hybrid(&ctx, &query, "an id-scoped DML on today's row must leave yesterday's coverage intact").await?, row);
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
    let derived_before = env.tier_rows(TIER_1H).await?;
    env.insert("y-late", yesterday_noon + 90_000_000, Some("cart"), Some(999), "rollup hybrid fixture").await?;
    env.certify().await?;
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
    let derived_after = env.tier_rows(TIER_1H).await?;
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
    // D-5 and D-4 are sealed and far outside the dedup lookback (default 1).
    let (cfg, env) = backfill_env("rollup_backfill").await?;
    let db = Arc::clone(&env.db);
    assert_eq!(env.tier_rows(TIER_1M).await?, 0, "the sweep must NOT reach sealed days — that gap is what the backfill exists to close");

    // Units run, not days built — the coordinator splits a day across slices,
    // so only the coverage assertion below states the property. This one just
    // stops the test passing vacuously on a drain that claimed nothing.
    assert!(plan_and_drain_backfill(&db).await? > 0, "the backfill must actually run units");
    assert_eq!(env.tier_rows(TIER_1M).await?, 2, "both sealed spans must be rolled up");

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
    let restarted = Arc::new(Database::with_config(cfg).await?);
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
    let (_cfg, env) = backfill_env("rollup_cov_producer").await?;
    // Guard against a vacuous verdict: an empty drain would "prove" the defect
    // without ever exercising the commit path that should record coverage.
    assert!(plan_and_drain_backfill(&env.db).await? > 0, "no rollup units ran, so this says nothing about coverage recording");
    assert!(env.db.rollup_coverage_entries() > 0, "a committed rollup must record DATE-level coverage, not only slice coverage");
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
    let ts = yesterday_noon();
    let cfg = tuned_cfg("cert_restart", |c| {
        c.maintenance.timefusion_read_dedup_skip_swept = true;
        c.maintenance.timefusion_dedup_certification_persist = true;
    });
    let project_id = new_project_id();
    let sql = window_select("name", &project_id, ts - 1, ts + 1_000);

    {
        let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
        commit_span_run(&db, &project_id, ts, 0, 40).await?;
        sweep_once(&db).await?;
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
    let ts = yesterday_noon();
    // No rollups: they bypass the version guard entirely (`needs_rollup_retry`),
    // which would hide the regression this test exists for.
    let (db, project_id) = skip_swept_db("dedup_confirming_pass").await?;

    // The same keys twice, in separate flushes: cross-file duplicates, so the
    // first pass must rewrite and therefore cannot certify.
    for _ in 0..2 {
        commit_span_run(&db, &project_id, ts, 0, 40).await?;
    }
    let certs = || counter_value(scan_metric_names::CERT_GRANTED_TOTAL);

    sweep_once(&db).await?;
    assert_eq!(certs(), 0, "a pass that rewrites must not certify what it just rewrote");

    // No write in between — that is the whole point.
    sweep_once(&db).await?;
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
    let ts = yesterday_noon();
    let (db, project_id) = skip_swept_db("dedup_denial_split").await?;
    commit_span_run(&db, &project_id, ts, 0, 40).await?;

    let sql = window_select("name", &project_id, ts - 1, ts + 1_000);
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

    sweep_once(&db).await?;
    db.query_delta_only(&sql).await?;
    let certified = counts();
    assert_eq!(certified.2 - never.2, 1, "a swept partition must actually grant the skip, or the split below measures nothing");

    // A commit into the certified partition moves its fingerprint. This is the
    // irreducible denial — no persistence layer recovers it.
    commit_span_run(&db, &project_id, ts, 40, 80).await?;
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
    let ts = yesterday_noon();
    let (db, project_id) = skip_swept_db("dedup_empty_window").await?;

    // Rows for ONE day, then query a window whose dates are all EARLIER — so
    // every date resolves to no files for this project and the loop skips them
    // all without ever consulting a certification.
    commit_span_run(&db, &project_id, ts, 0, 40).await?;

    let sql = window_select("name", &project_id, ts - 10 * 86_400_000_000, ts - 7 * 86_400_000_000);
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

    let arm = |per_file: bool, tag: &'static str| {
        dedup_skip_parity_arm(
            tag,
            move |cfg| cfg.maintenance.timefusion_read_dedup_skip_per_file = per_file,
            DUPLICATED,
            UNIQUE,
            CHURN,
            scan_metric_names::DEDUP_SKIPPED_PER_FILE,
        )
    };
    let (authoritative, control_skips) = arm(false, "per_file_parity_off").await?;
    let (with_skip, skips) = arm(true, "per_file_parity_on").await?;

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

    // One dataset, two engines: skip disabled (the authority) and enabled.
    let arm = |skip: bool, tag: &'static str| {
        dedup_skip_parity_arm(tag, move |cfg| cfg.maintenance.timefusion_read_dedup_skip_swept = skip, DUPLICATED, UNIQUE, 0, scan_metric_names::DEDUP_SKIPPED)
    };
    let (authoritative, control_skips) = arm(false, "count_parity_dedup_on").await?;
    let (skipped, skips) = arm(true, "count_parity_skip_on").await?;

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
    let (db, project_id) = plain_db("count_star_multi_day").await?;

    // Noon on each of the last DAYS days: distinct `date=` partitions, and far
    // enough from midnight that a UTC rollover mid-test cannot move a row.
    for back in 1..=DAYS {
        let base = noon_days_ago(back);
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
        let (lo, hi) = (noon_days_ago(back) - 1, noon_days_ago(1) + PER_DAY as i64 + 1);

        // The authority is the scan: the rows `SELECT id` actually returns,
        // after DedupExec has collapsed the versions. `count(*)` must equal it.
        let scanned = delta_row_count(&db, &window_select("id", &project_id, lo, hi)).await? as i64;
        let counted = delta_scalar(&db, &window_select("count(*)", &project_id, lo, hi)).await?;

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
    let (recent_ts, older_ts) = (noon_days_ago(0), noon_days_ago(3));

    let run = |per_date: bool| async move {
        let (db, project_id) = tuned_db(&format!("per_date_skip_{per_date}"), |c| {
            c.maintenance.timefusion_read_dedup_skip_swept = true;
            c.maintenance.timefusion_read_dedup_skip_per_date = per_date;
        })
        .await?;
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
        sweep_once(&db).await?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("older_key", older_ts, "final")?], true, None).await?;

        let sql = format!(
            "SELECT array_element(hashes, 1) AS tag FROM otel_logs_and_spans WHERE project_id = '{project_id}' \
             AND timestamp >= to_timestamp_micros({lo}) AND timestamp <= to_timestamp_micros({hi}) ORDER BY tag",
            lo = older_ts - 1,
            hi = recent_ts + 1,
        );
        let mut rows = col0_strings(&db.query_delta_only(&sql).await?);
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
    let base = TestConfigBuilder::new("pruned_to_nothing").with_buffer_mode(BufferMode::FlushImmediately).build();
    let (db, project_id) = db_of(tweak(base, |c| {
        c.maintenance.timefusion_file_bloom_pruning = true;
        c.maintenance.timefusion_read_dedup_skip_swept = true;
        c.maintenance.timefusion_read_dedup_skip_per_date = true;
    }))
    .await?;
    let ts = chrono::Utc::now().timestamp_micros();
    commit_span_run(&db, &project_id, ts, 0, 8).await?;
    // Blooms are what prune whole files; without the sidecars the scan keeps
    // every file and the leg is never emptied.
    db.bloom_sidecar_reconcile().await?;

    let sql = format!(
        "SELECT name FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND id = 'no-such-id-anywhere' \
         AND timestamp >= to_timestamp_micros({}) AND timestamp <= to_timestamp_micros({})",
        ts - 1_000,
        ts + 1_000,
    );
    assert_eq!(delta_row_count(&db, &sql).await?, 0, "the needle is in no file — and getting there must not panic");
    Ok(())
}

/// A DV scan must retain physical row positions until its mask is consumed,
/// then discard out-of-window rows before the read-side dedup buffers them.
#[serial]
#[tokio::test]
async fn dv_window_filters_before_read_dedup() -> Result<()> {
    let (db, project_id) = buffered_db("dv_window_filter").await?;
    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    write_to(&db, "mor_dormant", &project_id, (0..30).map(|i| mor_row(&format!("k{i}"), "v", &project_id, ts + i * 1_000_000, None)).collect(), true).await?;
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    ctx.sql(&format!("DELETE FROM mor_dormant WHERE project_id = '{project_id}' AND id IN ('k2', 'k12')")).await?.collect().await?;
    let table = db.resolve_table(&project_id, "mor_dormant").await?;
    assert!(
        table.read().await.snapshot()?.snapshot().log_data().iter().any(|f| f.deletion_vector_descriptor().is_some()),
        "fixture must contain a real deletion vector"
    );
    let at = |offset: i64| chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts + offset * 1_000_000).unwrap().format("%Y-%m-%d %H:%M:%S%.f").to_string();
    let sql = format!("SELECT id FROM mor_dormant WHERE project_id = '{project_id}' AND timestamp >= '{}' AND timestamp < '{}'", at(10), at(15));
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let batches = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx()).await?;
    let mut ids = col0_strings(&batches);
    ids.sort();
    assert_eq!(ids, ["k10", "k11", "k13", "k14"], "filtering must not shift deletion-vector row positions");
    let dedup = find_node(&plan, "DedupExec").expect("fixture must exercise read-side dedup");
    let input_rows = dedup.metrics().unwrap().sum_by_name("input_rows").unwrap().as_usize();
    assert_eq!(input_rows, 4, "out-of-window rows reached dedup: {}", rendered(&plan));
    Ok(())
}
