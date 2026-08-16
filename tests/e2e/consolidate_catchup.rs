//! Sealed partitions must converge WITHOUT the daily cron.
//!
//! `consolidate_sealed_partitions` sweeps every cold date in one long job on a
//! 02:30 cron, so it only helps if the process lives through the whole sweep.
//! Prod restarts every 30-120 minutes and does not. Measured 2026-08-01, files
//! per partition per tenant:
//!
//!   07-28   07-29   07-30   07-31   08-01
//!      99      57      71    3515    2271   (tenant 87576849)
//!       4       1       1    3128    1782   (tenant 98fdd4f3)
//!
//! Everything the sweep reached collapsed to 1-99 files; the newest sealed day
//! never got there and stayed at ~3000. That is not cosmetic — file count arms
//! the wide-scan gate (see wide_scan_gate.rs) and drives untracked decode heap.
//!
//! `consolidate_catchup` does the same work in a bounded slice off the frequent
//! hot-compaction tick, so progress accrues across restarts.

use std::time::Duration;

use timefusion::clock;
use timefusion::test_utils::test_helpers::delta_physical_row_count;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at, insert_for};

async fn explain(client: &tokio_postgres::Client, sql: &str) -> anyhow::Result<String> {
    Ok(client
        .query(&format!("EXPLAIN {sql}"), &[])
        .await?
        .iter()
        .map(|r| (0..r.len()).map(|c| r.try_get::<_, String>(c).unwrap_or_default()).collect::<Vec<_>>().join(" | "))
        .collect::<Vec<_>>()
        .join("\n"))
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_sealed_partition_the_daily_sweep_missed_converges_on_the_frequent_tick() -> anyhow::Result<()> {
    let bucket_secs = 60u64;
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(bucket_secs)).with_retention(Duration::from_secs(60 * 60)).start().await?;
    let client = env.pg_client().await?;

    // Build a fragmented partition on a day that is already sealed once the
    // clock moves on: one flush per file, well inside the same UTC day.
    let sec = 1_000_000i64;
    let day_start = FROZEN_START_MICROS - (FROZEN_START_MICROS % (86_400 * sec));
    let base = day_start + 3_600 * sec;
    for f in 0..10i64 {
        for i in 0..3i64 {
            insert_at(&client, &format!("c-{f}-{i}"), base + f * 60 * sec + i * sec).await?;
        }
        // One key retried into every independent flush file. It must remain
        // logically one row and the sorted cold rewrite must collapse its ten
        // physical versions without disturbing the 30 distinct rows.
        insert_at(&client, "cross-file-dup", base).await?;
        env.advance(Duration::from_secs(bucket_secs * 2));
        env.force_flush().await?;
    }

    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let before = {
        let t = table_ref.read().await;
        t.snapshot()?.log_data().iter().count()
    };
    assert!(before >= 5, "fixture must leave a fragmented partition, got {before} file(s)");
    assert_eq!(delta_physical_row_count(&table_ref).await?, 40, "fixture must contain ten physical copies of the retry key");

    // Seal the day: move the clock into the NEXT UTC day so the partition is
    // cold (cold_optimize_after_days = 1). This is the state prod was in.
    clock::set_micros(day_start + 86_400 * sec + 6 * 3_600 * sec);

    // Bounded slices, exactly as the hot-compaction tick calls it. Several
    // ticks stand in for "the process restarted a few times".
    for _ in 0..8 {
        env.db().consolidate_catchup(&table_ref, "otel_logs_and_spans", 4).await?;
    }

    let after = {
        let t = table_ref.read().await;
        t.snapshot()?.log_data().iter().count()
    };
    assert!(
        after * 2 <= before,
        "a sealed, fragmented partition must converge from the frequent tick alone — the daily cron is exactly what \
         prod does not survive. Measured 10 -> 1 locally. before={before} after={after}"
    );

    // Convergence must be a fixed point, not a rewrite loop: once packed, more
    // ticks must not keep rewriting (that would burn IO and lose OCC races).
    let settled = {
        for _ in 0..3 {
            env.db().consolidate_catchup(&table_ref, "otel_logs_and_spans", 4).await?;
        }
        let t = table_ref.read().await;
        t.snapshot()?.log_data().iter().count()
    };
    assert_eq!(settled, after, "catch-up must reach a fixed point, not rewrite the same files every tick");
    assert_eq!(delta_physical_row_count(&table_ref).await?, 31, "sorted consolidation must physically collapse the cross-file retry versions");

    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 31, "consolidation must preserve every logical row exactly once");

    let plan = explain(&client, "SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp DESC LIMIT 10").await?;
    assert!(
        plan.contains("DedupExec: keys=[timestamp, id], mode=bounded"),
        "sorted consolidation must leave an ordering the read-side dedup can stream instead of buffering the full historical scan. Plan was:\n{plan}"
    );

    Ok(())
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn catchup_scores_actionable_project_debt_not_date_wide_file_count() -> anyhow::Result<()> {
    let bucket_secs = 60u64;
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(bucket_secs)).with_retention(Duration::from_secs(60 * 60)).start().await?;
    let client = env.pg_client().await?;
    let sec = 1_000_000i64;
    let day = 86_400 * sec;
    let day_start = FROZEN_START_MICROS - (FROZEN_START_MICROS % day);
    let (older, newer) = (day_start - day + 3_600 * sec, day_start + 3_600 * sec);

    // Five files on the newer date, but each belongs to a different tenant:
    // none is actionable because compaction cannot cross tenant partitions.
    for i in 0..5 {
        insert_for(&client, &format!("single-{i}"), &format!("single-{i}"), newer + i * sec).await?;
    }
    env.force_flush().await?;

    // Four files for one tenant on the older date: less date-wide debt, but
    // this is the only pair a tenant-scoped rewrite can actually shrink.
    for i in 0..4 {
        insert_for(&client, "fragmented", &format!("fragmented-{i}"), older + i * sec).await?;
        env.advance(Duration::from_secs(bucket_secs * 2));
        env.force_flush().await?;
    }
    clock::set_micros(day_start + 2 * day);

    let table_ref = env.db().resolve_table("fragmented", "otel_logs_and_spans").await?;
    let file_counts = || async {
        let table = table_ref.read().await;
        let uris: Vec<String> = table.get_file_uris().map(|it| it.collect()).unwrap_or_default();
        let older_marker = format!("project_id=fragmented/date={}", chrono::DateTime::from_timestamp_micros(older).unwrap().date_naive());
        let newer_marker = format!("date={}", chrono::DateTime::from_timestamp_micros(newer).unwrap().date_naive());
        (uris.iter().filter(|u| u.contains(&older_marker)).count(), uris.iter().filter(|u| u.contains(&newer_marker)).count())
    };
    let before = file_counts().await;
    assert_eq!(before, (4, 5), "fixture must reproduce date-wide false debt");

    env.db().consolidate_catchup(&table_ref, "otel_logs_and_spans", 4).await?;
    let after = file_counts().await;
    assert!(after.0 < before.0, "catch-up must shrink the actionable tenant/date pair: before={before:?} after={after:?}");
    assert_eq!(after.1, before.1, "one-file tenant partitions are already converged and must not be rewritten");
    Ok(())
}
