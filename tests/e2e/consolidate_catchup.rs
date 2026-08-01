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

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_sealed_partition_the_daily_sweep_missed_converges_on_the_frequent_tick() -> anyhow::Result<()> {
    let bucket_secs = 60u64;
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(bucket_secs))
        .with_retention(Duration::from_secs(60 * 60))
        .with_optimize_sort_by()
        .start()
        .await?;
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
        env.advance(Duration::from_secs(bucket_secs * 2));
        env.force_flush().await?;
    }

    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let before = {
        let t = table_ref.read().await;
        t.snapshot()?.log_data().iter().count()
    };
    assert!(before >= 5, "fixture must leave a fragmented partition, got {before} file(s)");

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

    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 30, "consolidation must not lose or duplicate rows");

    Ok(())
}
