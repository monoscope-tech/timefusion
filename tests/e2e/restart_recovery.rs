//! Restart recovery: simulates a process crash by shutting down the
//! BufferedWriteLayer and re-bootstrapping against the same MinIO bucket +
//! data_dir. Asserts:
//!   - rows that were flushed to Delta pre-restart are still queryable
//!   - rows that lived only in WAL+MemBuffer pre-restart are restored via
//!     WAL replay

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at, insert_for};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn flushed_rows_survive_restart() -> anyhow::Result<()> {
    let mut env = E2eEnv::builder().start().await?;
    {
        let client = env.pg_client().await?;
        for i in 0..5 {
            insert_at(&client, &format!("f-{i}"), FROZEN_START_MICROS).await?;
        }
        let stats = env.force_flush().await?;
        assert!(stats.buckets_flushed > 0, "expected at least one bucket flushed, got {stats:?}");
        // Client must drop before restart so the pgwire shutdown notify
        // doesn't fight in-flight queries.
    }

    env.restart().await?;

    let client = env.pg_client().await?;
    let count: i64 = tokio::time::timeout(
        Duration::from_secs(10),
        client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]),
    )
    .await
    .map_err(|_| anyhow::anyhow!("post-restart SELECT timed out"))??
    .get(0);
    assert_eq!(count, 5, "flushed rows lost across restart");
    Ok(())
}

// Formerly #[ignore]d as a "harness-side gap": the second-pass read returned
// zero entries though the WAL held the inserts. That was not harness-side —
// it was the 2026-07-08 acked-write-loss bug itself: replay's retention
// cutoff compared virtual-clock `now` (frozen at ~2030 here) against the
// real-clock WAL stamps, so every entry looked aged-out and was
// checkpoint-consumed without being applied. With the cutoff removed (the
// persisted cursor is the replay boundary), this passes and guards the
// production crash class end-to-end.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn unflushed_rows_replayed_from_wal() -> anyhow::Result<()> {
    // Disable Foyer; push flush/eviction far into the future so the
    // background tasks can't advance the WAL cursor past our writes
    // before we crash. The assertion is strictly about WAL replay.
    let mut env = E2eEnv::builder()
        .with_foyer_disabled()
        .with_flush_interval(Duration::from_secs(3600))
        .with_eviction_interval(Duration::from_secs(3600))
        .start()
        .await?;
    {
        let client = env.pg_client().await?;
        for i in 0..3 {
            insert_at(&client, &format!("w-{i}"), FROZEN_START_MICROS).await?;
        }
        // Deliberately do NOT call force_flush — rows are only in WAL+MemBuffer.
        let stats = env.snapshot_stats();
        assert!(stats.mem_total_rows >= 3, "expected rows in MemBuffer pre-crash, got {stats:?}");
    }

    // Wait long enough for the 200ms WAL fsync schedule to flush writes to
    // disk — otherwise crash_for_test() may drop unfsynced bytes.
    tokio::time::sleep(Duration::from_millis(400)).await;

    env.restart().await?;

    let stats = env.snapshot_stats();
    assert!(
        stats.mem_total_rows >= 3,
        "WAL replay did not restore rows into MemBuffer; post-restart stats={stats:?}"
    );

    let client = env.pg_client().await?;
    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 3, "rows lost across restart — WAL replay broken");
    Ok(())
}

/// Cold-start latency benchmark. Builds Delta + WAL history, dirty-crashes,
/// re-bootstraps, and asserts the second bootstrap completes in seconds — the
/// prod symptom we're targeting is "FATAL: the database system is starting up"
/// being returned for minutes after a dirty restart (cursor-snapshot missing
/// or stale). With `bootstrap()` now mirroring main.rs's cursor reconcile,
/// this test exercises the same slow path prod hits.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn cold_start_under_five_seconds() -> anyhow::Result<()> {
    let mut env = E2eEnv::builder().start().await?;
    {
        let client = env.pg_client().await?;
        // Stress what actually scales in derive_wal_cursors_from_delta:
        //   - many (project, table) topic pairs → many Delta tables to open + scan
        //   - several flush rounds per project → real commit history depth
        //   - a final un-flushed batch per project → WAL replay has actual work
        const PROJECTS: usize = 10;
        const FLUSHED_ROUNDS: usize = 3;
        for round in 0..FLUSHED_ROUNDS {
            for p in 0..PROJECTS {
                for i in 0..5 {
                    insert_for(&client, &format!("p-{p}"), &format!("c-{round}-{i}"), FROZEN_START_MICROS).await?;
                }
            }
            env.force_flush().await?;
        }
        // One more batch per project that we deliberately do NOT flush — so
        // the WAL has un-replayed entries past the Delta watermark on crash.
        for p in 0..PROJECTS {
            for i in 0..5 {
                insert_for(&client, &format!("p-{p}"), &format!("u-{i}"), FROZEN_START_MICROS).await?;
            }
        }
        // Let WAL fsync (200ms schedule) catch up before the dirty crash.
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    let t0 = std::time::Instant::now();
    env.restart().await?;
    let restart_elapsed = t0.elapsed();

    // 5s is the loose initial bar — prod is currently >1min so any honest
    // local reproduction will fail this until we cut the dominant phase.
    // Per-phase timing is logged from bootstrap.rs; grep for `bootstrap.phase=`.
    assert!(
        restart_elapsed < Duration::from_secs(5),
        "cold-start regression: re-bootstrap took {:?} (target <5s)",
        restart_elapsed
    );
    Ok(())
}
