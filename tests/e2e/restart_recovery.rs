//! Restart recovery: crash the BufferedWriteLayer and re-bootstrap against the
//! same bucket + data_dir, asserting flushed rows survive and unflushed rows
//! come back via WAL replay.

use std::time::Duration;

use super::harness::{E2eEnv, E2eEnvBuilder, FROZEN_START_MICROS, insert_at, insert_for};
use super::ordering_pushdown::count_rows;

async fn insert_n(client: &tokio_postgres::Client, prefix: &str, n: usize) -> anyhow::Result<()> {
    for i in 0..n {
        insert_at(client, &format!("{prefix}-{i}"), FROZEN_START_MICROS).await?;
    }
    Ok(())
}

/// Foyer off and flush/eviction pushed far out, so no background task can
/// advance the WAL cursor past our writes; every flush must be explicit.
fn quiesced() -> E2eEnvBuilder {
    E2eEnv::builder().with_foyer_disabled().with_flush_interval(Duration::from_secs(3600)).with_eviction_interval(Duration::from_secs(3600))
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn flushed_rows_survive_restart() -> anyhow::Result<()> {
    let mut env = E2eEnv::builder().start().await?;
    {
        let client = env.pg_client().await?;
        insert_n(&client, "f", 5).await?;
        let stats = env.force_flush().await?;
        assert!(stats.buckets_flushed > 0, "expected at least one bucket flushed, got {stats:?}");
        // Client must drop before restart: pgwire shutdown fights in-flight queries.
    }

    env.restart().await?;

    let client = env.pg_client().await?;
    let count = tokio::time::timeout(Duration::from_secs(10), count_rows(&client, "e2e_project"))
        .await
        .map_err(|_| anyhow::anyhow!("post-restart SELECT timed out"))??;
    assert_eq!(count, 5, "flushed rows lost across restart");
    Ok(())
}

// Guards acked-write loss: the persisted cursor, not a wall-clock retention
// cutoff, is the replay boundary (this env runs on a frozen virtual clock).
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn unflushed_rows_replayed_from_wal() -> anyhow::Result<()> {
    let mut env = quiesced().start().await?;
    {
        let client = env.pg_client().await?;
        insert_n(&client, "w", 3).await?;
        // Deliberately do NOT call force_flush — rows are only in WAL+MemBuffer.
        let stats = env.snapshot_stats();
        assert!(stats.mem_total_rows >= 3, "expected rows in MemBuffer pre-crash, got {stats:?}");
    }

    // Let the WAL fsync land; crash_for_test() drops unfsynced bytes.
    tokio::time::sleep(Duration::from_millis(400)).await;

    env.restart().await?;

    let stats = env.snapshot_stats();
    assert!(stats.mem_total_rows >= 3, "WAL replay did not restore rows into MemBuffer; post-restart stats={stats:?}");

    let client = env.pg_client().await?;
    assert_eq!(count_rows(&client, "e2e_project").await?, 3, "rows lost across restart — WAL replay broken");
    Ok(())
}

/// Cold-start latency benchmark: build Delta + WAL history, dirty-crash, and
/// assert the re-bootstrap completes in seconds rather than minutes.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn cold_start_under_five_seconds() -> anyhow::Result<()> {
    const PROJECTS: usize = 10;
    const FLUSHED_ROUNDS: usize = 3;
    let mut env = E2eEnv::builder().start().await?;
    {
        let client = env.pg_client().await?;
        // Stress what scales in derive_wal_cursors_from_delta: many topic pairs,
        // real commit-history depth, and un-replayed WAL entries.
        for round in 0..FLUSHED_ROUNDS {
            for p in 0..PROJECTS {
                for i in 0..5 {
                    insert_for(&client, &format!("p-{p}"), &format!("c-{round}-{i}"), FROZEN_START_MICROS).await?;
                }
            }
            env.force_flush().await?;
        }
        // Deliberately unflushed, so the WAL has entries past the Delta watermark.
        for p in 0..PROJECTS {
            for i in 0..5 {
                insert_for(&client, &format!("p-{p}"), &format!("u-{i}"), FROZEN_START_MICROS).await?;
            }
        }
        // Let the WAL fsync catch up before the dirty crash.
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    let t0 = std::time::Instant::now();
    env.restart().await?;
    let restart_elapsed = t0.elapsed();

    // Per project: no tenant may inherit a co-tenant's cursor, and both the
    // flushed prefix and the unflushed WAL tail must survive.
    let client = env.pg_client().await?;
    for p in 0..PROJECTS {
        let project = format!("p-{p}");
        let count = count_rows(&client, &project).await?;
        assert_eq!(count, (FLUSHED_ROUNDS * 5 + 5) as i64, "dirty restart lost or duplicated rows for {project}");
    }

    // Isolated this is ~4s; the bar is 30s because suite parallelism inflates
    // wall-clock ~3x, and it still catches the minutes-class regression.
    assert!(restart_elapsed < Duration::from_secs(30), "cold-start regression: re-bootstrap took {:?} (in-suite bar <30s; isolated bar ~5s)", restart_elapsed);
    Ok(())
}

/// The landed-batch skip end to end: commit writes `timefusion.landed_digests`
/// -> boot scan installs it into the layer -> the re-flush of the replayed rows
/// is declined.
///
/// The duplicate must be produced by the drop-cursor-advance hook, not by
/// re-sending rows: `otel_logs_and_spans` is `version_append`, so an inbound
/// write gets a fresh `updated_at` and is genuinely different content. Only WAL
/// replay preserves the durable stamp, so a client cannot spoof a landed identity.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn replayed_rows_that_delta_already_holds_are_not_written_again() -> anyhow::Result<()> {
    let mut env = quiesced().with_landed_skip().start().await?;
    {
        let client = env.pg_client().await?;
        insert_n(&client, "ld", 5).await?;
        // The commit lands; the advance that should follow it does not.
        env.buffered_layer().set_drop_cursor_advance_for_test(true);
        let stats = env.force_flush().await?;
        assert!(stats.buckets_flushed > 0, "nothing was committed, so there is no landed identity to find: {stats:?}");
    }
    tokio::time::sleep(Duration::from_millis(400)).await;

    env.restart().await?;

    // Rows already durable in Delta are back in MemBuffer, queued to be written again.
    let stats = env.snapshot_stats();
    assert!(stats.wal_replay_rows >= 5, "replay did not re-insert the committed rows, so there is no duplicate to decline: {stats:?}");
    assert_eq!(stats.landed_skips_total, 0, "a fresh process has skipped nothing yet");

    let after = env.force_flush().await?;
    let stats = env.snapshot_stats();
    assert!(
        stats.landed_skips_total > 0,
        "the re-write of already-committed rows was NOT declined — the identity did not survive Delta metadata -> boot scan -> skip (flush={after:?}, stats={stats:?})"
    );

    let client = env.pg_client().await?;
    assert_eq!(count_rows(&client, "e2e_project").await?, 5, "the declined flush must not have cost any rows");
    Ok(())
}
