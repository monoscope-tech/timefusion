//! Tests that recently-flushed data is served from Foyer rather than S3.
//! Asserts on Foyer's `inner_gets` (S3 body fetches) rather than wall-clock so
//! it's deterministic on CI.

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

async fn insert_and_flush(env: &E2eEnv) -> anyhow::Result<()> {
    let client = env.pg_client().await?;
    for i in 0..50 {
        insert_at(&client, &format!("w-{i}"), FROZEN_START_MICROS).await?;
    }
    env.force_flush().await?;
    Ok(())
}

/// `SELECT id` forces a real column-body read; COUNT is answered from statistics
/// and never touches the parquet body Foyer caches.
const BODY_READ: &str = "SELECT id FROM otel_logs_and_spans WHERE project_id = $1 ORDER BY id LIMIT 5";

async fn body_read_cost(env: &E2eEnv) -> anyhow::Result<(u64, u64, usize)> {
    let client = env.pg_client().await?;
    let before = env.foyer_stats().await.expect("foyer enabled").main;
    let rows = client.query(BODY_READ, &[&"e2e_project"]).await?;
    let after = env.foyer_stats().await.expect("foyer enabled").main;
    Ok((after.hits.saturating_sub(before.hits), after.inner_gets.saturating_sub(before.inner_gets), rows.len()))
}

/// Once the hot tail is evicted from the MemBuffer, a recent body read must be
/// served from Foyer (write-through cached on flush) with zero S3 body GETs.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn evicted_hot_tail_body_read_served_from_foyer_not_s3() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_foyer_enabled().with_warm_full_files().start().await?;
    // The boot preloader would otherwise issue unrelated GETs against tables
    // that do not exist in the fresh bucket, polluting the counters.
    env.db().cancel_maintenance();
    insert_and_flush(&env).await?;

    // While still in the MemBuffer, the read touches neither parquet nor Foyer.
    let (hits_mem, s3_mem, rows_mem) = body_read_cost(&env).await?;
    assert!(rows_mem > 0, "rows must be visible from the MemBuffer");
    assert_eq!((hits_mem, s3_mem), (0, 0), "in-memory read must not touch parquet/Foyer, got hits={hits_mem} s3={s3_mem}");

    // Evict so the next read must go through Foyer to S3-backed parquet.
    env.force_evict().await?;

    let (_hits, s3_gets, rows) = body_read_cost(&env).await?;
    assert!(rows > 0, "rows must still be visible from Delta after eviction");
    assert_eq!(s3_gets, 0, "evicted hot-tail body read should be served from Foyer (write-through pin), 0 S3 GETs, got {s3_gets}");
    Ok(())
}

/// Documents the post-restart cost of the first recent body read. Foyer's L2 disk
/// cache persists across restart under the same data dir.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn hot_tail_pin_survives_restart() -> anyhow::Result<()> {
    let mut env = E2eEnv::builder().with_foyer_enabled().with_warm_full_files().start().await?;
    insert_and_flush(&env).await?;
    env.force_evict().await?;

    env.restart().await?;
    // Give boot-time warming a moment to run against the persisted files.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (_hits, s3_gets, rows) = body_read_cost(&env).await?;
    assert!(rows > 0, "rows must be visible after restart");
    // Deliberately not asserted on: this prints the post-restart cost so a
    // regression is visible, but the value is not a guaranteed invariant.
    eprintln!("POST-RESTART first recent body read: s3_body_gets={s3_gets} (0 = served from persisted Foyer/warm)");
    Ok(())
}
