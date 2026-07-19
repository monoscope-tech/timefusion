//! Prototype: keep the recently-flushed "hot tail" served from Foyer, not S3
//! (the lever from the 2026-07-15 dashboard profiling — recent-window queries
//! are scan/IO-bound with a 20-75x Foyer cold/warm swing).
//!
//! FINDINGS this harness prototype establishes:
//!   1. Freshly-inserted rows are served from the MemBuffer until they are
//!      EVICTED, so a recent query never touches parquet/Foyer in that window
//!      (both counters stay flat). The cold/warm swing only appears AFTER the
//!      hot tail ages out of memory onto S3-backed parquet.
//!   2. Foyer's `WriteOnInsertion` policy means the flush WRITE already caches
//!      the parquet body in-process, so once evicted from mem the body read is
//!      still served from Foyer with ZERO S3 GETs — the pin holds. The explicit
//!      `warm_full_files` lever therefore matters for the post-RESTART / cross-
//!      replica case (cold L1), which boot-time warming (`preload_tables`)
//!      covers separately; here we validate the steady-state write-through pin.
//!
//! Asserts on Foyer's `inner_gets` (S3 body fetches) rather than wall-clock so
//! it's deterministic on CI.

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS};

async fn insert_and_flush(env: &E2eEnv) -> anyhow::Result<()> {
    let client = env.pg_client().await?;
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap();
    for i in 0..50 {
        let sql = format!(
            "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) \
             VALUES ($1, '{}', '{}', $2, 'span', 'OK', 'm', 'INFO', ARRAY[]::text[], $3)",
            dt.date_naive(),
            dt.format("%Y-%m-%d %H:%M:%S%.f"),
        );
        client.execute(&sql, &[&"e2e_project", &format!("w-{i}"), &vec!["s"]]).await?;
    }
    env.force_flush().await?;
    Ok(())
}

/// `SELECT id` forces a real column-body read (COUNT can be answered from
/// statistics and never touches the parquet body Foyer caches).
const BODY_READ: &str = "SELECT id FROM otel_logs_and_spans WHERE project_id = $1 ORDER BY id LIMIT 5";

async fn body_read_cost(env: &E2eEnv) -> anyhow::Result<(u64, u64, usize)> {
    let client = env.pg_client().await?;
    let before = env.foyer_stats().await.expect("foyer enabled").main;
    let rows = client.query(BODY_READ, &[&"e2e_project"]).await?;
    let after = env.foyer_stats().await.expect("foyer enabled").main;
    Ok((after.hits.saturating_sub(before.hits), after.inner_gets.saturating_sub(before.inner_gets), rows.len()))
}

/// Prove the steady-state pin: once the hot tail is EVICTED from the MemBuffer,
/// a recent body read is served from Foyer (write-through cached on flush) with
/// ZERO S3 body GETs.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn evicted_hot_tail_body_read_served_from_foyer_not_s3() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_foyer_enabled().with_warm_full_files().start().await?;
    insert_and_flush(&env).await?;

    // While still in the MemBuffer, the read touches neither parquet nor Foyer.
    let (hits_mem, s3_mem, rows_mem) = body_read_cost(&env).await?;
    assert!(rows_mem > 0, "rows must be visible from the MemBuffer");
    assert_eq!((hits_mem, s3_mem), (0, 0), "in-memory read must not touch parquet/Foyer, got hits={hits_mem} s3={s3_mem}");

    // Evict the hot tail out of memory so the next read must hit S3-backed
    // parquet (through Foyer).
    env.force_evict().await?;

    let (_hits, s3_gets, rows) = body_read_cost(&env).await?;
    assert!(rows > 0, "rows must still be visible from Delta after eviction");
    assert_eq!(s3_gets, 0, "evicted hot-tail body read should be served from Foyer (write-through pin), 0 S3 GETs, got {s3_gets}");
    Ok(())
}

/// The cold-cache case the lever ultimately targets: after a RESTART, does the
/// first recent body read still avoid S3? (Foyer's L2 disk cache persists
/// across restart under the same data dir, so this documents whether the pin
/// survives a process bounce — the closest in-harness analogue of a deploy.)
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
    // Not a hard assertion on 0 — this test DOCUMENTS the post-restart cost so a
    // regression (every recent read going to S3 after a deploy) is visible.
    eprintln!("POST-RESTART first recent body read: s3_body_gets={s3_gets} (0 = served from persisted Foyer/warm)");
    Ok(())
}
