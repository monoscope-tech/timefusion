//! Memory pressure: with a very small max-memory budget, hammering inserts
//! must NOT deadlock or OOM — the early-flush path on `insert()` should
//! drain to Delta. Asserts liveness (we eventually return) and that pressure
//! pct does not get stuck at 100.

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn many_inserts_under_tight_budget_do_not_deadlock() -> anyhow::Result<()> {
    let env = E2eEnv::builder()
        .with_max_memory_mb(64) // hits the MIN_BUFFER_BYTES floor
        .start()
        .await?;
    let client = env.pg_client().await?;

    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap();
    let sql = format!(
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) \
         VALUES ($1, '{}', '{}', $2, 'span', 'OK', $3, 'INFO', ARRAY[]::text[], $4)",
        dt.date_naive(),
        dt.format("%Y-%m-%d %H:%M:%S%.f"),
    );

    let big_msg = "x".repeat(8 * 1024);
    let big_summary: Vec<String> = (0..16).map(|_| big_msg.clone()).collect();

    let run = async {
        for i in 0..200 {
            client.execute(&sql, &[&"e2e_project", &format!("p-{i}"), &big_msg, &big_summary]).await?;
        }
        anyhow::Result::<()>::Ok(())
    };

    tokio::time::timeout(Duration::from_secs(60), run).await.map_err(|_| anyhow::anyhow!("inserts under pressure deadlocked"))??;

    Ok(())
}

/// Backpressure-not-rejection through the full prod path. Unlike the liveness
/// test above (whose ~25MB never crosses the limit), this pushes ~150MB into a
/// single open bucket — well past the ~76.8MB hard limit on a 64MB budget — so
/// the open bucket itself is the pressure and only the force-flush escalation
/// can drain it. With the flush-to-make-room fix every insert must still
/// succeed (no "Memory limit exceeded"), backpressure must register, and all
/// rows must be durable + queryable (some in Delta via force-flush, the rest in
/// MemBuffer). Pre-fix this returned a Postgres error once the buffer crossed
/// the limit (prod 2026-06-11: 15.8GB > 8GB, every insert rejected).
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn inserts_over_hard_limit_apply_backpressure_not_rejection() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_max_memory_mb(64).start().await?;
    let client = env.pg_client().await?;

    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap();
    let sql = format!(
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) \
         VALUES ($1, '{}', '{}', $2, 'span', 'OK', $3, 'INFO', ARRAY[]::text[], $4)",
        dt.date_naive(),
        dt.format("%Y-%m-%d %H:%M:%S%.f"),
    );

    // ~1MB/row × 150 = ~150MB into one (current) bucket — only the current-bucket
    // force-flush escalation can relieve this.
    let big_msg = "x".repeat(64 * 1024);
    let big_summary: Vec<String> = (0..16).map(|_| big_msg.clone()).collect();

    let run = async {
        for i in 0..150 {
            client
                .execute(&sql, &[&"e2e_bp", &format!("p-{i}"), &big_msg, &big_summary])
                .await
                .map_err(|e| anyhow::anyhow!("insert {i} rejected instead of applying backpressure: {e}"))?;
        }
        anyhow::Result::<()>::Ok(())
    };
    tokio::time::timeout(Duration::from_secs(90), run).await.map_err(|_| anyhow::anyhow!("inserts under backpressure deadlocked"))??;

    // The ~150MB lands in one open bucket, so only the current-bucket
    // force-flush escalation can drain it — assert that ran. (We no longer
    // require `backpressure_engaged_total >= 1`: the background flusher now
    // escalates proactively at the pressure threshold, often draining the
    // bucket before any insert reaches the hard-limit backpressure path.)
    assert!(env.snapshot_stats().backpressure_force_flush_total >= 1, "force-flush must drain the over-budget open bucket");

    let rows = client.query("SELECT count(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_bp"]).await?;
    let n: i64 = rows[0].get(0);
    assert_eq!(n, 150, "all backpressured inserts must be durable + queryable");

    Ok(())
}
