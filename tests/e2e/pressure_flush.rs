//! Memory pressure: inserts past the memory budget must apply backpressure and
//! force-flush rather than deadlock or be rejected.

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS};
use super::ordering_pushdown::count_rows;

/// Pushes ~100MB into a single open bucket, past the hard limit on a 64MB
/// budget: every insert must still succeed and all rows stay queryable.
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

    const ROWS: usize = 100;

    // ~1MB/row × 100 = ~100MB into one open bucket, beyond the hard limit.
    let big_msg = "x".repeat(64 * 1024);
    let big_summary: Vec<String> = (0..16).map(|_| big_msg.clone()).collect();

    let run = async {
        for i in 0..ROWS {
            client
                .execute(&sql, &[&"e2e_bp", &format!("p-{i}"), &big_msg, &big_summary])
                .await
                .map_err(|e| anyhow::anyhow!("insert {i} rejected instead of applying backpressure: {e}"))?;
        }
        anyhow::Result::<()>::Ok(())
    };
    tokio::time::timeout(Duration::from_secs(90), run).await.map_err(|_| anyhow::anyhow!("inserts under backpressure deadlocked"))??;

    // Only the current-bucket force-flush escalation can drain an open bucket;
    // `backpressure_engaged_total` is not asserted because proactive flushing
    // often drains it before an insert hits the hard-limit path.
    assert!(env.snapshot_stats().backpressure_force_flush_total >= 1, "force-flush must drain the over-budget open bucket");

    assert_eq!(count_rows(&client, "e2e_bp").await?, ROWS as i64, "all backpressured inserts must be durable + queryable");

    Ok(())
}
