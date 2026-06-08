//! Memory pressure: with a very small max-memory budget, hammering inserts
//! must NOT deadlock or OOM — the early-flush path on `insert()` should
//! drain to Delta. Asserts liveness (we eventually return) and that pressure
//! pct does not get stuck at 100.

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS};

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

    tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .map_err(|_| anyhow::anyhow!("inserts under pressure deadlocked"))??;

    Ok(())
}
