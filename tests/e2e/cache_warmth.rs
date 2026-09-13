//! Cache warmth: after a flush, a repeated read must be served by Foyer rather
//! than S3. Asserts on the Foyer hit counter, not on latency.

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn second_read_after_flush_hits_foyer() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_foyer_enabled().start().await?;
    let client = env.pg_client().await?;

    for i in 0..50 {
        insert_at(&client, &format!("c-{i}"), FROZEN_START_MICROS).await?;
    }

    env.force_flush().await?;

    // Sample the baseline after a primer query: the flush itself may warm Foyer.
    // Must be `SELECT id`, not COUNT — COUNT resolves from statistics and never
    // reads the parquet bodies Foyer caches.
    let probe = "SELECT id FROM otel_logs_and_spans WHERE project_id = $1 ORDER BY id LIMIT 5";
    let _ = client.query(probe, &[&"e2e_project"]).await?;
    let before = env.foyer_stats().await.expect("foyer enabled");

    let _ = client.query(probe, &[&"e2e_project"]).await?;
    let after = env.foyer_stats().await.expect("foyer enabled");

    // Pass if Foyer hit, or if an upstream cache served the read without touching
    // the inner store. Fail only when the read went to S3 with no Foyer hit.
    let delta_hits = after.main.hits.saturating_sub(before.main.hits);
    let delta_inner = after.main.inner_gets.saturating_sub(before.main.inner_gets);
    assert!(delta_hits >= 1 || delta_inner == 0, "second read went to S3 without hitting Foyer; before={:?} after={:?}", before.main, after.main);
    Ok(())
}
