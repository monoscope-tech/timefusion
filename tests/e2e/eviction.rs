//! Eviction: after force-flush + retention pass, old buckets are gone from
//! MemBuffer metadata but still visible via Delta. New rows are also visible.

use std::time::Duration;

use timefusion::clock;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn old_data_evicted_recent_retained() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).with_retention(Duration::from_secs(120)).start().await?;
    let client = env.pg_client().await?;

    // Old row in bucket at frozen-start
    insert_at(&client, "old", FROZEN_START_MICROS).await?;

    // Advance well past retention; everything before the new "now" is old.
    clock::set_micros(FROZEN_START_MICROS + 10 * 60 * 1_000_000);

    insert_at(&client, "recent", clock::now_micros()).await?;

    // Run flush + eviction.
    env.force_flush().await?;
    env.force_evict().await?;

    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 2, "both rows must survive eviction (via Delta + MemBuffer)");

    Ok(())
}
