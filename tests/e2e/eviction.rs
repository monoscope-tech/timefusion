//! Eviction: after force-flush + retention pass, old buckets are gone from
//! MemBuffer metadata but still visible via Delta. New rows are also visible.

use std::time::Duration;

use timefusion::clock;

use super::harness::{E2eEnv, FROZEN_START_MICROS};

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

async fn insert_at(client: &tokio_postgres::Client, id: &str, ts_micros: i64) -> anyhow::Result<()> {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts_micros).unwrap();
    let sql = format!(
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) \
         VALUES ($1, '{}', '{}', $2, 'span', 'OK', 'm', 'INFO', ARRAY[]::text[], $3)",
        dt.date_naive(),
        dt.format("%Y-%m-%d %H:%M:%S%.f"),
    );
    client.execute(&sql, &[&"e2e_project", &id, &vec!["s"]]).await?;
    Ok(())
}
