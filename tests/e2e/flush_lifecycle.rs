//! Flush lifecycle: insert rows that span two MemBuffer time buckets, advance
//! the virtual clock past one bucket boundary, run an eviction pass (which
//! flushes only *completed* buckets), and assert:
//!   - exactly one bucket flushed (cumulative flush counter +1)
//!   - the other (current) bucket retained in MemBuffer
//!   - rows still queryable through the union of MemBuffer + Delta

use std::time::Duration;

use timefusion::clock;

use super::harness::{E2eEnv, FROZEN_START_MICROS};

#[tokio::test(flavor = "multi_thread")]
async fn flush_completed_bucket_only() -> anyhow::Result<()> {
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(60))
        .with_retention(Duration::from_secs(60 * 60))
        .start()
        .await?;
    let client = env.pg_client().await?;

    let bucket_size_micros: i64 = 60 * 1_000_000;
    let bucket_a_ts = FROZEN_START_MICROS;
    let bucket_b_ts = FROZEN_START_MICROS + bucket_size_micros + 5_000_000; // well into next bucket

    insert_at(&client, "bucket-a-1", bucket_a_ts).await?;
    insert_at(&client, "bucket-b-1", bucket_b_ts).await?;

    // Park the clock inside bucket B: bucket A is now completed, bucket B
    // is still the "current" open bucket. Advancing further (e.g.
    // bucket_b_ts + 2*bucket_size) would also mark B as completed and
    // both would flush, defeating the test.
    clock::set_micros(bucket_b_ts);

    let stats_before = env.snapshot_stats();
    assert!(stats_before.mem_total_rows >= 2, "expected >=2 rows pre-flush, got {:?}", stats_before);

    // Eviction pass = flush_completed_buckets + drain_metadata. Bucket B is
    // still open and must stay in MemBuffer.
    env.force_evict().await?;

    let stats_after = env.snapshot_stats();
    let flushed = stats_after.flush_completed_total - stats_before.flush_completed_total;
    assert_eq!(flushed, 1, "expected exactly one bucket flushed, got {flushed} (stats: {stats_after:?})");

    // Bucket B should still be in MemBuffer; bucket A drained.
    assert_eq!(
        stats_after.mem_total_rows, 1,
        "expected bucket B's row to remain in MemBuffer, got {stats_after:?}"
    );

    // Both rows must still be visible via union(MemBuffer, Delta).
    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 2, "rows disappeared after flush — Delta+MemBuffer union broken");

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
