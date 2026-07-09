//! Flush lifecycle: insert rows that span two MemBuffer time buckets, advance
//! the virtual clock past one bucket boundary, run an eviction pass (which
//! flushes only *completed* buckets), and assert:
//!   - exactly one bucket flushed (cumulative flush counter +1)
//!   - the other (current) bucket retained in MemBuffer
//!   - rows still queryable through the union of MemBuffer + Delta

use std::time::Duration;

use timefusion::clock;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn flush_completed_bucket_only() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).with_retention(Duration::from_secs(60 * 60)).start().await?;
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
    assert_eq!(stats_after.mem_total_rows, 1, "expected bucket B's row to remain in MemBuffer, got {stats_after:?}");

    // Both rows must still be visible via union(MemBuffer, Delta).
    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 2, "rows disappeared after flush — Delta+MemBuffer union broken");

    Ok(())
}

/// `FLUSH` over pgwire drains the whole MemBuffer (open bucket included) to
/// Delta. Ops run it right before a planned restart so the stop grace never
/// bounds the shutdown flush and restart replay is near-empty.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn flush_command_drains_membuffer() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let client = env.pg_client().await?;

    insert_at(&client, "flush-cmd-1", FROZEN_START_MICROS).await?;
    insert_at(&client, "flush-cmd-2", FROZEN_START_MICROS + 61 * 1_000_000).await?;
    assert!(env.snapshot_stats().mem_total_rows >= 2);

    // The wire tag is `FLUSH {total_rows}`; tokio-postgres exposes the tag's
    // trailing count as the CommandComplete payload — assert on it so a
    // regression garbling the tag or the flushed-row count fails here.
    let flushed = client
        .simple_query("FLUSH")
        .await?
        .into_iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::CommandComplete(n) => Some(n),
            _ => None,
        })
        .expect("FLUSH must return a command tag");
    assert_eq!(flushed, 2, "FLUSH tag must report the flushed row count");

    let stats = env.snapshot_stats();
    assert_eq!(stats.mem_total_rows, 0, "FLUSH left rows in MemBuffer: {stats:?}");
    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 2, "rows lost across FLUSH — must be durable in Delta");

    Ok(())
}
