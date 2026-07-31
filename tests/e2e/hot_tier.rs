//! Hot tier (P1): a bucket drained out of MemBuffer is DEMOTED to a local
//! Arrow IPC file instead of dropped, still answers queries as the scan's
//! third leg, and survives a restart (index rebuilt by `rescan`).

use std::time::Duration;

use timefusion::clock;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

fn arrow_files(dir: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    entries
        .flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() { arrow_files(&p) } else { usize::from(p.extension().is_some_and(|x| x == "arrow")) }
        })
        .sum()
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn drained_buckets_are_demoted_served_and_survive_restart() -> anyhow::Result<()> {
    let mut env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).with_retention(Duration::from_secs(120)).start().await?;
    let client = env.pg_client().await?;

    insert_at(&client, "old", FROZEN_START_MICROS).await?;
    clock::set_micros(FROZEN_START_MICROS + 10 * 60 * 1_000_000);
    insert_at(&client, "recent", clock::now_micros()).await?;

    // Flush commits to Delta and drains the buckets; eviction waits out the
    // in-flight demotion (it shares the demote permit) and then GCs the tier.
    env.force_flush().await?;
    env.force_evict().await?;
    assert!(env.snapshot_stats().hot_tier.files > 0, "a drained bucket must be demoted, not dropped");
    assert!(arrow_files(&env.data_dir.join("hot_tier")) > 0, "demoted buckets must exist as .arrow files on disk");

    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 2, "demoted rows stay queryable");
    // ...and an UNBOUNDED scan must not have touched the tier at all: the hot
    // leg is materialized eagerly into heap, so it is only consulted for scans
    // shallow enough to sit inside the tier's window (`skip_for_lookback`).
    assert_eq!(env.snapshot_stats().hot_tier.read_hits, 0, "a scan with no lower time bound must skip the hot leg entirely");

    // A recent-window read — what the tier exists for — does hit it.
    let lo = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS - 3_600_000_000).unwrap().format("%Y-%m-%d %H:%M:%S%.f");
    let sql = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'e2e_project' AND timestamp >= '{lo}'");
    let count: i64 = client.query_one(&sql, &[]).await?.get(0);
    assert_eq!(count, 2, "the hot leg must serve exactly the same rows, once each");
    let hot = env.snapshot_stats().hot_tier;
    assert!(hot.read_hits > 0, "the scan must have read the hot tier ({hot:?})");
    assert_eq!(hot.read_misses, 0, "no hot-tier file may read as torn/absent ({hot:?})");
    assert_eq!(hot.leg_budget_stops, 0, "two rows must not exhaust the per-scan byte budget ({hot:?})");

    // Restart warmth: the index is rebuilt from disk, rows still answer, and
    // the union still yields each row exactly once (no mem/hot/delta overlap).
    env.restart().await?;
    let client = env.pg_client().await?;
    let after = env.snapshot_stats().hot_tier;
    assert!(after.files > 0, "rescan must rebuild the index after restart ({after:?})");
    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 2, "restart keeps demoted rows queryable exactly once");

    Ok(())
}
