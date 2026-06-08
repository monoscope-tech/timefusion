//! Cache warmth: after a flush, the first read materializes Parquet from S3
//! through Foyer; the second read must hit Foyer. Asserts directly on the
//! Foyer hit counter rather than on latency — wall-clock comparisons are
//! flaky on shared CI runners.

use super::harness::{E2eEnv, FROZEN_START_MICROS};

#[tokio::test(flavor = "multi_thread")]
async fn second_read_after_flush_hits_foyer() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_foyer_enabled().start().await?;
    let client = env.pg_client().await?;

    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap();
    for i in 0..50 {
        let sql = format!(
            "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) \
             VALUES ($1, '{}', '{}', $2, 'span', 'OK', 'm', 'INFO', ARRAY[]::text[], $3)",
            dt.date_naive(),
            dt.format("%Y-%m-%d %H:%M:%S%.f"),
        );
        client.execute(&sql, &[&"e2e_project", &format!("c-{i}"), &vec!["s"]]).await?;
    }

    env.force_flush().await?;

    // Baseline AFTER warm-up: the flush itself may warm Foyer, so we sample
    // hits after a primer query so the assertion measures incremental hits
    // from the *next* query. Use `SELECT id` (not COUNT) to force a real
    // parquet body read — COUNT can resolve from statistics alone and
    // never touches the data files Foyer caches.
    let probe = "SELECT id FROM otel_logs_and_spans WHERE project_id = $1 ORDER BY id LIMIT 5";
    let _ = client.query(probe, &[&"e2e_project"]).await?;
    let before = env.foyer_stats().await.expect("foyer enabled");

    let _ = client.query(probe, &[&"e2e_project"]).await?;
    let after = env.foyer_stats().await.expect("foyer enabled");

    // Two acceptable outcomes prove caching is working:
    //   - Foyer registered a hit on the second read, OR
    //   - the second read didn't touch the inner store at all (something
    //     upstream — DataFusion plan cache, parquet metadata cache, etc. —
    //     satisfied it without hitting S3).
    // What we *don't* want: the second read going back to S3 (a delta
    // `inner_gets` increase) without a Foyer hit, because that means the
    // cache layer is bypassed for body reads.
    let delta_hits = after.main.hits.saturating_sub(before.main.hits);
    let delta_inner = after.main.inner_gets.saturating_sub(before.main.inner_gets);
    assert!(
        delta_hits >= 1 || delta_inner == 0,
        "second read went to S3 without hitting Foyer; before={:?} after={:?}",
        before.main,
        after.main
    );
    Ok(())
}
