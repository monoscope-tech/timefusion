//! Times the same flush workload with the sort skipped and with it taken, to
//! bound what sorting on the flush (ingest) path costs.

use std::time::{Duration, Instant};

use timefusion::support;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

async fn timed_flush(skip_bytes: usize, rows: i64) -> anyhow::Result<(Duration, i64)> {
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(60))
        .with_retention(Duration::from_secs(60 * 60))
        .with_sort_skip_bytes(skip_bytes)
        .start()
        .await?;
    let client = env.pg_client().await?;
    for i in 0..rows {
        // Scrambled event time: an append-ordered bucket takes the already-sorted
        // fast path and measures nothing.
        let jitter = ((i * 7919) % rows) * 1_000;
        insert_at(&client, &format!("f-{i}"), FROZEN_START_MICROS - 600_000_000 + jitter).await?;
    }
    support::set_micros(FROZEN_START_MICROS);
    let t = Instant::now();
    env.force_flush().await?;
    let elapsed = t.elapsed();
    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    Ok((elapsed, count))
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn sorting_the_flush_does_not_blow_up_flush_latency() -> anyhow::Result<()> {
    const ROWS: i64 = 400;
    let (skipped, n_skipped) = timed_flush(0, ROWS).await?;
    let (sorted, n_sorted) = timed_flush(usize::MAX, ROWS).await?;

    println!("FLUSH COST rows={ROWS} skipped={skipped:?} sorted={sorted:?} ratio={:.2}", sorted.as_secs_f64() / skipped.as_secs_f64().max(1e-9));

    assert_eq!(n_skipped, ROWS, "the skipped-sort flush must persist every row");
    assert_eq!(n_sorted, ROWS, "the sorted flush must persist every row");
    // Loose by design: guards an order-of-magnitude regression, not CI variance.
    assert!(
        sorted < skipped * 10 + Duration::from_secs(5),
        "sorting the flush must not cost an order of magnitude: skipped={skipped:?} sorted={sorted:?}. \
         The merge path sorts each batch and k-way merges (freeing runs as they drain), so it should be \
         close to the unsorted write — if this fires, the flush sort regressed to a whole-bucket materialisation."
    );
    Ok(())
}
