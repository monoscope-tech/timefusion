//! What does sorting on the FLUSH path actually cost?
//!
//! The flush sort is skipped above `timefusion_sort_skip_bytes`, and the file is
//! then written unsorted — which poisons the partition's footer ordering for
//! every scan touching it. Raising that threshold is only safe if flush latency
//! does not regress, because flush is on the ingest path.
//!
//! This times the same workload twice: once with the sort skipped (threshold 0,
//! today's behaviour for a large bucket) and once with it taken (threshold
//! high, the merge path). It asserts the ROW COUNTS match and prints both
//! timings; the assertion is deliberately loose because CI timing is noisy —
//! the number is for the operator, the guard is against a pathological blowup.

use std::time::{Duration, Instant};

use timefusion::clock;

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
        // Scrambled event time so the sort has real work: an append-ordered
        // bucket takes `sort_one_batch`'s already-sorted fast path and would
        // measure nothing.
        let jitter = ((i * 7919) % rows) * 1_000;
        insert_at(&client, &format!("f-{i}"), FROZEN_START_MICROS - 600_000_000 + jitter).await?;
    }
    clock::set_micros(FROZEN_START_MICROS);
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
    // Loose by design — this guards against an order-of-magnitude regression,
    // not against normal variance on a shared CI box.
    assert!(
        sorted < skipped * 10 + Duration::from_secs(5),
        "sorting the flush must not cost an order of magnitude: skipped={skipped:?} sorted={sorted:?}. \
         The merge path sorts each batch and k-way merges (freeing runs as they drain), so it should be \
         close to the unsorted write — if this fires, the flush sort regressed to a whole-bucket materialisation."
    );
    Ok(())
}
