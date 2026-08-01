//! An oversized flush group must ESCALATE to the spilling sort, not skip it.
//!
//! History, all on 2026-08-01. The in-process flush sort was capped at 256 MiB
//! of in-memory Arrow and SKIPPED past it, writing the file with no
//! `sorting_columns` footer — and one such file disables the reader's
//! all-or-nothing ordering for every scan touching the partition. Raising the
//! cap to 4 GiB fixed the footer and broke the box: that sort allocates OUTSIDE
//! the DataFusion pool, so on a 26 GB prod box it authorised multi-GB untracked
//! allocations on the INGEST path, and both images carrying it OOM-killed.
//!
//! Neither end of that trade is acceptable, so the threshold is now an
//! ESCALATION point: past it the group sorts inside a DataFusion plan whose
//! pool is bounded and spills to disk. The footer stays honest AND the peak is
//! bounded by the pool instead of by the bucket.
//!
//! `with_sort_skip_bytes(0)` forces every group over the threshold, which is
//! what a real oversized bucket looks like.

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn an_oversized_flush_group_is_sorted_by_the_spilling_path_not_skipped() -> anyhow::Result<()> {
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(3600))
        .with_retention(Duration::from_secs(60 * 60))
        .with_optimize_sort_by()
        // Every group counts as oversized => always take the escalation path.
        .with_sort_skip_bytes(0)
        .start()
        .await?;
    let client = env.pg_client().await?;

    // One bucket => one flush file. With several files the reader cannot derive
    // an ordering across them (their time ranges overlap) however well each one
    // is sorted, so a multi-file fixture would fail for a reason unrelated to
    // the escalation. Scrambled event time within that bucket: an
    // append-ordered bucket would come out sorted even if the sort were
    // skipped, which would prove nothing.
    let sec = 1_000_000i64;
    let base = FROZEN_START_MICROS - 600 * sec;
    const N: i64 = 40;
    for i in 0..N {
        let jitter = ((i * 17) % N) * sec;
        insert_at(&client, &format!("e-{i:03}"), base + jitter).await?;
    }
    env.force_flush().await?;

    // Every row survived the escalated sort exactly once.
    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, N, "the spilling sort must not lose or duplicate rows");

    // ...and the data really is ordered. If the escalation silently fell back
    // to the unsorted write, the footer would be absent and this scan would
    // need a blocking sort to answer.
    let plan: String = client
        .query("EXPLAIN SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp DESC LIMIT 5", &[])
        .await?
        .iter()
        .map(|r| (0..r.len()).map(|c| r.try_get::<_, String>(c).unwrap_or_default()).collect::<Vec<_>>().join(" | "))
        .collect::<Vec<_>>()
        .join("\n");
    // `mode=bounded` is the proof: DedupExec can only take its bounded
    // per-timestamp-run seen-set when its INPUT declares an ordering, and the
    // only way this single flush file declares one is a `sorting_columns`
    // footer — which the escalated sort is what earns. If the escalation had
    // fallen back to writing unsorted, this reads `mode=full-set`.
    // (Not asserting SortPreservingMergeExec: with one file there is a single
    // partition and nothing to merge, so its absence proves nothing.)
    assert!(
        plan.contains("mode=bounded"),
        "the escalated flush must declare a sorted footer; `mode=full-set` here means it fell back to writing \
         unsorted, i.e. the escalation did not happen. Plan was:\n{plan}"
    );

    // And the ordering is actually correct, not merely declared.
    let top: Vec<String> = client
        .query("SELECT id FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp DESC LIMIT 3", &[])
        .await?
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    let mut expect: Vec<(i64, String)> = (0..N).map(|i| (((i * 17) % N), format!("e-{i:03}"))).collect();
    expect.sort_by(|a, b| b.0.cmp(&a.0));
    assert_eq!(top, expect.iter().take(3).map(|(_, id)| id.clone()).collect::<Vec<_>>(), "declared order must match actual order");

    Ok(())
}
