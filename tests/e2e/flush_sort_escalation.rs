//! An oversized flush group must escalate to the bounded, spilling DataFusion
//! sort rather than skip sorting and write a file with no `sorting_columns`
//! footer.

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

    // One bucket => one flush file: across several files the reader cannot derive an
    // ordering anyway. Event time is scrambled within the bucket so an append-ordered
    // bucket cannot pass with the sort skipped.
    let sec = 1_000_000i64;
    let base = FROZEN_START_MICROS - 600 * sec;
    const N: i64 = 40;
    for i in 0..N {
        let jitter = ((i * 17) % N) * sec;
        insert_at(&client, &format!("e-{i:03}"), base + jitter).await?;
    }
    env.force_flush().await?;

    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, N, "the spilling sort must not lose or duplicate rows");

    let plan: String = client
        .query("EXPLAIN SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp DESC LIMIT 5", &[])
        .await?
        .iter()
        .map(|r| (0..r.len()).map(|c| r.try_get::<_, String>(c).unwrap_or_default()).collect::<Vec<_>>().join(" | "))
        .collect::<Vec<_>>()
        .join("\n");
    // `mode=bounded` is the proof: DedupExec only takes its bounded seen-set when its
    // input declares an ordering, which for one file means a `sorting_columns` footer.
    // SortPreservingMergeExec is not asserted: one file = one partition, nothing to merge.
    assert!(
        plan.contains("mode=bounded"),
        "the escalated flush must declare a sorted footer; `mode=full-set` here means it fell back to writing \
         unsorted, i.e. the escalation did not happen. Plan was:\n{plan}"
    );

    // The ordering is actually correct, not merely declared.
    let top: Vec<String> = client
        .query("SELECT id FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp DESC LIMIT 3", &[])
        .await?
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    let mut expect: Vec<(i64, String)> = (0..N).map(|i| (((i * 17) % N), format!("e-{i:03}"))).collect();
    expect.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    assert_eq!(top, expect.iter().take(3).map(|(_, id)| id.clone()).collect::<Vec<_>>(), "declared order must match actual order");

    Ok(())
}
