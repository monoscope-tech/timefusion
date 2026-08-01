//! Hot-tail compaction must leave the partition with an HONEST sorted footer.
//!
//! Prod 2026-08-01: it did not, and nothing caught it. `stage_hot_bin` packed a
//! bin to `light_optimize_target_size` (256 MB of FILE bytes) and then handed it
//! to an in-process Arrow sort budgeted at 256 MB of IN-MEMORY bytes. zstd on
//! otel data is ~17x, so every bin arrived ~17x over budget, the sort was
//! silently skipped, and the output was written with `declare_sorted=false`:
//! 0 of the 8 largest files in a live partition declared `sorting_columns`.
//!
//! One such file is enough. The reader's `derive_common_ordering` is
//! all-or-nothing, so the whole scan lost its declared ordering — costing the
//! streaming top-N pushdown AND forcing `DedupExec` into its unbounded
//! `full-set` seen-set, which is the per-query memory behind the OOM/restart
//! cycle that made cold reads 26-68s.
//!
//! The guard has to exercise the SIZE condition, not just the happy path: a bin
//! small enough to fit the old budget was sorted correctly even before the fix,
//! so a test that only compacts a few rows proves nothing. `with_sort_skip_bytes`
//! shrinks the in-process budget to zero, which is what a 17x-oversized bin
//! looked like in production.

use std::time::Duration;

use timefusion::clock;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

/// After hot-tail compaction, a recent-window `ORDER BY timestamp DESC LIMIT n`
/// must still plan as a streaming merge — which it can only do if every file in
/// the partition, including the freshly compacted one, declares its ordering.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn hot_tail_output_declares_its_sorted_footer_even_when_the_bin_exceeds_the_sort_budget() -> anyhow::Result<()> {
    let bucket_secs = 60u64;
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(bucket_secs))
        .with_retention(Duration::from_secs(60 * 60))
        .with_optimize_sort_by()
        // The production shape: every bin is over the in-process sort budget.
        .with_sort_skip_bytes(0)
        .start()
        .await?;
    let client = env.pg_client().await?;

    // The hot tail only considers TODAY's partition and only files whose EVENT
    // time is sealed (15 min behind the clock), so the rows go 30 min before the
    // frozen "now" and the clock stays inside the same UTC day.
    let sec = 1_000_000i64;
    let base = FROZEN_START_MICROS - 1800 * sec;
    // 6 flushes: `timefusion_compact_min_files` is 5, so a smaller run selects no
    // bin at all and the pass is a silent no-op.
    for b in 0..6i64 {
        for i in 0..3i64 {
            let idx = b * 3 + i;
            insert_at(&client, &format!("h-{idx}"), base + idx * 20 * sec).await?;
        }
        env.advance(Duration::from_secs(bucket_secs * 2));
        env.force_flush().await?;
    }

    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    env.db().optimize_table_light(&table_ref, "otel_logs_and_spans").await?;

    // Fresh rows so the scan spans MemBuffer ∪ the compacted Delta partition —
    // the shape a dashboard query actually takes.
    // Advance to the frozen start so every written row is now sealed.
    clock::set_micros(FROZEN_START_MICROS);
    let new_base = FROZEN_START_MICROS - 60 * sec;
    for i in 0..3i64 {
        insert_at(&client, &format!("m-{i}"), new_base + i * sec).await?;
    }

    // The hot tail collapsed every Delta file into one, and the fresh rows are
    // still in MemBuffer — so Delta contributes exactly the compacted file. If
    // that file declares its ordering, the scan advertises one and the top-N
    // stays a streaming merge; if compaction wrote it unsorted, the same query
    // degrades to a blocking SortExec. That is the whole bug, end to end.
    let sql = "SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp DESC LIMIT 3";
    let plan: String = client
        .query(&format!("EXPLAIN {sql}"), &[])
        .await?
        .iter()
        .map(|r| (0..r.len()).map(|c| r.try_get::<_, String>(c).unwrap_or_default()).collect::<Vec<_>>().join(" | "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("SortPreservingMergeExec"),
        "hot-tail compaction must declare its output sorted even when the bin exceeds the in-process sort budget \
         (it sorts inside the DataFusion plan — pooled, spillable, streaming — instead of via `sort_batches_by_schema`). \
         A blocking SortExec here means the compacted file was written unsorted: the 2026-08-01 bug, where a 256 MB \
         FILE-byte bin was ~17x over a 256 MB in-memory budget and EVERY hot-tail output silently lost its footer. \
         Plan was:\n{plan}"
    );

    // ...and the rewrite must not have lost or duplicated anything.
    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 21, "18 compacted + 3 buffered rows must survive the sorted rewrite exactly once");

    let top: Vec<String> = client.query(sql, &[]).await?.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(top, vec!["m-2", "m-1", "m-0"], "newest-first ordering must still be correct after the rewrite");

    Ok(())
}
