//! Timestamp-ordering + LIMIT pushdown (the "latest N events" dashboard path).
//!
//! With the schema's `timestamp` sorted DESCENDING, flushed Delta files carry an
//! honest `[timestamp DESC, …]` footer that the delta-rs fork advertises as the
//! scan's output ordering. `OrderedUnionForTopK` then sorts the (unordered)
//! MemBuffer branch of the `mem∪delta` union to match, so `ORDER BY timestamp
//! DESC LIMIT n` collapses to a streaming `SortPreservingMergeExec` + fetch that
//! reads the newest rows/files and stops — instead of a full blocking `SortExec`
//! over the whole window.
//!
//! Asserts both the plan shape (SortPreservingMergeExec present) and correctness
//! (rows come back strictly DESC and the true top-n, spanning both stores).

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn order_by_ts_desc_limit_merges_mem_and_delta() -> anyhow::Result<()> {
    let bucket_secs = 60u64;
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(bucket_secs)).with_retention(Duration::from_secs(60 * 60)).start().await?;
    let client = env.pg_client().await?;

    let sec = 1_000_000i64;
    // 5 older rows → flushed to Delta (physically DESC-sorted, honest footer).
    for i in 0..5 {
        insert_at(&client, &format!("old-{i}"), FROZEN_START_MICROS + i * sec).await?;
    }
    // Advance well past the bucket boundary so the old bucket is completed, then
    // flush it out to Delta and drain MemBuffer.
    env.advance(Duration::from_secs(bucket_secs * 3));
    env.force_flush().await?;

    // 5 newer rows stay in MemBuffer (current, open bucket).
    let new_base = FROZEN_START_MICROS + (bucket_secs as i64) * 3 * sec;
    for i in 0..5 {
        insert_at(&client, &format!("new-{i}"), new_base + i * sec).await?;
    }

    // The query has no lower time bound, so it spans MemBuffer ∪ Delta.
    let sql = "SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp DESC LIMIT 3";

    // Plan shape: the union became order-preserving → a streaming merge, not a
    // blocking sort over the full window.
    let plan: String = client
        .query(&format!("EXPLAIN {sql}"), &[])
        .await?
        .iter()
        .map(|r| (0..r.len()).map(|c| r.try_get::<_, String>(c).unwrap_or_default()).collect::<Vec<_>>().join(" | "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("SortPreservingMergeExec"), "expected a streaming SortPreservingMergeExec (ordering pushdown fired); plan was:\n{plan}");

    // Correctness: true top-3 newest, strictly descending, drawn from MemBuffer.
    let rows = client.query(sql, &[]).await?;
    let ids: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(ids, vec!["new-4", "new-3", "new-2"], "wrong top-n or wrong order; plan:\n{plan}");

    let ts: Vec<i64> = rows.iter().map(|r| r.get::<_, chrono::DateTime<chrono::Utc>>(1).timestamp_micros()).collect();
    assert!(ts.windows(2).all(|w| w[0] > w[1]), "timestamps not strictly descending: {ts:?}");

    Ok(())
}

// After compaction the partition's flush files are rewritten. With plain Compact
// (concatenation) the output declares no sort order, so the ordering pushdown
// dies within one optimize cycle. `OptimizeType::SortBy` globally sorts the
// rewrite and declares an honest DESC footer, so an *optimized* partition still
// advertises ordering and `ORDER BY timestamp DESC LIMIT n` stays a streaming
// merge. Drives the specific-date `compact_date` (optimize's window keys off the
// real wall clock, not the test's virtual clock).
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn optimized_partition_still_advertises_desc_ordering() -> anyhow::Result<()> {
    let bucket_secs = 60u64;
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(bucket_secs))
        .with_retention(Duration::from_secs(60 * 60))
        .with_optimize_sort_by()
        .start()
        .await?;
    let client = env.pg_client().await?;

    let sec = 1_000_000i64;
    // 9 rows across 3 buckets, flushed one bucket at a time → 3 distinct Delta
    // files for SortBy to actually merge. A single force_flush coalesces all
    // completed buckets into one file, which the incremental SortBy correctly
    // skips as a no-op (single-file bin) — leaving nothing to test.
    for b in 0..3i64 {
        for i in 0..3i64 {
            let idx = b * 3 + i;
            insert_at(&client, &format!("d-{idx}"), FROZEN_START_MICROS + idx * 20 * sec).await?;
        }
        env.advance(Duration::from_secs(bucket_secs * 2));
        env.force_flush().await?;
    }

    // Compact the partition → SortBy rewrite (globally sorted, honest DESC footer).
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap().date_naive();
    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let (removed, added) = env.db().compact_date(&table_ref, "otel_logs_and_spans", date, None).await?;
    assert!(removed >= 1 && added >= 1, "compaction should have rewritten files (removed={removed}, added={added})");

    // Fresh rows into MemBuffer so the query spans MemBuffer ∪ (optimized) Delta.
    let new_base = FROZEN_START_MICROS + (bucket_secs as i64) * 6 * sec;
    for i in 0..3 {
        insert_at(&client, &format!("m-{i}"), new_base + i * sec).await?;
    }

    let sql = "SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp DESC LIMIT 3";
    let plan: String = client
        .query(&format!("EXPLAIN {sql}"), &[])
        .await?
        .iter()
        .map(|r| (0..r.len()).map(|c| r.try_get::<_, String>(c).unwrap_or_default()).collect::<Vec<_>>().join(" | "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("SortPreservingMergeExec"), "optimized partition must still advertise DESC ordering (SortBy footer); plan was:\n{plan}");

    let rows = client.query(sql, &[]).await?;
    let ids: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(ids, vec!["m-2", "m-1", "m-0"], "wrong top-n/order after optimize; plan:\n{plan}");

    Ok(())
}

// One non-conforming file must not cost the conforming majority its ordering
// claim — the 30-day `log_list` mechanism.
//
// The delta-rs fork deliberately ISOLATES files whose footer does not declare
// the scan's common ordering: the conforming files keep the `[timestamp DESC]`
// claim in one `DataSourceExec` and the rest are unioned in as a sibling with no
// claim ("isolate, don't surrender"). But a `UnionExec` advertises an ordering
// only when EVERY child does, so the Delta leg as a whole then advertises none —
// and `ProjectRoutingTable::scan`'s own `mem ∪ delta` union sees an unordered,
// unsortable Delta leg, bails, and DataFusion plants a BLOCKING `SortExec` over
// the entire window. A `LIMIT 251` newest-first listing therefore reads every
// selected byte instead of the newest few files: prod p1 at 30 d selected 431 GB
// for 251 rows, while the same query at 14 d (all files conforming) streamed in
// 1.6 s.
//
// Fixture: a plain (non-SortBy) `compact_date` concatenates two flushed files
// into one file with no declared footer order, then one further flush adds a
// conforming file beside it. The claim must survive the isolation split.
/// Build the fixture at a given repair budget and return `(plan, top-3 ids)`.
///
/// A plain (non-SortBy) `compact_date` concatenates two flushed files into one
/// file with no declared footer order; one further flush adds a conforming file
/// beside it, which is the fork's isolation shape.
async fn isolated_union_plan(budget_mb: u64) -> anyhow::Result<(String, Vec<String>, String)> {
    let bucket_secs = 60u64;
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(bucket_secs))
        .with_retention(Duration::from_secs(60 * 60))
        // The fixture owns its flushes. Left on the default interval the periodic
        // task fires between two of the three inserts below, so the last flush
        // emits TWO files (1 row + 2) instead of one; the 2-row file holds
        // s-7,s-8 in ASCENDING order and declares no `timestamp DESC` footer, so
        // NO child of the union carries the claim and `repair_isolated_scan_ordering`
        // has nothing to propagate. That is what made this test intermittent —
        // the fixture never built the conforming/non-conforming pair it asserts on.
        .with_flush_interval(Duration::from_secs(3600))
        .with_unordered_leg_sort_max_mb(budget_mb)
        .start()
        .await?;
    let client = env.pg_client().await?;
    let sec = 1_000_000i64;

    for b in 0..2i64 {
        for i in 0..3i64 {
            insert_at(&client, &format!("u-{}", b * 3 + i), FROZEN_START_MICROS + (b * 3 + i) * sec).await?;
        }
        env.advance(Duration::from_secs(bucket_secs * 2));
        env.force_flush().await?;
    }
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap().date_naive();
    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let (removed, added) = env.db().compact_date(&table_ref, "otel_logs_and_spans", date, None).await?;
    assert!(removed >= 2 && added >= 1, "the fixture needs a real concatenation (removed={removed}, added={added})");

    // ONE statement, not three. Three separate INSERTs are three MemBuffer
    // batches, and whether the flush coalesces them into one Delta file is a
    // timing decision — when it does not, the conforming file the test needs
    // arrives as a 1-row and a 2-row file, the 2-row one holds s-7,s-8
    // ASCENDING and declares no `timestamp DESC` footer, and NO child of the
    // union carries the ordering claim for the repair to propagate. That is the
    // whole of this test's intermittency; it was never the repair.
    {
        let dt = |i: i64| chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS + (6 + i) * sec).unwrap();
        let values = (0..3i64)
            .map(|i| {
                format!(
                    "('e2e_project', '{}', '{}', 's-{}', 'span', 'OK', 'm', 'INFO', ARRAY[]::text[], ARRAY['s'])",
                    dt(i).date_naive(),
                    dt(i).format("%Y-%m-%d %H:%M:%S%.f"),
                    6 + i
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        client
            .execute(
                &format!(
                    "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) VALUES {values}"
                ),
                &[],
            )
            .await?;
    }
    env.advance(Duration::from_secs(bucket_secs * 2));
    env.force_flush().await?;

    // Fail on the FIXTURE, not on the plan, when the shape is wrong: "3 files
    // where 2 were intended" is a different bug from "the claim was lost", and
    // reading it off a physical plan cost hours.
    {
        let t = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
        let n = t.read().await.snapshot().map_or(0, |s| s.log_data().iter().count());
        anyhow::ensure!(n == 2, "fixture must be one compacted non-conforming file beside one conforming flush, got {n} files");
    }

    let sql = "SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp DESC LIMIT 3";
    let plan: String = client
        .query(&format!("EXPLAIN {sql}"), &[])
        .await?
        .iter()
        .map(|r| (0..r.len()).map(|c| r.try_get::<_, String>(c).unwrap_or_default()).collect::<Vec<_>>().join(" | "))
        .collect::<Vec<_>>()
        .join("\n");
    let ids = client.query(sql, &[]).await?.iter().map(|r| r.get::<_, String>(0)).collect();
    // The fixture's own shape, from the Delta log. A plan missing the ordering
    // claim is a SYMPTOM; whether the fixture even built the two-file
    // conforming/non-conforming pair it intends is the thing to check first.
    let table = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let files = {
        let t = table.read().await;
        t.snapshot().map_or_else(
            |e| format!("no snapshot: {e}"),
            |s| {
                s.log_data()
                    .iter()
                    .map(|f| {
                        // numRecords and the min/max timestamp, not size: the otel schema's
                        // footer dominates a small file, so a 3-row and a 6-row file weigh
                        // the same and size cannot tell the fixture's shapes apart.
                        let stats = f.stats().map_or_else(|| "no stats".into(), |s| s.to_string());
                        format!("    ...{} {}", f.path().chars().rev().take(20).collect::<String>().chars().rev().collect::<String>(), stats)
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            },
        )
    };
    Ok((plan, ids, files))
}

// One non-conforming file must not cost the conforming majority its ordering
// claim — the 30-day `log_list` mechanism.
//
// The delta-rs fork deliberately ISOLATES files whose footer does not declare the
// scan's common ordering: the conforming files keep the `[timestamp DESC]` claim
// in one `DataSourceExec` and the rest are unioned in as a sibling with no claim
// ("isolate, don't surrender"). But a `UnionExec` advertises an ordering only when
// EVERY child does, so the Delta leg as a whole then advertises none — `DedupExec`
// falls to its unbounded `full-set` mode and `ORDER BY timestamp DESC LIMIT n`
// stays a BLOCKING `SortExec` over the whole window. A 251-row newest-first
// listing therefore READS every selected byte: prod p1 at 30 d selected 431 GB for
// 251 rows, while the same query at 14 d (all files conforming) streamed in 1.6 s.
//
// Both directions are asserted. Budget 0 is not a separate early return in
// `repair_isolated_scan_ordering` — it flows through the same `bytes <= max_bytes`
// test — so pinning the declining case also pins the comparison's direction.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn one_unsorted_file_does_not_cost_the_majority_its_ordering() -> anyhow::Result<()> {
    let (plan, ids, files) = isolated_union_plan(64).await?;
    assert!(
        plan.contains("SortPreservingMergeExec"),
        "one unsorted file must not disable the streaming merge for the conforming majority.\nlive files:\n{files}\nplan was:\n{plan}"
    );
    // The same lost claim is what drops `DedupExec` into its unbounded mode.
    assert!(!plan.contains("mode=full-set"), "DedupExec must stay bounded once the ordering is restored; plan was:\n{plan}");
    assert_eq!(ids, vec!["s-8", "s-7", "s-6"], "wrong top-n or order; plan:\n{plan}");

    // Budget 0: the repair must decline, restoring the un-repaired plan exactly —
    // this is the guard that keeps a whole-window leg from ever being sorted.
    let (off, off_ids, _off_files) = isolated_union_plan(0).await?;
    assert!(!off.contains("SortPreservingMergeExec") && off.contains("mode=full-set"), "budget 0 must leave the plan un-repaired; plan was:\n{off}");
    assert_eq!(off_ids, ids, "declining the repair must not change the answer");
    Ok(())
}
