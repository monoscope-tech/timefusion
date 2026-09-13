//! Timestamp-ordering + LIMIT pushdown: `ORDER BY timestamp DESC LIMIT n` must
//! plan as a streaming `SortPreservingMergeExec` over `mem ∪ delta`, not a
//! blocking `SortExec` over the whole window. Asserts plan shape and correctness.

use std::time::Duration;

use tokio_postgres::Client;

use super::harness::{E2eEnv, E2eEnvBuilder, FROZEN_START_MICROS, insert_at};

const SEC: i64 = 1_000_000;
const BUCKET_SECS: u64 = 60;

// Helpers shared with the sibling e2e scenarios, reached as
// `super::ordering_pushdown::<name>`.

/// Run `stmt` and flatten every result row to one `a | b | c` line.
pub async fn flat_rows(client: &Client, stmt: &str) -> anyhow::Result<String> {
    Ok(client
        .query(stmt, &[])
        .await?
        .iter()
        .map(|r| (0..r.len()).map(|c| r.try_get::<_, String>(c).unwrap_or_default()).collect::<Vec<_>>().join(" | "))
        .collect::<Vec<_>>()
        .join("\n"))
}

/// The physical plan for `sql`, flattened to one string per EXPLAIN row.
pub async fn explain(client: &Client, sql: &str) -> anyhow::Result<String> {
    flat_rows(client, &format!("EXPLAIN {sql}")).await
}

/// Rows visible for one project.
pub async fn count_rows(client: &Client, project: &str) -> anyhow::Result<i64> {
    Ok(client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&project]).await?.get(0))
}

/// Advance a day past retention and evict, so MemBuffer is empty and any read
/// that follows is served purely from Delta.
pub async fn drain_membuffer(env: &E2eEnv) -> anyhow::Result<()> {
    env.advance(Duration::from_secs(24 * 60 * 60));
    env.force_evict().await?;
    let mem = env.snapshot_stats().mem_total_rows;
    anyhow::ensure!(mem == 0, "MemBuffer not drained ({mem} rows left) — the read below would not isolate Delta");
    Ok(())
}

/// One-minute buckets, one hour of retention: the shared "hot partition" fixture.
pub fn hot_partition_builder() -> E2eEnvBuilder {
    E2eEnv::builder().with_bucket_duration(Duration::from_secs(BUCKET_SECS)).with_retention(Duration::from_secs(60 * 60))
}

/// The partition date the frozen clock writes into.
pub fn frozen_date() -> chrono::NaiveDate {
    chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap().date_naive()
}

/// `flushes` Delta files of 5 `old-*` rows each, plus 5 newer `new-*` rows left
/// in the current (open) MemBuffer bucket — so a query with no lower time bound
/// spans MemBuffer ∪ Delta, across multiple ordered Delta readers.
async fn mem_and_delta_env(flushes: i64) -> anyhow::Result<(E2eEnv, Client)> {
    let env = hot_partition_builder().start().await?;
    let client = env.pg_client().await?;
    for batch in 0..flushes {
        let base = FROZEN_START_MICROS + batch * (BUCKET_SECS as i64) * 3 * SEC;
        for i in 0..5 {
            insert_at(&client, &format!("old-{}", batch * 5 + i), base + i * SEC).await?;
        }
        env.advance(Duration::from_secs(BUCKET_SECS * 3));
        env.force_flush().await?;
    }
    let new_base = FROZEN_START_MICROS + (BUCKET_SECS as i64) * 3 * flushes * SEC;
    for i in 0..5 {
        insert_at(&client, &format!("new-{i}"), new_base + i * SEC).await?;
    }
    Ok((env, client))
}

/// A large expression projection (jsonb_build_array, casts, coalesce) must not
/// block the streaming/top-K plan for a newest-first listing.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn the_monoscope_log_explorer_listing_streams() -> anyhow::Result<()> {
    let (_env, client) = mem_and_delta_env(1).await?;

    let sql = "SELECT jsonb_build_array(id, to_char(timestamp at time zone 'UTC', 'YYYY-MM-DD'), context___trace_id, name, duration, \
               resource___service___name, parent_id, cast(extract(epoch from (start_time)) * 1000 as bigint), \
               coalesce(errors is not null or (kind = 'server' and (lower(level) = 'error' or severity___severity_number >= 17 or status_code = 'ERROR')), false), \
               to_jsonb(summary), context___span_id, kind) \
               FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp DESC LIMIT 3";
    let plan = explain(&client, sql).await?;

    assert!(
        plan.contains("SortPreservingMergeExec") || plan.contains("TopK"),
        "the log-explorer listing must stream or top-K, not materialise the window; plan was:\n{plan}"
    );
    assert!(!plan.contains("mode=full-set"), "DedupExec must stay bounded for this listing; plan was:\n{plan}");
    Ok(())
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn order_by_ts_desc_limit_merges_mem_and_delta() -> anyhow::Result<()> {
    // Two separate flushes exercise ordered readers across multiple Delta files.
    let (_env, client) = mem_and_delta_env(2).await?;

    // No lower time bound, so the query spans MemBuffer ∪ Delta.
    let sql = "SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp DESC LIMIT 12";

    let plan = explain(&client, sql).await?;
    assert!(plan.contains("SortPreservingMergeExec"), "expected a streaming SortPreservingMergeExec (ordering pushdown fired); plan was:\n{plan}");

    let rows = client.query(sql, &[]).await?;
    let ids: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(
        ids,
        vec!["new-4", "new-3", "new-2", "new-1", "new-0", "old-9", "old-8", "old-7", "old-6", "old-5", "old-4", "old-3"],
        "wrong top-n or wrong order; plan:\n{plan}"
    );

    let ts: Vec<i64> = rows.iter().map(|r| r.get::<_, chrono::DateTime<chrono::Utc>>(1).timestamp_micros()).collect();
    assert!(ts.windows(2).all(|w| w[0] > w[1]), "timestamps not strictly descending: {ts:?}");

    Ok(())
}

// `OptimizeType::SortBy` must leave an honest DESC footer so an optimized
// partition keeps the streaming merge. Uses the specific-date `compact_date`
// because optimize's window keys off the real wall clock, not the virtual clock.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn optimized_partition_still_advertises_desc_ordering() -> anyhow::Result<()> {
    let env = hot_partition_builder().with_optimize_sort_by().start().await?;
    let client = env.pg_client().await?;

    // Flush one bucket at a time: a single force_flush would coalesce into one
    // file, which SortBy skips as a no-op, leaving nothing to test.
    for b in 0..3i64 {
        for i in 0..3i64 {
            let idx = b * 3 + i;
            insert_at(&client, &format!("d-{idx}"), FROZEN_START_MICROS + idx * 20 * SEC).await?;
        }
        env.advance(Duration::from_secs(BUCKET_SECS * 2));
        env.force_flush().await?;
    }

    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let (removed, added) = env.db().compact_date(&table_ref, "otel_logs_and_spans", frozen_date(), None).await?;
    assert!(removed >= 1 && added >= 1, "compaction should have rewritten files (removed={removed}, added={added})");

    // Fresh rows into MemBuffer so the query spans MemBuffer ∪ (optimized) Delta.
    let new_base = FROZEN_START_MICROS + (BUCKET_SECS as i64) * 6 * SEC;
    for i in 0..3 {
        insert_at(&client, &format!("m-{i}"), new_base + i * SEC).await?;
    }

    let sql = "SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp DESC LIMIT 3";
    let plan = explain(&client, sql).await?;
    assert!(plan.contains("SortPreservingMergeExec"), "optimized partition must still advertise DESC ordering (SortBy footer); plan was:\n{plan}");

    let rows = client.query(sql, &[]).await?;
    let ids: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(ids, vec!["m-2", "m-1", "m-0"], "wrong top-n/order after optimize; plan:\n{plan}");

    Ok(())
}

/// Build the fixture at a given repair budget and return `(plan, top-3 ids, files)`.
///
/// Shape: one concatenated file with no declared footer order beside one
/// conforming flush — the fork's isolation case.
async fn isolated_union_plan(budget_mb: u64) -> anyhow::Result<(String, Vec<String>, String)> {
    let env = hot_partition_builder()
        // The fixture must own its flushes: a periodic flush firing mid-fixture
        // splits the last file, so no union child carries the ordering claim.
        .with_flush_interval(Duration::from_secs(3600))
        .with_unordered_leg_sort_max_mb(budget_mb)
        .start()
        .await?;
    let client = env.pg_client().await?;

    for b in 0..2i64 {
        for i in 0..3i64 {
            insert_at(&client, &format!("u-{}", b * 3 + i), FROZEN_START_MICROS + (b * 3 + i) * SEC).await?;
        }
        env.advance(Duration::from_secs(BUCKET_SECS * 2));
        env.force_flush().await?;
    }
    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let (removed, added) = env.db().compact_date(&table_ref, "otel_logs_and_spans", frozen_date(), None).await?;
    assert!(removed >= 2 && added >= 1, "the fixture needs a real concatenation (removed={removed}, added={added})");

    // ONE statement, not three: separate INSERTs make the file count a timing
    // decision, with the split-file consequence described above.
    let values = (0..3i64)
        .map(|i| {
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS + (6 + i) * SEC).unwrap();
            format!(
                "('e2e_project', '{}', '{}', 's-{}', 'span', 'OK', 'm', 'INFO', ARRAY[]::text[], ARRAY['s'])",
                dt.date_naive(),
                dt.format("%Y-%m-%d %H:%M:%S%.f"),
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
    env.advance(Duration::from_secs(BUCKET_SECS * 2));
    env.force_flush().await?;

    // Fail on the FIXTURE, not the plan: "wrong file count" is a different bug
    // from "the ordering claim was lost".
    let n = table_ref.read().await.snapshot().map_or(0, |s| s.log_data().iter().count());
    anyhow::ensure!(n == 2, "fixture must be one compacted non-conforming file beside one conforming flush, got {n} files");

    let sql = "SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp DESC LIMIT 3";
    let plan = explain(&client, sql).await?;
    let ids = client.query(sql, &[]).await?.iter().map(|r| r.get::<_, String>(0)).collect();
    // The fixture's own shape, from the Delta log, for assertion messages.
    let files = {
        let t = table_ref.read().await;
        t.snapshot().map_or_else(
            |e| format!("no snapshot: {e}"),
            |s| {
                s.log_data()
                    .iter()
                    .map(|f| {
                        // numRecords/min/max, not size: the otel footer dominates a small
                        // file, so size cannot tell the fixture's shapes apart.
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
// claim. A `UnionExec` advertises an ordering only when EVERY child does, so an
// isolated no-claim sibling otherwise drops the whole Delta leg's ordering and
// the query degrades to a blocking whole-window `SortExec` plus unbounded dedup.
// Budget 0 flows through the same `bytes <= max_bytes` test as any other budget,
// so pinning the declining case also pins the comparison's direction.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn one_unsorted_file_does_not_cost_the_majority_its_ordering() -> anyhow::Result<()> {
    let (plan, ids, files) = isolated_union_plan(64).await?;
    assert!(
        plan.contains("SortPreservingMergeExec"),
        "one unsorted file must not disable the streaming merge for the conforming majority.\nlive files:\n{files}\nplan was:\n{plan}"
    );
    assert!(!plan.contains("mode=full-set"), "DedupExec must stay bounded once the ordering is restored; plan was:\n{plan}");
    assert_eq!(ids, vec!["s-8", "s-7", "s-6"], "wrong top-n or order; plan:\n{plan}");

    // Budget 0: the repair must decline, keeping a whole-window leg from ever being sorted.
    let (off, off_ids, _off_files) = isolated_union_plan(0).await?;
    assert!(!off.contains("SortPreservingMergeExec") && off.contains("mode=full-set"), "budget 0 must leave the plan un-repaired; plan was:\n{off}");
    assert_eq!(off_ids, ids, "declining the repair must not change the answer");
    Ok(())
}
