//! E2E: heavy-query admission — the "manage many connections" property (#304).
//!
//! Many concurrent pgwire clients issuing an unbounded `ORDER BY` (the
//! pool-exhausting shape) are QUEUED to K, never rejected: every client
//! completes with the full result, the gate admits each heavy query EXACTLY
//! once, and at least one waited for a slot. This exercises the real pgwire
//! session path — the wiring the unit tests (which use a one-partition
//! `EmptyExec`) cannot see: that the rule is registered on the pgwire session,
//! wraps the true root, and acquires one permit per query, not per partition.

use anyhow::Result;
use futures::future::try_join_all;
use timefusion::{
    database::scan_metric_names,
    observability::{counter_value, init_local_metrics_for_test},
};

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

const CLIENTS: usize = 16;
/// Unbounded sort (no LIMIT) over the flushed rows — plans a spilling `SortExec`,
/// which is exactly what `contains_spilling_sort` gates.
const HEAVY_SQL: &str = "SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp";

/// Seed `rows` and flush them to Delta so the sort scans real files (a heavier,
/// slower plan than a MemBuffer read — keeps the queued window non-racy).
async fn seed(env: &E2eEnv, rows: usize) -> Result<usize> {
    let client = env.pg_client().await?;
    for i in 0..rows {
        insert_at(&client, &format!("r{i}"), FROZEN_START_MICROS + i as i64 * 1_000_000).await?;
    }
    env.force_flush().await?;
    Ok(rows)
}

async fn fire_all(env: &E2eEnv) -> Result<Vec<usize>> {
    try_join_all((0..CLIENTS).map(|_| async move {
        let client = env.pg_client().await?;
        Result::<usize>::Ok(client.query(HEAVY_SQL, &[]).await?.len())
    }))
    .await
}

/// 16 clients, K∈{4,8} (pool-derived, floored at 4) < 16, so most queue.
/// All complete; admitted moves by exactly 16; at least one waited.
#[tokio::test(flavor = "multi_thread")]
async fn heavy_admission_queues_concurrent_clients() -> Result<()> {
    init_local_metrics_for_test();
    let env = E2eEnv::builder().start().await?;
    let n = seed(&env, 1000).await?;

    let admitted0 = counter_value(scan_metric_names::HEAVY_QUERY_ADMITTED);
    let queued0 = counter_value(scan_metric_names::HEAVY_QUERY_QUEUED);
    let counts = fire_all(&env).await?;

    assert_eq!(counts.len(), CLIENTS, "every client completed — none rejected");
    assert!(counts.iter().all(|&c| c == n), "each client got the full result: {counts:?}");
    assert_eq!(
        counter_value(scan_metric_names::HEAVY_QUERY_ADMITTED) - admitted0,
        CLIENTS as u64,
        "the gate admits each heavy query EXACTLY once — a >16 count means the root fans out to multiple partitions and each takes its own permit",
    );
    assert!(counter_value(scan_metric_names::HEAVY_QUERY_QUEUED) - queued0 >= 1, "with K < {CLIENTS} at least one query must have waited for a slot",);
    Ok(())
}

/// #304 PRECISION — the gate catches ONLY genuinely unbounded sorts. A bounded list query is
/// TopK (not gated); an unbounded `ORDER BY` is gated; and — the case that could have made the
/// gate dangerous — a rollup-MISSED aggregate is NOT gated. One might fear its dedup adds an
/// unbounded sort (its outer `LIMIT` sits above the `GROUP BY`, so it can't push into the
/// input), which at a low rollup hit rate would throttle dashboard histograms, not just raw
/// scans. This test proves it does not: dedup is a bounded `DedupExec` (not a SortExec) and the
/// GROUP BY is hash-aggregated. `EXPLAIN` over pgwire is the plan #304 actually sees; the local
/// `ctx.sql` path re-runs optimizer passes the pgwire path skips (CLAUDE.md).
#[tokio::test(flavor = "multi_thread")]
async fn the_gate_catches_unbounded_sorts_but_not_bounded_topk() -> Result<()> {
    async fn explain(client: &tokio_postgres::Client, sql: &str) -> Result<String> {
        Ok(client.query(&format!("EXPLAIN {sql}"), &[]).await?.iter().map(|r| r.get::<_, String>(1)).collect::<Vec<_>>().join("\n"))
    }
    let env = E2eEnv::builder().start().await?;
    seed(&env, 1000).await?;
    let client = env.pg_client().await?;
    let base = "FROM otel_logs_and_spans WHERE project_id = 'e2e_project'";
    let list = explain(&client, &format!("SELECT id, timestamp {base} ORDER BY timestamp DESC LIMIT 251")).await?;
    let unbounded = explain(&client, &format!("SELECT id, timestamp {base} ORDER BY timestamp")).await?;
    let agg = explain(&client, &format!("SELECT time_bucket('60 seconds', timestamp) tb, count(*) {base} GROUP BY tb ORDER BY tb DESC LIMIT 500")).await?;
    let gated = |p: &str| p.contains("AdmissionExec");
    assert!(gated(&unbounded), "an unbounded ORDER BY (spilling sort) must be gated:\n{unbounded}");
    assert!(!gated(&list), "a bounded TopK list query must NOT be gated:\n{list}");
    // The load-bearing precision check: a rollup-MISSED aggregate is NOT gated. Its dedup is
    // a custom bounded `DedupExec` (consumes pre-sorted parquet — NOT a SortExec), the GROUP
    // BY is hash-aggregated (`UnorderedAggregateInput`, no sort), and the only sort is the
    // outer `ORDER BY … LIMIT` TopK (fetch=Some). So even at a low rollup hit rate the gate
    // does not throttle dashboard histograms — only genuinely unbounded ORDER BY queries.
    assert!(!gated(&agg), "a rollup-missed aggregate has no unbounded sort (DedupExec, not SortExec) and must NOT be gated:\n{agg}");
    Ok(())
}
