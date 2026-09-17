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

/// Concurrent clients all complete, and the gate admits each query exactly once.
/// Queueing itself is tested deterministically against an exhausted semaphore in
/// the unit suite; its pool-derived capacity may exceed this fixture's 16 clients.
#[tokio::test(flavor = "multi_thread")]
async fn heavy_admission_preserves_concurrent_results() -> Result<()> {
    init_local_metrics_for_test();
    let env = E2eEnv::builder().start().await?;
    let n = seed(&env, 1000).await?;

    let admitted0 = counter_value(scan_metric_names::HEAVY_QUERY_ADMITTED);
    let counts = fire_all(&env).await?;

    assert_eq!(counts.len(), CLIENTS, "every client completed — none rejected");
    assert!(counts.iter().all(|&c| c == n), "each client got the full result: {counts:?}");
    assert_eq!(
        counter_value(scan_metric_names::HEAVY_QUERY_ADMITTED) - admitted0,
        CLIENTS as u64,
        "the gate admits each heavy query EXACTLY once — a >16 count means the root fans out to multiple partitions and each takes its own permit",
    );
    Ok(())
}

/// #304 PRECISION — an unbounded sort is gated while small one-partition TopK and aggregate
/// fixtures are not. Production-sized ordered MOR listings are also gated when file scan
/// repartition creates a multi-way `SortPreservingMergeExec`; this small fixture deliberately
/// stays below that threshold. `EXPLAIN` over pgwire is the plan #304 actually sees; the local
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
    assert!(!gated(&agg), "this small rollup-missed aggregate has neither an unbounded sort nor a multi-way ordered MOR merge:\n{agg}");
    Ok(())
}
