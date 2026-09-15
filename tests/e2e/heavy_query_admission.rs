//! E2E: heavy-query admission — the "manage many connections" property (#304).
//!
//! Many concurrent pgwire clients issuing an unbounded `ORDER BY` (the
//! pool-exhausting shape) are QUEUED to K, never rejected: every client
//! completes with the full result, the gate admits each heavy query EXACTLY
//! once, and at least one waited for a slot. This exercises the real pgwire
//! session path — the wiring the unit tests (which use a one-partition
//! `EmptyExec`) cannot see: that the rule is registered only when the flag is
//! on, wraps the true root, and acquires one permit per query, not per
//! partition.

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

/// Flag ON: 16 clients, K∈{4,8} (pool-derived, floored at 4) < 16, so most queue.
/// All complete; admitted moves by exactly 16; at least one waited.
#[tokio::test(flavor = "multi_thread")]
async fn heavy_admission_queues_concurrent_clients_when_on() -> Result<()> {
    init_local_metrics_for_test();
    let env = E2eEnv::builder().with_heavy_query_admission().start().await?;
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

/// Flag OFF (prod default): the rule is never installed, so the gate is inert —
/// the counter does not move. This is the falsifiable evidence behind the
/// "flag off = zero prod behavior change" claim.
#[tokio::test(flavor = "multi_thread")]
async fn heavy_admission_is_inert_when_off() -> Result<()> {
    init_local_metrics_for_test();
    let env = E2eEnv::builder().start().await?;
    let n = seed(&env, 200).await?;

    let admitted0 = counter_value(scan_metric_names::HEAVY_QUERY_ADMITTED);
    let counts = fire_all(&env).await?;

    assert!(counts.iter().all(|&c| c == n), "all clients still succeed, ungated: {counts:?}");
    assert_eq!(counter_value(scan_metric_names::HEAVY_QUERY_ADMITTED) - admitted0, 0, "flag off: the admission rule is never registered on the session",);
    Ok(())
}
