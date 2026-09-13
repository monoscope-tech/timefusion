//! The wide-scan admission gate must key on selected work (file count / bytes),
//! not on lookback depth: the tests below pair queries of identical depth that
//! differ only in how much the scan actually selects.

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};
use super::ordering_pushdown::{explain, hot_partition_builder};

const HOUR: i64 = 3_600_000_000;
const BUCKET_SECS: u64 = 60;

/// Flushes `files` files an hour apart from `base`, `rows` rows each, ids `{prefix}-{file}-{row}`.
async fn flush_hourly_files(env: &E2eEnv, client: &tokio_postgres::Client, prefix: &str, base: i64, files: i64, rows: i64) -> anyhow::Result<()> {
    for f in 0..files {
        for i in 0..rows {
            insert_at(client, &format!("{prefix}-{f}-{i}"), base + f * HOUR + i * 1_000_000).await?;
        }
        env.advance(Duration::from_secs(BUCKET_SECS * 2));
        env.force_flush().await?;
    }
    Ok(())
}

fn assert_gated(plan: &str, gated: bool, why: &str) {
    assert_eq!(plan.contains("GatedScanExec"), gated, "{why} Plan was:\n{plan}");
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn deep_but_well_pruned_scan_is_not_gated_while_a_many_file_scan_still_is() -> anyhow::Result<()> {
    // Pin the file budget the test reasons about; the prod default is far too
    // high for 12 e2e files to trip.
    let env = hot_partition_builder()
        // Keep the deliberately fragmented 12-file fixture: this test inspects
        // the pre-compaction plan.
        .without_light_optimize()
        .with_wide_scan_max_files(8)
        .start()
        .await?;
    let client = env.pg_client().await?;

    // One file per flush, an hour apart; 12 files exceed the 8-file budget.
    let base = FROZEN_START_MICROS - 40 * HOUR;
    flush_hourly_files(&env, &client, "g", base, 12, 3).await?;

    // Deep and wide: reaches back 40h and selects every file.
    let wide = "SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' \
                AND timestamp > now() - interval '40 hours' ORDER BY timestamp DESC LIMIT 50";
    // Deep and narrow: the same 40h lookback, but an upper bound prunes it to ~one file.
    let narrow = &format!(
        "SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' \
         AND timestamp > now() - interval '40 hours' AND timestamp < {} ORDER BY timestamp DESC LIMIT 50",
        format_args!("to_timestamp_micros({})", base + HOUR + 500_000)
    );

    assert_gated(
        &explain(&client, wide).await?,
        true,
        "a deep scan that actually selects 12 files must stay gated — this is the OOM guard from 2026-07-20 and \
         relaxing it for well-pruned queries must not disarm it.",
    );
    assert_gated(
        &explain(&client, narrow).await?,
        false,
        "a scan at the SAME 40h depth that prunes to a single file must NOT be gated: it decodes almost nothing, \
         so gating only queues it behind a shared 16-permit semaphore. In prod that cost 40-57s for a query that \
         read ONE file and 8.24 KB.",
    );

    let rows: Vec<String> = client.query(narrow, &[]).await?.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(rows, vec!["g-1-0", "g-0-2", "g-0-1", "g-0-0"], "ungating must not alter results or ordering");

    Ok(())
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn deep_single_file_over_the_byte_budget_is_gated() -> anyhow::Result<()> {
    let env = hot_partition_builder()
        .with_wide_scan_max_files(usize::MAX)
        // Zero makes any non-empty selected file exceed the byte exemption,
        // exercising the byte branch with a tiny fixture.
        .with_wide_scan_max_mb(0)
        .start()
        .await?;
    let client = env.pg_client().await?;
    flush_hourly_files(&env, &client, "byte-budget", FROZEN_START_MICROS - 40 * HOUR, 1, 1).await?;

    let query = "SELECT id FROM otel_logs_and_spans WHERE project_id = 'e2e_project' \
                 AND timestamp > now() - interval '41 hours' ORDER BY timestamp DESC LIMIT 1";
    assert_gated(
        &explain(&client, query).await?,
        true,
        "a deep scan whose selected bytes exceed the exemption must share the decode gate even when it selects one file.",
    );
    assert_eq!(client.query_one(query, &[]).await?.get::<_, String>(0), "byte-budget-0-0");
    Ok(())
}
