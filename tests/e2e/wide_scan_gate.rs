//! The wide-scan admission gate must key on WORK, not on lookback depth.
//!
//! `GatedScanExec` bounds concurrent Parquet decode across all queries, because
//! decode heap is untracked by the DataFusion memory pool and one 7-day
//! dashboard at ~48-way parallelism OOM-restarted prod (2026-07-20). That guard
//! is real and must stay.
//!
//! But it was armed by lookback DEPTH alone, and depth stops predicting decode
//! cost once file pruning works. Prod 2026-08-01, same tenant, same
//! `ORDER BY timestamp DESC LIMIT 50`, only the window changed:
//!
//!   115 min ->    255 ms, 422 ms
//!   125 min -> 39_808 ms, 57_268 ms
//!
//! `EXPLAIN ANALYZE` on the slow one showed a perfectly good plan —
//! `mode=bounded`, declared `output_ordering`, ONE file, 8.24 KB scanned, every
//! metric in microseconds — wrapped in `GatedScanExec: permits=0`. It was doing
//! no work; it was queued ~40s behind a saturated 16-permit semaphore, because
//! crossing `timefusion_wide_scan_lookback_hours` (2) is all it took.
//!
//! Both queries below have the SAME lookback depth, so depth cannot distinguish
//! them. Only the selected file count differs — which is exactly the signal the
//! gate now reads.

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

const HOUR: i64 = 3_600_000_000;
const BUCKET_SECS: u64 = 60;

async fn explain(client: &tokio_postgres::Client, sql: &str) -> anyhow::Result<String> {
    Ok(client
        .query(&format!("EXPLAIN {sql}"), &[])
        .await?
        .iter()
        .map(|r| (0..r.len()).map(|c| r.try_get::<_, String>(c).unwrap_or_default()).collect::<Vec<_>>().join(" | "))
        .collect::<Vec<_>>()
        .join("\n"))
}

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
    // Pin the file budget the test reasons about: the prod default moved to
    // 256 files (2026-08-02), which 12 e2e files can never trip.
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(BUCKET_SECS))
        .with_retention(Duration::from_secs(60 * 60))
        // Preserve the deliberately fragmented 12-file fixture. Source
        // compaction has its own coordinator tests; this test isolates scan
        // admission and must inspect the pre-compaction plan.
        .without_light_optimize()
        .with_wide_scan_max_files(8)
        .start()
        .await?;
    let client = env.pg_client().await?;

    // One file per flush, each an hour apart, all deep enough to cross the
    // lookback rule; 12 files exceed the 8-file budget, so the unpruned query
    // must still be gated.
    let base = FROZEN_START_MICROS - 40 * HOUR;
    flush_hourly_files(&env, &client, "g", base, 12, 3).await?;

    // Deep AND wide: reaches back 40h and selects every file.
    let wide = "SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' \
                AND timestamp > now() - interval '40 hours' ORDER BY timestamp DESC LIMIT 50";
    // Deep AND narrow: the SAME 40h lookback — so identical depth, and the old
    // depth-only rule gated it — but an upper bound prunes it to ~one file.
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

    // The relaxation must not have changed what the query returns.
    let rows: Vec<String> = client.query(narrow, &[]).await?.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(rows, vec!["g-1-0", "g-0-2", "g-0-1", "g-0-0"], "ungating must not alter results or ordering");

    Ok(())
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn deep_single_file_over_the_byte_budget_is_gated() -> anyhow::Result<()> {
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(BUCKET_SECS))
        .with_retention(Duration::from_secs(60 * 60))
        .with_wide_scan_max_files(usize::MAX)
        // Zero makes any non-empty selected file exceed the byte exemption.
        // This keeps the fixture tiny while exercising the same branch as the
        // 222 MB historical production file that decoded to ~4 GiB of heap.
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
