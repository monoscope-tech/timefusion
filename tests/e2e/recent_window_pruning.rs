//! Guards that parquet predicate pushdown survives on a compacted, sorted file:
//! enabling the Deletion-Vectors table feature must not disable pushdown
//! table-wide (the per-file `has_selection_vectors` guard is the correct scope).

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};
use super::ordering_pushdown::{flat_rows, frozen_date, hot_partition_builder};

const BUCKET_SECS: u64 = 60;
const SEC: i64 = 1_000_000;

/// Format a micros timestamp as the SQL literal the queries compare against.
fn ts(micros: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_micros(micros).unwrap().format("%Y-%m-%d %H:%M:%S%.f").to_string()
}

/// Insert `chunks * 100` rows 1s apart from `FROZEN_START_MICROS`, flushing after each
/// chunk so several Delta files exist, then drain the MemBuffer so the query hits Delta
/// only (no unordered mem branch).
async fn seed_flushed_chunks(env: &E2eEnv, client: &tokio_postgres::Client, chunks: i64) -> anyhow::Result<()> {
    for idx in 0..chunks * 100 {
        insert_at(client, &format!("r-{idx:04}"), FROZEN_START_MICROS + idx * SEC).await?;
        if idx % 100 == 99 {
            env.advance(Duration::from_secs(BUCKET_SECS * 2));
            env.force_flush().await?;
        }
    }
    env.advance(Duration::from_secs(60 * 61));
    env.force_evict().await?;
    Ok(())
}

/// Parse a scalar DataSourceExec metric `name=N` (first digit run after `name`).
fn scan_metric(plan: &str, name: &str) -> Option<i64> {
    let i = plan.rfind(name)?;
    plan[i + name.len()..].split(|c: char| !c.is_ascii_digit()).find(|s| !s.is_empty())?.parse().ok()
}

async fn explain_analyze(client: &tokio_postgres::Client, sql: &str) -> anyhow::Result<String> {
    flat_rows(client, &format!("EXPLAIN ANALYZE {sql}")).await
}

/// Convertible conjuncts must still reach the parquet scan when a
/// non-convertible `text_match` conjunct rides along in the same AND.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn text_match_conjunct_does_not_poison_parquet_pushdown() -> anyhow::Result<()> {
    let env = hot_partition_builder()
        .with_page_row_count_limit(50)
        // Off for a deterministic file list: the sidecar index is built by a
        // detached task, so the prefilter may or may not prune every file by
        // query time. It is irrelevant to what is under guard here.
        .with_tantivy_prefilter(false)
        .start()
        .await?;
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;

    seed_flushed_chunks(&env, &client, 2).await?;

    let start_ts = ts(FROZEN_START_MICROS);
    // Explicit text_match mirrors what the tantivy rewrite injects, without
    // depending on the optimizer rule firing in this harness.
    let sql = format!(
        "SELECT count(*) FROM otel_logs_and_spans WHERE project_id = 'e2e_project' \
         AND name = 'no-such-name' AND text_match(name, 'no-such-name') AND timestamp >= '{start_ts}'"
    );
    let matched: i64 = client.query_one(&sql, &[]).await?.get(0);
    assert_eq!(matched, 0);

    let plan = explain_analyze(&client, &sql).await?;
    // The equality must be applied AT the scan, not above the dedup: pushed, it
    // eliminates every row and DedupExec sees nothing.
    let dedup_line = plan.lines().find(|l| l.contains("DedupExec")).unwrap_or_default();
    let dedup_input = scan_metric(dedup_line, "input_rows=").unwrap_or(i64::MAX);
    assert_eq!(
        dedup_input, 0,
        "rows reached DedupExec — the name equality was not applied at the parquet scan \
         (the text_match conjunct poisoned the delta leg's predicate).\nplan:\n{plan}"
    );
    // Companion guard: an empty leg must be dropped before the union rather than
    // vetoing the declared ordering, which would drop DedupExec to full-set.
    // The DataSourceExec check first distinguishes "no leg at all" from a regression.
    assert!(plan.contains("DataSourceExec"), "no file was scanned at all, so this run proves nothing about leg ordering.\nplan:\n{plan}");
    assert!(dedup_line.contains("bounded["), "DedupExec fell to full-set — an empty leg vetoed the declared ordering.\nplan:\n{plan}");
    Ok(())
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn recent_window_prunes_within_compacted_file() -> anyhow::Result<()> {
    // Small pages (50 rows) so ~600 rows → ~12 pages in one row group.
    // Deletion Vectors stay ON (harness default) — that is what is under guard.
    let env = hot_partition_builder().with_optimize_sort_by().with_page_row_count_limit(50).start().await?;
    // The test asserts on its own `compact_date` call; the background coordinator
    // must not consume the same files first.
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;

    // Flushed in chunks so several Delta files exist for compaction to merge.
    let total_rows = 600i64;
    seed_flushed_chunks(&env, &client, total_rows / 100).await?;

    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let (removed, added) = env.db().compact_date(&table_ref, "otel_logs_and_spans", frozen_date(), None).await?;
    assert!(removed >= 2 && added >= 1, "compaction should merge files (removed={removed}, added={added})");

    // Narrow trailing window: newest ~50 rows of the 600s span.
    let cutoff_ts = ts(FROZEN_START_MICROS + (total_rows - 50) * SEC);
    let sql = format!("SELECT count(*) FROM otel_logs_and_spans WHERE project_id = 'e2e_project' AND timestamp > '{cutoff_ts}'");

    let matched: i64 = client.query_one(&sql, &[]).await?.get(0);
    assert_eq!(matched, 49, "window should select 49 rows (> cutoff), got {matched}");

    let plan = explain_analyze(&client, &sql).await?;
    // The signal is rows actually read from parquet: with the predicate pushed,
    // page-index + row pushdown skip all but the newest rows.
    let scanned = scan_metric(&plan, "output_rows=").unwrap_or(total_rows);
    let pushdown_pruned = scan_metric(&plan, "pushdown_rows_pruned=").unwrap_or(0);

    assert!(
        pushdown_pruned > 0,
        "predicate was not pushed into the parquet scan (pushdown_rows_pruned=0); \
         the Deletion-Vectors feature gate disabled parquet pushdown.\nplan:\n{plan}"
    );
    assert!(scanned < total_rows / 2, "scan read {scanned}/{total_rows} rows for a 49-row window — pruning not effective.\nplan:\n{plan}");

    Ok(())
}
