//! Regression guard for the "recent 1h window is slow" bug (2026-07-20).
//!
//! A compacted hot partition is one big timestamp-DESC-sorted file (in prod a
//! single 300k-row row group spanning ~4h). A `timestamp > now()-1h` query
//! should read only the newest rows via parquet page-index + row pushdown — the
//! file is sorted and carries a per-page timestamp ColumnIndex. But with the
//! Deletion-Vectors table feature enabled (prod default), the delta-rs scan
//! DROPPED the parquet predicate entirely (`process_filters` gated all parquet
//! pushdown on `!DeletionVectors`), so the scan read every row and filtered in a
//! FilterExec above it — a ~13× over-read. The per-file `has_selection_vectors`
//! guard already skips pushdown for files that actually carry a DV, so the
//! blanket feature-level gate was unnecessary and is what this test locks down.

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

/// Parse a scalar DataSourceExec metric `name=N` (first digit run after `name`).
fn scan_metric(plan: &str, name: &str) -> Option<i64> {
    let i = plan.rfind(name)?;
    plan[i + name.len()..].split(|c: char| !c.is_ascii_digit()).find(|s| !s.is_empty())?.parse().ok()
}

async fn explain_analyze(client: &tokio_postgres::Client, sql: &str) -> anyhow::Result<String> {
    Ok(client
        .query(&format!("EXPLAIN ANALYZE {sql}"), &[])
        .await?
        .iter()
        .map(|r| (0..r.len()).map(|c| r.try_get::<_, String>(c).unwrap_or_default()).collect::<Vec<_>>().join(" | "))
        .collect::<Vec<_>>()
        .join("\n"))
}

/// Regression guard for the text_match conjunction-poisoning bug (2026-08-20).
///
/// The tantivy optimizer injects `text_match(col, val)` beside every string
/// equality. delta-rs' `process_filters` deliberately includes that
/// non-convertible UDF in the combined parquet predicate, and one failed bind
/// of the conjunction discarded the WHOLE predicate — the equality and
/// timestamp bounds too. Prod: a delta-only 4h window emitted 2.11M rows for a
/// trace_id equality matching 0; at 24h the full-set dedup blew its 2 GiB cap.
/// The convertible conjuncts must reach the parquet scan even when a
/// text_match conjunct rides along.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn text_match_conjunct_does_not_poison_parquet_pushdown() -> anyhow::Result<()> {
    let bucket_secs = 60u64;
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(bucket_secs))
        .with_retention(Duration::from_secs(60 * 60))
        .with_page_row_count_limit(50)
        // Deterministic file list. The flush spawns the sidecar tantivy index
        // as a DETACHED task, so whether the index exists by query time is a
        // race the test can neither observe nor await — and once it does, the
        // prefilter finds no hit for `no-such-name` and pushes an empty
        // `id IN ()`, which delta-rs evaluates statically to false and drops
        // EVERY file. The leg becomes `EmptyExec`, which declares no ordering,
        // so `DedupExec` correctly reports `full-set` over zero rows and the
        // assertions below fire on a plan that is optimal.
        //
        // CI 2026-08-24 (run 32772841710) caught the race mid-test: the plain
        // SELECT scanned `add_files_seen=2`, `tantivy_index_built` landed 20 ms
        // later, and the EXPLAIN ANALYZE logged `Predicate statically evaluated
        // to false; skipping all files`. That is the whole flake — the guard
        // was measuring which side of a race it landed on, not a regression.
        //
        // Nothing under guard here is the prefilter's: the bug is delta-rs'
        // `process_filters` composing the parquet predicate, which runs on the
        // original filters (text_match conjunct included) either way.
        .with_tantivy_prefilter(false)
        .start()
        .await?;
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;

    let sec = 1_000_000i64;
    for chunk in 0..2i64 {
        for i in 0..100i64 {
            let idx = chunk * 100 + i;
            insert_at(&client, &format!("r-{idx:04}"), FROZEN_START_MICROS + idx * sec).await?;
        }
        env.advance(Duration::from_secs(bucket_secs * 2));
        env.force_flush().await?;
    }
    // Drain MemBuffer so the query hits Delta only.
    env.advance(Duration::from_secs(60 * 61));
    env.force_evict().await?;

    let start_ts = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap().format("%Y-%m-%d %H:%M:%S%.f");
    // Explicit text_match mirrors what the tantivy rewrite injects, without
    // depending on the optimizer rule firing in this harness.
    let sql = format!(
        "SELECT count(*) FROM otel_logs_and_spans WHERE project_id = 'e2e_project' \
         AND name = 'no-such-name' AND text_match(name, 'no-such-name') AND timestamp >= '{start_ts}'"
    );
    let matched: i64 = client.query_one(&sql, &[]).await?.get(0);
    assert_eq!(matched, 0);

    let plan = explain_analyze(&client, &sql).await?;
    // The invariant is that the equality is applied AT the scan, not above the
    // dedup: with it pushed, row-group stats (or the row filter) eliminate every
    // row and DedupExec sees nothing. Broken (predicate=None on the tantivy
    // split path), the scan decodes the whole window and dedup buffers it —
    // the prod 2 GiB full-set abort shape.
    let dedup_line = plan.lines().find(|l| l.contains("DedupExec")).unwrap_or_default();
    let dedup_input = scan_metric(dedup_line, "input_rows=").unwrap_or(i64::MAX);
    assert_eq!(
        dedup_input, 0,
        "rows reached DedupExec — the name equality was not applied at the parquet scan \
         (the text_match conjunct poisoned the delta leg's predicate).\nplan:\n{plan}"
    );
    // Companion regression: a tantivy-pruned-to-empty leg (EmptyExec, no
    // declared ordering) used to veto the merge requirement, dropping DedupExec
    // to full-set — the mode whose 2 GiB ceiling killed prod point lookups.
    // With empty legs dropped before the union, the ordered delta leg keeps
    // bounded streaming dedup.
    // Belt and braces for the race above: if the Delta leg is ever pruned to
    // nothing again, say so rather than reporting a regression that is not one.
    // There is no ordered leg to preserve when there is no leg at all.
    assert!(plan.contains("DataSourceExec"), "no file was scanned at all, so this run proves nothing about leg ordering.\nplan:\n{plan}");
    assert!(dedup_line.contains("bounded["), "DedupExec fell to full-set — an empty leg vetoed the declared ordering.\nplan:\n{plan}");
    Ok(())
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn recent_window_prunes_within_compacted_file() -> anyhow::Result<()> {
    let bucket_secs = 60u64;
    // Small pages (50 rows) so ~600 rows → ~12 pages in one row group, mirroring
    // prod's single-row-group compacted file but with fine page granularity.
    // Deletion Vectors stay ON (harness default = prod config) — the bug.
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(bucket_secs))
        .with_retention(Duration::from_secs(60 * 60))
        .with_optimize_sort_by()
        .with_page_row_count_limit(50)
        .start()
        .await?;
    // The test invokes `compact_date` itself and asserts that call's actions.
    // Do not let the background coordinator consume the same files first.
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;

    // 600 rows spanning ~10 minutes (1s apart), flushed in chunks so several
    // Delta files exist for the compaction to merge into one sorted file.
    let sec = 1_000_000i64;
    let total_rows = 600i64;
    for chunk in 0..6i64 {
        for i in 0..100i64 {
            let idx = chunk * 100 + i;
            insert_at(&client, &format!("r-{idx:04}"), FROZEN_START_MICROS + idx * sec).await?;
        }
        env.advance(Duration::from_secs(bucket_secs * 2));
        env.force_flush().await?;
    }
    // Drain MemBuffer so the query hits Delta only (no unordered mem branch).
    env.advance(Duration::from_secs(60 * 61));
    env.force_evict().await?;

    // Compact → one timestamp-DESC-sorted file, one row group, ~12 pages.
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap().date_naive();
    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let (removed, added) = env.db().compact_date(&table_ref, "otel_logs_and_spans", date, None).await?;
    assert!(removed >= 2 && added >= 1, "compaction should merge files (removed={removed}, added={added})");

    // Narrow trailing window: newest ~50 rows (last ~50s of the 600s span).
    let cutoff = FROZEN_START_MICROS + (total_rows - 50) * sec;
    let cutoff_ts = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(cutoff).unwrap().format("%Y-%m-%d %H:%M:%S%.f");
    let sql = format!("SELECT count(*) FROM otel_logs_and_spans WHERE project_id = 'e2e_project' AND timestamp > '{cutoff_ts}'");

    // Sanity: the window really is ~50 rows out of 600.
    let matched: i64 = client.query_one(&sql, &[]).await?.get(0);
    assert_eq!(matched, 49, "window should select 49 rows (> cutoff), got {matched}");

    let plan = explain_analyze(&client, &sql).await?;
    // The definitive signal is rows actually read from parquet: with the
    // timestamp predicate pushed into the scan, page-index + row pushdown skip
    // all but the newest rows. Without pushdown (the bug) the scan reads every
    // row and filters above it.
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
