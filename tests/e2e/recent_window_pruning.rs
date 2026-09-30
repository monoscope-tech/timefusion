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

/// A past partition, so `dedup_partition` clears its sealed-chunk guard.
const PAST: i64 = 1_735_689_600_000_000;

/// Insert span `r-{idx}` at `PAST + idx` seconds; every tenth is a server span.
async fn insert_past_span(client: &tokio_postgres::Client, idx: i64, name: &str) -> Result<u64, tokio_postgres::Error> {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(PAST + idx * SEC).unwrap();
    let kind = if idx % 10 == 0 { "server" } else { "internal" };
    client
        .execute(
            &format!(
                "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, kind, status_code, status_message, level, hashes, summary) \
                 VALUES ('e2e_project', '{}', '{}', $1, $2, $3, 'OK', 'm', 'INFO', ARRAY[]::text[], $4)",
                dt.date_naive(),
                dt.format("%Y-%m-%d %H:%M:%S%.f"),
            ),
            &[&format!("r-{idx:04}"), &name, &kind, &vec!["s"]],
        )
        .await
}

/// Dedup the `PAST` partition, expecting exactly one masked row; returns (files, DV-bearing files).
async fn dedup_past_partition(env: &E2eEnv) -> anyhow::Result<(usize, usize)> {
    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let date = chrono::DateTime::from_timestamp_micros(PAST).unwrap().date_naive();
    let (dropped, _) = env.db().dedup_partition(&table_ref, "otel_logs_and_spans", "e2e_project", date).await?;
    assert_eq!(dropped, 1);
    let t = table_ref.read().await;
    let files: Vec<_> = t.snapshot()?.snapshot().log_data().iter().map(|f| f.deletion_vector_descriptor().is_some()).collect();
    Ok((files.len(), files.iter().filter(|d| **d).count()))
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

/// One DV-bearing file in a scan used to strip the parquet predicate from EVERY file
/// in it, the timestamp bound included: DV keep-masks are positional, so the fork
/// withheld pushdown scan-wide instead of only from the masked files. Prod (2026-09-28)
/// decoded 4.94M rows to keep 2.18K on a 6h window once dedup had masked a file in
/// today's partition. The query is the prod probe, `text_match`-inside-OR included.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dv_bearing_file_keeps_parquet_pushdown_on_its_siblings() -> anyhow::Result<()> {
    // The eviction tick also flushes completed buckets (every PAST bucket is), so at the
    // harness's 1s default it splits the fixture's files wherever it lands.
    let env = hot_partition_builder()
        .with_deletion_vectors()
        .with_flush_interval(Duration::from_secs(3600))
        .with_eviction_interval(Duration::from_secs(3600))
        .with_tantivy_prefilter(false)
        .start()
        .await?;
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;

    let insert = async |idx: i64, name: &str| insert_past_span(&client, idx, name).await;
    // Two files of 100 rows; the first also holds a same-key, different-content copy of
    // a row in the second, which dedup then masks with a deletion vector.
    for idx in 0..100 {
        insert(idx, "span").await?;
    }
    insert(150, "span-v2").await?;
    env.force_flush().await?;
    for idx in 100..200 {
        insert(idx, "span").await?;
    }
    env.force_flush().await?;

    let (files, dv_files) = dedup_past_partition(&env).await?;
    assert!(files == 2 && dv_files == 1, "fixture needs one DV-bearing file and one DV-free one: {dv_files}/{files}");

    let sql = format!(
        "SELECT time_bucket('10 minutes', timestamp) AS b, COUNT(*) FROM otel_logs_and_spans \
         WHERE project_id = 'e2e_project' AND (kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http') \
         AND timestamp >= '{}' GROUP BY 1",
        ts(PAST + 50 * SEC)
    );
    let total: i64 = client.query(&sql, &[]).await?.iter().map(|r| r.get::<_, i64>(1)).sum();
    assert_eq!(total, 15, "server spans at idx 50,60,..,190");

    let plan = explain_analyze(&client, &sql).await?;
    assert!(plan.contains("text_match(kind"), "the tantivy rewrite did not inject text_match, so this is not the prod shape.\nplan:\n{plan}");
    let scans: Vec<&str> = plan.lines().filter(|l| l.contains("DataSourceExec") && l.contains("file_type=parquet")).collect();
    assert!(
        scans.iter().any(|l| l.contains("predicate=timestamp@")),
        "no parquet scan carries the timestamp predicate — one DV-bearing file stripped pushdown scan-wide.\nplan:\n{plan}"
    );
    // Cost: the DV-free file's 100 rows hold 10 server spans; pushed down, the rest
    // never leave the parquet reader.
    let pruned: i64 = scans.iter().filter_map(|l| scan_metric(l, "pushdown_rows_pruned=")).sum();
    assert!(pruned >= 90, "pushdown pruned {pruned} rows — the DV-free file was decoded in full.\nplan:\n{plan}");
    // Splitting masked from DV-free files unions two sources; the merge must keep the
    // declared ordering or DedupExec falls to full-set.
    let dedup_line = plan.lines().find(|l| l.contains("DedupExec")).unwrap_or_default();
    assert!(dedup_line.contains("bounded["), "DedupExec fell to full-set over the split DV leg.\nplan:\n{plan}");
    Ok(())
}

/// One DV-masked file used to force the whole Delta leg into ONE stream: a merge sat below
/// `DeltaScanExec`, so the transform, casts and filters ran on one core, and because the merge
/// interleaves overlapping files the per-file mask split shredded every batch (prod 2026-09-28:
/// 2.57K parquet batches became 332.5K, ~14 rows each, ~7s of an 8.4s 6h query). Masks are
/// positional per file, so each whole file masks correctly in whichever partition owns it.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dv_masked_scan_keeps_one_stream_per_file_group() -> anyhow::Result<()> {
    const FILES: i64 = 4;
    const PER_FILE: i64 = 100;
    let env = hot_partition_builder().with_deletion_vectors().with_flush_interval(Duration::from_secs(3600)).with_tantivy_prefilter(false).start().await?;
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;

    // Round-robin timestamps across files so every file overlaps every other: an ordered merge
    // over them alternates files row by row. File 0 also holds a changed copy of `r-0001`
    // (file 1), which dedup masks with a deletion vector.
    for f in 0..FILES {
        for k in 0..PER_FILE {
            insert_past_span(&client, k * FILES + f, "span").await?;
        }
        if f == 0 {
            insert_past_span(&client, 1, "span-v2").await?;
        }
        env.force_flush().await?;
    }
    let (files, dv_files) = dedup_past_partition(&env).await?;
    assert!(files >= FILES as usize && dv_files == 1, "fixture needs one DV-bearing file among DV-free ones: {dv_files}/{files}");

    let sql = format!(
        "SELECT time_bucket('10 minutes', timestamp) AS b, COUNT(*) FROM otel_logs_and_spans \
         WHERE project_id = 'e2e_project' AND timestamp >= '{}' GROUP BY 1",
        ts(PAST)
    );
    let count = async || -> anyhow::Result<i64> { Ok(client.query(&sql, &[]).await?.iter().map(|r| r.get::<_, i64>(1)).sum()) };
    assert_eq!(count().await?, FILES * PER_FILE, "the masked copy of r-0001 must not be counted");

    let plan = explain_analyze(&client, &sql).await?;
    let lines: Vec<&str> = plan.lines().collect();
    let delta = lines.iter().position(|l| l.contains("DeltaScanExec")).unwrap_or_else(|| panic!("no Delta leg.\nplan:\n{plan}"));
    assert!(
        !["SortPreservingMergeExec", "CoalescePartitionsExec"].iter().any(|m| lines[delta + 1].contains(m)),
        "the DV-masked Delta leg is merged into one stream below DeltaScanExec.\nplan:\n{plan}"
    );
    // Cost: each parquet batch comes from one file, so masking may split it at most once.
    let source_batches: i64 =
        lines.iter().filter(|l| l.contains("DataSourceExec") && l.contains("file_type=parquet")).filter_map(|l| scan_metric(l, "output_batches=")).sum();
    let delta_batches = scan_metric(lines[delta], "output_batches=").unwrap_or(i64::MAX);
    assert!(
        source_batches > 0 && delta_batches <= 2 * source_batches,
        "DeltaScanExec emitted {delta_batches} batches from {source_batches} parquet batches — files were interleaved before masking.\nplan:\n{plan}"
    );
    let dedup_line = lines.iter().find(|l| l.contains("DedupExec")).copied().unwrap_or_default();
    assert!(dedup_line.contains("bounded["), "DedupExec fell to full-set over the per-group DV leg.\nplan:\n{plan}");

    // Invite byte-range splitting of every file, the masked one included: a mask is positional
    // over the whole file, so it must still hide exactly the one stale row.
    client.batch_execute("SET datafusion.optimizer.repartition_file_min_size = 1").await?;
    assert_eq!(count().await?, FILES * PER_FILE, "a split DV-masked file re-exposed or hid rows");
    let stale: i64 = client.query_one("SELECT count(*) FROM otel_logs_and_spans WHERE project_id = 'e2e_project' AND id = 'r-0001'", &[]).await?.get(0);
    assert_eq!(stale, 1, "the masked copy of r-0001 reappeared");
    Ok(())
}
