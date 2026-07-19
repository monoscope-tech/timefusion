//! Reproduces monoscope's hash-enrichment flow: write a span with empty `hashes`,
//! then almost immediately `UPDATE ... FROM (unnest) ... SET hashes = COALESCE(hashes,'{}') || ARRAY[tag]`
//! joined on (span_id, trace_id), and query with `hashes && ARRAY[tag]` (array overlap).
//! Exercises it both while the row is still in the MemBuffer and after it has flushed
//! to Delta (where the update lands as a merge-on-read deletion-vector rewrite).

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS};

async fn insert_span(client: &tokio_postgres::Client, id: &str, span: &str, trace: &str, ts: i64) -> anyhow::Result<()> {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap();
    let sql = format!(
        "INSERT INTO otel_logs_and_spans \
         (project_id, date, timestamp, id, name, status_code, level, hashes, summary, context___span_id, context___trace_id) \
         VALUES ('e2e_project', '{}', '{}', $1, 'span', 'OK', 'INFO', ARRAY[]::text[], $2, $3, $4)",
        dt.date_naive(),
        dt.format("%Y-%m-%d %H:%M:%S%.f"),
    );
    client.execute(&sql, &[&id, &vec!["s"], &span, &trace]).await?;
    Ok(())
}

/// The exact enrichment UPDATE-2 shape from monoscope BackgroundJobs.hs.
async fn enrich(client: &tokio_postgres::Client, span: &str, trace: &str, tag: &str) -> anyhow::Result<u64> {
    let sql = format!(
        "UPDATE otel_logs_and_spans o \
           SET hashes = COALESCE(o.hashes, '{{}}'::text[]) || ARRAY['{tag}'] \
           FROM ( SELECT unnest(ARRAY['{span}']::text[]) AS span_id, \
                         unnest(ARRAY['{trace}']::text[]) AS trace_id, \
                         unnest(ARRAY['{tag}']::text[])   AS tag ) u \
           WHERE o.project_id = 'e2e_project' \
             AND o.context___span_id = u.span_id \
             AND o.context___trace_id = u.trace_id \
             AND NOT (COALESCE(o.hashes, '{{}}'::text[]) @> ARRAY[u.tag])"
    );
    Ok(client.execute(&sql, &[]).await?)
}

/// Enrichment where one batch carries the SAME (span,trace) key twice with
/// different tags — the prod "MERGE matched a target row against multiple source
/// rows" shape. Both tags must be applied (append-accumulate), so the merge must
/// split same-key rows into successive rounds, not dedup them.
async fn enrich_multi(client: &tokio_postgres::Client, span: &str, trace: &str, tags: &[&str]) -> anyhow::Result<u64> {
    let arr = |xs: &[&str]| xs.iter().map(|x| format!("'{x}'")).collect::<Vec<_>>().join(",");
    let spans = arr(&vec![span; tags.len()]);
    let traces = arr(&vec![trace; tags.len()]);
    let sql = format!(
        "UPDATE otel_logs_and_spans o \
           SET hashes = COALESCE(o.hashes, '{{}}'::text[]) || ARRAY[u.tag] \
           FROM ( SELECT unnest(ARRAY[{spans}]::text[]) AS span_id, \
                         unnest(ARRAY[{traces}]::text[]) AS trace_id, \
                         unnest(ARRAY[{}]::text[])   AS tag ) u \
           WHERE o.project_id = 'e2e_project' \
             AND o.context___span_id = u.span_id \
             AND o.context___trace_id = u.trace_id \
             AND NOT (COALESCE(o.hashes, '{{}}'::text[]) @> ARRAY[u.tag])",
        arr(tags)
    );
    Ok(client.execute(&sql, &[]).await?)
}

/// Exact prod shape: two equi-keys AND `timestamp >= lo AND timestamp < hi`
/// bounds (monoscope always sends them). Reproduces the prod "No field named
/// otel_logs_and_spans.context___span_id" schema error if the time bounds change
/// the merge plan's projection.
async fn enrich_bounded(client: &tokio_postgres::Client, span: &str, trace: &str, tag: &str, lo: i64, hi: i64) -> anyhow::Result<u64> {
    let ts = |m: i64| chrono::DateTime::<chrono::Utc>::from_timestamp_micros(m).unwrap().format("%Y-%m-%d %H:%M:%S%.f").to_string();
    let sql = format!(
        "UPDATE otel_logs_and_spans o \
           SET hashes = COALESCE(o.hashes, '{{}}'::text[]) || ARRAY[u.tag] \
           FROM ( SELECT unnest(ARRAY['{span}']::text[]) AS span_id, \
                         unnest(ARRAY['{trace}']::text[]) AS trace_id, \
                         unnest(ARRAY['{tag}']::text[])   AS tag ) u \
           WHERE o.project_id = 'e2e_project' AND o.timestamp >= '{}' AND o.timestamp < '{}' \
             AND o.context___span_id = u.span_id \
             AND o.context___trace_id = u.trace_id \
             AND NOT (COALESCE(o.hashes, '{{}}'::text[]) @> ARRAY[u.tag])",
        ts(lo),
        ts(hi)
    );
    Ok(client.execute(&sql, &[]).await?)
}

/// The EXACT monoscope prod shape: nested source subquery with `ORDER BY`, and
/// parameterized `unnest($1::text[])` arrays. The Sort + nested projection change
/// the join plan so the equi-key equality is retained in the Filter (and thus in
/// the DV merge's target_predicate) — reproducing the prod "No field named
/// otel_logs_and_spans.context___span_id" crash + dropped rows. Flat inlined
/// unnest (see `enrich`) does NOT trigger it.
async fn enrich_prod_shape(client: &tokio_postgres::Client, spans: &[&str], traces: &[&str], tags: &[&str], lo: i64, hi: i64) -> anyhow::Result<u64> {
    let ts = |m: i64| chrono::DateTime::<chrono::Utc>::from_timestamp_micros(m).unwrap().format("%Y-%m-%d %H:%M:%S%.f").to_string();
    let sql = format!(
        "UPDATE otel_logs_and_spans o \
           SET hashes = COALESCE(o.hashes, '{{}}'::text[]) || ARRAY[u.tag] \
           FROM ( SELECT span_id, trace_id, tag FROM \
                    ( SELECT unnest($1::text[]) AS span_id, unnest($2::text[]) AS trace_id, unnest($3::text[]) AS tag ) raw \
                  ORDER BY span_id, trace_id ) u \
           WHERE o.project_id = 'e2e_project' AND o.timestamp >= '{}' AND o.timestamp < '{}' \
             AND o.context___span_id = u.span_id \
             AND o.context___trace_id = u.trace_id \
             AND NOT (COALESCE(o.hashes, '{{}}'::text[]) @> ARRAY[u.tag])",
        ts(lo),
        ts(hi)
    );
    let (sv, tv, gv) = (spans.to_vec(), traces.to_vec(), tags.to_vec());
    Ok(client.execute(&sql, &[&sv, &tv, &gv]).await?)
}

async fn count_by_hash(client: &tokio_postgres::Client, tag: &str) -> anyhow::Result<i64> {
    Ok(client
        .query_one(&format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'e2e_project' AND hashes && ARRAY['{tag}']::text[]"), &[])
        .await?
        .get(0))
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn hash_enrichment_queryable_membuffer_and_after_flush() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_deletion_vectors().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let client = env.pg_client().await?;

    // --- Stage 1: row in the MemBuffer (not yet flushed) ---
    insert_span(&client, "h-1", "span-1", "trace-1", FROZEN_START_MICROS).await?;
    let updated = enrich(&client, "span-1", "trace-1", "H1").await?;
    assert_eq!(updated, 1, "enrichment UPDATE matched no rows in MemBuffer");
    assert_eq!(count_by_hash(&client, "H1").await?, 1, "MemBuffer: hashes && ARRAY['H1'] did not match the enriched row");

    // --- Stage 2: flush to Delta, same query must still match ---
    env.force_flush().await?;
    assert_eq!(count_by_hash(&client, "H1").await?, 1, "after flush: enriched hash lost");

    // --- Stage 3: enrich a *flushed* row (Delta merge-on-read DV path) ---
    insert_span(&client, "h-2", "span-2", "trace-2", FROZEN_START_MICROS + 1_000_000).await?;
    env.force_flush().await?;
    let updated = enrich(&client, "span-2", "trace-2", "H2").await?;
    assert_eq!(updated, 1, "enrichment UPDATE matched no flushed rows (Delta/DV)");
    assert_eq!(count_by_hash(&client, "H2").await?, 1, "flushed+DV: hashes && ARRAY['H2'] did not match");
    // The first row's hash must be unaffected.
    assert_eq!(count_by_hash(&client, "H1").await?, 1, "H1 lost after enriching a different row");

    // --- Stage 4: the monoscope query shape — filter by hashes WITH ORDER BY/LIMIT
    // (projection pushdown may drop `hashes` from the scan while the predicate
    // still references it). Reproduces prod "Predicate references unknown column: hashes". ---
    let rows = client
        .query(
            "SELECT id, timestamp, hashes FROM otel_logs_and_spans \
             WHERE project_id = 'e2e_project' AND hashes && ARRAY['H1']::text[] \
             ORDER BY timestamp DESC LIMIT 5",
            &[],
        )
        .await?;
    assert_eq!(rows.len(), 1, "ORDER BY + LIMIT hash filter returned wrong rows");

    let rows = client
        .query(
            "SELECT id, timestamp, hashes FROM otel_logs_and_spans \
             WHERE project_id = 'e2e_project' AND array_length(hashes, 1) > 0 \
             ORDER BY timestamp DESC LIMIT 5",
            &[],
        )
        .await?;
    assert_eq!(rows.len(), 2, "ORDER BY + LIMIT array_length filter returned wrong rows");

    // --- Stage 5: `hashes IS NOT NULL` predicate on the List column. In prod this
    // pushed to delta_kernel data-skipping and errored "Predicate references unknown
    // column: hashes", breaking every hash query that includes the null-check. ---
    let rows = client
        .query(
            "SELECT id FROM otel_logs_and_spans \
             WHERE project_id = 'e2e_project' AND hashes IS NOT NULL AND array_length(hashes, 1) > 0 LIMIT 5",
            &[],
        )
        .await?;
    assert_eq!(rows.len(), 2, "hashes IS NOT NULL filter dropped rows or errored");

    Ok(())
}

/// Repro for the prod "MERGE matched a target row against multiple source rows"
/// failures: a single enrichment batch with the same (span,trace) key repeated
/// with distinct tags must apply ALL tags, on both the MemBuffer and DV paths.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn hash_enrichment_same_key_multiple_tags_applies_all() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_deletion_vectors().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let client = env.pg_client().await?;

    // MemBuffer path.
    insert_span(&client, "m-1", "span-m", "trace-m", FROZEN_START_MICROS).await?;
    enrich_multi(&client, "span-m", "trace-m", &["A", "B", "C"]).await?;
    for t in ["A", "B", "C"] {
        assert_eq!(count_by_hash(&client, t).await?, 1, "MemBuffer: tag {t} not applied from a same-key multi-tag batch");
    }

    // DV path: flushed row.
    insert_span(&client, "m-2", "span-n", "trace-n", FROZEN_START_MICROS + 1_000_000).await?;
    env.force_flush().await?;
    enrich_multi(&client, "span-n", "trace-n", &["X", "Y"]).await?;
    for t in ["X", "Y"] {
        assert_eq!(count_by_hash(&client, t).await?, 1, "DV: tag {t} not applied from a same-key multi-tag batch");
    }
    Ok(())
}

/// Insert `rows` spans (distinct span/trace ids under `prefix`) into one flushed
/// parquet file. Returns nothing; caller evicts.
async fn insert_file(client: &tokio_postgres::Client, env: &E2eEnv, prefix: &str, rows: usize, ts: i64) -> anyhow::Result<()> {
    for r in 0..rows {
        insert_span(client, &format!("{prefix}-{r}"), &format!("{prefix}-span-{r}"), &format!("{prefix}-trace-{r}"), ts).await?;
    }
    env.force_flush().await?;
    Ok(())
}

/// #1 LOCAL A/B BENCHMARK (run on demand): enrich one span against many files
/// with the bloom-prune ON vs OFF and compare merge wall-clock. Proves the
/// join-key IN-filter actually reduces merge scan work locally — not just in prod.
/// `cargo test --test e2e --features e2e bench_bloom_prune_ab -- --ignored --nocapture`
#[ignore]
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn bench_bloom_prune_ab() -> anyhow::Result<()> {
    const FILES: usize = 20;
    const ROWS_PER_FILE: usize = 400;

    async fn run(prune: bool) -> anyhow::Result<std::time::Duration> {
        let env = E2eEnv::builder().with_deletion_vectors().with_dml_merge_key_prune(prune).with_bucket_duration(Duration::from_secs(60)).start().await?;
        let client = env.pg_client().await?;
        for f in 0..FILES {
            insert_file(&client, &env, &format!("f{f}"), ROWS_PER_FILE, FROZEN_START_MICROS).await?;
        }
        env.advance(Duration::from_secs(600));
        env.force_evict().await?; // all Delta, cold MemBuffer → merge scans Delta
        // Enrich one span living in the LAST file — prune must skip the other 19 files.
        let (span, trace) = (format!("f{}-span-7", FILES - 1), format!("f{}-trace-7", FILES - 1));
        let t = std::time::Instant::now();
        let updated = enrich(&client, &span, &trace, "BENCH").await?;
        let elapsed = t.elapsed();
        assert_eq!(updated, 1, "prune={prune}: enrichment matched wrong count");
        assert_eq!(count_by_hash(&client, "BENCH").await?, 1, "prune={prune}: soundness — tag lost");
        Ok(elapsed)
    }

    let off = run(false).await?;
    let on = run(true).await?;
    eprintln!("BLOOM-PRUNE A/B: {FILES} files × {ROWS_PER_FILE} rows, enrich 1 span — prune OFF {off:?} vs ON {on:?} ({:.1}× faster)", off.as_secs_f64() / on.as_secs_f64().max(1e-9));
    assert!(on < off, "bloom prune (ON {on:?}) should scan less than full (OFF {off:?})");
    Ok(())
}

/// #1 bloom-prune soundness: the `key IN (source keys)` filter pushed into the DV
/// merge scan (so parquet bloom filters skip non-matching files) must never drop a
/// real match. Insert N spans into N separate flushed+evicted files (same
/// timestamp, so only the join key distinguishes them), enrich several across
/// different files, and assert every one lands — a false bloom-negative would lose
/// a tag here.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn hash_enrichment_bloom_prune_never_drops_a_match() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_deletion_vectors().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let client = env.pg_client().await?;

    const N: usize = 8;
    for i in 0..N {
        insert_span(&client, &format!("p-{i}"), &format!("span-{i}"), &format!("trace-{i}"), FROZEN_START_MICROS).await?;
        env.force_flush().await?; // one parquet file per span
    }
    // Evict from the MemBuffer so the enrichment MUST hit the Delta merge scan.
    env.advance(Duration::from_secs(600));
    env.force_evict().await?;

    // Enrich spans in several different files; each must be found despite the
    // bloom-prune IN-filter narrowing the scan.
    for i in [3usize, 6, 0, 7] {
        let tag = format!("P{i}");
        let updated = enrich(&client, &format!("span-{i}"), &format!("trace-{i}"), &tag).await?;
        assert_eq!(updated, 1, "bloom-prune enrichment of span-{i} matched no rows (false bloom negative?)");
    }
    for i in [3usize, 6, 0, 7] {
        assert_eq!(count_by_hash(&client, &format!("P{i}")).await?, 1, "bloom-prune: tag P{i} lost");
    }
    Ok(())
}

/// Coverage of the exact monoscope prod shape (nested source subquery with
/// ORDER BY + parameterized unnest arrays) on the DV path. NOTE: this does NOT
/// reproduce the prod "No field named ...context___span_id" crash — it passes
/// even with strip_source_conjuncts reverted, so the real trigger is some prod
/// condition not captured locally (verified 2026-07-19). Kept as happy-path
/// coverage of the prod SQL shape; the strip fix is covered by strip_tests +
/// confirmed in prod (errors 8/10min → 0 on 73a9d3d).
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn hash_enrichment_prod_shape_ordered_subquery_dv_path() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_deletion_vectors().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let client = env.pg_client().await?;

    insert_span(&client, "ps-1", "span-x", "trace-x", FROZEN_START_MICROS).await?;
    insert_span(&client, "ps-2", "span-y", "trace-y", FROZEN_START_MICROS + 1_000_000).await?;
    env.force_flush().await?;
    env.advance(Duration::from_secs(600));
    env.force_evict().await?; // rows Delta-only → DV merge path

    let lo = FROZEN_START_MICROS - 60_000_000;
    let hi = FROZEN_START_MICROS + 120_000_000;
    let updated = enrich_prod_shape(&client, &["span-x", "span-y"], &["trace-x", "trace-y"], &["PX", "PY"], lo, hi).await?;
    assert_eq!(updated, 2, "prod-shape enrichment matched wrong row count (crash → 0/dropped)");
    assert_eq!(count_by_hash(&client, "PX").await?, 1, "prod-shape: tag PX lost (the context___span_id crash)");
    assert_eq!(count_by_hash(&client, "PY").await?, 1, "prod-shape: tag PY lost");
    Ok(())
}

/// Repro attempt for the prod "No field named otel_logs_and_spans.context___span_id"
/// schema error — exact prod shape with `timestamp` bounds, on the DV path.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn hash_enrichment_bounded_timestamp_dv_path() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_deletion_vectors().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let client = env.pg_client().await?;

    insert_span(&client, "b-1", "span-b", "trace-b", FROZEN_START_MICROS).await?;
    env.force_flush().await?;
    let lo = FROZEN_START_MICROS - 60_000_000;
    let hi = FROZEN_START_MICROS + 60_000_000;
    let updated = enrich_bounded(&client, "span-b", "trace-b", "B1", lo, hi).await?;
    assert_eq!(updated, 1, "bounded enrichment matched no flushed rows");
    assert_eq!(count_by_hash(&client, "B1").await?, 1, "bounded DV enrichment: hash not applied");
    Ok(())
}
