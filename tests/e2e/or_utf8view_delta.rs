//! Repro for the 2026-06-16 dashboard correctness bug: `WHERE kind='a' OR
//! kind='b'` on tantivy-indexed string columns (`kind`, `name`) returned 0 rows
//! from Delta while single equality was correct.
//!
//! Root cause: the rewriter turned `kind='x'` into `text_match(kind,'x')`, and
//! the Delta prefilter **intersected** the per-term id sets (sound for AND,
//! empty for OR) → `id IN ([])` → delta-kernel "statically false, skip all
//! files". Fixed two ways: (1) exact `=`/`!=` no longer route through tantivy
//! (bloom/stats handle them); (2) `collect_text_matches` skips OR subtrees.
//!
//! The first test drives data into Delta (force_flush + force_evict) so the
//! query is served purely from parquet, where the prefilter ran. The second
//! keeps data in MemBuffer and uses `LIKE … OR … LIKE` on an ngram3-indexed
//! column — the path that still routes through tantivy after exact `=` was
//! un-routed — to cover the MemBuffer side of the OR-skip guard
//! (`search_with_snapshot`'s intersecting prefilter).

use std::time::Duration;

use timefusion::clock;

use super::harness::{E2eEnv, FROZEN_START_MICROS};

async fn insert_row(client: &tokio_postgres::Client, project_id: &str, id: &str, kind: &str, status_message: &str, ts_micros: i64) -> anyhow::Result<()> {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts_micros).unwrap();
    let sql = format!(
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, kind, status_code, status_message, level, hashes, summary) \
         VALUES ($1, '{}', '{}', $2, 'span', $3, 'OK', $4, 'INFO', ARRAY[]::text[], $5)",
        dt.date_naive(),
        dt.format("%Y-%m-%d %H:%M:%S%.f"),
    );
    client.execute(&sql, &[&project_id, &id, &kind, &status_message, &vec!["s"]]).await?;
    Ok(())
}

async fn count(client: &tokio_postgres::Client, pid: &str, sql: &str) -> i64 {
    client.query_one(sql, &[&pid]).await.unwrap().get::<_, i64>(0)
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn or_equality_on_utf8view_delta_matches_in_list() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).with_retention(Duration::from_secs(120)).start().await?;
    let client = env.pg_client().await?;
    let pid = "e2e_project";

    let kinds: Vec<&str> = (0..40)
        .map(|i| match i % 10 {
            0..=2 => "client",
            3 => "server",
            _ => "internal",
        })
        .collect();
    let (mut n_client, mut n_internal, mut n_server) = (0i64, 0i64, 0i64);
    for (i, kind) in kinds.iter().enumerate() {
        match *kind {
            "client" => n_client += 1,
            "internal" => n_internal += 1,
            _ => n_server += 1,
        }
        insert_row(&client, pid, &format!("row-{i}"), kind, "m", FROZEN_START_MICROS).await?;
    }

    // Push everything from MemBuffer into Delta, then evict MemBuffer so the
    // query is served purely from parquet.
    clock::set_micros(FROZEN_START_MICROS + 10 * 60 * 1_000_000);
    env.force_flush().await?;
    env.force_evict().await?;

    let total = count(&client, pid, "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id=$1").await;
    let c_client = count(&client, pid, "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id=$1 AND kind='client'").await;
    let c_internal = count(&client, pid, "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id=$1 AND kind='internal'").await;
    let c_or = count(&client, pid, "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id=$1 AND (kind='client' OR kind='internal')").await;
    let c_in = count(&client, pid, "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id=$1 AND kind IN ('client','internal')").await;
    let c_or3 = count(&client, pid, "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id=$1 AND (kind='server' OR kind='client' OR kind='internal')").await;

    eprintln!(
        "total={total} (expected {}) | client={c_client}/{n_client} internal={c_internal}/{n_internal} OR={c_or} IN={c_in} OR3={c_or3}",
        n_client + n_internal + n_server
    );
    assert_eq!(c_client, n_client, "single eq client");
    assert_eq!(c_internal, n_internal, "single eq internal");
    assert_eq!(c_in, n_client + n_internal, "IN list");
    assert_eq!(c_or, n_client + n_internal, "OR of two equalities (THE BUG)");
    assert_eq!(c_or3, n_client + n_internal + n_server, "OR of three equalities");
    Ok(())
}

/// MemBuffer-side guard: `LIKE … OR … LIKE` on the ngram3-indexed
/// `status_message` is still tantivy-routed (only exact `=` was un-routed), so
/// it exercises `collect_text_matches`'s OR-skip on the MemBuffer prefilter
/// (`search_with_snapshot` intersects per-term id sets). Without the guard the
/// two LIKE terms would intersect to ∅ → 0 rows. Data is NOT flushed, so it
/// stays MemBuffer-resident.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn or_like_on_indexed_col_membuffer_matches_union() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).with_retention(Duration::from_secs(120)).start().await?;
    let client = env.pg_client().await?;
    let pid = "e2e_project";

    // 3 "alpha", 5 "bravo", 2 "charlie" — distinct substrings on an ngram3 col.
    let msgs: Vec<&str> = std::iter::repeat("alpha")
        .take(3)
        .chain(std::iter::repeat("bravo").take(5))
        .chain(std::iter::repeat("charlie").take(2))
        .collect();
    for (i, msg) in msgs.iter().enumerate() {
        insert_row(&client, pid, &format!("m-{i}"), "server", msg, FROZEN_START_MICROS).await?;
    }

    let alpha = count(&client, pid, "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id=$1 AND status_message LIKE '%alpha%'").await;
    let bravo = count(&client, pid, "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id=$1 AND status_message LIKE '%bravo%'").await;
    let or = count(
        &client,
        pid,
        "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id=$1 AND (status_message LIKE '%alpha%' OR status_message LIKE '%bravo%')",
    )
    .await;

    eprintln!("membuffer LIKE: alpha={alpha} bravo={bravo} OR={or}");
    assert_eq!(alpha, 3, "single LIKE alpha");
    assert_eq!(bravo, 5, "single LIKE bravo");
    assert_eq!(or, 8, "LIKE OR LIKE must union, not intersect (the guard)");
    Ok(())
}
