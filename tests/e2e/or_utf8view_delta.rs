//! `OR` of predicates on tantivy-indexed string columns must union, not
//! intersect: the tantivy prefilter intersects per-term id sets, which is sound
//! for AND but yields 0 rows for OR. Covered on both the Delta (flushed) and
//! MemBuffer (unflushed) paths.

use timefusion::support;

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

/// `COUNT(*)` for `pid`, with `pred` ANDed onto the project filter (`""` = the
/// whole project). One prefix keeps the plan shape identical across cases.
async fn count(client: &tokio_postgres::Client, pid: &str, pred: &str) -> i64 {
    let and = if pred.is_empty() { String::new() } else { format!(" AND {pred}") };
    let sql = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id=$1{and}");
    client.query_one(&sql, &[&pid]).await.unwrap().get::<_, i64>(0)
}

const PID: &str = "e2e_project";

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn or_equality_on_utf8view_delta_matches_in_list() -> anyhow::Result<()> {
    let (env, client) = E2eEnv::short_buckets().await?;

    let kinds: Vec<&str> = (0..40)
        .map(|i| match i % 10 {
            0..=2 => "client",
            3 => "server",
            _ => "internal",
        })
        .collect();
    let tally = |k: &str| kinds.iter().filter(|x| **x == k).count() as i64;
    let (n_client, n_internal, n_server) = (tally("client"), tally("internal"), tally("server"));
    for (i, kind) in kinds.iter().enumerate() {
        insert_row(&client, PID, &format!("row-{i}"), kind, "m", FROZEN_START_MICROS).await?;
    }

    // Flush + evict so the query is served purely from Delta parquet.
    support::set_micros(FROZEN_START_MICROS + 10 * 60 * 1_000_000);
    env.flush_and_evict().await?;

    let total = count(&client, PID, "").await;
    let c_client = count(&client, PID, "kind='client'").await;
    let c_internal = count(&client, PID, "kind='internal'").await;
    let c_or = count(&client, PID, "(kind='client' OR kind='internal')").await;
    let c_in = count(&client, PID, "kind IN ('client','internal')").await;
    let c_or3 = count(&client, PID, "(kind='server' OR kind='client' OR kind='internal')").await;

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
/// `status_message` is tantivy-routed, so it exercises the OR-skip on the
/// MemBuffer prefilter. Data is deliberately NOT flushed.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn or_like_on_indexed_col_membuffer_matches_union() -> anyhow::Result<()> {
    let (_env, client) = E2eEnv::short_buckets().await?;

    // 3 "alpha", 5 "bravo", 2 "charlie" — distinct substrings on an ngram3 col.
    let msgs: Vec<&str> = std::iter::repeat_n("alpha", 3).chain(std::iter::repeat_n("bravo", 5)).chain(std::iter::repeat_n("charlie", 2)).collect();
    for (i, msg) in msgs.iter().enumerate() {
        insert_row(&client, PID, &format!("m-{i}"), "server", msg, FROZEN_START_MICROS).await?;
    }

    let alpha = count(&client, PID, "status_message LIKE '%alpha%'").await;
    let bravo = count(&client, PID, "status_message LIKE '%bravo%'").await;
    let or = count(&client, PID, "(status_message LIKE '%alpha%' OR status_message LIKE '%bravo%')").await;

    eprintln!("membuffer LIKE: alpha={alpha} bravo={bravo} OR={or}");
    assert_eq!(alpha, 3, "single LIKE alpha");
    assert_eq!(bravo, 5, "single LIKE bravo");
    assert_eq!(or, 8, "LIKE OR LIKE must union, not intersect (the guard)");
    Ok(())
}
