//! `INSERT … SELECT … FROM unnest(<array-per-column>)` end-to-end, in MemBuffer
//! and after a flush to Delta. Unlike `INSERT … VALUES`, the SELECT path enforces
//! non-nullability, so every non-nullable column must be supplied; JSON columns
//! sent as `text[]` must coerce to Variant.

use timefusion::support;

use super::harness::{E2eEnv, FROZEN_START_MICROS};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn insert_select_unnest_coerces_text_to_variant() -> anyhow::Result<()> {
    let (env, client) = E2eEnv::short_buckets().await?;
    let pid = "e2e_unnest";
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap();

    // All params bound as text[]: TF's pgwire rejects uuid[]/jsonb[].
    let n = 3usize;
    let pids = vec![pid.to_string(); n];
    let ids: Vec<String> = (0..n).map(|i| format!("id-{i}")).collect();
    let tss: Vec<String> = vec![dt.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string(); n];
    let dates: Vec<String> = vec![dt.format("%Y-%m-%d").to_string(); n];
    let bodies: Vec<String> = (0..n).map(|i| format!("{{\"msg\":\"hello-{i}\"}}")).collect();
    let attrs: Vec<String> = (0..n).map(|i| format!("{{\"k\":{i}}}")).collect();
    // summary elements joined with 0x1F; the comma in the second element pins delimiter safety.
    let summaries: Vec<String> = vec!["a\u{1f}b,c".to_string(); n];

    let sql = "INSERT INTO otel_logs_and_spans \
        (project_id, id, timestamp, date, summary, body, attributes) \
        SELECT u.pid, u.id, u.ts::timestamp, u.d::date, string_to_array(u.summary, chr(31)), u.body, u.attrs \
        FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[]) \
        AS u(pid, id, ts, d, summary, body, attrs)";
    client.execute(sql, &[&pids, &ids, &tss, &dates, &summaries, &bodies, &attrs]).await?;

    // Keep the Variant extraction non-aggregate: an aggregate over it hits an
    // unrelated aggregate-schema path.
    let cnt: i64 = client.query_one("SELECT COUNT(*)::bigint FROM otel_logs_and_spans WHERE project_id=$1", &[&pid]).await?.get(0);
    assert_eq!(cnt, n as i64, "all rows inserted via unnest");
    let msg: String = client.query_one("SELECT body ->> 'msg' FROM otel_logs_and_spans WHERE project_id=$1 AND id='id-0'", &[&pid]).await?.get(0);
    assert_eq!(msg, "hello-0", "body coerced to queryable Variant in MemBuffer");

    let s: Vec<String> = client.query_one("SELECT summary FROM otel_logs_and_spans WHERE project_id=$1 AND id='id-0'", &[&pid]).await?.get(0);
    assert_eq!(s, vec!["a".to_string(), "b,c".to_string()], "string_to_array(chr(31)) is comma-safe");

    // Flush to Delta + evict MemBuffer: the Variant predicate must still hold from parquet.
    support::set_micros(FROZEN_START_MICROS + 10 * 60 * 1_000_000);
    env.flush_and_evict().await?;
    let n_hit: i64 =
        client.query_one("SELECT COUNT(*)::bigint FROM otel_logs_and_spans WHERE project_id=$1 AND body ->> 'msg' = 'hello-1'", &[&pid]).await?.get(0);
    assert_eq!(n_hit, 1, "Variant predicate holds from Delta after flush");
    Ok(())
}
