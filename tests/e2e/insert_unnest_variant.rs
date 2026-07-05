//! Feasibility + regression test for the monoscope bulk-insert refactor
//! (2026-07-05): replace the 45k-placeholder multi-row `INSERT … VALUES` with a
//! column-oriented `INSERT … SELECT … FROM unnest(<array-per-column>)` so
//! planning is O(cols) not O(rows×cols) and the plan text is stable.
//!
//! Two things had to hold for the refactor to work on the TF leg, both proven
//! here end-to-end (MemBuffer AND Delta):
//!   1. INSERT…SELECT…unnest works AT ALL. It does — earlier failures were a
//!      red herring: the only non-nullable columns are date/timestamp/id/summary,
//!      and partial probes omitted `summary` → "Invalid batch column has null but
//!      schema specifies non-nullable". `INSERT…VALUES` tolerates that via its
//!      fast-path; SELECT enforces it. Provide all four and SELECT works.
//!   2. JSON/Variant columns sent as `text[]` coerce to Variant. TF's pgwire
//!      rejects `uuid[]`/`jsonb[]`, so id + JSON columns go as `text[]`;
//!      `VariantInsertRewriter` wraps them with json_to_variant on the Projection
//!      input. `summary` round-trips via `string_to_array(_, chr(31))` (0x1F
//!      delimiter — comma-safe, empty→{}, verified identical on TF+TS).

use std::time::Duration;

use timefusion::clock;

use super::harness::{E2eEnv, FROZEN_START_MICROS};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn insert_select_unnest_coerces_text_to_variant() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).with_retention(Duration::from_secs(120)).start().await?;
    let client = env.pg_client().await?;
    let pid = "e2e_unnest";
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap();

    // All params bound as text[] (avoids client OID inference; the real monoscope
    // path uses native array encoders). Provide ALL non-nullable columns
    // (project_id, id, timestamp, date, summary). body/attributes are JSON sent
    // as text → must coerce to Variant.
    let n = 3usize;
    let pids = vec![pid.to_string(); n];
    let ids: Vec<String> = (0..n).map(|i| format!("id-{i}")).collect();
    let tss: Vec<String> = vec![dt.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string(); n];
    let dates: Vec<String> = vec![dt.format("%Y-%m-%d").to_string(); n];
    let bodies: Vec<String> = (0..n).map(|i| format!("{{\"msg\":\"hello-{i}\"}}")).collect();
    let attrs: Vec<String> = (0..n).map(|i| format!("{{\"k\":{i}}}")).collect();
    // summary elements joined with 0x1F; second element has a comma (delimiter safety).
    let summaries: Vec<String> = vec!["a\u{1f}b,c".to_string(); n];

    let sql = "INSERT INTO otel_logs_and_spans \
        (project_id, id, timestamp, date, summary, body, attributes) \
        SELECT u.pid, u.id, u.ts::timestamp, u.d::date, string_to_array(u.summary, chr(31)), u.body, u.attrs \
        FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[]) \
        AS u(pid, id, ts, d, summary, body, attrs)";
    client.execute(sql, &[&pids, &ids, &tss, &dates, &summaries, &bodies, &attrs]).await?;

    // MemBuffer read. COUNT(*) proves the INSERT committed; a NON-aggregate
    // `body ->> 'msg'` select proves text→Variant coercion (avoid MIN/aggregate
    // over a Variant extraction, which hits an unrelated aggregate-schema path).
    let cnt: i64 = client.query_one("SELECT COUNT(*)::bigint FROM otel_logs_and_spans WHERE project_id=$1", &[&pid]).await?.get(0);
    assert_eq!(cnt, n as i64, "all rows inserted via unnest");
    let msg: String = client
        .query_one("SELECT body ->> 'msg' FROM otel_logs_and_spans WHERE project_id=$1 AND id='id-0'", &[&pid])
        .await?
        .get(0);
    assert_eq!(msg, "hello-0", "body coerced to queryable Variant in MemBuffer");

    // summary reconstructed with the comma-containing element intact.
    let s: Vec<String> = client.query_one("SELECT summary FROM otel_logs_and_spans WHERE project_id=$1 AND id='id-0'", &[&pid]).await?.get(0);
    assert_eq!(s, vec!["a".to_string(), "b,c".to_string()], "string_to_array(chr(31)) is comma-safe");

    // Flush to Delta + evict MemBuffer → the Variant predicate must still hold from parquet.
    clock::set_micros(FROZEN_START_MICROS + 10 * 60 * 1_000_000);
    env.force_flush().await?;
    env.force_evict().await?;
    let n_hit: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM otel_logs_and_spans WHERE project_id=$1 AND body ->> 'msg' = 'hello-1'",
            &[&pid],
        )
        .await?
        .get(0);
    assert_eq!(n_hit, 1, "Variant predicate holds from Delta after flush");
    Ok(())
}
