//! What does one pgwire INSERT actually cost, as a function of rows and columns?
//!
//! Prod 2026-09-11: monoscope's ingest is gated entirely on TimeFusion INSERT
//! latency — 1,124 `insert into otel_logs_and_spans` statements crossed the 1 s
//! slow-statement threshold in 25 minutes, mean 18.1 s, p90 59.9 s, max 207.1 s,
//! against row counts p50 499 / p90 6,866 / max 27,361. That is a ~6-9 ms/row
//! slope holding across the whole distribution, which points at per-row work
//! rather than a fixed queue. This measures the same statement shape
//! (`INSERT … SELECT … FROM unnest(<array-per-column>)`, the form
//! `Models.Telemetry.unnestInsert` builds) on an idle box, at the real table's
//! width, so the prod number can be attributed: reproduce the slope here and
//! the cost is intrinsic to the insert path; miss it by an order of magnitude
//! and the gap is the environment (fsyncs on a saturated array, runtime workers
//! held by the maintenance journal) rather than the code.
//!
//! `#[ignore]`d — it is a measurement, not an assertion about correctness, and
//! the widest case writes 27k rows over 93 columns. Run it with:
//!   cargo nextest run --features e2e -E 'binary(e2e)' \
//!     --run-ignored=all insert_unnest_cost_scales --no-capture

use std::time::{Duration, Instant};

use timefusion::schema::get_schema;

use super::harness::{E2eEnv, FROZEN_START_MICROS};

/// Columns the statement always carries: the four non-nullable ones plus
/// `project_id` for routing and `body` so the narrowest case still exercises
/// Variant coercion.
const BASE_COLS: usize = 6;

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement, not a correctness assertion; writes up to 27k rows"]
async fn insert_unnest_cost_scales_with_rows_and_columns() -> anyhow::Result<()> {
    // The default harness buffer is 64 MB; the widest case alone exceeds it, so
    // it would fail admission rather than report a time.
    let env =
        E2eEnv::builder().with_bucket_duration(Duration::from_secs(600)).with_retention(Duration::from_secs(3600)).with_max_memory_mb(4096).start().await?;
    let client = env.pg_client().await?;
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap();
    let (ts, date) = (dt.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string(), dt.format("%Y-%m-%d").to_string());

    // Take the extra columns from the real schema rather than inventing names,
    // so "prod width" means the width prod actually binds.
    let schema = get_schema("otel_logs_and_spans").expect("otel_logs_and_spans schema");
    let extra = |want: &str, skip: &[&str]| -> Vec<String> {
        schema.fields.iter().filter(|f| f.data_type == want && !skip.contains(&f.name.as_str())).map(|f| f.name.clone()).collect()
    };
    let variant_pool = extra("Variant", &["body"]);
    let text_pool = extra("Utf8", &["id", "project_id"]);

    println!("\n  rows  columns  variant  elapsed_ms  us_per_row");
    for &(n_text, n_variant) in &[(0usize, 0usize), (0, 5), (text_pool.len(), variant_pool.len())] {
        for &n in &[500usize, 5_000, 27_000] {
            let pid = format!("e2e_scale_{n_text}_{n_variant}_{n}");
            // One array per column, exactly as the Haskell side binds them.
            let pids = vec![pid.clone(); n];
            let ids: Vec<String> = (0..n).map(|i| format!("id-{i}")).collect();
            let tss = vec![ts.clone(); n];
            let dates = vec![date.clone(); n];
            let summaries = vec!["a\u{1f}b,c".to_string(); n];
            let bodies: Vec<String> = (0..n).map(|i| format!("{{\"msg\":\"hello-{i}\",\"n\":{i}}}")).collect();
            // Payloads sized like a real span's attribute map, not a toy object.
            let variants: Vec<Vec<String>> =
                (0..n_variant).map(|c| (0..n).map(|i| format!("{{\"col\":{c},\"row\":{i},\"pad\":\"{}\"}}", "x".repeat(120))).collect()).collect();
            let texts: Vec<Vec<String>> = (0..n_text).map(|c| (0..n).map(|i| format!("v-{c}-{i}")).collect()).collect();

            let named: Vec<&str> = variant_pool[..n_variant].iter().chain(text_pool[..n_text].iter()).map(String::as_str).collect();
            let extra_names = named.iter().map(|n| format!(", {n}")).collect::<String>();
            let extra_sel = (0..named.len()).map(|c| format!(", u.x{c}")).collect::<String>();
            let extra_arr = (0..named.len()).map(|c| format!(", ${}::text[]", BASE_COLS + 1 + c)).collect::<String>();
            let extra_alias = (0..named.len()).map(|c| format!(", x{c}")).collect::<String>();
            let sql = format!(
                "INSERT INTO otel_logs_and_spans (project_id, id, timestamp, date, summary, body{extra_names}) \
                 SELECT u.pid, u.id, u.ts::timestamp, u.d::date, string_to_array(u.summary, chr(31)), u.body{extra_sel} \
                 FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[]{extra_arr}) \
                 AS u(pid, id, ts, d, summary, body{extra_alias})"
            );

            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&pids, &ids, &tss, &dates, &summaries, &bodies];
            params.extend(variants.iter().chain(texts.iter()).map(|f| f as &(dyn tokio_postgres::types::ToSql + Sync)));

            let start = Instant::now();
            client.execute(sql.as_str(), &params).await?;
            let elapsed = start.elapsed();

            let count: i64 = client.query_one("SELECT COUNT(*)::bigint FROM otel_logs_and_spans WHERE project_id=$1", &[&pid]).await?.get(0);
            assert_eq!(count, n as i64, "every row of the {n}-row insert landed");
            println!("{n:6}  {:7}  {:7}  {:10}  {:10.1}", BASE_COLS + named.len(), n_variant + 1, elapsed.as_millis(), elapsed.as_micros() as f64 / n as f64);
        }
    }
    Ok(())
}
