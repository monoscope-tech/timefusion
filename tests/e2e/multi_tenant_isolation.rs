//! Multi-tenant: two project_ids in the same unified table must not leak
//! into each other's results.

use super::harness::{E2eEnv, FROZEN_START_MICROS};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn project_id_filter_isolates_tenants() -> anyhow::Result<()> {
    let env = E2eEnv::builder().start().await?;
    // The harness pre-warms "e2e_project". Add a second tenant explicitly.
    env.db().get_or_create_table("e2e_other", "otel_logs_and_spans").await?;
    let client = env.pg_client().await?;

    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap();
    let sql = format!(
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) \
         VALUES ($1, '{}', '{}', $2, 'span', 'OK', 'm', 'INFO', ARRAY[]::text[], $3)",
        dt.date_naive(),
        dt.format("%Y-%m-%d %H:%M:%S%.f"),
    );
    for i in 0..3 {
        client.execute(&sql, &[&"e2e_project", &format!("a-{i}"), &vec!["s"]]).await?;
    }
    for i in 0..5 {
        client.execute(&sql, &[&"e2e_other", &format!("b-{i}"), &vec!["s"]]).await?;
    }

    let a: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    let b: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_other"]).await?.get(0);
    assert_eq!(a, 3, "project A leaked or lost rows: got {a}");
    assert_eq!(b, 5, "project B leaked or lost rows: got {b}");
    Ok(())
}
