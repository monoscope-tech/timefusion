//! Smoke test: the regression guard for the actual prod symptom we just hit
//! — "no response returned to queries on timefusion even though responses
//! were always returned in the past". Every E2E run starts here. If this
//! breaks, nothing else matters.

use std::time::Duration;

use super::harness::E2eEnv;

const QUERY_RESPONSE_BUDGET: Duration = Duration::from_secs(5);

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn pgwire_query_returns_response() -> anyhow::Result<()> {
    let env = E2eEnv::builder().start().await?;
    let client = env.pg_client().await?;

    let insert = format!(
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) \
         VALUES ($1, {}, '{}', $2, $3, $4, $5, $6, ARRAY[]::text[], $7)",
        chrono::Utc::now().date_naive(),
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
    );

    tokio::time::timeout(QUERY_RESPONSE_BUDGET, client.execute(&insert, &[&"e2e_project", &"smoke-1", &"smoke", &"OK", &"hi", &"INFO", &vec!["s"]]))
        .await
        .map_err(|_| anyhow::anyhow!("INSERT did not return within {QUERY_RESPONSE_BUDGET:?}"))??;

    let count: i64 = tokio::time::timeout(
        QUERY_RESPONSE_BUDGET,
        client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&"e2e_project", &"smoke-1"]),
    )
    .await
    .map_err(|_| anyhow::anyhow!("SELECT did not return within {QUERY_RESPONSE_BUDGET:?}"))??
    .get(0);
    assert_eq!(count, 1, "row was not visible after insert");

    Ok(())
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn count_star_returns_correct_value() -> anyhow::Result<()> {
    let env = E2eEnv::builder().start().await?;
    let client = env.pg_client().await?;

    let insert = format!(
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) \
         VALUES ($1, {}, '{}', $2, $3, $4, $5, $6, ARRAY[]::text[], $7)",
        chrono::Utc::now().date_naive(),
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
    );

    for i in 0..7 {
        client.execute(&insert, &[&"e2e_project", &format!("smoke-{i}"), &"s", &"OK", &"m", &"INFO", &vec!["s"]]).await?;
    }
    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 7);
    Ok(())
}
