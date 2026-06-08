//! Z-order compaction idempotence (commit 4a41fea regression guard):
//! running OPTIMIZE on the same partition twice must be a no-op the second
//! time — the file set should not change. We assert directly on Delta's
//! file URIs rather than on internal counters so the test survives a
//! refactor of the optimize path.

use super::harness::{E2eEnv, FROZEN_START_MICROS};

#[tokio::test(flavor = "multi_thread")]
async fn second_optimize_is_a_noop() -> anyhow::Result<()> {
    let env = E2eEnv::builder().start().await?;
    let client = env.pg_client().await?;

    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap();
    let sql = format!(
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) \
         VALUES ($1, '{}', '{}', $2, 'span', 'OK', 'm', 'INFO', ARRAY[]::text[], $3)",
        dt.date_naive(),
        dt.format("%Y-%m-%d %H:%M:%S%.f"),
    );
    for i in 0..20 {
        client.execute(&sql, &[&"e2e_project", &format!("z-{i}"), &vec!["s"]]).await?;
    }
    // Land everything in Delta so OPTIMIZE has something to work on.
    env.force_flush().await?;

    let db = env.db();
    let table_ref = timefusion::database::get_unified_delta_table(db.unified_tables(), "otel_logs_and_spans")
        .await
        .ok_or_else(|| anyhow::anyhow!("unified table not found"))?;

    db.optimize_table(&table_ref, "otel_logs_and_spans", None).await?;
    let files_after_first: Vec<String> = db.list_file_uris("e2e_project", "otel_logs_and_spans").await?;

    db.optimize_table(&table_ref, "otel_logs_and_spans", None).await?;
    let files_after_second: Vec<String> = db.list_file_uris("e2e_project", "otel_logs_and_spans").await?;

    // Exact equality (as sets): the idempotence guard should skip rewriting
    // unchanged partitions, leaving the file set untouched.
    let s1: std::collections::HashSet<_> = files_after_first.iter().collect();
    let s2: std::collections::HashSet<_> = files_after_second.iter().collect();
    assert_eq!(s1, s2, "second OPTIMIZE rewrote files (churn): {s1:?} vs {s2:?}");

    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 20);
    Ok(())
}
