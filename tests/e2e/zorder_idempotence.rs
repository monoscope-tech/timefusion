//! Running OPTIMIZE twice on the same partition must leave the Delta file set unchanged.

use super::harness::{E2eEnv, FROZEN_START_MICROS};
use super::ordering_pushdown::count_rows;

#[serial_test::serial]
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
    env.force_flush().await?;

    let db = env.db();
    let table_ref = timefusion::database::get_unified_delta_table(db.unified_tables(), "otel_logs_and_spans")
        .await
        .ok_or_else(|| anyhow::anyhow!("unified table not found"))?;

    let mut file_sets = Vec::new();
    for _ in 0..2 {
        db.optimize_table(&table_ref, "otel_logs_and_spans", None).await?;
        file_sets.push(db.list_file_uris("e2e_project", "otel_logs_and_spans").await?.into_iter().collect::<std::collections::HashSet<_>>());
    }

    assert_eq!(file_sets[0], file_sets[1], "second OPTIMIZE rewrote files (churn): {:?} vs {:?}", file_sets[0], file_sets[1]);

    assert_eq!(count_rows(&client, "e2e_project").await?, 20);
    Ok(())
}
