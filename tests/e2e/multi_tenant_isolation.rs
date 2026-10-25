//! Two project_ids in the same unified table must not leak into each other's results.

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_for};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn project_id_filter_isolates_tenants() -> anyhow::Result<()> {
    let env = E2eEnv::builder().start().await?;
    // The harness pre-warms "e2e_project". Add a second tenant explicitly.
    env.db().get_or_create_table("e2e_other", "otel_logs_and_spans").await?;
    let client = env.pg_client().await?;

    for (project, prefix, rows) in [("e2e_project", "a", 3), ("e2e_other", "b", 5)] {
        for i in 0..rows {
            insert_for(&client, project, &format!("{prefix}-{i}"), FROZEN_START_MICROS).await?;
        }
    }

    for (project, want) in [("e2e_project", 3i64), ("e2e_other", 5)] {
        let got: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&project]).await?.get(0);
        assert_eq!(got, want, "project {project} leaked or lost rows: got {got}");
    }
    Ok(())
}
