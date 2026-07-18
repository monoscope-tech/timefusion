//! Merge-on-read deletion vectors end-to-end. With
//! `timefusion_use_deletion_vectors` on, a Delta UPDATE/DELETE masks the matched
//! rows with a roaring-bitmap deletion vector instead of rewriting whole files:
//! the original parquet stays live (re-added with a DV) and an UPDATE appends
//! only the rewritten rows. This exercises the full prod path (pgwire → flush →
//! Delta on MinIO → DV write → DV-aware read) against the forked delta-rs.

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

/// Live parquet data files for the default tenant table.
async fn parquet_files(env: &E2eEnv) -> anyhow::Result<Vec<String>> {
    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let uris: Vec<String> = { table_ref.read().await.get_file_uris()?.collect() };
    Ok(uris.into_iter().filter(|u| u.ends_with(".parquet")).collect())
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dv_update_and_delete_hide_rows_without_rewriting_files() -> anyhow::Result<()> {
    let env = E2eEnv::builder()
        .with_deletion_vectors()
        .with_bucket_duration(Duration::from_secs(60))
        .start()
        .await?;
    let client = env.pg_client().await?;

    let sec = 1_000_000i64;
    for i in 0..5 {
        insert_at(&client, &format!("u-{i}"), FROZEN_START_MICROS + i * sec).await?;
    }
    // Flush to Delta so the DML targets a Delta parquet file, not the MemBuffer.
    env.advance(Duration::from_secs(180));
    env.force_flush().await?;
    let files_before = parquet_files(&env).await?;
    assert_eq!(files_before.len(), 1, "expected one flushed data file, got {files_before:?}");

    // DV UPDATE: mask row u-1 in the original file and append its rewritten copy.
    client
        .execute(
            "UPDATE otel_logs_and_spans SET status_code = 'ERR' WHERE project_id = 'e2e_project' AND id = 'u-1'",
            &[],
        )
        .await?;

    // Merge-on-read: original file stays live (masked) + one appended file.
    let files_after = parquet_files(&env).await?;
    assert_eq!(
        files_after.len(),
        2,
        "DV UPDATE should keep the masked original and append the rewritten row (got {files_after:?})"
    );
    assert!(
        files_before.iter().all(|f| files_after.contains(f)),
        "the original file must remain live under a DV, not be rewritten"
    );

    // Row count unchanged; the masked original row is hidden and the new one shows.
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"])
        .await?
        .get(0);
    assert_eq!(count, 5, "UPDATE must not change the row count");

    let updated: String = client
        .query_one(
            "SELECT status_code FROM otel_logs_and_spans WHERE project_id = $1 AND id = 'u-1'",
            &[&"e2e_project"],
        )
        .await?
        .get(0);
    assert_eq!(updated, "ERR", "the DV-updated row must read back the new value");

    let untouched: String = client
        .query_one(
            "SELECT status_code FROM otel_logs_and_spans WHERE project_id = $1 AND id = 'u-3'",
            &[&"e2e_project"],
        )
        .await?
        .get(0);
    assert_eq!(untouched, "OK", "unmatched rows stay untouched");

    // DV DELETE: mask row u-2.
    client
        .execute(
            "DELETE FROM otel_logs_and_spans WHERE project_id = 'e2e_project' AND id = 'u-2'",
            &[],
        )
        .await?;
    let after_delete: i64 = client
        .query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"])
        .await?
        .get(0);
    assert_eq!(after_delete, 4, "DV DELETE must hide exactly the matched row");

    let gone: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND id = 'u-2'",
            &[&"e2e_project"],
        )
        .await?
        .get(0);
    assert_eq!(gone, 0, "deleted row must not reappear");

    Ok(())
}
