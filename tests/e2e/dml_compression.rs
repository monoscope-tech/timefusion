//! DML rewrites (UPDATE/DELETE/MERGE) must use TF's zstd writer properties, not
//! delta-rs's SNAPPY default. Regression for the `.snappy.parquet` files the
//! merge/update/delete paths produced before `Database::dml_writer_properties`
//! was wired into all three builders (observed in prod 2026-07-14).

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dml_update_rewrites_are_zstd_not_snappy() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let client = env.pg_client().await?;

    let sec = 1_000_000i64;
    for i in 0..5 {
        insert_at(&client, &format!("u-{i}"), FROZEN_START_MICROS + i * sec).await?;
    }
    // Flush to Delta so the UPDATE rewrites a Delta parquet file (not MemBuffer).
    env.advance(Duration::from_secs(180));
    env.force_flush().await?;

    // Simple UPDATE routes to perform_delta_update (UpdateBuilder), executed
    // immediately (dml_coalesce_secs defaults to 0). It rewrites the matched file.
    client.execute("UPDATE otel_logs_and_spans SET status_code = 'ERR' WHERE project_id = 'e2e_project' AND id = 'u-1'", &[]).await?;

    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let uris: Vec<String> = { table_ref.read().await.get_file_uris()?.collect() };
    let parquet: Vec<&String> = uris.iter().filter(|u| u.ends_with(".parquet")).collect();
    assert!(!parquet.is_empty(), "expected live data files after UPDATE");

    let snappy: Vec<&String> = parquet.iter().filter(|u| u.ends_with(".snappy.parquet")).copied().collect();
    assert!(snappy.is_empty(), "DML rewrite produced SNAPPY files (should be zstd): {snappy:?}");
    assert!(parquet.iter().all(|u| u.ends_with(".zstd.parquet")), "expected all data files to be zstd: {parquet:?}");

    Ok(())
}
