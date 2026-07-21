//! Bulk-write alias. `INSERT INTO otel_logs_and_spans__bulk ...` must commit
//! straight to Delta (`skip_queue=true`), bypassing the BufferedWriteLayer
//! (WAL + MemBuffer), while the same rows stay queryable from the real table.
//! This backs the DLQ-drain / backfill path that must not pressure the live
//! MemBuffer. The session context is shared across connections, so a dedicated
//! table name (not a per-connection GUC) is how a client opts into the direct
//! path.

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

/// Same row shape as `harness::insert_for`, but targeting the `__bulk` alias.
async fn insert_bulk(client: &tokio_postgres::Client, id: &str, ts_micros: i64) -> anyhow::Result<()> {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts_micros).unwrap();
    let sql = format!(
        "INSERT INTO otel_logs_and_spans__bulk (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) \
         VALUES ($1, '{}', '{}', $2, 'span', 'OK', 'm', 'INFO', ARRAY[]::text[], $3)",
        dt.date_naive(),
        dt.format("%Y-%m-%d %H:%M:%S%.f"),
    );
    client.execute(&sql, &[&"e2e_project", &id, &vec!["s"]]).await?;
    Ok(())
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn bulk_alias_skips_membuffer_but_is_queryable() -> anyhow::Result<()> {
    let env = E2eEnv::builder().start().await?;
    let client = env.pg_client().await?;

    // Control: a normal INSERT lands in the MemBuffer.
    insert_at(&client, "buffered-1", FROZEN_START_MICROS).await?;
    let buffered_rows = env.snapshot_stats().mem_total_rows;
    assert!(buffered_rows >= 1, "normal insert should buffer, got {buffered_rows}");

    // Bulk alias must NOT grow the MemBuffer (skip_queue → straight to Delta).
    insert_bulk(&client, "bulk-1", FROZEN_START_MICROS).await?;
    let after_bulk = env.snapshot_stats().mem_total_rows;
    assert_eq!(after_bulk, buffered_rows, "bulk insert must bypass the MemBuffer; rows grew {buffered_rows}→{after_bulk}");

    // ...yet the bulk row is immediately visible in the real table (it's in Delta).
    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&"e2e_project", &"bulk-1"]).await?.get(0);
    assert_eq!(count, 1, "bulk row not visible in otel_logs_and_spans (skip_queue write lost)");

    Ok(())
}
