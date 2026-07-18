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

/// OPTIMIZE/compaction must consolidate deletion vectors: reading DV-masked files,
/// dropping the deleted rows, and producing DV-free files — never resurrecting the
/// logically-deleted rows.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dv_compaction_consolidates_deletion_vectors() -> anyhow::Result<()> {
    let env = E2eEnv::builder()
        .with_deletion_vectors()
        .with_bucket_duration(Duration::from_secs(60))
        .start()
        .await?;
    let client = env.pg_client().await?;

    let sec = 1_000_000i64;
    for i in 0..20 {
        insert_at(&client, &format!("c-{i}"), FROZEN_START_MICROS + i * sec).await?;
    }
    env.force_flush().await?;

    // DV DELETE 3 rows and DV UPDATE 2 rows (mask + append).
    client
        .execute("DELETE FROM otel_logs_and_spans WHERE project_id = 'e2e_project' AND id IN ('c-1','c-2','c-3')", &[])
        .await?;
    client
        .execute("UPDATE otel_logs_and_spans SET status_code = 'ERR' WHERE project_id = 'e2e_project' AND id IN ('c-4','c-5')", &[])
        .await?;

    // Full compaction: reads DV-masked data, drops deleted rows, writes DV-free files.
    let db = env.db();
    let table_ref = timefusion::database::get_unified_delta_table(db.unified_tables(), "otel_logs_and_spans")
        .await
        .ok_or_else(|| anyhow::anyhow!("unified table not found"))?;
    db.optimize_table(&table_ref, "otel_logs_and_spans", None).await?;

    // Post-compaction: deleted rows stay gone, updated rows keep their new value.
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"])
        .await?
        .get(0);
    assert_eq!(count, 17, "compaction resurrected DV-deleted rows");

    let errs: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND status_code = 'ERR'",
            &[&"e2e_project"],
        )
        .await?
        .get(0);
    assert_eq!(errs, 2, "DV-updated rows lost their value across compaction");

    for id in ["c-1", "c-2", "c-3"] {
        let gone: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2",
                &[&"e2e_project", &id],
            )
            .await?
            .get(0);
        assert_eq!(gone, 0, "deleted row {id} reappeared after compaction");
    }
    Ok(())
}

/// UPDATE ... FROM (the hash-enrichment MERGE shape) as merge-on-read: matched
/// target rows are masked + their updated copies appended, not whole-file rewritten.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dv_merge_update_from_source_masks_and_appends() -> anyhow::Result<()> {
    let env = E2eEnv::builder()
        .with_deletion_vectors()
        .with_bucket_duration(Duration::from_secs(60))
        .start()
        .await?;
    let client = env.pg_client().await?;

    let sec = 1_000_000i64;
    for i in 0..5 {
        insert_at(&client, &format!("m-{i}"), FROZEN_START_MICROS + i * sec).await?;
    }
    env.advance(Duration::from_secs(180));
    env.force_flush().await?;
    let files_before = parquet_files(&env).await?;
    assert_eq!(files_before.len(), 1, "expected one flushed data file, got {files_before:?}");

    // MERGE-update: join the target against a VALUES source on id, set status_code
    // from the source. Routes through perform_delta_merge_update -> DV merge op.
    client
        .execute(
            "UPDATE otel_logs_and_spans SET status_code = src.newcode \
             FROM (VALUES ('m-1', 'X1'), ('m-3', 'X3')) AS src(sid, newcode) \
             WHERE otel_logs_and_spans.project_id = 'e2e_project' AND otel_logs_and_spans.id = src.sid",
            &[],
        )
        .await?;

    let files_after = parquet_files(&env).await?;
    assert!(
        files_after.len() > files_before.len() && files_before.iter().all(|f| files_after.contains(f)),
        "DV merge-update should keep the masked original and append updated rows (before={files_before:?} after={files_after:?})"
    );

    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"])
        .await?
        .get(0);
    assert_eq!(count, 5, "merge-update must not change the row count");

    for (id, expected) in [("m-1", "X1"), ("m-3", "X3"), ("m-2", "OK"), ("m-0", "OK")] {
        let got: String = client
            .query_one(
                "SELECT status_code FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2",
                &[&"e2e_project", &id],
            )
            .await?
            .get(0);
        assert_eq!(got, expected, "row {id} should read status_code={expected}");
    }

    Ok(())
}
