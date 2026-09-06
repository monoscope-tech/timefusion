//! Merge-on-read deletion vectors end-to-end. With
//! `timefusion_use_deletion_vectors` on, a Delta UPDATE/DELETE masks the matched
//! rows with a roaring-bitmap deletion vector instead of rewriting whole files:
//! the original parquet stays live (re-added with a DV) and an UPDATE appends
//! only the rewritten rows. This exercises the full prod path (pgwire → flush →
//! Delta on MinIO → DV write → DV-aware read) against the forked delta-rs.
//!
//! Subject is `mor_dormant`, not otel: once `otel_logs_and_spans` set
//! `version_append`, an UPDATE there appends a row version instead of masking
//! and rewriting, so it can no longer witness deletion-vector behaviour.

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_dormant_at};

/// Live parquet data files for the default tenant table.
async fn parquet_files(env: &E2eEnv) -> anyhow::Result<Vec<String>> {
    let table_ref = env.db().resolve_table("e2e_project", "mor_dormant").await?;
    let uris: Vec<String> = { table_ref.read().await.get_file_uris()?.collect() };
    Ok(uris.into_iter().filter(|u| u.ends_with(".parquet")).collect())
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dv_update_and_delete_hide_rows_without_rewriting_files() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_deletion_vectors().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let client = env.pg_client().await?;

    let sec = 1_000_000i64;
    for i in 0..5 {
        insert_dormant_at(&client, &format!("u-{i}"), FROZEN_START_MICROS + i * sec).await?;
    }
    // Flush to Delta so the DML targets a Delta parquet file, not the MemBuffer.
    env.advance(Duration::from_secs(180));
    env.force_flush().await?;
    let files_before = parquet_files(&env).await?;
    assert_eq!(files_before.len(), 1, "expected one flushed data file, got {files_before:?}");

    // DV UPDATE: mask row u-1 in the original file and append its rewritten copy.
    client.execute("UPDATE mor_dormant SET status_code = 'ERR' WHERE project_id = 'e2e_project' AND id = 'u-1'", &[]).await?;

    // Merge-on-read: original file stays live (masked) + one appended file.
    let files_after = parquet_files(&env).await?;
    assert_eq!(files_after.len(), 2, "DV UPDATE should keep the masked original and append the rewritten row (got {files_after:?})");
    assert!(files_before.iter().all(|f| files_after.contains(f)), "the original file must remain live under a DV, not be rewritten");

    // Row count unchanged; the masked original row is hidden and the new one shows.
    let count: i64 = client.query_one("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 5, "UPDATE must not change the row count");

    let updated: String = client.query_one("SELECT status_code FROM mor_dormant WHERE project_id = $1 AND id = 'u-1'", &[&"e2e_project"]).await?.get(0);
    assert_eq!(updated, "ERR", "the DV-updated row must read back the new value");

    let untouched: String = client.query_one("SELECT status_code FROM mor_dormant WHERE project_id = $1 AND id = 'u-3'", &[&"e2e_project"]).await?.get(0);
    assert_eq!(untouched, "OK", "unmatched rows stay untouched");

    // DV DELETE: mask row u-2.
    client.execute("DELETE FROM mor_dormant WHERE project_id = 'e2e_project' AND id = 'u-2'", &[]).await?;
    let after_delete: i64 = client.query_one("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(after_delete, 4, "DV DELETE must hide exactly the matched row");

    let gone: i64 = client.query_one("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1 AND id = 'u-2'", &[&"e2e_project"]).await?.get(0);
    assert_eq!(gone, 0, "deleted row must not reappear");

    Ok(())
}

/// DV-DEDUP: maintenance dedup drops a physical duplicate that spans two files by
/// masking the loser with a deletion vector — NOT by rewriting the files. Then
/// OPTIMIZE consolidates the DV without resurrecting the dropped row. This is the
/// 100x lever: a whole-file rewrite to drop 0.0008% of rows becomes a bitmap.
///
/// Uses a PAST partition (real now − ≫2h) so the public `dedup_partition`
/// (slice=None) clears the 2h sealed-chunk guard, which the future-dated
/// `FROZEN_START_MICROS` fixtures never would.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dv_dedup_drops_cross_file_duplicate_without_rewriting() -> anyhow::Result<()> {
    use std::collections::HashSet;

    let env = E2eEnv::builder().with_deletion_vectors().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let client = env.pg_client().await?;

    let past = 1_735_689_600_000_000i64; // 2025-01-01, sealed relative to real now
    let sec = 1_000_000i64;
    // File 1: three unique rows + the row we will duplicate.
    for i in 0..3 {
        insert_dormant_at(&client, &format!("u-{i}"), past + i * sec).await?;
    }
    insert_dormant_at(&client, "dup", past + 100 * sec).await?;
    env.force_flush().await?;
    // File 2: the DUPLICATE (identical timestamp+id dedup key) + one more unique.
    insert_dormant_at(&client, "dup", past + 100 * sec).await?;
    insert_dormant_at(&client, "u-3", past + 3 * sec).await?;
    env.force_flush().await?;

    let files_before = parquet_files(&env).await?;
    assert_eq!(files_before.len(), 2, "expected two flushed files, got {files_before:?}");

    // Read-time DedupExec already hides the physical duplicate, so the logical
    // count is 5 before dedup runs — dedup's job is to make that physical.
    let count_before: i64 = client.query_one("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count_before, 5, "read-time dedup already resolves the duplicate");

    // DV-dedup the partition.
    let table_ref = env.db().resolve_table("e2e_project", "mor_dormant").await?;
    let date = chrono::DateTime::from_timestamp_micros(past).unwrap().date_naive();
    let (dropped, complete) = env.db().dedup_partition(&table_ref, "mor_dormant", "e2e_project", date).await?;
    assert_eq!(dropped, 1, "exactly one physical duplicate dropped");
    assert!(complete, "partition must certify clean");

    // The two source files are NOT rewritten (same paths) and exactly one now
    // carries a deletion vector — the whole point of the lever.
    let files_after = parquet_files(&env).await?;
    assert_eq!(
        files_after.iter().collect::<HashSet<_>>(),
        files_before.iter().collect::<HashSet<_>>(),
        "DV-dedup must mask, not rewrite — file paths must be unchanged"
    );
    let dv_files = {
        let t = table_ref.read().await;
        t.snapshot()?.snapshot().log_data().iter().filter(|f| f.deletion_vector_descriptor().is_some()).count()
    };
    assert_eq!(dv_files, 1, "exactly one file must carry a deletion vector, got {dv_files}");

    let count_after: i64 = client.query_one("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count_after, 5, "count unchanged by DV-dedup");

    // No resurrection: OPTIMIZE reads DV-masked, drops the loser physically, and
    // writes DV-free files.
    let unified = timefusion::database::get_unified_delta_table(env.db().unified_tables(), "mor_dormant")
        .await
        .ok_or_else(|| anyhow::anyhow!("unified table not found"))?;
    env.db().optimize_table(&unified, "mor_dormant", None).await?;
    let count_final: i64 = client.query_one("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count_final, 5, "OPTIMIZE must not resurrect the DV-dropped duplicate");
    let dup_final: i64 = client.query_one("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1 AND id = 'dup'", &[&"e2e_project"]).await?.get(0);
    assert_eq!(dup_final, 1, "the duplicate must resolve to exactly one row");

    Ok(())
}

/// Deletion vectors live in the Delta log, not the WAL. A full crash-restart must
/// reload them from the committed log so masked rows stay masked and updates persist —
/// guards the snapshot-reload / checkpoint-replay path against dropping DV descriptors.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dv_state_survives_restart() -> anyhow::Result<()> {
    let mut env = E2eEnv::builder().with_deletion_vectors().with_bucket_duration(Duration::from_secs(60)).start().await?;
    {
        let client = env.pg_client().await?;
        let sec = 1_000_000i64;
        for i in 0..6 {
            insert_dormant_at(&client, &format!("r-{i}"), FROZEN_START_MICROS + i * sec).await?;
        }
        env.force_flush().await?;
        client.execute("DELETE FROM mor_dormant WHERE project_id = 'e2e_project' AND id IN ('r-1','r-2')", &[]).await?;
        client.execute("UPDATE mor_dormant SET status_code = 'ERR' WHERE project_id = 'e2e_project' AND id = 'r-3'", &[]).await?;
        let count: i64 = client.query_one("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
        assert_eq!(count, 4, "pre-restart count wrong");
    }

    env.restart().await?;

    let client = env.pg_client().await?;
    let count: i64 = client.query_one("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 4, "DV-deleted rows resurrected across restart");
    for id in ["r-1", "r-2"] {
        let gone: i64 = client.query_one("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1 AND id = $2", &[&"e2e_project", &id]).await?.get(0);
        assert_eq!(gone, 0, "deleted row {id} came back after restart");
    }
    let updated: String = client.query_one("SELECT status_code FROM mor_dormant WHERE project_id = $1 AND id = 'r-3'", &[&"e2e_project"]).await?.get(0);
    assert_eq!(updated, "ERR", "DV update lost across restart");
    Ok(())
}

/// OPTIMIZE/compaction must consolidate deletion vectors: reading DV-masked files,
/// dropping the deleted rows, and producing DV-free files — never resurrecting the
/// logically-deleted rows.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dv_compaction_consolidates_deletion_vectors() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_deletion_vectors().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let client = env.pg_client().await?;

    let sec = 1_000_000i64;
    for i in 0..20 {
        insert_dormant_at(&client, &format!("c-{i}"), FROZEN_START_MICROS + i * sec).await?;
    }
    env.force_flush().await?;

    // DV DELETE 3 rows and DV UPDATE 2 rows (mask + append).
    client.execute("DELETE FROM mor_dormant WHERE project_id = 'e2e_project' AND id IN ('c-1','c-2','c-3')", &[]).await?;
    client.execute("UPDATE mor_dormant SET status_code = 'ERR' WHERE project_id = 'e2e_project' AND id IN ('c-4','c-5')", &[]).await?;

    // Full compaction: reads DV-masked data, drops deleted rows, writes DV-free files.
    let db = env.db();
    let table_ref =
        timefusion::database::get_unified_delta_table(db.unified_tables(), "mor_dormant").await.ok_or_else(|| anyhow::anyhow!("unified table not found"))?;
    db.optimize_table(&table_ref, "mor_dormant", None).await?;

    // Post-compaction: deleted rows stay gone, updated rows keep their new value.
    let count: i64 = client.query_one("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 17, "compaction resurrected DV-deleted rows");

    let errs: i64 = client.query_one("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1 AND status_code = 'ERR'", &[&"e2e_project"]).await?.get(0);
    assert_eq!(errs, 2, "DV-updated rows lost their value across compaction");

    for id in ["c-1", "c-2", "c-3"] {
        let gone: i64 = client.query_one("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1 AND id = $2", &[&"e2e_project", &id]).await?.get(0);
        assert_eq!(gone, 0, "deleted row {id} reappeared after compaction");
    }
    Ok(())
}

/// UPDATE ... FROM (the hash-enrichment MERGE shape) as merge-on-read: matched
/// target rows are masked + their updated copies appended, not whole-file rewritten.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dv_merge_update_from_source_masks_and_appends() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_deletion_vectors().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let client = env.pg_client().await?;

    let sec = 1_000_000i64;
    for i in 0..5 {
        insert_dormant_at(&client, &format!("m-{i}"), FROZEN_START_MICROS + i * sec).await?;
    }
    env.advance(Duration::from_secs(180));
    env.force_flush().await?;
    let files_before = parquet_files(&env).await?;
    assert_eq!(files_before.len(), 1, "expected one flushed data file, got {files_before:?}");

    // MERGE-update: join the target against a VALUES source on id, set status_code
    // from the source. Routes through perform_delta_merge_update -> DV merge op.
    client
        .execute(
            "UPDATE mor_dormant SET status_code = src.newcode \
             FROM (VALUES ('m-1', 'X1'), ('m-3', 'X3')) AS src(sid, newcode) \
             WHERE mor_dormant.project_id = 'e2e_project' AND mor_dormant.id = src.sid",
            &[],
        )
        .await?;

    let files_after = parquet_files(&env).await?;
    assert!(
        files_after.len() > files_before.len() && files_before.iter().all(|f| files_after.contains(f)),
        "DV merge-update should keep the masked original and append updated rows (before={files_before:?} after={files_after:?})"
    );

    let count: i64 = client.query_one("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 5, "merge-update must not change the row count");

    for (id, expected) in [("m-1", "X1"), ("m-3", "X3"), ("m-2", "OK"), ("m-0", "OK")] {
        let got: String = client.query_one("SELECT status_code FROM mor_dormant WHERE project_id = $1 AND id = $2", &[&"e2e_project", &id]).await?.get(0);
        assert_eq!(got, expected, "row {id} should read status_code={expected}");
    }

    Ok(())
}
