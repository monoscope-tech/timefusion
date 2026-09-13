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

use super::harness::{E2eEnv, E2eEnvBuilder, FROZEN_START_MICROS, insert_dormant_at, insert_dormant_named};
use tokio_postgres::Client;

/// DV-enabled env; callers chain any extra builder options plus `.start()`.
fn dv_env() -> E2eEnvBuilder {
    E2eEnv::builder().with_deletion_vectors().with_bucket_duration(Duration::from_secs(60))
}

/// Live parquet data files for the default tenant table.
async fn parquet_files(env: &E2eEnv) -> anyhow::Result<Vec<String>> {
    let table_ref = env.db().resolve_table("e2e_project", "mor_dormant").await?;
    let uris: Vec<String> = { table_ref.read().await.get_file_uris()?.collect() };
    Ok(uris.into_iter().filter(|u| u.ends_with(".parquet")).collect())
}

/// `n` rows `{prefix}-0..n` one second apart from `base`.
async fn seed(client: &Client, prefix: &str, n: i64, base: i64) -> anyhow::Result<()> {
    for i in 0..n {
        insert_dormant_at(client, &format!("{prefix}-{i}"), base + i * 1_000_000).await?;
    }
    Ok(())
}

/// Tenant row count, with an optional extra `AND ...` predicate.
async fn count(client: &Client, filter: &str) -> anyhow::Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM mor_dormant WHERE project_id = $1 {filter}");
    Ok(client.query_one(sql.as_str(), &[&"e2e_project"]).await?.get(0))
}

async fn status_code(client: &Client, id: &str) -> anyhow::Result<String> {
    Ok(client.query_one("SELECT status_code FROM mor_dormant WHERE project_id = $1 AND id = $2", &[&"e2e_project", &id]).await?.get(0))
}

/// Every id must read back invisible; `ctx` names the operation that must not resurrect it.
async fn assert_ids_gone(client: &Client, ids: &[&str], ctx: &str) -> anyhow::Result<()> {
    for id in ids {
        assert_eq!(count(client, &format!("AND id = '{id}'")).await?, 0, "row {id} must stay gone: {ctx}");
    }
    Ok(())
}

/// Seed `n` rows and flush into exactly ONE Delta parquet file (the advance past the
/// bucket duration is what makes the bucket flushable), so DML targets Delta, not MemBuffer.
async fn seed_into_one_file(env: &E2eEnv, client: &Client, prefix: &str, n: i64) -> anyhow::Result<Vec<String>> {
    seed(client, prefix, n, FROZEN_START_MICROS).await?;
    env.advance(Duration::from_secs(180));
    env.force_flush().await?;
    let files = parquet_files(env).await?;
    assert_eq!(files.len(), 1, "expected one flushed data file, got {files:?}");
    Ok(files)
}

async fn unified_table(env: &E2eEnv) -> anyhow::Result<std::sync::Arc<tokio::sync::RwLock<deltalake::DeltaTable>>> {
    timefusion::database::get_unified_delta_table(env.db().unified_tables(), "mor_dormant").await.ok_or_else(|| anyhow::anyhow!("unified table not found"))
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dv_update_and_delete_hide_rows_without_rewriting_files() -> anyhow::Result<()> {
    let env = dv_env().start().await?;
    let client = env.pg_client().await?;

    let files_before = seed_into_one_file(&env, &client, "u", 5).await?;

    // DV UPDATE: mask row u-1 in the original file and append its rewritten copy.
    client.execute("UPDATE mor_dormant SET status_code = 'ERR' WHERE project_id = 'e2e_project' AND id = 'u-1'", &[]).await?;

    // Merge-on-read: original file stays live (masked) + one appended file.
    let files_after = parquet_files(&env).await?;
    assert_eq!(files_after.len(), 2, "DV UPDATE should keep the masked original and append the rewritten row (got {files_after:?})");
    assert!(files_before.iter().all(|f| files_after.contains(f)), "the original file must remain live under a DV, not be rewritten");

    // Row count unchanged; the masked original row is hidden and the new one shows.
    assert_eq!(count(&client, "").await?, 5, "UPDATE must not change the row count");
    assert_eq!(status_code(&client, "u-1").await?, "ERR", "the DV-updated row must read back the new value");
    assert_eq!(status_code(&client, "u-3").await?, "OK", "unmatched rows stay untouched");

    // DV DELETE: mask row u-2.
    client.execute("DELETE FROM mor_dormant WHERE project_id = 'e2e_project' AND id = 'u-2'", &[]).await?;
    assert_eq!(count(&client, "").await?, 4, "DV DELETE must hide exactly the matched row");
    assert_ids_gone(&client, &["u-2"], "DV DELETE").await?;

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

    // The two explicit flushes define the two source files. A background tick
    // between INSERTs can otherwise split the fixture on a busy test runner.
    let env = dv_env().with_flush_interval(Duration::from_secs(3600)).start().await?;
    let client = env.pg_client().await?;

    let past = 1_735_689_600_000_000i64; // 2025-01-01, sealed relative to real now
    let sec = 1_000_000i64;
    // File 1: three unique rows + the row we will duplicate.
    seed(&client, "u", 3, past).await?;
    insert_dormant_at(&client, "dup", past + 100 * sec).await?;
    env.force_flush().await?;
    // File 2: the DUPLICATE (identical timestamp+id dedup key; different
    // content, else the ingest-time content-identity filter drops it before it
    // ever becomes a physical duplicate) + one more unique.
    insert_dormant_named(&client, "dup", past + 100 * sec, "span-v2").await?;
    insert_dormant_at(&client, "u-3", past + 3 * sec).await?;
    env.force_flush().await?;

    let files_before = parquet_files(&env).await?;
    assert_eq!(files_before.len(), 2, "expected two flushed files, got {files_before:?}");

    // Read-time DedupExec already hides the physical duplicate, so the logical
    // count is 5 before dedup runs — dedup's job is to make that physical.
    assert_eq!(count(&client, "").await?, 5, "read-time dedup already resolves the duplicate");

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

    assert_eq!(count(&client, "").await?, 5, "count unchanged by DV-dedup");

    // No resurrection: OPTIMIZE reads DV-masked, drops the loser physically, and
    // writes DV-free files.
    env.db().optimize_table(&unified_table(&env).await?, "mor_dormant", None).await?;
    assert_eq!(count(&client, "").await?, 5, "OPTIMIZE must not resurrect the DV-dropped duplicate");
    assert_eq!(count(&client, "AND id = 'dup'").await?, 1, "the duplicate must resolve to exactly one row");

    Ok(())
}

/// Deletion vectors live in the Delta log, not the WAL. A full crash-restart must
/// reload them from the committed log so masked rows stay masked and updates persist —
/// guards the snapshot-reload / checkpoint-replay path against dropping DV descriptors.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dv_state_survives_restart() -> anyhow::Result<()> {
    let mut env = dv_env().start().await?;
    {
        let client = env.pg_client().await?;
        seed(&client, "r", 6, FROZEN_START_MICROS).await?;
        env.force_flush().await?;
        client.execute("DELETE FROM mor_dormant WHERE project_id = 'e2e_project' AND id IN ('r-1','r-2')", &[]).await?;
        client.execute("UPDATE mor_dormant SET status_code = 'ERR' WHERE project_id = 'e2e_project' AND id = 'r-3'", &[]).await?;
        assert_eq!(count(&client, "").await?, 4, "pre-restart count wrong");
    }

    env.restart().await?;

    let client = env.pg_client().await?;
    assert_eq!(count(&client, "").await?, 4, "DV-deleted rows resurrected across restart");
    assert_ids_gone(&client, &["r-1", "r-2"], "restart must reload DVs from the Delta log").await?;
    assert_eq!(status_code(&client, "r-3").await?, "ERR", "DV update lost across restart");
    Ok(())
}

/// OPTIMIZE/compaction must consolidate deletion vectors: reading DV-masked files,
/// dropping the deleted rows, and producing DV-free files — never resurrecting the
/// logically-deleted rows.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dv_compaction_consolidates_deletion_vectors() -> anyhow::Result<()> {
    let env = dv_env().start().await?;
    let client = env.pg_client().await?;

    seed(&client, "c", 20, FROZEN_START_MICROS).await?;
    env.force_flush().await?;

    // DV DELETE 3 rows and DV UPDATE 2 rows (mask + append).
    client.execute("DELETE FROM mor_dormant WHERE project_id = 'e2e_project' AND id IN ('c-1','c-2','c-3')", &[]).await?;
    client.execute("UPDATE mor_dormant SET status_code = 'ERR' WHERE project_id = 'e2e_project' AND id IN ('c-4','c-5')", &[]).await?;

    // Full compaction: reads DV-masked data, drops deleted rows, writes DV-free files.
    env.db().optimize_table(&unified_table(&env).await?, "mor_dormant", None).await?;

    // Post-compaction: deleted rows stay gone, updated rows keep their new value.
    assert_eq!(count(&client, "").await?, 17, "compaction resurrected DV-deleted rows");
    assert_eq!(count(&client, "AND status_code = 'ERR'").await?, 2, "DV-updated rows lost their value across compaction");

    assert_ids_gone(&client, &["c-1", "c-2", "c-3"], "compaction must consolidate DVs, not resurrect rows").await?;
    Ok(())
}

/// UPDATE ... FROM (the hash-enrichment MERGE shape) as merge-on-read: matched
/// target rows are masked + their updated copies appended, not whole-file rewritten.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn dv_merge_update_from_source_masks_and_appends() -> anyhow::Result<()> {
    let env = dv_env().start().await?;
    let client = env.pg_client().await?;

    let files_before = seed_into_one_file(&env, &client, "m", 5).await?;

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

    assert_eq!(count(&client, "").await?, 5, "merge-update must not change the row count");

    for (id, expected) in [("m-1", "X1"), ("m-3", "X3"), ("m-2", "OK"), ("m-0", "OK")] {
        assert_eq!(status_code(&client, id).await?, expected, "row {id} should read status_code={expected}");
    }

    Ok(())
}
