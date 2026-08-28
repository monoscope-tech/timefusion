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
    if count != 7 {
        // WHICH LEG lost them. A bare `4 != 7` cannot distinguish rows that were
        // never durable from rows that exist but are momentarily unreadable, and
        // this failure was filed as a flake for three days on exactly that
        // ambiguity. Everything below is READ-ONLY diagnosis of a live failure.
        let ids: Vec<String> = client
            .query("SELECT id FROM otel_logs_and_spans WHERE project_id = $1 ORDER BY id", &[&"e2e_project"])
            .await?
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        // Is it TRANSIENT? If a second identical query returns 7, the rows were
        // always durable and the first read was wrong — a read-path race, not loss.
        let again: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
        // MemBuffer's own view, bypassing SQL entirely.
        let mem_rows: usize = env
            .db()
            .buffered_layer()
            .map(|layer| {
                layer
                    .mem_buffer()
                    .query("e2e_project", "otel_logs_and_spans", &[])
                    .map(|batches| batches.iter().map(datafusion::arrow::array::RecordBatch::num_rows).sum())
                    .unwrap_or(usize::MAX)
            })
            .unwrap_or(0);
        // Delta's own view, bypassing SQL entirely.
        let delta_files = match env.db().resolve_table("e2e_project", "otel_logs_and_spans").await {
            Ok(table_ref) => {
                let table = table_ref.read().await;
                table.snapshot().map(|s| s.log_data().iter().count()).unwrap_or(0)
            }
            Err(_) => 0,
        };
        panic!(
            "COUNT(*) returned {count} of 7 acked inserts.\n  \
             visible ids ({}): {ids:?}\n  \
             re-query immediately: {again} (== 7 means TRANSIENT: rows were durable, the read was wrong)\n  \
             MemBuffer rows: {mem_rows}\n  \
             Delta files: {delta_files}",
            ids.len()
        );
    }
    Ok(())
}

/// The whole suite's OCC coverage is only as sound as the object store's
/// put-if-absent. Delta commits via `PutMode::Create`; a store that answers it
/// with a plain overwrite lets two writers "commit" the same version and the
/// loser's actions — acked, committed rows — vanish with no error anywhere.
/// That is exactly how `append_during_dv_merge_is_not_dropped` was flaky until
/// 2026-07-30 (see `harness::MINIO_TAG`). Assert the precondition on the very
/// store commits are written through, cache wrapper included, so a container
/// image or storage-option change can never silently re-disarm it.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn harness_object_store_enforces_atomic_commits() -> anyhow::Result<()> {
    use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};

    let env = E2eEnv::builder().start().await?;
    let store = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?.read().await.log_store().object_store(None);
    let path = object_store::path::Path::from("_delta_log/put_if_absent_probe.json");
    let create = || PutOptions { mode: PutMode::Create, ..Default::default() };

    store.put_opts(&path, "first".into(), create()).await?;
    let clobber = store.put_opts(&path, "second".into(), create()).await;
    assert!(
        matches!(clobber, Err(object_store::Error::AlreadyExists { .. })),
        "object store did NOT enforce put-if-absent — Delta commit versions are not atomic here, so every concurrent-writer test in this suite is unsound: {clobber:?}"
    );
    assert_eq!(store.get(&path).await?.bytes().await?, "first".as_bytes(), "losing put overwrote the winning commit file");
    Ok(())
}
