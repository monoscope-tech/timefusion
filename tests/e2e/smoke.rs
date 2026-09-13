//! Smoke tests: pgwire answers queries at all, COUNT(*) is correct, and the
//! harness object store enforces put-if-absent.

use std::time::Duration;

use super::{harness::E2eEnv, ordering_pushdown::count_rows};

const QUERY_RESPONSE_BUDGET: Duration = Duration::from_secs(5);

/// Single-row INSERT with id/name/status_code/status_message/level/summary bound as `$2..$7`.
fn insert_sql() -> String {
    format!(
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) \
         VALUES ($1, {}, '{}', $2, $3, $4, $5, $6, ARRAY[]::text[], $7)",
        chrono::Utc::now().date_naive(),
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
    )
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn pgwire_query_returns_response() -> anyhow::Result<()> {
    let env = E2eEnv::builder().start().await?;
    let client = env.pg_client().await?;
    let insert = insert_sql();

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
    let insert = insert_sql();

    for i in 0..7 {
        client.execute(&insert, &[&"e2e_project", &format!("smoke-{i}"), &"s", &"OK", &"m", &"INFO", &vec!["s"]]).await?;
    }
    let count = count_rows(&client, "e2e_project").await?;
    if count != 7 {
        // Read-only diagnosis: identify WHICH leg lost the rows before panicking.
        let ids: Vec<String> = client
            .query("SELECT id FROM otel_logs_and_spans WHERE project_id = $1 ORDER BY id", &[&"e2e_project"])
            .await?
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        // A second query returning 7 means a read-path race, not row loss.
        let again = count_rows(&client, "e2e_project").await?;
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
        // Delta's own view, bypassing SQL. Version matters as well as file count:
        // a file existing and the planned-against snapshot knowing it are different claims.
        let (delta_files, delta_version) = match env.db().resolve_table("e2e_project", "otel_logs_and_spans").await {
            Ok(table_ref) => {
                let table = table_ref.read().await;
                (table.snapshot().map(|s| s.log_data().iter().count()).unwrap_or(0), table.version())
            }
            Err(_) => (0, None),
        };
        // Discriminator: `7 - mem_rows` here means the union (mask/dedup) lost them;
        // 0 means the scan cannot see a committed file (snapshot staleness).
        let delta_only = match env.db().query_delta_only("SELECT id FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY id").await {
            Ok(batches) => datafusion::arrow::util::pretty::pretty_format_batches(&batches).map_or_else(|e| e.to_string(), |t| t.to_string()),
            Err(e) => format!("query_delta_only failed: {e}"),
        };
        panic!(
            "COUNT(*) returned {count} of 7 acked inserts.\n  \
             visible ids ({}): {ids:?}\n  \
             re-query immediately: {again} (== 7 means TRANSIENT: rows were durable, the read was wrong)\n  \
             MemBuffer rows: {mem_rows}\n  \
             Delta files: {delta_files} at version {delta_version:?}\n  \
             Delta-only ids:\n{delta_only}",
            ids.len()
        );
    }
    Ok(())
}

/// Precondition for every concurrent-writer test: Delta commits via `PutMode::Create`,
/// so a store that silently overwrites instead lets two writers claim the same version.
/// Probed through the same store (cache wrapper included) that commits are written to.
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
