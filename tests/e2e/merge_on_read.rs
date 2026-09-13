//! Merge-on-read DML (`docs/plans/2026-08-01-merge-on-read-dml.md`): on a
//! `version_append` table an UPDATE/DELETE appends a new row version instead of
//! rewriting anything, and the read path resolves versions.
//!
//! These run against `mor_versioned` — the only shipped schema with
//! `version_append: true` — through the full prod path (pgwire → WAL →
//! MemBuffer → flush → Delta), because every bug this file guards
//! against is an interaction between those layers rather than anything visible
//! to a unit test.

use std::time::Duration;

use timefusion::support;

use super::harness::{E2eEnv, FROZEN_START_MICROS};

const PROJECT: &str = "mor_project";

/// Short bucket/retention so a bucket can be sealed and flushed within a test.
async fn env_and_client() -> anyhow::Result<(E2eEnv, tokio_postgres::Client)> {
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).with_retention(Duration::from_secs(120)).start().await?;
    let client = env.pg_client().await?;
    Ok((env, client))
}

/// Commit MemBuffer to Delta and drop it, so what follows reads from Delta.
async fn settle(env: &E2eEnv) -> anyhow::Result<()> {
    env.force_flush().await?;
    env.force_evict().await?;
    Ok(())
}

/// `mor_versioned` has no `insert_at` helper (that one is otel-shaped), so
/// rows are written with plain SQL at an explicit timestamp.
async fn insert_row(client: &tokio_postgres::Client, id: &str, name: &str, ts_micros: i64) -> anyhow::Result<()> {
    let ts = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts_micros).unwrap();
    client
        .execute(
            "INSERT INTO mor_versioned (project_id, timestamp, id, date, name) VALUES ($1, $2, $3, $4, $5)",
            &[&PROJECT, &ts, &id, &ts.date_naive(), &name],
        )
        .await?;
    Ok(())
}

/// `row0..rowN-1`, all in one bucket at `FROZEN_START_MICROS`.
async fn seed(client: &tokio_postgres::Client, n: i64) -> anyhow::Result<()> {
    for i in 0..n {
        insert_row(client, &format!("row{i}"), "base", FROZEN_START_MICROS + i).await?;
    }
    Ok(())
}

async fn count(client: &tokio_postgres::Client) -> anyhow::Result<i64> {
    Ok(client.query_one("SELECT COUNT(*) FROM mor_versioned WHERE project_id = $1", &[&PROJECT]).await?.get(0))
}

async fn name_of(client: &tokio_postgres::Client, id: &str) -> anyhow::Result<Option<String>> {
    let rows = client.query("SELECT name FROM mor_versioned WHERE project_id = $1 AND id = $2", &[&PROJECT, &id]).await?;
    assert!(rows.len() <= 1, "a dedup key must resolve to at most ONE row, got {} — version collapse is broken", rows.len());
    Ok(rows.first().map(|r| r.get(0)))
}

/// THE regression this whole design turns on. An UPDATE appends a new version
/// carrying the row's ORIGINAL timestamp, so it lands in a bucket whose window
/// Delta already holds every other row for. If that bucket is left to claim the
/// window (MemBuffer authoritative ⇒ Delta excluded), the update makes every
/// UNTOUCHED row in the window vanish — a 1-row statement silently deleting
/// thousands. Guarded by `BufferedWriteLayer::insert_versions`.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn update_appends_a_version_without_hiding_the_windows_other_rows() -> anyhow::Result<()> {
    let (env, client) = env_and_client().await?;

    // All eight rows share one bucket, so the updated row's window is exactly
    // the window the other seven live in.
    seed(&client, 8).await?;
    assert_eq!(count(&client).await?, 8, "baseline");

    // Commit to Delta so the update below is resolved against rows that live
    // in Delta, not MemBuffer. The clock must leave the rows' bucket first —
    // only a SEALED bucket is flushable.
    support::set_micros(FROZEN_START_MICROS + 10 * 60 * 1_000_000);
    settle(&env).await?;

    client.execute("UPDATE mor_versioned SET name = 'enriched' WHERE project_id = $1 AND id = 'row3'", &[&PROJECT]).await?;

    assert_eq!(count(&client).await?, 8, "an UPDATE must not hide the untouched rows sharing its time window");
    assert_eq!(name_of(&client, "row3").await?.as_deref(), Some("enriched"), "the appended version must win");
    assert_eq!(name_of(&client, "row4").await?.as_deref(), Some("base"), "a sibling row must be untouched");

    // ...and the version still resolves after the appended row itself is
    // flushed, which is when merge-on-read (rather than MemBuffer) is what
    // keeps the newer copy visible.
    settle(&env).await?;
    assert_eq!(name_of(&client, "row3").await?.as_deref(), Some("enriched"), "the newer version must survive its own flush");
    assert_eq!(count(&client).await?, 8, "flushing the appended version must not double-count it");

    Ok(())
}

/// A DELETE appends a tombstone version; the row disappears from reads and from
/// COUNT(*), and its siblings do not.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn delete_appends_a_tombstone_that_hides_only_its_own_key() -> anyhow::Result<()> {
    let (env, client) = env_and_client().await?;

    seed(&client, 4).await?;
    settle(&env).await?;

    client.execute("DELETE FROM mor_versioned WHERE project_id = $1 AND id = 'row1'", &[&PROJECT]).await?;

    assert_eq!(name_of(&client, "row1").await?, None, "a tombstoned key must not be returned");
    assert_eq!(count(&client).await?, 3, "COUNT(*) must not count a tombstoned key");
    assert_eq!(name_of(&client, "row2").await?.as_deref(), Some("base"), "a sibling must survive the tombstone");

    // The tombstone must still suppress the row once it is itself committed —
    // i.e. the suppression is a property of the data, not of the tombstone
    // happening to sit in MemBuffer.
    settle(&env).await?;
    assert_eq!(name_of(&client, "row1").await?, None, "the tombstone must survive its own flush");
    assert_eq!(count(&client).await?, 3, "COUNT(*) after the tombstone is flushed");

    Ok(())
}

/// Re-updating the same key repeatedly must converge on the newest version
/// rather than accumulating visible rows — the read-amplification property that
/// bounds merge-on-read.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn repeated_updates_of_one_key_still_resolve_to_one_row() -> anyhow::Result<()> {
    let (env, client) = env_and_client().await?;

    insert_row(&client, "hot", "v0", FROZEN_START_MICROS).await?;
    env.force_flush().await?;

    for v in 1..=5 {
        client.execute(&format!("UPDATE mor_versioned SET name = 'v{v}' WHERE project_id = $1 AND id = 'hot'"), &[&PROJECT]).await?;
    }

    assert_eq!(name_of(&client, "hot").await?.as_deref(), Some("v5"), "the greatest version stamp must win");
    assert_eq!(count(&client).await?, 1, "five versions of one key must read as one row");

    Ok(())
}
