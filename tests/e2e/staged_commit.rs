//! Staged-commit write path (`Database::insert_records_batch`): parquet is
//! encoded + uploaded to S3 OUTSIDE the global `delta_commit_lock`, and only
//! the tiny commit-log append is serialized. These tests pin the two
//! correctness properties that the split must preserve:
//!   1. flushed rows actually persist to Delta (read back after MemBuffer drain)
//!   2. concurrent commits to the shared unified table lose nothing (OCC retry)
//!
//! plus the schema-evolution fallback to the locked WriteBuilder merge path.

use std::{sync::Arc, time::Duration};

use arrow::{
    array::{RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_for};

/// One day in micros — well past the test retention so a single `force_evict`
/// drains every bucket out of MemBuffer, leaving Delta as the only source.
const ONE_DAY_MICROS: i64 = 24 * 60 * 60 * 1_000_000;

/// Force-flush, then advance past retention and evict so MemBuffer is empty —
/// any row still queryable afterwards was persisted to Delta by the staged
/// commit, not served from the in-memory buffer.
async fn flush_then_drain(env: &E2eEnv) -> anyhow::Result<()> {
    env.force_flush().await?;
    env.advance(Duration::from_micros(ONE_DAY_MICROS as u64));
    env.force_evict().await?;
    let mem = env.snapshot_stats().mem_total_rows;
    assert_eq!(mem, 0, "MemBuffer not drained ({mem} rows left) — query below wouldn't isolate Delta");
    Ok(())
}

/// Rows flushed through the staged path must be readable from Delta after the
/// MemBuffer is drained. If the staged parquet upload or the commit-log append
/// dropped anything, the post-drain count is short.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn staged_flush_persists_to_delta() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let client = env.pg_client().await?;

    let n = 80;
    for i in 0..n {
        insert_for(&client, "e2e_project", &format!("row-{i}"), FROZEN_START_MICROS + i * 1_000).await?;
    }

    flush_then_drain(&env).await?;

    // MemBuffer is drained (asserted in flush_then_drain), so this count is
    // served purely from Delta — proof the staged commit persisted every row.
    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, n, "rows lost on the staged commit path (read purely from Delta)");
    Ok(())
}

/// Several default projects share ONE unified Delta table. This drives the
/// staged path directly (skip_queue=true) from 5 tasks *in parallel*, so their
/// parquet uploads overlap outside `delta_commit_lock` and their commits queue
/// on it. Every row must land — no tenant lost to an interleaving.
///
/// Note: this exercises concurrent *staging* + the serialized commit queue, NOT
/// the OCC retry branch — within one process `delta_commit_lock` + the
/// refresh-under-lock serialize all commits, so they never self-conflict. OCC
/// retry only fires against an external/maintenance committer, which isn't
/// reproducible here. The earlier `force_flush` version didn't even overlap the
/// uploads (flush_all_now loops buckets sequentially).
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_unified_table_staging_loses_nothing() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let projects = ["default_a", "default_b", "default_c", "default_d", "default_e"];
    let per = 20;
    for p in projects {
        env.db().get_or_create_table(p, "otel_logs_and_spans").await?;
    }

    let mut handles = Vec::new();
    for p in projects {
        let db = env.db().clone();
        handles.push(tokio::spawn(async move {
            for i in 0..per {
                let batch =
                    timefusion::test_utils::test_helpers::json_to_batch(vec![timefusion::test_utils::test_helpers::test_span(&format!("{p}-{i}"), "span", p)])?;
                // skip_queue=true → straight to the staged commit path, bypassing MemBuffer.
                db.insert_records_batch(p, "otel_logs_and_spans", vec![batch], true, None).await?;
            }
            anyhow::Ok(())
        }));
    }
    for h in handles {
        h.await??;
    }

    let client = env.pg_client().await?;
    for p in projects {
        let c: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&p]).await?.get(0);
        assert_eq!(c, per as i64, "tenant {p} lost rows under concurrent staged commits to the unified table");
    }
    Ok(())
}

/// A batch carrying a column absent from the table schema cannot go through the
/// Default-mode staged writer (delta-rs forbids MergeSchema on a partitioned
/// table); it must fall back to the locked WriteBuilder merge path and still
/// commit. Driven directly through `insert_records_batch` (skip_queue=true,
/// the flush path) because pgwire INSERTs are schema-validated upstream and
/// can't introduce a new column.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn schema_evolving_batch_falls_back_to_merge() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let db = env.db();
    db.get_or_create_table("evolve_proj", "otel_logs_and_spans").await?;

    // Standard otel batch + one column the table schema doesn't have.
    let base = timefusion::test_utils::test_helpers::json_to_batch(vec![timefusion::test_utils::test_helpers::test_span("evo-1", "span", "evolve_proj")])?;
    let n = base.num_rows();
    let mut fields: Vec<Arc<Field>> = base.schema().fields().iter().cloned().collect();
    let mut cols = base.columns().to_vec();
    fields.push(Arc::new(Field::new("staged_commit_new_col", DataType::Utf8, true)));
    cols.push(Arc::new(StringArray::from(vec!["x"; n])));
    let evolved = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?;

    // Must not error — the fallback merges the new column into the table schema.
    let added = db.insert_records_batch("evolve_proj", "otel_logs_and_spans", vec![evolved], true, None).await?;
    assert!(!added.is_empty(), "merge fallback wrote no files");

    let client = env.pg_client().await?;
    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"evolve_proj"]).await?.get(0);
    assert_eq!(count, 1, "schema-evolving row not persisted via merge fallback");
    Ok(())
}
