//! Staged-commit write path (`Database::insert_records_batch`): parquet upload
//! happens outside the global `delta_commit_lock`; only the commit-log append is
//! serialized.

use std::{sync::Arc, time::Duration};

use arrow::{
    array::{RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};

use timefusion::support::test_helpers::{json_to_batch, test_span};

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_for};
use super::ordering_pushdown::{count_rows, drain_membuffer};

/// Short buckets so one `force_flush` seals them — shared by every test here.
async fn env60() -> anyhow::Result<E2eEnv> {
    E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).start().await
}

fn span_batch(id: &str, project: &str) -> anyhow::Result<RecordBatch> {
    json_to_batch(vec![test_span(id, "span", project)])
}

/// Rows flushed through the staged path must be readable from Delta after the
/// MemBuffer is drained.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn staged_flush_persists_to_delta() -> anyhow::Result<()> {
    let env = env60().await?;
    let client = env.pg_client().await?;

    let n = 80;
    for i in 0..n {
        insert_for(&client, "e2e_project", &format!("row-{i}"), FROZEN_START_MICROS + i * 1_000).await?;
    }

    // Drained MemBuffer ⇒ the count below is served purely from Delta.
    env.force_flush().await?;
    drain_membuffer(&env).await?;

    assert_eq!(count_rows(&client, "e2e_project").await?, n, "rows lost on the staged commit path (read purely from Delta)");
    Ok(())
}

/// Several default projects share ONE unified Delta table; parallel staged
/// writes must lose no tenant's rows. Covers concurrent staging + the serialized
/// commit queue, not the OCC retry branch (in-process commits never self-conflict).
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_unified_table_staging_loses_nothing() -> anyhow::Result<()> {
    let env = env60().await?;
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
                // skip_queue=true → staged commit path, bypassing MemBuffer.
                db.insert_records_batch(p, "otel_logs_and_spans", vec![span_batch(&format!("{p}-{i}"), p)?], true, None).await?;
            }
            anyhow::Ok(())
        }));
    }
    for h in handles {
        h.await??;
    }

    let client = env.pg_client().await?;
    for p in projects {
        assert_eq!(count_rows(&client, p).await?, per as i64, "tenant {p} lost rows under concurrent staged commits to the unified table");
    }
    Ok(())
}

/// A batch with a column absent from the table schema cannot use the staged
/// writer (delta-rs forbids MergeSchema on a partitioned table); it must fall
/// back to the locked WriteBuilder merge path and still commit. Driven through
/// `insert_records_batch` because pgwire INSERTs are schema-validated upstream.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn schema_evolving_batch_falls_back_to_merge() -> anyhow::Result<()> {
    let env = env60().await?;
    let db = env.db();
    db.get_or_create_table("evolve_proj", "otel_logs_and_spans").await?;

    let base = span_batch("evo-1", "evolve_proj")?;
    let n = base.num_rows();
    let mut fields: Vec<Arc<Field>> = base.schema().fields().iter().cloned().collect();
    let mut cols = base.columns().to_vec();
    fields.push(Arc::new(Field::new("staged_commit_new_col", DataType::Utf8, true)));
    cols.push(Arc::new(StringArray::from(vec!["x"; n])));
    let evolved = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?;

    let added = db.insert_records_batch("evolve_proj", "otel_logs_and_spans", vec![evolved], true, None).await?;
    assert!(!added.is_empty(), "merge fallback wrote no files");

    let client = env.pg_client().await?;
    assert_eq!(count_rows(&client, "evolve_proj").await?, 1, "schema-evolving row not persisted via merge fallback");
    Ok(())
}
