//! Post-commit hook resilience: checkpointing stays off the commit path, the
//! landed probe classifies commits correctly, and reconcile removes dangling Adds.

use std::sync::atomic::Ordering::Relaxed;

use timefusion::observability::maintenance_stats;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_for};
use super::ordering_pushdown::{count_rows, drain_membuffer};

/// Build an env (optionally forcing `checkpoint_interval`), insert `n` rows and
/// flush them to Delta; asserts the flush succeeded.
async fn flushed_env(checkpoint_interval: Option<u64>, n: i64) -> anyhow::Result<E2eEnv> {
    let mut b = E2eEnv::builder();
    if let Some(i) = checkpoint_interval {
        b = b.with_checkpoint_interval(i);
    }
    let env = b.start().await?;
    let client = env.pg_client().await?;
    for i in 0..n {
        insert_for(&client, "e2e_project", &format!("row-{i}"), FROZEN_START_MICROS + i * 1_000).await?;
    }
    assert_eq!(env.force_flush().await?.buckets_failed, 0, "flush must succeed");
    Ok(env)
}

/// Advance past retention and evict so the COUNT reads purely from Delta.
async fn drained_count(env: &E2eEnv) -> anyhow::Result<i64> {
    drain_membuffer(env).await?;
    count_rows(&env.pg_client().await?, "e2e_project").await
}

/// The flush commit path must NOT checkpoint, even with `checkpoint_interval = 1`:
/// a failing checkpoint/log-cleanup hook must never fail a commit that landed.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn commit_path_does_not_checkpoint() -> anyhow::Result<()> {
    let env = flushed_env(Some(1), 40).await?;

    let checkpoints = env.db().test_checkpoint_file_count("e2e_project", "otel_logs_and_spans").await?;
    assert_eq!(checkpoints, 0, "commit path must NOT checkpoint (hook must be off the flush path)");

    assert_eq!(drained_count(&env).await?, 40, "rows lost on the flush path");
    Ok(())
}

/// The landed probe drives whether the flush error arm deletes staged parquet,
/// so it must answer Landed for committed adds and NotLanded for adds never logged.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn probe_distinguishes_landed_from_not_landed() -> anyhow::Result<()> {
    let env = flushed_env(None, 10).await?;

    assert!(env.db().test_probe_landed("e2e_project", "otel_logs_and_spans").await?, "committed adds ⇒ Landed");
    assert!(env.db().test_probe_bogus_not_landed("e2e_project", "otel_logs_and_spans").await?, "an add never written to the log ⇒ NotLanded");
    Ok(())
}

/// The out-of-band maintenance task is the only thing that checkpoints; it must
/// create one once the version advanced by `checkpoint_interval`.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn out_of_band_checkpoint_runs() -> anyhow::Result<()> {
    let env = flushed_env(Some(1), 5).await?;
    assert_eq!(env.db().test_checkpoint_file_count("e2e_project", "otel_logs_and_spans").await?, 0, "no checkpoint yet");

    let before = maintenance_stats().checkpoints_created.load(Relaxed);
    env.db().run_checkpoint_maintenance().await;
    assert!(maintenance_stats().checkpoints_created.load(Relaxed) > before, "checkpoint task ran no checkpoint");
    assert!(env.db().test_checkpoint_file_count("e2e_project", "otel_logs_and_spans").await? >= 1, "out-of-band task must create a checkpoint file");
    Ok(())
}

/// Reconcile must Remove an Add whose parquet was deleted and bump `dangling_removed`.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn reconcile_removes_dangling_add() -> anyhow::Result<()> {
    let env = flushed_env(None, 10).await?;

    // A committed parquet vanishes.
    env.db().test_delete_first_active_file("e2e_project", "otel_logs_and_spans").await?;

    let before = maintenance_stats().dangling_removed.load(Relaxed);
    env.db().run_reconcile_maintenance().await;
    assert!(maintenance_stats().dangling_removed.load(Relaxed) > before, "reconcile did not Remove the dangling Add");

    // Planning a Delta scan must not error afterwards.
    let _ = drained_count(&env).await?;
    Ok(())
}
