//! Post-commit hook resilience (2026-07-09 incident). delta-rs runs checkpoint +
//! expired-log cleanup in a post-commit hook AFTER `N.json` is durably
//! written; an R2 500 there surfaced as a commit error that the flush path
//! misread as "commit never landed" → it deleted the parquet the landed commit
//! referenced (14 dangling Adds). These tests pin the four fixes:
//!
//! - A: the commit path does NOT checkpoint (Phase 1) — so a checkpoint/log
//!   endpoint being down can't fail a flush; checkpointing moved out-of-band.
//! - B: the landed probe reports Landed for committed adds and NotLanded for
//!   adds never written to the log (Phase 3) — it never false-"Landed"s a
//!   failed commit (which would drain the bucket into deleted parquet).
//! - C: the out-of-band checkpoint task DOES checkpoint (Phase 2).
//! - D: reconcile Remove's an Add whose parquet was deleted (Phase 4).

use std::{sync::atomic::Ordering::Relaxed, time::Duration};

use timefusion::observability::maintenance_stats;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_for};

const ONE_DAY_MICROS: i64 = 24 * 60 * 60 * 1_000_000;

/// Build an env (optionally forcing `checkpoint_interval`), insert `n` rows and
/// flush them to Delta. The flush must succeed — every test below builds on a
/// landed commit.
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
    env.advance(Duration::from_micros(ONE_DAY_MICROS as u64));
    env.force_evict().await?;
    let mem = env.snapshot_stats().mem_total_rows;
    assert_eq!(mem, 0, "MemBuffer not drained ({mem} rows left)");
    let client = env.pg_client().await?;
    Ok(client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0))
}

/// A. The flush commit path must NOT create a checkpoint, even with
/// `checkpoint_interval = 1` (which would force a checkpoint on every commit if
/// the delta-rs post-commit hook were still on the commit path). This is the
/// regression guard for the incident's root cause: a checkpoint/log-cleanup
/// hook failing a commit that already landed. Flush must succeed and persist.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn commit_path_does_not_checkpoint() -> anyhow::Result<()> {
    let env = flushed_env(Some(1), 40).await?;

    let checkpoints = env.db().test_checkpoint_file_count("e2e_project", "otel_logs_and_spans").await?;
    assert_eq!(checkpoints, 0, "commit path must NOT checkpoint (hook must be off the flush path)");

    assert_eq!(drained_count(&env).await?, 40, "rows lost on the flush path");
    Ok(())
}

/// B. The landed probe: Landed for the actual committed adds, NotLanded for an
/// add whose path was never written to the log. This is the decision the flush
/// error arm makes before deleting staged parquet — a false-"Landed" on a
/// genuinely-failed commit would drain the bucket into files that get cleaned
/// up → data loss; a false-"NotLanded" on a landed commit would delete the
/// committed parquet → the incident's dangling Adds.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn probe_distinguishes_landed_from_not_landed() -> anyhow::Result<()> {
    let env = flushed_env(None, 10).await?;

    assert!(env.db().test_probe_landed("e2e_project", "otel_logs_and_spans").await?, "committed adds ⇒ Landed");
    assert!(env.db().test_probe_bogus_not_landed("e2e_project", "otel_logs_and_spans").await?, "an add never written to the log ⇒ NotLanded");
    Ok(())
}

/// C. The out-of-band checkpoint task creates a checkpoint once the version has
/// advanced by `checkpoint_interval` (Phase 2 — the only thing that checkpoints
/// now). Pairs with test A: A proves the commit path leaves 0 checkpoints, this
/// proves the maintenance task then creates one.
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

/// D. Reconcile Remove's an Add whose parquet was deleted (the incident's
/// residue) and bumps `dangling_removed` (Phase 4).
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn reconcile_removes_dangling_add() -> anyhow::Result<()> {
    let env = flushed_env(None, 10).await?;

    // Simulate the commit-path deletion bug: a committed parquet vanishes.
    env.db().test_delete_first_active_file("e2e_project", "otel_logs_and_spans").await?;

    let before = maintenance_stats().dangling_removed.load(Relaxed);
    env.db().run_reconcile_maintenance().await;
    assert!(maintenance_stats().dangling_removed.load(Relaxed) > before, "reconcile did not Remove the dangling Add");

    // Table is consistent again: planning a Delta scan must not error on the
    let _ = drained_count(&env).await?;
    Ok(())
}
