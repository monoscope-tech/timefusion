//! Hot-tail compaction must leave the partition with an honest sorted footer:
//! the reader's `derive_common_ordering` is all-or-nothing, so one file without
//! `sorting_columns` costs the whole scan its declared ordering.
//!
//! These tests must exercise the SIZE condition, not the happy path — a bin that
//! fits the in-process sort budget was always sorted correctly.
//! `with_sort_skip_bytes(0)` shrinks that budget to zero to force the over-budget
//! path.

use std::time::Duration;

use timefusion::{database::TailPass, support};
use tokio_postgres::Client;

use super::harness::{E2eEnv, E2eEnvBuilder, FROZEN_START_MICROS, insert_at};

const SEC: i64 = 1_000_000;
const PROJECT: &str = "e2e_project";
const TABLE: &str = "otel_logs_and_spans";

/// Shape shared by every test here: 60s buckets, an hour of retention, a sort key.
fn base_env() -> E2eEnvBuilder {
    E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).with_retention(Duration::from_secs(60 * 60)).with_optimize_sort_by()
}

/// `flushes` separately-committed files of 3 rows each in one partition.
/// `seal = Some(d)` advances the clock past the bucket before flushing (the
/// today/hot-tail shape); `seal = None` snaps the clock back to the frozen start
/// after each flush, producing a backdated sealed partition.
async fn seed_flushes(env: &E2eEnv, client: &Client, prefix: &str, flushes: i64, ts: impl Fn(i64, i64) -> i64, seal: Option<Duration>) -> anyhow::Result<()> {
    for b in 0..flushes {
        for i in 0..3i64 {
            insert_at(client, &format!("{prefix}-{b}-{i}"), ts(b, i)).await?;
        }
        if let Some(d) = seal {
            env.advance(d);
        }
        env.force_flush().await?;
        if seal.is_none() {
            support::set_micros(FROZEN_START_MICROS);
        }
    }
    Ok(())
}

/// `n` light-optimize ticks of `pass` against the fixture table.
async fn light_ticks(env: &E2eEnv, pass: TailPass, n: usize) -> anyhow::Result<()> {
    let table_ref = env.db().resolve_table(PROJECT, TABLE).await?;
    for _ in 0..n {
        env.db().optimize_table_light(&table_ref, TABLE, pass).await?;
    }
    Ok(())
}

async fn live_files(env: &E2eEnv) -> anyhow::Result<Vec<String>> {
    let table_ref = env.db().resolve_table(PROJECT, TABLE).await?;
    let t = table_ref.read().await;
    Ok(t.snapshot()?.log_data().iter().map(|f| f.path().to_string()).collect())
}

fn repair_ticks_yielded() -> u64 {
    timefusion::observability::maintenance_stats().repair_ticks_yielded.load(std::sync::atomic::Ordering::Relaxed)
}

async fn row_count(client: &Client) -> anyhow::Result<i64> {
    Ok(client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&PROJECT]).await?.get(0))
}

/// After hot-tail compaction, `ORDER BY timestamp DESC LIMIT n` must still plan
/// as a streaming merge — only possible if every file, including the freshly
/// compacted one, declares its ordering.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn hot_tail_output_declares_its_sorted_footer_even_when_the_bin_exceeds_the_sort_budget() -> anyhow::Result<()> {
    let env = base_env()
        // Every bin is over the in-process sort budget.
        .with_sort_skip_bytes(0)
        .start()
        .await?;
    // Keep the background coordinator from racing the fixture files this test
    // compacts by hand.
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;

    // Hot tail only considers TODAY's partition and only files whose EVENT time
    // is sealed (15 min behind the clock).
    let base = FROZEN_START_MICROS - 1800 * SEC;
    // 6 flushes: `timefusion_compact_min_files` is 5, fewer selects no bin at all.
    seed_flushes(&env, &client, "h", 6, |b, i| base + (b * 3 + i) * 20 * SEC, Some(Duration::from_secs(120))).await?;

    light_ticks(&env, TailPass::Pack, 1).await?;

    // Fresh rows so the scan spans MemBuffer ∪ the compacted Delta partition.
    // The frozen start makes every written row sealed.
    support::set_micros(FROZEN_START_MICROS);
    let new_base = FROZEN_START_MICROS - 60 * SEC;
    for i in 0..3i64 {
        insert_at(&client, &format!("m-{i}"), new_base + i * SEC).await?;
    }

    // Delta now contributes exactly the compacted file; if it declares its
    // ordering the top-N stays a streaming merge, otherwise a blocking SortExec.
    let sql = "SELECT id, timestamp FROM otel_logs_and_spans WHERE project_id = 'e2e_project' ORDER BY timestamp DESC LIMIT 3";
    let plan: String = client
        .query(&format!("EXPLAIN {sql}"), &[])
        .await?
        .iter()
        .map(|r| (0..r.len()).map(|c| r.try_get::<_, String>(c).unwrap_or_default()).collect::<Vec<_>>().join(" | "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("SortPreservingMergeExec"),
        "hot-tail compaction must declare its output sorted even when the bin exceeds the in-process sort budget \
         (it sorts inside the DataFusion plan — pooled, spillable, streaming — instead of via `sort_batches_by_schema`). \
         A blocking SortExec here means the compacted file was written unsorted: the 2026-08-01 bug, where a 256 MB \
         FILE-byte bin was ~17x over a 256 MB in-memory budget and EVERY hot-tail output silently lost its footer. \
         Plan was:\n{plan}"
    );

    assert_eq!(row_count(&client).await?, 21, "18 compacted + 3 buffered rows must survive the sorted rewrite exactly once");

    let top: Vec<String> = client.query(sql, &[]).await?.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(top, vec!["m-2", "m-1", "m-0"], "newest-first ordering must still be correct after the rewrite");

    Ok(())
}

/// The REPAIR half: a file that is already "converged" (>= 7/8 of target) but
/// carries no sorted footer must be rewritten anyway — packing skips it, so
/// repair is the only path that can restore the partition's footer ordering.
/// The target size is shrunk so a test-sized file lands in that state.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn hot_tail_repairs_a_converged_file_that_has_no_sorted_footer() -> anyhow::Result<()> {
    let env = base_env()
        // Every flush output is unsorted...
        .with_sort_skip_bytes(0)
        // ...and counts as converged, so only repair can ever rewrite it.
        .with_light_optimize_target(1024)
        .start()
        .await?;
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;

    let base = FROZEN_START_MICROS - 1800 * SEC;
    seed_flushes(&env, &client, "r", 6, |b, i| base + (b * 3 + i) * 20 * SEC, Some(Duration::from_secs(120))).await?;
    support::set_micros(FROZEN_START_MICROS);

    let before = live_files(&env).await?;
    assert!(!before.is_empty(), "the fixture must have produced files to repair");

    // Several ticks: repair takes ONE file per bin, and only once a project has
    // no packable slice left.
    light_ticks(&env, TailPass::Pack, 6).await?;

    let after = live_files(&env).await?;
    let rewritten = before.iter().filter(|p| !after.contains(p)).count();
    assert!(
        rewritten > 0,
        "a converged-but-unsorted file must be rewritten by the repair pass — otherwise nothing ever restores the \
         partition's footer ordering. before={before:?} after={after:?}"
    );

    // Repair must converge: a rewritten file is tagged sorted, so it is never
    // re-selected into an infinite 1->1 rewrite loop.
    light_ticks(&env, TailPass::Pack, 3).await?;
    let settled = live_files(&env).await?;
    let churn = after.iter().filter(|p| !settled.contains(p)).count();
    assert_eq!(churn, 0, "repair must be one-time: a rewritten file carries SORTED_RUN_TAG and is never re-selected");

    assert_eq!(row_count(&client).await?, 18, "the repair must not lose or duplicate rows");

    Ok(())
}

/// A repair pass must not end because ONE of a project's candidates turned out
/// to be fine. Admission offers every un-verified sealed file as a suspect (the
/// `delta-rs.optimize.sort_by` tag lies, so only the footer decides), so the next
/// candidate is usually already sorted; clearing it must RE-SELECT rather than
/// drop the project from the pass.
///
/// SCOPE: pins the walk — one pass clears EVERY sorted suspect in the partition.
/// It does not isolate the per-wave re-plan call site, which would need a
/// hand-written footer-less parquet.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn one_repair_pass_clears_every_sorted_suspect_not_one_per_pass() -> anyhow::Result<()> {
    // Yesterday: repair only scans SEALED dates (today belongs to packing).
    let yesterday = FROZEN_START_MICROS - 24 * 3600 * SEC;
    let env = base_env()
        // Converged, so nothing but repair would ever look at these files.
        .with_light_optimize_target(1024)
        // REQUIRED: the flush path also writes `repair_verified_sorted.txt`, so
        // with marking on the assertion below is satisfied by the flush and the
        // repair pass could clear nothing and still pass.
        .without_write_time_sort_marking()
        .start()
        .await?;
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;

    // Four separate flushes -> four untagged suspects in one sealed partition.
    const SUSPECTS: usize = 4;
    seed_flushes(&env, &client, "s", SUSPECTS as i64, |b, i| yesterday + (b * 300 + i * 20) * SEC, None).await?;

    let before = live_files(&env).await?.len();
    assert!(before >= SUSPECTS, "fixture must produce at least {SUSPECTS} suspects, got {before}");

    light_ticks(&env, TailPass::Repair, 1).await?;

    // The persisted verified-sorted set is what admission consults.
    let verified = std::fs::read_to_string(env.data_dir.join("repair_verified_sorted.txt")).unwrap_or_default();
    let cleared = verified.lines().filter(|l| !l.trim().is_empty()).count();
    assert!(
        cleared >= before,
        "one pass must walk PAST each cleared suspect to the next: cleared {cleared} of {before}. \
         Stopping at the first is how a 663-file backlog moved ~1 file per pass. file={verified:?}"
    );

    assert_eq!(row_count(&client).await?, (SUSPECTS as i64) * 3, "verification must not touch data");

    Ok(())
}

/// Two tables must not repair at the same time: `round_robin_bins` serialises
/// repair only WITHIN a table, but the light pool is shared, so two concurrent
/// repair sorts can exhaust it.
///
/// The loser must SKIP its tick, not queue — a repair pass owns a 144-minute
/// budget, so blocking would stall the other table for hours.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_second_table_skips_its_repair_tick_rather_than_sharing_the_light_pool() -> anyhow::Result<()> {
    let yesterday = FROZEN_START_MICROS - 24 * 3600 * SEC;
    let env = base_env()
        .with_light_optimize_target(1024)
        // Without this the fixture has NO repair work: write-time marking records
        // flushed files as verified-sorted on commit, both passes find zero
        // suspects, and since `tokio::join!` polls sequentially a pass that does
        // no IO never overlaps the other — the assertion would read 0.
        .without_write_time_sort_marking()
        .start()
        .await?;
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;
    seed_flushes(&env, &client, "x", 4, |b, i| yesterday + (b * 300 + i * 20) * SEC, None).await?;

    let table_ref = env.db().resolve_table(PROJECT, TABLE).await?;
    let before = repair_ticks_yielded();

    // Same table twice reproduces the cross-table contention: one process-wide
    // permit, two concurrent passes.
    let (a, b) =
        tokio::join!(env.db().optimize_table_light(&table_ref, TABLE, TailPass::Repair), env.db().optimize_table_light(&table_ref, TABLE, TailPass::Repair),);
    a?;
    b?;
    let yielded = repair_ticks_yielded() - before;
    assert_eq!(yielded, 1, "exactly one of two overlapping repair passes must yield the permit, got {yielded}");

    // And the permit must be RELEASED: a later pass still runs.
    env.db().optimize_table_light(&table_ref, TABLE, TailPass::Repair).await?;
    let after = repair_ticks_yielded() - before;
    assert_eq!(after, 1, "the permit leaked — a pass that ran alone still yielded");

    Ok(())
}
