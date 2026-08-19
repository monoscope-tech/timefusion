//! Hot-tail compaction must leave the partition with an HONEST sorted footer.
//!
//! Prod 2026-08-01: it did not, and nothing caught it. `stage_hot_bin` packed a
//! bin to `light_optimize_target_size` (256 MB of FILE bytes) and then handed it
//! to an in-process Arrow sort budgeted at 256 MB of IN-MEMORY bytes. zstd on
//! otel data is ~17x, so every bin arrived ~17x over budget, the sort was
//! silently skipped, and the output was written with `declare_sorted=false`:
//! 0 of the 8 largest files in a live partition declared `sorting_columns`.
//!
//! One such file is enough. The reader's `derive_common_ordering` is
//! all-or-nothing, so the whole scan lost its declared ordering — costing the
//! streaming top-N pushdown AND forcing `DedupExec` into its unbounded
//! `full-set` seen-set, which is the per-query memory behind the OOM/restart
//! cycle that made cold reads 26-68s.
//!
//! The guard has to exercise the SIZE condition, not just the happy path: a bin
//! small enough to fit the old budget was sorted correctly even before the fix,
//! so a test that only compacts a few rows proves nothing. `with_sort_skip_bytes`
//! shrinks the in-process budget to zero, which is what a 17x-oversized bin
//! looked like in production.

use std::time::Duration;

use timefusion::support;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

/// After hot-tail compaction, a recent-window `ORDER BY timestamp DESC LIMIT n`
/// must still plan as a streaming merge — which it can only do if every file in
/// the partition, including the freshly compacted one, declares its ordering.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn hot_tail_output_declares_its_sorted_footer_even_when_the_bin_exceeds_the_sort_budget() -> anyhow::Result<()> {
    let bucket_secs = 60u64;
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(bucket_secs))
        .with_retention(Duration::from_secs(60 * 60))
        .with_optimize_sort_by()
        // The production shape: every bin is over the in-process sort budget.
        .with_sort_skip_bytes(0)
        .start()
        .await?;
    // This test invokes light optimize directly and asserts that call's
    // rewrite. Keep the background coordinator from consuming or racing the
    // same fixture files.
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;

    // The hot tail only considers TODAY's partition and only files whose EVENT
    // time is sealed (15 min behind the clock), so the rows go 30 min before the
    // frozen "now" and the clock stays inside the same UTC day.
    let sec = 1_000_000i64;
    let base = FROZEN_START_MICROS - 1800 * sec;
    // 6 flushes: `timefusion_compact_min_files` is 5, so a smaller run selects no
    // bin at all and the pass is a silent no-op.
    for b in 0..6i64 {
        for i in 0..3i64 {
            let idx = b * 3 + i;
            insert_at(&client, &format!("h-{idx}"), base + idx * 20 * sec).await?;
        }
        env.advance(Duration::from_secs(bucket_secs * 2));
        env.force_flush().await?;
    }

    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    env.db().optimize_table_light(&table_ref, "otel_logs_and_spans", timefusion::database::TailPass::Pack).await?;

    // Fresh rows so the scan spans MemBuffer ∪ the compacted Delta partition —
    // the shape a dashboard query actually takes.
    // Advance to the frozen start so every written row is now sealed.
    support::set_micros(FROZEN_START_MICROS);
    let new_base = FROZEN_START_MICROS - 60 * sec;
    for i in 0..3i64 {
        insert_at(&client, &format!("m-{i}"), new_base + i * sec).await?;
    }

    // The hot tail collapsed every Delta file into one, and the fresh rows are
    // still in MemBuffer — so Delta contributes exactly the compacted file. If
    // that file declares its ordering, the scan advertises one and the top-N
    // stays a streaming merge; if compaction wrote it unsorted, the same query
    // degrades to a blocking SortExec. That is the whole bug, end to end.
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

    // ...and the rewrite must not have lost or duplicated anything.
    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 21, "18 compacted + 3 buffered rows must survive the sorted rewrite exactly once");

    let top: Vec<String> = client.query(sql, &[]).await?.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(top, vec!["m-2", "m-1", "m-0"], "newest-first ordering must still be correct after the rewrite");

    Ok(())
}

/// The REPAIR half: a file that is already "converged" (>= 7/8 of target) but
/// carries no sorted footer must be rewritten anyway.
///
/// Prod 2026-08-01 had 265-778MB files in exactly that state. Hot-tail skipped
/// them as converged, and the daily consolidate/recompress crons that would
/// have fixed them had not run in 24h — a job firing at 02:30 rarely survives a
/// process restarting every 30-120 minutes. So nothing repaired them, and one
/// of them is enough to disable the reader's all-or-nothing footer ordering for
/// every scan touching the partition.
///
/// The target size is shrunk so a test-sized file lands in the same state.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn hot_tail_repairs_a_converged_file_that_has_no_sorted_footer() -> anyhow::Result<()> {
    let bucket_secs = 60u64;
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(bucket_secs))
        .with_retention(Duration::from_secs(60 * 60))
        .with_optimize_sort_by()
        // Every flush output is unsorted (the large-coalesced-bucket shape)...
        .with_sort_skip_bytes(0)
        // ...and counts as converged, so the ONLY way it is ever rewritten is
        // the repair path.
        .with_light_optimize_target(1024)
        .start()
        .await?;
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;

    let sec = 1_000_000i64;
    let base = FROZEN_START_MICROS - 1800 * sec;
    for b in 0..6i64 {
        for i in 0..3i64 {
            let idx = b * 3 + i;
            insert_at(&client, &format!("r-{idx}"), base + idx * 20 * sec).await?;
        }
        env.advance(Duration::from_secs(bucket_secs * 2));
        env.force_flush().await?;
    }
    support::set_micros(FROZEN_START_MICROS);

    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let before: Vec<String> = {
        let t = table_ref.read().await;
        t.snapshot()?.log_data().iter().map(|f| f.path().to_string()).collect()
    };
    assert!(!before.is_empty(), "the fixture must have produced files to repair");

    // Several ticks: repair takes ONE file per bin and only once a project has
    // no packable slice left, so a backlog drains gradually by design.
    for _ in 0..6 {
        env.db().optimize_table_light(&table_ref, "otel_logs_and_spans", timefusion::database::TailPass::Pack).await?;
    }

    let after: Vec<String> = {
        let t = table_ref.read().await;
        t.snapshot()?.log_data().iter().map(|f| f.path().to_string()).collect()
    };
    let rewritten = before.iter().filter(|p| !after.contains(p)).count();
    assert!(
        rewritten > 0,
        "a converged-but-unsorted file must be rewritten by the repair pass — otherwise nothing ever restores the \
         partition's footer ordering. before={before:?} after={after:?}"
    );

    // The repair must converge: once rewritten the output is a tagged sorted
    // run, so further ticks must stop touching it. Without that this is an
    // infinite 1->1 rewrite loop.
    let settled: Vec<String> = {
        for _ in 0..3 {
            env.db().optimize_table_light(&table_ref, "otel_logs_and_spans", timefusion::database::TailPass::Pack).await?;
        }
        let t = table_ref.read().await;
        t.snapshot()?.log_data().iter().map(|f| f.path().to_string()).collect()
    };
    let churn = after.iter().filter(|p| !settled.contains(p)).count();
    assert_eq!(churn, 0, "repair must be one-time: a rewritten file carries SORTED_RUN_TAG and is never re-selected");

    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, 18, "the repair must not lose or duplicate rows");

    Ok(())
}

/// A repair pass must not end because ONE of a project's candidates turned out
/// to be fine.
///
/// Admission offers every un-verified sealed file as a suspect — the
/// `delta-rs.optimize.sort_by` tag lies, so only the footer decides — which
/// means a project's next candidate is usually a correctly-sorted file rather
/// than poison. Clearing it leaves that project with no bin, and without a
/// RE-SELECT it drops out of the wave engine's `pending` set for the whole
/// PASS rather than the wave.
///
/// Prod 2026-08-10, project 87576849 (663 footer-less files interleaved with
/// ~450 sorted ones): every repair pass ended in ~14s having repaired ~1 file,
/// with `planned=6 completed=6 brakes=0` and its 8640s budget untouched — the
/// pass was ending on an EMPTY PLAN, not on waves or time. Fixed in d50cedc by
/// sharing `reselect_until_real_work` between both call sites.
///
/// SCOPE: this pins the walk — one pass clears EVERY sorted suspect in the
/// partition, not one per pass. It does not isolate the per-wave re-plan call
/// site specifically, because manufacturing a genuinely footer-less file needs
/// a hand-written parquet (the flush path escalates to a pooled sort rather
/// than skipping, so `with_sort_skip_bytes(0)` no longer produces one).
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn one_repair_pass_clears_every_sorted_suspect_not_one_per_pass() -> anyhow::Result<()> {
    let sec = 1_000_000i64;
    // Yesterday: repair only scans SEALED dates (today belongs to packing).
    let yesterday = FROZEN_START_MICROS - 24 * 3600 * sec;
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(60))
        .with_retention(Duration::from_secs(60 * 60))
        .with_optimize_sort_by()
        // Converged, so nothing but repair would ever look at these files.
        .with_light_optimize_target(1024)
        .start()
        .await?;
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;

    // Four separate flushes -> four untagged suspects in one sealed partition.
    const SUSPECTS: usize = 4;
    for b in 0..SUSPECTS as i64 {
        for i in 0..3i64 {
            insert_at(&client, &format!("s-{b}-{i}"), yesterday + (b * 300 + i * 20) * sec).await?;
        }
        env.force_flush().await?;
        support::set_micros(FROZEN_START_MICROS);
    }

    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let before = {
        let t = table_ref.read().await;
        t.snapshot()?.log_data().iter().count()
    };
    assert!(before >= SUSPECTS, "fixture must produce at least {SUSPECTS} suspects, got {before}");

    env.db().optimize_table_light(&table_ref, "otel_logs_and_spans", timefusion::database::TailPass::Repair).await?;

    // The verified-sorted set is what admission consults, and it is persisted —
    // so it is also the thing a pass that gave up early leaves half-written.
    let verified = std::fs::read_to_string(env.data_dir.join("repair_verified_sorted.txt")).unwrap_or_default();
    let cleared = verified.lines().filter(|l| !l.trim().is_empty()).count();
    assert!(
        cleared >= before,
        "one pass must walk PAST each cleared suspect to the next: cleared {cleared} of {before}. \
         Stopping at the first is how a 663-file backlog moved ~1 file per pass. file={verified:?}"
    );

    let count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"e2e_project"]).await?.get(0);
    assert_eq!(count, (SUSPECTS as i64) * 3, "verification must not touch data");

    Ok(())
}

/// Two tables must not repair at the same time.
///
/// `round_robin_bins` serialises repair WITHIN a table (concurrency
/// `(k/2).max(1)` = 1), and `REPAIR_SORT_PARTITIONS` is justified by "repair
/// runs exactly ONE bin at a time" — but the light pool is shared by every
/// table, so that was only ever true per table. Prod 2026-08-11 11:30:
/// `otel_metrics` and `otel_logs_and_spans` repaired concurrently and the pool
/// held two sorts (`ExternalSorter#36474` at 13.6 GB plus a second merge at
/// 1245 MB), killing a 981 MB bin with 2.1 MB left of a 15.4 GB pool.
///
/// The loser must SKIP its tick, not queue: a repair pass owns a 144-minute
/// budget, so blocking would stall the other table for hours.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_second_table_skips_its_repair_tick_rather_than_sharing_the_light_pool() -> anyhow::Result<()> {
    let sec = 1_000_000i64;
    let yesterday = FROZEN_START_MICROS - 24 * 3600 * sec;
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(60))
        .with_retention(Duration::from_secs(60 * 60))
        .with_optimize_sort_by()
        .with_light_optimize_target(1024)
        .start()
        .await?;
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;
    for b in 0..4i64 {
        for i in 0..3i64 {
            insert_at(&client, &format!("x-{b}-{i}"), yesterday + (b * 300 + i * 20) * sec).await?;
        }
        env.force_flush().await?;
        support::set_micros(FROZEN_START_MICROS);
    }

    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let before = timefusion::observability::maintenance_stats().repair_ticks_yielded.load(std::sync::atomic::Ordering::Relaxed);

    // Same table twice is the same contention the two tables have: one
    // process-wide permit, two concurrent passes.
    let (a, b) = tokio::join!(
        env.db().optimize_table_light(&table_ref, "otel_logs_and_spans", timefusion::database::TailPass::Repair),
        env.db().optimize_table_light(&table_ref, "otel_logs_and_spans", timefusion::database::TailPass::Repair),
    );
    a?;
    b?;
    let yielded = timefusion::observability::maintenance_stats().repair_ticks_yielded.load(std::sync::atomic::Ordering::Relaxed) - before;
    assert_eq!(yielded, 1, "exactly one of two overlapping repair passes must yield the permit, got {yielded}");

    // And the permit must be RELEASED: a later pass still runs.
    env.db().optimize_table_light(&table_ref, "otel_logs_and_spans", timefusion::database::TailPass::Repair).await?;
    let after = timefusion::observability::maintenance_stats().repair_ticks_yielded.load(std::sync::atomic::Ordering::Relaxed) - before;
    assert_eq!(after, 1, "the permit leaked — a pass that ran alone still yielded");

    Ok(())
}
