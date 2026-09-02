//! Resumable footer repair: a repair rewrite that was already STAGED must
//! survive the restart that killed its process.
//!
//! Prod 2026-08-08: a footer-repair bin is one 700MB-1GiB whole-file rewrite
//! taking 40+ minutes. An 08:02 deploy and an unexplained 06:45 same-image task
//! replacement each threw away ~2GB of already-written parquet, and the next
//! pass started the identical file from scratch — so the backlog never moved.
//! With resume, loss is bounded to "the bytes not yet written".
//!
//! The resume is hooked at bin SELECTION: before staging, the pass looks for an
//! intent whose input set is exactly this bin's. That is what makes the restart
//! case free — selection is deterministic given the snapshot, so the first pass
//! after a restart re-selects the killed pass's files. The two invariants these
//! tests pin are the ones that make committing those bytes safe: every input
//! still live (staleness) and output rows == input rows (a repair is
//! data-preserving, so a truncated staging is detectable). Declining is always
//! safe — staged parquet is invisible to readers until the atomic commit — so
//! the failure to guard against is a WRONG commit, not a missed one.

use std::time::Duration;

use timefusion::{database::TailPass, support};

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

const TABLE: &str = "otel_logs_and_spans";
const PROJECT: &str = "e2e_project";
/// Past `STAGED_INTENT_MIN_AGE_SECS` (30 min), the rolling-deploy gate that
/// stops one instance from adopting another's half-written bin.
const BACKDATE_SECS: u64 = 60 * 60;

/// Six flushes of unsorted, already-"converged" files — the prod shape the
/// repair path exists for. Returns the resulting Delta file list.
async fn footerless_partition(env: &E2eEnv) -> anyhow::Result<Vec<String>> {
    let client = env.pg_client().await?;
    let sec = 1_000_000i64;
    let base = FROZEN_START_MICROS - 1800 * sec;
    for b in 0..6i64 {
        for i in 0..3i64 {
            let idx = b * 3 + i;
            insert_at(&client, &format!("rr-{idx}"), base + idx * 20 * sec).await?;
        }
        env.advance(Duration::from_secs(120));
        env.force_flush().await?;
    }
    support::set_micros(FROZEN_START_MICROS);
    let files = live_files(env).await?;
    assert!(files.len() > 1, "fixture must produce several files to rewrite, got {files:?}");
    Ok(files)
}

async fn live_files(env: &E2eEnv) -> anyhow::Result<Vec<String>> {
    let table_ref = env.db().resolve_table(PROJECT, TABLE).await?;
    let t = table_ref.read().await;
    Ok(t.snapshot()?.log_data().iter().map(|f| f.path().to_string()).collect())
}

/// Stage the bin the next real pass would select, then abandon it — the state
/// a killed process leaves behind. Going through the planner is the point:
/// resume matches on input-set equality.
async fn abandon_one_bin(env: &E2eEnv) -> anyhow::Result<Vec<String>> {
    let table_ref = env.db().resolve_table(PROJECT, TABLE).await?;
    let bin = env.db().stage_and_abandon_first_bin(&table_ref, TABLE, TailPass::Pack).await?;
    let (project_id, files) = bin.expect("the pass must have a bin to stage — otherwise the fixture isn't compactable");
    assert_eq!(project_id, PROJECT);
    Ok(files)
}

fn manifest_path(env: &E2eEnv) -> std::path::PathBuf {
    env.data_dir.join("staged_intent.jsonl")
}

/// Age every manifest entry past the rolling-deploy gate. Without this the
/// entries are "young" — indistinguishable from a live instance's in-flight
/// staging — and resume correctly refuses to touch them.
fn backdate_manifest(env: &E2eEnv) -> anyhow::Result<Vec<serde_json::Value>> {
    let path = manifest_path(env);
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();
    let entries: Vec<serde_json::Value> = std::fs::read_to_string(&path)?
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let mut v: serde_json::Value = serde_json::from_str(l)?;
            v["recorded_at"] = serde_json::json!(now - BACKDATE_SECS);
            anyhow::Ok(v)
        })
        .collect::<anyhow::Result<_>>()?;
    std::fs::write(&path, entries.iter().map(serde_json::Value::to_string).collect::<Vec<_>>().join("\n") + "\n")?;
    Ok(entries)
}

fn staged_outputs(entries: &[serde_json::Value]) -> Vec<String> {
    entries.iter().flat_map(|e| e["paths"].as_array().cloned().unwrap_or_default()).filter_map(|p| p.as_str().map(str::to_string)).collect()
}

fn live_manifest_entries(env: &E2eEnv) -> usize {
    std::fs::read_to_string(manifest_path(env)).map_or(0, |c| c.lines().filter(|l| !l.trim().is_empty()).count())
}

/// THE test: stage a repair, kill the process, and prove the next pass commits
/// the staged bytes instead of redoing the rewrite.
///
/// "Instead of" is the load-bearing word, and the assertion for it is that the
/// file now live is the one staged BEFORE the restart. A pass that quietly
/// re-staged would also end with the right row count — and would have burned
/// the 40 minutes this exists to save.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_repair_pass_commits_the_bin_a_killed_process_had_already_staged() -> anyhow::Result<()> {
    let mut env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(60))
        .with_retention(Duration::from_secs(60 * 60))
        .with_optimize_sort_by()
        // Every flush output lands unsorted and already "converged", so the
        // repair path is the only thing that would ever rewrite it.
        .with_sort_skip_bytes(0)
        .with_light_optimize_target(1024)
        .with_repair_resume()
        .start()
        .await?;
    env.db().cancel_maintenance();

    let before = footerless_partition(&env).await?;
    let rows_before: i64 = {
        let client = env.pg_client().await?;
        client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&PROJECT]).await?.get(0)
    };

    // The rewrite that gets killed: staged parquet on S3, an intent line on
    // disk, no commit.
    let inputs = abandon_one_bin(&env).await?;
    let entries = backdate_manifest(&env)?;
    assert_eq!(entries.len(), 1, "the abandoned bin must have left an intent line");
    let entry = &entries[0];
    let mut recorded_inputs: Vec<String> =
        entry["target_paths"].as_array().cloned().unwrap_or_default().iter().filter_map(|p| p.as_str().map(str::to_string)).collect();
    recorded_inputs.sort();
    let mut expected = inputs.clone();
    expected.sort();
    assert_eq!(recorded_inputs, expected, "a repair intent must record its INPUTS, or resume can't tell a valid staging from a stale one");
    assert!(!entry["adds"].as_array().is_none_or(|a| a.is_empty()), "a repair intent must record its Add actions, or resume has to re-read footers");
    let staged = staged_outputs(&entries);

    // The restart the deploy/healthcheck used to make fatal.
    env.restart().await?;
    env.db().cancel_maintenance();

    let table_ref = env.db().resolve_table(PROJECT, TABLE).await?;
    env.db().optimize_table_light(&table_ref, TABLE, TailPass::Pack).await?;

    // The pre-restart output is LIVE and its inputs are tombstoned. Staged
    // parquet is uuid-named by the writer, so nobody else could have produced
    // that path: a pass that re-staged would show a fresh uuid and leave this
    // one orphaned. That is the whole claim — the 40 minutes were not respent.
    let after = live_files(&env).await?;
    for out in &staged {
        assert!(after.contains(out), "the pre-restart staging must have been COMMITTED, not re-staged: {out} missing from {after:?}");
    }
    for input in &inputs {
        assert!(!after.contains(input), "the resumed commit must tombstone its inputs — {input} is still live");
    }
    assert_eq!(live_manifest_entries(&env), 0, "commit_wave clears the intent it landed");

    // Row preservation, end to end. This is the whole point: the rewrite was
    // done before the crash, and committing it must change nothing logically.
    let client = env.pg_client().await?;
    let rows_after: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&PROJECT]).await?.get(0);
    assert_eq!(rows_after, rows_before, "a resumed repair is data-preserving — it must not lose or duplicate a row");

    // A resumed file must be an ordinary compaction output, not a special case:
    // let the rest of the partition repair normally on top of it.
    // (`hot_tail_sorted_footer.rs` owns the "footers end up honest" assertion.)
    for _ in 0..before.len() {
        env.db().optimize_table_light(&table_ref, TABLE, TailPass::Pack).await?;
    }
    let rows_end: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&PROJECT]).await?.get(0);
    assert_eq!(rows_end, rows_before, "the follow-up repairs must not lose or duplicate a row either");
    Ok(())
}

/// A staged output whose inputs were rewritten underneath it is garbage:
/// committing it would resurrect removed rows and drop new ones. Two abandoned
/// bins over the SAME inputs make that concrete — once the first is committed,
/// the second's inputs are gone. It must be declined, and its parquet reclaimed
/// by the boot reconcile rather than leaking on S3 forever.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_staged_bin_whose_inputs_were_rewritten_is_declined_and_reclaimed() -> anyhow::Result<()> {
    let mut env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(60))
        .with_retention(Duration::from_secs(60 * 60))
        .with_optimize_sort_by()
        .with_sort_skip_bytes(0)
        .with_light_optimize_target(1024)
        .with_repair_resume()
        .start()
        .await?;
    env.db().cancel_maintenance();

    let before = footerless_partition(&env).await?;
    let first = abandon_one_bin(&env).await?;
    let second = abandon_one_bin(&env).await?;
    assert_eq!(first, second, "the planner must re-select the same bin, or these are not same-input twins");
    let staged = staged_outputs(&backdate_manifest(&env)?);
    env.restart().await?;
    env.db().cancel_maintenance();

    let table_ref = env.db().resolve_table(PROJECT, TABLE).await?;
    env.db().optimize_table_light(&table_ref, TABLE, TailPass::Pack).await?;
    let after = live_files(&env).await?;
    assert_eq!(after.len(), before.len() - first.len() + 1, "exactly one of two same-input bins may commit: {after:?}");

    let orphans: Vec<&String> = staged.iter().filter(|p| !after.contains(p)).collect();
    assert!(!orphans.is_empty(), "the twin's output should still be sitting there before reconcile");
    env.db().reconcile_staged_intents(&table_ref, TABLE).await;
    let store = { table_ref.read().await.log_store().object_store(None) };
    for orphan in &orphans {
        let head = object_store::ObjectStoreExt::head(store.as_ref(), &object_store::path::Path::from(orphan.as_str())).await;
        assert!(matches!(head, Err(object_store::Error::NotFound { .. })), "reconcile must delete the declined bin's staged parquet: {orphan}");
    }
    assert_eq!(live_manifest_entries(&env), 0, "both intents are settled — the manifest must be empty");
    Ok(())
}

/// Off by default: the first deploy ships the widened manifest WITHOUT
/// committing from it, and flipping the flag back off must restore today's
/// behaviour exactly (the pass re-stages, reconcile deletes the orphan).
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn resume_is_a_no_op_while_the_kill_switch_is_off() -> anyhow::Result<()> {
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(60))
        .with_retention(Duration::from_secs(60 * 60))
        .with_optimize_sort_by()
        .with_sort_skip_bytes(0)
        .with_light_optimize_target(1024)
        .start()
        .await?;
    env.db().cancel_maintenance();

    footerless_partition(&env).await?;
    abandon_one_bin(&env).await?;
    let table_ref = env.db().resolve_table(PROJECT, TABLE).await?;
    let staged = staged_outputs(&backdate_manifest(&env)?);

    env.db().optimize_table_light(&table_ref, TABLE, TailPass::Pack).await?;
    let after = live_files(&env).await?;
    assert!(
        after.iter().all(|p| !staged.contains(p)),
        "with the kill switch off, nothing may be committed from the manifest: after={after:?} staged={staged:?}"
    );
    Ok(())
}
