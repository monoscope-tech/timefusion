//! Resumable footer repair: a repair rewrite that was already STAGED must
//! survive the restart that killed its process.
//!
//! Resume is hooked at bin SELECTION — before staging, the pass looks for an
//! intent whose input set is exactly this bin's. A commit is only safe when
//! every input is still live and output rows == input rows; declining is always
//! safe, since staged parquet is invisible to readers until the atomic commit.

use std::time::Duration;

use timefusion::{database::TailPass, support};

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};
use super::ordering_pushdown::{count_rows, hot_partition_builder};

const TABLE: &str = "otel_logs_and_spans";
const PROJECT: &str = "e2e_project";
/// Past `STAGED_INTENT_MIN_AGE_SECS` (30 min), the gate that stops one instance
/// from adopting another's half-written bin.
const BACKDATE_SECS: u64 = 60 * 60;

/// Six flushes of unsorted, already-"converged" files; returns the Delta file list.
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

/// Every flush output lands unsorted and already "converged", so the repair path
/// is the only thing that would rewrite it. `resume` flips the kill switch.
async fn repair_env(resume: bool) -> anyhow::Result<E2eEnv> {
    let b = hot_partition_builder().with_optimize_sort_by().with_sort_skip_bytes(0).with_light_optimize_target(1024);
    let env = if resume { b.with_repair_resume() } else { b }.start().await?;
    env.db().cancel_maintenance();
    Ok(env)
}

/// A fresh client per call: `env.restart()` rebinds the pgwire port, so a
/// connection held across a restart is dead.
async fn row_count(env: &E2eEnv) -> anyhow::Result<i64> {
    count_rows(&env.pg_client().await?, PROJECT).await
}

async fn live_files(env: &E2eEnv) -> anyhow::Result<Vec<String>> {
    let table_ref = env.db().resolve_table(PROJECT, TABLE).await?;
    let t = table_ref.read().await;
    Ok(t.snapshot()?.log_data().iter().map(|f| f.path().to_string()).collect())
}

/// Stage the bin the next real pass would select, then abandon it — the state a
/// killed process leaves behind. Must go through the planner: resume matches on
/// input-set equality.
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

/// Age every manifest entry past the rolling-deploy gate; young entries are
/// indistinguishable from another instance's in-flight staging and are refused.
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

/// Stage a repair, kill the process, and prove the next pass commits the staged
/// bytes instead of redoing the rewrite: the file now live must be the one staged
/// BEFORE the restart (a re-stage would also give the right row count).
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_repair_pass_commits_the_bin_a_killed_process_had_already_staged() -> anyhow::Result<()> {
    let mut env = repair_env(true).await?;

    let before = footerless_partition(&env).await?;
    let rows_before = row_count(&env).await?;

    // The rewrite that gets killed: staged parquet, an intent line, no commit.
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

    env.restart().await?;
    env.db().cancel_maintenance();

    let table_ref = env.db().resolve_table(PROJECT, TABLE).await?;
    env.db().optimize_table_light(&table_ref, TABLE, TailPass::Pack).await?;

    // Staged parquet is uuid-named, so a re-stage would show a fresh path and
    // leave this one orphaned — seeing it live proves the bytes were reused.
    let after = live_files(&env).await?;
    for out in &staged {
        assert!(after.contains(out), "the pre-restart staging must have been COMMITTED, not re-staged: {out} missing from {after:?}");
    }
    for input in &inputs {
        assert!(!after.contains(input), "the resumed commit must tombstone its inputs — {input} is still live");
    }
    assert_eq!(live_manifest_entries(&env), 0, "commit_wave clears the intent it landed");

    assert_eq!(row_count(&env).await?, rows_before, "a resumed repair is data-preserving — it must not lose or duplicate a row");

    // A resumed file must be an ordinary compaction output: the rest of the
    // partition has to repair normally on top of it.
    for _ in 0..before.len() {
        env.db().optimize_table_light(&table_ref, TABLE, TailPass::Pack).await?;
    }
    assert_eq!(row_count(&env).await?, rows_before, "the follow-up repairs must not lose or duplicate a row either");
    Ok(())
}

/// A staged output whose inputs were rewritten underneath it must be declined
/// (committing it would resurrect removed rows) and its parquet reclaimed by
/// reconcile. Two abandoned bins over the SAME inputs make that concrete.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_staged_bin_whose_inputs_were_rewritten_is_declined_and_reclaimed() -> anyhow::Result<()> {
    let mut env = repair_env(true).await?;

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

/// With the kill switch off, nothing may ever be committed from the manifest.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn resume_is_a_no_op_while_the_kill_switch_is_off() -> anyhow::Result<()> {
    let env = repair_env(false).await?;

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
