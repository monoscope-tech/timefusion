//! Key-level dedup restriction (`timefusion_read_dedup_key_restrict`): a proved file
//! blocked by overlapping late files sends through `DedupExec` only the rows whose key a
//! late file holds. Everything here goes through pgwire, against `mor_versioned`.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use test_case::test_case;
use tokio_postgres::Client;

use super::harness::{E2eEnv, E2eEnvBuilder};
use super::ordering_pushdown::flat_rows;
use super::per_file_split::{DAY0, certify_table, lit, stat, ts};

const SEC: i64 = 1_000_000;
const DAY: i64 = 86_400 * SEC;

/// The expected visible state: `(timestamp, id) -> name`, tombstoned keys removed.
type Model = BTreeMap<(i64, String), String>;

async fn insert(client: &Client, project: &str, id: &str, name: &str, at: i64) -> anyhow::Result<()> {
    client
        .execute(
            "INSERT INTO mor_versioned (project_id, timestamp, id, date, name) VALUES ($1, $2, $3, $4, $5)",
            &[&project, &ts(at), &id, &ts(at).date_naive(), &name],
        )
        .await?;
    Ok(())
}

/// Commit, straight to Delta and unstamped, an OLDER version of `key` — the shape a WAL
/// replay re-lands after the newer version was flushed. It must lose to the proved row,
/// so this is what makes the member half of the restriction load-bearing.
async fn land_stale_version(env: &E2eEnv, project: &str, (at, id): &(i64, String)) -> anyhow::Result<()> {
    let row = serde_json::json!({
        "timestamp": at, "id": id, "name": "stale", "project_id": project,
        "date": ts(*at).date_naive().to_string(), "updated_at": 1, "hashes": [],
    });
    let batch = timefusion::support::test_helpers::json_to_batch_for("mor_versioned", vec![row])?;
    let handle = env.db().resolve_table(project, "mor_versioned").await?;
    let mut table = handle.write().await;
    *table = table.clone().write(vec![batch]).await?;
    Ok(())
}

/// Every visible row, as `(timestamp, id) -> name`. `window` adds the day's time bounds,
/// which is what lets the scan take the per-file split at all; without it the scan has
/// no window and runs the full `DedupExec`, making it the reference answer.
async fn visible(client: &Client, project: &str, day: i64, window: bool) -> anyhow::Result<Model> {
    let bounds = if window { format!(" AND timestamp >= {} AND timestamp < {}", lit(day), lit(day + DAY)) } else { String::new() };
    let sql = format!("SELECT timestamp, id, name FROM mor_versioned WHERE project_id = '{project}'{bounds}");
    let rows = client.query(&sql, &[]).await?;
    let mut out = Model::new();
    for r in rows {
        let key = (r.get::<_, DateTime<Utc>>(0).timestamp_micros(), r.get::<_, String>(1));
        assert!(out.insert(key.clone(), r.get(2)).is_none(), "key {key:?} returned twice — a version escaped dedup");
    }
    Ok(out)
}

async fn count(client: &Client, project: &str, day: i64) -> anyhow::Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM mor_versioned WHERE project_id = '{project}' AND timestamp >= {} AND timestamp < {}", lit(day), lit(day + DAY));
    Ok(client.query_one(&sql, &[]).await?.get(0))
}

/// Run the day-wide Dedup unit until it certifies (a pass that masks duplicates proves
/// the next one clean), so the day's files are proved.
async fn certify(env: &E2eEnv, project: &str, day: i64) -> anyhow::Result<()> {
    certify_table(env, "mor_versioned", project, day).await
}

/// Live files of the project's partitions carrying a deletion vector.
async fn dv_files(env: &E2eEnv, project: &str) -> anyhow::Result<usize> {
    let table = env.db().resolve_table(project, "mor_versioned").await?;
    let table = table.read().await;
    let prefix = format!("project_id={project}/");
    Ok(table.snapshot()?.snapshot().log_data().iter().filter(|f| f.path().contains(&prefix) && f.deletion_vector_descriptor().is_some()).count())
}

/// Deterministic xorshift, so every case is reproducible from its seed.
struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n.max(1)
    }
}

/// One seeded MoR history per case:
/// - base rows over ~3 minutes (several proved files), some sharing a timestamp;
/// - optional pre-proof churn, so the proof holds DV-masked files and tombstones;
/// - 1-3 late rounds of UPDATEs (a key may recur across rounds), DELETEs, late inserts
///   of new keys inside the proved span, and stale re-landed versions that must lose;
/// - in some cases a final round left in the MemBuffer.
///
/// The windowed read (restricted when the flag is on) must equal both the unwindowed
/// read (full `DedupExec`) and the model.
#[test_case(true ; "restricted")]
#[test_case(false ; "flag off")]
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn key_restricted_dedup_matches_full_dedup(restrict: bool) -> anyhow::Result<()> {
    const CASES: u64 = 8;
    timefusion::observability::init_local_metrics_for_test();
    let env = E2eEnvBuilder::default().with_dedup_key_restrict(restrict).with_tantivy_prefilter(false).without_light_optimize().start().await?;
    let client = env.pg_client().await?;
    let (mut restricted_cases, mut restricted_with_dvs) = (0, 0);
    for case in 0..CASES {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (case + 1).wrapping_mul(0x2545_F491_4F6C_DD1D));
        let (project, day) = (format!("kd{case}"), DAY0 + case as i64 * DAY);
        let base_at = |k: i64| day + 3_600 * SEC + (k / 2) * 7 * SEC;
        let mut model = Model::new();
        let base = 20 + rng.below(40) as i64;
        for k in 0..base {
            let id = format!("r{k}");
            insert(&client, &project, &id, "v0", base_at(k)).await?;
            model.insert((base_at(k), id), "v0".into());
        }
        env.force_flush().await?;
        let mut stamp = 0;
        let mut round = async |env: &E2eEnv, model: &mut Model, rng: &mut Rng, flush: bool| -> anyhow::Result<()> {
            for _ in 0..1 + rng.below(4) {
                // Half the picks come from a small hot set, so keys recur across late files.
                let pool = if rng.below(2) == 0 { model.len().min(4) } else { model.len() };
                let Some(key) = model.keys().nth(rng.below(pool as u64) as usize).cloned() else { break };
                stamp += 1;
                let name = format!("u{stamp}");
                client.execute(&format!("UPDATE mor_versioned SET name = '{name}' WHERE project_id = '{project}' AND id = '{}'", key.1), &[]).await?;
                model.insert(key, name);
            }
            if rng.below(2) == 0
                && let Some(key) = model.keys().nth(rng.below(model.len() as u64) as usize)
            {
                land_stale_version(env, &project, key).await?;
            }
            if rng.below(3) == 0
                && let Some(key) = model.keys().nth(rng.below(model.len() as u64) as usize).cloned()
            {
                client.execute(&format!("DELETE FROM mor_versioned WHERE project_id = '{project}' AND id = '{}'", key.1), &[]).await?;
                model.remove(&key);
            }
            for _ in 0..rng.below(3) {
                stamp += 1;
                let (id, at) = (format!("late{stamp}"), base_at(rng.below(base as u64) as i64));
                insert(&client, &project, &id, "late", at).await?;
                model.insert((at, id), "late".into());
            }
            if flush {
                env.force_flush().await?;
            }
            Ok(())
        };
        if rng.below(2) == 0 {
            round(&env, &mut model, &mut rng, true).await?;
        }
        certify(&env, &project, day).await?;
        for _ in 0..1 + rng.below(3) {
            round(&env, &mut model, &mut rng, true).await?;
        }
        let with_mem = rng.below(3) == 0;
        if with_mem {
            round(&env, &mut model, &mut rng, false).await?;
        }

        let scans = stat(&client, "dedup_key_restrict_scans").await?;
        let windowed = visible(&client, &project, day, true).await?;
        let took = stat(&client, "dedup_key_restrict_scans").await? > scans;
        let dvs = dv_files(&env, &project).await?;
        restricted_cases += usize::from(took);
        restricted_with_dvs += usize::from(took && dvs > 0);
        eprintln!("case {case}: base={base} keys={} mem={with_mem} dv_files={dvs} restricted={took}", model.len());
        let reference = visible(&client, &project, day, false).await?;
        assert_eq!(windowed, reference, "case {case} (mem={with_mem}): windowed read diverged from full dedup");
        assert_eq!(windowed, model, "case {case} (mem={with_mem}): read diverged from the model");
        assert_eq!(count(&client, &project, day).await?, model.len() as i64, "case {case}: COUNT(*) diverged");
    }
    let restricted_total = stat(&client, "dedup_key_restrict_scans").await?;
    match restrict {
        // Otherwise the differential compares two full dedups and proves nothing.
        true => {
            assert!(restricted_cases * 2 >= CASES as usize, "only {restricted_cases}/{CASES} cases took the restricted path");
            assert!(restricted_with_dvs > 0, "no restricted case read a DV-masked file");
        }
        false => assert_eq!(restricted_total, 0, "the flag is off, yet a scan restricted"),
    }
    Ok(())
}

/// Parse `name=N` from the first plan line containing `node`.
fn metric(plan: &str, node: &str, name: &str) -> Option<i64> {
    let line = plan.lines().find(|l| l.contains(node))?;
    let i = line.find(name)?;
    line[i + name.len()..].split(|c: char| !c.is_ascii_digit()).find(|s| !s.is_empty())?.parse().ok()
}

/// The COST claim: 200 proved rows plus a late file (3 versions of proved rows and 2 new
/// keys) overlapping most of them. Restricted, `DedupExec` sees exactly the 5 late rows and
/// the 3 proved rows they match; the rest bypass it. Unrestricted — flag off, or a key set
/// over its cap — every proved row the late file overlaps goes through (measured: 184).
#[test_case(true, None, true ; "restricted")]
#[test_case(false, None, false ; "flag off")]
#[test_case(true, Some(1), false ; "key set over the cap falls back")]
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn only_matching_proved_rows_reach_dedup(restrict: bool, max_keys: Option<usize>, restricted: bool) -> anyhow::Result<()> {
    timefusion::observability::init_local_metrics_for_test();
    let mut builder = E2eEnvBuilder::default().with_dedup_key_restrict(restrict).with_tantivy_prefilter(false).without_light_optimize();
    if let Some(n) = max_keys {
        builder = builder.with_dedup_key_restrict_max_keys(n);
    }
    let env = builder.start().await?;
    let client = env.pg_client().await?;
    let (project, day) = ("cost", DAY0);
    let at = |k: i64| day + 3_600 * SEC + k * 100_000;
    for k in 0..200 {
        insert(&client, project, &format!("r{k}"), "v0", at(k)).await?;
    }
    env.force_flush().await?;
    certify(&env, project, day).await?;
    for k in [10, 50, 150] {
        client.execute(&format!("UPDATE mor_versioned SET name = 'new' WHERE project_id = '{project}' AND id = 'r{k}'"), &[]).await?;
    }
    insert(&client, project, "late-a", "late", at(20)).await?;
    insert(&client, project, "late-b", "late", at(120)).await?;
    env.force_flush().await?;

    let fallbacks = stat(&client, "dedup_key_restrict_fallbacks").await?;
    let sql = format!("SELECT id, name FROM mor_versioned WHERE project_id = '{project}' AND timestamp >= {} AND timestamp < {}", lit(day), lit(day + DAY));
    let plan = flat_rows(&client, &format!("EXPLAIN ANALYZE {sql}")).await?;
    let dedup_rows = metric(&plan, "DedupExec", "input_rows=").unwrap_or(-1);
    match restricted {
        true => {
            assert_eq!(dedup_rows, 5 + 3, "rows entering DedupExec\nplan:\n{plan}");
            let bypassed = metric(&plan, "keep=non-members", "output_rows=").unwrap_or(-1);
            assert!(bypassed >= 100, "blocked proved rows bypassing DedupExec: {bypassed}\nplan:\n{plan}");
        }
        false => {
            assert!(dedup_rows >= 5 + 100, "an unrestricted scan sends the blocked proved files through DedupExec: {dedup_rows}\nplan:\n{plan}");
            assert!(!plan.contains("KeyFilterExec"), "an unrestricted scan must plan as today\nplan:\n{plan}");
        }
    }
    assert_eq!(stat(&client, "dedup_key_restrict_fallbacks").await? > fallbacks, max_keys.is_some(), "fallback counter");
    let rows = client.query(&sql, &[]).await?;
    assert_eq!(rows.len(), 202, "200 proved keys + 2 late keys");
    assert_eq!(rows.iter().filter(|r| r.get::<_, String>(1) == "new").count(), 3, "the updated versions must win");
    Ok(())
}
