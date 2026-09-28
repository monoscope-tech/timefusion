//! The per-file dedup split under the tantivy prefilter, over pgwire.

use chrono::{DateTime, Utc};
use timefusion::maintenance_coordinator::Operation;
use tokio_postgres::Client;

use super::harness::E2eEnvBuilder;

const SEC: i64 = 1_000_000;
const DAY: i64 = 86_400 * SEC;
/// 2025-01-01: a sealed past day, so the Dedup unit's sealed-chunk guard passes.
pub const DAY0: i64 = 1_735_689_600 * SEC;

pub fn ts(micros: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(micros).unwrap()
}

pub fn lit(micros: i64) -> String {
    format!("'{}'", ts(micros).format("%Y-%m-%dT%H:%M:%S%.6fZ"))
}

pub async fn stat(client: &Client, key: &str) -> anyhow::Result<i64> {
    let rows = client.query(&format!("SELECT value FROM timefusion_stats WHERE component = 'scan' AND key = '{key}'"), &[]).await?;
    Ok(rows.first().map_or(0, |r| r.get::<_, String>(0).parse().unwrap_or(0)))
}

/// Run the day-wide Dedup unit twice (a pass that masks duplicates proves the next clean).
pub async fn certify_table(env: &super::harness::E2eEnv, table: &str, project: &str, day: i64) -> anyhow::Result<()> {
    for _ in 0..2 {
        env.db().run_unit_once(table, project, ts(day).date_naive(), Operation::Dedup, 24, 0, None).await?;
    }
    Ok(())
}

async fn insert_named(client: &Client, project: &str, id: &str, name: &str, at: i64) -> anyhow::Result<()> {
    let dt = ts(at);
    let sql = format!(
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) \
         VALUES ($1, '{}', '{}', $2, '{name}', 'OK', 'm', 'INFO', ARRAY[]::text[], $3)",
        dt.date_naive(),
        dt.format("%Y-%m-%d %H:%M:%S%.f"),
    );
    client.execute(&sql, &[&project, &id, &vec!["s"]]).await?;
    Ok(())
}

/// Every in-window file indexed made each side of the per-file split take the
/// single-provider fast path, which reads EVERY live file — so a proved file's matches
/// were counted once through `DedupExec` and again on the skip leg (3 needles read as
/// 4 on master). A side pruned to nothing likewise fell back to an unrestricted scan.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn per_file_split_under_the_tantivy_prefilter_reads_each_file_once() -> anyhow::Result<()> {
    timefusion::observability::init_local_metrics_for_test();
    let env = E2eEnvBuilder::default().without_light_optimize().start().await?;
    let client = env.pg_client().await?;
    let (project, day) = ("tv", DAY0);
    // Two proved files an hour apart, one needle each; the late needle overlaps only the
    // second, so the first skips `DedupExec` and the second does not.
    for hour in [1, 2] {
        for k in 0..60 {
            insert_named(&client, project, &format!("h{hour}-{k}"), if k == 5 { "needle" } else { "span" }, day + hour * 3_600 * SEC + k * SEC).await?;
        }
        env.force_flush().await?;
    }
    certify_table(&env, "otel_logs_and_spans", project, day).await?;
    insert_named(&client, project, "late", "needle", day + 7_200 * SEC + 3 * SEC).await?;
    env.force_flush().await?;
    let sql = |term: &str| {
        format!(
            "SELECT id FROM otel_logs_and_spans WHERE project_id = '{project}' AND text_match(name, '{term}') AND timestamp >= {} AND timestamp < {}",
            lit(day),
            lit(day + DAY)
        )
    };
    // The sidecar index builds in a detached task; only a read the prefilter served
    // exercises the path under guard.
    for _ in 0..60 {
        let (used, split) = (stat(&client, "prefilter_used").await?, stat(&client, "dedup_skipped_per_file").await?);
        let rows = client.query(&sql("needle"), &[]).await?;
        if stat(&client, "prefilter_used").await? > used {
            assert!(stat(&client, "dedup_skipped_per_file").await? > split, "the read must take the per-file split");
            assert_eq!(rows.len(), 3, "each needle exactly once");
            assert!(client.query(&sql("absent"), &[]).await?.is_empty(), "a term no file holds must read nothing, not fail");
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    anyhow::bail!("the tantivy prefilter never served the read, so nothing was tested")
}
