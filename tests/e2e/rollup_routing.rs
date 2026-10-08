//! Monoscope's dashboard SQL, sent verbatim over pgwire, must route to the rollup tiers.

use super::harness::{E2eEnv, FROZEN_START_MICROS};

/// Regression (2026-10-08): three overview widgets never routed in prod and timed out at
/// 90 s over 7–30 days. Error rate missed with `missing_measure`: its bound `$1 = 0`
/// stayed Int32 beside the Int32 status column while the declared filter's `0` widened
/// the column to Int64. Requests-by-status missed with `unknown_filter`: the tantivy hint
/// inside the HTTP scope's OR (`kind = 'server' AND text_match(kind, 'server')`) hid
/// that the scope is all dimensions of `endpoints_1m`.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn monoscope_overview_widgets_route_to_rollup_tiers() -> anyhow::Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    use timefusion::maintenance_coordinator::{Operation, TaskState};
    let env = E2eEnv::builder().start().await?;
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;
    let day = chrono::DateTime::from_timestamp_micros(FROZEN_START_MICROS).unwrap().date_naive().pred_opt().unwrap();
    let noon = day.and_hms_opt(12, 0, 0).unwrap().and_utc();
    // Server spans 3 of 5 erroring (two 5xx, one ERROR status); the client 500 is out of scope.
    for (minute, kind, status, http_status) in [
        (10, "server", "OK", 200),
        (15, "server", "OK", 500),
        (20, "server", "OK", 503),
        (25, "server", "OK", 404),
        (30, "server", "ERROR", 200),
        (35, "client", "OK", 500),
    ] {
        client
            .execute(
                "INSERT INTO otel_logs_and_spans (project_id, timestamp, id, hashes, summary, name, kind, status_code, attributes___http___response___status_code) \
                 VALUES ('e2e_project', $1, $2, ARRAY[]::text[], ARRAY['fixture'], 'GET', $3, $4, $5)",
                &[&(noon + chrono::Duration::minutes(minute)), &format!("span-{minute}"), &kind, &status, &http_status],
            )
            .await?;
    }
    env.force_flush().await?;
    env.advance(std::time::Duration::from_secs(2 * 86_400));
    env.force_evict().await?;
    let table = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    for _ in 0..2 {
        env.db().dedup_today_partitions(&table, "otel_logs_and_spans", "otel_logs_and_spans").await?;
    }
    for tier in ["dashboard_1m_v4", "endpoints_1m"] {
        let built = env.db().run_unit_once("otel_logs_and_spans", "e2e_project", day, Operation::BaseRollup, 24, 0, Some(tier)).await?;
        assert_eq!(built.state, Some(TaskState::Complete), "{tier} must be published");
    }

    let scope = format!("project_id='e2e_project' and timestamp between '{}' and '{}'", noon.to_rfc3339(), (noon + chrono::Duration::hours(4)).to_rfc3339());
    let http = "(kind = 'server' or name = 'apitoolkit-http-span' or name = 'monoscope.http')";
    let status = "coalesce(cast(attributes___http___response___status_code as text), 'unknown')";
    let stats = timefusion::observability::maintenance_stats();
    let hits = || stats.rollup_hits_full.load(Relaxed) + stats.rollup_hits_hybrid.load(Relaxed);
    for (widget, sql, want) in [
        (
            "error rate",
            format!(
                "select extract(epoch from time_bucket('1 hour', timestamp))::integer, 'value', round((coalesce(((count(*) filter (where status_code = 'ERROR' \
                 or coalesce(attributes___http___response___status_code, 0) >= 500)::float * 100.0) / nullif(count(*)::float, 0)), 0))::numeric, 2)::float \
                 from otel_logs_and_spans where {scope} and ((({http}))) group by time_bucket('1 hour', timestamp) order by time_bucket('1 hour', timestamp) desc"
            ),
            vec!["value 60.0"],
        ),
        (
            "requests by status",
            format!(
                "select extract(epoch from time_bucket('1 hour', timestamp))::integer, {status}, count(*)::float as count_ from otel_logs_and_spans \
                 where {scope} and (({http} and attributes___http___response___status_code is not null)) \
                 group by time_bucket('1 hour', timestamp), {status} order by time_bucket('1 hour', timestamp) desc limit 10000"
            ),
            vec!["200 2.0", "404 1.0", "500 1.0", "503 1.0"],
        ),
        (
            "top endpoints",
            format!(
                "select name, count(*)::bigint from otel_logs_and_spans where {scope} and name is not null and kind = 'server' group by name order by count(*) desc limit 20"
            ),
            vec!["GET 5"],
        ),
    ] {
        let before = hits();
        let mut rows: Vec<String> = client
            .simple_query(&sql)
            .await?
            .into_iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    Some((row.len().saturating_sub(2)..row.len()).filter_map(|i| row.get(i)).collect::<Vec<_>>().join(" "))
                }
                _ => None,
            })
            .collect();
        rows.sort();
        assert_eq!(rows, want, "{widget}");
        assert_eq!(hits() - before, 1, "the {widget} widget must route to a rollup tier");
    }
    Ok(())
}
