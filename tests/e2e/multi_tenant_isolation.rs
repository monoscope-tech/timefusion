//! Two project_ids in the same unified table must not leak into each other's results.

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_for};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn project_id_filter_isolates_tenants() -> anyhow::Result<()> {
    let env = E2eEnv::builder().start().await?;
    // The harness pre-warms "e2e_project". Add a second tenant explicitly.
    env.db().get_or_create_table("e2e_other", "otel_logs_and_spans").await?;
    let client = env.pg_client().await?;

    for (project, prefix, rows) in [("e2e_project", "a", 3), ("e2e_other", "b", 5)] {
        for i in 0..rows {
            insert_for(&client, project, &format!("{prefix}-{i}"), FROZEN_START_MICROS).await?;
        }
    }

    for (project, want) in [("e2e_project", 3i64), ("e2e_other", 5)] {
        let got: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&project]).await?.get(0);
        assert_eq!(got, want, "project {project} leaked or lost rows: got {got}");
    }
    Ok(())
}

/// RUM's bound project/window/scope must preserve browser populations and use the
/// whole-hour session tier, including raw observations in the partial edge hour.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn prepared_rum_sessions_use_browser_rollups_and_raw_edges() -> anyhow::Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    use timefusion::maintenance_coordinator::{Operation, TaskState};
    let env = E2eEnv::builder().start().await?;
    env.db().cancel_maintenance();
    let client = env.pg_client().await?;
    let today = chrono::DateTime::from_timestamp_micros(FROZEN_START_MICROS).unwrap().date_naive();
    let day = today.pred_opt().unwrap();
    let noon = day.and_hms_opt(12, 0, 0).unwrap().and_utc();
    for (id, minutes, language, name, status, environment, service, user, agent) in [
        ("browser-a", 10, "rust", "documentLoad", "OK", "production", "web", "a", None),
        ("browser-b", 35, "javascript", "click", "ERROR", "production", "web", "b", None),
        ("browser-other-env", 50, "js", "click", "OK", "staging", "web", "c", None),
        ("browser-other-service", 70, "webjs", "click", "OK", "production", "shop", "d", None),
        ("browser-edge", 190, "webjs", "Pageview /edge", "OK", "production", "web", "e", None),
        ("server-noise", 195, "rust", "server", "ERROR", "production", "web", "z", None),
        ("agent-browser", 45, "rust", "server", "OK", "production", "web", "aa", Some("Mozilla/5.0")),
        ("fetch-browser", 80, "rust", "documentFetch", "OK", "production", "web", "ab", None),
        ("page-browser", 15, "rust", "Pageview /only-name", "OK", "production", "web", "ac", None),
    ] {
        let timestamp = noon + chrono::Duration::minutes(minutes);
        let full_name = format!("{user}-name");
        let email = format!("{user}@fixture");
        client
            .execute(
                "INSERT INTO otel_logs_and_spans (project_id, timestamp, id, hashes, summary, name, status_code, \
             attributes___session___id, resource___telemetry___sdk___language, resource___deployment___environment___name, \
             resource___service___name, attributes___user___id, attributes___user___full_name, attributes___user___email, resource___user_agent___original) \
             VALUES ('e2e_project', $1, $2, ARRAY[]::text[], ARRAY['fixture'], $3, $4, 'session', $5, $6, $7, $8, $9, $10, $11)",
                &[&timestamp, &id, &name, &status, &language, &environment, &service, &user, &full_name, &email, &agent],
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
    let built = env.db().run_unit_once("otel_logs_and_spans", "e2e_project", day, Operation::BaseRollup, 24, 0, Some("sessions_1h_v2")).await?;
    assert_eq!(built.state, Some(TaskState::Complete), "browser session tier must be published");
    let sql = "SELECT attributes___session___id, MIN(timestamp), MAX(timestamp), COUNT(*)::bigint, \
               COUNT(*) FILTER (WHERE status_code='ERROR' OR lower(COALESCE(level,''))='error' OR attributes___exception___type IS NOT NULL)::bigint, \
               COUNT(*) FILTER (WHERE name LIKE 'Pageview %' OR name='documentLoad')::bigint, \
               MAX(attributes___user___id),MAX(attributes___user___full_name),MAX(attributes___user___email),MAX(resource___service___name), \
               NULL::text AS last_page,NULL::text AS user_agent,false AS has_replay \
               FROM otel_logs_and_spans WHERE project_id=$1 AND timestamp >= $2 AND timestamp <= $3 \
               AND ($4::text IS NULL OR resource___deployment___environment___name=$5) \
               AND ($6::text IS NULL OR resource___service___name=$7) \
               AND (resource___telemetry___sdk___language IN ('webjs','javascript','js') OR resource___user_agent___original IS NOT NULL \
                    OR name IN ('documentLoad','documentFetch') OR name LIKE 'Pageview %' OR name='documentLoad') \
               AND attributes___session___id IS NOT NULL AND attributes___session___id<>'' \
               GROUP BY attributes___session___id HAVING true ORDER BY MAX(timestamp) DESC LIMIT 200";
    let from = noon + chrono::Duration::minutes(5);
    let to = noon + chrono::Duration::minutes(200);
    let stats = timefusion::observability::maintenance_stats();
    let literal =
        (2..=7).fold(sql.replace("project_id=$1", "project_id='e2e_project'"), |sql, index| sql.replace(&format!("${index}"), &format!("${}", index - 1)));
    for (environment, service, counts, (first, last), max_user, max_service) in [
        (None, None, [8i64, 1, 3], (10, 190), "e", "web"),
        (Some("production"), None, [7, 1, 3], (10, 190), "e", "web"),
        (None, Some("web"), [7, 1, 3], (10, 190), "e", "web"),
        (Some("production"), Some("shop"), [1, 0, 0], (70, 70), "d", "shop"),
        (Some("staging"), None, [1, 0, 0], (50, 50), "c", "web"),
    ] {
        for ((from, to), bound_project) in
            itertools::iproduct!([(from, to), (noon, noon + chrono::Duration::hours(4) - chrono::Duration::microseconds(1))], [true, false])
        {
            let counter = if from == noon { &stats.rollup_hits_full } else { &stats.rollup_hits_hybrid };
            let before = counter.load(Relaxed);
            let rows = if bound_project {
                client.query(sql, &[&"e2e_project", &from, &to, &environment, &environment, &service, &service]).await?
            } else {
                client.query(&literal, &[&from, &to, &environment, &environment, &service, &service]).await?
            };
            assert_eq!(rows.len(), 1);
            let row = &rows[0];
            assert_eq!(row.get::<_, String>(0), "session");
            assert_eq!(row.get::<_, chrono::DateTime<chrono::Utc>>(1), noon + chrono::Duration::minutes(first));
            assert_eq!(row.get::<_, chrono::DateTime<chrono::Utc>>(2), noon + chrono::Duration::minutes(last));
            assert_eq!([row.get::<_, i64>(3), row.get::<_, i64>(4), row.get::<_, i64>(5)], counts);
            for (column, suffix) in [(6, ""), (7, "-name"), (8, "@fixture")] {
                assert_eq!(row.get::<_, String>(column), format!("{max_user}{suffix}"));
            }
            assert_eq!(row.get::<_, String>(9), max_service);
            assert_eq!(
                counter.load(Relaxed) - before,
                1,
                "RUM query must hit the browser tier: bound_project={bound_project}, environment={environment:?}, service={service:?}"
            );
            println!(
                "bound_project={bound_project} environment={environment:?} service={service:?} full_hour_window={} rollup_hits={} counts={counts:?}",
                from == noon,
                counter.load(Relaxed) - before
            );
        }
    }
    Ok(())
}
