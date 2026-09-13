//! Guards that optimizing a multi-column `Unnest` plan does not panic:
//! `Unnest::expressions()` exposes exprs that `Unnest::with_new_exprs` rejects.
use std::sync::Arc;

use serial_test::serial;
use test_case::test_case;
use timefusion::{database::Database, support::test_helpers::minio_test_config};

async fn tf_session() -> anyhow::Result<datafusion::prelude::SessionContext> {
    timefusion::support::init_test_logging();
    let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let cfg = minio_test_config(&test_id, &format!("/tmp/timefusion-unnest-{test_id}"));
    let db = Arc::new(Database::with_config(cfg).await?);
    let mut ctx = db.clone().create_session_context();
    db.setup_session_context(&mut ctx)?;
    Ok(ctx)
}

// Minimal trigger: multi-column UNNEST with `get_field` struct access above it.
#[test_case(
    "SELECT (u.st1)['k'] AS a, (u.st2)['k'] AS b FROM ( \
       SELECT s.st1 AS st1, s.st2 AS st2, unnest(s.arr1) AS x, unnest(s.arr2) AS y \
       FROM (SELECT named_struct('k','v') AS st1, named_struct('k','w') AS st2, \
                    [1,2] AS arr1, [3,4] AS arr2) s \
     ) u" ; "multi_unnest_with_getfield_optimizes")]
// UPDATE whose FROM is a multi-column unnest source joined to the target.
#[test_case(
    "UPDATE otel_logs_and_spans o \
        SET hashes = COALESCE(o.hashes, '{}'::text[]) || ARRAY[u.tag] \
        FROM ( \
          SELECT unnest(ARRAY['s1','s2']::text[]) AS span_id, \
                 unnest(ARRAY['t1','t2']::text[]) AS trace_id, \
                 unnest(ARRAY['pat:a','pat:b']::text[]) AS tag \
        ) u \
        WHERE o.project_id = 'p1' \
          AND o.timestamp >= '2026-06-30T00:00:00Z' \
          AND o.timestamp <  '2026-06-30T23:59:59Z' \
          AND o.context___span_id = u.span_id \
          AND o.context___trace_id = u.trace_id \
          AND NOT (COALESCE(o.hashes, '{}'::text[]) @> ARRAY[u.tag])" ; "update_from_multi_unnest_optimizes")]
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn plan_and_optimize_without_panicking(sql: &'static str) -> anyhow::Result<()> {
    let ctx = tf_session().await?;
    let logical = ctx.state().create_logical_plan(sql).await?;
    ctx.state().optimize(&logical).map_err(|e| anyhow::anyhow!("push_down_leaf_projections regressed: {e}"))?;
    Ok(())
}
