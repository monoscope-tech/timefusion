//! Regression: DataFusion 54's `push_down_leaf_projections` optimizer rule
//! (datafusion-optimizer `extract_leaf_expressions.rs:1218`) crashes with
//! `Internal error: Assertion failed: expr.is_empty()` on a multi-column
//! `Unnest` whose plan contains `get_field` (struct/map access) routing into
//! the Unnest. `Unnest::expressions()` exposes its `exec_columns`, but
//! `Unnest::with_new_exprs` asserts no exprs — the rule round-trips through
//! both and panics.
//!
//! Observed in prod via monoscope's UPDATE-2 dual-write
//! (`BackgroundJobs.hs` `dualExecPgTf`):
//!   UPDATE otel_logs_and_spans o SET hashes = … FROM
//!     (SELECT unnest($1) AS span_id, unnest($2) AS trace_id,
//!             unnest($3) AS tag) u WHERE o.… = u.…
use std::sync::Arc;

use serial_test::serial;
use timefusion::{database::Database, test_utils::test_helpers::minio_test_config};

async fn tf_session() -> anyhow::Result<datafusion::prelude::SessionContext> {
    timefusion::test_utils::init_test_logging();
    let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let cfg = minio_test_config(&test_id, &format!("/tmp/timefusion-unnest-{test_id}"));
    let db = Arc::new(Database::with_config(cfg).await?);
    let mut ctx = db.clone().create_session_context();
    db.setup_session_context(&mut ctx)?;
    Ok(ctx)
}

/// Minimal trigger: a multi-column UNNEST with struct columns accessed via
/// `get_field` above it. Planning must not panic in `push_down_leaf_projections`.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn multi_unnest_with_getfield_optimizes() -> anyhow::Result<()> {
    let ctx = tf_session().await?;
    let sql = "SELECT (u.st1)['k'] AS a, (u.st2)['k'] AS b FROM ( \
                 SELECT s.st1 AS st1, s.st2 AS st2, unnest(s.arr1) AS x, unnest(s.arr2) AS y \
                 FROM (SELECT named_struct('k','v') AS st1, named_struct('k','w') AS st2, \
                              [1,2] AS arr1, [3,4] AS arr2) s \
               ) u";
    let logical = ctx.state().create_logical_plan(sql).await?;
    ctx.state().optimize(&logical).map_err(|e| anyhow::anyhow!("push_down_leaf_projections regressed: {e}"))?;
    Ok(())
}

/// The prod UPDATE-2 shape (multi-column unnest source joined to the target).
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn update_from_multi_unnest_optimizes() -> anyhow::Result<()> {
    let ctx = tf_session().await?;
    let sql = "UPDATE otel_logs_and_spans o \
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
                    AND NOT (COALESCE(o.hashes, '{}'::text[]) @> ARRAY[u.tag])";
    let logical = ctx.state().create_logical_plan(sql).await?;
    ctx.state().optimize(&logical)?;
    Ok(())
}
