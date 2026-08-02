//! Regression for the 2026-07-20 plan-cache finding: dashboard now()-bearing
//! shapes negative-cached (shape_hits=0) because the shape path pre-optimizes a
//! plan with unbound placeholders. Two failure modes were reproduced, and the
//! fix (parameterize ONLY now() as a TYPED cast, keep other literals inline) is
//! asserted to build. See src/plan_cache.rs parameterize_statement / the
//! include_strings=false now()-path.
use std::sync::Arc;

use datafusion::{
    arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field, Schema, TimeUnit},
    },
    prelude::SessionContext,
    sql::parser::DFParser,
};

async fn ctx() -> SessionContext {
    let mut ctx = SessionContext::new();
    timefusion::functions::register_custom_functions(&mut ctx).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("project_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("level", DataType::Utf8, true),
    ]));
    ctx.register_batch("otel_logs_and_spans", RecordBatch::new_empty(schema)).unwrap();
    ctx
}

/// Mirror get_or_build_shape: parse → statement_to_plan → optimize.
async fn build(sql: &str) -> Result<(), String> {
    let ctx = ctx().await;
    let state = ctx.state();
    let stmt = DFParser::parse_sql(sql).map_err(|e| format!("parse: {e}"))?.pop_front().unwrap();
    let plan = state.statement_to_plan(stmt).await.map_err(|e| format!("statement_to_plan: {e}"))?;
    state.optimize(&plan).map_err(|e| format!("optimize: {e}"))?;
    Ok(())
}

#[tokio::test]
async fn now_only_typed_shape_builds_while_bare_placeholder_shapes_fail() {
    // FIX: now() → CAST($n AS TIMESTAMPTZ), all other literals inline. This is
    // what the include_strings=false now()-path produces, and it must build so
    // the shape caches (repeated dashboard refreshes reuse it).
    let fixed = "SELECT greatest(count(*)::float8) FROM otel_logs_and_spans \
        WHERE project_id = 'p' AND timestamp >= CAST($1 AS TIMESTAMPTZ) - INTERVAL '1 hour' AND ((level = 'error')) \
        GROUP BY time_bucket('1 minute', timestamp) ORDER BY time_bucket('1 minute', timestamp) DESC LIMIT 100";
    assert!(build(fixed).await.is_ok(), "typed-now, literals-inline shape must build");

    // Bug B: a BARE (untyped) now-placeholder in `- INTERVAL` fails type inference.
    let bare_now = "SELECT count(*) FROM otel_logs_and_spans WHERE timestamp >= $1 - INTERVAL '1 hour'";
    assert!(build(bare_now).await.is_err(), "bare now-placeholder arithmetic should fail (why we CAST)");

    // Bug A: lifting the string inside INTERVAL → INTERVAL $n is unplannable.
    let interval_ph = "SELECT count(*) FROM otel_logs_and_spans WHERE timestamp >= now() - INTERVAL $1";
    assert!(build(interval_ph).await.is_err(), "INTERVAL placeholder should fail (why we keep it inline)");
}
