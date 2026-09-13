//! Plan-cache shape building: only now() may be parameterized, as a typed cast.
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
    timefusion::read::functions::register_custom_functions(&mut ctx).unwrap();
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
    // now() → CAST($n AS TIMESTAMPTZ), all other literals inline: must build, or nothing caches.
    let fixed = "SELECT greatest(count(*)::float8) FROM otel_logs_and_spans \
        WHERE project_id = 'p' AND timestamp >= CAST($1 AS TIMESTAMPTZ) - INTERVAL '1 hour' AND ((level = 'error')) \
        GROUP BY time_bucket('1 minute', timestamp) ORDER BY time_bucket('1 minute', timestamp) DESC LIMIT 100";
    assert!(build(fixed).await.is_ok(), "typed-now, literals-inline shape must build");

    // A bare (untyped) placeholder in `- INTERVAL` fails type inference — hence the CAST.
    let bare_now = "SELECT count(*) FROM otel_logs_and_spans WHERE timestamp >= $1 - INTERVAL '1 hour'";
    assert!(build(bare_now).await.is_err(), "bare now-placeholder arithmetic should fail (why we CAST)");

    // `INTERVAL $n` is unplannable — hence interval strings stay inline.
    let interval_ph = "SELECT count(*) FROM otel_logs_and_spans WHERE timestamp >= now() - INTERVAL $1";
    assert!(build(interval_ph).await.is_err(), "INTERVAL placeholder should fail (why we keep it inline)");
}
