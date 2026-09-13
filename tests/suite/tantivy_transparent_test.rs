//! Analyzer-level tests for the tantivy predicate rewriter: which SQL predicates
//! get an additive `text_match` injected into the LogicalPlan. No MinIO/Delta —
//! end-to-end prefilter behavior lives in `tantivy_e2e_test.rs`.

#![cfg(test)]

use std::sync::Arc;

use anyhow::Result;
use datafusion::{execution::context::SessionContext, logical_expr::LogicalPlan};
use test_case::test_case;
use timefusion::{
    config::{AppConfig, TantivyConfig},
    database::Database,
};

/// Build a minimal in-memory session context with the prod schemas registered.
async fn analyzer_only_ctx() -> Result<SessionContext> {
    let mut c = AppConfig::default();
    c.aws.aws_s3_bucket = Some("test-bucket".to_string());
    c.aws.aws_s3_endpoint = "http://localhost:1".to_string();
    c.core.timefusion_data_dir = std::env::temp_dir().join("tf-analyzer-test");
    c.cache.timefusion_foyer_disabled = true;
    let db = Database::with_config(Arc::new(c)).await?;
    let db_arc = Arc::new(db.clone());
    let mut ctx = db_arc.create_session_context();
    db.setup_session_context(&mut ctx)?;
    Ok(ctx)
}

/// Parse + analyze a SELECT and return the post-analyzer LogicalPlan.
/// Analyzer rules only run inside `state.optimize()`, so the returned plan is
/// also optimized — harmless here, since the optimizer never removes text_match.
async fn analyze(ctx: &SessionContext, sql: &str) -> Result<LogicalPlan> {
    let plan = ctx.state().create_logical_plan(sql).await?;
    Ok(ctx.state().optimize(&plan)?)
}

fn plan_str(plan: &LogicalPlan) -> String {
    plan.display_indent_schema().to_string()
}

/// SELECT over the prod schema with `pred` ANDed onto the mandatory project filter.
fn sql(pred: &str) -> String {
    format!("SELECT id FROM otel_logs_and_spans WHERE project_id = 'p' AND {pred}")
}

/// Analyzed plan text for one predicate, on a fresh analyzer-only context.
async fn plan_for(pred: &str) -> String {
    let ctx = analyzer_only_ctx().await.expect("ctx");
    let s = plan_str(&analyze(&ctx, &sql(pred)).await.expect("analyze"));
    println!("plan for `{pred}`:\n{s}");
    s
}

// Schema tokenizers these cases rely on: `level` = raw (single token, case-sensitive),
// `status_message`/`name` = ngram3, `resource___service___name` = not indexed.
// Injection is always ADDITIVE — the original predicate stays as the post-filter backstop,
// so a skipped rewrite is only a lost optimization, never a wrong answer.
#[test_case("level = 'ERROR'" => true ; "routes_exact_eq_on_raw_indexed_column")]
#[test_case("level LIKE '%RR%'" => false ; "skips_infix_like_on_raw_tokenized_column")]
#[test_case("resource___service___name = 'abc'" => false ; "skips_non_indexed_columns")]
// `+` is a tantivy QueryParser metachar — skip rather than misparse.
#[test_case("status_message LIKE '%foo+bar%'" => false ; "skips_special_chars_in_literal")]
#[test_case("status_message LIKE '%failed%'" => true ; "handles_infix_like_on_ngram3_column")]
#[test_case("status_message LIKE '%failed'" => true ; "handles_suffix_like_on_ngram3_column")]
#[test_case("status_message ILIKE '%FAILED%'" => true ; "handles_ilike_on_ngram3_column")]
// ILIKE on a case-sensitive raw column must not push down, or case variants are missed.
#[test_case("level ILIKE 'error'" => false ; "skips_ilike_on_raw_tokenized_column")]
// Sub-3-char literal yields no full trigram on an ngram3 column.
#[test_case("name = 'ok'" => false ; "skips_sub_3_char_eq_on_ngram3")]
#[tokio::test]
async fn rewriter_routes_predicate(pred: &str) -> bool {
    plan_for(pred).await.contains("text_match")
}

#[test_case("name LIKE 'api%'", &["text_match", "api*"] ; "rewriter_handles_trailing_wildcard_like")]
// Both columns get an injection. Filter pushdown duplicates each call in the printed plan,
// so assert presence per column rather than an exact count.
#[test_case("level LIKE 'ERR%' AND name LIKE 'svc%'", &["text_match(level", "text_match(name"] ; "rewriter_handles_multiple_indexed_predicates")]
#[tokio::test]
async fn rewriter_injects(pred: &'static str, needles: &'static [&'static str]) {
    let s = plan_for(pred).await;
    for needle in needles {
        assert!(s.contains(needle), "expected `{needle}` in the plan for `{pred}`, got:\n{s}");
    }
}

#[tokio::test]
async fn rewriter_is_idempotent_under_replanning() -> Result<()> {
    let ctx = analyzer_only_ctx().await?;
    let sql = sql("status_message LIKE '%failed%'");
    let p1 = plan_str(&analyze(&ctx, &sql).await?);
    let p2 = plan_str(&analyze(&ctx, &sql).await?);
    assert_eq!(p1, p2, "non-deterministic plan");
    assert!(p1.contains("text_match"), "expected text_match in plan, got:\n{}", p1);
    Ok(())
}

#[test]
fn indexed_tables_auto_discovers_prod_schema_and_is_schema_only() {
    // The schema is the only source of indexed tables — there is no override knob.
    let cfg = TantivyConfig::default();
    let tables = cfg.indexed_tables();
    assert!(tables.iter().any(|t| t == "otel_logs_and_spans"), "expected otel_logs_and_spans to be auto-discovered, got {:?}", tables);
    assert!(!tables.iter().any(|t| t == "custom_table"));
}

#[test]
fn prefilter_knobs_have_sane_defaults() {
    // Must go through `AppConfig::default()` (serde defaults); bare
    // `TantivyConfig::default()` derives 0 for these usize fields.
    let cfg = AppConfig::default();
    assert!(cfg.tantivy.prefilter_max_hits() >= 1000, "got {}", cfg.tantivy.prefilter_max_hits());
    let s = cfg.tantivy.prefilter_min_selectivity_pct();
    assert!(s > 0 && s <= 100);
}
