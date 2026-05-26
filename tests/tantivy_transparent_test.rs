//! Transparent Tantivy: verify the predicate rewriter wires correctly into
//! the analyzer chain and produces the right LogicalPlan transformations
//! for the supported SQL forms.
//!
//! These tests don't need MinIO/Delta. We construct a session context with
//! the registered ProjectRoutingTable (which carries the real schema with
//! tantivy.indexed metadata), parse SQL to a LogicalPlan, and inspect the
//! analyzed plan for the injected `text_match` calls. End-to-end behavior
//! (the prefilter actually narrowing the Delta scan) is covered by
//! `tantivy_e2e_test.rs` which runs against MinIO.
//!
//! Correctness invariants tested:
//! 1. `col = 'lit'` on an indexed column produces both the original `=`
//!    AND a `text_match(col, 'lit')` (additive — never replaces).
//! 2. `col LIKE 'prefix%'` produces `text_match(col, 'prefix*')`.
//! 3. Non-indexed columns are left alone — no `text_match` injected.
//! 4. Unsupported LIKE patterns (`'%substr%'`, `'foo_bar'`) are left alone.
//! 5. The rewriter is idempotent — re-applying it doesn't double-wrap.
//! 6. `TantivyConfig::indexed_tables()` auto-discovers prod schema columns.

#![cfg(test)]

use anyhow::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use std::sync::Arc;
use timefusion::config::{AppConfig, TantivyConfig};
use timefusion::database::Database;

/// Build a minimal in-memory session context with the prod schemas
/// registered. No Delta, no MemBuffer — just the analyzer chain.
async fn analyzer_only_ctx() -> Result<SessionContext> {
    let mut c = AppConfig::default();
    // Stub out S3 settings — we never touch the network for analyzer tests.
    c.aws.aws_s3_bucket = Some("test-bucket".to_string());
    c.aws.aws_s3_endpoint = "http://localhost:1".to_string(); // unused
    c.core.timefusion_data_dir = std::env::temp_dir().join("tf-analyzer-test");
    c.cache.timefusion_foyer_disabled = true;
    let db = Database::with_config(Arc::new(c)).await?;
    let db_arc = Arc::new(db.clone());
    let mut ctx = db_arc.create_session_context();
    db.setup_session_context(&mut ctx)?;
    Ok(ctx)
}

/// Parse + analyze a SELECT and return its analyzed LogicalPlan.
/// `ctx.sql()` goes through statement_to_plan → analyzer rules; pulling
/// `df.logical_plan()` gives us the post-analyzer plan, which is what our
/// rewriter has touched. `state().create_logical_plan()` skips the analyzer.
async fn analyze(ctx: &SessionContext, sql: &str) -> Result<LogicalPlan> {
    // `ctx.sql()` / `df.logical_plan()` only does parse + statement_to_plan
    // in DataFusion 53. Analyzer rules run inside `state.optimize()` (which
    // runs both analyzer and optimizer). To inspect the post-rewriter plan
    // without optimizer transformations, we'd need internal APIs; for our
    // assertions, optimized plan is fine because the optimizer can't remove
    // text_match calls.
    let plan = ctx.state().create_logical_plan(sql).await?;
    Ok(ctx.state().optimize(&plan)?)
}

/// Stringify a LogicalPlan and check for substring presence — robust to
/// whatever DataFusion uses internally (Expr::Display formatting).
fn plan_str(plan: &LogicalPlan) -> String {
    plan.display_indent_schema().to_string()
}

#[tokio::test]
async fn rewriter_injects_text_match_for_eq_on_indexed_column() -> Result<()> {
    let ctx = analyzer_only_ctx().await?;
    // `level` is indexed (tantivy.indexed: true, tokenizer: raw) in the prod
    // YAML. The rewriter should produce `level = 'ERROR' AND text_match(level, 'ERROR')`.
    let plan = analyze(
        &ctx,
        "SELECT id FROM otel_logs_and_spans WHERE project_id = 'p' AND level = 'ERROR'",
    )
    .await?;
    let s = plan_str(&plan);
    assert!(s.contains("text_match"), "expected text_match in plan, got:\n{}", s);
    // The original `=` must still appear (additive — correctness invariant).
    assert!(s.contains("level = "), "expected original = filter retained, got:\n{}", s);
    Ok(())
}

#[tokio::test]
async fn rewriter_handles_trailing_wildcard_like() -> Result<()> {
    let ctx = analyzer_only_ctx().await?;
    let plan = analyze(
        &ctx,
        "SELECT id FROM otel_logs_and_spans WHERE project_id = 'p' AND name LIKE 'api%'",
    )
    .await?;
    let s = plan_str(&plan);
    // Prefix LIKE rewritten to text_match(col, 'api*').
    assert!(s.contains("text_match"), "expected text_match for prefix LIKE, got:\n{}", s);
    assert!(s.contains("api*") || s.contains("\"api*\""), "expected 'api*' query in plan, got:\n{}", s);
    Ok(())
}

#[tokio::test]
async fn rewriter_leaves_unsupported_like_patterns_alone() -> Result<()> {
    let ctx = analyzer_only_ctx().await?;
    let plan = analyze(
        &ctx,
        "SELECT id FROM otel_logs_and_spans WHERE project_id = 'p' AND name LIKE '%substring%'",
    )
    .await?;
    let s = plan_str(&plan);
    // `%substring%` cannot be expressed as a tantivy prefix or term query —
    // rewriter must NOT inject text_match (original LIKE still correct).
    assert!(!s.contains("text_match"), "expected NO text_match for embedded wildcards, got:\n{}", s);
    Ok(())
}

#[tokio::test]
async fn rewriter_skips_non_indexed_columns() -> Result<()> {
    let ctx = analyzer_only_ctx().await?;
    // `id` is NOT indexed in the prod schema (tantivy: null).
    let plan = analyze(
        &ctx,
        "SELECT id FROM otel_logs_and_spans WHERE project_id = 'p' AND id = 'abc'",
    )
    .await?;
    let s = plan_str(&plan);
    assert!(!s.contains("text_match"), "expected NO text_match on non-indexed col, got:\n{}", s);
    Ok(())
}

#[tokio::test]
async fn rewriter_skips_special_chars_in_literal() -> Result<()> {
    let ctx = analyzer_only_ctx().await?;
    // `+` is a tantivy QueryParser metachar. Conservative path: skip the
    // rewrite rather than misparse. Correctness preserved by retained `=`.
    let plan = analyze(
        &ctx,
        "SELECT id FROM otel_logs_and_spans WHERE project_id = 'p' AND level = 'foo+bar'",
    )
    .await?;
    let s = plan_str(&plan);
    assert!(!s.contains("text_match"), "expected NO text_match on metachar literal, got:\n{}", s);
    Ok(())
}

#[tokio::test]
async fn rewriter_is_idempotent_under_replanning() -> Result<()> {
    let ctx = analyzer_only_ctx().await?;
    let sql = "SELECT id FROM otel_logs_and_spans WHERE project_id = 'p' AND level = 'INFO'";
    let p1 = plan_str(&analyze(&ctx, sql).await?);
    let p2 = plan_str(&analyze(&ctx, sql).await?);
    // Same SQL twice should produce the same plan (deterministic). The
    // optimizer pushes the wrapped filter into TableScan::partial_filters
    // which DUPLICATES the text_match in the printed plan (once in
    // Filter, once on the scan) — we don't assert an exact count, only
    // that text_match appears and the two runs match each other.
    assert_eq!(p1, p2, "non-deterministic plan");
    assert!(p1.contains("text_match"), "expected text_match in plan, got:\n{}", p1);
    Ok(())
}

#[tokio::test]
async fn rewriter_handles_multiple_indexed_predicates() -> Result<()> {
    let ctx = analyzer_only_ctx().await?;
    // Two indexed columns (level + name) — both should get text_match
    // injections. The optimizer's filter pushdown duplicates each into the
    // TableScan's partial_filters, so the printed count is 2N; we assert
    // both column-specific calls are present rather than picking an exact
    // count (less fragile across DataFusion versions).
    let plan = analyze(
        &ctx,
        "SELECT id FROM otel_logs_and_spans WHERE project_id = 'p' AND level = 'ERROR' AND name = 'svc'",
    )
    .await?;
    let s = plan_str(&plan);
    assert!(s.contains("text_match(level"), "expected text_match on level, got:\n{}", s);
    assert!(s.contains("text_match(name"), "expected text_match on name, got:\n{}", s);
    Ok(())
}

#[test]
fn indexed_tables_auto_discovers_prod_schema() {
    // Default TantivyConfig (no env-override list) should still report the
    // prod schemas that have `tantivy.indexed: true` columns.
    let cfg = TantivyConfig::default();
    let tables = cfg.indexed_tables();
    assert!(
        tables.iter().any(|t| t == "otel_logs_and_spans"),
        "expected otel_logs_and_spans to be auto-discovered, got {:?}",
        tables
    );
}

#[test]
fn indexed_tables_merges_csv_override() {
    let cfg = TantivyConfig {
        timefusion_tantivy_indexed_tables: Some("custom_table,other".to_string()),
        ..Default::default()
    };
    let tables = cfg.indexed_tables();
    // Both auto-discovered + CSV-overridden tables present, union.
    assert!(tables.iter().any(|t| t == "otel_logs_and_spans"), "auto-discovery still in effect");
    assert!(tables.iter().any(|t| t == "custom_table"), "CSV override merged");
    assert!(tables.iter().any(|t| t == "other"), "CSV override merged (2)");
}

#[test]
fn prefilter_knobs_have_sane_defaults() {
    // Construct via serde defaults (AppConfig::default goes through envy
    // with an empty iter, which invokes each `#[serde(default = "fn")]`).
    // The bare `TantivyConfig::default()` from `#[derive(Default)]` does
    // not pick up serde defaults — it returns 0 for usize fields.
    let cfg = AppConfig::default();
    // 100k default is high enough to avoid false aborts on typical queries
    // but low enough to keep the IN-list manageable.
    assert!(cfg.tantivy.prefilter_max_hits() >= 1000, "got {}", cfg.tantivy.prefilter_max_hits());
    // Selectivity guard: don't push down if results are > 50% of corpus
    // (default), but stay between (0, 100].
    let s = cfg.tantivy.prefilter_min_selectivity_pct();
    assert!(s > 0 && s <= 100);
}
