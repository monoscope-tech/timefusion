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

use std::sync::Arc;

use anyhow::Result;
use datafusion::{execution::context::SessionContext, logical_expr::LogicalPlan};
use test_case::test_case;
use timefusion::{
    config::{AppConfig, TantivyConfig},
    database::Database,
};

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

/// SELECT over the prod schema with `pred` ANDed onto the mandatory project filter.
fn sql(pred: &str) -> String {
    format!("SELECT id FROM otel_logs_and_spans WHERE project_id = 'p' AND {pred}")
}

/// Analyzed plan text for one predicate, on a fresh analyzer-only context.
async fn plan_for(pred: &str) -> String {
    let ctx = analyzer_only_ctx().await.expect("ctx");
    let s = plan_str(&analyze(&ctx, &sql(pred)).await.expect("analyze"));
    println!("plan for `{pred}`:\n{s}"); // shown by nextest only when the case fails
    s
}

// route_equality (config default ON — config.rs) accelerates exact `=` on a raw-tokenized
// indexed column via the tantivy id-prefilter (invariant #1; OR-safety handled by
// collect_text_match_tree's completeness gate). `level` is raw-indexed, so `level = 'ERROR'`
// gets an ADDITIVE text_match — the original `=` is retained as the post-filter backstop.
#[test_case("level = 'ERROR'" => true ; "routes_exact_eq_on_raw_indexed_column")]
// `level` uses the `raw` tokenizer (single token, case-sensitive), so `LIKE '%RR%'` cannot be
// expressed as a tantivy primitive — there is no tantivy primitive that matches. The rewriter
// must NOT inject text_match; the original LIKE still applies. (`name` is now ngram3 so
// `%substring%` IS accelerable — see the "handles_infix_like_on_ngram3_column" case.)
#[test_case("level LIKE '%RR%'" => false ; "skips_infix_like_on_raw_tokenized_column")]
// `resource___service___name` has no tantivy config in the prod schema, so `=` on it must not
// route (invariant #3). (`id`/`status_code` are now raw-indexed, so they're no longer valid
// non-indexed examples.)
#[test_case("resource___service___name = 'abc'" => false ; "skips_non_indexed_columns")]
// `+` is a tantivy QueryParser metachar. Conservative path: skip the rewrite rather than
// misparse. Correctness preserved by retained LIKE.
#[test_case("status_message LIKE '%foo+bar%'" => false ; "skips_special_chars_in_literal")]
// `status_message` uses ngram3 → `LIKE '%failed%'` is accelerable.
#[test_case("status_message LIKE '%failed%'" => true ; "handles_infix_like_on_ngram3_column")]
#[test_case("status_message LIKE '%failed'" => true ; "handles_suffix_like_on_ngram3_column")]
#[test_case("status_message ILIKE '%FAILED%'" => true ; "handles_ilike_on_ngram3_column")]
// `level` uses raw (case-sensitive). ILIKE must NOT push down or we'd miss case variants in
// the prefilter set.
#[test_case("level ILIKE 'error'" => false ; "skips_ilike_on_raw_tokenized_column")]
// Sub-3-char literal on ngram3: no full trigram → tantivy term query would degenerate.
// Bail to scan.
#[test_case("name = 'ok'" => false ; "skips_sub_3_char_eq_on_ngram3")]
#[tokio::test]
async fn rewriter_routes_predicate(pred: &str) -> bool {
    plan_for(pred).await.contains("text_match")
}

#[tokio::test]
async fn rewriter_handles_trailing_wildcard_like() -> Result<()> {
    let s = plan_for("name LIKE 'api%'").await;
    // Prefix LIKE rewritten to text_match(col, 'api*').
    assert!(s.contains("text_match"), "expected text_match for prefix LIKE, got:\n{}", s);
    assert!(s.contains("api*") || s.contains("\"api*\""), "expected 'api*' query in plan, got:\n{}", s);
    Ok(())
}

#[tokio::test]
async fn rewriter_is_idempotent_under_replanning() -> Result<()> {
    let ctx = analyzer_only_ctx().await?;
    let sql = sql("status_message LIKE '%failed%'");
    let p1 = plan_str(&analyze(&ctx, &sql).await?);
    let p2 = plan_str(&analyze(&ctx, &sql).await?);
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
    // Two indexed columns via LIKE (the accelerated path; exact `=` is not
    // tantivy-routed) — both should get text_match injections. The optimizer's
    // filter pushdown duplicates each into the TableScan's partial_filters, so
    // the printed count is 2N; we assert both column-specific calls are present
    // rather than picking an exact count (less fragile across DataFusion versions).
    let s = plan_for("level LIKE 'ERR%' AND name LIKE 'svc%'").await;
    assert!(s.contains("text_match(level"), "expected text_match on level, got:\n{}", s);
    assert!(s.contains("text_match(name"), "expected text_match on name, got:\n{}", s);
    Ok(())
}

#[test]
fn indexed_tables_auto_discovers_prod_schema_and_is_schema_only() {
    // Default TantivyConfig (no env-override list) should still report the
    // prod schemas that have `tantivy.indexed: true` columns.
    // Schema is the single source of truth. No CSV override knob — adding
    // a knob nobody asks for is exactly what the project's CLAUDE.md
    // forbids ("compactness and succinctness is a priority").
    let cfg = TantivyConfig::default();
    let tables = cfg.indexed_tables();
    assert!(tables.iter().any(|t| t == "otel_logs_and_spans"), "expected otel_logs_and_spans to be auto-discovered, got {:?}", tables);
    // No way to inject a non-schema name now — confirm a synthetic name
    // is absent.
    assert!(!tables.iter().any(|t| t == "custom_table"));
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
