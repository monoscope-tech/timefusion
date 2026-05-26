//! Transparent Tantivy acceleration for standard SQL predicates.
//!
//! Rewrites `col = 'literal'` and `col LIKE 'pattern'` on
//! tantivy-indexed columns by **additively** AND-ing a `text_match(col, q)`
//! call to the predicate. The original comparison is never removed — it
//! still applies as a post-filter on MemBuffer rows and Delta files whose
//! tantivy index hasn't built yet (post-flush lag). The `text_match` call,
//! once picked up by the existing routing logic in `ProjectRoutingTable`,
//! produces an `id IN (...)` prefilter that narrows the Delta scan.
//!
//! Correctness invariants:
//! 1. The original predicate is preserved verbatim in the plan, so any row
//!    that satisfies it (regardless of whether tantivy returned it) is
//!    correctly emitted.
//! 2. We only rewrite predicates on columns that are confirmed
//!    `tantivy.indexed: true` in the table's YAML schema. Non-indexed
//!    columns are left alone — adding `text_match` on them would be a
//!    correctness bug (the UDF's substring fallback works, but the prefilter
//!    would return `None` and we'd waste a round trip).
//! 3. Already-wrapped predicates aren't rewrapped — the analyzer is
//!    idempotent under repeated passes.
//! 4. LIKE patterns with non-trailing wildcards (e.g. `'%substr%'`) are left
//!    alone; tantivy term/prefix queries can't express them without an
//!    n-gram tokenizer, which v1 doesn't ship.
//!
//! Patterns currently rewritten:
//!
//! | SQL form                | tantivy query passed to text_match     |
//! |-------------------------|----------------------------------------|
//! | `col = 'literal'`       | `'literal'`                            |
//! | `col LIKE 'literal'`    | `'literal'`  (no wildcards)            |
//! | `col LIKE 'prefix%'`    | `'prefix*'` (trailing-wildcard prefix) |
//!
//! Patterns explicitly NOT rewritten in v1: `'%suffix'`, `'%substr%'`,
//! `ILIKE` (case-insensitive coupling depends on tokenizer choice — left
//! to the existing `text_match` UDF substring fallback for now), and any
//! pattern containing `_` (single-char wildcard).

use datafusion::common::{
    Result,
    tree_node::{Transformed, TreeNode, TreeNodeRecursion},
};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, ScalarUDF, expr::Like, expr::ScalarFunction, lit};
use datafusion::optimizer::AnalyzerRule;
use datafusion::scalar::ScalarValue;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use crate::tantivy_index::udf::{TEXT_MATCH_NAME, TextMatchUdf};

#[derive(Debug, Default)]
pub struct TantivyPredicateRewriter;

impl AnalyzerRule for TantivyPredicateRewriter {
    fn name(&self) -> &str {
        "tantivy_predicate_rewriter"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        if matches!(plan, LogicalPlan::Dml(_)) {
            return Ok(plan);
        }
        Ok(plan.transform_down(|p| rewrite_node(p))?.data)
    }
}

fn rewrite_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Filter(mut filter) => {
            let Some(table) = find_indexed_table(&filter.input) else {
                return Ok(Transformed::no(LogicalPlan::Filter(filter)));
            };
            let columns = match indexed_columns_for(&table) {
                Some(c) if !c.is_empty() => c,
                _ => return Ok(Transformed::no(LogicalPlan::Filter(filter))),
            };
            let new_pred = filter.predicate.clone().transform_down(|e| rewrite_expr(e, &columns))?.data;
            filter.predicate = new_pred;
            Ok(Transformed::yes(LogicalPlan::Filter(filter)))
        }
        _ => Ok(Transformed::no(plan)),
    }
}

fn rewrite_expr(expr: Expr, indexed_columns: &HashSet<String>) -> Result<Transformed<Expr>> {
    // Skip the children of a text_match call (already a tantivy predicate).
    if let Expr::ScalarFunction(sf) = &expr {
        if sf.func.name() == TEXT_MATCH_NAME {
            return Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump));
        }
    }
    if let Some((column, query)) = match_indexed_predicate(&expr, indexed_columns) {
        let tm = text_match_call(column, query);
        let wrapped = Expr::BinaryExpr(BinaryExpr::new(Box::new(expr), Operator::And, Box::new(tm)));
        // Jump: do not recurse into the wrapped tree — we'd re-match the
        // inner Eq/Like and produce `(x AND tm) AND tm` infinitely.
        Ok(Transformed::new(wrapped, true, TreeNodeRecursion::Jump))
    } else {
        Ok(Transformed::no(expr))
    }
}

/// If `expr` is a rewritable predicate on an indexed column, return
/// `(column_name, tantivy_query)`. Tantivy query syntax: `term` for exact,
/// `term*` for prefix.
fn match_indexed_predicate(expr: &Expr, indexed_columns: &HashSet<String>) -> Option<(String, String)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) => {
            let (col, lit) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), Expr::Literal(s, _)) => (c, s),
                (Expr::Literal(s, _), Expr::Column(c)) => (c, s),
                _ => return None,
            };
            if !indexed_columns.contains(&c_name(col)) {
                return None;
            }
            let s = extract_utf8_literal(lit)?;
            // Conservative: bail on any literal containing tantivy QueryParser
            // metachars. Correctness preserved because the original `=`
            // predicate stays in the plan.
            if !s.chars().all(is_tantivy_safe_term_char) || s.is_empty() {
                return None;
            }
            Some((c_name(col), tantivy_escape_term(&s)))
        }
        Expr::Like(Like {
            negated: false,
            expr: l,
            pattern: r,
            escape_char,
            case_insensitive: false,
        }) => {
            let Expr::Column(c) = l.as_ref() else { return None };
            if !indexed_columns.contains(&c_name(c)) {
                return None;
            }
            let Expr::Literal(s, _) = r.as_ref() else { return None };
            let pat = extract_utf8_literal(s)?;
            classify_like_pattern(&pat, *escape_char).map(|q| (c_name(c), q))
        }
        _ => None,
    }
}

fn c_name(c: &datafusion::common::Column) -> String {
    c.name.clone()
}

fn extract_utf8_literal(s: &ScalarValue) -> Option<String> {
    match s {
        ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Decide which Tantivy query form a SQL LIKE pattern maps to. Only handles:
/// - no wildcard:   `'foo'`          -> term `foo`
/// - trailing `%`:  `'foo%'`         -> prefix `foo*`
///
/// `_` (single-char wildcard) is treated as a non-supported pattern.
/// Embedded `%` (`'fo%o'`, `'%foo%'`) is non-supported.
fn classify_like_pattern(pat: &str, escape: Option<char>) -> Option<String> {
    let esc = escape.unwrap_or('\\');
    let mut out = String::new();
    let mut chars = pat.chars().peekable();
    let mut trailing_wildcard = false;
    let total_chars = pat.chars().count();
    let mut idx = 0;
    while let Some(c) = chars.next() {
        idx += 1;
        if c == esc {
            // Next char is literal.
            if let Some(&n) = chars.peek() {
                chars.next();
                idx += 1;
                if !is_tantivy_safe_term_char(n) {
                    return None;
                }
                out.push(n);
                continue;
            } else {
                return None; // trailing escape
            }
        }
        if c == '_' {
            return None;
        }
        if c == '%' {
            if idx == total_chars {
                trailing_wildcard = true;
            } else {
                return None; // leading or embedded %
            }
            continue;
        }
        if !is_tantivy_safe_term_char(c) {
            // Special chars that would confuse QueryParser; bail rather than
            // mis-escape.
            return None;
        }
        out.push(c);
    }
    if out.is_empty() {
        return None;
    }
    Some(if trailing_wildcard { format!("{}*", out) } else { out })
}

/// Conservative: only allow alnum, dot, dash, underscore, slash, colon,
/// `@`, and space. Tantivy QueryParser interprets many ASCII punctuation
/// chars (`+ - && || ! ( ) { } [ ] ^ " ~ * ? : \ /`) as syntax. If the
/// literal contains anything else, we leave the predicate alone (the
/// original `=` / `LIKE` still applies — correctness preserved).
fn is_tantivy_safe_term_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '.' | '-' | '_' | ' ' | '/' | '@')
}

fn tantivy_escape_term(s: &str) -> String {
    // For exact-term equality we pass the literal as-is when safe; otherwise
    // bail (caller already filtered). This keeps the query string simple and
    // matches the "raw" tokenizer used by indexed keyword columns.
    s.to_string()
}

fn text_match_call(column: String, query: String) -> Expr {
    Expr::ScalarFunction(ScalarFunction {
        func: text_match_udf_arc(),
        args: vec![Expr::Column(datafusion::common::Column::new_unqualified(column)), lit(query)],
    })
}

/// Cache the ScalarUDF Arc — analyzer rules run on every query.
fn text_match_udf_arc() -> Arc<ScalarUDF> {
    static CELL: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    CELL.get_or_init(|| Arc::new(ScalarUDF::from(TextMatchUdf::default()))).clone()
}

/// Walk down a plan tree to find a TableScan whose name matches an indexed
/// table. Stops at the first one (predicates above only see one scan in
/// practice; cross-table joins on indexed columns aren't supported in v1
/// — each filter is rewritten relative to its own subtree's scan).
fn find_indexed_table(plan: &LogicalPlan) -> Option<String> {
    let mut found = None;
    let _ = plan.apply(|p| {
        if let LogicalPlan::TableScan(ts) = p {
            let name = ts.table_name.table().to_string();
            if indexed_columns_for(&name).is_some_and(|cols| !cols.is_empty()) {
                found = Some(name);
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Indexed columns for a table from the static schema registry. Returns
/// `None` when the table isn't in the registry (custom/dynamic tables);
/// callers treat that as "skip rewrite" — correct behavior because we have
/// no schema to consult.
fn indexed_columns_for(table: &str) -> Option<HashSet<String>> {
    static CACHE: OnceLock<HashMap<String, HashSet<String>>> = OnceLock::new();
    let map = CACHE.get_or_init(|| {
        let mut m: HashMap<String, HashSet<String>> = HashMap::new();
        for name in crate::schema_loader::registry().list_tables() {
            if let Some(schema) = crate::schema_loader::registry().get(&name) {
                let cols: HashSet<String> = schema
                    .fields
                    .iter()
                    .filter(|f| f.tantivy.as_ref().is_some_and(|t| t.indexed))
                    .map(|f| f.name.clone())
                    .collect();
                if !cols.is_empty() {
                    m.insert(name, cols);
                }
            }
        }
        m
    });
    map.get(table).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn like_classifier_exact_no_wildcards() {
        assert_eq!(classify_like_pattern("foo", None), Some("foo".to_string()));
    }

    #[test]
    fn like_classifier_trailing_wildcard() {
        assert_eq!(classify_like_pattern("foo%", None), Some("foo*".to_string()));
    }

    #[test]
    fn like_classifier_leading_wildcard_unsupported() {
        assert_eq!(classify_like_pattern("%foo", None), None);
    }

    #[test]
    fn like_classifier_embedded_wildcard_unsupported() {
        assert_eq!(classify_like_pattern("fo%o", None), None);
    }

    #[test]
    fn like_classifier_underscore_unsupported() {
        assert_eq!(classify_like_pattern("fo_", None), None);
    }

    #[test]
    fn like_classifier_special_char_bails() {
        assert_eq!(classify_like_pattern("foo+bar", None), None);
    }

    #[test]
    fn like_classifier_safe_dots_dashes_allowed() {
        assert_eq!(classify_like_pattern("svc.user-api", None), Some("svc.user-api".to_string()));
    }

    #[test]
    fn like_classifier_escape_metachar_bails_conservatively() {
        // Escape produces a literal `%` in the SQL semantics — but `%` is
        // not in our safe-term char set, so we bail and the original LIKE
        // predicate still applies (correctness retained, perf opportunity
        // lost — acceptable for v1).
        assert_eq!(classify_like_pattern("foo\\%", Some('\\')), None);
    }

    #[test]
    fn match_indexed_eq_picks_up_known_column() {
        // Column name not in the registry → no rewrite.
        let cols = HashSet::from(["service_name".to_string()]);
        let e = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(datafusion::common::Column::new_unqualified("service_name"))),
            Operator::Eq,
            Box::new(lit("user-api")),
        ));
        let got = match_indexed_predicate(&e, &cols);
        assert_eq!(got, Some(("service_name".into(), "user-api".into())));

        let other = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(datafusion::common::Column::new_unqualified("other_col"))),
            Operator::Eq,
            Box::new(lit("x")),
        ));
        assert_eq!(match_indexed_predicate(&other, &cols), None);
    }
}
