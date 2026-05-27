//! Transparent Tantivy acceleration for standard SQL predicates.
//!
//! Rewrites `col = 'literal'`, `col LIKE 'pattern'`, and `col ILIKE 'pattern'`
//! on tantivy-indexed columns by **additively** AND-ing a `text_match(col, q)`
//! call to the predicate. The original comparison is never removed — it
//! still applies as a post-filter on MemBuffer rows and Delta files whose
//! tantivy index hasn't built yet (post-flush lag). The `text_match` call,
//! once picked up by the existing routing logic in `ProjectRoutingTable`,
//! produces an `id IN (...)` prefilter that narrows the Delta scan.
//!
//! Correctness invariants:
//! 1. The original predicate is preserved verbatim in the plan.
//! 2. Only rewrite predicates on columns confirmed `tantivy.indexed: true`.
//! 3. Idempotent under repeated passes.
//! 4. Patterns the *target column's tokenizer* can't accelerate are left
//!    alone (correctness preserved via the original predicate).
//!
//! Patterns by tokenizer:
//!
//! | SQL form              | raw   | default | ngram3 |
//! |-----------------------|-------|---------|--------|
//! | `col = 'lit'`         | ✅ exact | ✅ exact | ✅ exact (case-insens via ngram lowercaser; Delta `=` re-filters case) |
//! | `col LIKE 'lit'`      | ✅    | ✅      | ✅      |
//! | `col LIKE 'pre%'`     | ✅ prefix | ✅ prefix | ✅ prefix |
//! | `col LIKE '%suf'`     | ❌    | ❌      | ✅ via ngram |
//! | `col LIKE '%mid%'`    | ❌    | ❌      | ✅ via ngram |
//! | `col ILIKE 'lit'`     | ❌    | ✅ (lowercased literal) | ✅ |
//! | `col ILIKE '%mid%'`   | ❌    | ❌      | ✅ |
//!
//! `_` (single-char wildcard) is never accelerated — semantics don't map
//! cleanly to any tantivy primitive. Strings shorter than 3 chars on
//! ngram3 columns fall through (no full trigram available).

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use datafusion::{
    common::{
        Result,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    config::ConfigOptions,
    logical_expr::{
        BinaryExpr, Expr, LogicalPlan, Operator, ScalarUDF,
        expr::{Like, ScalarFunction},
        lit,
    },
    optimizer::AnalyzerRule,
    scalar::ScalarValue,
};

use crate::tantivy_index::{
    schema::{DEFAULT_TOKENIZER, NGRAM3_TOKENIZER, RAW_TOKENIZER},
    udf::{TEXT_MATCH_NAME, TextMatchUdf},
};

/// Minimum literal length we'll accelerate on ngram3. Tantivy's 3-gram
/// tokenizer produces no tokens for inputs shorter than `n` characters, so
/// a 2-char query would match every doc (degenerate) — bail to scan.
const NGRAM_MIN_QUERY_LEN: usize = 3;

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
        Ok(plan.transform_down(rewrite_node)?.data)
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

fn rewrite_expr(expr: Expr, indexed_columns: &HashMap<String, &'static str>) -> Result<Transformed<Expr>> {
    // Skip the children of a text_match call (already a tantivy predicate).
    if let Expr::ScalarFunction(sf) = &expr
        && sf.func.name() == TEXT_MATCH_NAME
    {
        return Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump));
    }
    if let Some((column, query)) = match_indexed_predicate(&expr, indexed_columns) {
        let tm = text_match_call(column, query);
        let wrapped = Expr::BinaryExpr(BinaryExpr::new(Box::new(expr), Operator::And, Box::new(tm)));
        Ok(Transformed::new(wrapped, true, TreeNodeRecursion::Jump))
    } else {
        Ok(Transformed::no(expr))
    }
}

/// If `expr` is a rewritable predicate on an indexed column, return
/// `(column_name, tantivy_query)`. Decision depends on the column's
/// tokenizer — raw can't do substring; ngram3 can do everything; default
/// is in between.
fn match_indexed_predicate(expr: &Expr, indexed_columns: &HashMap<String, &'static str>) -> Option<(String, String)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) => {
            let (col, lit) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), Expr::Literal(s, _)) => (c, s),
                (Expr::Literal(s, _), Expr::Column(c)) => (c, s),
                _ => return None,
            };
            let tok = *indexed_columns.get(&c_name(col))?;
            let s = extract_utf8_literal(lit)?;
            // Raw and default tokenizers want safe-char terms (raw is
            // single-token, default does word split — both struggle with
            // QueryParser metachars). ngram3 sees the literal char-by-char
            // and lowercases, so we still gate on safe chars to keep the
            // injected `text_match` UDF call simple.
            if !s.chars().all(is_tantivy_safe_term_char) || s.is_empty() {
                return None;
            }
            // Skip ngram3 acceleration for sub-3-char literals (no
            // valid trigram → tantivy returns everything).
            if tok == NGRAM3_TOKENIZER && s.chars().count() < NGRAM_MIN_QUERY_LEN {
                return None;
            }
            Some((c_name(col), tantivy_escape_term(&s)))
        }
        Expr::Like(Like {
            negated: false,
            expr: l,
            pattern: r,
            escape_char,
            case_insensitive,
        }) => {
            let Expr::Column(c) = l.as_ref() else { return None };
            let tok = *indexed_columns.get(&c_name(c))?;
            // ILIKE on raw (case-sensitive single token) is not accelerable
            // without a parallel case-insensitive index — skip.
            if *case_insensitive && tok == RAW_TOKENIZER {
                return None;
            }
            let Expr::Literal(s, _) = r.as_ref() else { return None };
            let pat = extract_utf8_literal(s)?;
            let allow_substring = tok == NGRAM3_TOKENIZER;
            let q = classify_like_pattern(&pat, *escape_char, allow_substring)?;
            // ngram3 tokenizer lowercases on both index and query side, so
            // ILIKE comes for free. For "default" tokenizer (also lowercased)
            // the query parser also lowercases. So no extra work needed —
            // case sensitivity is already lost in the prefilter, and the
            // original LIKE/ILIKE predicate re-runs on the Delta side with
            // correct semantics.
            if tok == NGRAM3_TOKENIZER && q.chars().filter(|c| *c != '*').count() < NGRAM_MIN_QUERY_LEN {
                return None;
            }
            Some((c_name(c), q))
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

/// Decide which Tantivy query form a SQL LIKE pattern maps to.
///
/// `allow_substring=false` (raw/default tokenizer):
///   - `'foo'`     → term `foo`
///   - `'foo%'`    → prefix `foo*`
///   - `'%foo'`, `'%foo%'`, embedded `%` → unsupported (None)
///
/// `allow_substring=true` (ngram3 tokenizer):
///   - `'foo'`     → term `foo`
///   - `'foo%'`    → prefix `foo*`
///   - `'%foo'`    → term `foo`  (n-gram match by tantivy)
///   - `'%foo%'`   → term `foo`
///   - Embedded `%` between literal chars (e.g. `'a%b'`) → unsupported
///
/// `_` (single-char wildcard) is never accelerable. Returns None.
fn classify_like_pattern(pat: &str, escape: Option<char>, allow_substring: bool) -> Option<String> {
    let esc = escape.unwrap_or('\\');
    let chars: Vec<char> = pat.chars().collect();
    let total = chars.len();
    if total == 0 {
        return None;
    }
    let mut out = String::new();
    let mut i = 0;
    let mut leading_wildcard = false;
    let mut trailing_wildcard = false;
    // Detect leading %
    if chars[0] == '%' {
        leading_wildcard = true;
        i = 1;
    }
    while i < total {
        let c = chars[i];
        if c == esc {
            // Next char is literal.
            i += 1;
            if i >= total {
                return None; // trailing escape
            }
            let n = chars[i];
            if !is_tantivy_safe_term_char(n) {
                return None;
            }
            out.push(n);
            i += 1;
            continue;
        }
        if c == '_' {
            return None;
        }
        if c == '%' {
            if i + 1 == total {
                trailing_wildcard = true;
                break;
            }
            // Embedded %: only the leading-or-trailing-only forms are
            // handled here. `'a%b'` would need positional ranking that
            // tantivy can't trivially give us. Bail.
            return None;
        }
        if !is_tantivy_safe_term_char(c) {
            return None;
        }
        out.push(c);
        i += 1;
    }
    if out.is_empty() {
        return None;
    }
    Some(match (leading_wildcard, trailing_wildcard) {
        // Plain exact / prefix / suffix / infix matches.
        (false, false) => out,                // 'foo'
        (false, true) => format!("{}*", out), // 'foo%' (prefix)
        // Suffix-only and infix forms only meaningful on ngram3; for raw/
        // default tokenizers we'd be sending tantivy a query that matches
        // the substring as a whole token (it won't). Bail.
        (true, false) | (true, true) if !allow_substring => return None,
        (true, _) => out, // ngram3 will trigram-match the substring
    })
}

/// Conservative: only allow alnum, dot, dash, underscore, slash, `@`, and
/// space. Colon is deliberately *excluded* — Tantivy QueryParser treats it as
/// field-delimiter syntax. The QueryParser also interprets many other ASCII
/// punctuation chars (`+ - && || ! ( ) { } [ ] ^ " ~ * ? : \\ /`) as syntax;
/// if the literal contains anything outside our allowlist we leave the
/// predicate alone (the original `=` / `LIKE` still applies — correctness
/// preserved).
///
/// Note: space is treated by the QueryParser as an implicit `AND` between
/// terms, so `'foo bar'` matches docs containing both `foo` and `bar`, not
/// the phrase. Acceptable here because `text_match` is additive — the
/// original `=` / `LIKE` re-filters as the correctness backstop.
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

/// Indexed columns for a table from the static schema registry — keyed by
/// column name, value is the resolved tokenizer (raw/default/ngram3).
/// Returns `None` when the table isn't in the registry.
///
/// The cache is populated *once* on first call. This is safe because
/// `schema_loader::registry()` is compiled-in YAML and immutable. If we ever
/// add runtime/hot-reload of schemas, this OnceLock must be replaced with an
/// invalidatable structure — newly-added Tantivy-indexed tables would
/// otherwise silently never accelerate.
fn indexed_columns_for(table: &str) -> Option<HashMap<String, &'static str>> {
    static CACHE: OnceLock<HashMap<String, HashMap<String, &'static str>>> = OnceLock::new();
    let map = CACHE.get_or_init(|| {
        let mut m: HashMap<String, HashMap<String, &'static str>> = HashMap::new();
        for name in crate::schema_loader::registry().list_tables() {
            if let Some(schema) = crate::schema_loader::registry().get(&name) {
                let cols: HashMap<String, &'static str> = schema
                    .fields
                    .iter()
                    .filter_map(|f| {
                        let cfg = f.tantivy.as_ref()?;
                        if !cfg.indexed {
                            return None;
                        }
                        let tok = match cfg.tokenizer.as_deref().unwrap_or(NGRAM3_TOKENIZER) {
                            RAW_TOKENIZER => RAW_TOKENIZER,
                            DEFAULT_TOKENIZER => DEFAULT_TOKENIZER,
                            _ => NGRAM3_TOKENIZER,
                        };
                        Some((f.name.clone(), tok))
                    })
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
        assert_eq!(classify_like_pattern("foo", None, false), Some("foo".to_string()));
    }

    #[test]
    fn like_classifier_trailing_wildcard() {
        assert_eq!(classify_like_pattern("foo%", None, false), Some("foo*".to_string()));
    }

    #[test]
    fn like_classifier_leading_wildcard_unsupported_on_raw() {
        assert_eq!(classify_like_pattern("%foo", None, false), None);
    }

    #[test]
    fn like_classifier_leading_wildcard_supported_on_ngram3() {
        assert_eq!(classify_like_pattern("%foo", None, true), Some("foo".to_string()));
    }

    #[test]
    fn like_classifier_infix_supported_on_ngram3() {
        assert_eq!(classify_like_pattern("%foo%", None, true), Some("foo".to_string()));
    }

    #[test]
    fn like_classifier_infix_unsupported_on_raw() {
        assert_eq!(classify_like_pattern("%foo%", None, false), None);
    }

    #[test]
    fn like_classifier_embedded_percent_unsupported() {
        assert_eq!(classify_like_pattern("fo%o", None, true), None);
        assert_eq!(classify_like_pattern("fo%o", None, false), None);
    }

    #[test]
    fn like_classifier_underscore_unsupported() {
        assert_eq!(classify_like_pattern("fo_", None, true), None);
    }

    #[test]
    fn like_classifier_special_char_bails() {
        assert_eq!(classify_like_pattern("foo+bar", None, true), None);
    }

    #[test]
    fn like_classifier_safe_dots_dashes_allowed() {
        assert_eq!(classify_like_pattern("svc.user-api", None, false), Some("svc.user-api".to_string()));
    }

    #[test]
    fn like_classifier_escape_metachar_bails_conservatively() {
        assert_eq!(classify_like_pattern("foo\\%", Some('\\'), false), None);
    }

    #[test]
    fn match_indexed_eq_picks_up_known_column() {
        let cols: HashMap<String, &'static str> = HashMap::from([("service_name".to_string(), RAW_TOKENIZER)]);
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

    #[test]
    fn match_eq_skips_short_literals_on_ngram3() {
        // Sub-3-char literal on an ngram3 column has no full trigram; bail
        // to avoid a tantivy match-everything degenerate query.
        let cols: HashMap<String, &'static str> = HashMap::from([("c".to_string(), NGRAM3_TOKENIZER)]);
        let e = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(datafusion::common::Column::new_unqualified("c"))),
            Operator::Eq,
            Box::new(lit("ab")),
        ));
        assert_eq!(match_indexed_predicate(&e, &cols), None);
    }

    #[test]
    fn match_ilike_skipped_on_raw_columns() {
        // ILIKE on a raw-tokenized (case-sensitive) column would silently
        // miss case variants; skip the rewrite.
        let cols: HashMap<String, &'static str> = HashMap::from([("c".to_string(), RAW_TOKENIZER)]);
        let e = Expr::Like(Like {
            negated:          false,
            expr:             Box::new(Expr::Column(datafusion::common::Column::new_unqualified("c"))),
            pattern:          Box::new(lit("foo")),
            escape_char:      None,
            case_insensitive: true,
        });
        assert_eq!(match_indexed_predicate(&e, &cols), None);
    }

    #[test]
    fn match_ilike_substring_works_on_ngram3() {
        let cols: HashMap<String, &'static str> = HashMap::from([("c".to_string(), NGRAM3_TOKENIZER)]);
        let e = Expr::Like(Like {
            negated:          false,
            expr:             Box::new(Expr::Column(datafusion::common::Column::new_unqualified("c"))),
            pattern:          Box::new(lit("%foo%")),
            escape_char:      None,
            case_insensitive: true,
        });
        assert_eq!(match_indexed_predicate(&e, &cols), Some(("c".into(), "foo".into())));
    }
}
