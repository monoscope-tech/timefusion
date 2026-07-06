//! Transparent Tantivy acceleration for standard SQL predicates.
//!
//! Rewrites `col LIKE 'pattern'` and `col ILIKE 'pattern'` on tantivy-indexed
//! columns by **additively** AND-ing a `text_match(col, q)` call to the
//! predicate. Exact `col = 'lit'` on **raw-tokenized** columns is also routed
//! (gated by `tantivy.route_equality`, default on) — this is the lever for
//! high-cardinality id lookups (trace_id/span_id/id/parent_id) that bloom/stats
//! can't prune when row groups are coarse. `IN`-lists on raw columns route as
//! an OR of per-item `text_match` calls (same gates as `=`, capped at
//! `MAX_ROUTED_IN_LIST`). Correctness under `OR` is enforced by
//! `collect_text_match_tree`: an OR node is only routable when every branch
//! is completely covered by a `text_match`, and the original predicate is
//! always preserved as the post-filter backstop. `!=` / `NOT IN` are never
//! routed — negation has no term form.
//! The original comparison is never removed — it
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
//! | `col = 'lit'`         | ✅ term (route_equality) | ❌ (bloom/stats) | ❌ (bloom/stats) |
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
};

use crate::{
    optimizers::extract_utf8_string,
    tantivy_index::{
        schema::{DEFAULT_TOKENIZER, NGRAM3_TOKENIZER, RAW_TOKENIZER},
        udf::{NGRAM_MIN_QUERY_LEN, TEXT_MATCH_NAME, TextMatchUdf, classify_like_pattern, is_eq_term_safe},
    },
};

#[derive(Debug, Default)]
pub struct TantivyPredicateRewriter {
    /// Route exact `=` on raw columns through tantivy (`tantivy.route_equality`).
    /// Carried as a field rather than read from the global config singleton so
    /// the rule works under tests that build a `Database` from a local config
    /// (the singleton may be uninitialized → `config()` panics).
    route_equality: bool,
}

impl TantivyPredicateRewriter {
    pub fn new(route_equality: bool) -> Self {
        Self { route_equality }
    }
}

impl AnalyzerRule for TantivyPredicateRewriter {
    fn name(&self) -> &str {
        "tantivy_predicate_rewriter"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        if matches!(plan, LogicalPlan::Dml(_)) {
            return Ok(plan);
        }
        let allow_eq = self.route_equality;
        Ok(plan.transform_down(|p| rewrite_node(p, allow_eq))?.data)
    }
}

fn rewrite_node(plan: LogicalPlan, allow_eq: bool) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Filter(mut filter) => {
            let Some(table) = find_indexed_table(&filter.input) else {
                return Ok(Transformed::no(LogicalPlan::Filter(filter)));
            };
            let columns = match indexed_columns_for(&table) {
                Some(c) if !c.is_empty() => c,
                _ => return Ok(Transformed::no(LogicalPlan::Filter(filter))),
            };
            let new_pred = filter.predicate.clone().transform_down(|e| rewrite_expr(e, columns, allow_eq))?.data;
            filter.predicate = new_pred;
            Ok(Transformed::yes(LogicalPlan::Filter(filter)))
        }
        _ => Ok(Transformed::no(plan)),
    }
}

/// Longest IN-list we'll expand into an OR of `text_match` calls. Beyond
/// this the per-item query cost outweighs the pruning (and the selectivity
/// cutoff would likely reject the hit set anyway).
const MAX_ROUTED_IN_LIST: usize = 100;

fn rewrite_expr(expr: Expr, indexed_columns: &HashMap<String, &'static str>, allow_eq: bool) -> Result<Transformed<Expr>> {
    // Skip the children of a text_match call (already a tantivy predicate).
    if let Expr::ScalarFunction(sf) = &expr
        && sf.func.name() == TEXT_MATCH_NAME
    {
        return Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump));
    }
    if let Some((column, route)) = match_indexed_predicate(&expr, indexed_columns, allow_eq) {
        let tm = match route {
            Route::Ready(query) => text_match_call(column, query),
            Route::Deferred { rhs, kind } => text_match_deferred_call(column, rhs, kind),
        };
        let wrapped = Expr::BinaryExpr(BinaryExpr::new(Box::new(expr), Operator::And, Box::new(tm)));
        return Ok(Transformed::new(wrapped, true, TreeNodeRecursion::Jump));
    }
    if let Some((column, items)) = match_indexed_in_list(&expr, indexed_columns, allow_eq) {
        let ors = items
            .into_iter()
            .map(|route| match route {
                Route::Ready(q) => text_match_call(column.clone(), q),
                Route::Deferred { rhs, kind } => text_match_deferred_call(column.clone(), rhs, kind),
            })
            .reduce(|a, b| Expr::BinaryExpr(BinaryExpr::new(Box::new(a), Operator::Or, Box::new(b))))
            .expect("non-empty by construction");
        let wrapped = Expr::BinaryExpr(BinaryExpr::new(Box::new(expr), Operator::And, Box::new(ors)));
        return Ok(Transformed::new(wrapped, true, TreeNodeRecursion::Jump));
    }
    Ok(Transformed::no(expr))
}

/// `col IN ('a','b',...)` on a RAW-tokenized column → the per-item term
/// queries, under the same gates as exact `=` routing (raw tokenizer,
/// eq-term-safe literals, `route_equality` flag). Placeholder items defer to
/// scan-time classification. `NOT IN` is never routed.
fn match_indexed_in_list(expr: &Expr, indexed_columns: &HashMap<String, &'static str>, allow_eq: bool) -> Option<(String, Vec<Route>)> {
    use datafusion::logical_expr::expr::InList;
    let Expr::InList(InList { expr: col, list, negated: false }) = expr else {
        return None;
    };
    if !allow_eq || list.is_empty() || list.len() > MAX_ROUTED_IN_LIST {
        return None;
    }
    let Expr::Column(c) = col.as_ref() else { return None };
    let name = c_name(c);
    if *indexed_columns.get(&name)? != RAW_TOKENIZER {
        return None;
    }
    let items: Option<Vec<Route>> = list
        .iter()
        .map(|e| match e {
            Expr::Literal(s, _) => extract_utf8_string(s)
                .filter(|v| !v.is_empty() && v.chars().all(is_eq_term_safe))
                .map(Route::Ready),
            Expr::Placeholder(_) => Some(Route::Deferred {
                rhs:  e.clone(),
                kind: "eq".to_string(),
            }),
            _ => None,
        })
        .collect();
    items.map(|items| (name, items))
}

/// How a routed predicate reaches tantivy.
#[derive(Debug, PartialEq)]
enum Route {
    /// Literal classified at plan time → `text_match(col, query)`.
    Ready(String),
    /// `$N` placeholder — can't classify until parameter substitution.
    /// Emitted as `text_match(col, $N, kind)`; the scan-side collector runs
    /// `classify_deferred(kind, value)` once the literal is known and treats
    /// unclassifiable values as opaque (original predicate post-filters).
    Deferred { rhs: Expr, kind: String },
}

/// If `expr` is a rewritable predicate on an indexed column, return
/// `(column_name, route)`. Decision depends on the column's
/// tokenizer — raw can't do substring; ngram3 can do everything; default
/// is in between.
fn match_indexed_predicate(expr: &Expr, indexed_columns: &HashMap<String, &'static str>, allow_eq: bool) -> Option<(String, Route)> {
    match expr {
        // Exact `col = 'lit'` on a RAW-tokenized column: route as a term query.
        // Raw is a single case-sensitive token, so the tantivy match set equals
        // the `=` match set (the id-prefilter is exact, not just a superset).
        // Safe under OR — `collect_text_match_tree` only routes an OR when
        // every branch is completely covered; otherwise the subtree is opaque
        // and the preserved `=` post-filters. `!=` is never routed (no term
        // form). Gated by `route_equality` for instant rollback.
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) if allow_eq => {
            let (c, rhs) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), other) | (other, Expr::Column(c)) => (c, other),
                _ => return None,
            };
            let name = c_name(c);
            if *indexed_columns.get(&name)? != RAW_TOKENIZER {
                return None; // only exact-match (raw) columns; ngram3/default are lossy for `=`
            }
            match rhs {
                Expr::Literal(s, _) => {
                    let v = extract_utf8_string(s)?;
                    // Bail on empty or QueryParser-unsafe literals (the `=` still applies).
                    if v.is_empty() || !v.chars().all(is_eq_term_safe) {
                        return None;
                    }
                    Some((name, Route::Ready(v)))
                }
                // Prepared-statement path: value unknown until Bind. Route
                // with a deferred tag so plans cached with placeholders keep
                // the prefilter (classified at scan time post-substitution).
                Expr::Placeholder(_) => Some((
                    name,
                    Route::Deferred {
                        rhs:  rhs.clone(),
                        kind: "eq".to_string(),
                    },
                )),
                _ => None,
            }
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
            match r.as_ref() {
                Expr::Literal(s, _) => {
                    let pat = extract_utf8_string(s)?;
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
                    Some((c_name(c), Route::Ready(q)))
                }
                // Pattern arrives at Bind: defer classification. Custom escape
                // chars aren't carried in the tag — don't route them.
                Expr::Placeholder(_) if escape_char.is_none() => Some((
                    c_name(c),
                    Route::Deferred {
                        rhs:  r.as_ref().clone(),
                        kind: format!("{}:{tok}", if *case_insensitive { "ilike" } else { "like" }),
                    },
                )),
                _ => None,
            }
        }
        _ => None,
    }
}

fn c_name(c: &datafusion::common::Column) -> String {
    c.name.clone()
}

fn text_match_call(column: String, query: String) -> Expr {
    Expr::ScalarFunction(ScalarFunction {
        func: text_match_udf_arc(),
        args: vec![Expr::Column(datafusion::common::Column::new_unqualified(column)), lit(query)],
    })
}

/// Deferred form: the value expr is a `$N` placeholder; `kind` tells the
/// scan-side collector how to classify it after substitution.
fn text_match_deferred_call(column: String, rhs: Expr, kind: String) -> Expr {
    Expr::ScalarFunction(ScalarFunction {
        func: text_match_udf_arc(),
        args: vec![Expr::Column(datafusion::common::Column::new_unqualified(column)), rhs, lit(kind)],
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
fn indexed_columns_for(table: &str) -> Option<&'static HashMap<String, &'static str>> {
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
    map.get(table)
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

    fn eq(col: &str, val: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(datafusion::common::Column::new_unqualified(col))),
            Operator::Eq,
            Box::new(lit(val)),
        ))
    }

    #[test]
    fn match_eq_routed_on_raw_when_enabled() {
        // P0: exact `=` on a raw column is routed as a term query when the flag
        // is on — the high-card trace/span lookup acceleration.
        let cols: HashMap<String, &'static str> = HashMap::from([("context___trace_id".to_string(), RAW_TOKENIZER)]);
        assert_eq!(
            match_indexed_predicate(&eq("context___trace_id", "d01762b88f4ed54d"), &cols, true),
            Some(("context___trace_id".into(), Route::Ready("d01762b88f4ed54d".into())))
        );
    }

    #[test]
    fn match_eq_not_routed_when_flag_off() {
        // route_equality=false reverts to bloom/stats-only (the prior behavior).
        let cols: HashMap<String, &'static str> = HashMap::from([("context___trace_id".to_string(), RAW_TOKENIZER)]);
        assert_eq!(match_indexed_predicate(&eq("context___trace_id", "abc123"), &cols, false), None);
    }

    #[test]
    fn match_eq_not_routed_on_ngram3_or_neq() {
        // `=` only routes on raw (exact) tokenizers; ngram3 is lossy for equality.
        let cols: HashMap<String, &'static str> = HashMap::from([("name".to_string(), NGRAM3_TOKENIZER), ("tid".to_string(), RAW_TOKENIZER)]);
        assert_eq!(match_indexed_predicate(&eq("name", "runServer"), &cols, true), None);
        // `!=` is never routed (no term form), regardless of the flag.
        let neq = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(datafusion::common::Column::new_unqualified("tid"))),
            Operator::NotEq,
            Box::new(lit("abc")),
        ));
        assert_eq!(match_indexed_predicate(&neq, &cols, true), None);
    }

    #[test]
    fn match_eq_unsafe_literal_bails() {
        // Literals QueryParser would mis-handle against a single raw token are
        // left to the plain `=` (correctness over acceleration).
        let cols: HashMap<String, &'static str> = HashMap::from([("tid".to_string(), RAW_TOKENIZER)]);
        assert_eq!(match_indexed_predicate(&eq("tid", "a:b"), &cols, true), None, "colon is query syntax");
        assert_eq!(
            match_indexed_predicate(&eq("tid", "foo bar"), &cols, true),
            None,
            "space → AND-split can't match one raw token"
        );
        assert_eq!(match_indexed_predicate(&eq("tid", "a.b"), &cols, true), None, "dot conservatively excluded");
        assert_eq!(match_indexed_predicate(&eq("tid", ""), &cols, true), None, "empty");
        // A dashed UUID IS allowed — the embedded `-` survives (e2e-proven).
        let uid = "0fee13b9-ac71-5c55-acd1-109542595054";
        assert_eq!(match_indexed_predicate(&eq("tid", uid), &cols, true), Some(("tid".into(), Route::Ready(uid.into()))));
    }

    #[test]
    fn or_of_routed_eqs_becomes_or_node_but_partial_or_is_opaque() {
        // End-to-end OR-safety with the tree collector: a disjunction where
        // BOTH branches are rewritten routes as an Or node (union — new
        // capability); a disjunction with one unroutable branch must yield NO
        // prefilter at all (else the 2026-06-16 empty/partial-union bug
        // returns). A top-level conjunct still routes.
        use crate::tantivy_index::udf::{PredNode, collect_text_match_tree};
        let cols: HashMap<String, &'static str> = HashMap::from([("tid".to_string(), RAW_TOKENIZER), ("sid".to_string(), RAW_TOKENIZER)]);

        let or_pred = Expr::BinaryExpr(BinaryExpr::new(Box::new(eq("tid", "x")), Operator::Or, Box::new(eq("sid", "y"))));
        let rewritten = or_pred.transform_down(|e| rewrite_expr(e, &cols, true)).unwrap().data;
        match collect_text_match_tree(&[rewritten]) {
            Some(PredNode::Or(kids)) => assert_eq!(kids.len(), 2, "both routed branches must union"),
            other => panic!("expected Or node, got {other:?}"),
        }

        // One branch on an UN-indexed column → whole OR must be opaque.
        let cols_partial: HashMap<String, &'static str> = HashMap::from([("tid".to_string(), RAW_TOKENIZER)]);
        let or_pred = Expr::BinaryExpr(BinaryExpr::new(Box::new(eq("tid", "x")), Operator::Or, Box::new(eq("unindexed", "y"))));
        let rewritten = or_pred.transform_down(|e| rewrite_expr(e, &cols_partial, true)).unwrap().data;
        assert_eq!(
            collect_text_match_tree(&[rewritten]),
            None,
            "an OR with an unroutable branch must not seed the prefilter"
        );

        let and_pred = Expr::BinaryExpr(BinaryExpr::new(Box::new(eq("tid", "x")), Operator::And, Box::new(lit(true))));
        let rewritten = and_pred.transform_down(|e| rewrite_expr(e, &cols, true)).unwrap().data;
        match collect_text_match_tree(&[rewritten]) {
            Some(PredNode::Leaf(p)) => {
                assert_eq!(p.column, "tid");
                assert_eq!(p.query, "x");
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn in_list_routes_as_or_of_terms() {
        let cols: HashMap<String, &'static str> = HashMap::from([("tid".to_string(), RAW_TOKENIZER), ("name".to_string(), NGRAM3_TOKENIZER)]);
        let in_list = |col: &str, items: &[&str], negated: bool| {
            Expr::InList(datafusion::logical_expr::expr::InList {
                expr: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(col))),
                list: items.iter().map(|s| lit(*s)).collect(),
                negated,
            })
        };
        // Routable IN-list → collector sees an Or of leaves (complete via the
        // AND with the preserved original).
        use crate::tantivy_index::udf::{PredNode, collect_text_match_tree};
        let rewritten = in_list("tid", &["a", "b"], false).transform_down(|e| rewrite_expr(e, &cols, true)).unwrap().data;
        match collect_text_match_tree(&[rewritten]) {
            Some(PredNode::Or(kids)) => assert_eq!(kids.len(), 2),
            other => panic!("expected Or of 2 leaves, got {other:?}"),
        }
        // NOT IN, ngram3 column, unsafe literal, flag off → never routed.
        for (e, allow) in [
            (in_list("tid", &["a"], true), true),
            (in_list("name", &["abc"], false), true),
            (in_list("tid", &["a:b"], false), true),
            (in_list("tid", &["a"], false), false),
        ] {
            let rewritten = e.transform_down(|ex| rewrite_expr(ex, &cols, allow)).unwrap().data;
            assert_eq!(collect_text_match_tree(&[rewritten]), None);
        }
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
        assert_eq!(match_indexed_predicate(&e, &cols, true), None);
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
        assert_eq!(match_indexed_predicate(&e, &cols, true), Some(("c".into(), Route::Ready("foo".into()))));
    }
}
