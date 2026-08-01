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
//! routed — negation has no term form. The original comparison is never
//! removed — it still applies as a post-filter on MemBuffer rows and Delta
//! files whose tantivy index hasn't built yet (post-flush lag). The
//! `text_match` call, once picked up by the existing routing logic in
//! `ProjectRoutingTable`, produces an `id IN (...)` prefilter that narrows
//! the Delta scan.
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
//! | `col::text ~* 'sub'`  | ❌    | ❌      | ✅ via ngram |
//! | `col::text ~ 'sub'`   | ❌    | ❌      | ✅ via ngram |
//!
//! `~`/`~*` route ONLY when the pattern is a plain literal substring
//! (`regex_literal_substring`) and the column is text-typed — this is the
//! shape monoscope renders KQL `has`/`contains` into (`subject::text ~*
//! escapeRegex(term)`). Anchored (`^`/`$`, i.e. startswith/endswith) and any
//! other regex feature is left alone. Variant/List columns are excluded: the
//! index holds our canonical rendering, not SQL's `::text` cast output.
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
        Column, Result,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    config::ConfigOptions,
    logical_expr::{
        BinaryExpr, Expr, LogicalPlan, Operator, ScalarUDF, and,
        expr::{InList, Like, ScalarFunction},
        lit, or,
    },
    optimizer::AnalyzerRule,
};

use crate::{
    optimizers::extract_utf8_string,
    tantivy_index::{
        schema::{DEFAULT_TOKENIZER, NGRAM3_TOKENIZER, RAW_TOKENIZER},
        udf::{NGRAM_MIN_QUERY_LEN, TEXT_MATCH_NAME, TextMatchUdf, classify_like_pattern, is_eq_term_safe, regex_literal_substring},
    },
};

/// Per-column index facts the rewriter needs: the resolved tokenizer, and
/// whether the *stored* column is a plain string. The latter gates regex
/// routing: on a Variant/List column the tantivy index holds our own
/// canonical text rendering (`builder::variant_to_text`), which need not be
/// byte-identical to what a SQL `col::text` cast produces — routing there
/// could drop rows, so only text-typed columns are eligible.
type IndexedCol = (&'static str, bool);
type IndexedCols = HashMap<String, IndexedCol>;

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
        match plan {
            LogicalPlan::Dml(_) => Ok(plan),
            plan => Ok(plan.transform_down(|p| rewrite_node(p, self.route_equality))?.data),
        }
    }
}

fn rewrite_node(plan: LogicalPlan, allow_eq: bool) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Filter(mut filter) => match scanned_indexed_columns(&filter.input) {
            None => Ok(Transformed::no(LogicalPlan::Filter(filter))),
            Some(columns) => {
                let rewritten = std::mem::replace(&mut filter.predicate, lit(true)).transform_down(|e| rewrite_expr(e, columns, allow_eq))?;
                filter.predicate = rewritten.data;
                // The expr traversal's `tnr` (possibly `Jump`) must not leak into the plan walk.
                Ok(Transformed::new(LogicalPlan::Filter(filter), rewritten.transformed, TreeNodeRecursion::Continue))
            }
        },
        _ => Ok(Transformed::no(plan)),
    }
}

/// Longest IN-list we'll expand into an OR of `text_match` calls. Beyond
/// this the per-item query cost outweighs the pruning (and the selectivity
/// cutoff would likely reject the hit set anyway).
const MAX_ROUTED_IN_LIST: usize = 100;

fn rewrite_expr(expr: Expr, indexed_columns: &IndexedCols, allow_eq: bool) -> Result<Transformed<Expr>> {
    // Skip the children of a text_match call (already a tantivy predicate).
    if matches!(&expr, Expr::ScalarFunction(sf) if sf.func.name() == TEXT_MATCH_NAME) {
        return Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump));
    }
    let tantivy = match_indexed_predicate(&expr, indexed_columns, allow_eq).map(|(column, route)| route.into_call(column)).or_else(|| {
        match_indexed_in_list(&expr, indexed_columns, allow_eq).and_then(|(column, items)| items.into_iter().map(|r| r.into_call(column.clone())).reduce(or))
    });
    Ok(match tantivy {
        Some(tm) => Transformed::new(and(expr, tm), true, TreeNodeRecursion::Jump),
        None => Transformed::no(expr),
    })
}

/// `col IN ('a','b',...)` on a RAW-tokenized column → the per-item term
/// queries, under the same gates as exact `=` routing (raw tokenizer,
/// eq-term-safe literals, `route_equality` flag). Placeholder items defer to
/// scan-time classification. `NOT IN` is never routed.
fn match_indexed_in_list(expr: &Expr, indexed_columns: &IndexedCols, allow_eq: bool) -> Option<(String, Vec<Route>)> {
    let Expr::InList(InList { expr: col, list, negated: false }) = expr else {
        return None;
    };
    if !allow_eq || list.is_empty() || list.len() > MAX_ROUTED_IN_LIST {
        return None;
    }
    let Expr::Column(c) = col.as_ref() else { return None };
    if indexed_columns.get(&c.name)?.0 != RAW_TOKENIZER {
        return None;
    }
    list.iter().map(eq_term_route).collect::<Option<Vec<_>>>().map(|items| (c.name.clone(), items))
}

/// RHS of a raw-column equality (`=` or one `IN` item) as a route: literals are
/// classified now (bailing on empty / QueryParser-unsafe values, where the
/// preserved `=` still applies), placeholders defer to scan time.
fn eq_term_route(rhs: &Expr) -> Option<Route> {
    match rhs {
        Expr::Literal(s, _) => extract_utf8_string(s).filter(|v| !v.is_empty() && v.chars().all(is_eq_term_safe)).map(Route::Ready),
        // Prepared-statement path: value unknown until Bind. Route with a
        // deferred tag so plans cached with placeholders keep the prefilter.
        Expr::Placeholder(_) => Some(Route::Deferred { rhs: rhs.clone(), kind: "eq".into() }),
        _ => None,
    }
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

impl Route {
    /// `text_match(col, query)` — or `text_match(col, $N, kind)` when deferred.
    fn into_call(self, column: String) -> Expr {
        // Cached Arc — analyzer rules run on every query.
        static CELL: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
        let col = Expr::Column(Column::new_unqualified(column));
        let args = match self {
            Route::Ready(query) => vec![col, lit(query)],
            Route::Deferred { rhs, kind } => vec![col, rhs, lit(kind)],
        };
        Expr::ScalarFunction(ScalarFunction { func: CELL.get_or_init(|| Arc::new(ScalarUDF::from(TextMatchUdf::default()))).clone(), args })
    }
}

/// If `expr` is a rewritable predicate on an indexed column, return
/// `(column_name, route)`. Decision depends on the column's
/// tokenizer — raw can't do substring; ngram3 can do everything; default
/// is in between.
fn match_indexed_predicate(expr: &Expr, indexed_columns: &IndexedCols, allow_eq: bool) -> Option<(String, Route)> {
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
            if indexed_columns.get(&c.name)?.0 != RAW_TOKENIZER {
                return None; // only exact-match (raw) columns; ngram3/default are lossy for `=`
            }
            Some((c.name.clone(), eq_term_route(rhs)?))
        }
        Expr::Like(Like { negated: false, expr: l, pattern: r, escape_char, case_insensitive }) => {
            let Expr::Column(c) = l.as_ref() else { return None };
            let tok = indexed_columns.get(&c.name)?.0;
            // ILIKE on raw (case-sensitive single token) is not accelerable
            // without a parallel case-insensitive index — skip.
            if *case_insensitive && tok == RAW_TOKENIZER {
                return None;
            }
            // Both tokenizers that reach here lowercase on index and query side,
            // so ILIKE needs no extra work — case sensitivity is already lost in
            // the prefilter and the preserved LIKE/ILIKE re-runs with correct
            // semantics on the Delta side.
            let route = match r.as_ref() {
                Expr::Literal(s, _) => classify_like_pattern(&extract_utf8_string(s)?, *escape_char, tok == NGRAM3_TOKENIZER)
                    // ngram3 needs a full trigram to match anything.
                    .filter(|q| tok != NGRAM3_TOKENIZER || q.chars().filter(|c| *c != '*').count() >= NGRAM_MIN_QUERY_LEN)
                    .map(Route::Ready)?,
                // Pattern arrives at Bind: defer classification. Custom escape
                // chars aren't carried in the tag — don't route them.
                Expr::Placeholder(_) if escape_char.is_none() => {
                    Route::Deferred { rhs: r.as_ref().clone(), kind: format!("{}:{tok}", if *case_insensitive { "ilike" } else { "like" }) }
                }
                _ => return None,
            };
            Some((c.name.clone(), route))
        }
        // `col ~* 'substr'` / `col ~ 'substr'`, optionally through a
        // `CAST(col AS Utf8)` — the shape monoscope renders every KQL
        // has/contains into (`subject::text ~* escapeRegex(term)`). Routed as
        // the ngram3 substring query, i.e. identical to `col ILIKE '%substr%'`.
        //
        // Superset argument (what the `id IN (hits)` prefilter requires):
        // ngram3 lowercases + ASCII-folds both index and query side, so its
        // hit set is the case- and diacritic-insensitive substring match —
        // a superset of both `~` (case-sensitive) and `~*`. The original
        // regex is preserved as the post-filter, so results are unchanged.
        // Only PLAIN substrings route (see `regex_literal_substring`);
        // anchors and any other regex feature fall through untouched.
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::RegexMatch | Operator::RegexIMatch, right }) => {
            let c = column_through_string_cast(left)?;
            let (tok, text_typed) = *indexed_columns.get(&c.name)?;
            let Expr::Literal(s, _) = right.as_ref() else { return None };
            (tok == NGRAM3_TOKENIZER && text_typed)
                .then(|| regex_literal_substring(&extract_utf8_string(s)?).filter(|q| q.chars().count() >= NGRAM_MIN_QUERY_LEN))
                .flatten()
                .map(|q| (c.name.clone(), Route::Ready(q)))
        }
        _ => None,
    }
}

/// The column under zero or more string casts. Monoscope emits `col::text`,
/// which DataFusion may keep as a `Cast`/`TryCast` when the column is already
/// Utf8-ish; the cast is value-preserving for string types, so seeing through
/// it is safe (non-string sources are rejected by the `text_typed` gate).
fn column_through_string_cast(e: &Expr) -> Option<&Column> {
    use arrow::datatypes::DataType;
    use datafusion::logical_expr::expr::{Cast, TryCast};
    match e {
        Expr::Column(c) => Some(c),
        Expr::Cast(Cast { expr, field }) | Expr::TryCast(TryCast { expr, field })
            if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) =>
        {
            column_through_string_cast(expr)
        }
        _ => None,
    }
}

/// Indexed columns of the first TableScan below `plan` that has a tantivy
/// index. Stops at the first one (predicates above only see one scan in
/// practice; cross-table joins on indexed columns aren't supported in v1
/// — each filter is rewritten relative to its own subtree's scan).
fn scanned_indexed_columns(plan: &LogicalPlan) -> Option<&'static IndexedCols> {
    // `mut` forced: TreeNode::apply's visitor is FnMut(&Self) -> Result<TreeNodeRecursion>,
    // with no fold/accumulator variant able to return the found value directly.
    let mut found = None;
    let _ = plan.apply(|p| {
        Ok(match p {
            LogicalPlan::TableScan(ts) => match indexed_columns_for(ts.table_name.table()) {
                Some(cols) => {
                    found = Some(cols);
                    TreeNodeRecursion::Stop
                }
                None => TreeNodeRecursion::Continue,
            },
            _ => TreeNodeRecursion::Continue,
        })
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
fn indexed_columns_for(table: &str) -> Option<&'static IndexedCols> {
    static CACHE: OnceLock<HashMap<String, IndexedCols>> = OnceLock::new();
    CACHE
        .get_or_init(|| {
            let registry = crate::schema_loader::registry();
            registry
                .list_tables()
                .into_iter()
                .filter_map(|name| {
                    let cols: IndexedCols = registry
                        .get(&name)?
                        .fields
                        .iter()
                        .filter_map(|f| {
                            let cfg = f.tantivy.as_ref().filter(|c| c.indexed)?;
                            let tok = match cfg.tokenizer.as_deref().unwrap_or(NGRAM3_TOKENIZER) {
                                RAW_TOKENIZER => RAW_TOKENIZER,
                                DEFAULT_TOKENIZER => DEFAULT_TOKENIZER,
                                _ => NGRAM3_TOKENIZER,
                            };
                            Some((f.name.clone(), (tok, matches!(f.data_type.as_str(), "Utf8" | "LargeUtf8" | "Utf8View"))))
                        })
                        .collect();
                    (!cols.is_empty()).then_some((name, cols))
                })
                .collect()
        })
        .get(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tantivy_index::udf::{PredNode, collect_text_match_tree};

    /// `(pattern, escape, allow_substring)` → routed query, over every
    /// tokenizer-visible shape. `_` and embedded `%` never accelerate;
    /// leading `%` only on ngram3 (`allow_substring`).
    #[test]
    fn like_classifier_cases() {
        for (pat, esc, substring, want) in [
            ("foo", None, false, Some("foo")),
            ("foo%", None, false, Some("foo*")),
            ("%foo", None, false, None),
            ("%foo", None, true, Some("foo")),
            ("%foo%", None, true, Some("foo")),
            ("%foo%", None, false, None),
            ("fo%o", None, true, None),
            ("fo%o", None, false, None),
            ("fo_", None, true, None),
            ("foo+bar", None, true, None),
            ("svc.user-api", None, false, Some("svc.user-api")),
            ("foo\\%", Some('\\'), false, None), // escaped metachar: bail conservatively
        ] {
            assert_eq!(classify_like_pattern(pat, esc, substring), want.map(str::to_string), "{pat:?} escape={esc:?} substring={substring}");
        }
    }

    /// Test column map; every entry is text-typed unless a test overrides it.
    fn cols_of<const N: usize>(items: [(&str, &'static str); N]) -> IndexedCols {
        items.into_iter().map(|(k, tok)| (k.to_string(), (tok, true))).collect()
    }

    fn col(name: &str) -> Expr {
        Expr::Column(Column::new_unqualified(name))
    }

    fn cmp(c: &str, op: Operator, val: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(Box::new(col(c)), op, Box::new(lit(val))))
    }

    fn eq(c: &str, val: &str) -> Expr {
        cmp(c, Operator::Eq, val)
    }

    /// Rewrite `e`, then collect the prefilter tree the scan side would see.
    fn routed_tree(e: Expr, cols: &IndexedCols, allow_eq: bool) -> Option<PredNode> {
        collect_text_match_tree(&[e.transform_down(|x| rewrite_expr(x, cols, allow_eq)).unwrap().data])
    }

    /// P0: exact `=` on a raw column is the high-cardinality trace/span lookup
    /// acceleration. Every other shape (ngram3, `!=`, flag off, literals the
    /// QueryParser would mis-handle against a single raw token) falls back to
    /// the plain `=` — correctness over acceleration.
    #[test]
    fn match_eq_routes_raw_columns_only_when_enabled() {
        let cols = cols_of([("tid", RAW_TOKENIZER), ("name", NGRAM3_TOKENIZER)]);
        let uid = "0fee13b9-ac71-5c55-acd1-109542595054";
        for (expr, allow_eq, want, why) in [
            (eq("tid", "d01762b88f4ed54d"), true, Some("d01762b88f4ed54d"), "raw column + flag on routes as a term"),
            (eq("tid", uid), true, Some(uid), "dashed uuid: the `-` survives (e2e-proven)"),
            (eq("tid", "abc123"), false, None, "flag off reverts to bloom/stats"),
            (eq("name", "runServer"), true, None, "ngram3 is lossy for equality"),
            (cmp("tid", Operator::NotEq, "abc"), true, None, "`!=` has no term form"),
            (eq("tid", "a:b"), true, None, "colon is query syntax"),
            (eq("tid", "foo bar"), true, None, "space → AND-split can't match one raw token"),
            (eq("tid", "a.b"), true, None, "dot conservatively excluded"),
            (eq("tid", ""), true, None, "empty"),
        ] {
            let want = want.map(|q| ("tid".to_string(), Route::Ready(q.to_string())));
            assert_eq!(match_indexed_predicate(&expr, &cols, allow_eq), want, "{why}");
        }
    }

    #[test]
    fn or_of_routed_eqs_becomes_or_node_but_partial_or_is_opaque() {
        // End-to-end OR-safety with the tree collector: a disjunction where
        // BOTH branches are rewritten routes as an Or node (union — new
        // capability); a disjunction with one unroutable branch must yield NO
        // prefilter at all (else the 2026-06-16 empty/partial-union bug
        // returns). A top-level conjunct still routes.
        let cols = cols_of([("tid", RAW_TOKENIZER), ("sid", RAW_TOKENIZER)]);

        let tree = routed_tree(or(eq("tid", "x"), eq("sid", "y")), &cols, true);
        assert!(matches!(&tree, Some(PredNode::Or(kids)) if kids.len() == 2), "both routed branches must union, got {tree:?}");

        // One branch on an UN-indexed column → whole OR must be opaque.
        let cols_partial = cols_of([("tid", RAW_TOKENIZER)]);
        let partial = routed_tree(or(eq("tid", "x"), eq("unindexed", "y")), &cols_partial, true);
        assert_eq!(partial, None, "an OR with an unroutable branch must not seed the prefilter");

        let tree = routed_tree(and(eq("tid", "x"), lit(true)), &cols, true);
        assert!(matches!(&tree, Some(PredNode::Leaf(p)) if p.column == "tid" && p.query == "x"), "expected a tid/x leaf, got {tree:?}");
    }

    #[test]
    fn in_list_routes_as_or_of_terms() {
        let cols = cols_of([("tid", RAW_TOKENIZER), ("name", NGRAM3_TOKENIZER)]);
        let in_list =
            |c: &str, items: &[&str], negated: bool| Expr::InList(InList { expr: Box::new(col(c)), list: items.iter().map(|s| lit(*s)).collect(), negated });
        // Routable IN-list → collector sees an Or of leaves (complete via the
        // AND with the preserved original).
        let tree = routed_tree(in_list("tid", &["a", "b"], false), &cols, true);
        assert!(matches!(&tree, Some(PredNode::Or(kids)) if kids.len() == 2), "expected an Or of 2 leaves, got {tree:?}");
        // NOT IN, ngram3 column, unsafe literal, flag off → never routed.
        for (e, allow) in [
            (in_list("tid", &["a"], true), true),
            (in_list("name", &["abc"], false), true),
            (in_list("tid", &["a:b"], false), true),
            (in_list("tid", &["a"], false), false),
        ] {
            assert_eq!(routed_tree(e, &cols, allow), None);
        }
    }

    #[test]
    fn match_ilike_routes_on_ngram3_but_never_on_raw() {
        // ILIKE on a raw-tokenized (case-sensitive) column would silently miss
        // case variants; ngram3 lowercases both sides, so substrings route.
        let cols = cols_of([("raw", RAW_TOKENIZER), ("c", NGRAM3_TOKENIZER)]);
        let ilike = |c: &str, pat: &str| {
            Expr::Like(Like { negated: false, expr: Box::new(col(c)), pattern: Box::new(lit(pat)), escape_char: None, case_insensitive: true })
        };
        assert_eq!(match_indexed_predicate(&ilike("raw", "foo"), &cols, true), None);
        assert_eq!(match_indexed_predicate(&ilike("c", "%foo%"), &cols, true), Some(("c".into(), Route::Ready("foo".into()))));
    }

    /// `col::text ~* 'lit'` — the shape monoscope renders every KQL
    /// has/contains into. Before this it never matched, so the ngram3 index
    /// was built on ingest and never read.
    #[test]
    fn match_regex_imatch_through_cast_routes_on_ngram3() {
        use arrow::datatypes::DataType;
        use datafusion::logical_expr::expr::Cast;
        let cols = cols_of([("name", NGRAM3_TOKENIZER), ("tid", RAW_TOKENIZER)]);
        let re = |c: &str, op: Operator, pat: &str, cast: bool| {
            let lhs = if cast { Expr::Cast(Cast::new(Box::new(col(c)), DataType::Utf8)) } else { col(c) };
            Expr::BinaryExpr(BinaryExpr::new(Box::new(lhs), op, Box::new(lit(pat))))
        };

        for op in [Operator::RegexIMatch, Operator::RegexMatch] {
            assert_eq!(
                match_indexed_predicate(&re("name", op, "runServer", true), &cols, true),
                Some(("name".into(), Route::Ready("runServer".into()))),
                "cast-wrapped {op:?} on an ngram3 column must route"
            );
            // Bare column (DataFusion may fold the no-op cast) routes too.
            assert_eq!(match_indexed_predicate(&re("name", op, "runServer", false), &cols, true), Some(("name".into(), Route::Ready("runServer".into()))));
        }
        // `escapeRegex` output: `.` arrives as `\.` and decodes to a literal.
        assert_eq!(
            match_indexed_predicate(&re("name", Operator::RegexIMatch, "svc\\.user-api", true), &cols, true),
            Some(("name".into(), Route::Ready("svc.user-api".into())))
        );
        // Unescaped metachars / anchors (startswith & endswith) / short
        // patterns / raw-tokenized columns / non-literal RHS: never routed.
        for (c, pat) in [
            ("name", "run.*"),
            ("name", "a|b"),
            ("name", "^foo"),
            ("name", "foo$"),
            ("name", "fo(o)"),
            ("name", "\\yword\\y"), // \y is a word boundary, not an escaped literal
            ("name", "ab"),         // < NGRAM_MIN_QUERY_LEN
            ("name", ""),
            ("name", "a\\"), // trailing backslash
            ("tid", "abcdef"),
        ] {
            assert_eq!(match_indexed_predicate(&re(c, Operator::RegexIMatch, pat, true), &cols, true), None, "{c} ~* {pat:?} must not route");
        }
        // Variant/List column (not text-typed): the index holds our canonical
        // rendering, which `::text` need not reproduce — never routed.
        let variant_cols: IndexedCols = HashMap::from([("body".to_string(), (NGRAM3_TOKENIZER, false))]);
        assert_eq!(match_indexed_predicate(&re("body", Operator::RegexIMatch, "boom", true), &variant_cols, true), None);
    }
}
