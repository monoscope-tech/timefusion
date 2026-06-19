//! Rewrite Postgres array literals (`'{}'`, `'{a,b}'`) into typed list
//! literals where an array type is expected.
//!
//! Postgres resolves `COALESCE(hashes, '{}')` by treating the untyped string
//! literal as an array literal of the other argument's type. DataFusion's
//! coercion can't unify `List(Utf8View)` with `Utf8`, so the same expression
//! failed at planning — which broke monoscope's `lookupOtelRecord`
//! random-access query (`COALESCE(hashes,'{}')`, `COALESCE(summary,'{}')`).
//!
//! This rule runs before `TypeCoercion`. For any `coalesce` call where at
//! least one argument types to a list and another is a `'{...}'` string
//! literal, the literal is re-parsed as a PG array literal of the list's
//! element type. Only string element types are handled (the schema's arrays
//! are all `VARCHAR[]`); anything else is left for TypeCoercion to report.
//!
//! Known divergences from PG (each pinned by a test):
//! - a string COLUMN (or unrewritable literal) coalesced with a list errors
//!   like PG ("cannot be matched") rather than silently wrapping into a
//!   one-element list via arrow's blanket `(_, List)` cast — see
//!   `coalesce_string_column_with_list_errors`.
//! - malformed quoting WITHIN a brace-wrapped literal (`{"a"x,b}`, `{a"b"}`)
//!   parses leniently (PG errors) — see `pg_array_parse_shapes`.
//! - multi-dimensional literals are rejected → arg left untouched, the
//!   string-arg guard errors (PG supports them; schema is 1-D only).

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::DataType,
    common::{
        DFSchema, Result, plan_err,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    logical_expr::{Expr, ExprSchemable, LogicalPlan, expr::ScalarFunction},
    optimizer::AnalyzerRule,
    scalar::ScalarValue,
};

/// `coalesce` wrapper whose coercion additionally unifies string args into a
/// sibling list type. Needed because the SQL planner computes projection
/// schemas (→ `coerce_types`) BEFORE analyzer rules run, so
/// `coalesce(List, Utf8)` must type-check up front; the analyzer rule below
/// then replaces the string literal with a real list literal so TypeCoercion
/// never has to cast Utf8 → List (unsupported in Arrow).
///
/// Registered under the built-in's name, shadowing it session-wide; every
/// trait method delegates to the inner built-in. On a DataFusion upgrade,
/// re-check `ScalarUDFImpl` for new methods whose defaults would diverge
/// from the built-in coalesce and forward them here too — the const assert
/// below fails the build on a version bump until that audit happens.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgCoalesceUdf {
    inner: Arc<datafusion::logical_expr::ScalarUDF>,
}

/// DataFusion version `PgCoalesceUdf`'s method forwarding was last audited
/// against, compared at compile time to `datafusion::DATAFUSION_VERSION`.
/// A `cargo update` of datafusion breaks the build here on purpose:
/// re-audit `ScalarUDFImpl` for new methods, forward them above, then bump.
/// Maintenance invariant: DATAFUSION_VERSION resolves through the delta-rs
/// fork's patched graph — the fork must not bump it independently of the
/// workspace pin, or this check would pass against the wrong version. By the
/// same token, fork upgrades that keep the version string still need the
/// manual audit — the tripwire only catches plain `cargo update`s.
const AUDITED_DATAFUSION_VERSION: &str = "54.0.0";
// Byte loop because `&str` equality (PartialEq) isn't const-callable on
// stable — assert!(a == b) won't compile in a const block.
const _: () = {
    let (a, b) = (datafusion::DATAFUSION_VERSION.as_bytes(), AUDITED_DATAFUSION_VERSION.as_bytes());
    assert!(
        a.len() == b.len(),
        "DataFusion bumped: re-audit PgCoalesceUdf's ScalarUDFImpl forwarding, then update AUDITED_DATAFUSION_VERSION"
    );
    let mut i = 0;
    while i < a.len() {
        assert!(
            a[i] == b[i],
            "DataFusion bumped: re-audit PgCoalesceUdf's ScalarUDFImpl forwarding, then update AUDITED_DATAFUSION_VERSION"
        );
        i += 1;
    }
};

impl Default for PgCoalesceUdf {
    fn default() -> Self {
        Self {
            inner: datafusion::functions::core::coalesce(),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for PgCoalesceUdf {
    fn name(&self) -> &str {
        "coalesce"
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        self.inner.signature()
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.inner().return_type(arg_types)
    }
    fn return_field_from_args(&self, args: datafusion::logical_expr::ReturnFieldArgs) -> Result<datafusion::arrow::datatypes::FieldRef> {
        self.inner.inner().return_field_from_args(args)
    }
    fn invoke_with_args(&self, args: datafusion::logical_expr::ScalarFunctionArgs) -> Result<datafusion::logical_expr::ColumnarValue> {
        self.inner.inner().invoke_with_args(args)
    }
    fn short_circuits(&self) -> bool {
        self.inner.inner().short_circuits()
    }
    fn simplify(
        &self, args: Vec<Expr>, info: &datafusion::logical_expr::simplify::SimplifyContext,
    ) -> Result<datafusion::logical_expr::simplify::ExprSimplifyResult> {
        self.inner.inner().simplify(args, info)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types).or_else(|e| {
            // Fallback only runs after the built-in coercion failed. Promote
            // EVERY string arg (not just literals — arg types carry no
            // expression info here) to the sibling list type: PG treats any
            // string in array position as an array literal. The analyzer rule
            // rewrites the literals to real lists before execution and rejects
            // any string arg it can't rewrite (real string columns, malformed
            // literals) with a PG-style planning error — required because
            // arrow-cast's blanket `(_, List)` rule would otherwise "cast" the
            // string by wrapping it in a single-element list, silently.
            // First list type wins. If a call ever mixes list element types
            // (coalesce(utf8_list, int_list, '{}')) the retried coercion below
            // fails on the second list and the original error surfaces — no
            // silent mis-typing, just a planner-time error like today.
            let list_t = arg_types
                .iter()
                .find(|t| matches!(t, DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..)))
                .ok_or(e)?
                .clone();
            let patched: Vec<DataType> = arg_types
                .iter()
                .map(|t| {
                    if matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8) {
                        list_t.clone()
                    } else {
                        t.clone()
                    }
                })
                .collect();
            self.inner.coerce_types(&patched)
        })
    }
}

#[derive(Debug, Default)]
pub struct PgArrayLiteralRewriter;

impl AnalyzerRule for PgArrayLiteralRewriter {
    fn name(&self) -> &str {
        "pg_array_literal_rewriter"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(rewrite_in_plan).map(|t| t.data)
    }
}

fn rewrite_in_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut input_schemas: Vec<_> = plan.inputs().iter().map(|i| i.schema().clone()).collect();
    // Leaf nodes (TableScan with pushed-down filters, Values) have no inputs;
    // their exprs are typed by their own schema instead. Without this, a
    // coalesce pushed into a TableScan's filter list would skip both the
    // rewrite and the string-arg guard below — arrow's
    // wrap-into-single-element-list cast would then fire silently. A filter on
    // a column the scan doesn't project still won't resolve here (filters are
    // typed by the source schema); it falls through to the TypeCoercion error.
    if input_schemas.is_empty() {
        input_schemas.push(Arc::clone(plan.schema()));
    }
    plan.map_expressions(|expr| expr.transform_up(|e| rewrite_in_expr(e, &input_schemas)))
}

fn rewrite_in_expr(expr: Expr, input_schemas: &[Arc<DFSchema>]) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return Ok(Transformed::no(expr));
    };
    let no = |args| Ok(Transformed::no(Expr::ScalarFunction(ScalarFunction { func: Arc::clone(&func), args })));
    if func.name() != "coalesce" {
        return no(args);
    }

    // Element type of the first arg that resolves to a list type.
    let elem_type = args.iter().find_map(|a| {
        input_schemas.iter().find_map(|s| match a.get_type(s.as_ref()).ok()? {
            DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => Some(f.data_type().clone()),
            _ => None,
        })
    });
    let Some(elem_type) = elem_type else { return no(args) };

    let mut changed = false;
    let new_args = args
        .into_iter()
        .map(|a| match &a {
            Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _) => {
                match parse_pg_string_array(s).and_then(|elems| pg_elems_to_list(elems, &elem_type)) {
                    Some(list) => {
                        changed = true;
                        Expr::Literal(list, None)
                    }
                    None => a,
                }
            }
            _ => a,
        })
        .collect::<Vec<_>>();
    // Any arg still string-typed here (a real string column, or a literal that
    // didn't parse as a PG array) survived only because PgCoalesceUdf's
    // coerce_types fallback over-promoted it to pass planning. Letting it
    // reach TypeCoercion would NOT error: arrow-cast's blanket `(_, List)`
    // rule wraps each value in a single-element list — a silently wrong
    // result. Reject it like PG ("COALESCE types ... cannot be matched").
    if let Some(bad) = new_args.iter().find_map(|a| {
        input_schemas.iter().find_map(|s| match a.get_type(s.as_ref()) {
            Ok(t @ (DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8)) => Some(t),
            _ => None,
        })
    }) {
        return plan_err!("COALESCE types {bad} and List({elem_type}) cannot be matched");
    }
    let rebuilt = Expr::ScalarFunction(ScalarFunction { func, args: new_args });
    Ok(if changed { Transformed::yes(rebuilt) } else { Transformed::no(rebuilt) })
}

/// Convert parsed PG array elements to a typed list literal. None on any
/// element that doesn't parse as `elem_type` (caller leaves the arg alone).
/// Always emits `ScalarValue::List` even when the sibling column is
/// LargeList/FixedSizeList: coerce_types may claim those, but TypeCoercion
/// then casts the literal (arrow supports List → Large/FixedSizeList), so no
/// physical-planning mismatch — the variant only matters for element type.
fn pg_elems_to_list(elems: Vec<Option<String>>, elem_type: &DataType) -> Option<ScalarValue> {
    let vals: Option<Vec<ScalarValue>> = elems
        .into_iter()
        .map(|e| match e {
            None => ScalarValue::try_from(elem_type).ok(),
            Some(s) => ScalarValue::try_from_string(s, elem_type).ok(),
        })
        .collect();
    Some(ScalarValue::List(ScalarValue::new_list_nullable(&vals?, elem_type)))
}

/// Parse a PG array literal of strings: `{}`, `{a,b}`, `{"a,b",NULL}`.
/// Returns None if `s` isn't brace-wrapped (not an array literal), or if it
/// contains unquoted nested braces (multi-dimensional arrays like
/// `{{a},{b}}` — the schema is 1-D only, and misparsing the inner braces as
/// element text would be silently wrong; bail so the arg is left untouched).
/// Malformed quoting parses leniently rather than strictly: bailing would
/// only swap one error (this rewrite skipped → TypeCoercion's) for another,
/// while leniency keeps stable-but-sloppy client literals working.
/// Only the bare `NULL` keyword is a null element; `\N` is COPY text-format
/// syntax, not array-literal syntax, and stays literal text (as in PG).
fn parse_pg_string_array(s: &str) -> Option<Vec<Option<String>>> {
    let inner = s.trim().strip_prefix('{')?.strip_suffix('}')?;
    if inner.trim().is_empty() {
        return Some(vec![]);
    }
    let (mut elems, mut cur, mut in_quotes, mut was_quoted) = (Vec::new(), String::new(), false, false);
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' if in_quotes => cur.push(chars.next()?),
            '{' | '}' if !in_quotes => return None, // multi-dimensional literal
            '"' => {
                if !in_quotes && cur.trim().is_empty() {
                    cur.clear(); // drop whitespace before an opening quote
                }
                in_quotes = !in_quotes;
                was_quoted = true;
            }
            ',' if !in_quotes => {
                elems.push(finish_elem(&mut cur, &mut was_quoted));
            }
            _ if in_quotes || !was_quoted => cur.push(c),
            _ => {} // ignore trailing chars after a closing quote
        }
    }
    elems.push(finish_elem(&mut cur, &mut was_quoted));
    Some(elems)
}

fn finish_elem(cur: &mut String, was_quoted: &mut bool) -> Option<String> {
    let raw = std::mem::take(cur);
    let quoted = std::mem::take(was_quoted);
    let trimmed = raw.trim();
    if !quoted && trimmed.eq_ignore_ascii_case("null") {
        None
    } else {
        Some(if quoted { raw } else { trimmed.to_string() })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{execution::session_state::SessionStateBuilder, prelude::SessionContext};

    use super::*;

    fn ctx_with_rule() -> SessionContext {
        let rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> =
            vec![Arc::new(PgArrayLiteralRewriter), Arc::new(datafusion::optimizer::analyzer::type_coercion::TypeCoercion::new())];
        let state = SessionStateBuilder::new().with_default_features().with_analyzer_rules(rules).build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_udf(datafusion::logical_expr::ScalarUDF::from(PgCoalesceUdf::default()));
        ctx
    }

    async fn one_string(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx.sql(sql).await.expect("plan ok").collect().await.expect("exec ok");
        datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap().to_string()
    }

    #[tokio::test]
    async fn coalesce_empty_pg_array_literal() {
        let ctx = ctx_with_rule();
        // cardinality(empty list) = 0 — asserts the actual coalesced value,
        // not merely that planning succeeded.
        let out = one_string(&ctx, "SELECT cardinality(COALESCE(CAST(NULL AS VARCHAR[]), '{}')) AS n FROM (SELECT 1)").await;
        assert!(out.contains("| 0 "), "{out}");
    }

    #[tokio::test]
    async fn coalesce_nonempty_pg_array_literal() {
        let ctx = ctx_with_rule();
        let out = one_string(&ctx, "SELECT COALESCE(CAST(NULL AS VARCHAR[]), '{a, b, \"c,d\", NULL}') AS v FROM (SELECT 1)").await;
        assert!(out.contains("[a, b, c,d, ]"), "{out}");
    }

    #[tokio::test]
    async fn non_array_string_untouched() {
        let ctx = ctx_with_rule();
        // No list-typed arg → rule must not fire; plain string coalesce still works.
        let out = one_string(&ctx, "SELECT COALESCE(CAST(NULL AS VARCHAR), '{}') AS v FROM (SELECT 1)").await;
        assert!(out.contains("{}"), "{out}");
    }

    // Analyzer rules run at physical planning, so the error may surface at
    // sql() or collect() — either way it must never execute successfully.
    async fn expect_plan_error(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(e) => e.to_string(),
            Ok(df) => df.collect().await.expect_err("query must fail planning").to_string(),
        }
    }

    // arrow-cast's blanket `(_, List)` rule casts ANY type to a list by
    // wrapping each value in a single-element list, so without an explicit
    // guard these queries would silently return `[varchar_value]` instead of
    // failing like PG ("COALESCE types ... cannot be matched").
    #[tokio::test]
    async fn coalesce_string_column_with_list_errors() {
        let ctx = ctx_with_rule();
        let err = expect_plan_error(
            &ctx,
            "SELECT COALESCE(v, l) FROM (SELECT CAST('x' AS VARCHAR) AS v, CAST(NULL AS VARCHAR[]) AS l)",
        )
        .await;
        assert!(err.contains("cannot be matched"), "{err}");
    }

    #[tokio::test]
    async fn coalesce_unparseable_literal_with_list_errors() {
        let ctx = ctx_with_rule();
        let err = expect_plan_error(&ctx, "SELECT COALESCE(CAST(NULL AS VARCHAR[]), 'not-an-array') FROM (SELECT 1)").await;
        assert!(err.contains("cannot be matched"), "{err}");
    }

    // Guards the leaf-node path in rewrite_in_plan: a TableScan carrying a
    // pushed-down coalesce filter has no inputs, so its exprs must be typed
    // against its own schema for the rewrite to fire.
    #[test]
    fn rewrites_inside_table_scan_filters() {
        use datafusion::{
            arrow::datatypes::{Field, Schema},
            logical_expr::{
                col, lit,
                logical_plan::builder::{LogicalPlanBuilder, LogicalTableSource},
            },
        };
        let schema = Schema::new(vec![Field::new(
            "hashes",
            DataType::List(Field::new("item", DataType::Utf8, true).into()),
            true,
        )]);
        let coalesce = Expr::ScalarFunction(ScalarFunction::new_udf(datafusion::functions::core::coalesce(), vec![col("hashes"), lit("{}")]));
        let scan = LogicalPlanBuilder::scan_with_filters("t", Arc::new(LogicalTableSource::new(schema.into())), None, vec![coalesce.eq(col("hashes"))])
            .unwrap()
            .build()
            .unwrap();
        let analyzed = PgArrayLiteralRewriter.analyze(scan, &ConfigOptions::default()).unwrap();
        let LogicalPlan::TableScan(ts) = analyzed else { panic!("expected TableScan") };
        let Expr::BinaryExpr(be) = &ts.filters[0] else { panic!("expected eq filter") };
        let Expr::ScalarFunction(f) = be.left.as_ref() else {
            panic!("expected coalesce")
        };
        assert!(
            matches!(f.args[1], Expr::Literal(ScalarValue::List(_), _)),
            "array literal in TableScan filter not rewritten: {:?}",
            f.args[1]
        );
    }

    #[test]
    fn pg_array_parse_shapes() {
        assert_eq!(parse_pg_string_array("{}"), Some(vec![]));
        assert_eq!(parse_pg_string_array("{a,b}"), Some(vec![Some("a".into()), Some("b".into())]));
        assert_eq!(parse_pg_string_array(r#"{"a,b", c }"#), Some(vec![Some("a,b".into()), Some("c".into())]));
        assert_eq!(parse_pg_string_array("{NULL,\"NULL\"}"), Some(vec![None, Some("NULL".into())]));
        // Chars between a closing quote and the next comma are dropped — PG
        // rejects these literals outright; we parse leniently and keep the
        // quoted value. Pinned so the behavior is intentional, not accidental.
        assert_eq!(parse_pg_string_array("{\"a\"x,b}"), Some(vec![Some("a".into()), Some("b".into())]));
        // The mirror case — non-whitespace BEFORE an opening quote — is also
        // PG-invalid; we leniently concatenate. Pinned for the same reason.
        assert_eq!(parse_pg_string_array("{a\"b\"}"), Some(vec![Some("ab".into())]));
        // Lone backslash at end of input (inside quotes, escaping nothing):
        // chars.next()? propagates None → arg left unrewritten → the
        // string-arg guard errors (never a panic or a half-parsed list).
        assert_eq!(parse_pg_string_array("{\"a\\}"), None);
        // Multi-dimensional literals are rejected, not silently flattened;
        // braces inside quotes are ordinary element text.
        assert_eq!(parse_pg_string_array("{{a},{b}}"), None);
        assert_eq!(parse_pg_string_array(r#"{"{x}",y}"#), Some(vec![Some("{x}".into()), Some("y".into())]));
        assert_eq!(parse_pg_string_array("plain"), None);
    }
}
