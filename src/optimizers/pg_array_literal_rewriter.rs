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

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::DataType,
    common::{
        DFSchema, Result,
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
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgCoalesceUdf {
    inner: Arc<datafusion::logical_expr::ScalarUDF>,
}

impl Default for PgCoalesceUdf {
    fn default() -> Self {
        Self {
            inner: datafusion::functions::core::coalesce(),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for PgCoalesceUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
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
    let input_schemas: Vec<_> = plan.inputs().iter().map(|i| i.schema().clone()).collect();
    if input_schemas.is_empty() {
        return Ok(Transformed::no(plan));
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
        .collect();
    let rebuilt = Expr::ScalarFunction(ScalarFunction { func, args: new_args });
    Ok(if changed { Transformed::yes(rebuilt) } else { Transformed::no(rebuilt) })
}

/// Convert parsed PG array elements to a typed list literal. None on any
/// element that doesn't parse as `elem_type` (caller leaves the arg alone).
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
/// Returns None if `s` isn't brace-wrapped (not an array literal).
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
        let rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![
            Arc::new(PgArrayLiteralRewriter),
            Arc::new(datafusion::optimizer::analyzer::type_coercion::TypeCoercion::new()),
        ];
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
        let out = one_string(&ctx, "SELECT array_length(COALESCE(CAST(NULL AS VARCHAR[]), '{}'), 1) FROM (SELECT 1)").await;
        // empty list → array_length over dim 1 is NULL in DF, so just assert it planned & ran
        assert!(out.contains("---"), "{out}");
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

    #[test]
    fn pg_array_parse_shapes() {
        assert_eq!(parse_pg_string_array("{}"), Some(vec![]));
        assert_eq!(parse_pg_string_array("{a,b}"), Some(vec![Some("a".into()), Some("b".into())]));
        assert_eq!(parse_pg_string_array(r#"{"a,b", c }"#), Some(vec![Some("a,b".into()), Some("c".into())]));
        assert_eq!(parse_pg_string_array("{NULL,\"NULL\"}"), Some(vec![None, Some("NULL".into())]));
        assert_eq!(parse_pg_string_array("plain"), None);
    }
}
