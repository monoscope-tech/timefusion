//! Rewrites `row_to_json(t)` — a bare relation alias standing for a whole row —
//! into `row_to_json(named_struct('c1', t.c1, …))`.
//!
//! PostgreSQL lets a table alias name the entire row in a function argument.
//! DataFusion rejects it while PLANNING the SQL ("No field named t. Valid
//! fields are t.total, t.active"), which is before any analyzer or optimizer
//! rule can see the plan — so unlike [`super::ExistsInProjection`] this has to
//! happen on the statement, not the plan.
//!
//! pgAdmin's dashboard polls exactly this shape every 5 seconds:
//!
//! ```sql
//! SELECT 'x' AS chart_name, pg_catalog.row_to_json(t) AS chart_data
//! FROM (SELECT (…) AS "total", (…) AS "active") t
//! ```
//!
//! The column names come from the derived table's own SELECT aliases, which are
//! present in the AST, so no schema lookup is needed. A relation whose columns
//! are not all explicitly aliased is left alone — inventing names there would be
//! guessing, and the statement fails exactly as it does today.

use datafusion::sql::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, Value, ValueWithSpan, VisitMut, VisitorMut,
};

/// Cheap guard so the parse/unparse round trip only happens for statements that
/// could possibly need it.
pub fn might_need_rewrite(sql: &str) -> bool {
    sql.to_ascii_lowercase().contains("row_to_json")
}

/// Returns the rewritten statement only when something actually changed, so an
/// untouched statement is never round-tripped through the unparser.
pub fn rewrite(statement: &mut Statement) -> bool {
    let mut visitor = RowToJsonRecord { rewrote: false };
    let _ = statement.visit(&mut visitor);
    visitor.rewrote
}

struct RowToJsonRecord {
    rewrote: bool,
}

impl VisitorMut for RowToJsonRecord {
    type Break = ();

    fn post_visit_query(&mut self, query: &mut Query) -> std::ops::ControlFlow<Self::Break> {
        self.rewrite_set_expr(query.body.as_mut());
        std::ops::ControlFlow::Continue(())
    }
}

impl RowToJsonRecord {
    /// A UNION's branches are `SetExpr`s, not `Query`s, so they are never
    /// reached by matching on `query.body` alone. pgAdmin's dashboard sends one
    /// `SELECT ... UNION ALL SELECT ...` per chart, and every branch was being
    /// skipped. Re-running over an already-rewritten branch is a no-op, since
    /// its argument is no longer a bare identifier.
    fn rewrite_set_expr(&mut self, body: &mut SetExpr) {
        match body {
            SetExpr::Select(select) => self.rewrite_select(select),
            SetExpr::SetOperation { left, right, .. } => {
                self.rewrite_set_expr(left);
                self.rewrite_set_expr(right);
            }
            SetExpr::Query(query) => self.rewrite_set_expr(query.body.as_mut()),
            _ => {}
        }
    }

    fn rewrite_select(&mut self, select: &mut Select) {
        // `post_visit_query` means inner queries are already rewritten, so the
        // aliases collected here belong to this SELECT's own FROM.
        let relations: Vec<(String, Vec<String>)> = select.from.iter().filter_map(|from| derived_columns(&from.relation)).collect();
        if relations.is_empty() {
            return;
        }
        for item in &mut select.projection {
            let expr = match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
                _ => continue,
            };
            self.rewrite_expr(expr, &relations);
        }
    }

    fn rewrite_expr(&mut self, expr: &mut Expr, relations: &[(String, Vec<String>)]) {
        let Expr::Function(function) = expr else { return };
        if !is_row_to_json(&function.name) {
            return;
        }
        // pgAdmin writes `pg_catalog.row_to_json`, and a schema-qualified UDF
        // name does not resolve ("Invalid function 'pg_catalog.row_to_json'").
        if function.name.0.len() > 1 {
            function.name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("row_to_json"))]);
            self.rewrote = true;
        }
        let FunctionArguments::List(FunctionArgumentList { args, .. }) = &mut function.args else { return };
        let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident)))] = args.as_mut_slice() else {
            return;
        };
        let Some((alias, columns)) = relations.iter().find(|(alias, _)| alias.eq_ignore_ascii_case(&ident.value)) else {
            return;
        };
        args[0] = FunctionArg::Unnamed(FunctionArgExpr::Expr(named_struct(alias, columns)));
        self.rewrote = true;
    }
}

fn is_row_to_json(name: &ObjectName) -> bool {
    name.0.last().is_some_and(|part| match part {
        ObjectNamePart::Identifier(ident) => ident.value.eq_ignore_ascii_case("row_to_json"),
        _ => false,
    })
}

/// `named_struct('total', t."total", 'active', t."active", …)`, preserving the
/// declared column order.
fn named_struct(alias: &str, columns: &[String]) -> Expr {
    let args = columns
        .iter()
        .flat_map(|column| {
            [
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(ValueWithSpan::from(Value::SingleQuotedString(column.clone()))))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(vec![
                    Ident::new(alias.to_string()),
                    Ident::with_quote('"', column.clone()),
                ]))),
            ]
        })
        .collect();
    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("named_struct"))]),
        args: FunctionArguments::List(FunctionArgumentList { duplicate_treatment: None, args, clauses: vec![] }),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// `(SELECT … AS a, … AS b) t` → `("t", ["a", "b"])`. `None` unless the relation
/// is a derived table with an alias and every projected column is explicitly
/// named, since the column names are the whole point.
fn derived_columns(relation: &TableFactor) -> Option<(String, Vec<String>)> {
    let TableFactor::Derived { subquery, alias, .. } = relation else { return None };
    let alias = alias.as_ref()?;
    let SetExpr::Select(select) = subquery.body.as_ref() else { return None };
    let columns = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
            // A bare column still has a well-defined name.
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(ident.value.clone()),
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => parts.last().map(|part| part.value.clone()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    (!columns.is_empty()).then(|| (alias.name.value.clone(), columns))
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    use super::*;

    fn rewritten(sql: &str) -> String {
        let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("parses");
        let changed = rewrite(&mut statements[0]);
        assert!(changed, "expected a rewrite for: {sql}");
        statements[0].to_string()
    }

    #[test]
    fn bare_alias_becomes_named_struct_in_declared_order() {
        let sql = rewritten(r#"SELECT row_to_json(t) FROM (SELECT 1 AS "total", 2 AS "active") t"#);
        assert!(sql.contains(r#"named_struct('total', t."total", 'active', t."active")"#), "got: {sql}");
    }

    /// pgAdmin schema-qualifies the call.
    #[test]
    fn qualified_pg_catalog_call_is_rewritten() {
        let sql = rewritten(r#"SELECT pg_catalog.row_to_json(t) FROM (SELECT 1 AS "a") t"#);
        assert!(sql.contains("named_struct('a', t.\"a\")"), "got: {sql}");
        // A schema-qualified UDF name does not resolve in DataFusion.
        assert!(!sql.contains("pg_catalog.row_to_json"), "qualifier must be stripped: {sql}");
    }

    /// The shape prod actually sends: one branch per chart, UNION ALL. A visitor
    /// that only matches `query.body == Select` silently skips every branch.
    #[test]
    fn every_union_branch_is_rewritten() {
        let sql = rewritten(
            r#"SELECT 'a' AS chart_name, pg_catalog.row_to_json(t) AS chart_data FROM (SELECT 1 AS "Total") t
               UNION ALL
               SELECT 'b' AS chart_name, pg_catalog.row_to_json(t) AS chart_data FROM (SELECT 2 AS "Active") t"#,
        );
        assert!(sql.contains(r#"named_struct('Total', t."Total")"#), "first branch: {sql}");
        assert!(sql.contains(r#"named_struct('Active', t."Active")"#), "second branch: {sql}");
        assert!(!sql.contains("row_to_json(t)"), "no branch may keep the bare alias: {sql}");
    }

    fn unchanged(sql: &str) {
        let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("parses");
        assert!(!rewrite(&mut statements[0]), "should not rewrite: {sql}");
    }

    /// An unnamed column has no name to key the object by; guessing one would be
    /// worse than the planning error the user already gets.
    #[test]
    fn unaliased_derived_column_is_left_alone() {
        unchanged("SELECT row_to_json(t) FROM (SELECT count(*), 1 AS b) t");
    }

    /// `row_to_json(some_column)` is an ordinary call on a value, not a record.
    #[test]
    fn non_relation_identifier_is_left_alone() {
        unchanged("SELECT row_to_json(payload) FROM events");
    }

    /// A real table alias is not a derived table: its columns are not in the AST.
    #[test]
    fn plain_table_alias_is_left_alone() {
        unchanged("SELECT row_to_json(t) FROM some_table t");
    }
}
