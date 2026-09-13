//! Recognizes fixed-width count histograms without changing unsupported SQL.
use datafusion::{
    common::ScalarValue,
    logical_expr::{Expr, LogicalPlan, Operator},
};

use super::histogram::{HistogramWindow, Membership};

const MAX_BUCKETS: usize = 100_000;

pub(crate) struct HistogramQuery<'a> {
    pub matched: &'a LogicalPlan,
    pub table: String,
    pub project: String,
    pub window: HistogramWindow,
    pub membership: Membership,
}

fn unalias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => unalias(&alias.expr),
        _ => expr,
    }
}

fn utf8(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::Utf8View(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => Some(value),
        _ => None,
    }
}

fn string(expr: &Expr) -> Option<&str> {
    let Expr::Literal(value, _) = unalias(expr) else { return None };
    utf8(value)
}

fn strings(expr: &Expr) -> Option<Vec<String>> {
    use arrow::array::Array;
    let Expr::Literal(ScalarValue::List(list), _) = unalias(expr) else { return None };
    (list.len() == 1 && !list.is_null(0)).then_some(())?;
    let values = list.value(0);
    (!values.is_empty() && values.null_count() == 0).then_some(())?;
    (0..values.len())
        .map(|row| {
            let value = ScalarValue::try_from_array(&values, row).ok()?;
            utf8(&value).map(str::to_owned)
        })
        .collect()
}

fn column(expr: &Expr, name: &str) -> bool {
    matches!(unalias(expr), Expr::Column(column) if column.name == name)
}

fn jsonpath_member(path: &str) -> Option<String> {
    // Validate with the same parser as the UDF. Match only this small grammar;
    // parsing the final JSON string preserves quotes, escapes, and Unicode.
    sql_json_path::JsonPath::new(path).ok()?;
    let trimmed = path.trim();
    let stripped = trimmed.strip_prefix("lax").map_or(trimmed, str::trim_start);
    let path = ["$", "[", "*", "]", "?", "(", "@", "=="].into_iter().try_fold(stripped, |path, token| Some(path.strip_prefix(token)?.trim_start()))?;
    serde_json::from_str(path.strip_suffix(')')?.trim_end()).ok()
}

fn membership(expr: &Expr) -> Option<Membership> {
    match unalias(expr) {
        Expr::ScalarFunction(function) => match (function.name(), function.args.as_slice()) {
            ("jsonb_path_exists", [Expr::ScalarFunction(json), path]) => {
                let [Expr::Column(column)] = json.args.as_slice() else { return None };
                matches!(json.name(), "to_jsonb" | "to_json").then_some(())?;
                Some(Membership::Contains { column: column.name.clone(), value: jsonpath_member(string(path)?)? })
            }
            ("array_has", [Expr::Column(column), value]) => Some(Membership::Contains { column: column.name.clone(), value: string(value)?.into() }),
            (name @ ("array_has_all" | "array_has_any"), [Expr::Column(column), values]) => {
                strings(values)?.into_iter().map(|value| Membership::Contains { column: column.name.clone(), value }).reduce(|left, right| {
                    let (left, right) = (Box::new(left), Box::new(right));
                    if name == "array_has_all" { Membership::And(left, right) } else { Membership::Or(left, right) }
                })
            }
            _ => None,
        },
        Expr::BinaryExpr(binary) => {
            let left = Box::new(membership(&binary.left)?);
            let right = Box::new(membership(&binary.right)?);
            match binary.op {
                Operator::And => Some(Membership::And(left, right)),
                Operator::Or => Some(Membership::Or(left, right)),
                _ => None,
            }
        }
        _ => None,
    }
}

pub(crate) fn match_query(plan: &LogicalPlan) -> Option<HistogramQuery<'_>> {
    let original = match plan {
        LogicalPlan::Aggregate(aggregate) => aggregate,
        LogicalPlan::Projection(_) | LogicalPlan::Sort(_) | LogicalPlan::Limit(_) => return plan.inputs().first().copied().and_then(match_query),
        _ => return None,
    };
    let inlined = crate::rollup::inline_common_exprs(original);
    let aggregate = inlined.as_ref().unwrap_or(original);
    let [group] = aggregate.group_expr.as_slice() else { return None };
    let [count] = aggregate.aggr_expr.as_slice() else { return None };
    let Expr::AggregateFunction(count) = unalias(count) else { return None };
    (count.func.name() == "count" && !count.params.distinct && count.params.filter.is_none() && count.params.order_by.is_empty()).then_some(())?;
    match count.params.args.as_slice() {
        [] => {}
        [Expr::Literal(value, _)] if !value.is_null() => {}
        _ => return None,
    }
    let Expr::ScalarFunction(bucket) = unalias(group) else { return None };
    let [width, timestamp] = bucket.args.as_slice() else { return None };
    (bucket.name() == "time_bucket" && column(timestamp, "timestamp")).then_some(())?;
    let width = match unalias(width) {
        Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(value)), _) => {
            crate::read::functions::interval_to_micros(value.months, value.days, value.nanoseconds).ok()?
        }
        value => crate::read::functions::parse_interval_to_micros(string(value)?).ok()?,
    };
    let mut input = aggregate.input.as_ref();
    while let LogicalPlan::SubqueryAlias(alias) = input {
        input = alias.input.as_ref();
    }
    let LogicalPlan::Union(union) = input else { return match_source(plan, input, width) };
    // RangeParallelDedup splits wide windows below the aggregate. Only exact
    // adjacent ranges with the same source and predicate can be recombined.
    // UNION maps columns by position, so names must agree before walking them.
    union.inputs.iter().all(|branch| union.schema.has_equivalent_names_and_types(branch.schema()).is_ok()).then_some(())?;
    let mut sources = union.inputs.iter().map(|input| match_source(plan, input, width)).collect::<Option<Vec<_>>>()?;
    sources.sort_unstable_by_key(|source| source.window.bounds().0);
    let mut sources = sources.into_iter();
    let first = sources.next()?;
    sources.try_fold(first, |mut combined, source| {
        if combined.table != source.table
            || combined.project != source.project
            || combined.membership != source.membership
            || combined.window.bounds().1 != source.window.bounds().0
        {
            return None;
        }
        combined.window = HistogramWindow::new(combined.window.bounds().0, source.window.bounds().1, width, 0, MAX_BUCKETS).ok()?;
        Some(combined)
    })
}

fn match_source<'a>(matched: &'a LogicalPlan, input: &LogicalPlan, width: i64) -> Option<HistogramQuery<'a>> {
    let mut filters = Vec::new();
    let table = crate::rollup::source_and_filters(input, &mut filters).ok()?;
    let schema = crate::schema::get_schema(&table)?;
    let mut pending = filters.iter().collect::<Vec<_>>();
    let (mut lo, mut hi, mut project, mut predicates) = (None::<i64>, None::<i64>, None::<String>, Vec::new());
    while let Some(expr) = pending.pop() {
        if let Expr::BinaryExpr(binary) = unalias(expr) {
            if binary.op == Operator::And {
                pending.extend([binary.left.as_ref(), binary.right.as_ref()]);
                continue;
            }
            if binary.op == Operator::Eq && column(&binary.left, "project_id") {
                let value = string(&binary.right)?;
                if project.as_deref().is_some_and(|old| old != value) {
                    return None;
                }
                project = Some(value.into());
                continue;
            }
            if column(&binary.left, "timestamp") {
                let Expr::Literal(ScalarValue::TimestampMicrosecond(Some(value), _), _) = unalias(&binary.right) else { return None };
                // lo is exclusive-shifted for `>`, hi for `<=`; both narrow toward the tighter bound.
                match binary.op {
                    Operator::GtEq | Operator::Gt => {
                        let value = value.checked_add(i64::from(binary.op == Operator::Gt))?;
                        lo = Some(lo.map_or(value, |old| old.max(value)));
                    }
                    Operator::Lt | Operator::LtEq => {
                        let value = value.checked_add(i64::from(binary.op == Operator::LtEq))?;
                        hi = Some(hi.map_or(value, |old| old.min(value)));
                    }
                    _ => return None,
                }
                continue;
            }
        }
        predicates.push(membership(expr)?);
    }
    let membership = predicates.into_iter().reduce(|left, right| Membership::And(Box::new(left), Box::new(right)))?;
    if !membership.columns().iter().all(|column| {
        schema.fields.iter().any(|field| {
            field.name == *column && field.tantivy.as_ref().is_some_and(|config| config.indexed && config.list_mode == crate::schema::TantivyListMode::Elements)
        })
    }) {
        return None;
    }
    Some(HistogramQuery { matched, table, project: project?, window: HistogramWindow::new(lo?, hi?, width, 0, MAX_BUCKETS).ok()?, membership })
}

#[cfg(test)]
mod tests {
    #[test_case::test_case(r#"$[*] ? (@ == "err:a")"# => Some("err:a".to_owned()))]
    #[test_case::test_case(r#" lax $ [ * ] ? ( @ == "a\"b" ) "# => Some("a\"b".to_owned()))]
    #[test_case::test_case(r#"$[*]?(@=="α")"# => Some("α".to_owned()); "literal unicode")]
    #[test_case::test_case(r#"$[*]?(@=="\u03b1")"# => None)]
    #[test_case::test_case(r#"$[*]?(@=="")"# => Some(String::new()))]
    #[test_case::test_case(r#"$[*]?(@!="a")"# => None)]
    #[test_case::test_case(r#"$[*]?(@=="a" || @=="b")"# => None)]
    #[test_case::test_case(r#"$[*].name?(@=="a")"# => None)]
    #[test_case::test_case(r#"$[*]?(@==null)"# => None)]
    fn exact_jsonpath_membership(path: &str) -> Option<String> {
        let value = super::jsonpath_member(path);
        if let Some(value) = &value {
            assert!(sql_json_path::JsonPath::new(path).unwrap().exists(&serde_json::json!([value])).unwrap());
        }
        value
    }
}
