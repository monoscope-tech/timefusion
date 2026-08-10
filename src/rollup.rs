//! Schema-driven dashboard rollups.
//!
//! A rollup is built only after its source partition is duplicate-free. This
//! module owns the deterministic aggregate SQL and the conversion from its
//! output to the generated target schema. Read routing lives here as well, but
//! is deliberately conservative: an unsupported query must use raw data.

use crate::schema_loader::RollupSpec;

/// Why a query cannot use a rollup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissReason {
    UnsupportedShape,
    MissingProject,
    UnboundedTime,
    UnknownGroupBy,
    UnknownFilter,
    MissingMeasure,
    NonDecomposableAggregate,
    PartialBucket,
    IncompleteCoverage,
    RewriteSchemaMismatch,
}

impl MissReason {
    pub const fn label(self) -> &'static str {
        match self {
            Self::UnsupportedShape => "unsupported_shape",
            Self::MissingProject => "missing_project",
            Self::UnboundedTime => "unbounded_time",
            Self::UnknownGroupBy => "unknown_group_by",
            Self::UnknownFilter => "unknown_filter",
            Self::MissingMeasure => "missing_measure",
            Self::NonDecomposableAggregate => "non_decomposable",
            Self::PartialBucket => "unaligned_range",
            Self::IncompleteCoverage => "incomplete_coverage",
            Self::RewriteSchemaMismatch => "rewrite_schema_mismatch",
        }
    }
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// SQL that builds one source `(project_id, date)` partition.
///
/// Aggregate filters belong on each aggregate rather than in the row `WHERE`
/// clause. Moving them would make unrelated measures observe the wrong rows.
pub fn build_partition_sql(spec: &RollupSpec, source: &str, project_id: &str, date: &str) -> anyhow::Result<String> {
    let grain = spec.grain_micros().ok_or_else(|| anyhow::anyhow!("invalid rollup grain `{}`", spec.grain))?;
    let dimensions = spec.dimensions.join(", ");
    let measures = spec
        .measures
        .iter()
        .map(|measure| {
            let expression = match (measure.agg.as_str(), measure.column.as_deref()) {
                ("count", None) => "COUNT(*)".to_string(),
                ("count", Some(column)) => format!("COUNT({column})"),
                ("tdigest", Some(column)) => format!("percentile_agg(CAST({column} AS DOUBLE))"),
                (aggregate, Some(column)) => format!("{}({column})", aggregate.to_uppercase()),
                (aggregate, None) => return Err(anyhow::anyhow!("{} measure `{}` needs a source column", aggregate, measure.name)),
            };
            Ok(match &measure.filter {
                Some(filter) => format!("{expression} FILTER (WHERE {filter}) AS {}", measure.name),
                None => format!("{expression} AS {}", measure.name),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(", ");
    let select_dimensions = (!dimensions.is_empty()).then(|| format!(", {dimensions}")).unwrap_or_default();
    let group_by = std::iter::once("1".to_string()).chain((2..).take(spec.dimensions.len()).map(|index| index.to_string())).collect::<Vec<_>>().join(", ");

    Ok(format!(
        "SELECT to_timestamp_micros(CAST(FLOOR(EXTRACT(EPOCH FROM timestamp) * 1000000 / {grain}) AS BIGINT) * {grain}) AS timestamp{select_dimensions}, {measures} \
         FROM {source} WHERE project_id = {} AND date = {} GROUP BY {group_by}",
        sql_literal(project_id),
        sql_literal(date),
    ))
}

fn generated_bucket_id(bucket: i64, grain: i64, generation: &str, dimensions: &[datafusion::scalar::ScalarValue]) -> String {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    (bucket, grain, generation).hash(&mut hasher);
    dimensions.len().hash(&mut hasher);
    for dimension in dimensions {
        format!("{dimension:?}").hash(&mut hasher);
    }
    format!("{bucket}-{:016x}", hasher.finish())
}

/// Convert aggregate batches into rows for the generated rollup schema.
///
/// The aggregate output contains `timestamp`, then configured dimensions and
/// configured measures. All remaining target fields are internal identity or
/// partition fields. The conversion deliberately copies each configured Arrow
/// array and casts it only at the generated target boundary, so binary digest
/// state and non-string dimensions retain their types.
pub fn to_rollup_batches(
    spec: &RollupSpec, source: &str, project_id: &str, date: &str, generation: &str, aggregated: &[arrow::record_batch::RecordBatch],
) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
    use arrow::{
        array::{Array, ArrayRef, BooleanArray, Date32Array, StringArray, TimestampMicrosecondArray},
        compute::kernels::cast::cast,
        datatypes::DataType,
    };
    use std::sync::Arc;

    let target = spec.table_name(source);
    let schema = crate::schema_loader::get_schema(&target).ok_or_else(|| anyhow::anyhow!("{target} schema missing"))?.schema_ref();
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| anyhow::anyhow!("invalid Unix epoch date"))?;
    let date_days = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")?.signed_duration_since(epoch).num_days();
    let date_days = i32::try_from(date_days).map_err(|_| anyhow::anyhow!("rollup date `{date}` is outside Date32"))?;
    let grain = spec.grain_micros().ok_or_else(|| anyhow::anyhow!("invalid rollup grain `{}`", spec.grain))?;
    let now = crate::clock::now_micros();

    aggregated
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .map(|batch| {
            let rows = batch.num_rows();
            let timestamp = batch.column_by_name("timestamp").ok_or_else(|| anyhow::anyhow!("rollup aggregate is missing timestamp"))?;
            let timestamp = cast(timestamp, &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())))?;
            let timestamp = timestamp
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| anyhow::anyhow!("rollup aggregate timestamp cannot cast to microseconds"))?;
            let timestamps = (0..rows)
                .map(|row| (!timestamp.is_null(row)).then(|| timestamp.value(row)).ok_or_else(|| anyhow::anyhow!("rollup aggregate timestamp is null")))
                .collect::<anyhow::Result<Vec<_>>>()?;
            let dimension_columns = spec
                .dimensions
                .iter()
                .map(|name| batch.column_by_name(name).ok_or_else(|| anyhow::anyhow!("rollup aggregate is missing dimension `{name}`")))
                .collect::<anyhow::Result<Vec<&ArrayRef>>>()?;
            let ids = (0..rows)
                .map(|row| {
                    let values = dimension_columns
                        .iter()
                        .map(|column| datafusion::scalar::ScalarValue::try_from_array(column, row))
                        .collect::<datafusion::common::Result<Vec<_>>>()?;
                    Ok(generated_bucket_id(timestamps[row], grain, generation, &values))
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?;

            let columns = schema
                .fields()
                .iter()
                .map(|field| -> anyhow::Result<ArrayRef> {
                    let array: ArrayRef = match field.name().as_str() {
                        "project_id" => Arc::new(StringArray::from(vec![Some(project_id); rows])),
                        "timestamp" => Arc::new(TimestampMicrosecondArray::from(timestamps.clone()).with_timezone("UTC")),
                        "date" => Arc::new(Date32Array::from(vec![date_days; rows])),
                        "id" => Arc::new(StringArray::from(ids.clone())),
                        "updated_at" => Arc::new(TimestampMicrosecondArray::from(vec![now; rows]).with_timezone("UTC")),
                        "deleted" => Arc::new(BooleanArray::from(vec![Some(false); rows])),
                        "rollup_generation" => Arc::new(StringArray::from(vec![Some(generation); rows])),
                        name if spec.dimensions.iter().any(|dimension| dimension == name) || spec.measures.iter().any(|measure| measure.name == name) => {
                            batch.column_by_name(name).cloned().ok_or_else(|| anyhow::anyhow!("rollup aggregate is missing `{name}`"))?
                        }
                        name => anyhow::bail!("generated rollup schema has unsupported field `{name}`"),
                    };
                    Ok(if array.data_type() == field.data_type() { array } else { cast(&array, field.data_type())? })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(arrow::record_batch::RecordBatch::try_new(Arc::clone(&schema), columns)?)
        })
        .collect()
}

#[derive(Debug)]
pub(crate) struct RoutedRollup {
    pub source: String,
    pub project_id: String,
    pub lo: i64,
    pub hi: i64,
    pub grain: i64,
    pub target: String,
    pub outer_projection: Option<datafusion::logical_expr::logical_plan::Projection>,
    row_filters: Vec<String>,
    select: String,
    group_by: String,
}

impl RoutedRollup {
    pub fn sql(&self, generations: &[(String, String)]) -> String {
        let generations = generations
            .iter()
            .map(|(date, generation)| format!("(date = {} AND rollup_generation = {})", sql_literal(date), sql_literal(generation)))
            .collect::<Vec<_>>()
            .join(" OR ");
        let group_by = (!self.group_by.is_empty()).then(|| format!(" GROUP BY {}", self.group_by));
        let row_filters = self.row_filters.iter().map(|filter| format!(" AND ({filter})")).collect::<String>();
        format!(
            "SELECT {} FROM {} WHERE project_id = {} AND timestamp >= to_timestamp_micros({}) AND timestamp < to_timestamp_micros({}) AND ({}){}{}",
            self.select,
            self.target,
            sql_literal(&self.project_id),
            self.lo,
            self.hi,
            generations,
            row_filters,
            group_by.unwrap_or_default(),
        )
    }
}

fn unaliased(expr: &datafusion::logical_expr::Expr) -> &datafusion::logical_expr::Expr {
    match expr {
        datafusion::logical_expr::Expr::Alias(alias) => unaliased(&alias.expr),
        datafusion::logical_expr::Expr::Cast(cast) => unaliased(&cast.expr),
        expr => expr,
    }
}

fn column_name(expr: &datafusion::logical_expr::Expr) -> Option<&str> {
    match unaliased(expr) {
        datafusion::logical_expr::Expr::Column(column) => Some(&column.name),
        _ => None,
    }
}

fn string_literal(expr: &datafusion::logical_expr::Expr) -> Option<&str> {
    match unaliased(expr) {
        datafusion::logical_expr::Expr::Literal(
            datafusion::scalar::ScalarValue::Utf8(Some(value)) | datafusion::scalar::ScalarValue::Utf8View(Some(value)),
            _,
        ) => Some(value),
        _ => None,
    }
}

fn timestamp_literal(expr: &datafusion::logical_expr::Expr) -> Option<i64> {
    match unaliased(expr) {
        datafusion::logical_expr::Expr::Literal(
            datafusion::scalar::ScalarValue::TimestampMicrosecond(Some(value), _) | datafusion::scalar::ScalarValue::Int64(Some(value)),
            _,
        ) => Some(*value),
        _ => None,
    }
}

fn dimension_filter_sql(expr: &datafusion::logical_expr::Expr, dimensions: &[String]) -> Option<String> {
    use datafusion::{
        logical_expr::{Expr, Operator},
        scalar::ScalarValue,
    };

    let literal = |value: &ScalarValue| match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::Utf8View(Some(value)) => Some(sql_literal(value)),
        ScalarValue::Boolean(Some(value)) => Some(value.to_string()),
        ScalarValue::Int8(Some(value)) => Some(value.to_string()),
        ScalarValue::Int16(Some(value)) => Some(value.to_string()),
        ScalarValue::Int32(Some(value)) => Some(value.to_string()),
        ScalarValue::Int64(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt8(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt16(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt32(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt64(Some(value)) => Some(value.to_string()),
        ScalarValue::Float32(Some(value)) => Some(value.to_string()),
        ScalarValue::Float64(Some(value)) => Some(value.to_string()),
        ScalarValue::Null => Some("NULL".to_string()),
        _ => None,
    };
    let operator = |operator| match operator {
        Operator::Eq => Some("="),
        Operator::NotEq => Some("<>"),
        Operator::Lt => Some("<"),
        Operator::LtEq => Some("<="),
        Operator::Gt => Some(">"),
        Operator::GtEq => Some(">="),
        Operator::And => Some("AND"),
        Operator::Or => Some("OR"),
        _ => None,
    };
    match unaliased(expr) {
        Expr::Column(column) if dimensions.iter().any(|dimension| dimension == &column.name) => Some(column.name.clone()),
        Expr::Literal(value, _) => literal(value),
        Expr::BinaryExpr(binary) => {
            Some(format!("{} {} {}", dimension_filter_sql(&binary.left, dimensions)?, operator(binary.op)?, dimension_filter_sql(&binary.right, dimensions)?))
        }
        Expr::IsNull(expr) => Some(format!("{} IS NULL", dimension_filter_sql(expr, dimensions)?)),
        Expr::IsNotNull(expr) => Some(format!("{} IS NOT NULL", dimension_filter_sql(expr, dimensions)?)),
        _ => None,
    }
}

fn canonical(expr: &datafusion::logical_expr::Expr) -> String {
    use datafusion::logical_expr::{Expr, Operator};
    match unaliased(expr) {
        Expr::Column(column) => column.name.clone(),
        Expr::Literal(value, _) => format!("{value:?}"),
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
            let mut terms = Vec::new();
            fn collect(expr: &Expr, operator: Operator, terms: &mut Vec<String>) {
                match unaliased(expr) {
                    Expr::BinaryExpr(binary) if binary.op == operator => {
                        collect(&binary.left, operator, terms);
                        collect(&binary.right, operator, terms);
                    }
                    expr => terms.push(canonical(expr)),
                }
            }
            collect(expr, binary.op, &mut terms);
            terms.sort();
            format!(
                "({})",
                terms.join(match binary.op {
                    Operator::And => " AND ",
                    _ => " OR ",
                })
            )
        }
        Expr::BinaryExpr(binary) => format!("({} {:?} {})", canonical(&binary.left), binary.op, canonical(&binary.right)),
        Expr::IsNotNull(expr) => format!("{} IS NOT NULL", canonical(expr)),
        Expr::ScalarFunction(function) => format!("{}({})", function.name(), function.args.iter().map(canonical).collect::<Vec<_>>().join(",")),
        expr => format!("{expr:?}"),
    }
}

fn canonical_and<'a>(expressions: impl IntoIterator<Item = &'a datafusion::logical_expr::Expr>) -> String {
    let mut expressions = expressions.into_iter().map(canonical).collect::<Vec<_>>();
    expressions.sort();
    expressions.join(" AND ")
}

fn parse_bucket_micros(value: &str) -> Option<i64> {
    let mut parts = value.split_whitespace();
    let value = parts.next()?.parse::<i64>().ok()?;
    let unit = parts.next()?.trim_end_matches('s');
    let unit = match unit {
        "second" | "sec" => 1_000_000,
        "minute" | "min" => 60_000_000,
        "hour" | "hr" => 3_600_000_000,
        "day" => 86_400_000_000,
        _ => return None,
    };
    value.checked_mul(unit)
}

fn source_and_filters(plan: &datafusion::logical_expr::LogicalPlan, filters: &mut Vec<datafusion::logical_expr::Expr>) -> Option<String> {
    use datafusion::logical_expr::LogicalPlan;
    match plan {
        LogicalPlan::Projection(projection) => source_and_filters(&projection.input, filters),
        LogicalPlan::Filter(filter) => {
            filters.push(filter.predicate.clone());
            source_and_filters(&filter.input, filters)
        }
        LogicalPlan::TableScan(scan) => {
            filters.extend(scan.filters.clone());
            Some(scan.table_name.table().to_string())
        }
        _ => None,
    }
}

async fn measure_filters<'a>(
    session: &datafusion::execution::context::SessionState, source: &str, spec: &'a RollupSpec,
) -> Result<Vec<(&'a crate::schema_loader::RollupMeasure, String)>, MissReason> {
    let mut filters = Vec::with_capacity(spec.measures.len());
    for measure in &spec.measures {
        let filter = match &measure.filter {
            None => String::new(),
            Some(filter) => {
                let plan = session.create_logical_plan(&format!("SELECT * FROM {source} WHERE {filter}")).await.map_err(|_| MissReason::UnknownFilter)?;
                let mut filters = Vec::new();
                source_and_filters(&plan, &mut filters).ok_or(MissReason::UnknownFilter)?;
                canonical_and(&filters)
            }
        };
        filters.push((measure, filter));
    }
    Ok(filters)
}

/// Match a direct aggregate over one declared source table and construct its
/// re-aggregation request. It deliberately rejects a shape it cannot prove.
pub(crate) async fn match_aggregate(
    plan: &datafusion::logical_expr::LogicalPlan, session: &datafusion::execution::context::SessionState,
) -> Result<Option<RoutedRollup>, MissReason> {
    use datafusion::logical_expr::{Expr, LogicalPlan, Operator, utils::split_conjunction};

    let (aggregate, output_names, outer_projection) = match plan {
        LogicalPlan::Aggregate(aggregate) => (aggregate, None, None),
        LogicalPlan::Projection(projection) => {
            let LogicalPlan::Aggregate(aggregate) = projection.input.as_ref() else { return Ok(None) };
            if projection.expr.iter().all(|expr| matches!(unaliased(expr), Expr::Column(_))) {
                (aggregate, Some(projection.schema.fields().iter().map(|field| field.name().to_string()).collect::<Vec<_>>()), None)
            } else {
                (aggregate, None, Some(projection.clone()))
            }
        }
        _ => return Ok(None),
    };
    let mut predicates = Vec::new();
    let Some(source) = source_and_filters(&aggregate.input, &mut predicates) else { return Ok(None) };
    let Some(schema) = crate::schema_loader::get_schema(&source).filter(|schema| !schema.rollups.is_empty()) else { return Ok(None) };
    let spec = schema.rollups.first().ok_or(MissReason::UnsupportedShape)?;
    let mut project_id = None;
    let (mut lo, mut hi) = (None, None);
    let (mut row_filters, mut residual) = (Vec::new(), Vec::new());
    for term in predicates.iter().flat_map(split_conjunction) {
        match term {
            Expr::Between(between) if !between.negated && column_name(&between.expr) == Some("timestamp") => {
                let (Some(lower), Some(upper)) = (timestamp_literal(&between.low), timestamp_literal(&between.high)) else {
                    return Err(MissReason::UnboundedTime);
                };
                lo = Some(lo.map_or(lower, |current: i64| current.max(lower)));
                hi = Some(
                    hi.map_or_else(|| upper.checked_add(1), |current: i64| upper.checked_add(1).map(|upper| current.min(upper)))
                        .ok_or(MissReason::UnboundedTime)?,
                );
            }
            Expr::BinaryExpr(binary) if matches!(binary.op, Operator::Eq) && column_name(&binary.left) == Some("project_id") => {
                project_id = string_literal(&binary.right).map(str::to_string)
            }
            Expr::BinaryExpr(binary) if matches!(binary.op, Operator::Eq) && column_name(&binary.right) == Some("project_id") => {
                project_id = string_literal(&binary.left).map(str::to_string)
            }
            Expr::BinaryExpr(binary) if column_name(&binary.left) == Some("timestamp") => {
                let Some(value) = timestamp_literal(&binary.right) else { return Err(MissReason::UnboundedTime) };
                match binary.op {
                    Operator::GtEq => lo = Some(lo.map_or(value, |current: i64| current.max(value))),
                    Operator::Gt => {
                        lo = Some(lo.map_or(value.checked_add(1).ok_or(MissReason::UnboundedTime)?, |current: i64| current.max(value.saturating_add(1))))
                    }
                    Operator::Lt => hi = Some(hi.map_or(value, |current: i64| current.min(value))),
                    Operator::LtEq => {
                        hi = Some(hi.map_or(value.checked_add(1).ok_or(MissReason::UnboundedTime)?, |current: i64| current.min(value.saturating_add(1))))
                    }
                    _ => residual.push(term.clone()),
                }
            }
            term => match dimension_filter_sql(term, &spec.dimensions) {
                Some(filter) => row_filters.push(filter),
                None => residual.push(term.clone()),
            },
        }
    }
    let project_id = project_id.ok_or(MissReason::MissingProject)?;
    let (lo, hi) = lo.zip(hi).filter(|(lo, hi)| lo < hi).ok_or(MissReason::UnboundedTime)?;
    let grain = spec.grain_micros().ok_or(MissReason::UnsupportedShape)?;
    let configured_filters = measure_filters(session, &source, spec).await?;
    let residual_filter = canonical_and(&residual);

    let mut select = Vec::new();
    let mut group_by = Vec::new();
    for (index, expression) in aggregate.group_expr.iter().enumerate() {
        let alias = output_names.as_ref().and_then(|names| names.get(index)).map_or_else(|| aggregate.schema.field(index).name(), String::as_str);
        match unaliased(expression) {
            Expr::Column(column) if spec.dimensions.iter().any(|dimension| dimension == &column.name) => {
                select.push(format!("{} AS \"{}\"", column.name, alias.replace('"', "\"\"")))
            }
            Expr::ScalarFunction(function)
                if function.name().eq_ignore_ascii_case("time_bucket") && function.args.len() == 2 && column_name(&function.args[1]) == Some("timestamp") =>
            {
                let interval = string_literal(&function.args[0]).ok_or(MissReason::UnsupportedShape)?;
                let width = parse_bucket_micros(interval).ok_or(MissReason::UnsupportedShape)?;
                if width < grain || width % grain != 0 {
                    return Err(MissReason::PartialBucket);
                }
                select.push(format!("time_bucket({}, timestamp) AS \"{}\"", sql_literal(interval), alias.replace('"', "\"\"")));
            }
            Expr::Column(_) => return Err(MissReason::UnknownGroupBy),
            _ => return Err(MissReason::UnsupportedShape),
        }
        group_by.push((index + 1).to_string());
    }

    for (index, expression) in aggregate.aggr_expr.iter().enumerate() {
        let output_index = aggregate.group_expr.len() + index;
        let (expression, alias) = match expression {
            Expr::Alias(alias) => {
                (alias.expr.as_ref(), output_names.as_ref().and_then(|names| names.get(output_index)).cloned().unwrap_or_else(|| alias.name.clone()))
            }
            expression => (
                expression,
                output_names
                    .as_ref()
                    .and_then(|names| names.get(output_index))
                    .cloned()
                    .unwrap_or_else(|| aggregate.schema.field(output_index).name().to_string()),
            ),
        };
        let Expr::AggregateFunction(function) = unaliased(expression) else { return Err(MissReason::NonDecomposableAggregate) };
        if function.params.distinct || !function.params.order_by.is_empty() {
            return Err(MissReason::NonDecomposableAggregate);
        }
        let local_filter = canonical_and(function.params.filter.iter().map(|filter| filter.as_ref()));
        let mut filters = [residual_filter.as_str(), local_filter.as_str()].into_iter().filter(|filter| !filter.is_empty()).collect::<Vec<_>>();
        filters.sort();
        let filter = filters.join(" AND ");
        let name = function.func.name().to_ascii_lowercase();
        let column = function.params.args.first().and_then(column_name).map(str::to_string);
        let measure = |aggregate: &str, column: Option<&str>| {
            configured_filters
                .iter()
                .find(|(measure, measure_filter)| measure.agg == aggregate && measure.column.as_deref() == column && *measure_filter == filter)
                .map(|(measure, _)| measure.name.as_str())
        };
        let state = match name.as_str() {
            "count" => measure("count", column.as_deref()).map(|measure| format!("COALESCE(SUM({measure}), 0)")),
            "sum" => measure("sum", column.as_deref()).map(|measure| format!("SUM({measure})")),
            "min" => measure("min", column.as_deref()).map(|measure| format!("MIN({measure})")),
            "max" => measure("max", column.as_deref()).map(|measure| format!("MAX({measure})")),
            "avg" => {
                let sum = measure("sum", column.as_deref());
                let count = measure("count", column.as_deref());
                sum.zip(count)
                    .map(|(sum, count)| format!("CASE WHEN COALESCE(SUM({count}), 0) = 0 THEN CAST(NULL AS DOUBLE) ELSE SUM({sum}) / SUM({count}) END"))
            }
            "percentile_agg" => measure("tdigest", column.as_deref()).map(|measure| format!("tdigest_merge({measure})")),
            _ => return Err(MissReason::NonDecomposableAggregate),
        }
        .ok_or(MissReason::MissingMeasure)?;
        select.push(format!("{state} AS \"{}\"", alias.replace('"', "\"\"")));
    }

    Ok(Some(RoutedRollup {
        source,
        project_id,
        lo,
        hi,
        grain,
        target: spec.table_name(&schema.table_name),
        outer_projection,
        row_filters,
        select: select.join(", "),
        group_by: group_by.join(", "),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Array, BinaryArray, Int64Array, StringArray, StringViewArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    const SOURCE: &str = "otel_logs_and_spans";

    fn spec() -> RollupSpec {
        crate::schema_loader::get_schema(SOURCE).expect("source schema").rollups.first().expect("declared rollup").clone()
    }

    #[test]
    fn declared_rollup_is_generated_with_its_configured_fields() {
        let source = crate::schema_loader::get_schema(SOURCE).expect("source schema");
        let spec = spec();
        let target = crate::schema_loader::get_schema(&spec.table_name(SOURCE)).expect("generated rollup schema");
        for name in spec.dimensions.iter().chain(spec.measures.iter().map(|measure| &measure.name)) {
            assert!(target.fields.iter().any(|field| field.name == *name), "missing configured rollup field `{name}`");
        }
        assert!(target.fields.iter().any(|field| field.name == "rollup_generation"));
        assert_eq!(target.partitions, source.partitions);
        assert!(!target.version_append);
    }

    #[test]
    fn build_sql_uses_exact_count_and_tdigest_states() {
        let sql = build_partition_sql(&spec(), SOURCE, "pro'ject", "2026-08-01").expect("valid SQL");
        assert!(sql.contains("COUNT(duration) AS duration_count"));
        assert!(sql.contains("percentile_agg(CAST(duration AS DOUBLE))"));
        assert!(sql.contains("project_id = 'pro''ject'"));
        assert!(sql.contains("GROUP BY 1, 2, 3, 4"));
    }

    #[tokio::test]
    async fn matcher_rewrites_a_certifiable_count_aggregate() {
        let mut ctx = datafusion::prelude::SessionContext::new();
        crate::functions::register_custom_functions(&mut ctx).expect("functions register");
        let table = datafusion::datasource::MemTable::try_new(crate::schema_loader::get_schema(SOURCE).expect("source schema").schema_ref(), vec![vec![]])
            .expect("empty source table");
        ctx.register_table(SOURCE, Arc::new(table)).expect("register source");
        let state = ctx.state();
        let plan = state
            .create_logical_plan(
                "SELECT COUNT(*) FROM otel_logs_and_spans \
                 WHERE project_id = 'project' \
                   AND timestamp >= to_timestamp_micros(60000000) \
                   AND timestamp < to_timestamp_micros(120000000)",
            )
            .await
            .expect("parse count");
        let plan = state.optimize(&plan).expect("optimize count");
        let route = match_aggregate(&plan, &state).await.expect("match count").expect("declared rollup route");
        assert_eq!(route.target, "otel_logs_and_spans_rollup_dashboard_1m_v2");
        assert_eq!(route.project_id, "project");
        assert!(route.sql(&[("1970-01-01".into(), "generation".into())]).contains("COALESCE(SUM(request_count), 0)"));
    }

    #[tokio::test]
    async fn matcher_preserves_a_scalar_outer_projection() {
        let mut ctx = datafusion::prelude::SessionContext::new();
        crate::functions::register_custom_functions(&mut ctx).expect("functions register");
        for table_name in [SOURCE, "otel_logs_and_spans_rollup_dashboard_1m_v2"] {
            let table = datafusion::datasource::MemTable::try_new(crate::schema_loader::get_schema(table_name).expect("schema").schema_ref(), vec![vec![]])
                .expect("empty table");
            ctx.register_table(table_name, Arc::new(table)).expect("register table");
        }
        let state = ctx.state();
        let plan = state
            .create_logical_plan(
                "SELECT COUNT(*) + 1 AS total FROM otel_logs_and_spans \
                 WHERE project_id = 'project' \
                   AND timestamp >= to_timestamp_micros(60000000) \
                   AND timestamp < to_timestamp_micros(120000000)",
            )
            .await
            .expect("parse count");
        let plan = state.optimize(&plan).expect("optimize count");
        let route = match_aggregate(&plan, &state).await.expect("match count").expect("declared rollup route");
        let rewritten = state.create_logical_plan(&route.sql(&[("1970-01-01".into(), "generation".into())])).await.expect("parse rollup query");
        let projection = route.outer_projection.expect("scalar projection");
        let rewritten = datafusion::logical_expr::LogicalPlan::Projection(
            datafusion::logical_expr::logical_plan::Projection::try_new(projection.expr, Arc::new(rewritten)).expect("reapply projection"),
        );
        assert_eq!(rewritten.schema(), plan.schema());
    }

    #[tokio::test]
    async fn matcher_applies_dimension_predicates_to_the_rollup_scan() {
        let mut ctx = datafusion::prelude::SessionContext::new();
        crate::functions::register_custom_functions(&mut ctx).expect("functions register");
        let table = datafusion::datasource::MemTable::try_new(crate::schema_loader::get_schema(SOURCE).expect("source schema").schema_ref(), vec![vec![]])
            .expect("empty source table");
        ctx.register_table(SOURCE, Arc::new(table)).expect("register source");
        let state = ctx.state();
        let plan = state
            .create_logical_plan(
                "SELECT COUNT(*) FROM otel_logs_and_spans \
                 WHERE project_id = 'project' AND kind = 'server' \
                   AND timestamp >= to_timestamp_micros(60000000) \
                   AND timestamp < to_timestamp_micros(120000000)",
            )
            .await
            .expect("parse count");
        let route = match_aggregate(&state.optimize(&plan).expect("optimize count"), &state).await.expect("match count").expect("declared rollup route");
        assert!(route.sql(&[("1970-01-01".into(), "generation".into())]).contains("AND (kind = 'server')"));
    }

    #[test]
    fn shaped_batches_preserve_binary_measure_and_generation() {
        let spec = spec();
        let mut fields = vec![Arc::new(Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false))];
        fields.extend(spec.dimensions.iter().map(|name| Arc::new(Field::new(name, DataType::Utf8, true))));
        fields.extend(spec.measures.iter().map(|measure| {
            Arc::new(Field::new(&measure.name, if measure.agg == "tdigest" { DataType::Binary } else { DataType::Int64 }, measure.agg != "count"))
        }));
        let mut columns: Vec<arrow::array::ArrayRef> = vec![Arc::new(TimestampMicrosecondArray::from(vec![1_000_000]).with_timezone("UTC"))];
        columns.extend(spec.dimensions.iter().map(|_| Arc::new(StringArray::from(vec![Some("value")])) as arrow::array::ArrayRef));
        columns.extend(spec.measures.iter().map(|measure| {
            if measure.agg == "tdigest" {
                Arc::new(BinaryArray::from(vec![Some(&[1_u8, 2, 3][..])])) as arrow::array::ArrayRef
            } else {
                Arc::new(Int64Array::from(vec![Some(1)])) as arrow::array::ArrayRef
            }
        }));
        let input = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("aggregate batch");
        let output = to_rollup_batches(&spec, SOURCE, "project", "1970-01-01", "generation-a", &[input.clone()]).expect("shape rollup");
        let other = to_rollup_batches(&spec, SOURCE, "project", "1970-01-01", "generation-b", &[input]).expect("shape rollup");
        let batch = output.first().expect("one output batch");
        let generation = batch.column_by_name("rollup_generation").expect("generation").as_any().downcast_ref::<StringViewArray>().expect("utf8 generation");
        assert_eq!(generation.value(0), "generation-a");
        let digest = batch.column_by_name("server_duration_digest").expect("digest");
        assert_eq!(digest.data_type(), &DataType::Binary);
        assert_eq!(digest.as_any().downcast_ref::<BinaryArray>().expect("binary digest").value(0), &[1, 2, 3]);
        assert_ne!(
            batch.column_by_name("id").expect("id").as_any().downcast_ref::<StringViewArray>().expect("utf8 id").value(0),
            other[0].column_by_name("id").expect("id").as_any().downcast_ref::<StringViewArray>().expect("utf8 id").value(0),
            "generations must not share a dedup identity"
        );
    }
}
