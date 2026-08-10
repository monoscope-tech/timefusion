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
    /// No rollup was ever built for a date in the window.
    NotBuilt,
    /// A rollup exists for the date but the source has moved under it.
    StaleCoverage,
    /// Coverage cannot be established at all: buffered rows, or a window whose
    /// dates cannot be enumerated.
    IncompleteCoverage,
    /// The certified interior is too small a slice of the window to be worth the
    /// union's second scan.
    TinyInterior,
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
            // Names the ONLY thing this reason still means: a `time_bucket`
            // width that is not a multiple of the grain. The window's own
            // alignment stopped mattering once raw fringes were added.
            Self::PartialBucket => "unaligned_bucket_width",
            Self::NotBuilt => "not_built",
            Self::StaleCoverage => "stale_coverage",
            Self::IncompleteCoverage => "incomplete_coverage",
            Self::TinyInterior => "tiny_interior",
            Self::RewriteSchemaMismatch => "rewrite_schema_mismatch",
        }
    }
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Deterministic identity for one built partition.
///
/// It was a random UUID, which made the rollup rows on S3 unreadable after a
/// restart: reads filter on the generation and the only copy of it lived in a
/// DashMap. Deriving it from the inputs makes the rollup TABLE the durable
/// record — coverage can be recovered by reading back what is stored and
/// checking it still matches — and makes a rebuild over an unchanged source
/// produce byte-identical `id`s, so a replace is idempotent per row rather than
/// only per partition.
///
/// The spec participates because adding a measure without bumping the table
/// name would otherwise serve rows built under the old spec as if current.
pub fn generation_id(spec: &RollupSpec, source: &str, project_id: &str, date: &str, source_fp: u64) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = fnv::FnvHasher::default();
    source_fp.hash(&mut hasher);
    format!("{spec:?}").hash(&mut hasher);
    (source, project_id, date).hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// SQL that builds one source `(project_id, date)` partition.
///
/// Aggregate filters belong on each aggregate rather than in the row `WHERE`
/// clause. Moving them would make unrelated measures observe the wrong rows.
pub fn build_partition_sql(spec: &RollupSpec, source: &str, project_id: &str, date: &str) -> anyhow::Result<String> {
    build_partition_sql_from(spec, source, source, project_id, date)
}

/// `build_partition_sql`, but reading `from` instead of the raw source.
///
/// When `from` is a finer rollup the measures are re-aggregated as STATES —
/// `SUM` of counts and sums, `MIN`/`MAX` of extrema, `tdigest_merge` of digests —
/// and each measure's declared `filter` is deliberately NOT re-applied: the base
/// row already had it applied when it was built, and the filter's columns do not
/// even exist on the base table.
pub fn build_partition_sql_from(spec: &RollupSpec, source: &str, from: &str, project_id: &str, date: &str) -> anyhow::Result<String> {
    let grain = spec.grain_micros().ok_or_else(|| anyhow::anyhow!("invalid rollup grain `{}`", spec.grain))?;
    let derived = from != source;
    let dimensions = spec.dimensions.join(", ");
    let measures = spec
        .measures
        .iter()
        .map(|measure| {
            if derived {
                let merge = match measure.agg.as_str() {
                    "min" => "MIN",
                    "max" => "MAX",
                    "tdigest" => "tdigest_merge",
                    _ => "SUM",
                };
                return Ok(format!("{merge}({}) AS {}", measure.name, measure.name));
            }
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
    let source = from;
    let select_dimensions = if dimensions.is_empty() { String::new() } else { format!(", {dimensions}") };
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

/// A plan node peeled off the query root so the rewrite can be re-wrapped in it.
#[derive(Debug, Clone)]
pub(crate) enum PlanWrapper {
    Sort(Vec<datafusion::logical_expr::SortExpr>, Option<usize>),
    Limit(Option<Box<datafusion::logical_expr::Expr>>, Option<Box<datafusion::logical_expr::Expr>>),
}

impl PlanWrapper {
    /// Peel one wrapper off the root, returning it and the plan beneath.
    fn peel(plan: &datafusion::logical_expr::LogicalPlan) -> Option<(Self, &datafusion::logical_expr::LogicalPlan)> {
        use datafusion::logical_expr::LogicalPlan;
        match plan {
            LogicalPlan::Sort(sort) => Some((Self::Sort(sort.expr.clone(), sort.fetch), &sort.input)),
            LogicalPlan::Limit(limit) => Some((Self::Limit(limit.skip.clone(), limit.fetch.clone()), &limit.input)),
            _ => None,
        }
    }

    /// Re-apply `wrappers` (outermost first, as peeled) above `plan`.
    pub fn rewrap(wrappers: Vec<Self>, plan: datafusion::logical_expr::LogicalPlan) -> datafusion::logical_expr::LogicalPlan {
        use datafusion::logical_expr::{LogicalPlan, logical_plan};
        wrappers.into_iter().rev().fold(plan, |input, wrapper| match wrapper {
            Self::Sort(expr, fetch) => LogicalPlan::Sort(logical_plan::Sort { expr, input: std::sync::Arc::new(input), fetch }),
            Self::Limit(skip, fetch) => LogicalPlan::Limit(logical_plan::Limit { skip, fetch, input: std::sync::Arc::new(input) }),
        })
    }
}

/// How a measure's per-leg partial states combine into the query's answer.
///
/// The same combinator serves both shapes: over measure columns when the rollup
/// answers alone, and over the union's state aliases when a raw leg is present.
/// That is only sound because every variant is associative over a *partition* of
/// the row set — which is exactly what [`interior`] guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Merge {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    TDigest,
}

impl Merge {
    /// State columns consumed, in order. `Avg` is the only multi-state merge: an
    /// average is not a state, so the legs must carry sum and count apart or the
    /// union would average two averages.
    const fn arity(self) -> usize {
        if matches!(self, Self::Avg) { 2 } else { 1 }
    }

    /// The associative operator that folds one state column across legs.
    const fn partial_op(self) -> &'static str {
        match self {
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::TDigest => "tdigest_merge",
            _ => "SUM",
        }
    }

    /// Combine `states` into the query's output value.
    fn sql(self, states: &[String]) -> String {
        match (self, states) {
            (Self::Count, [count]) => format!("COALESCE(SUM({count}), 0)"),
            (Self::Sum, [sum]) => format!("SUM({sum})"),
            (Self::Min, [min]) => format!("MIN({min})"),
            (Self::Max, [max]) => format!("MAX({max})"),
            // The CAST is on the dividend, not the result: both states are
            // Int64, so dividing first truncates and the outer type only widens
            // an already-wrong value.
            (Self::Avg, [sum, count]) => {
                format!("CASE WHEN COALESCE(SUM({count}), 0) = 0 THEN CAST(NULL AS DOUBLE) ELSE CAST(SUM({sum}) AS DOUBLE) / CAST(SUM({count}) AS DOUBLE) END")
            }
            (Self::TDigest, [digest]) => format!("tdigest_merge({digest})"),
            // `arity()` is the single source of truth for how many states each
            // variant is built with, so this is unreachable by construction.
            _ => unreachable!("merge {self:?} built with {} states", states.len()),
        }
    }
}

/// One output aggregate, resolved against the declared rollup.
#[derive(Debug)]
struct RoutedMeasure {
    alias: String,
    merge: Merge,
    /// Rollup-table measure columns, one per state, in `merge` order.
    measures: Vec<String>,
    /// Raw-leg aggregate SQL, one per state, in `merge` order. Rendered from the
    /// measure's declared filter text rather than by unparsing the query's
    /// `Expr` — the matcher has already proven the two are canonically equal.
    raw: Vec<String>,
}

/// The union pays a second scan, a second aggregation and a union barrier, so a
/// sliver of certified interior is strictly worse than the raw plan it replaces.
const MIN_INTERIOR_FRACTION: i64 = 5;
const MIN_INTERIOR_BUCKETS: i64 = 2;

const fn floor_grain(value: i64, grain: i64) -> i64 {
    value - value.rem_euclid(grain)
}

const fn ceil_grain(value: i64, grain: i64) -> i64 {
    let remainder = value.rem_euclid(grain);
    if remainder == 0 { value } else { value + (grain - remainder) }
}

/// The grain-aligned `[a, b)` the rollup leg may own inside `[lo, hi)`, or
/// `None` when the raw plan should be left alone.
///
/// `horizon` is the exclusive bound of the certified, buffer-free prefix. Both
/// endpoints are snapped to a grain boundary because a rollup row is indivisible:
/// half of one cannot be given to a fringe. That alignment is the invariant the
/// whole rewrite rests on — with `a` or `b` off-grain the legs either double
/// count a bucket or drop one, and no aggregate can detect it afterwards.
pub(crate) fn interior(lo: i64, hi: i64, grain: i64, horizon: i64) -> Option<(i64, i64)> {
    let (start, end) = (ceil_grain(lo, grain), floor_grain(hi.min(horizon), grain));
    debug_assert!(start.rem_euclid(grain) == 0 && end.rem_euclid(grain) == 0, "interior endpoints must be grain-aligned");
    let width = end.checked_sub(start).filter(|width| *width > 0)?;
    (width >= MIN_INTERIOR_BUCKETS.saturating_mul(grain) && width >= hi.saturating_sub(lo) / MIN_INTERIOR_FRACTION).then_some((start, end))
}

#[derive(Debug)]
pub(crate) struct RoutedRollup {
    pub source: String,
    pub project_id: String,
    pub lo: i64,
    pub hi: i64,
    pub grain: i64,
    pub target: String,
    /// Projection expressions to re-apply above the rewrite, qualifiers stripped.
    pub outer_projection: Option<Vec<datafusion::logical_expr::Expr>>,
    /// A HAVING predicate to re-apply directly above the rewrite, below
    /// `outer_projection`. It references the aggregate's own output names.
    pub having: Option<datafusion::logical_expr::Expr>,
    /// Sort/Limit peeled off the query root, outermost first.
    pub wrappers: Vec<PlanWrapper>,
    row_filters: Vec<String>,
    /// `(expression, output alias)`. Every expression is valid, and means the
    /// same thing, on BOTH tables — which is what lets the union share them.
    groups: Vec<(String, String)>,
    measures: Vec<RoutedMeasure>,
}

impl RoutedRollup {
    /// A half-open range predicate. Only `>=`/`<` are ever emitted: an inclusive
    /// bound on either side of a shared boundary double counts a whole bucket.
    fn range_sql(&(start, end): &(i64, i64)) -> String {
        format!("(timestamp >= to_timestamp_micros({start}) AND timestamp < to_timestamp_micros({end}))")
    }

    fn quoted(alias: &str) -> String {
        format!("\"{}\"", alias.replace('"', "\"\""))
    }

    /// `GROUP BY 1, 2, …`, positional so it stays valid whether the select list
    /// carries synthetic leg aliases or the query's own names. Empty for an
    /// ungrouped aggregate.
    fn group_by(&self) -> String {
        if self.groups.is_empty() {
            return String::new();
        }
        format!(" GROUP BY {}", (1..=self.groups.len()).map(|index| index.to_string()).collect::<Vec<_>>().join(", "))
    }

    /// One partial-aggregate SELECT over `ranges`, with synthetic column names.
    ///
    /// The aliases are synthetic (`__g0`, `__s1_0`) rather than the query's, so a
    /// dashboard alias like `c` or `time` can never collide with a state column.
    fn leg(&self, table: &str, ranges: &[(i64, i64)], extra: &str) -> String {
        let select = self
            .groups
            .iter()
            .enumerate()
            .map(|(index, (expression, _))| format!("{expression} AS __g{index}"))
            .chain(self.measures.iter().enumerate().flat_map(|(index, measure)| {
                let states = if table == self.target {
                    measure.measures.iter().map(|column| format!("{}({column})", measure.merge.partial_op())).collect::<Vec<_>>()
                } else {
                    measure.raw.clone()
                };
                states.into_iter().enumerate().map(move |(state, sql)| format!("{sql} AS __s{index}_{state}"))
            }))
            .collect::<Vec<_>>()
            .join(", ");
        let ranges = ranges.iter().map(Self::range_sql).collect::<Vec<_>>().join(" OR ");
        let row_filters = self.row_filters.iter().map(|filter| format!(" AND ({filter})")).collect::<String>();
        let group_by = self.group_by();
        format!("SELECT {select} FROM {table} WHERE project_id = {} AND ({ranges}){extra}{row_filters}{group_by}", sql_literal(&self.project_id))
    }

    /// The rewrite. `interior` is the grain-aligned `[a, b)` the rollup leg owns;
    /// the raw leg owns exactly `[lo, a)` and `[b, hi)`, so the three are a
    /// partition of `[lo, hi)` — no gap, no overlap.
    ///
    /// When the interior is the whole window this collapses to a single
    /// statement against the rollup table, which is both cheaper and the shape
    /// the aligned fast path has always emitted.
    pub fn sql(&self, generations: &[(String, String)], interior: (i64, i64)) -> String {
        let generations = format!(
            " AND ({})",
            generations
                .iter()
                .map(|(date, generation)| format!("(date = {} AND rollup_generation = {})", sql_literal(date), sql_literal(generation)))
                .collect::<Vec<_>>()
                .join(" OR ")
        );
        let fringes: Vec<(i64, i64)> = [(self.lo, interior.0), (interior.1, self.hi)].into_iter().filter(|(start, end)| start < end).collect();
        if fringes.is_empty() {
            // Single leg: the rollup rows ARE the partial states, so the merge
            // applies directly to the measure columns.
            let select = self
                .groups
                .iter()
                .map(|(expression, alias)| format!("{expression} AS {}", Self::quoted(alias)))
                .chain(self.measures.iter().map(|measure| format!("{} AS {}", measure.merge.sql(&measure.measures), Self::quoted(&measure.alias))))
                .collect::<Vec<_>>()
                .join(", ");
            let row_filters = self.row_filters.iter().map(|filter| format!(" AND ({filter})")).collect::<String>();
            let group_by = self.group_by();
            return format!(
                "SELECT {select} FROM {} WHERE project_id = {} AND ({}){generations}{row_filters}{group_by}",
                self.target,
                sql_literal(&self.project_id),
                Self::range_sql(&interior),
            );
        }
        let outer = self
            .groups
            .iter()
            .enumerate()
            .map(|(index, (_, alias))| format!("__g{index} AS {}", Self::quoted(alias)))
            .chain(self.measures.iter().enumerate().map(|(index, measure)| {
                let states = (0..measure.merge.arity()).map(|state| format!("__s{index}_{state}")).collect::<Vec<_>>();
                format!("{} AS {}", measure.merge.sql(&states), Self::quoted(&measure.alias))
            }))
            .collect::<Vec<_>>()
            .join(", ");
        let group_by = self.group_by();
        format!(
            "SELECT {outer} FROM ({} UNION ALL {}) AS rollup_union{group_by}",
            self.leg(&self.target, std::slice::from_ref(&interior), &generations),
            self.leg(&self.source, &fringes, ""),
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

/// The literal that `column = '…'` compares against, in either operand order.
///
/// `None` for any other shape — including `project_id = other_column`, which
/// must stay a predicate the matcher cannot honour rather than being consumed.
fn eq_literal<'a>(expr: &'a datafusion::logical_expr::Expr, column: &str) -> Option<&'a str> {
    use datafusion::logical_expr::{Expr, Operator};
    let Expr::BinaryExpr(binary) = unaliased(expr) else { return None };
    (binary.op == Operator::Eq)
        .then_some([(&binary.left, &binary.right), (&binary.right, &binary.left)])?
        .into_iter()
        .find_map(|(name, value)| (column_name(name) == Some(column)).then(|| string_literal(value)).flatten())
}

/// A timestamp bound in microseconds, whatever precision the literal carries.
///
/// `now()` const-folds to a NANOSECOND literal, so accepting only microseconds
/// silently refused every `timestamp < now()` window — which is most ad-hoc
/// queries, and was measured missing in production while the identical window
/// written with explicit literals routed.
///
/// Sub-microsecond bounds convert by rounding UP, which is exact rather than
/// merely close: the column is microsecond-typed, so a row at `t` µs satisfies
/// `t*1000 >= lo_ns` exactly when `t >= ceil(lo_ns/1000)`, and `t*1000 < hi_ns`
/// exactly when `t < ceil(hi_ns/1000)`. The same ceiling therefore serves both
/// ends; flooring either one would shift the window by up to a microsecond and
/// silently include or drop rows.
fn timestamp_literal(expr: &datafusion::logical_expr::Expr) -> Option<i64> {
    use datafusion::scalar::ScalarValue;
    let ceil_div = |value: i64, per_micro: i64| value.checked_add(per_micro - 1).map(|v| v.div_euclid(per_micro));
    match unaliased(expr) {
        datafusion::logical_expr::Expr::Literal(value, _) => match value {
            ScalarValue::TimestampMicrosecond(Some(value), _) | ScalarValue::Int64(Some(value)) => Some(*value),
            ScalarValue::TimestampNanosecond(Some(value), _) => ceil_div(*value, 1_000),
            ScalarValue::TimestampMillisecond(Some(value), _) => value.checked_mul(1_000),
            ScalarValue::TimestampSecond(Some(value), _) => value.checked_mul(1_000_000),
            _ => None,
        },
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
        // `metric_name IN (…)` is how every metrics panel selects its series;
        // without this the whole otel_metrics dashboard is one residual filter
        // away from never routing.
        Expr::InList(list) => Some(format!(
            "{} {}IN ({})",
            dimension_filter_sql(&list.expr, dimensions)?,
            if list.negated { "NOT " } else { "" },
            list.list.iter().map(|item| dimension_filter_sql(item, dimensions)).collect::<Option<Vec<_>>>()?.join(", ")
        )),
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
        // Only a rename-free projection may be walked through. `hash AS name`
        // would otherwise make the matcher read a declared dimension off the
        // wrong source column and answer with the wrong values.
        LogicalPlan::Projection(projection) if projection.expr.iter().all(|expr| matches!(expr, datafusion::logical_expr::Expr::Column(_))) => {
            source_and_filters(&projection.input, filters)
        }
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
            // Canonicalized through the SAME pipeline the query side went
            // through — optimized (so literals carry the coerced type) and split
            // into conjuncts — or the two strings can never be equal and every
            // filtered measure is dead weight.
            Some(filter) => {
                // `SELECT timestamp`, never `SELECT *`: on a real session `*`
                // expands to every column, and the Variant rewriter wraps the
                // Variant ones in `variant_to_json(…)`. That projection is not
                // rename-free, so `source_and_filters` refuses to walk it and
                // EVERY filtered measure — hence every spec declaring one —
                // failed to resolve. Unit tests could not see it: a bare
                // `MemTable` session has no Variant rewriter registered.
                let plan =
                    session.create_logical_plan(&format!("SELECT timestamp FROM {source} WHERE {filter}")).await.map_err(|_| MissReason::UnknownFilter)?;
                let plan = session.optimize(&plan).map_err(|_| MissReason::UnknownFilter)?;
                let mut filters = Vec::new();
                source_and_filters(&plan, &mut filters).ok_or(MissReason::UnknownFilter)?;
                canonical_and(filters.iter().flat_map(datafusion::logical_expr::utils::split_conjunction))
            }
        };
        filters.push((measure, filter));
    }
    Ok(filters)
}

/// Strip table qualifiers from every column reference.
///
/// A HAVING predicate and an outer projection are lifted off the source plan,
/// so their group-by columns are qualified `otel_logs_and_spans.kind`. The
/// rewrite reads from the rollup table, so those would not resolve. Names are
/// unique in the rewrite's output by construction — each is a SELECT alias.
fn unqualified(expr: datafusion::logical_expr::Expr) -> Result<datafusion::logical_expr::Expr, MissReason> {
    use datafusion::{
        common::tree_node::{Transformed, TreeNode},
        logical_expr::Expr,
    };
    expr.transform(|expr| {
        Ok(match expr {
            Expr::Column(column) => Transformed::yes(Expr::Column(datafusion::common::Column::new_unqualified(column.name))),
            expr => Transformed::no(expr),
        })
    })
    .map(|transformed| transformed.data)
    .map_err(|_| MissReason::UnsupportedShape)
}

/// An `Aggregate`, or the `Filter(Aggregate)` a HAVING clause plans to.
fn aggregate_with_having(
    plan: &datafusion::logical_expr::LogicalPlan,
) -> Option<(&datafusion::logical_expr::Aggregate, Option<datafusion::logical_expr::Expr>)> {
    use datafusion::logical_expr::LogicalPlan;
    match plan {
        LogicalPlan::Aggregate(aggregate) => Some((aggregate, None)),
        LogicalPlan::Filter(filter) => match filter.input.as_ref() {
            LogicalPlan::Aggregate(aggregate) => Some((aggregate, Some(filter.predicate.clone()))),
            _ => None,
        },
        _ => None,
    }
}

/// Decline a plan the matcher cannot rewrite.
///
/// `Ok(None)` means "not our business" and stays uncounted — otherwise every
/// ordinary SELECT would land in `rollup_miss`. But when the plan *is* an
/// aggregate over a rollup-declaring source, the decline is a real shape gap
/// and must be visible: an operator who enables read routing and sees zero
/// hits AND zero misses cannot tell the gate from the matcher.
fn decline(plan: &datafusion::logical_expr::LogicalPlan) -> Result<Option<RoutedRollup>, MissReason> {
    use datafusion::logical_expr::LogicalPlan;
    fn aggregate_source(plan: &LogicalPlan) -> Option<String> {
        match plan {
            LogicalPlan::Aggregate(aggregate) => source_and_filters(&aggregate.input, &mut Vec::new()),
            plan => plan.inputs().into_iter().find_map(aggregate_source),
        }
    }
    match aggregate_source(plan) {
        Some(source) if crate::schema_loader::get_schema(&source).is_some_and(|schema| !schema.rollups.is_empty()) => Err(MissReason::UnsupportedShape),
        _ => Ok(None),
    }
}

/// Match a direct aggregate over one declared source table and construct its
/// re-aggregation request. It deliberately rejects a shape it cannot prove.
pub(crate) async fn match_aggregate(
    plan: &datafusion::logical_expr::LogicalPlan, session: &datafusion::execution::context::SessionState,
) -> Result<Option<RoutedRollup>, MissReason> {
    use datafusion::logical_expr::{Expr, LogicalPlan};

    // Every real dashboard query ends in `ORDER BY … DESC`, often with a LIMIT,
    // so the optimized root is `Sort(Projection(Aggregate))`. Peel those and
    // hand them back for the caller to re-apply above the rewrite — matching
    // only a bare aggregate root would leave the feature inert in production.
    let mut wrappers = Vec::new();
    let mut plan = plan;
    while let Some((wrapper, input)) = PlanWrapper::peel(plan) {
        wrappers.push(wrapper);
        plan = input;
    }

    let (aggregate, having, output_names, outer_projection) = match aggregate_with_having(plan) {
        Some((aggregate, having)) => (aggregate, having, None, None),
        None => {
            let LogicalPlan::Projection(projection) = plan else { return decline(plan) };
            let Some((aggregate, having)) = aggregate_with_having(&projection.input) else { return decline(plan) };
            // A HAVING between the two breaks the positional projection-to-
            // aggregate name mapping, so keep the aggregate's own schema names
            // — the filter references those — and re-apply the projection above
            // the rewrite rather than absorbing it into the SELECT aliases.
            if having.is_none() && projection.expr.iter().all(|expr| matches!(unaliased(expr), Expr::Column(_))) {
                (aggregate, None, Some(projection.schema.fields().iter().map(|field| field.name().to_string()).collect::<Vec<_>>()), None)
            } else {
                (aggregate, having, None, Some(projection.clone()))
            }
        }
    };
    let mut predicates = Vec::new();
    let Some(source) = source_and_filters(&aggregate.input, &mut predicates) else { return Ok(None) };
    let Some(schema) = crate::schema_loader::get_schema(&source).filter(|schema| !schema.rollups.is_empty()) else { return Ok(None) };

    // Try every declared rollup, coarsest grain first: a coarser grain reads
    // strictly fewer rows for the same answer. Ties break toward the narrower
    // dimension set, which is the smaller table. A spec that cannot serve this
    // query — wrong grain, missing dimension, missing measure — declines and the
    // next one is tried, so adding a spec can only ever widen what routes.
    let mut candidates: Vec<&RollupSpec> = schema.rollups.iter().collect();
    candidates.sort_by_key(|spec| (std::cmp::Reverse(spec.grain_micros().unwrap_or(0)), spec.dimensions.len()));
    let mut first_miss = None;
    for spec in candidates {
        match route_with_spec(spec, &source, &schema.table_name, &predicates, aggregate, output_names.as_ref(), session).await {
            Ok(mut route) => {
                route.outer_projection =
                    outer_projection.map(|projection| projection.expr.into_iter().map(unqualified).collect::<Result<Vec<_>, _>>()).transpose()?;
                route.having = having.map(unqualified).transpose()?;
                route.wrappers = wrappers;
                return Ok(Some(route));
            }
            // Report the FIRST spec's reason: it is the one the operator most
            // likely intended to serve this query, so it is the actionable gap.
            Err(reason) => first_miss = first_miss.or(Some(reason)),
        }
    }
    Err(first_miss.unwrap_or(MissReason::UnsupportedShape))
}

/// Resolve one query against one declared rollup spec.
///
/// Returns a route with `outer_projection`, `having` and `wrappers` left empty —
/// those are properties of the query root, not of the spec, and the caller fills
/// them in once a spec has won.
#[allow(clippy::too_many_arguments)]
async fn route_with_spec(
    spec: &RollupSpec, source: &str, table_name: &str, predicates: &[datafusion::logical_expr::Expr], aggregate: &datafusion::logical_expr::Aggregate,
    output_names: Option<&Vec<String>>, session: &datafusion::execution::context::SessionState,
) -> Result<RoutedRollup, MissReason> {
    use datafusion::logical_expr::{Expr, Operator, utils::split_conjunction};

    let mut project_id = None;
    let (mut lo, mut hi) = (None, None);
    let mut row_filters = Vec::new();
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
                    _ => return Err(MissReason::UnknownFilter),
                }
            }
            // A predicate we can push into the rollup scan is a dimension filter;
            // one we cannot may NOT be answered by picking a measure that was
            // pre-filtered the same way. The measure carries the right value, but
            // the raw query also *eliminates* the groups and buckets where
            // nothing matched, and re-aggregating the rollup resurrects them as
            // 0/NULL rows. (`count(*) FILTER (WHERE …)` is unaffected — an
            // aggregate filter changes values, never which groups exist.)
            term => match (eq_literal(term, "project_id"), dimension_filter_sql(term, &spec.dimensions)) {
                // Two different literals cannot both hold; never keep the last.
                (Some(value), _) if project_id.as_deref().is_some_and(|current| current != value) => return Err(MissReason::MissingProject),
                (Some(value), _) => project_id = Some(value.to_string()),
                (None, Some(filter)) => row_filters.push(filter),
                (None, None) => return Err(MissReason::UnknownFilter),
            },
        }
    }
    let project_id = project_id.ok_or(MissReason::MissingProject)?;
    let (lo, hi) = lo.zip(hi).filter(|(lo, hi)| lo < hi).ok_or(MissReason::UnboundedTime)?;
    let grain = spec.grain_micros().ok_or(MissReason::UnsupportedShape)?;
    // A grain too coarse for the window can never yield a usable interior — the
    // aligned span inside it is empty or below the cost floor. Rejecting it here
    // rather than at interior() matters because specs are tried coarsest-first:
    // otherwise a 1h tier would shadow the 1m tier that CAN serve a 10-minute
    // window, and the query would miss entirely instead of routing.
    if hi.saturating_sub(lo) < grain.saturating_mul(MIN_INTERIOR_BUCKETS) {
        return Err(MissReason::TinyInterior);
    }
    let configured_filters = measure_filters(session, source, spec).await?;

    let mut groups = Vec::new();
    for (index, expression) in aggregate.group_expr.iter().enumerate() {
        let alias = output_names.and_then(|names| names.get(index)).map_or_else(|| aggregate.schema.field(index).name(), String::as_str);
        let expression = match unaliased(expression) {
            Expr::Column(column) if spec.dimensions.iter().any(|dimension| dimension == &column.name) => column.name.clone(),
            Expr::ScalarFunction(function)
                if function.name().eq_ignore_ascii_case("time_bucket") && function.args.len() == 2 && column_name(&function.args[1]) == Some("timestamp") =>
            {
                let interval = string_literal(&function.args[0]).ok_or(MissReason::UnsupportedShape)?;
                let width = parse_bucket_micros(interval).ok_or(MissReason::UnsupportedShape)?;
                // A width that is not a whole number of grains would make one
                // rollup row straddle two output buckets, and no state can be
                // split across them. Raw fringes do not change this: the
                // interior is still answered by whole rollup rows.
                if width < grain || width % grain != 0 {
                    return Err(MissReason::PartialBucket);
                }
                format!("time_bucket({}, timestamp)", sql_literal(interval))
            }
            Expr::Column(_) => return Err(MissReason::UnknownGroupBy),
            _ => return Err(MissReason::UnsupportedShape),
        };
        groups.push((expression, alias.to_string()));
    }

    let mut measures = Vec::new();
    for (index, expression) in aggregate.aggr_expr.iter().enumerate() {
        let output_index = aggregate.group_expr.len() + index;
        let (expression, alias) = match expression {
            Expr::Alias(alias) => (alias.expr.as_ref(), output_names.and_then(|names| names.get(output_index)).cloned().unwrap_or_else(|| alias.name.clone())),
            expression => (
                expression,
                output_names.and_then(|names| names.get(output_index)).cloned().unwrap_or_else(|| aggregate.schema.field(output_index).name().to_string()),
            ),
        };
        let Expr::AggregateFunction(function) = unaliased(expression) else { return Err(MissReason::NonDecomposableAggregate) };
        if function.params.distinct || !function.params.order_by.is_empty() {
            return Err(MissReason::NonDecomposableAggregate);
        }
        let filter = canonical_and(function.params.filter.iter().flat_map(|filter| split_conjunction(filter.as_ref())));
        let name = function.func.name().to_ascii_lowercase();
        let column = function.params.args.first().and_then(column_name).map(str::to_string);
        let measure = |aggregate: &str, column: Option<&str>| {
            configured_filters
                .iter()
                .find(|(measure, measure_filter)| measure.agg == aggregate && measure.column.as_deref() == column && *measure_filter == filter)
                .map(|(measure, _)| *measure)
        };
        // The raw leg reproduces the measure's DECLARED filter text verbatim.
        // The matcher has already proven the query's aggregate filter
        // canonicalizes to the same predicate, so this is exact — and it avoids
        // unparsing an optimized `Expr` back into SQL.
        let raw = |expression: String, declared: Option<&String>| match declared {
            Some(filter) => format!("{expression} FILTER (WHERE {filter})"),
            None => expression,
        };
        let (merge, resolved) = match name.as_str() {
            "count" => (Merge::Count, measure("count", column.as_deref()).map(|m| vec![m])),
            "sum" => (Merge::Sum, measure("sum", column.as_deref()).map(|m| vec![m])),
            "min" => (Merge::Min, measure("min", column.as_deref()).map(|m| vec![m])),
            "max" => (Merge::Max, measure("max", column.as_deref()).map(|m| vec![m])),
            "avg" => (Merge::Avg, measure("sum", column.as_deref()).zip(measure("count", column.as_deref())).map(|(sum, count)| vec![sum, count])),
            "percentile_agg" => (Merge::TDigest, measure("tdigest", column.as_deref()).map(|m| vec![m])),
            _ => return Err(MissReason::NonDecomposableAggregate),
        };
        let resolved = resolved.ok_or(MissReason::MissingMeasure)?;
        debug_assert_eq!(resolved.len(), merge.arity(), "{merge:?} resolved the wrong number of measures");
        let raw = resolved
            .iter()
            .map(|measure| {
                let aggregate = measure.agg.to_uppercase();
                let expression = match (merge, measure.column.as_deref()) {
                    (Merge::TDigest, Some(column)) => format!("percentile_agg(CAST({column} AS DOUBLE))"),
                    (_, None) => "COUNT(*)".to_string(),
                    (_, Some(column)) => format!("{aggregate}({column})"),
                };
                raw(expression, measure.filter.as_ref())
            })
            .collect();
        measures.push(RoutedMeasure { alias, merge, measures: resolved.iter().map(|measure| measure.name.clone()).collect(), raw });
    }

    Ok(RoutedRollup {
        source: source.to_string(),
        project_id,
        lo,
        hi,
        grain,
        target: spec.table_name(table_name),
        outer_projection: None,
        having: None,
        wrappers: Vec::new(),
        row_filters,
        groups,
        measures,
    })
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
        // A rollup must declare NO dedup keys. `replace_rollup_partition` removes
        // every existing file in the partition in the same commit that adds the
        // new ones, so duplicates are impossible — and declaring keys made every
        // read plan a `DedupExec` over them, which the rewrite's
        // dimensions-and-measures projection does not carry. In production that
        // turned every routed query into "DedupExec key `id` not in input
        // schema" and then a failed raw fallback.
        assert!(target.dedup_keys.is_empty(), "a rollup must not require read-time dedup: {:?}", target.dedup_keys);
    }

    #[test]
    fn build_sql_uses_exact_count_and_tdigest_states() {
        let sql = build_partition_sql(&spec(), SOURCE, "pro'ject", "2026-08-01").expect("valid SQL");
        assert!(sql.contains("COUNT(duration) AS duration_count"));
        assert!(sql.contains("percentile_agg(CAST(duration AS DOUBLE))"));
        assert!(sql.contains("project_id = 'pro''ject'"));
        assert!(sql.contains("GROUP BY 1, 2, 3, 4"));
    }

    const TARGET: &str = "otel_logs_and_spans_rollup_dashboard_1m_v2";
    /// Ten grains wide. A window narrower than `MIN_INTERIOR_BUCKETS` grains can
    /// never route — the aligned interior would be at most one bucket — so a
    /// one-minute fixture would exercise the rejection path, not the matcher.
    const WINDOW: &str = "timestamp >= to_timestamp_micros(60000000) AND timestamp < to_timestamp_micros(660000000)";
    const WIDE_HORIZON: i64 = 540_000_000;

    /// Register the source AND its generated rollup so a route can be planned
    /// back into a real logical plan.
    async fn session() -> datafusion::execution::context::SessionState {
        let mut ctx = datafusion::prelude::SessionContext::new();
        crate::functions::register_custom_functions(&mut ctx).expect("functions register");
        // Every declared tier, not just the fine one: specs are tried
        // coarsest-first, so a session missing the coarse table could not plan a
        // rewrite the matcher legitimately chose.
        let targets = crate::schema_loader::get_schema(SOURCE).expect("source schema").rollups.iter().map(|spec| spec.table_name(SOURCE)).collect::<Vec<_>>();
        for table_name in std::iter::once(SOURCE.to_string()).chain(targets) {
            let table = datafusion::datasource::MemTable::try_new(crate::schema_loader::get_schema(&table_name).expect("schema").schema_ref(), vec![vec![]])
                .expect("empty table");
            ctx.register_table(table_name.as_str(), Arc::new(table)).expect("register table");
        }
        ctx.state()
    }

    async fn optimized(state: &datafusion::execution::context::SessionState, sql: &str) -> datafusion::logical_expr::LogicalPlan {
        state.optimize(&state.create_logical_plan(sql).await.expect("parse")).expect("optimize")
    }

    async fn route_for(state: &datafusion::execution::context::SessionState, sql: &str) -> Result<Option<RoutedRollup>, MissReason> {
        match_aggregate(&optimized(state, sql).await, state).await
    }

    /// The rewrite when the rollup owns the whole window — the single-leg shape.
    fn generated_sql(route: &RoutedRollup) -> String {
        route.sql(&[("1970-01-01".into(), "generation".into())], (route.lo, route.hi))
    }

    /// The rewrite when only part of the window is certified, so raw fringes and
    /// a live tail are unioned in.
    fn hybrid_sql(route: &RoutedRollup, horizon: i64) -> String {
        let interior = interior(route.lo, route.hi, route.grain, horizon).expect("a routable interior");
        route.sql(&[("1970-01-01".into(), "generation".into())], interior)
    }

    #[tokio::test]
    async fn matcher_rewrites_a_certifiable_count_aggregate() {
        let state = session().await;
        let route = route_for(&state, &format!("SELECT COUNT(*) FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}"))
            .await
            .expect("match count")
            .expect("declared rollup route");
        assert_eq!(route.target, TARGET);
        assert_eq!(route.project_id, "project");
        assert!(generated_sql(&route).contains("COALESCE(SUM(request_count), 0)"));
    }

    #[tokio::test]
    async fn matcher_preserves_a_scalar_outer_projection() {
        let state = session().await;
        let sql = format!("SELECT COUNT(*) + 1 AS total FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}");
        let plan = optimized(&state, &sql).await;
        let route = route_for(&state, &sql).await.expect("match count").expect("declared rollup route");
        let rewritten = state.create_logical_plan(&generated_sql(&route)).await.expect("parse rollup query");
        let projection = route.outer_projection.expect("scalar projection");
        let rewritten = datafusion::logical_expr::LogicalPlan::Projection(
            datafusion::logical_expr::logical_plan::Projection::try_new(projection, Arc::new(rewritten)).expect("reapply projection"),
        );
        assert_eq!(rewritten.schema(), plan.schema());
    }

    #[tokio::test]
    async fn matcher_applies_dimension_predicates_to_the_rollup_scan() {
        let state = session().await;
        let route = route_for(&state, &format!("SELECT COUNT(*) FROM {SOURCE} WHERE project_id = 'project' AND kind = 'server' AND {WINDOW}"))
            .await
            .expect("match count")
            .expect("declared rollup route");
        assert!(generated_sql(&route).contains("AND (kind = 'server')"));
    }

    /// The three ranges must PARTITION `[lo, hi)`: a gap drops rows, an overlap
    /// counts them twice, and no downstream aggregate can detect either. Both
    /// interior endpoints must also be grain-aligned, because a rollup row is
    /// indivisible — half of one cannot be handed to a fringe.
    #[test]
    fn the_interior_and_its_fringes_partition_the_window() {
        let grain = 60_000_000;
        for lo in [0_i64, 1, 59_999_999, 60_000_000, 137_000_017] {
            for width in [grain, grain * 3, grain * 40, grain * 40 + 7] {
                for horizon_offset in [0_i64, 1, grain, grain * 7, width] {
                    let hi = lo + width;
                    let horizon = lo + horizon_offset;
                    let Some((start, end)) = interior(lo, hi, grain, horizon) else { continue };
                    assert_eq!(start.rem_euclid(grain), 0, "interior start {start} must be grain-aligned");
                    assert_eq!(end.rem_euclid(grain), 0, "interior end {end} must be grain-aligned");
                    assert!(lo <= start && start < end && end <= hi, "interior ({start},{end}) must sit inside [{lo},{hi})");
                    assert!(end <= horizon, "interior must not reach past the certified horizon {horizon}");
                    // Contiguity: fringe, interior, fringe, back to back.
                    let covered = (start - lo) + (end - start) + (hi - end);
                    assert_eq!(covered, hi - lo, "the three ranges must cover [{lo},{hi}) exactly once");
                }
            }
        }
    }

    /// The shape production actually sends: microsecond-precision bounds ending
    /// at wall-clock `now`. Before raw fringes this was refused outright, which
    /// is why the feature never served a single query.
    #[tokio::test]
    async fn an_unaligned_live_window_emits_a_raw_fringe_and_a_live_tail() {
        let state = session().await;
        let (lo, hi) = (60_000_017_i64, 660_000_042_i64);
        let sql = format!(
            "SELECT resource___service___name, COUNT(*) AS c FROM {SOURCE} \
             WHERE project_id = 'project' AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi}) GROUP BY 1"
        );
        let route = route_for(&state, &sql).await.expect("match").expect("route");
        let horizon = 540_000_000;
        let generated = hybrid_sql(&route, horizon);
        assert!(generated.contains("UNION ALL"), "an uncertified tail must union a raw leg: {generated}");
        // The rollup leg owns exactly [ceil_g(lo), floor_g(horizon)); the raw leg
        // owns the two fringes. Shared endpoints appear once as `<` and once as
        // `>=`, never as an inclusive bound on both sides.
        assert!(generated.contains(&format!("timestamp >= to_timestamp_micros(120000000) AND timestamp < to_timestamp_micros({horizon})")));
        assert!(generated.contains(&format!("timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros(120000000)")));
        assert!(generated.contains(&format!("timestamp >= to_timestamp_micros({horizon}) AND timestamp < to_timestamp_micros({hi})")));
        assert!(!generated.contains("<="), "an inclusive bound at a shared boundary double counts a bucket: {generated}");
        assert!(generated.contains(TARGET) && generated.contains(&format!("FROM {SOURCE}")), "both legs must be present: {generated}");
    }

    /// An average is not a mergeable state. If the legs carried `avg` each, the
    /// union would average two averages and silently weight a 3-row minute the
    /// same as a 3-million-row one.
    #[tokio::test]
    async fn avg_unions_as_separate_sum_and_count_states() {
        let state = session().await;
        let route =
            route_for(&state, &format!("SELECT avg(duration) FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}")).await.expect("match").expect("route");
        let generated = hybrid_sql(&route, WIDE_HORIZON);
        // The query's own output name is `avg(otel_logs_and_spans.duration)`, so
        // only the union body can be checked for a leg-level average.
        let legs = generated.split_once("FROM (").expect("union body").1;
        assert!(!legs.contains("avg("), "no leg may compute an average: {legs}");
        assert!(generated.contains("SUM(duration_sum) AS __s0_0") && generated.contains("SUM(duration_count) AS __s0_1"), "rollup leg states: {generated}");
        assert!(generated.contains("SUM(duration) AS __s0_0") && generated.contains("COUNT(duration) AS __s0_1"), "raw leg states: {generated}");
        assert!(generated.contains("CAST(SUM(__s0_0) AS DOUBLE) / CAST(SUM(__s0_1) AS DOUBLE)"), "the merge must divide in floating point: {generated}");
    }

    /// The raw leg must apply the SAME predicate the stored measure was built
    /// with, or the two legs answer different questions and the union is a
    /// plausible-looking wrong number.
    #[tokio::test]
    async fn the_raw_leg_reproduces_the_measure_filter_verbatim() {
        let state = session().await;
        let filter = "kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http'";
        let sql = format!(
            "SELECT approx_percentile(0.95, percentile_agg(CAST(duration AS DOUBLE)) FILTER (WHERE {filter})) AS p95 \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}"
        );
        let route = route_for(&state, &sql).await.expect("match").expect("route");
        let generated = hybrid_sql(&route, WIDE_HORIZON);
        assert!(
            generated.contains(&format!("percentile_agg(CAST(duration AS DOUBLE)) FILTER (WHERE {filter})")),
            "raw leg must carry the declared filter: {generated}"
        );
        assert!(generated.contains("tdigest_merge(server_duration_digest) AS __s0_0"), "rollup leg merges stored digest state: {generated}");
        assert!(generated.contains("tdigest_merge(__s0_0)"), "the outer merge folds both legs' digests: {generated}");
    }

    /// The union widens nullability and re-types every state column, so the
    /// acceptance gate in `dml.rs` is doing more work here than on the single-leg
    /// path. This is also what catches a `Binary` state unifying to `BinaryView`,
    /// which `tdigest_merge` cannot downcast.
    #[tokio::test]
    async fn the_union_rewrite_matches_the_original_schema() {
        let state = session().await;
        let sql = format!(
            "SELECT time_bucket('10 minutes', timestamp) AS bucket, resource___service___name, COUNT(*) AS c, avg(duration) AS mean, \
                    min(duration) AS lo, max(duration) AS hi \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1, 2 ORDER BY 1 DESC LIMIT 10"
        );
        let original = optimized(&state, &sql).await;
        let route = route_for(&state, &sql).await.expect("match").expect("route");
        let generated = hybrid_sql(&route, WIDE_HORIZON);
        assert!(generated.contains("UNION ALL"), "precondition: this is the union path");
        let plan = state.create_logical_plan(&generated).await.expect("parse union rewrite");
        let rewrapped = PlanWrapper::rewrap(route.wrappers, plan);
        rewrapped.schema().has_equivalent_names_and_types(original.schema()).expect("names and types must match");
    }

    /// The metrics dashboard's shape: `AVG(value)` and a percentile, grouped by
    /// `metric_name`, selected with `metric_name IN (…)`. The `IN` is the part
    /// that used to fall through to `unknown_filter` and disqualify the panel.
    #[tokio::test]
    async fn a_metrics_panel_routes_with_an_in_filter_and_a_digest() {
        let mut ctx = datafusion::prelude::SessionContext::new();
        crate::functions::register_custom_functions(&mut ctx).expect("functions register");
        for table_name in ["otel_metrics", "otel_metrics_rollup_metrics_1m_v1"] {
            let table = datafusion::datasource::MemTable::try_new(crate::schema_loader::get_schema(table_name).expect("schema").schema_ref(), vec![vec![]])
                .expect("empty table");
            ctx.register_table(table_name, Arc::new(table)).expect("register table");
        }
        let state = ctx.state();
        let sql = format!(
            "SELECT time_bucket('1 minute', timestamp) AS bucket, metric_name, AVG(value) AS mean, \
                    approx_percentile(0.95, percentile_agg(CAST(value AS DOUBLE))) AS p95 \
             FROM otel_metrics \
             WHERE project_id = 'project' \
               AND metric_name IN ('system.cpu.load_average.1m', 'system.cpu.load_average.5m', 'redis.memory.used', 'redis.memory.rss') \
               AND {WINDOW} \
             GROUP BY 1, 2 ORDER BY 1 DESC"
        );
        let original = optimized(&state, &sql).await;
        let route = match_aggregate(&original, &state).await.expect("match").expect("route");
        assert_eq!(route.target, "otel_metrics_rollup_metrics_1m_v1");
        let generated = hybrid_sql(&route, WIDE_HORIZON);
        // DataFusion inlines a short `IN` to `OR`s, so this list is deliberately
        // long enough to survive as an `InList` and exercise that branch.
        assert_eq!(
            generated.matches("metric_name IN ('system.cpu.load_average.1m', 'system.cpu.load_average.5m', 'redis.memory.used', 'redis.memory.rss')").count(),
            2,
            "the IN filter must be pushed into BOTH legs, not left residual: {generated}"
        );
        assert!(generated.contains("tdigest_merge(value_digest)") && generated.contains("percentile_agg(CAST(value AS DOUBLE))"), "{generated}");
        // `approx_percentile(…)` is a scalar over the aggregate, so the query's
        // own projection is re-applied above the rewrite — exactly as `dml.rs`
        // does — and only then must the schemas agree.
        let plan = state.create_logical_plan(&generated).await.expect("parse metrics rewrite");
        let plan = datafusion::logical_expr::LogicalPlan::Projection(
            datafusion::logical_expr::logical_plan::Projection::try_new(route.outer_projection.expect("scalar projection"), Arc::new(plan))
                .expect("reapply projection"),
        );
        let rewrapped = PlanWrapper::rewrap(route.wrappers, plan);
        rewrapped.schema().has_equivalent_names_and_types(original.schema()).expect("names and types must match");
    }

    /// `now()` folds to a NANOSECOND literal, so a microsecond-only matcher
    /// refused every `timestamp < now()` window. Measured in production: the
    /// same window written with explicit literals routed, with `now()` it missed.
    #[test]
    fn a_timestamp_bound_is_read_at_any_precision() {
        use datafusion::{logical_expr::Expr, scalar::ScalarValue};
        let literal = |value: ScalarValue| Expr::Literal(value, None);
        assert_eq!(timestamp_literal(&literal(ScalarValue::TimestampMicrosecond(Some(1_500), None))), Some(1_500));
        assert_eq!(timestamp_literal(&literal(ScalarValue::TimestampMillisecond(Some(2), None))), Some(2_000));
        assert_eq!(timestamp_literal(&literal(ScalarValue::TimestampSecond(Some(3), None))), Some(3_000_000));
        // Rounds UP, which is exact against a microsecond column: a row at 2µs
        // satisfies `ts >= 1500ns`, a row at 1µs does not.
        assert_eq!(timestamp_literal(&literal(ScalarValue::TimestampNanosecond(Some(1_500), None))), Some(2));
        assert_eq!(timestamp_literal(&literal(ScalarValue::TimestampNanosecond(Some(2_000), None))), Some(2));
        assert_eq!(timestamp_literal(&literal(ScalarValue::Utf8(Some("nope".into())))), None);
    }

    /// Coarsest-first selection must still respect the window. A 1h tier reads
    /// 60x fewer rows for a 30-day chart, but it cannot answer a 10-minute one —
    /// and because it is tried FIRST, letting it match would shadow the 1m tier
    /// and turn a query that used to route into a miss.
    #[tokio::test]
    async fn grain_selection_prefers_the_coarsest_tier_the_window_can_use() {
        let state = session().await;
        let wide = format!(
            "SELECT COUNT(*) FROM {SOURCE} WHERE project_id = 'project' \
               AND timestamp >= to_timestamp_micros(0) AND timestamp < to_timestamp_micros(864000000000) \
             GROUP BY time_bucket('1 hours', timestamp)"
        );
        assert_eq!(
            route_for(&state, &wide).await.expect("match").expect("route").target,
            "otel_logs_and_spans_rollup_dashboard_1h_v1",
            "a 10-day window bucketed hourly must use the coarse tier"
        );

        // Ten minutes: the 1h tier cannot cover it, so the 1m tier must win.
        let narrow = format!(
            "SELECT COUNT(*) FROM {SOURCE} WHERE project_id = 'project' \
               AND timestamp >= to_timestamp_micros(0) AND timestamp < to_timestamp_micros(600000000) \
             GROUP BY time_bucket('5 minutes', timestamp)"
        );
        assert_eq!(route_for(&state, &narrow).await.expect("match").expect("route").target, TARGET, "a 10-minute window must fall back to the fine tier");
    }

    /// A sliver of certified interior is strictly worse than the raw plan: the
    /// fringes still scan nearly the whole window, and the rollup leg plus the
    /// union barrier are pure overhead on top.
    #[test]
    fn a_sliver_of_certified_interior_declines_the_union() {
        let grain = 60_000_000;
        let (lo, hi) = (0, grain * 100);
        assert_eq!(interior(lo, hi, grain, grain * 3), None, "3 of 100 buckets is not worth a second scan");
        assert_eq!(interior(lo, hi, grain, grain), None, "one bucket is below the floor even when it is the whole horizon");
        assert!(interior(lo, hi, grain, grain * 40).is_some(), "40 of 100 buckets is worth it");
    }

    /// A dimension-grouped query is the shape every dashboard panel sends. The
    /// rewrite's aliases are unqualified while the source aggregate keeps the
    /// table qualifier, so a derived `==` on DFSchema rejected all of them —
    /// the feature was inert for exactly the queries it was built for.
    #[tokio::test]
    async fn a_grouped_rewrite_matches_the_original_schema_despite_qualifiers() {
        let state = session().await;
        let sql = format!("SELECT resource___service___name, COUNT(*) FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1");
        let original = optimized(&state, &sql).await;
        let route = route_for(&state, &sql).await.expect("match").expect("route");
        let rewritten = state.create_logical_plan(&generated_sql(&route)).await.expect("parse rollup query");
        assert_ne!(rewritten.schema(), original.schema(), "precondition: the qualifiers really do differ");
        rewritten.schema().has_equivalent_names_and_types(original.schema()).expect("names and types must match");
    }

    /// `ORDER BY ... DESC` is on every real dashboard query, so a matcher that
    /// only accepts a bare aggregate root never fires in production.
    #[tokio::test]
    async fn an_order_by_above_the_aggregate_is_peeled_and_reapplied() {
        let state = session().await;
        let sql = format!(
            "SELECT resource___service___name, COUNT(*) AS c FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1 ORDER BY 1 DESC LIMIT 10"
        );
        let original = optimized(&state, &sql).await;
        let route = route_for(&state, &sql).await.expect("match").expect("route");
        assert!(!route.wrappers.is_empty(), "Sort/Limit must be peeled, not rejected");
        let plan = state.create_logical_plan(&generated_sql(&route)).await.expect("parse rollup query");
        let rewrapped = PlanWrapper::rewrap(route.wrappers, plan);
        // The optimizer folds the LIMIT into the Sort's `fetch`, so one Sort is
        // the whole peeled prefix — the ordering must be back on top of the
        // rewrite, or a routed dashboard query returns rows in rollup order.
        let datafusion::logical_expr::LogicalPlan::Sort(sort) = &rewrapped else { panic!("the peeled ORDER BY must be back on top: {rewrapped:?}") };
        assert_eq!(sort.fetch, Some(10), "the LIMIT folded into the Sort must survive too");
        rewrapped.schema().has_equivalent_names_and_types(original.schema()).expect("names and types must match");
    }

    /// `HAVING` plans to a `Filter` between the aggregate and the projection,
    /// which used to abort routing. It must be re-applied above the rewrite —
    /// dropping it would return the rows the query asked to exclude.
    #[tokio::test]
    async fn a_having_clause_is_reapplied_above_the_rewrite() {
        let state = session().await;
        let sql = format!("SELECT kind, COUNT(*) AS c FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1 HAVING COUNT(*) > 0");
        let original = optimized(&state, &sql).await;
        let route = route_for(&state, &sql).await.expect("match").expect("route");
        let having = route.having.clone().expect("the HAVING predicate must be carried, not dropped");
        let plan = state.create_logical_plan(&generated_sql(&route)).await.expect("parse rollup query");
        let filtered =
            datafusion::logical_expr::LogicalPlan::Filter(datafusion::logical_expr::Filter::try_new(having, Arc::new(plan)).expect("reapply having"));
        let rebuilt = match route.outer_projection {
            Some(projection) => datafusion::logical_expr::LogicalPlan::Projection(
                datafusion::logical_expr::logical_plan::Projection::try_new(projection, Arc::new(filtered)).expect("reapply projection"),
            ),
            None => filtered,
        };
        rebuilt.schema().has_equivalent_names_and_types(original.schema()).expect("names and types must match");
    }

    /// A shape gap over a rollup-declaring source must reach `rollup_miss`;
    /// a plan that simply is not our business must stay uncounted, or every
    /// ordinary SELECT would pollute the counter.
    #[tokio::test]
    async fn an_unsupported_shape_is_counted_but_an_unrelated_query_is_not() {
        let state = session().await;
        let union = format!(
            "SELECT kind, COUNT(*) AS c FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1 \
             UNION ALL SELECT kind, COUNT(*) AS c FROM {SOURCE} WHERE project_id = 'other' AND {WINDOW} GROUP BY 1"
        );
        let unsupported = route_for(&state, &union).await;
        assert_eq!(unsupported.err(), Some(MissReason::UnsupportedShape), "a rollup source we cannot rewrite must be visible");
        let unrelated = route_for(&state, &format!("SELECT kind FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}")).await;
        assert!(matches!(unrelated, Ok(None)), "a non-aggregate query must not be counted as a miss: {unrelated:?}");
    }

    /// An `avg` over two Int64 measures divides as integers; the CASE's DOUBLE
    /// only widens an already-truncated value, so the schema gate cannot see it.
    #[tokio::test]
    async fn avg_casts_before_dividing_so_it_does_not_truncate() {
        let state = session().await;
        let route =
            route_for(&state, &format!("SELECT avg(duration) FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}")).await.expect("match").expect("route");
        let sql = generated_sql(&route);
        assert!(sql.contains("CAST(SUM(duration_sum) AS DOUBLE) / CAST(SUM(duration_count) AS DOUBLE)"), "avg must divide in floating point: {sql}");
    }

    /// A `WHERE` predicate that only *selects* a pre-filtered measure is not
    /// applied by the rewrite, so groups and buckets the raw query eliminates
    /// come back as 0/NULL rows. Refuse the route instead.
    #[tokio::test]
    async fn a_residual_row_filter_refuses_the_route_rather_than_inventing_zero_rows() {
        let state = session().await;
        // `name` is not a declared dimension, so it cannot be pushed into the
        // rollup scan — it can only pick a pre-filtered measure.
        let reason = route_for(&state, &format!("SELECT COUNT(*) FROM {SOURCE} WHERE project_id = 'project' AND name = 'monoscope.http' AND {WINDOW}"))
            .await
            .expect_err("a residual filter must not route");
        assert_eq!(reason, MissReason::UnknownFilter);
    }

    /// `project_id = <column>` used to be consumed and dropped, so the rollup
    /// answered without a predicate the raw query enforces.
    #[tokio::test]
    async fn a_non_literal_project_id_predicate_is_not_silently_dropped() {
        let state = session().await;
        let route = route_for(&state, &format!("SELECT COUNT(*) FROM {SOURCE} WHERE project_id = name AND project_id = 'project' AND {WINDOW}")).await;
        assert!(matches!(route, Err(MissReason::UnknownFilter) | Ok(None)), "must not route while ignoring `project_id = name`: {route:?}");
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
        let output = to_rollup_batches(&spec, SOURCE, "project", "1970-01-01", "generation-a", std::slice::from_ref(&input)).expect("shape rollup");
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
