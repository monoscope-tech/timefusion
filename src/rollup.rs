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
/// One bit per hour of a UTC day. `ALL_HOURS` is the conservative value: every
/// invalidation means it unless the caller can prove a narrower set.
pub(crate) const ALL_HOURS: u32 = (1 << 24) - 1;
const HOUR_MICROS: i64 = 3_600_000_000;

/// The `[start, end)` ranges `hours` marks on the day beginning at `day_start`,
/// with adjacent hours merged so a contiguous span costs one predicate.
pub(crate) fn dirty_ranges(day_start: i64, hours: u32) -> Vec<(i64, i64)> {
    let mut ranges: Vec<(i64, i64)> = Vec::new();
    for hour in 0..24 {
        if hours & (1 << hour) == 0 {
            continue;
        }
        let (start, end) = (day_start + hour * HOUR_MICROS, day_start + (hour + 1) * HOUR_MICROS);
        match ranges.last_mut() {
            Some(last) if last.1 == start => last.1 = end,
            _ => ranges.push((start, end)),
        }
    }
    ranges
}

/// True when whole rollup buckets nest inside an hour, which is what lets an
/// hour be rebuilt without splitting one. A grain that does not divide an hour
/// would leave a bucket half-rebuilt and half-carried-forward.
pub(crate) fn grain_fits_hours(spec: &RollupSpec) -> bool {
    spec.grain_micros().is_some_and(|grain| grain > 0 && HOUR_MICROS % grain == 0)
}

pub fn build_partition_sql_from(spec: &RollupSpec, source: &str, from: &str, project_id: &str, date: &str) -> anyhow::Result<String> {
    build_partition_sql_ranges(spec, source, from, "", project_id, date, &[])
}

/// The partition's rows, rebuilt over `ranges` only and carried forward from
/// `target` everywhere else. Empty `ranges` means the whole day, from scratch.
///
/// The carried-forward rows are re-emitted verbatim: they were aggregated from
/// source rows that have not changed since, so re-aggregating them would produce
/// the same numbers at the cost of scanning the raw partition again — which is
/// the entire expense this exists to avoid.
pub(crate) fn build_partition_sql_ranges(
    spec: &RollupSpec, source: &str, from: &str, target: &str, project_id: &str, date: &str, ranges: &[(i64, i64)],
) -> anyhow::Result<String> {
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

    let partition = format!("project_id = {} AND date = {}", sql_literal(project_id), sql_literal(date));
    let rebuilt = format!(
        "SELECT to_timestamp_micros(CAST(FLOOR(EXTRACT(EPOCH FROM timestamp) * 1000000 / {grain}) AS BIGINT) * {grain}) AS timestamp{select_dimensions}, {measures} \
         FROM {source} WHERE {partition}"
    );
    if ranges.is_empty() {
        return Ok(format!("{rebuilt} GROUP BY {group_by}"));
    }
    // Only `>=`/`<`, and the SAME range list drives both legs — the rebuilt
    // hours and the carried-forward ones must partition the day exactly, or a
    // bucket is either counted twice or silently dropped from the rollup.
    let dirty = ranges
        .iter()
        .map(|(start, end)| format!("(timestamp >= to_timestamp_micros({start}) AND timestamp < to_timestamp_micros({end}))"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let carried = spec.measures.iter().map(|measure| measure.name.clone()).collect::<Vec<_>>().join(", ");
    Ok(format!(
        "{rebuilt} AND ({dirty}) GROUP BY {group_by} \
         UNION ALL SELECT timestamp{select_dimensions}, {carried} FROM {target} WHERE {partition} AND NOT ({dirty})"
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
/// Test-only: the single-interval shape, kept because the cost-floor and
/// alignment cases read far more clearly against one range than a set.
#[cfg(test)]
pub(crate) fn interior(lo: i64, hi: i64, grain: i64, horizon: i64) -> Option<(i64, i64)> {
    interiors(lo, hi, grain, horizon, &[(lo, hi)]).into_iter().next()
}

/// Every grain-aligned range the rollup may own inside `[lo, hi)`.
///
/// `covered` is the set of ranges whose coverage was proved current, in
/// ascending order; `horizon` caps them all, because a row still in the
/// MemBuffer is missing from EVERY rollup partition regardless of which dates
/// are certified.
///
/// Taking a set rather than a prefix is what stops one stale day in the middle
/// of a window discarding the rollup for every day after it — measured in
/// production, a single uncovered date made a 7-day query scan 8.4M raw rows
/// while the other six days sat fully built.
pub(crate) fn interiors(lo: i64, hi: i64, grain: i64, horizon: i64, covered: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let capped = hi.min(horizon);
    let ranges: Vec<(i64, i64)> = covered
        .iter()
        .filter_map(|(start, end)| {
            let (start, end) = (ceil_grain((*start).max(lo), grain), floor_grain((*end).min(capped), grain));
            debug_assert!(start.rem_euclid(grain) == 0 && end.rem_euclid(grain) == 0, "interior endpoints must be grain-aligned");
            // A run too short to hold whole buckets is handed back to the raw
            // leg: it would cost a second scan to save less than one bucket.
            (end.saturating_sub(start) >= MIN_INTERIOR_BUCKETS.saturating_mul(grain)).then_some((start, end))
        })
        .collect();
    // The floor applies to the TOTAL, not to each run: what makes the union
    // worth its extra scan and barrier is how much of the window it removes from
    // the raw leg, and that is the sum however it is distributed. It is measured
    // against the window the QUERY asked for, never the horizon-capped one — a
    // sliver of certified data is no more worth a second scan just because the
    // buffer bound happens to sit just past it.
    let total: i64 = ranges.iter().map(|(start, end)| end - start).sum();
    if total < hi.saturating_sub(lo) / MIN_INTERIOR_FRACTION { Vec::new() } else { ranges }
}

/// `[lo, hi)` minus `ranges` — the raw leg's share.
///
/// `ranges` must be ascending and disjoint, which `interiors` guarantees. Every
/// bound is half-open and comes from the SAME list that drives the rollup leg,
/// so the two together partition `[lo, hi)`: no row is read twice, none is
/// missed, and no aggregate downstream could detect it if one were.
pub(crate) fn complement(lo: i64, hi: i64, ranges: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut gaps = Vec::new();
    let mut cursor = lo;
    for (start, end) in ranges {
        if cursor < *start {
            gaps.push((cursor, *start));
        }
        cursor = cursor.max(*end);
    }
    if cursor < hi {
        gaps.push((cursor, hi));
    }
    gaps
}

#[derive(Debug)]
pub(crate) struct RoutedRollup {
    pub source: String,
    pub project_id: String,
    pub lo: i64,
    pub hi: i64,
    pub grain: i64,
    pub target: String,
    /// The `Aggregate` node this route replaces, verbatim. The caller substitutes
    /// the rewrite for exactly this node and leaves every node above it alone, so
    /// this is the whole interface between the matcher and the plan.
    pub matched: datafusion::logical_expr::LogicalPlan,
    /// A `COUNT` over the promoted row filter, carried through the legs but NOT
    /// selected: it exists only to power the `HAVING` that reproduces group
    /// elimination. See `promote` in `route_with_spec`.
    guard: Option<RoutedMeasure>,
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
            .chain(self.measures.iter().chain(self.guard.iter()).enumerate().flat_map(|(index, measure)| {
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
    pub fn sql(&self, generations: &[(String, String)], interiors: &[(i64, i64)]) -> String {
        let generations = format!(
            " AND ({})",
            generations
                .iter()
                .map(|(date, generation)| format!("(date = {} AND rollup_generation = {})", sql_literal(date), sql_literal(generation)))
                .collect::<Vec<_>>()
                .join(" OR ")
        );
        let fringes = complement(self.lo, self.hi, interiors);
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
            let having = self.guard.as_ref().map_or_else(String::new, |guard| format!(" HAVING {} > 0", guard.merge.sql(&guard.measures)));
            return format!(
                "SELECT {select} FROM {} WHERE project_id = {} AND ({}){generations}{row_filters}{group_by}{having}",
                self.target,
                sql_literal(&self.project_id),
                interiors.iter().map(Self::range_sql).collect::<Vec<_>>().join(" OR "),
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
        let having =
            self.guard.as_ref().map_or_else(String::new, |guard| format!(" HAVING {} > 0", guard.merge.sql(&[format!("__s{}_0", self.measures.len())])));
        format!(
            "SELECT {outer} FROM ({} UNION ALL {}) AS rollup_union{group_by}{having}",
            self.leg(&self.target, interiors, &generations),
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
        // The three string scalars collapse to ONE spelling. `{:?}` names the
        // Rust type, so `Utf8("server")` and `Utf8View("server")` canonicalized
        // differently and a query's filter could not match the identical
        // declared one. Prod hit this and no local test could: the plan cache
        // lifts a literal to `$N` and casts it to the inferred param type, while
        // the measure probe parses the same literal inline and coerces it its
        // own way — two spellings of one predicate. A MemTable session and the
        // integration harness both lack a plan cache, so both agreed.
        Expr::Literal(value, _) => match value {
            datafusion::scalar::ScalarValue::Utf8(value)
            | datafusion::scalar::ScalarValue::Utf8View(value)
            | datafusion::scalar::ScalarValue::LargeUtf8(value) => format!("Str({value:?})"),
            value => format!("{value:?}"),
        },
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
            let mut operands: Vec<&Expr> = Vec::new();
            fn collect<'a>(expr: &'a Expr, operator: Operator, operands: &mut Vec<&'a Expr>) {
                match unaliased(expr) {
                    Expr::BinaryExpr(binary) if binary.op == operator => {
                        collect(&binary.left, operator, operands);
                        collect(&binary.right, operator, operands);
                    }
                    expr => operands.push(expr),
                }
            }
            collect(expr, binary.op, &mut operands);
            if binary.op == Operator::And {
                strip_index_hints(&mut operands);
            }
            let mut terms = operands.into_iter().map(canonical).collect::<Vec<_>>();
            terms.sort();
            // Idempotence, at EVERY level rather than only the outermost one:
            // the duplicated conjuncts observed in prod were nested inside an OR
            // branch, where a top-level dedupe cannot reach them.
            terms.dedup();
            // Stripping or deduping can leave one operand, and a one-element
            // conjunction IS that operand — `((X))` must not differ from `(X)`.
            if terms.len() == 1 {
                return terms.remove(0);
            }
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

/// Drop tantivy `text_match` accelerators from one AND level.
///
/// `optimizers::tantivy_rewriter` ADDITIVELY ANDs `text_match(col, q)` next to a
/// predicate it can accelerate and, by its own stated invariant, never removes
/// the original comparison — so the semantics live entirely in the other terms
/// and the hint is noise for an equality comparison.
///
/// It has to go, because the two sides do not receive the same hints. Measured
/// in prod 2026-08-12, the query side of the Golden Signals filter carried
/// THREE hints in two different arities — `text_match(kind,"server")` and
/// `text_match(kind,"server","eq")` — where the declared measure filter carried
/// one. That is also a rewriter bug (its invariant 3 claims idempotence under
/// repeated passes), but the matcher must not depend on the rewriter being
/// idempotent to compare two spellings of the same predicate.
///
/// Only a hint on a column this AND level already compares is dropped, so a
/// `text_match` the USER wrote against some other column is preserved and the
/// filter correctly fails to match any declared measure.
fn strip_index_hints(operands: &mut Vec<&datafusion::logical_expr::Expr>) {
    use datafusion::logical_expr::Expr;
    fn hint_column(expr: &Expr) -> Option<String> {
        match unaliased(expr) {
            Expr::ScalarFunction(function) if function.name() == "text_match" => match unaliased(function.args.first()?) {
                Expr::Column(column) => Some(column.name.clone()),
                _ => None,
            },
            _ => None,
        }
    }
    let compared: std::collections::HashSet<String> = operands
        .iter()
        .filter(|operand| hint_column(operand).is_none())
        .flat_map(|operand| operand.column_refs().into_iter().map(|column| column.name.clone()))
        .collect();
    operands.retain(|operand| hint_column(operand).is_none_or(|column| !compared.contains(&column)));
}

fn canonical_and<'a>(expressions: impl IntoIterator<Item = &'a datafusion::logical_expr::Expr>) -> String {
    let mut expressions = expressions.into_iter().map(canonical).collect::<Vec<_>>();
    expressions.sort();
    // AND is idempotent, so `X AND X` must canonicalize to `X`. Both sides
    // genuinely do repeat conjuncts: the optimizer leaves a predicate on the
    // Filter node AND re-pushes it into the TableScan's `partial_filters`, so
    // `source_and_filters` collects it twice (observed in prod 2026-08-12 —
    // every declared filter printed as `(X) AND (X)`). It happened to cancel
    // out while both sides duplicated equally, which is precisely the kind of
    // accident that stops holding the moment one side is shaped differently.
    expressions.dedup();
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

/// Does this predicate constrain ONLY `project_id`/`timestamp`?
///
/// Those are the two the probe injects to satisfy the scan admission guard, and
/// they are also the two a measure filter can never usefully be about — the
/// rollup is already partitioned by project and bucketed by time. So dropping
/// them recovers exactly the declared predicate.
fn is_probe_scaffolding(expr: &datafusion::logical_expr::Expr) -> bool {
    let mut columns = std::collections::HashSet::new();
    if datafusion::logical_expr::utils::expr_to_columns(expr, &mut columns).is_err() {
        return false;
    }
    !columns.is_empty() && columns.iter().all(|column| matches!(column.name.as_str(), "project_id" | "timestamp"))
}

/// Canonicalized declared filters, keyed by (source, spec, measure).
///
/// The probe below PLANS a statement per filtered measure, and the matcher runs
/// on every aggregate query — 9 filtered measures across 2 tiers is 18 logical
/// plans per query, paid even by queries that then decline. The canonical form
/// depends only on the declared filter text and the schema's coercion rules;
/// the project and time bounds are injected purely to satisfy the scan
/// admission guard and stripped straight back out, so the result is the same
/// for every caller and safe to memoize for the life of the process.
static MEASURE_FILTERS: std::sync::OnceLock<dashmap::DashMap<(String, String, String), String>> = std::sync::OnceLock::new();

async fn measure_filters<'a>(
    session: &datafusion::execution::context::SessionState, source: &str, spec: &'a RollupSpec, project_id: &str, lo: i64, hi: i64,
) -> Result<Vec<(&'a crate::schema_loader::RollupMeasure, String)>, MissReason> {
    let cache = MEASURE_FILTERS.get_or_init(dashmap::DashMap::new);
    let mut filters = Vec::with_capacity(spec.measures.len());
    for measure in &spec.measures {
        let key = (source.to_string(), spec.name.clone().unwrap_or_default(), measure.name.clone());
        if let Some(cached) = cache.get(&key) {
            filters.push((measure, cached.clone()));
            continue;
        }
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
                //
                // The probe also carries the QUERY's project and time bounds.
                // Without them the scan admission guard rejects it outright —
                // an unbounded scan of the whole table is exactly what that
                // guard exists to stop — so on a real session every filtered
                // measure failed and `otel_logs_and_spans`, whose nine
                // `server_*` measures all declare filters, could never route at
                // all. `otel_metrics` routed only because its measures declare
                // none, so it skipped this probe entirely. The injected
                // predicates are stripped back out below.
                let probe = format!(
                    "SELECT timestamp FROM {source} WHERE project_id = {} AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi}) AND ({filter})",
                    sql_literal(project_id)
                );
                // A failure here disqualifies EVERY filtered measure, so the spec
                // stops routing at all — and from outside the process it looks
                // identical to a query whose own filter simply did not match.
                // Discarding the error is what made the two indistinguishable.
                let planned = async {
                    let plan = session.create_logical_plan(&probe).await?;
                    session.optimize(&plan)
                }
                .await;
                let plan = planned.map_err(|error| {
                    tracing::warn!(event = "rollup_measure_probe_failed", source, measure = %measure.name, probe, %error, "a declared measure filter could not be planned");
                    MissReason::UnknownFilter
                })?;
                let mut filters = Vec::new();
                source_and_filters(&plan, &mut filters).ok_or_else(|| {
                    tracing::warn!(event = "rollup_measure_probe_unwalkable", source, measure = %measure.name, plan = %plan.display_indent(), "a declared measure filter planned to a shape the matcher cannot read");
                    MissReason::UnknownFilter
                })?;
                canonical_and(filters.iter().flat_map(datafusion::logical_expr::utils::split_conjunction).filter(|term| !is_probe_scaffolding(term)))
            }
        };
        cache.insert(key, filter.clone());
        filters.push((measure, filter));
    }
    Ok(filters)
}

/// Every rollup that could serve this aggregate, best first.
///
/// The caller picks: only it knows which tiers are actually BUILT for the dates
/// in the window, and the best tier on paper is often the one with a hole in it.
pub(crate) async fn match_aggregates(
    plan: &datafusion::logical_expr::LogicalPlan, session: &datafusion::execution::context::SessionState,
) -> Result<Vec<RoutedRollup>, MissReason> {
    use datafusion::logical_expr::LogicalPlan;

    /// The outermost `Aggregate`, wherever the optimizer put it.
    ///
    /// Nothing above it is inspected, peeled or rebuilt. The rewrite is
    /// substituted for this node IN PLACE, and a node carrying the aggregate's
    /// output names and types is interchangeable with it by construction —
    /// which is a property of the aggregate alone. Earlier versions matched a
    /// fixed grammar of parents (`Sort`, then `Limit`, then a second
    /// `Projection`…) and production kept producing one layer more than the
    /// grammar knew, because the shape depends on the session's analyzer rules.
    fn outermost_aggregate(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Aggregate(_) => Some(plan),
            plan => plan.inputs().into_iter().find_map(outermost_aggregate),
        }
    }

    // Read paths only. Searching the whole tree — rather than a peeled root —
    // means an aggregate nested inside a statement is now reachable, and neither
    // kind is ours: rewriting inside an UPDATE would return a plan the DML
    // interception below never sees, and rewriting inside an EXPLAIN would
    // record a hit for a query that never runs.
    if matches!(
        plan,
        LogicalPlan::Dml(_) | LogicalPlan::Ddl(_) | LogicalPlan::Copy(_) | LogicalPlan::Explain(_) | LogicalPlan::Analyze(_) | LogicalPlan::Statement(_)
    ) {
        return Ok(Vec::new());
    }
    let Some(matched) = outermost_aggregate(plan) else { return Ok(Vec::new()) };
    let LogicalPlan::Aggregate(aggregate) = matched else { unreachable!("outermost_aggregate returns an Aggregate") };
    let mut predicates = Vec::new();
    let Some(source) = source_and_filters(&aggregate.input, &mut predicates) else { return Ok(Vec::new()) };
    let Some(schema) = crate::schema_loader::get_schema(&source).filter(|schema| !schema.rollups.is_empty()) else { return Ok(Vec::new()) };

    // Try every declared rollup, coarsest grain first: a coarser grain reads
    // strictly fewer rows for the same answer. Ties break toward the narrower
    // dimension set, which is the smaller table. A spec that cannot serve this
    // query — wrong grain, missing dimension, missing measure — declines and the
    // next one is tried, so adding a spec can only ever widen what routes.
    let mut candidates: Vec<&RollupSpec> = schema.rollups.iter().collect();
    candidates.sort_by_key(|spec| (std::cmp::Reverse(spec.grain_micros().unwrap_or(0)), spec.dimensions.len()));
    let mut first_miss = None;
    let mut routes = Vec::new();
    for spec in candidates {
        match route_with_spec(spec, &source, &schema.table_name, &predicates, aggregate, session).await {
            Ok(route) => routes.push(route),
            // Report the FIRST spec's reason: it is the one the operator most
            // likely intended to serve this query, so it is the actionable gap.
            Err(reason) => first_miss = first_miss.or(Some(reason)),
        }
    }
    // EVERY viable spec, not just the best one on paper. Coverage is not known
    // here — it is per (spec, date) and lives in the database — so returning
    // only the coarsest match would hand back a tier that happens to be
    // unbuilt for the oldest date in the window and miss, while a finer tier
    // that IS built sits unused. Measured in production: the 1h tier covered
    // 08-05.. and the 1m tier 08-04.., and a window starting 08-04 missed.
    if !routes.is_empty() {
        return Ok(routes);
    }
    // The per-reason miss counters say WHICH gap, but a shape gap is the one
    // with nothing to look at — and the one no bare test session can reproduce,
    // since the nodes above an aggregate depend on the session's analyzer rules.
    // So it alone carries the plan text at warn; every other reason is a
    // declared-schema question the counters already answer, and keeping those at
    // debug stops an ordinary unroutable panel from spamming prod's warn stream.
    let reason = first_miss.unwrap_or(MissReason::UnsupportedShape);
    let shape = matched.display_indent_schema().to_string().lines().take(6).collect::<Vec<_>>().join(" | ");
    match reason {
        MissReason::UnsupportedShape => {
            tracing::warn!(event = "rollup_declined_shape", source, reason = reason.label(), plan = %shape, "no declared rollup can serve this aggregate")
        }
        _ => tracing::debug!(event = "rollup_declined_shape", source, reason = reason.label(), plan = %shape, "no declared rollup can serve this aggregate"),
    }
    Err(reason)
}

/// Narrow `[lo, hi)` by one conjunct that bounds `timestamp`.
///
/// `Ok(false)` means the term says nothing about `timestamp` and is the
/// caller's to interpret; `Err` means it bounds `timestamp` in a way we cannot
/// read, which must never be silently ignored — a dropped bound widens the
/// window and would serve rows the query excluded.
fn narrow_timestamp(term: &datafusion::logical_expr::Expr, lo: &mut Option<i64>, hi: &mut Option<i64>) -> Result<bool, MissReason> {
    use datafusion::logical_expr::{Expr, Operator};
    match term {
        Expr::Between(between) if !between.negated && column_name(&between.expr) == Some("timestamp") => {
            let (Some(lower), Some(upper)) = (timestamp_literal(&between.low), timestamp_literal(&between.high)) else {
                return Err(MissReason::UnboundedTime);
            };
            *lo = Some(lo.map_or(lower, |current: i64| current.max(lower)));
            *hi = Some(
                hi.map_or_else(|| upper.checked_add(1), |current: i64| upper.checked_add(1).map(|upper| current.min(upper)))
                    .ok_or(MissReason::UnboundedTime)?,
            );
        }
        Expr::BinaryExpr(binary) if column_name(&binary.left) == Some("timestamp") => {
            let Some(value) = timestamp_literal(&binary.right) else { return Err(MissReason::UnboundedTime) };
            match binary.op {
                Operator::GtEq => *lo = Some(lo.map_or(value, |current: i64| current.max(value))),
                Operator::Gt => {
                    *lo = Some(lo.map_or(value.checked_add(1).ok_or(MissReason::UnboundedTime)?, |current: i64| current.max(value.saturating_add(1))))
                }
                Operator::Lt => *hi = Some(hi.map_or(value, |current: i64| current.min(value))),
                Operator::LtEq => {
                    *hi = Some(hi.map_or(value.checked_add(1).ok_or(MissReason::UnboundedTime)?, |current: i64| current.min(value.saturating_add(1))))
                }
                _ => return Err(MissReason::UnknownFilter),
            }
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// The half-open `[lo, hi)` microsecond window `predicate` confines `timestamp`
/// to, or `None` when it does not bound it on both sides.
///
/// Only conjuncts narrow: `split_conjunction` hands an `OR` back whole, so a
/// disjunction contributes no bound and the window stays open — which is the
/// safe direction for every caller.
pub(crate) fn timestamp_window(predicate: &datafusion::logical_expr::Expr) -> Option<(i64, i64)> {
    let (mut lo, mut hi) = (None, None);
    for term in datafusion::logical_expr::utils::split_conjunction(predicate) {
        // An unreadable bound is not "no bound": treat it as unbounded.
        if narrow_timestamp(term, &mut lo, &mut hi).is_err() {
            return None;
        }
    }
    lo.zip(hi).filter(|(lo, hi)| lo < hi)
}

/// Resolve one query against one declared rollup spec.
///
/// Every output is aliased with the aggregate's OWN field name, never a name
/// borrowed from a projection above it: the rewrite is substituted for the
/// aggregate itself, and the untouched nodes above reference those names.
async fn route_with_spec(
    spec: &RollupSpec, source: &str, table_name: &str, predicates: &[datafusion::logical_expr::Expr], aggregate: &datafusion::logical_expr::Aggregate,
    session: &datafusion::execution::context::SessionState,
) -> Result<RoutedRollup, MissReason> {
    use datafusion::logical_expr::{Expr, utils::split_conjunction};

    let mut project_id = None;
    let (mut lo, mut hi) = (None, None);
    let mut row_filters = Vec::new();
    let mut promotable: Vec<&Expr> = Vec::new();
    for term in predicates.iter().flat_map(split_conjunction) {
        if narrow_timestamp(term, &mut lo, &mut hi)? {
            continue;
        }
        // A predicate we can push into the rollup scan is a dimension filter;
        // one we cannot may NOT be answered by picking a measure that was
        // pre-filtered the same way. The measure carries the right value, but
        // the raw query also *eliminates* the groups and buckets where nothing
        // matched, and re-aggregating the rollup resurrects them as 0/NULL rows.
        // (`count(*) FILTER (WHERE …)` is unaffected — an aggregate filter
        // changes values, never which groups exist.)
        match (eq_literal(term, "project_id"), dimension_filter_sql(term, &spec.dimensions)) {
            // Two different literals cannot both hold; never keep the last.
            (Some(value), _) if project_id.as_deref().is_some_and(|current| current != value) => return Err(MissReason::MissingProject),
            (Some(value), _) => project_id = Some(value.to_string()),
            (None, Some(filter)) => row_filters.push(filter),
            (None, None) => promotable.push(term),
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
    // With nothing to promote, a window too narrow for this grain is the whole
    // answer and the measure probe below — which plans a statement per filtered
    // measure — is pure waste. With a residual present, the filter is the
    // actionable diagnosis and must be reported in preference to the grain, so
    // the same check runs again once the promotion has been resolved.
    let too_narrow = hi.saturating_sub(lo) < grain.saturating_mul(MIN_INTERIOR_BUCKETS);
    if too_narrow && promotable.is_empty() {
        return Err(MissReason::TinyInterior);
    }
    let configured_filters = measure_filters(session, source, spec, &project_id, lo, hi).await?;

    // ROW-FILTER PROMOTION. A residual predicate is normally fatal: the rollup
    // aggregated over every row, so re-aggregating it resurrects the groups the
    // raw query eliminated, as 0/NULL rows rather than absent ones.
    //
    // But when the residual canonicalizes to exactly a DECLARED measure filter,
    // the pre-filtered measures already carry the right values, and a `HAVING`
    // over a count sharing that filter reproduces the elimination exactly — a
    // bucket where nothing matched has a zero count and is dropped. That makes
    // every panel using the standard `server` filter routable without changing
    // what any of them display, which the monoscope-side alternatives could not:
    // dropping the filter counts every span, and moving it into per-aggregate
    // FILTER clauses turns absent buckets into zeros.
    //
    // The guard must be a `count` with NO column: `count(col)` skips nulls, so a
    // bucket whose only matching rows had a null column would be dropped where
    // the raw query kept it.
    let promoted = (!promotable.is_empty()).then(|| canonical_and(promotable.iter().copied()));
    let guard = match &promoted {
        None => None,
        Some(promoted) => Some(
            configured_filters
                .iter()
                .find(|(measure, filter)| measure.agg == "count" && measure.column.is_none() && filter == promoted)
                .map(|(measure, _)| *measure)
                .ok_or_else(|| {
                    // The two canonical strings are the whole content of this
                    // decline and there is no way to see them from outside. A
                    // near-miss between textually identical predicates has now
                    // cost two deploys to diagnose; print both.
                    tracing::warn!(
                        event = "rollup_promotion_unmatched",
                        source,
                        spec = spec.name.as_deref().unwrap_or_default(),
                        promoted = %promoted,
                        declared = %configured_filters
                            .iter()
                            .filter(|(measure, _)| measure.agg == "count" && measure.column.is_none())
                            .map(|(measure, filter)| format!("{}={filter}", measure.name))
                            .collect::<Vec<_>>()
                            .join(" | "),
                        "a residual row filter matched no declared count measure"
                    );
                    MissReason::UnknownFilter
                })?,
        ),
    };

    if too_narrow {
        return Err(MissReason::TinyInterior);
    }

    let mut groups = Vec::new();
    for (index, expression) in aggregate.group_expr.iter().enumerate() {
        let alias = aggregate.schema.field(index).name();
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
        let alias = aggregate.schema.field(aggregate.group_expr.len() + index).name().to_string();
        let Expr::AggregateFunction(function) = unaliased(expression) else { return Err(MissReason::NonDecomposableAggregate) };
        if function.params.distinct || !function.params.order_by.is_empty() {
            return Err(MissReason::NonDecomposableAggregate);
        }
        // The promoted conjuncts join the aggregate's own, so a `count(*) FILTER
        // (WHERE status_code = 'ERROR' …)` under the `server` row filter resolves
        // to `server_error_count`, which is declared as exactly that conjunction.
        let filter = canonical_and(function.params.filter.iter().flat_map(|filter| split_conjunction(filter.as_ref())).chain(promotable.iter().copied()));
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

    let guard = guard.map(|measure| RoutedMeasure {
        alias: "__guard".to_string(),
        merge: Merge::Count,
        measures: vec![measure.name.clone()],
        raw: vec![match measure.filter.as_ref() {
            Some(filter) => format!("COUNT(*) FILTER (WHERE {filter})"),
            None => "COUNT(*)".to_string(),
        }],
    });

    Ok(RoutedRollup {
        source: source.to_string(),
        project_id,
        lo,
        hi,
        grain,
        target: spec.table_name(table_name),
        matched: datafusion::logical_expr::LogicalPlan::Aggregate(aggregate.clone()),
        guard,
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
    use datafusion::common::tree_node::TreeNode as _;
    use std::sync::Arc;

    const SOURCE: &str = "otel_logs_and_spans";

    fn spec() -> RollupSpec {
        crate::schema_loader::get_schema(SOURCE).expect("source schema").rollups.first().expect("declared rollup").clone()
    }

    /// The Golden Signals miss, reproduced exactly as prod produced it.
    ///
    /// `tantivy_rewriter` ADDITIVELY ANDs `text_match` hints beside a predicate
    /// it can accelerate. It does not add the same ones to both sides: measured
    /// 2026-08-12, the query carried `text_match(kind,"server")` AND
    /// `text_match(kind,"server","eq")` where the declared measure filter
    /// carried only the two-arg form. Every earlier test agreed with prod
    /// because no bare test session registers that rewriter at all.
    #[test]
    fn a_filter_carrying_tantivy_hints_matches_the_same_filter_without_them() {
        use datafusion::logical_expr::{col, lit};
        let text_match = |args: Vec<datafusion::logical_expr::Expr>| {
            datafusion::logical_expr::Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                crate::tantivy_index::udf::text_match_udf().into(),
                args,
            ))
        };
        let base = col("kind").eq(lit("server")).or(col("name").eq(lit("monoscope.http")));
        // What the declared measure filter canonicalizes to (one hint).
        let declared = col("kind").eq(lit("server")).and(text_match(vec![col("kind"), lit("server")])).or(col("name").eq(lit("monoscope.http")));
        // What the query carries: a second hint, in the three-arg arity.
        let query = col("kind")
            .eq(lit("server"))
            .and(text_match(vec![col("kind"), lit("server")]))
            .and(text_match(vec![col("kind"), lit("server"), lit("eq")]))
            .or(col("name").eq(lit("monoscope.http")));
        assert_eq!(canonical_and([&query]), canonical_and([&declared]), "hint arity must not decide whether a panel routes");
        assert_eq!(canonical_and([&query]), canonical_and([&base]), "a hint is an accelerator, not a predicate");
    }

    /// `AND` is idempotent, so a conjunct repeated by the planner must not change
    /// the canonical string. Prod 2026-08-12 printed every declared measure
    /// filter as `(X) AND (X)`: the optimizer leaves a predicate on the Filter
    /// node and re-pushes it into the TableScan's `partial_filters`, so both
    /// copies were collected. The two sides duplicated equally and so still
    /// compared equal — by luck, not construction.
    #[test]
    fn a_conjunct_repeated_by_the_planner_canonicalizes_once() {
        use datafusion::logical_expr::{col, lit};
        let (left, right) = (col("kind").eq(lit("server")), col("name").eq(lit("monoscope.http")));
        let once = canonical_and([&left, &right]);
        assert_eq!(canonical_and([&left, &right, &left]), once, "a repeated conjunct must not change the canonical form");
        assert_eq!(canonical_and([&right, &left, &right, &left]), once, "order and multiplicity must both be canonical");
        assert!(!once.contains(" AND ") || once.matches(" AND ").count() == 1, "two distinct conjuncts join exactly once: {once}");
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
        match_aggregates(&optimized(state, sql).await, state).await.map(|routes| routes.into_iter().next())
    }

    /// Route `sql`, then reassemble exactly as `dml.rs` does — the same two
    /// functions, not a test-only copy — and assert the result still describes
    /// the query's own schema. That is the whole contract of the in-place
    /// substitution: whatever the optimizer put above the aggregate survives
    /// untouched, so the matcher never has to recognise it.
    ///
    /// `horizon` selects the rewrite shape: `None` for the single-leg rollup,
    /// `Some(_)` for the raw-fringe union.
    async fn assert_substitutes(
        state: &datafusion::execution::context::SessionState, sql: &str, horizon: Option<i64>,
    ) -> (datafusion::logical_expr::LogicalPlan, String) {
        let original = optimized(state, sql).await;
        let route = match_aggregates(&original, state).await.expect("match").into_iter().next().expect("route");
        let generated = horizon.map_or_else(|| generated_sql(&route), |horizon| hybrid_sql(&route, horizon));
        let rewrite = state.create_logical_plan(&generated).await.expect("parse rewrite");
        let rewrite = crate::dml::requalified(rewrite, route.matched.schema()).expect("requalify the rewrite to the aggregate's fields");
        let rebuilt = crate::dml::substitute(&original, &route.matched, rewrite).expect("substitute the rewrite for the aggregate");
        rebuilt.schema().has_equivalent_names_and_types(original.schema()).expect("names and types must match");
        (rebuilt, generated)
    }

    /// The rewrite when the rollup owns the whole window — the single-leg shape.
    fn generated_sql(route: &RoutedRollup) -> String {
        route.sql(&[("1970-01-01".into(), "generation".into())], &[(route.lo, route.hi)])
    }

    /// The rewrite when only part of the window is certified, so raw fringes and
    /// a live tail are unioned in.
    fn hybrid_sql(route: &RoutedRollup, horizon: i64) -> String {
        let interior = interior(route.lo, route.hi, route.grain, horizon).expect("a routable interior");
        route.sql(&[("1970-01-01".into(), "generation".into())], &[interior])
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
    async fn a_scalar_projection_above_the_aggregate_survives_untouched() {
        let state = session().await;
        let sql = format!("SELECT COUNT(*) + 1 AS total FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}");
        assert_substitutes(&state, &sql, None).await;
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
        let (_, generated) = assert_substitutes(&state, &sql, Some(WIDE_HORIZON)).await;
        assert!(generated.contains("UNION ALL"), "precondition: this is the union path");
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
        let route = match_aggregates(&original, &state).await.expect("match").into_iter().next().expect("route");
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
        // own projection sits above it and must survive the substitution.
        assert_substitutes(&state, &sql, Some(WIDE_HORIZON)).await;
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

    /// Grouping by a bucket without SELECTing it is an ordinary dashboard shape
    /// ("count per hour, ordered by count"). The aggregate then has one more
    /// output than the projection above it, and the rewrite must be named for
    /// the AGGREGATE — an earlier version absorbed the projection's names
    /// positionally and aliased the GROUP BY column with the first measure's.
    #[tokio::test]
    async fn a_group_key_that_is_not_selected_does_not_steal_a_measure_name() {
        let state = session().await;
        let sql = format!(
            "SELECT COUNT(*) AS c, avg(duration) AS m FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} \
             GROUP BY time_bucket('1 hours', timestamp) ORDER BY 1 DESC"
        );
        let (_, generated) = assert_substitutes(&state, &sql, None).await;
        assert!(!generated.contains("timestamp) AS \"c\""), "the bucket must not be aliased with a measure's name: {generated}");
    }

    /// The exact plan prod's `rollup_declined_shape` log printed for the most
    /// common dashboard query there is: `ORDER BY <an aggregate> LIMIT n` puts
    /// the Sort BELOW the outer projection (the sort key must still exist when
    /// the sort runs, and is dropped after), giving
    /// `Projection(Sort(Projection(Aggregate)))`. Every peeling matcher chased
    /// this one layer at a time and lost, because the layers above an aggregate
    /// are whatever the session's analyzer rules produced — not a grammar.
    #[tokio::test]
    async fn the_shape_that_defeated_every_peeling_matcher_routes() {
        let state = session().await;
        let sql = format!(
            "SELECT COUNT(*) AS c, avg(duration)::BIGINT AS m FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} \
             GROUP BY time_bucket('1 hours', timestamp) ORDER BY 1 DESC LIMIT 2"
        );
        let (rebuilt, _) = assert_substitutes(&state, &sql, None).await;
        // The ORDER BY/LIMIT must still be there: a routed dashboard query that
        // lost its sort returns rows in rollup order and silently truncates the
        // wrong two.
        let sorts = rebuilt.exists(|node| Ok(matches!(node, datafusion::logical_expr::LogicalPlan::Sort(sort) if sort.fetch == Some(2)))).expect("walk");
        assert!(sorts, "the ORDER BY … LIMIT 2 must survive the substitution: {rebuilt}");
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

    /// A dimension-grouped query is the shape every dashboard panel sends, and
    /// it is the one that makes qualifiers load-bearing: a rewrite is a SELECT,
    /// so its aliases are unqualified, while the aggregate's group-by column
    /// keeps `otel_logs_and_spans.`. Every untouched node above resolves columns
    /// on `(qualifier, name)`, so the substitute must reproduce BOTH.
    #[tokio::test]
    async fn the_substituted_rewrite_carries_the_aggregates_qualifiers() {
        let state = session().await;
        let sql = format!("SELECT resource___service___name, COUNT(*) FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1");
        let original = optimized(&state, &sql).await;
        let route = match_aggregates(&original, &state).await.expect("match").into_iter().next().expect("route");
        let fields = |plan: &datafusion::logical_expr::LogicalPlan| {
            plan.schema().iter().map(|(qualifier, field)| (qualifier.cloned(), field.name().clone())).collect::<Vec<_>>()
        };
        let bare = state.create_logical_plan(&generated_sql(&route)).await.expect("parse rollup query");
        assert_ne!(fields(&bare), fields(&route.matched), "precondition: the raw rewrite really is missing the qualifiers");
        let requalified = crate::dml::requalified(bare, route.matched.schema()).expect("requalify");
        assert_eq!(fields(&requalified), fields(&route.matched), "the substitute must be field-for-field the aggregate it replaces");
        requalified.schema().has_equivalent_names_and_types(route.matched.schema()).expect("names and types must match");
        assert_substitutes(&state, &sql, None).await;
    }

    /// The rewrite must be aliased with the aggregate's OWN field names, however
    /// arbitrary they are — never a name derived from the expression.
    ///
    /// Prod makes this concrete. The plan cache builds a template with the
    /// literals lifted to `$N`, then substitutes the VALUES back; the field
    /// names stay frozen as the template's, so a real production aggregate reads
    ///
    /// ```text
    /// groupBy=[[time_bucket(Utf8View("30 seconds"), timestamp)
    ///             AS time_bucket($1,otel_logs_and_spans.timestamp)]]
    /// ```
    ///
    /// — a readable literal under a name nothing else could reconstruct. Every
    /// node above references that name, so the substitute has to reproduce it.
    #[tokio::test]
    async fn the_rewrite_is_aliased_with_the_aggregates_own_field_names() {
        let state = session().await;
        // The production mechanism exactly: plan the template, then substitute
        // the value. `replace_params_with_values` aliases the new literal back to
        // the placeholder's name to keep the schema stable, which is how a field
        // ends up named for an expression that is no longer there.
        let sql = format!(
            "SELECT time_bucket($1, timestamp) AS bucket, COUNT(*) AS c FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1 ORDER BY 1 DESC"
        );
        let template = state.create_logical_plan(&sql).await.expect("plan the template");
        let bound = template
            .replace_params_with_values(&datafusion::common::ParamValues::List(vec![datafusion::scalar::ScalarValue::Utf8(Some("1 hours".into())).into()]))
            .expect("substitute the literal");
        let original = state.optimize(&bound).expect("optimize");
        let route = match_aggregates(&original, &state).await.expect("match").into_iter().next().expect("route");

        let name = route.matched.schema().field(0).name().clone();
        assert!(name.contains("$1"), "precondition: the aggregate's field name must still carry the template's placeholder, got {name}");
        let generated = generated_sql(&route);
        assert!(
            generated.contains(&format!("AS \"{name}\"")),
            "the rollup leg must carry the aggregate's own field name, not one derived from the expression: {generated}"
        );

        let rewrite = state.create_logical_plan(&generated).await.expect("parse rewrite");
        let rewrite = crate::dml::requalified(rewrite, route.matched.schema()).expect("requalify");
        let rebuilt = crate::dml::substitute(&original, &route.matched, rewrite).expect("substitute");
        rebuilt.schema().has_equivalent_names_and_types(original.schema()).expect("names and types must match");
    }

    /// What `timestamp_window` refuses matters more than what it reads: it
    /// decides how much rollup coverage a DML statement destroys, and a window
    /// narrower than the statement's true reach would leave coverage standing
    /// for a partition that changed.
    #[tokio::test]
    async fn a_timestamp_window_is_read_only_from_conjuncts_that_bound_both_ends() {
        async fn predicate(state: &datafusion::execution::context::SessionState, sql: &str) -> Option<(i64, i64)> {
            let plan = optimized(state, &format!("SELECT id FROM {SOURCE} WHERE {sql}")).await;
            let mut filters = Vec::new();
            source_and_filters(&plan, &mut filters).expect("source");
            filters.into_iter().reduce(datafusion::logical_expr::and).and_then(|filter| timestamp_window(&filter))
        }
        let state = session().await;
        let predicate = |sql: &'static str| predicate(&state, sql);
        assert_eq!(
            predicate("project_id = 'p' AND timestamp >= to_timestamp_micros(100) AND timestamp < to_timestamp_micros(500)").await,
            Some((100, 500)),
            "a half-open conjunction is the shape enrichment sends"
        );
        // Inclusive upper: the exclusive bound is one microsecond past it, or a
        // row exactly on the boundary sits outside the invalidated dates.
        assert_eq!(predicate("project_id = 'p' AND timestamp BETWEEN to_timestamp_micros(100) AND to_timestamp_micros(499)").await, Some((100, 500)));
        // One-sided, and disjunctions, leave the window open — the caller must
        // fall back to invalidating everything.
        assert_eq!(predicate("project_id = 'p' AND timestamp >= to_timestamp_micros(100)").await, None, "an open-ended range must not narrow anything");
        assert_eq!(
            predicate("project_id = 'p' AND (timestamp < to_timestamp_micros(100) OR timestamp >= to_timestamp_micros(500))").await,
            None,
            "a disjunction reaches outside any single range"
        );
    }

    /// A hole in the middle of a window must cost only the days it covers, not
    /// every day after it. Measured in production: one uncovered date made a
    /// 7-day query scan 8.4M raw rows while six days sat fully built.
    ///
    /// The invariant is the same one the single interval had, now over a SET:
    /// the rollup intervals and the raw complement must partition `[lo, hi)`
    /// exactly — a gap drops rows, an overlap counts them twice, and nothing
    /// downstream can tell.
    #[test]
    fn a_gap_in_coverage_costs_only_the_days_it_covers() {
        const DAY: i64 = 86_400_000_000;
        let grain = 3_600_000_000;
        let (lo, hi) = (0, 7 * DAY);
        // Days 0-1 and 4-6 covered; day 2-3 is the hole.
        let covered = [(0, 2 * DAY), (4 * DAY, 7 * DAY)];
        let intervals = interiors(lo, hi, grain, hi, &covered);
        assert_eq!(intervals, vec![(0, 2 * DAY), (4 * DAY, 7 * DAY)], "both runs must be kept, not just the prefix");

        let gaps = complement(lo, hi, &intervals);
        assert_eq!(gaps, vec![(2 * DAY, 4 * DAY)], "only the hole is read raw");
        let total: i64 = intervals.iter().chain(gaps.iter()).map(|(start, end)| end - start).sum();
        assert_eq!(total, hi - lo, "the two legs must cover the window exactly once");

        // The prefix rule would have kept only days 0-1 and scanned five raw.
        let prefix_only: i64 = intervals.iter().map(|(start, end)| end - start).sum();
        assert_eq!(prefix_only, 5 * DAY, "five of seven days must still come from the rollup");
    }

    /// The complement must stay a partition for any interval set, including the
    /// degenerate ones — empty, whole-window, and touching either end.
    #[test]
    fn the_rollup_intervals_and_their_complement_always_partition_the_window() {
        let (lo, hi) = (100_i64, 1_000_i64);
        for ranges in [vec![], vec![(lo, hi)], vec![(lo, 400)], vec![(400, hi)], vec![(200, 300), (500, 600)], vec![(lo, 300), (300, 600)], vec![(900, hi)]] {
            let gaps = complement(lo, hi, &ranges);
            let total: i64 = ranges.iter().chain(gaps.iter()).map(|(start, end)| end - start).sum();
            assert_eq!(total, hi - lo, "ranges {ranges:?} + gaps {gaps:?} must cover [{lo},{hi}) exactly once");
            for gap in &gaps {
                assert!(gap.0 < gap.1, "empty gaps must not be emitted: {gaps:?}");
                assert!(ranges.iter().all(|range| gap.1 <= range.0 || gap.0 >= range.1), "gap {gap:?} overlaps {ranges:?}");
            }
        }
    }

    /// The buffer horizon caps EVERY interval, not just the last one: a row
    /// still in the MemBuffer is missing from every rollup partition, whichever
    /// dates happen to be certified.
    #[test]
    fn the_buffer_horizon_caps_every_interval() {
        const DAY: i64 = 86_400_000_000;
        let grain = 3_600_000_000;
        let covered = [(0, 2 * DAY), (2 * DAY, 4 * DAY)];
        let intervals = interiors(0, 4 * DAY, grain, 3 * DAY, &covered);
        assert!(intervals.iter().all(|(_, end)| *end <= 3 * DAY), "nothing may reach past the horizon: {intervals:?}");
    }

    /// The rebuilt hours and the carried-forward hours must PARTITION the day:
    /// a gap silently drops buckets from the rollup, an overlap double-counts
    /// them, and neither is visible from the read side — the partition still
    /// looks like a complete day. Same invariant as the read-side fringe split,
    /// and the same reason it gets a property test rather than an example.
    #[test]
    fn rebuilt_and_carried_forward_hours_partition_the_day() {
        const DAY: i64 = 86_400_000_000;
        for day_start in [0_i64, 1_754_784_000_000_000, -DAY] {
            for hours in [1u32, 0b101, 1 << 23, 0xFF_FF00, ALL_HOURS, 0b1010_1010_1010_1010_1010_1010] {
                let ranges = dirty_ranges(day_start, hours);
                let covered: i64 = ranges.iter().map(|(start, end)| end - start).sum();
                assert_eq!(covered, i64::from(hours.count_ones()) * 3_600_000_000, "ranges must cover exactly the marked hours");
                // Merged, disjoint and ascending — the SQL ORs them, so an
                // overlap would double-count a bucket.
                for pair in ranges.windows(2) {
                    assert!(pair[0].1 < pair[1].0, "ranges must be disjoint and non-adjacent after merging: {ranges:?}");
                }
                assert!(ranges.iter().all(|(start, end)| start < end && *start >= day_start && *end <= day_start + DAY), "{ranges:?}");
            }
        }
        assert!(dirty_ranges(0, 0).is_empty(), "nothing dirty must rebuild nothing");
        assert_eq!(dirty_ranges(0, ALL_HOURS), vec![(0, DAY)], "a fully dirty day must merge to one range");
    }

    /// A filter's canonical form must not depend on which Rust string scalar the
    /// planner happened to produce. Prod declined the Golden Signals filter for
    /// exactly this reason while every local test accepted it.
    #[test]
    fn a_string_literal_canonicalizes_the_same_whatever_its_arrow_type() {
        use datafusion::{logical_expr::Expr, scalar::ScalarValue};
        let literal = |value: ScalarValue| canonical(&Expr::Literal(value, None));
        let utf8 = literal(ScalarValue::Utf8(Some("server".into())));
        assert_eq!(utf8, literal(ScalarValue::Utf8View(Some("server".into()))), "Utf8 and Utf8View must agree");
        assert_eq!(utf8, literal(ScalarValue::LargeUtf8(Some("server".into()))), "LargeUtf8 must agree too");
        assert_ne!(utf8, literal(ScalarValue::Utf8(Some("client".into()))), "different values must still differ");
        // Non-string scalars keep their type, so an integer cannot collide with
        // a string that merely prints the same.
        assert_ne!(literal(ScalarValue::Int64(Some(1))), literal(ScalarValue::Utf8(Some("1".into()))));
    }

    /// monoscope's Golden Signals panels filter rows with exactly the predicate
    /// the `server_*` measures declare, which used to hit the residual-filter
    /// refusal and made the flagship dashboard unroutable.
    ///
    /// The promotion must carry a HAVING: without it a bucket where nothing
    /// matched comes back as a 0 row instead of being absent, which is a
    /// different chart.
    #[tokio::test]
    async fn a_row_filter_that_matches_a_declared_measure_filter_routes_with_a_having() {
        let state = session().await;
        const SERVER: &str = "(kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http')";
        let sql = format!(
            "SELECT time_bucket('1 hours', timestamp) AS bucket, COUNT(*) AS c \
             FROM {SOURCE} WHERE project_id = 'project' AND {SERVER} AND {WINDOW} GROUP BY 1 ORDER BY 1 DESC"
        );
        let (_, generated) = assert_substitutes(&state, &sql, None).await;
        assert!(generated.contains("server_request_count"), "the pre-filtered measure must answer it: {generated}");
        assert!(generated.contains("HAVING"), "group elimination must be reproduced, or empty buckets come back as zeros: {generated}");
    }

    /// The aggregate's own FILTER and the promoted row filter combine, so the
    /// error widget resolves to `server_error_count` — declared as exactly that
    /// conjunction.
    #[tokio::test]
    async fn an_aggregate_filter_combines_with_the_promoted_row_filter() {
        let state = session().await;
        const SERVER: &str = "(kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http')";
        let sql = format!(
            "SELECT time_bucket('1 hours', timestamp) AS bucket, \
                    COUNT(*) FILTER (WHERE status_code = 'ERROR' OR COALESCE(attributes___http___response___status_code, 0) >= 500) AS errors \
             FROM {SOURCE} WHERE project_id = 'project' AND {SERVER} AND {WINDOW} GROUP BY 1"
        );
        let (_, generated) = assert_substitutes(&state, &sql, None).await;
        assert!(generated.contains("server_error_count"), "the conjunction must resolve to the declared measure: {generated}");
    }

    /// A residual filter that matches NOTHING declared must still refuse. The
    /// promotion widens what routes; it must not widen what is answered wrongly.
    #[tokio::test]
    async fn a_residual_filter_with_no_declared_measure_still_refuses() {
        let state = session().await;
        let sql = format!(
            "SELECT COUNT(*) AS c FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} AND status_message = 'nope' \
             GROUP BY time_bucket('1 hours', timestamp)"
        );
        assert_eq!(route_for(&state, &sql).await.err(), Some(MissReason::UnknownFilter), "an unmatched residual must not be promoted");
    }

    /// An aggregate inside a CTE is reachable now, because the matcher searches
    /// the tree instead of peeling the root — monoscope's percentile panel wraps
    /// its aggregate in exactly this shape, and no amount of root-peeling could
    /// ever have seen it.
    #[tokio::test]
    async fn an_aggregate_inside_a_cte_is_reachable() {
        let state = session().await;
        let sql = format!(
            "WITH bucketed AS (SELECT time_bucket('1 hours', timestamp) AS t, avg(duration) AS mean \
                               FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1) \
             SELECT t, mean FROM bucketed ORDER BY 1 DESC"
        );
        assert_substitutes(&state, &sql, None).await;
    }

    /// `ORDER BY ... DESC` is on every real dashboard query, so a matcher that
    /// only accepts a bare aggregate root never fires in production.
    #[tokio::test]
    async fn an_order_by_above_the_aggregate_survives_the_substitution() {
        let state = session().await;
        let sql = format!(
            "SELECT resource___service___name, COUNT(*) AS c FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1 ORDER BY 1 DESC LIMIT 10"
        );
        let (rebuilt, _) = assert_substitutes(&state, &sql, None).await;
        // The optimizer folds the LIMIT into the Sort's `fetch`, so the Sort at
        // the root carries both. Losing either returns rows in rollup order.
        let datafusion::logical_expr::LogicalPlan::Sort(sort) = &rebuilt else { panic!("the ORDER BY must still be on top: {rebuilt}") };
        assert_eq!(sort.fetch, Some(10), "the LIMIT folded into the Sort must survive too");
    }

    /// `HAVING` plans to a `Filter` between the aggregate and the projection.
    /// It needs no handling at all now — it is simply one of the nodes above the
    /// aggregate — but dropping it would return the rows the query excluded, so
    /// the guard stays.
    #[tokio::test]
    async fn a_having_clause_still_filters_the_rewritten_plan() {
        let state = session().await;
        let sql = format!("SELECT kind, COUNT(*) AS c FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1 HAVING COUNT(*) > 0");
        let (rebuilt, _) = assert_substitutes(&state, &sql, None).await;
        assert!(
            rebuilt.exists(|node| Ok(matches!(node, datafusion::logical_expr::LogicalPlan::Filter(_)))).expect("walk"),
            "the HAVING filter must survive: {rebuilt}"
        );
    }

    /// An aggregate no spec can serve must reach `rollup_miss`; a plan that
    /// simply is not our business must stay uncounted, or every ordinary SELECT
    /// would pollute the counter.
    #[tokio::test]
    async fn an_unsupported_aggregate_is_counted_but_an_unrelated_query_is_not() {
        let state = session().await;
        // No pre-aggregated state can answer a standard deviation, so it must
        // decline — visibly. The window is wide enough for every tier, or the
        // reported reason would be the coarsest tier's `TinyInterior` instead.
        let stddev = format!(
            "SELECT stddev(duration) AS s FROM {SOURCE} WHERE project_id = 'project' \
               AND timestamp >= to_timestamp_micros(0) AND timestamp < to_timestamp_micros(864000000000)"
        );
        assert_eq!(
            route_for(&state, &stddev).await.err(),
            Some(MissReason::NonDecomposableAggregate),
            "an aggregate over a rollup source that no spec can serve must be visible"
        );
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
