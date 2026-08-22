//! Schema-driven dashboard rollups.
//!
//! A rollup is built only after its source partition is duplicate-free. This
//! module owns the deterministic aggregate SQL and the conversion from its
//! output to the generated target schema. Read routing lives here as well, but
//! is deliberately conservative: an unsupported query must use raw data.

use crate::schema::RollupSpec;

/// Why a query cannot use a rollup. Variant names ARE the `rollup_misses`
/// telemetry labels (snake_case); the two `serialize` overrides are historical
/// names prod dashboards already query on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumIter, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum MissReason {
    UnsupportedShape,
    MissingProject,
    UnboundedTime,
    UnknownGroupBy,
    UnknownFilter,
    /// The residual row filter constrains columns no declared measure filters
    /// on, so no rollup could ever have answered it. Separated from
    /// `UnknownFilter` so that counter means "a filter we should have matched
    /// and didn't" — see the decline site for the prod evidence.
    FilterNotEligible,
    MissingMeasure,
    #[strum(serialize = "non_decomposable")]
    NonDecomposableAggregate,
    /// Names the ONLY thing this reason still means: a `time_bucket` width that
    /// is not a multiple of the grain. The window's own alignment stopped
    /// mattering once raw fringes were added.
    #[strum(serialize = "unaligned_bucket_width")]
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
    /// Hybrid routing would create too many disjoint raw/rollup predicates.
    TooManyBranches,
    RewriteSchemaMismatch,
}

impl MissReason {
    pub fn label(self) -> &'static str {
        self.into()
    }
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Deterministic identity for one rollup generation.
///
/// It was a random UUID, which made the rollup rows on S3 unreadable after a
/// restart. It deliberately does not include the source fingerprint: independently
/// replaceable slices of one date must share a generation so a query can merge
/// them. The fingerprint remains in each Add tag and in the read ticket, where
/// it is a validity check rather than a row-selection key.
///
/// The spec participates because adding a measure without bumping the table
/// name would otherwise serve rows built under the old spec as if current.
pub fn generation_id(spec: &RollupSpec, source: &str, project_id: &str, date: &str, _source_fp: u64) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = fnv::FnvHasher::default();
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

/// The hours of a partition-day a committed file can hold rows for, from its
/// Delta stats JSON (`minValues.timestamp` / `maxValues.timestamp`). `None`
/// when stats or timestamp bounds are absent — the caller falls back to
/// `ALL_HOURS`, never to skipping work. A computed mask of zero (bounds
/// entirely outside the partition day) is likewise treated as absent.
///
/// This is what lets a boot reconcile invalidate the ONE hour a downtime
/// commit actually touched instead of all 24 (`enqueue_maintenance_hours`
/// with `ALL_HOURS` was ~312 durable tasks per active project per restart,
/// prod 2026-08-18 — the queue's dominant growth source under deploy churn).
/// The inclusive timestamp bounds a file's Delta statistics claim.
///
/// Writers spell them either as epoch micros or RFC 3339, so both are accepted.
/// `None` means the file makes no claim — never treat that as an empty range.
pub(crate) fn stats_time_range(stats: &str) -> Option<(i64, i64)> {
    let value: serde_json::Value = serde_json::from_str(stats).ok()?;
    let parse_ts = |side: &str| -> Option<i64> {
        let v = value.get(side)?.get("timestamp")?;
        if let Some(micros) = v.as_i64() {
            return Some(micros);
        }
        chrono::DateTime::parse_from_rfc3339(v.as_str()?).ok().map(|t| t.timestamp_micros())
    };
    Some((parse_ts("minValues")?, parse_ts("maxValues")?))
}

pub(crate) fn hours_from_stats_json(stats: &str, day_start_micros: i64) -> Option<u32> {
    let (lo, hi) = stats_time_range(stats)?;
    let (lo_h, hi_h) = ((lo - day_start_micros).div_euclid(HOUR_MICROS), (hi - day_start_micros).div_euclid(HOUR_MICROS));
    if hi_h < 0 || lo_h >= 24 || lo_h > hi_h {
        return None;
    }
    let mut mask = 0u32;
    for hour in lo_h.clamp(0, 23)..=hi_h.clamp(0, 23) {
        mask |= 1 << hour;
    }
    (mask != 0).then_some(mask)
}

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
                let expression = match measure.agg.as_str() {
                    "min" => format!("MIN({})", measure.name),
                    "max" => format!("MAX({})", measure.name),
                    "tdigest" => format!("tdigest_merge(CAST({} AS BYTEA))", measure.name),
                    "hll" => format!("hll_merge(CAST({} AS BYTEA))", measure.name),
                    _ => format!("SUM({})", measure.name),
                };
                return Ok(format!("{expression} AS {}", measure.name));
            }
            let expression = match (measure.agg.as_str(), measure.column.as_deref()) {
                ("count", None) => "COUNT(*)".to_string(),
                ("count", Some(column)) => format!("COUNT({column})"),
                ("tdigest", Some(column)) => format!("percentile_agg(CAST({column} AS DOUBLE))"),
                ("hll", Some(column)) => format!("hll_agg({column})"),
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

pub(crate) fn build_cohort_sql_range_mode(
    spec: &RollupSpec, _source: &str, from: &str, project_ids: &[String], date: &str, (start, end): (i64, i64), derived: bool,
) -> anyhow::Result<String> {
    if project_ids.is_empty() {
        anyhow::bail!("rollup cohort has no projects");
    }
    let grain = spec.grain_micros().ok_or_else(|| anyhow::anyhow!("invalid rollup grain `{}`", spec.grain))?;
    let dimensions = spec.dimensions.join(", ");
    let measures = spec
        .measures
        .iter()
        .map(|measure| {
            if derived {
                let expression = match measure.agg.as_str() {
                    "min" => format!("MIN({})", measure.name),
                    "max" => format!("MAX({})", measure.name),
                    "tdigest" => format!("tdigest_merge(CAST({} AS BYTEA))", measure.name),
                    "hll" => format!("hll_merge(CAST({} AS BYTEA))", measure.name),
                    _ => format!("SUM({})", measure.name),
                };
                return Ok(format!("{expression} AS {}", measure.name));
            }
            let expression = match (measure.agg.as_str(), measure.column.as_deref()) {
                ("count", None) => "COUNT(*)".to_string(),
                ("count", Some(column)) => format!("COUNT({column})"),
                ("tdigest", Some(column)) => format!("percentile_agg(CAST({column} AS DOUBLE))"),
                ("hll", Some(column)) => format!("hll_agg({column})"),
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
    let select_dimensions = if dimensions.is_empty() { String::new() } else { format!(", {dimensions}") };
    let group_by = std::iter::once("1".to_string())
        .chain(std::iter::once("2".to_string()))
        .chain((3..).take(spec.dimensions.len()).map(|index| index.to_string()))
        .collect::<Vec<_>>()
        .join(", ");
    let projects = project_ids.iter().map(|project| sql_literal(project)).collect::<Vec<_>>().join(", ");
    Ok(format!(
        "SELECT project_id, to_timestamp_micros(CAST(FLOOR(EXTRACT(EPOCH FROM timestamp) * 1000000 / {grain}) AS BIGINT) * {grain}) AS timestamp{select_dimensions}, {measures} \
         FROM {from} WHERE project_id IN ({projects}) AND date = {} AND timestamp >= to_timestamp_micros({start}) AND timestamp < to_timestamp_micros({end}) \
         GROUP BY {group_by}",
        sql_literal(date)
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
    let schema = crate::schema::get_schema(&target).ok_or_else(|| anyhow::anyhow!("{target} schema missing"))?.schema_ref();
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| anyhow::anyhow!("invalid Unix epoch date"))?;
    let date_days = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")?.signed_duration_since(epoch).num_days();
    let date_days = i32::try_from(date_days).map_err(|_| anyhow::anyhow!("rollup date `{date}` is outside Date32"))?;
    let grain = spec.grain_micros().ok_or_else(|| anyhow::anyhow!("invalid rollup grain `{}`", spec.grain))?;
    let now = crate::support::now_micros();

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

/// Split a cohort aggregate by its output `project_id` and shape each project
/// with its own generation. The aggregate must retain `project_id` as a group
/// key; synthesizing it from the cohort request would mix tenant identities.
pub(crate) fn to_rollup_batches_by_project(
    spec: &RollupSpec, source: &str, date: &str, generations: &std::collections::HashMap<String, String>, aggregated: &[arrow::record_batch::RecordBatch],
) -> anyhow::Result<std::collections::HashMap<String, Vec<arrow::record_batch::RecordBatch>>> {
    use arrow::{
        array::{Array, StringArray},
        compute::cast,
        datatypes::DataType,
    };

    let mut grouped: std::collections::HashMap<String, Vec<arrow::record_batch::RecordBatch>> = std::collections::HashMap::new();
    for batch in aggregated.iter().filter(|batch| batch.num_rows() > 0) {
        let projects = batch.column_by_name("project_id").ok_or_else(|| anyhow::anyhow!("cohort aggregate is missing project_id"))?;
        let projects = cast(projects, &DataType::Utf8)?;
        let projects = projects.as_any().downcast_ref::<StringArray>().ok_or_else(|| anyhow::anyhow!("cohort project_id cannot cast to Utf8"))?;
        let mut rows_by_project: std::collections::HashMap<&str, Vec<u32>> = std::collections::HashMap::new();
        for row in 0..batch.num_rows() {
            if projects.is_null(row) {
                anyhow::bail!("cohort aggregate project_id is null");
            }
            rows_by_project.entry(projects.value(row)).or_default().push(u32::try_from(row)?);
        }
        for (project_id, rows) in rows_by_project {
            let indices = arrow::array::UInt32Array::from(rows);
            let columns = batch.columns().iter().map(|column| arrow::compute::take(column, &indices, None)).collect::<arrow::error::Result<Vec<_>>>()?;
            let slice = arrow::record_batch::RecordBatch::try_new(batch.schema(), columns)?;
            let generation = generations.get(project_id).ok_or_else(|| anyhow::anyhow!("cohort output contains unexpected project `{project_id}`"))?;
            let shaped = to_rollup_batches(spec, source, project_id, date, generation, &[slice])?;
            grouped.entry(project_id.to_string()).or_default().extend(shaped);
        }
    }
    Ok(grouped)
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
    /// A distinct-count sketch. Like `TDigest`, the query's output IS the folded
    /// state: `distinct_count` reads the number out of it in the projection
    /// above the aggregate, which the rewrite never touches.
    Hll,
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
            Self::Hll => "hll_merge",
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
            (Self::Hll, [sketch]) => format!("hll_merge({sketch})"),
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

/// The upper bound of a window the query left open. Chosen as `i64::MAX` so it
/// compares above every real timestamp and so `complement` needs no special
/// case: the trailing gap simply runs to it, and `range_sql` renders that one
/// range without an upper bound.
pub(crate) const OPEN_END: i64 = i64::MAX;

/// May a date's slice coverage be read from the tier at all?
///
/// Slice coverage is the ONLY live coverage (the per-date map was dead code),
/// and until this guard existed it was checked for project/source/target/range
/// overlap and nothing else — no fingerprint, no epoch. A slice therefore kept
/// claiming its range after the source partition moved underneath it, and the
/// tier served an aggregate built from fewer rows than the partition now holds.
/// Prod 2026-08-22: P1's 08-20 rollup was short 21.7% and 08-21 short 96%, and
/// those two shortfalls summed to the 2.86M-row under-count a 3-day throughput
/// chart returned (`docs/plans/2026-08-22-rollup-correctness-and-routing.md`).
///
/// `witnesses` is each covering slice's record of how many rows the DATE
/// partition held when it was built, and `current` is how many it holds now.
/// Both must be the same computation — `PartitionStats::rows`, i.e. the
/// add-action `num_records` sum — or this compares unlike quantities and is
/// worthless: the build's decoded input excludes tombstones and superseded
/// merge-on-read versions, `num_records` counts them.
///
/// Witnesses are never SUMMED: slices of one day are built at different times
/// against a churning partition, so each is a snapshot of the whole date, and
/// summing snapshots would compare a total against one sample.
///
/// Callers evaluate one slice at a time. A witness is a statement about the
/// WHOLE partition ("it held N rows when I built"), so two slices that each
/// still agree with the present N are both current, independently — and a slice
/// that cannot be verified drops out alone instead of condemning its whole
/// date. Condemning the date meant a single un-rebuilt slice kept a fully
/// rebuilt day on the raw path, which with thousands of slices queued is
/// indefinite.
///
/// Equality is two-sided on purpose. "Rows only accrue" is false here — dedup
/// rewrites and vacuum shrink `num_records` too — so a disagreement in either
/// direction means the partition moved and the coverage cannot be trusted.
///
/// A slice with no witness predates the tag and CANNOT be verified, so it is
/// refused; those dates read raw until the coordinator republishes them.
pub(crate) fn slice_coverage_agrees(witnesses: &[Option<u64>], current: Option<u64>) -> bool {
    let Some(current) = current else { return false };
    !witnesses.is_empty() && witnesses.iter().all(|witness| *witness == Some(current))
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

pub(crate) fn hybrid_branch_count(lo: i64, hi: i64, ranges: &[(i64, i64)]) -> usize {
    ranges.len().saturating_add(complement(lo, hi, ranges).len())
}

/// Which projects the ROLLUP leg is allowed to answer for, and which must be
/// read raw across the whole window because their coverage was not proved.
///
/// Coverage is intersected across a cross-project query's projects, so before
/// this a single project short of coverage refused the query for every other
/// project too. Prod 2026-08-18: 9 of 10 projects had coverage for the window
/// and the tenth sent all ten to a raw scan.
#[derive(Debug, Default)]
pub(crate) struct ProjectSplit {
    /// `None` means every project the query reads — the pinned case, or a
    /// cross-project query where all of them proved coverage.
    pub covered: Option<Vec<String>>,
    /// Read raw over the WHOLE window. Disjoint from `covered` by construction.
    pub raw_only: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct RoutedRollup {
    pub source: String,
    /// The project the query pinned with `project_id = '…'`, or `None` when it
    /// GROUPS BY project_id instead. `None` means the rewrite reads every
    /// project's rollup rows, so its coverage must hold for every project with
    /// source data in the window — see `rollup_rewrite_for`.
    pub project_id: Option<String>,
    pub lo: i64,
    pub hi: i64,
    /// The query gave no upper bound and `hi` is a plan-time stand-in. The
    /// interior may use it; the trailing raw range may NOT, or the rewrite
    /// would drop every row after it — the newest rows, which is precisely what
    /// a live dashboard is showing.
    open_end: bool,
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

/// Quote a SQL identifier, escaping embedded `"` by doubling it.
pub(crate) fn quoted(alias: &str) -> String {
    format!("\"{}\"", alias.replace('"', "\"\""))
}

/// How a slice input collapses duplicate rows: `keys` identify a row, the
/// greatest `tiebreak` wins, and a true `tombstone` drops it.
pub(crate) struct SliceDedup<'a> {
    pub keys: &'a [String],
    pub tiebreak: Option<&'a str>,
    pub tombstone: Option<&'a str>,
}

/// The identity a generated rollup tier is physically written with.
///
/// A tier's own `TableSchema` deliberately declares NO `dedup_keys` — see
/// `RollupSpec::synthesize`, where doing so made every routed read plan a
/// `DedupExec` over columns the rewrite does not project ("DedupExec key `id`
/// not in input schema") and dropped every query back to a raw scan. That is a
/// statement about the QUERY path. It is not true of the tier's bytes: a
/// rebuild appends a new version of a bucket, and the replace-set that is meant
/// to retire the old file skips any file carrying no slice tags — so partitions
/// really do hold several versions of one `id` (prod 2026-08-20: 7.17 per id).
///
/// So a MAINTENANCE read of a tier must collapse versions explicitly, without
/// declaring keys the query planner would then act on.
pub(crate) fn rollup_tier_dedup(schema: &crate::schema::TableSchema) -> Option<(Vec<String>, &'static str, Option<&str>)> {
    let has = |name: &str| schema.fields.iter().any(|field| field.name == name);
    (has("timestamp") && has("id") && has("updated_at"))
        .then(|| (vec!["timestamp".to_owned(), "id".to_owned()], "updated_at", schema.tombstone_column.as_deref()))
}

/// A live tier file, as the publish path's replace-set sees it.
///
/// `slice` is `None` for a file carrying no `timefusion.slice_*` tags — either
/// written before tagging existed, or rewritten by something that dropped them
/// (a delta-rs OPTIMIZE keeps only its own `sort_by` tag).
pub(crate) struct LiveFile<'a> {
    pub slice: Option<(i64, i64)>,
    pub project: Option<&'a str>,
    pub partition: Option<(&'a str, &'a str)>,
    /// Inclusive timestamp bounds from the file's own Delta statistics, which
    /// survive tag loss — a rewrite strips tags but still writes stats.
    pub stats: Option<(i64, i64)>,
}

/// The publication a replace-set is being computed for.
pub(crate) struct SlicePublish<'a> {
    pub project_id: &'a str,
    pub date: &'a str,
    pub slice: (i64, i64),
    pub rows: u64,
    /// Tagged slice ranges that will be LIVE in this partition once this commit
    /// lands, including this slice itself. Their union is what proves an
    /// untagged file redundant when no single slice contains it.
    pub covered: &'a [(i64, i64)],
}

/// Whether this publication retires `file`.
///
/// A TAGGED file is retired when this slice CONTAINS it — see the replace-set's
/// own comment for why containment and not equality.
///
/// An UNTAGGED file was previously immortal: no tags meant not contained, meant
/// never removed, so every rebuild stacked another version of every `id` beside
/// it and nothing could ever collapse them (prod 2026-08-20: 352 such files,
/// 7.17 versions per id). It is retired only when this partition provably
/// reproduces it, by any of three proofs, cheapest first:
///
/// - the slice spans a WHOLE partition day, so it reproduces everything a file
///   in `date=D` can hold — files are partitioned by `(project_id, date)`, so
///   such a file cannot carry rows outside `D`;
/// - the file's own statistics place it inside this slice. Statistics survive
///   tag loss, so this reaches a file no tag can identify;
/// - the union of live tagged slices covers the file's statistics range. This
///   is the only proof that reaches a LARGE tenant: a day over
///   `MAX_DECODED_BYTES` is split before it ever runs, so no day-wide slice
///   exists for it, and a file spanning most of the day fits inside none of the
///   children either. 21 of the 70 files left after the 2026-08-20 repair were
///   exactly that shape — reachable by no single slice at any width.
///
/// All three require `rows > 0`: if raw has aged out the rebuild is empty and
/// the untagged file may be the only copy, so removing it would delete data
/// rather than a duplicate. And all three require the file to be in this very
/// partition — another project's file, or another day's, is not reproduced by
/// this slice however wide it is.
pub(crate) fn slice_retires(file: &LiveFile<'_>, publish: &SlicePublish<'_>) -> bool {
    let (start, end) = publish.slice;
    match file.slice {
        Some((file_start, file_end)) => file.project == Some(publish.project_id) && file_start >= start && file_end <= end,
        None => {
            if publish.rows == 0 || file.partition != Some((publish.project_id, publish.date)) {
                return false;
            }
            spans_whole_day(start, end) || file.stats.is_some_and(|(lo, hi)| (lo >= start && hi < end) || ranges_cover(publish.covered, (lo, hi)))
        }
    }
}

/// Whether the union of `ranges` covers every instant in `[lo, hi]`.
///
/// `hi` is INCLUSIVE — it is a row's timestamp, straight from file statistics —
/// while a slice's end is exclusive, so a range must reach strictly past `hi`.
pub(crate) fn ranges_cover(ranges: &[(i64, i64)], (lo, hi): (i64, i64)) -> bool {
    let mut sorted: Vec<(i64, i64)> = ranges.to_vec();
    sorted.sort_unstable();
    let mut reached = lo;
    for (start, end) in sorted {
        if start > reached {
            return false;
        }
        reached = reached.max(end);
        if reached > hi {
            return true;
        }
    }
    false
}

/// The sub-ranges of `untagged` that no live tagged slice covers — the exact
/// work that would close proof C for this partition.
///
/// Queueing the whole DAY instead is what stalled the 2026-08-22 mid-tail: a
/// day's rollup input is far over `MAX_DECODED_BYTES`, so the preflight shreds
/// it down the bisection ladder to the one-minute floor — prod held 3,455 units
/// for a single (project, tier, day), 1,423 of them pending. The actual holes
/// were 22 MINUTES wide (`18:00-18:22`, `12:00-12:22` — the leftovers of an
/// earlier shred), each one unit that fits the budget whole.
///
/// A partition with no tagged ranges at all yields the untagged spans
/// themselves, so this is a refinement of "rebuild the day", never a narrowing
/// below what was already queued.
///
/// Ends are EXCLUSIVE here, but a file's statistics `hi` is a row timestamp, so
/// callers pass `hi + 1`.
pub(crate) fn uncovered_gaps(untagged: &[(i64, i64)], tagged: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut covered = tagged.to_vec();
    covered.sort_unstable();
    let mut gaps: Vec<(i64, i64)> = Vec::new();
    for &(lo, hi) in untagged {
        let mut reached = lo;
        for &(start, end) in covered.iter().filter(|(_, end)| *end > lo) {
            if start >= hi {
                break;
            }
            if start > reached {
                gaps.push((reached, start.min(hi)));
            }
            reached = reached.max(end);
            if reached >= hi {
                break;
            }
        }
        if reached < hi {
            gaps.push((reached, hi));
        }
    }
    gaps.sort_unstable();
    gaps.dedup();
    gaps
}

/// A slice covering exactly one UTC calendar day — the partition granularity.
const fn spans_whole_day(start: i64, end: i64) -> bool {
    start.rem_euclid(crate::maintenance_coordinator::DAY_MICROS) == 0 && end.saturating_sub(start) == crate::maintenance_coordinator::DAY_MICROS
}

/// The SELECT a rollup slice reads its input through, collapsing duplicates
/// when `dedup` says how.
///
/// `schema` describes whatever is registered as `raw` — for a DERIVED tier that
/// is the BASE TIER, not the raw source. With `dedup` as `None` this emits a
/// bare `SELECT *`, which is what the derived path used to do: because a tier
/// holds superseded versions, the aggregate above then SUMs all of them. See
/// `a_merge_on_read_input_is_deduped_before_the_rollup_aggregate`.
pub(crate) fn slice_input_sql(
    schema: &crate::schema::TableSchema, dedup: Option<SliceDedup<'_>>, raw: &str, project_id: &str, (start, end): (i64, i64), shard_predicate: &str,
) -> String {
    let window = format!(
        "WHERE project_id = '{}' AND timestamp >= to_timestamp_micros({start}) AND timestamp < to_timestamp_micros({end}){shard_predicate}",
        project_id.replace('\'', "''")
    );
    let Some(dedup) = dedup.filter(|dedup| !dedup.keys.is_empty()) else {
        return format!("SELECT * FROM {raw} {window}");
    };
    let columns = schema.fields.iter().map(|field| quoted(&field.name)).collect::<Vec<_>>().join(", ");
    let keys = dedup.keys.iter().map(|field| quoted(field)).collect::<Vec<_>>().join(", ");
    let order = dedup.tiebreak.map_or_else(|| keys.clone(), |field| format!("{} DESC NULLS LAST", quoted(field)));
    let tombstone = dedup.tombstone.map_or_else(String::new, |field| format!(" AND COALESCE({}, false) = false", quoted(field)));
    format!(
        "SELECT {columns} FROM (SELECT {columns}, ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {order}) AS __tf_rn FROM {raw} \
         {window}) WHERE __tf_rn = 1{tombstone}"
    )
}

impl RoutedRollup {
    /// A half-open range predicate. Only `>=`/`<` are ever emitted: an inclusive
    /// bound on either side of a shared boundary double counts a whole bucket.
    /// `OPEN_END` marks the trailing range of a window the query left unbounded,
    /// which renders with no upper bound at all rather than at some stand-in
    /// instant. It is a sentinel and not a real timestamp: `to_timestamp_micros`
    /// of it is year 294247, so emitting it literally would be both meaningless
    /// and, at the boundary, wrong.
    fn range_sql(&(start, end): &(i64, i64)) -> String {
        if end == OPEN_END {
            return format!("(timestamp >= to_timestamp_micros({start}))");
        }
        format!("(timestamp >= to_timestamp_micros({start}) AND timestamp < to_timestamp_micros({end}))")
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
    fn leg(&self, table: &str, ranges: &[(i64, i64)], extra: &str, projects: &str) -> String {
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
        format!("SELECT {select} FROM {table} WHERE {projects}({ranges}){extra}{row_filters}{group_by}")
    }

    /// `project_id = '…' AND `, or empty when the query groups by project_id.
    /// Both legs and the single-leg shape share it so the rollup and raw sides
    /// can never disagree about which projects they read.
    fn project_predicate(&self) -> String {
        self.project_id.as_deref().map_or_else(String::new, |project| format!("project_id = {} AND ", sql_literal(project)))
    }

    /// `project_id IN (…) AND `, or the pinned predicate when the split names no
    /// subset. Empty only when the query groups by project_id AND every project
    /// proved coverage.
    fn projects_in(&self, projects: Option<&[String]>) -> String {
        match projects {
            Some(list) => format!("project_id IN ({}) AND ", list.iter().map(|p| sql_literal(p)).collect::<Vec<_>>().join(", ")),
            None => self.project_predicate(),
        }
    }

    /// The rewrite. `interior` is the grain-aligned `[a, b)` the rollup leg owns;
    /// the raw leg owns exactly `[lo, a)` and `[b, hi)`, so the three are a
    /// partition of `[lo, hi)` — no gap, no overlap.
    ///
    /// When the interior is the whole window this collapses to a single
    /// statement against the rollup table, which is both cheaper and the shape
    /// the aligned fast path has always emitted.
    pub fn sql(&self, generations: &[(String, String, String)], interiors: &[(i64, i64)], split: &ProjectSplit) -> String {
        let generations = format!(
            " AND ({})",
            generations
                .iter()
                // A generation id hashes the project, so a cross-project rewrite
                // cannot name one per date — it must name one per (project,
                // date) or it would accept another project's generation for the
                // same day.
                .map(|(project, date, generation)| match self.project_id {
                    Some(_) => format!("(date = {} AND rollup_generation = {})", sql_literal(date), sql_literal(generation)),
                    None =>
                        format!("(project_id = {} AND date = {} AND rollup_generation = {})", sql_literal(project), sql_literal(date), sql_literal(generation)),
                })
                .collect::<Vec<_>>()
                .join(" OR ")
        );
        // An open-ended window's raw leg must run to the sentinel, not to the
        // stand-in `hi`: `complement` would otherwise stop the tail there and
        // the rewrite would answer without the newest rows.
        let fringes = complement(self.lo, if self.open_end { OPEN_END } else { self.hi }, interiors);
        let rollup_projects = self.projects_in(split.covered.as_deref());
        // A project whose coverage was not proved is read raw across the WHOLE
        // window. Together with the covered projects' rollup interior and raw
        // fringes that is an exact partition of (project x time): every cell is
        // read by exactly one leg, so nothing is dropped and nothing double
        // counted. Without it a single uncovered project refused the query for
        // every other project.
        let raw_only_leg = (!split.raw_only.is_empty()).then(|| {
            let whole = [(self.lo, if self.open_end { OPEN_END } else { self.hi })];
            self.leg(&self.source, &whole, "", &self.projects_in(Some(&split.raw_only)))
        });
        if fringes.is_empty() && raw_only_leg.is_none() {
            // Single leg: the rollup rows ARE the partial states, so the merge
            // applies directly to the measure columns.
            let select = self
                .groups
                .iter()
                .map(|(expression, alias)| format!("{expression} AS {}", quoted(alias)))
                .chain(self.measures.iter().map(|measure| format!("{} AS {}", measure.merge.sql(&measure.measures), quoted(&measure.alias))))
                .collect::<Vec<_>>()
                .join(", ");
            let row_filters = self.row_filters.iter().map(|filter| format!(" AND ({filter})")).collect::<String>();
            let group_by = self.group_by();
            let having = self.guard.as_ref().map_or_else(String::new, |guard| format!(" HAVING {} > 0", guard.merge.sql(&guard.measures)));
            return format!(
                "SELECT {select} FROM {} WHERE {rollup_projects}({}){generations}{row_filters}{group_by}{having}",
                self.target,
                interiors.iter().map(Self::range_sql).collect::<Vec<_>>().join(" OR "),
            );
        }
        let outer = self
            .groups
            .iter()
            .enumerate()
            .map(|(index, (_, alias))| format!("__g{index} AS {}", quoted(alias)))
            .chain(self.measures.iter().enumerate().map(|(index, measure)| {
                let states = (0..measure.merge.arity()).map(|state| format!("__s{index}_{state}")).collect::<Vec<_>>();
                format!("{} AS {}", measure.merge.sql(&states), quoted(&measure.alias))
            }))
            .collect::<Vec<_>>()
            .join(", ");
        let group_by = self.group_by();
        let having =
            self.guard.as_ref().map_or_else(String::new, |guard| format!(" HAVING {} > 0", guard.merge.sql(&[format!("__s{}_0", self.measures.len())])));
        let mut legs = vec![self.leg(&self.target, interiors, &generations, &rollup_projects)];
        if !fringes.is_empty() {
            legs.push(self.leg(&self.source, &fringes, "", &rollup_projects));
        }
        legs.extend(raw_only_leg);
        format!("SELECT {outer} FROM ({}) AS rollup_union{group_by}{having}", legs.join(" UNION ALL "))
    }
}

fn unaliased(expr: &datafusion::logical_expr::Expr) -> &datafusion::logical_expr::Expr {
    match expr {
        datafusion::logical_expr::Expr::Alias(alias) => unaliased(&alias.expr),
        datafusion::logical_expr::Expr::Cast(cast) => unaliased(&cast.expr),
        expr => expr,
    }
}

/// `COALESCE(<column>, '<literal>')`, returning the column and the literal.
///
/// Two spellings, because DataFusion's simplifier rewrites `coalesce` into a
/// `CASE` before the matcher ever sees it — the form prod logs show as
/// `Case { expr: None, when_then_expr: [(IsNotNull(c), c)], else_expr: Some(lit) }`.
/// Matching only the `ScalarFunction` spelling silently never fires, which is
/// how this shape went unrouted in the first place.
///
/// Deliberately narrow: exactly one `WHEN`, whose predicate and result are the
/// SAME column, and a string literal fallback. A three-argument coalesce, or one
/// over an expression rather than a column, yields `None` and keeps declining.
fn coalesced_column(expr: &datafusion::logical_expr::Expr) -> Option<(&str, &str)> {
    use datafusion::logical_expr::Expr;
    match unaliased(expr) {
        Expr::ScalarFunction(function) if function.name().eq_ignore_ascii_case("coalesce") && function.args.len() == 2 => {
            Some((column_name(&function.args[0])?, string_literal(&function.args[1])?))
        }
        Expr::Case(case) if case.expr.is_none() && case.when_then_expr.len() == 1 => {
            let (when, then) = &case.when_then_expr[0];
            let Expr::IsNotNull(probed) = when.as_ref() else { return None };
            let column = column_name(probed)?;
            (column_name(then)? == column).then_some((column, string_literal(case.else_expr.as_ref()?)?))
        }
        _ => None,
    }
}

/// `extract(epoch from X)` — which DataFusion plans as `date_part('EPOCH', X)`
/// — optionally under an integer cast, returning `X` and the cast's SQL type.
///
/// Only `EPOCH` qualifies: every other field (`hour`, `dow`, …) is many-to-one
/// over buckets and would merge groups the raw path keeps apart. Only integer
/// casts qualify, because those are the ones whose SQL spelling is reproduced
/// exactly; anything else declines rather than guessing a type.
fn epoch_of(expr: &datafusion::logical_expr::Expr) -> Option<(&datafusion::logical_expr::Expr, Option<&'static str>)> {
    use datafusion::logical_expr::Expr;
    let (expr, cast) = match expr {
        Expr::Alias(alias) => (alias.expr.as_ref(), None),
        expr => (expr, None),
    };
    let (expr, cast) = match expr {
        Expr::Cast(inner) => (
            inner.expr.as_ref(),
            Some(match inner.field.data_type() {
                arrow::datatypes::DataType::Int32 => "INT",
                arrow::datatypes::DataType::Int64 => "BIGINT",
                _ => return None,
            }),
        ),
        expr => (expr, cast),
    };
    let Expr::ScalarFunction(function) = unaliased(expr) else { return None };
    (function.name().eq_ignore_ascii_case("date_part")
        && function.args.len() == 2
        && string_literal(&function.args[0]).is_some_and(|field| field.eq_ignore_ascii_case("EPOCH")))
    .then(|| (&function.args[1], cast))
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
    use datafusion::logical_expr::{Expr, Operator};
    fn hint_column(expr: &Expr) -> Option<String> {
        match unaliased(expr) {
            Expr::ScalarFunction(function) if function.name() == "text_match" => match unaliased(function.args.first()?) {
                Expr::Column(column) => Some(column.name.clone()),
                _ => None,
            },
            // The IN-list spelling: the rewriter expands `col IN (a, b, …)`
            // into an OR of per-item `text_match` calls, so the hint arrives as
            // a whole subtree rather than a node. Only an OR whose EVERY leaf
            // hints the SAME column counts — a mixed OR is a real predicate and
            // dropping it would widen the filter.
            Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
                let (left, right) = (hint_column(&binary.left)?, hint_column(&binary.right)?);
                (left == right).then_some(left)
            }
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
) -> Result<Vec<(&'a crate::schema::RollupMeasure, String)>, MissReason> {
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
    let Some(schema) = crate::schema::get_schema(&source).filter(|schema| !schema.rollups.is_empty()) else { return Ok(Vec::new()) };

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
        // `date_trunc(unit, timestamp) = X` bounds the window exactly as
        // `timestamp >= X AND timestamp < X + width(unit)` does. Left to fall
        // through it becomes a residual row filter with no declared measure
        // filter to match, and the whole query is refused — which is how
        // monoscope's hourly overview stayed a raw scan of every project even
        // after the cross-project fix landed.
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let Some((width, start)) = [(&binary.left, &binary.right), (&binary.right, &binary.left)]
                .into_iter()
                .find_map(|(truncated, literal)| Some((date_trunc_width(truncated)?, timestamp_literal(literal)?)))
            else {
                return Ok(false);
            };
            // No timestamp truncates to an unaligned instant, so this is
            // unsatisfiable rather than a window. Serving `[X, X+width)` for it
            // would return an hour of rows where the answer is none.
            if start.rem_euclid(width) != 0 {
                return Err(MissReason::UnknownFilter);
            }
            let end = start.checked_add(width).ok_or(MissReason::UnboundedTime)?;
            *lo = Some(lo.map_or(start, |current: i64| current.max(start)));
            *hi = Some(hi.map_or(end, |current: i64| current.min(end)));
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// The fixed width in microseconds of `date_trunc(unit, timestamp)`, or `None`
/// when the expression is not that.
///
/// Only epoch-aligned, constant-width units are listed. `month`/`quarter`/`year`
/// are not a fixed number of microseconds, and `week` truncates to Monday while
/// the epoch was a Thursday — so for all of them the alignment test the caller
/// applies would be wrong, and no `[X, X+width)` rewrite is exact.
fn date_trunc_width(expr: &datafusion::logical_expr::Expr) -> Option<i64> {
    let datafusion::logical_expr::Expr::ScalarFunction(function) = unaliased(expr) else { return None };
    if !function.name().eq_ignore_ascii_case("date_trunc") || function.args.len() != 2 || column_name(&function.args[1]) != Some("timestamp") {
        return None;
    }
    match string_literal(&function.args[0])?.to_ascii_lowercase().as_str() {
        "second" | "seconds" => Some(1_000_000),
        "minute" | "minutes" => Some(60_000_000),
        "hour" | "hours" => Some(3_600_000_000),
        "day" | "days" => Some(86_400_000_000),
        _ => None,
    }
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
    // `col IS NOT NULL`, held apart from `promotable`. monoscope emits it on EVERY
    // latency chart (`AND duration IS NOT NULL`), which made `filter_not_eligible`
    // the largest miss reason on prod — 2026-08-22's A/B isolated the predicate
    // exactly: the same p95 query declined `filter_not_eligible` with it and
    // `stale_coverage` without it, three reps each.
    let mut null_guards: Vec<&str> = Vec::new();
    // Strip tantivy hints BEFORE classifying, for the same reason `canonical`
    // strips them: they are accelerators the rewriter adds beside a predicate it
    // never removes. Here the stakes are higher than a cosmetic mismatch — a
    // hint on `kind` is orphaned into `promotable` once `kind = 'server'` itself
    // is consumed as a dimension filter, so the promoted filter carries a term
    // no declared measure can ever have and an otherwise routable panel is
    // refused. Observed in prod 2026-08-12.
    let mut terms: Vec<&Expr> = predicates.iter().flat_map(split_conjunction).collect();
    strip_index_hints(&mut terms);
    for term in terms {
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
            // `col IS NOT NULL` over a non-dimension is a residual like any other,
            // except that a `count(col)` measure expresses it EXACTLY — so it is set
            // aside here and resolved as the guard below rather than promoted.
            (None, None) => match unaliased(term) {
                Expr::IsNotNull(inner) if column_name(inner).is_some() => null_guards.push(column_name(inner).unwrap_or_default()),
                _ => promotable.push(term),
            },
        }
    }
    // Two different columns cannot both be expressed by one count measure, and the
    // guard is a single measure by construction. Fail closed rather than guard on
    // one and silently ignore the other.
    null_guards.dedup();
    if null_guards.len() > 1 {
        return Err(MissReason::FilterNotEligible);
    }
    let null_guard = null_guards.first().copied();
    // A query that neither pins nor groups by project_id would fold every
    // tenant into one row, so it stays refused. Grouping by it is answerable:
    // project_id is a real column on the rollup table, and the coverage check
    // downstream then has to hold for every project in the window rather than
    // for one.
    if project_id.is_none() && !aggregate.group_expr.iter().any(|expr| column_name(expr) == Some("project_id")) {
        return Err(MissReason::MissingProject);
    }
    // A dashboard panel writes its window as `timestamp >= now() - interval 'N
    // days'` and stops there. Demanding an upper bound sent every such query —
    // the wide ones, the only kind worth accelerating — to a full raw scan.
    // Prod 2026-08-15: a 7d panel timed out past the 60s statement cap and the
    // same query with `AND timestamp < now()` returned in 5.5s.
    //
    // `now` closes the window for the interior arithmetic ONLY. `open_end`
    // carries the missing bound through to `sql`, which leaves the trailing raw
    // range unbounded so rows past `now` are still returned.
    let open_end = hi.is_none();
    let (lo, hi) = lo.zip(hi.or_else(|| Some(crate::support::now_micros()))).filter(|(lo, hi)| lo < hi).ok_or(MissReason::UnboundedTime)?;
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
    // The probe's project literal is scaffolding the canonicalizer strips back
    // out — it exists only to satisfy the scan admission guard, and the result
    // is memoized across every caller. A cross-project query has no literal to
    // give it, and any value satisfies the guard's SHAPE check.
    let probe_project = project_id.as_deref().unwrap_or("rollup-probe");
    let configured_filters = measure_filters(session, source, spec, probe_project, lo, hi).await?;

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
    //
    // `col IS NOT NULL` is the same rule read one step wider. `count(col)` skips
    // nulls, so a measure `{agg: count, column: col, filter: F}` IS
    // `count(*) FILTER (WHERE F AND col IS NOT NULL)` — one lookup covers the bare
    // predicate, the promoted filter, and their conjunction, and it fails closed
    // when no such measure is declared. The conjunction matters: guarding on
    // `server_request_count > 0` and `duration_count > 0` SEPARATELY is weaker than
    // the raw query, because a bucket holding server rows with null duration beside
    // non-server rows with duration passes both and raw eliminates it.
    let promoted = (!promotable.is_empty()).then(|| canonical_and(promotable.iter().copied()));
    let guard = match (promoted.as_deref(), null_guard) {
        (None, None) => None,
        (promoted, column) => Some(
            configured_filters
                .iter()
                .find(|(measure, filter)| measure.agg == "count" && measure.column.as_deref() == column && filter.as_str() == promoted.unwrap_or_default())
                .map(|(measure, _)| *measure)
                .ok_or_else(|| {
                    let declared = || {
                        configured_filters
                            .iter()
                            .filter(|(measure, _)| measure.agg == "count" && measure.column.as_deref() == column)
                            .map(|(measure, filter)| format!("{}={filter}", measure.name))
                            .collect::<Vec<_>>()
                            .join(" | ")
                    };
                    // A residual constraining columns NO declared filter even
                    // mentions was never a candidate — it is log-explorer and
                    // facet traffic (`attributes___…___name IS NOT NULL`,
                    // `jsonb_path_exists`, `LIKE`), not a promotion that failed.
                    // Prod 2026-08-12: 84 declines in 3h, every one of them this
                    // shape and not one a dashboard panel. Folding those into
                    // `unknown_filter` made the counter unreadable — it could not
                    // distinguish "should have matched and didn't" from "never
                    // eligible" — and warning about them 84 times in 3h on a hot
                    // path is how the real case gets buried.
                    // A null guard is eligible on the same test read through the
                    // measure's COLUMN rather than its filter text: `duration IS
                    // NOT NULL` is a near-miss because `duration_count` exists,
                    // while `attributes___…___name IS NOT NULL` is log-explorer
                    // traffic no measure was ever going to answer.
                    let guard_column_declared =
                        column.is_some_and(|column| configured_filters.iter().any(|(measure, _)| measure.column.as_deref() == Some(column)));
                    if !guard_column_declared
                        && !promotable
                            .iter()
                            .flat_map(|expr| expr.column_refs())
                            .any(|column| configured_filters.iter().any(|(_, filter)| filter.contains(column.name.as_str())))
                    {
                        tracing::debug!(
                            event = "rollup_promotion_not_eligible",
                            source,
                            spec = spec.name.as_deref().unwrap_or_default(),
                            promoted = promoted.unwrap_or_default(),
                            null_guard = column.unwrap_or_default(),
                            "a residual row filter constrains columns no declared measure uses"
                        );
                        return MissReason::FilterNotEligible;
                    }
                    // A genuine near-miss: the residual talks about the same
                    // columns a declared measure filters on, yet did not match.
                    // The two canonical strings are the whole content of this
                    // decline and there is no way to see them from outside. This
                    // has already cost two deploys to diagnose; print both.
                    tracing::warn!(
                        event = "rollup_promotion_unmatched",
                        source,
                        spec = spec.name.as_deref().unwrap_or_default(),
                        promoted = promoted.unwrap_or_default(),
                        null_guard = column.unwrap_or_default(),
                        declared = %declared(),
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
        // monoscope's chart SQL groups by `extract(epoch from time_bucket(w,
        // timestamp))::integer`, never by the bare bucket, so every percentile
        // and grouped panel declined as `unsupported_shape` and scanned raw —
        // 3,525 ms against 278 ms routed at 3 days (2026-08-22 A/B).
        //
        // Epoch-of-bucket is injective on the bucket, so the grouping is
        // identical and only the spelling differs. The wrapper is REPRODUCED
        // rather than lifted: the rewrite is substituted for this aggregate and
        // must match its schema types, so dropping a cast here would fail
        // `has_equivalent_names_and_types` instead of routing.
        let epoch_wrapped = epoch_of(expression);
        let expression = match epoch_wrapped.map_or_else(|| unaliased(expression), |(inner, _)| inner) {
            // `project_id` is not a declared dimension — it is the partition
            // column, written on every rollup row by `to_rollup_batches` — but
            // it groups exactly like one.
            Expr::Column(column) if column.name == "project_id" || spec.dimensions.iter().any(|dimension| dimension == &column.name) => column.name.clone(),
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
            // monoscope spells EVERY grouped chart's dimension
            // `COALESCE(<dimension>, 'null')`, so the bare-column arm above
            // never matched one and every such panel scanned raw — 3,237 ms
            // against 276 ms routed at 3 days (2026-08-22 A/B).
            //
            // Sound because `COALESCE(dim, lit)` is a FUNCTION of `dim`: the
            // rollup's partition by `dim` therefore REFINES the partition by
            // `COALESCE(dim, lit)`, and re-aggregating decomposable states over
            // a refinement equals aggregating the raw rows. So the expression is
            // emitted verbatim onto both legs and NULL needs no special case —
            // the NULL cell and the literal-'null' cell simply merge, exactly as
            // the raw rows do.
            other
                if coalesced_column(other).is_some_and(|(column, _)| column == "project_id" || spec.dimensions.iter().any(|dimension| dimension == column)) =>
            {
                let (column, fallback) = coalesced_column(other).ok_or(MissReason::UnsupportedShape)?;
                format!("COALESCE({column}, {})", sql_literal(fallback))
            }
            Expr::Column(_) => return Err(MissReason::UnknownGroupBy),
            _ => return Err(MissReason::UnsupportedShape),
        };
        // Only the bucket may wear the epoch wrapper; a dimension grouped by
        // `extract(epoch …)` is nonsense and must not be silently accepted.
        let expression = match epoch_wrapped {
            Some((_, cast)) if expression.starts_with("time_bucket(") => {
                let epoch = format!("date_part('EPOCH', {expression})");
                match cast {
                    Some(sql_type) => format!("CAST({epoch} AS {sql_type})"),
                    None => epoch,
                }
            }
            Some(_) => return Err(MissReason::UnsupportedShape),
            None => expression,
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
        // Dropping `col IS NOT NULL` is only sound for aggregates that skip nulls
        // over THAT column. `SELECT count(*), p95(duration) … WHERE duration IS NOT
        // NULL` is real monoscope traffic (the top-K tables) and its `count(*)`
        // counts only rows with a duration, where `request_count` counts them all.
        if null_guard.is_some() && column.as_deref() != null_guard {
            return Err(MissReason::FilterNotEligible);
        }
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
            // `hll_agg`, a.k.a. Toolkit's `approx_count_distinct`. It behaves
            // exactly like `percentile_agg` above: the aggregate yields the STATE
            // and the scalar that reads a number out of it (`distinct_count`)
            // sits above, untouched. DataFusion's own `approx_distinct` is NOT
            // routed — it returns a bare count with no state to store. Exact
            // `COUNT(DISTINCT x)` stays non-decomposable and is still declined;
            // approximating it without being asked is what the measure list
            // refuses to do.
            "hll_agg" => (Merge::Hll, measure("hll", column.as_deref()).map(|m| vec![m])),
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
                    (Merge::Hll, Some(column)) => format!("hll_agg({column})"),
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
        // `COUNT(col)` when the guard carries one — the raw leg must eliminate the
        // same buckets the rollup leg's `HAVING sum(count(col)) > 0` does, and
        // `COUNT(*)` there would keep an all-null bucket the rollup drops.
        raw: vec![{
            let counted = measure.column.as_deref().map_or_else(|| "COUNT(*)".to_string(), |column| format!("COUNT({column})"));
            match measure.filter.as_ref() {
                Some(filter) => format!("{counted} FILTER (WHERE {filter})"),
                None => counted,
            }
        }],
    });

    Ok(RoutedRollup {
        source: source.to_string(),
        project_id,
        lo,
        hi,
        open_end,
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

    /// `MissReason::label` feeds the `rollup_misses` counter, so these strings
    /// are a prod dashboard contract, not an implementation detail. Pinned
    /// exhaustively (via `EnumIter`) so a rename or a new variant has to state
    /// its label here rather than silently changing what a panel counts.
    #[test]
    fn miss_reason_labels_are_the_prod_telemetry_contract() {
        use strum::IntoEnumIterator as _;
        assert_eq!(
            MissReason::iter().map(MissReason::label).collect::<Vec<_>>(),
            [
                "unsupported_shape",
                "missing_project",
                "unbounded_time",
                "unknown_group_by",
                "unknown_filter",
                "filter_not_eligible",
                "missing_measure",
                "non_decomposable",
                "unaligned_bucket_width",
                "not_built",
                "stale_coverage",
                "incomplete_coverage",
                "tiny_interior",
                "too_many_branches",
                "rewrite_schema_mismatch",
            ]
        );
    }

    use crate::maintenance_coordinator::DAY_MICROS;
    /// Any day-aligned instant; `slice_retires` takes the partition date as a
    /// label rather than deriving it, so which day this is does not matter.
    const DAY: i64 = 20_683 * DAY_MICROS;
    const HOUR: i64 = HOUR_MICROS;

    /// An untagged tier file used to be IMMORTAL — `slice_tag_range` returned
    /// `None`, the replace-set skipped it, and every rebuild stacked another
    /// version of every `id` beside it forever. It is retired only when this
    /// partition provably reproduces it.
    #[test_case::test_case(Some((DAY, DAY + HOUR)), None, (DAY, DAY + DAY_MICROS), &[], 9, true; "tagged file contained by the slice")]
    #[test_case::test_case(Some((DAY - HOUR, DAY + DAY_MICROS)), None, (DAY, DAY + DAY_MICROS), &[], 9, false; "tagged file wider than the slice")]
    #[test_case::test_case(None, None, (DAY, DAY + DAY_MICROS), &[], 9, true; "untagged, whole-day slice needs no stats")]
    #[test_case::test_case(None, None, (DAY, DAY + HOUR), &[], 9, false; "untagged, sub-day slice with no stats proves nothing")]
    #[test_case::test_case(None, Some((DAY, DAY + HOUR - 1)), (DAY, DAY + HOUR), &[], 9, true; "untagged, stats inside a sub-day slice")]
    #[test_case::test_case(None, Some((DAY, DAY + HOUR)), (DAY, DAY + HOUR), &[], 9, false; "untagged, stats touch the exclusive end")]
    #[test_case::test_case(None, Some((DAY, DAY + HOUR - 1)), (DAY, DAY + HOUR), &[], 0, false; "empty rebuild may be the only copy left")]
    // The split-tenant shape: no single slice contains the file, but the live
    // tagged slices tile the range it spans.
    #[test_case::test_case(None, Some((DAY, DAY + DAY_MICROS - 1)), (DAY, DAY + 6 * HOUR),
        &[(DAY, DAY + 6 * HOUR), (DAY + 6 * HOUR, DAY + 12 * HOUR), (DAY + 12 * HOUR, DAY + DAY_MICROS)], 9, true; "union of live slices tiles the file")]
    #[test_case::test_case(None, Some((DAY, DAY + DAY_MICROS - 1)), (DAY, DAY + 6 * HOUR),
        &[(DAY, DAY + 6 * HOUR), (DAY + 12 * HOUR, DAY + DAY_MICROS)], 9, false; "union has a hole so nothing is proven")]
    fn slice_retires_only_what_the_partition_provably_reproduces(
        slice: Option<(i64, i64)>, stats: Option<(i64, i64)>, published: (i64, i64), covered: &[(i64, i64)], rows: u64, expected: bool,
    ) {
        let file = LiveFile { slice, project: slice.map(|_| "p"), partition: Some(("p", "2026-08-18")), stats };
        let publish = SlicePublish { project_id: "p", date: "2026-08-18", slice: published, rows, covered };
        assert_eq!(slice_retires(&file, &publish), expected);
    }

    /// Identity still gates it: another project's file, or another day's, is not
    /// reproduced by this slice however wide it is.
    #[test_case::test_case(Some(("other", "2026-08-18")); "another project")]
    #[test_case::test_case(Some(("p", "2026-08-17")); "another day")]
    #[test_case::test_case(None; "no readable partition")]
    fn slice_never_retires_an_untagged_file_outside_its_own_partition(partition: Option<(&str, &str)>) {
        let file = LiveFile { slice: None, project: None, partition, stats: Some((DAY, DAY + HOUR)) };
        let publish = SlicePublish { project_id: "p", date: "2026-08-18", slice: (DAY, DAY + DAY_MICROS), rows: 9, covered: &[] };
        assert!(!slice_retires(&file, &publish));
    }

    /// The untagged gauge must not be masked by a sibling tier.
    ///
    /// It is one exported slot and the tiers publish independently, so storing each publish's own
    /// count overwrote it: prod 2026-08-20 read `rollup_tier_untagged_found = 0` — after a publish
    /// to the already-clean 1h tier — while the 1m tier still held 67 untagged files across 39
    /// cells. A gauge that reads clean over live damage is worse than no gauge, and this one exists
    /// precisely to catch a month of silent accumulation.
    #[test]
    fn the_untagged_gauge_sums_tiers_rather_than_letting_one_mask_another() {
        let per_tier: dashmap::DashMap<String, u64> = dashmap::DashMap::new();
        let exported = || -> u64 { per_tier.iter().map(|entry| *entry.value()).sum() };

        per_tier.insert("otel_logs_and_spans_rollup_dashboard_1m_v3".to_owned(), 67);
        assert_eq!(exported(), 67);

        // The clean tier publishes next. Under the shared-slot version this
        // reported 0 and the 1m tier's 67 files became invisible.
        per_tier.insert("otel_logs_and_spans_rollup_dashboard_1h_v2".to_owned(), 0);
        assert_eq!(exported(), 67, "a clean tier's publish must not hide another tier's damage");

        // Zero is reached only when EVERY tier is clean — which is what makes
        // "alarm on > 0" a statement about the whole tier population.
        per_tier.insert("otel_logs_and_spans_rollup_dashboard_1m_v3".to_owned(), 0);
        assert_eq!(exported(), 0);
    }

    #[test]
    fn ranges_cover_needs_an_unbroken_run_past_the_inclusive_end() {
        // Out of order, and adjacent rather than overlapping: both are normal
        // for slices republished at different widths.
        assert!(ranges_cover(&[(DAY + HOUR, DAY + 2 * HOUR), (DAY, DAY + HOUR)], (DAY, DAY + 2 * HOUR - 1)));
        // Reaching exactly the inclusive end is NOT enough: a slice end is
        // exclusive, so a row AT `hi` would not be reproduced.
        assert!(!ranges_cover(&[(DAY, DAY + HOUR)], (DAY, DAY + HOUR)));
        assert!(!ranges_cover(&[], (DAY, DAY)));
        // A gap anywhere, even one covered by a LATER range, breaks the proof.
        assert!(!ranges_cover(&[(DAY, DAY + HOUR), (DAY + 2 * HOUR, DAY + 9 * HOUR)], (DAY, DAY + 3 * HOUR)));
    }

    /// The complement is what must be REBUILT, and it is what makes the repair
    /// affordable: prod 2026-08-22's mid-tail holes were 22 minutes wide inside
    /// a day whose rollup input shreds into ~1,440 units.
    #[test]
    fn uncovered_gaps_are_the_complement_of_the_live_tagged_ranges() {
        // Nothing tagged: the whole span is the gap, which is the old
        // rebuild-the-day behaviour and the floor this must never fall below.
        assert_eq!(uncovered_gaps(&[(DAY, DAY + 2 * HOUR)], &[]), vec![(DAY, DAY + 2 * HOUR)]);
        // One interior hole between two published slices.
        assert_eq!(uncovered_gaps(&[(DAY, DAY + 3 * HOUR)], &[(DAY, DAY + HOUR), (DAY + 2 * HOUR, DAY + 3 * HOUR)]), vec![(DAY + HOUR, DAY + 2 * HOUR)]);
        // Fully covered — no work at all, and the last range reaching exactly
        // the exclusive end is enough.
        assert!(uncovered_gaps(&[(DAY, DAY + HOUR)], &[(DAY, DAY + HOUR)]).is_empty());
        // Ranges that overrun the span are clipped to it, out-of-order input is
        // fine, and two files sharing a hole report it once.
        assert_eq!(
            uncovered_gaps(&[(DAY + HOUR, DAY + 2 * HOUR), (DAY + HOUR, DAY + 2 * HOUR)], &[(DAY + 90 * 60_000_000, DAY + 9 * HOUR), (DAY, DAY + HOUR)]),
            vec![(DAY + HOUR, DAY + 90 * 60_000_000)]
        );
    }

    #[test]
    fn hours_from_stats_json_bounds_the_hours_a_file_can_touch() {
        let day_start = chrono::NaiveDate::from_ymd_opt(2026, 8, 18).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
        let at = |hour: i64, minute: i64| day_start + hour * HOUR_MICROS + minute * 60 * 1_000_000;

        // Epoch micros form: a file spanning 09:10..10:45 touches hours 9 and 10.
        let stats = format!(r#"{{"numRecords": 100, "minValues": {{"timestamp": {}}}, "maxValues": {{"timestamp": {}}}}}"#, at(9, 10), at(10, 45));
        assert_eq!(hours_from_stats_json(&stats, day_start), Some(0b11 << 9));

        // RFC 3339 form: a point file touches exactly one hour.
        let stats = r#"{"minValues": {"timestamp": "2026-08-18T03:00:00Z"}, "maxValues": {"timestamp": "2026-08-18T03:59:59.999999Z"}}"#;
        assert_eq!(hours_from_stats_json(stats, day_start), Some(1 << 3));

        // A whole-day file is honestly all hours.
        let stats = format!(r#"{{"minValues": {{"timestamp": {}}}, "maxValues": {{"timestamp": {}}}}}"#, day_start, day_start + 24 * HOUR_MICROS - 1);
        assert_eq!(hours_from_stats_json(&stats, day_start), Some(ALL_HOURS));

        // Bounds spilling outside the partition day are clamped, never trusted
        // into an empty mask.
        let stats = format!(r#"{{"minValues": {{"timestamp": {}}}, "maxValues": {{"timestamp": {}}}}}"#, day_start - 5 * HOUR_MICROS, at(2, 0));
        assert_eq!(hours_from_stats_json(&stats, day_start), Some(0b111));
        let stats = format!(r#"{{"minValues": {{"timestamp": {}}}, "maxValues": {{"timestamp": {}}}}}"#, day_start - 5 * HOUR_MICROS, day_start - HOUR_MICROS);
        assert_eq!(hours_from_stats_json(&stats, day_start), None, "a file wholly outside the day must read as unknown, not as nothing");

        // No stats, no timestamp bounds, garbage: all unknown.
        assert_eq!(hours_from_stats_json(r#"{"numRecords": 5}"#, day_start), None);
        assert_eq!(hours_from_stats_json("not json", day_start), None);
    }

    fn spec() -> RollupSpec {
        crate::schema::get_schema(SOURCE).expect("source schema").rollups.first().expect("declared rollup").clone()
    }

    /// A window with a lower bound and NO upper bound must still route.
    ///
    /// Prod 2026-08-15: monoscope's 7d and 14d panels timed out at the 60s
    /// statement cap. `route_with_spec` demanded BOTH bounds and answered
    /// `UnboundedTime`, so a window with fully built rollups fell back to a raw
    /// scan of every row in it. Measured on the demo project: `timestamp >=
    /// now() - interval '7 days'` timed out past 60s, while the identical query
    /// plus `AND timestamp < now()` returned in 5.5s.
    #[tokio::test]
    async fn an_open_ended_window_routes() {
        let state = session().await;
        let sql = format!(
            "SELECT count(*) FROM {SOURCE} WHERE project_id = 'p' AND timestamp >= to_timestamp_micros(1786500000000000) GROUP BY resource___service___name"
        );
        let route = route_for(&state, &sql).await;
        assert!(route.is_ok(), "an open-ended window must route: {route:?}");
    }

    /// The open end must reach the raw leg, not be silently closed at plan time.
    ///
    /// Substituting a concrete `hi` makes the interior computable, but if that
    /// same `hi` also closed the trailing fringe the rewrite would DROP every
    /// row after it — the newest rows, which is exactly what a live dashboard
    /// is looking at. The last raw range must carry no upper bound.
    #[tokio::test]
    async fn an_open_ended_window_keeps_an_open_raw_tail() {
        let state = session().await;
        let sql = format!(
            "SELECT count(*) FROM {SOURCE} WHERE project_id = 'p' AND timestamp >= to_timestamp_micros(1786500000000000) GROUP BY resource___service___name"
        );
        let route = route_for(&state, &sql).await.expect("route").expect("a route");
        let generated = hybrid_sql(&route, crate::support::now_micros());
        let tail = generated.rsplit("timestamp >=").next().expect("a trailing range");
        assert!(!tail.contains("timestamp <"), "the trailing raw range must stay open-ended, got: {generated}");
    }

    /// The partition invariant has to survive the open end.
    ///
    /// `complement` is what guarantees the rollup interior and the raw fringes
    /// cover the window exactly once. A gap is a silently missing row and an
    /// overlap is a silently doubled one, and no aggregate above can detect
    /// either. Substituting a stand-in `hi` is only safe while this holds all
    /// the way out to `OPEN_END`.
    #[test]
    fn an_open_ended_window_is_still_partitioned_exactly() {
        let (grain, lo) = (60_000_000i64, 1_786_500_000_000_000i64);
        let horizon = lo + grain * 90;
        let inner = interiors(lo, lo + grain * 100, grain, horizon, &[(lo, horizon)]);
        assert!(!inner.is_empty(), "the fixture must produce an interior to complement");

        let mut ranges: Vec<(i64, i64)> = inner.iter().chain(complement(lo, OPEN_END, &inner).iter()).copied().collect();
        ranges.sort_unstable();
        assert_eq!(ranges.first().expect("ranges").0, lo, "coverage must start at lo");
        assert_eq!(ranges.last().expect("ranges").1, OPEN_END, "coverage must run to the open end, or the newest rows are dropped");
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].1, pair[1].0, "no gap and no overlap between {:?} and {:?}", pair[0], pair[1]);
        }
    }

    /// A hint whose own predicate was consumed as a DIMENSION filter must not be
    /// left behind in the promotable set. Prod 2026-08-12 showed exactly this:
    /// `kind = 'server'` routed as a dimension filter while
    /// `text_match(kind,"server")` stayed as a residual, so the promoted filter
    /// carried a term no declared measure could ever have.
    #[tokio::test]
    async fn a_hint_beside_a_dimension_filter_does_not_become_a_residual() {
        let state = session().await;
        let hinted = format!(
            "SELECT count(*) FROM {SOURCE} WHERE project_id = 'p' AND kind = 'server' AND text_match(kind, 'server')              AND timestamp >= to_timestamp_micros(1786500000000000) AND timestamp < to_timestamp_micros(1786530000000000)              GROUP BY resource___service___name"
        );
        let plain = hinted.replace(" AND text_match(kind, 'server')", "");
        let (with, without) = (route_for(&state, &hinted).await, route_for(&state, &plain).await);
        assert!(without.is_ok(), "the un-hinted control must route: {without:?}");
        assert_eq!(with.is_ok(), without.is_ok(), "a hint must not change whether the query routes: {with:?}");
    }

    /// The IN-list spelling of the same hint, which is the one prod actually
    /// carries. `tantivy_rewriter` expands `kind IN (a, b)` into an **OR of
    /// per-item `text_match` calls**, so the leftover hint is not a bare
    /// `text_match` node but an `Or` tree — invisible to a stripper that only
    /// looks at the top of each conjunct. Measured 2026-08-22 on prod
    /// `b6d8c86`: `select distinct project_id … kind in (?,?,?,?)` promoted
    /// exactly `(text_match(kind,"client") OR … OR text_match(kind,"server"))`
    /// with its own `IN` already consumed as a dimension filter, and declined
    /// `unknown_filter` — 33 of 155 misses, against `rollup_hits_* = 0`.
    #[tokio::test]
    async fn an_in_list_hint_or_tree_does_not_become_a_residual() {
        let state = session().await;
        let hint = " AND (text_match(kind, 'server') OR text_match(kind, 'client'))";
        let plain = format!(
            "SELECT count(*) FROM {SOURCE} WHERE project_id = 'p' AND kind IN ('server', 'client')              AND timestamp >= to_timestamp_micros(1786500000000000) AND timestamp < to_timestamp_micros(1786530000000000)              GROUP BY resource___service___name"
        );
        let hinted = plain.replace(" AND timestamp >=", &format!("{hint} AND timestamp >="));
        let (with, without) = (route_for(&state, &hinted).await, route_for(&state, &plain).await);
        assert_eq!(with.is_ok(), without.is_ok(), "an OR-of-text_match hint must not change whether the query routes: hinted={with:?} plain={without:?}");
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
                crate::tantivy::udf::text_match_udf().into(),
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

    /// A derived tier reads the BASE TIER, which is merge-on-read: a rebuilt
    /// bucket appends a new version instead of replacing the old one. If that
    /// input is not deduped, the derived aggregate SUMs every superseded
    /// version and the tier is permanently wrong until the day is rebuilt.
    ///
    /// Prod 2026-08-20, project 98fdd4f3, hour 08-18 10:00: the 1m tier held
    /// 2,453 rows across 342 distinct ids (7.17 versions each), and the 1h tier
    /// derived from it reported 157,110 requests against a true 31,018 — every
    /// measure inflated by the same factor. A day whose base tier held one
    /// version per id (08-13) was exact, which is what identifies version
    /// multiplicity rather than the source data as the cause.
    #[test]
    fn a_merge_on_read_input_is_deduped_before_the_rollup_aggregate() {
        let base = crate::schema::get_schema("otel_logs_and_spans_rollup_dashboard_1m_v3").expect("the 1m tier is a declared rollup target");
        // A tier declares its identity, so reads collapse superseded versions
        // the same way the maintenance read does — see `synthesize` for why the
        // original "no read-time dedup" decision was reversed. `rollup_tier_dedup`
        // still exists because the maintenance read registers the tier directly
        // rather than through the routing table, so it cannot rely on the
        // planner having inserted a `DedupExec`.
        assert_eq!(base.dedup_keys, ["timestamp", "id"], "a rollup tier must declare its identity so reads collapse versions");
        let (keys, tiebreak, tombstone) = rollup_tier_dedup(base).expect("a generated tier carries timestamp/id/updated_at");

        let dedup = SliceDedup { keys: &keys, tiebreak: Some(tiebreak), tombstone };
        let sql = slice_input_sql(base, Some(dedup), "__raw", "p", (0, 60_000_000), "");
        assert!(sql.contains("ROW_NUMBER() OVER (PARTITION BY"), "a merge-on-read input must be collapsed to one row per key, got: {sql}");
        assert!(sql.contains("__tf_rn = 1"), "the collapse must keep exactly one version per key, got: {sql}");
        assert!(
            sql.contains("\"updated_at\" DESC NULLS LAST"),
            "it must keep the GREATEST version — keeping an arbitrary one is a different wrong answer, got: {sql}"
        );

        // The shape that actually ran in production: `derived` passed no dedup at
        // all, so the aggregate above summed every version. Kept as an explicit
        // contrast so the regression stays legible.
        let undeduped = slice_input_sql(base, None, "__raw", "p", (0, 60_000_000), "");
        assert!(!undeduped.contains("__tf_rn"), "no dedup means a bare SELECT; this is the shape that over-counted");
    }

    #[test]
    fn declared_rollup_is_generated_with_its_configured_fields() {
        let source = crate::schema::get_schema(SOURCE).expect("source schema");
        let spec = spec();
        let target = crate::schema::get_schema(&spec.table_name(SOURCE)).expect("generated rollup schema");
        for name in spec.dimensions.iter().chain(spec.measures.iter().map(|measure| &measure.name)) {
            assert!(target.fields.iter().any(|field| field.name == *name), "missing configured rollup field `{name}`");
        }
        assert!(target.fields.iter().any(|field| field.name == "rollup_generation"));
        assert_eq!(target.partitions, source.partitions);
        assert!(!target.version_append);
        // A rollup must declare NO dedup keys. `replace_rollup_partition` removes
        // every existing file in the partition in the same commit that adds the
        // Both tiers get the SAME identity, base and derived alike. The
        // asymmetry that preceded this — a derived tier protected by its input
        // collapse while the base tier had no read-time defence at all — is what
        // let one tier read 1.00 versions per id while the other read 5.64 with
        // no visible difference between them.
        assert_eq!(target.dedup_keys, ["timestamp", "id"], "every rollup tier declares the same identity");
        assert_eq!(target.dedup_tiebreak.as_deref(), Some("updated_at"), "keep-greatest needs the tiebreak");
    }

    #[test]
    fn slice_generation_is_stable_across_source_fingerprints() {
        let spec = spec();
        assert_eq!(generation_id(&spec, SOURCE, "p", "2026-08-15", 1), generation_id(&spec, SOURCE, "p", "2026-08-15", 2));
    }

    #[test]
    fn build_sql_uses_exact_count_and_tdigest_states() {
        let sql = build_partition_sql(&spec(), SOURCE, "pro'ject", "2026-08-01").expect("valid SQL");
        assert!(sql.contains("COUNT(duration) AS duration_count"));
        assert!(sql.contains("percentile_agg(CAST(duration AS DOUBLE))"));
        assert!(sql.contains("project_id = 'pro''ject'"));
        assert!(sql.contains("GROUP BY 1, 2, 3, 4"));
    }

    /// The two SQL shapes an `hll` measure has to produce: build the sketch from
    /// raw rows, and RE-AGGREGATE it when a coarse tier derives from a fine one.
    /// Getting the second wrong is silent — `SUM` over a Binary column is the
    /// default branch — so it is asserted rather than assumed.
    #[test]
    fn an_hll_measure_builds_a_sketch_and_re_aggregates_as_one() {
        let spec = |derive_from: Option<&str>| RollupSpec {
            grain: "1h".into(),
            name: Some("hll_shape".into()),
            dimensions: vec!["kind".into()],
            measures: vec![crate::schema::RollupMeasure {
                name: "traces".into(),
                agg: "hll".into(),
                column: Some("context___trace_id".into()),
                filter: Some("kind = 'server'".into()),
            }],
            derive_from: derive_from.map(str::to_string),
        };
        let raw = build_partition_sql(&spec(None), SOURCE, "project", "2026-08-01").expect("valid SQL");
        assert!(raw.contains("hll_agg(context___trace_id) FILTER (WHERE kind = 'server') AS traces"), "{raw}");

        // The declared filter is deliberately NOT re-applied on the derived leg:
        // the base row already had it, and its columns do not exist there.
        let derived = build_partition_sql_from(&spec(Some("fine")), SOURCE, "fine_table", "project", "2026-08-01").expect("valid SQL");
        assert!(derived.contains("hll_merge(CAST(traces AS BYTEA)) AS traces"), "{derived}");
        assert!(!derived.contains("FILTER"), "the derived leg must not re-apply the measure filter: {derived}");
    }

    /// `distinct_count(approx_count_distinct(x))` is what monoscope sends. The
    /// aggregate in the plan is `hll_agg`, and it must fold to the merged STATE —
    /// the `distinct_count` accessor stays in the projection above, exactly as
    /// `approx_percentile` does over `percentile_agg`.
    #[test]
    fn the_hll_merge_folds_states_and_leaves_the_accessor_alone() {
        assert_eq!(Merge::Hll.arity(), 1);
        assert_eq!(Merge::Hll.partial_op(), "hll_merge");
        assert_eq!(Merge::Hll.sql(&["__s0_0".to_string()]), "hll_merge(__s0_0)");
    }

    const TARGET: &str = "otel_logs_and_spans_rollup_dashboard_1m_v3";
    /// Ten grains wide. A window narrower than `MIN_INTERIOR_BUCKETS` grains can
    /// never route — the aligned interior would be at most one bucket — so a
    /// one-minute fixture would exercise the rejection path, not the matcher.
    const WINDOW: &str = "timestamp >= to_timestamp_micros(60000000) AND timestamp < to_timestamp_micros(660000000)";
    const WIDE_HORIZON: i64 = 540_000_000;

    /// Register the source AND its generated rollup so a route can be planned
    /// back into a real logical plan.
    async fn session() -> datafusion::execution::context::SessionState {
        let mut ctx = datafusion::prelude::SessionContext::new();
        crate::read::functions::register_custom_functions(&mut ctx).expect("functions register");
        // Every declared tier, not just the fine one: specs are tried
        // coarsest-first, so a session missing the coarse table could not plan a
        // rewrite the matcher legitimately chose.
        let targets = crate::schema::get_schema(SOURCE).expect("source schema").rollups.iter().map(|spec| spec.table_name(SOURCE)).collect::<Vec<_>>();
        for table_name in std::iter::once(SOURCE.to_string()).chain(targets) {
            let table = datafusion::datasource::MemTable::try_new(crate::schema::get_schema(&table_name).expect("schema").schema_ref(), vec![vec![]])
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
        route.sql(&[("project".into(), "1970-01-01".into(), "generation".into())], &[(route.lo, route.hi)], &ProjectSplit::default())
    }

    /// The rewrite when only part of the window is certified, so raw fringes and
    /// a live tail are unioned in.
    fn hybrid_sql(route: &RoutedRollup, horizon: i64) -> String {
        let interior = interior(route.lo, route.hi, route.grain, horizon).expect("a routable interior");
        route.sql(&[("project".into(), "1970-01-01".into(), "generation".into())], &[interior], &ProjectSplit::default())
    }

    /// The latency panel percentiles EVERY span, but the only declared tdigest
    /// carried the `server` filter, so even with the group-by fixed the query
    /// declined as `missing_measure` and scanned raw (2026-08-22: 3,420 ms with
    /// the group-by lifted, 278 ms once an unfiltered digest existed).
    ///
    /// Adding a measure changes `generation_id` — it hashes the whole spec — so
    /// every pre-existing cell becomes unreadable and the tier rebuilds rather
    /// than serving NULL digests as zeroed percentiles.
    #[tokio::test]
    async fn an_unfiltered_percentile_routes_to_the_unfiltered_digest() {
        let state = session().await;
        let sql = format!(
            "SELECT time_bucket('1 hours', timestamp) AS tb, percentile_agg(CAST(duration AS DOUBLE PRECISION)) \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1"
        );
        let route = route_for(&state, &sql).await.expect("match percentile").expect("declared rollup route");
        let generated = generated_sql(&route);
        assert!(generated.contains("duration_digest"), "the unfiltered digest must answer it: {generated}");
        assert!(!generated.contains("server_duration_digest"), "the server-filtered digest must NOT answer an unfiltered percentile: {generated}");
        assert_substitutes(&state, &sql, None).await;
    }

    /// THE shape monoscope's grouped charts emit for a dimension:
    /// `COALESCE(<dimension>, 'null')`, never the bare column. Every such panel
    /// declined as `unsupported_shape` and scanned raw — 3,237 ms against
    /// 276 ms routed at 3 days (2026-08-22 A/B) — and the two-key form below is
    /// the real one, bucket AND dimension together.
    #[tokio::test]
    async fn a_grouped_chart_coalescing_its_dimension_routes() {
        let state = session().await;
        let sql = format!(
            "SELECT time_bucket('1 hours', timestamp) AS tb, COALESCE(resource___service___name, 'null') AS svc, COUNT(*) \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1, 2"
        );
        let route = route_for(&state, &sql).await.expect("match count").expect("declared rollup route");
        let generated = generated_sql(&route);
        assert!(generated.contains("COALESCE(resource___service___name, 'null')"), "the coalesce must reach the rewrite verbatim: {generated}");
        assert_substitutes(&state, &sql, None).await;
    }

    /// The arm is deliberately narrow. A dimension the spec does not declare, a
    /// three-argument coalesce, and a coalesce over an EXPRESSION (monoscope's
    /// variant shape, `COALESCE(variant_to_json(resource)->…, 'null')`) all have
    /// to keep declining rather than routing to a column that is not there.
    #[test_case::test_case("COALESCE(status_message, 'null')" ; "a column that is not a declared dimension")]
    #[test_case::test_case("COALESCE(resource___service___name, name, 'null')" ; "three-argument coalesce")]
    #[test_case::test_case("COALESCE(CONCAT(resource___service___name, 'x'), 'null')" ; "coalesce over an expression, not a column")]
    #[tokio::test]
    async fn a_coalesce_the_matcher_cannot_prove_still_declines(group: &str) {
        let state = session().await;
        let sql = format!("SELECT {group} AS g, COUNT(*) FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1");
        assert!(matches!(route_for(&state, &sql).await, Err(_) | Ok(None)), "{group} must not route");
    }

    /// THE shape monoscope's charts actually emit — `extract(epoch from
    /// time_bucket(...))::integer` — which declined as `unsupported_shape` and
    /// sent every percentile and grouped panel to a raw scan. 2026-08-22 A/B on
    /// prod: 3,525 ms unrouted against 278 ms routed at 3 days.
    ///
    /// The wrapper must survive into the rewrite, cast and all: the rewrite is
    /// substituted for this aggregate and has to match its schema types.
    #[tokio::test]
    async fn a_chart_grouping_by_extract_epoch_of_a_bucket_routes() {
        let state = session().await;
        let sql = format!(
            "SELECT extract(epoch from time_bucket('1 hours', timestamp))::integer AS tb, COUNT(*) \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1"
        );
        let route = route_for(&state, &sql).await.expect("match count").expect("declared rollup route");
        let generated = generated_sql(&route);
        assert!(generated.contains("date_part('EPOCH', time_bucket('1 hours', timestamp))"), "the epoch wrapper must reach the rewrite: {generated}");
        assert!(generated.contains("CAST(date_part('EPOCH'"), "the integer cast must be reproduced or the schemas will not match: {generated}");
        assert_substitutes(&state, &sql, None).await;
    }

    /// `extract(epoch …)` is accepted because it is 1:1 over buckets. Every
    /// other field is many-to-one and would merge groups the raw path keeps
    /// apart, so it must keep declining rather than quietly returning a
    /// different answer.
    #[tokio::test]
    async fn extracting_any_field_but_epoch_from_a_bucket_still_declines() {
        let state = session().await;
        let sql = format!(
            "SELECT extract(hour from time_bucket('1 hours', timestamp))::integer AS tb, COUNT(*) \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1"
        );
        // Declining is the invariant; WHICH reason it declines with is not, so
        // this accepts a miss as readily as a `None` route.
        assert!(matches!(route_for(&state, &sql).await, Err(_) | Ok(None)), "a non-EPOCH field must not route");
    }

    #[tokio::test]
    async fn matcher_rewrites_a_certifiable_count_aggregate() {
        let state = session().await;
        let route = route_for(&state, &format!("SELECT COUNT(*) FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}"))
            .await
            .expect("match count")
            .expect("declared rollup route");
        assert_eq!(route.target, TARGET);
        assert_eq!(route.project_id.as_deref(), Some("project"));
        assert!(generated_sql(&route).contains("COALESCE(SUM(request_count), 0)"));
    }

    /// The single largest source of rollup misses in production: monoscope's
    /// cross-project overview, which GROUPS BY project_id instead of filtering
    /// on it. 2026-08-18 measured 2,870 of 2,948 misses (97%) as
    /// `missing_project`, and every sampled plan was this one shape — so it fell
    /// back to a raw scan of every project's rows roughly 24 times a minute.
    ///
    /// `project_id` is a real column on the rollup table (`to_rollup_batches`
    /// writes it), so grouping by it is answerable; only the demand for an
    /// equality literal stood in the way.
    #[tokio::test]
    async fn a_query_grouping_by_project_id_routes_without_an_equality_filter() {
        let state = session().await;
        let sql = format!("SELECT project_id, COUNT(*) FROM {SOURCE} WHERE {WINDOW} GROUP BY 1");
        let route = route_for(&state, &sql).await.expect("match count").expect("declared rollup route");
        assert_eq!(route.project_id, None, "no equality filter pins a project");
        let generated = generated_sql(&route);
        assert!(!generated.contains("WHERE project_id ="), "a cross-project rewrite must not pin one project: {generated}");
        assert!(generated.contains("SELECT project_id AS"), "project_id must survive as a group key: {generated}");
        // A generation id hashes the project, so the generation predicate must
        // name one per (project, date) rather than accepting any project's
        // generation for the day.
        assert!(generated.contains("(project_id = 'project' AND date = '1970-01-01'"), "generations must be qualified by project: {generated}");
        assert_substitutes(&state, &sql, None).await;
    }

    /// `date_trunc(unit, timestamp) = X` is a WINDOW, not a dimension filter.
    ///
    /// Treated as a residual it has no declared measure filter to match, so the
    /// whole query was refused as `filter_not_eligible`. Prod 2026-08-17 caught
    /// this the moment the cross-project fix landed: monoscope's overview query
    /// stopped saying `missing_project` at 23:23 and started saying
    /// `filter_not_eligible` at 23:40 — same query, one step further along, still
    /// a raw scan of every project.
    #[tokio::test]
    async fn a_date_trunc_equality_narrows_the_window_instead_of_being_promoted() {
        let state = session().await;
        // A day-wide window plus the hour the panel actually wants — the exact
        // shape prod sends.
        let hour = 3_600_000_000i64;
        let sql = format!(
            "SELECT project_id, COUNT(*) FROM {SOURCE} \
             WHERE timestamp >= to_timestamp_micros(0) AND timestamp < to_timestamp_micros(86400000000) \
             AND date_trunc('hour', timestamp) = to_timestamp_micros({}) GROUP BY 1",
            2 * hour
        );
        let route = route_for(&state, &sql).await.expect("match count").expect("declared rollup route");
        assert_eq!((route.lo, route.hi), (2 * hour, 3 * hour), "the window must narrow to the truncated hour");
        assert_substitutes(&state, &sql, None).await;
    }

    /// No timestamp truncates to an unaligned instant, so the predicate is
    /// unsatisfiable rather than a window. Inventing `[X, X+width)` for it would
    /// serve an hour of rows for a query that must return none.
    #[tokio::test]
    async fn a_date_trunc_equality_against_an_unaligned_literal_is_refused() {
        let state = session().await;
        let sql = format!(
            "SELECT project_id, COUNT(*) FROM {SOURCE} \
             WHERE timestamp >= to_timestamp_micros(0) AND timestamp < to_timestamp_micros(86400000000) \
             AND date_trunc('hour', timestamp) = to_timestamp_micros(90061000000) GROUP BY 1"
        );
        let miss = route_for(&state, &sql).await.expect_err("an unaligned truncation must be refused");
        assert!(matches!(miss, MissReason::UnknownFilter), "expected UnknownFilter, got {miss:?}");
    }

    /// One project short of coverage must not refuse the query for every other
    /// project. Prod 2026-08-18: 9 of 10 projects had coverage for the window and
    /// the tenth sent all ten to a raw scan of every row.
    ///
    /// The legs must PARTITION (project x time): covered projects read the rollup
    /// over the interior and raw over the fringes, uncovered projects read raw
    /// over the whole window. A gap drops rows and an overlap double counts them,
    /// and no downstream aggregate can detect either.
    #[tokio::test]
    async fn an_uncovered_project_reads_raw_while_the_others_still_route() {
        let state = session().await;
        let sql = format!("SELECT project_id, COUNT(*) FROM {SOURCE} WHERE {WINDOW} GROUP BY 1");
        let route = route_for(&state, &sql).await.expect("match").expect("route");
        let split = ProjectSplit { covered: Some(vec!["good".into(), "fine".into()]), raw_only: vec!["lagging".into()] };
        let generated = route.sql(&[("good".into(), "1970-01-01".into(), "generation".into())], &[(route.lo, route.hi)], &split);

        // The rollup leg answers only for the projects that proved coverage...
        assert!(
            generated.contains(&format!("FROM {TARGET} WHERE project_id IN ('good', 'fine')")),
            "rollup leg must be restricted to covered projects: {generated}"
        );
        // ...and the uncovered one is read raw across the WHOLE window, not dropped.
        assert!(generated.contains(&format!("FROM {SOURCE} WHERE project_id IN ('lagging')")), "the uncovered project must still be read, raw: {generated}");
        assert!(generated.contains("UNION ALL"), "the two must be unioned: {generated}");
        // The uncovered leg spans the full window, so no part of its data is lost
        // to an interior it was never in.
        assert!(generated.contains(&format!("timestamp >= to_timestamp_micros({})", route.lo)), "raw-only leg must start at the window start: {generated}");
    }

    /// With every project covered the rewrite must be byte-for-byte what it was
    /// before the split existed — no `IN` list, no extra leg. This is the case
    /// that runs constantly, and a needless predicate on it would be a permanent
    /// tax to fix a rare one.
    #[tokio::test]
    async fn an_all_covered_query_emits_exactly_the_unsplit_rewrite() {
        let state = session().await;
        let sql = format!("SELECT project_id, COUNT(*) FROM {SOURCE} WHERE {WINDOW} GROUP BY 1");
        let route = route_for(&state, &sql).await.expect("match").expect("route");
        let generations = [("p".to_string(), "1970-01-01".to_string(), "generation".to_string())];
        let unsplit = route.sql(&generations, &[(route.lo, route.hi)], &ProjectSplit::default());
        let all_covered = route.sql(&generations, &[(route.lo, route.hi)], &ProjectSplit { covered: None, raw_only: Vec::new() });
        assert_eq!(unsplit, all_covered);
        assert!(!unsplit.contains("project_id IN"), "no project list when every project is covered: {unsplit}");
    }

    /// Grouping by project_id is what makes the query answerable; merely
    /// omitting the filter does not. Without a group key the rewrite would
    /// silently fold every project into one row.
    #[tokio::test]
    async fn a_query_with_neither_a_project_filter_nor_a_project_group_is_refused() {
        let state = session().await;
        let miss = route_for(&state, &format!("SELECT COUNT(*) FROM {SOURCE} WHERE {WINDOW}")).await.expect_err("must be refused");
        assert!(matches!(miss, MissReason::MissingProject), "expected MissingProject, got {miss:?}");
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
        crate::read::functions::register_custom_functions(&mut ctx).expect("functions register");
        for table_name in ["otel_metrics", "otel_metrics_rollup_metrics_1m_v2"] {
            let table = datafusion::datasource::MemTable::try_new(crate::schema::get_schema(table_name).expect("schema").schema_ref(), vec![vec![]])
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
        assert_eq!(route.target, "otel_metrics_rollup_metrics_1m_v2");
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
            "otel_logs_and_spans_rollup_dashboard_1h_v2",
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

    /// The guard that stops the tier serving an aggregate built from a
    /// partition that has since moved. Prod 2026-08-22: a 3-day throughput
    /// chart returned 4.43M of 7.29M rows because nothing checked this.
    #[test_case::test_case(&[Some(100)], Some(100), true ; "single slice, partition unchanged")]
    #[test_case::test_case(&[Some(100), Some(100)], Some(100), true ; "every slice witnessed the same partition")]
    #[test_case::test_case(&[Some(100)], Some(150), false ; "rows arrived after the build: the 08-20 shape")]
    #[test_case::test_case(&[Some(100)], Some(90), false ; "rows REMOVED after the build — dedup and vacuum shrink num_records, so the check is two-sided")]
    #[test_case::test_case(&[Some(100), Some(150)], Some(150), false ; "slices disagree with each other: one was built before a change")]
    #[test_case::test_case(&[None], Some(100), false ; "a slice written before the tag cannot be verified, so it is refused")]
    #[test_case::test_case(&[Some(100), None], Some(100), false ; "one unverifiable slice condemns the date")]
    #[test_case::test_case(&[], Some(100), false ; "no slices cover the date")]
    #[test_case::test_case(&[Some(100)], None, false ; "the source partition reports no row count")]
    fn slice_coverage_is_trusted_only_when_every_witness_matches_the_partition_now(witnesses: &[Option<u64>], current: Option<u64>, trusted: bool) {
        assert_eq!(slice_coverage_agrees(witnesses, current), trusted);
    }

    #[test]
    fn hybrid_branch_count_includes_rollup_and_raw_ranges() {
        let ranges = vec![(10, 20), (30, 40), (50, 60)];
        assert_eq!(hybrid_branch_count(0, 70, &ranges), 7);
        assert_eq!(hybrid_branch_count(10, 60, &ranges), 5);
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
    ///
    /// `status_message` appears in no declared measure filter, so this is the
    /// *not eligible* half of the split: nothing could ever have answered it.
    #[tokio::test]
    async fn a_residual_filter_with_no_declared_measure_still_refuses() {
        let state = session().await;
        let sql = format!(
            "SELECT COUNT(*) AS c FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} AND status_message = 'nope' \
             GROUP BY time_bucket('1 hours', timestamp)"
        );
        assert_eq!(route_for(&state, &sql).await.err(), Some(MissReason::FilterNotEligible), "an unmatched residual must not be promoted");
    }

    /// The other half of the split, and the reason it exists: a residual that
    /// constrains a column a declared measure DOES filter on is a near-miss
    /// worth a warning, because it is the shape a real dashboard panel takes.
    ///
    /// Prod 2026-08-12 read `unknown_filter = 20` and it was 100% facet and
    /// log-explorer traffic (`attributes___…___name IS NOT NULL`,
    /// `jsonb_path_exists`, `LIKE`) — none of it a panel. Folding both cases
    /// into one counter made it impossible to tell "we should have matched this"
    /// from "this was never a candidate", so the number could not be acted on.
    /// These two tests are what stop them collapsing back together.
    #[tokio::test]
    async fn a_near_miss_on_a_declared_column_is_distinguished_from_an_ineligible_one() {
        let state = session().await;
        // The shape a drifted panel takes: the declared server filter plus one
        // extra conjunct. It cannot match, but it talks about `kind`/`name`,
        // which declared measures do filter on — so it is worth a warning.
        let sql = format!(
            "SELECT COUNT(*) AS c FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} \
                    AND (kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http') AND status_message = 'nope' \
             GROUP BY time_bucket('1 hours', timestamp)"
        );
        assert_eq!(
            route_for(&state, &sql).await.err(),
            Some(MissReason::UnknownFilter),
            "a residual on a column declared measures filter on is a near-miss, not an ineligible query"
        );
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

    #[test]
    fn cohort_batch_shaping_preserves_project_generations() {
        let spec = spec();
        let mut fields = vec![
            Arc::new(Field::new("project_id", DataType::Utf8, false)),
            Arc::new(Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false)),
        ];
        fields.extend(spec.dimensions.iter().map(|name| Arc::new(Field::new(name, DataType::Utf8, true))));
        fields.extend(spec.measures.iter().map(|measure| {
            Arc::new(Field::new(&measure.name, if measure.agg == "tdigest" { DataType::Binary } else { DataType::Int64 }, measure.agg != "count"))
        }));
        let mut columns: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some("project-b"), Some("project-a")])),
            Arc::new(TimestampMicrosecondArray::from(vec![2_000_000, 1_000_000]).with_timezone("UTC")),
        ];
        columns.extend(spec.dimensions.iter().map(|_| Arc::new(StringArray::from(vec![Some("b"), Some("a")])) as arrow::array::ArrayRef));
        columns.extend(spec.measures.iter().map(|measure| {
            if measure.agg == "tdigest" {
                Arc::new(BinaryArray::from(vec![Some(&[2_u8][..]), Some(&[1_u8][..])])) as arrow::array::ArrayRef
            } else {
                Arc::new(Int64Array::from(vec![Some(2), Some(1)])) as arrow::array::ArrayRef
            }
        }));
        let input = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("cohort aggregate batch");
        let generations =
            std::collections::HashMap::from([("project-a".to_string(), "generation-a".to_string()), ("project-b".to_string(), "generation-b".to_string())]);
        let output = to_rollup_batches_by_project(&spec, SOURCE, "1970-01-01", &generations, &[input]).expect("shape cohort");
        for (project, generation) in [("project-a", "generation-a"), ("project-b", "generation-b")] {
            let batch = &output[project][0];
            let project_ids = batch.column_by_name("project_id").expect("project id").as_any().downcast_ref::<StringViewArray>().expect("utf8 project id");
            let generations =
                batch.column_by_name("rollup_generation").expect("generation").as_any().downcast_ref::<StringViewArray>().expect("utf8 generation");
            assert_eq!(project_ids.value(0), project);
            assert_eq!(generations.value(0), generation);
        }
    }
}
