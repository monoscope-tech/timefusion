//! Schema-driven dashboard rollups: the aggregate SQL that builds them, the
//! conversion to the generated target schema, and read routing.

use crate::schema::{RollupMeasure, RollupSpec};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// Why a query cannot use a rollup. Variant names ARE the `rollup_misses`
/// telemetry labels (snake_case); the `serialize` overrides pin existing labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumIter, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum MissReason {
    UnsupportedShape,
    MissingProject,
    UnboundedTime,
    UnknownGroupBy,
    UnknownFilter,
    /// The residual row filter constrains columns no declared measure filters on.
    FilterNotEligible,
    /// NULL-guards on two different columns; a single count measure cannot express both.
    #[strum(serialize = "filter_multiple_null_guards")]
    FilterMultipleNullGuards,
    /// The measure's column and the query's NULL-guard column disagree, so the
    /// stored measure counts a different population.
    #[strum(serialize = "filter_null_guard_mismatch")]
    FilterNullGuardMismatch,
    MissingMeasure,
    #[strum(serialize = "non_decomposable")]
    NonDecomposableAggregate,
    /// A `time_bucket` width that is not a multiple of the grain.
    #[strum(serialize = "unaligned_bucket_width")]
    PartialBucket,
    /// No rollup was ever built for a date in the window.
    NotBuilt,
    /// A rollup exists for the date but the source has moved under it.
    StaleCoverage,
    /// Coverage cannot be established at all: buffered rows, or a window whose
    /// dates cannot be enumerated.
    IncompleteCoverage,
    /// The certified interior is too small to be worth the union's second scan.
    TinyInterior,
    /// Hybrid routing would create too many disjoint raw/rollup predicates.
    TooManyBranches,
    RewriteSchemaMismatch,
    /// An aggregate over a rollup-bearing table whose plan the matcher could not
    /// walk from the aggregate down to the scan (a node `source_and_filters`
    /// refuses). Counted only for tables that actually declare rollups.
    UnwalkableSource,
    /// The cell is fresh, but its FILES do not carry a measure the query needs.
    /// Distinct from `StaleCoverage`: nothing moved, the build never wrote the column.
    MeasureNotStored,
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
/// Excludes the source fingerprint: independently replaceable slices of one date
/// must share a generation so a query can merge them. `measures` restricts the
/// spec to what the cell actually materialized before hashing (`None` = whole spec).
pub fn generation_id(spec: &RollupSpec, source: &str, project_id: &str, date: &str, _source_fp: u64, measures: Option<&[String]>) -> String {
    let restricted = measures.map(|names| RollupSpec { measures: spec.measures.iter().filter(|m| names.contains(&m.name)).cloned().collect(), ..spec.clone() });
    let spec = restricted.as_ref().unwrap_or(spec);
    let mut hasher = fnv::FnvHasher::default();
    // Bump when source-read semantics change, not just the SQL spec; forces a rebuild.
    const MATERIALIZATION_VERSION: u8 = 1;
    MATERIALIZATION_VERSION.hash(&mut hasher);
    format!("{spec:?}").hash(&mut hasher);
    (source, project_id, date).hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// SQL that builds one source `(project_id, date)` partition. Measure filters
/// go on each aggregate, never in the row `WHERE` — moving them would make
/// unrelated measures observe the wrong rows.
pub fn build_partition_sql(spec: &RollupSpec, source: &str, project_id: &str, date: &str) -> anyhow::Result<String> {
    build_partition_sql_from(spec, source, source, project_id, date)
}

/// One bit per hour of a UTC day. `ALL_HOURS` is the conservative value: every
/// invalidation means it unless the caller can prove a narrower set.
pub(crate) const ALL_HOURS: u32 = (1 << 24) - 1;

/// The inclusive timestamp bounds a file's Delta statistics claim.
///
/// Writers spell them either as epoch micros or RFC 3339, so both are accepted.
/// `None` means the file makes no claim — never treat that as an empty range.
pub(crate) fn stats_time_range(stats: &str) -> Option<(i64, i64)> {
    let value: serde_json::Value = serde_json::from_str(stats).ok()?;
    let parse_ts = |side: &str| -> Option<i64> {
        let v = value.get(side)?.get("timestamp")?;
        v.as_i64().or_else(|| chrono::DateTime::parse_from_rfc3339(v.as_str()?).ok().map(|t| t.timestamp_micros()))
    };
    Some((parse_ts("minValues")?, parse_ts("maxValues")?))
}

/// The hours of a partition-day a committed file can hold rows for, from its
/// Delta stats JSON. `None` when bounds are absent or fall entirely outside the
/// day — callers must fall back to `ALL_HOURS`, never to skipping work.
pub(crate) fn hours_from_stats_json(stats: &str, day_start_micros: i64) -> Option<u32> {
    let (lo, hi) = stats_time_range(stats)?;
    let (lo_h, hi_h) = ((lo - day_start_micros).div_euclid(HOUR_MICROS), (hi - day_start_micros).div_euclid(HOUR_MICROS));
    if hi_h < 0 || lo_h >= 24 || lo_h > hi_h {
        return None;
    }
    let mask = (lo_h.clamp(0, 23)..=hi_h.clamp(0, 23)).fold(0u32, |mask, hour| mask | 1 << hour);
    (mask != 0).then_some(mask)
}

const HOUR_MICROS: i64 = 3_600_000_000;

/// The `[start, end)` ranges `hours` marks on the day beginning at `day_start`,
/// with adjacent hours merged so a contiguous span costs one predicate.
pub(crate) fn dirty_ranges(day_start: i64, hours: u32) -> Vec<(i64, i64)> {
    crate::write::mem_buffer::merge_ranges(
        (0..24).filter(|hour| hours & (1 << hour) != 0).map(|hour| (day_start + hour * HOUR_MICROS, day_start + (hour + 1) * HOUR_MICROS)).collect(),
    )
}

/// `build_partition_sql`, but reading `from` instead of the raw source.
///
/// When `from` is a finer rollup the measures are re-aggregated as STATES, and
/// each measure's declared `filter` is NOT re-applied: the base row already had
/// it applied, and the filter's columns do not exist on the base table.
pub fn build_partition_sql_from(spec: &RollupSpec, source: &str, from: &str, project_id: &str, date: &str) -> anyhow::Result<String> {
    build_partition_sql_ranges(spec, source, from, "", project_id, date, &[])
}

/// The rollup PARTIAL for one batch of rows, aggregated in memory.
///
/// Uses the SAME SQL the rebuild does (`build_cohort_sql_range_mode`); a second
/// spelling of the aggregate is how a rollup silently disagrees with itself.
///
/// Computes a partial only — no commit, no invalidation, no identity.
/// **Appending one is NOT safe on its own**: a batch aggregated twice
/// over-counts permanently and `min`/`max` cannot be retracted. Any caller that
/// appends these must carry a batch identity and skip an already-landed one.
pub async fn rollup_partial_for_batches(
    ctx: &datafusion::prelude::SessionContext, spec: &RollupSpec, source: &str, project_id: &str, date: &str, batches: &[arrow::record_batch::RecordBatch],
    window: (i64, i64),
) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
    let Some(first) = batches.first() else { return Ok(Vec::new()) };
    let input = format!("__partial_input_{}", uuid::Uuid::new_v4().simple());
    ctx.register_table(input.as_str(), std::sync::Arc::new(datafusion::datasource::MemTable::try_new(first.schema(), vec![batches.to_vec()])?))?;
    let sql = build_cohort_sql_range_mode(spec, source, &input, std::slice::from_ref(&project_id.to_string()), date, window, false)?;
    let partial = ctx.sql(&sql).await?.collect().await?;
    let _ = ctx.deregister_table(input.as_str());
    Ok(partial)
}

/// One measure's projection in a rollup build, as `<expression> AS <name>`.
///
/// `derived` selects the tier-to-tier merge over the build-from-raw aggregate.
/// A derived merge applies no filter: the base tier already applied it, and
/// applying it again over an aggregated column is a different predicate.
fn measure_projection(spec: &RollupSpec, measure: &RollupMeasure, derived: bool) -> anyhow::Result<String> {
    let expression = match (derived, measure.agg.as_str(), measure.column.as_deref()) {
        (true, aggregate @ ("min" | "max"), _) => format!("{}({})", aggregate.to_uppercase(), measure.name),
        (true, aggregate @ ("tdigest" | "hll"), _) => format!("{aggregate}_merge(CAST({} AS BYTEA))", measure.name),
        // Order by the COMPANION, never the base tier's bucket timestamp: a bucket
        // whose filter matched nothing stores NULL for both, and `NULLS LAST` makes
        // it win only when nothing else matched.
        (true, "first", _) => format!(
            "first_value({} ORDER BY {} NULLS LAST)",
            measure.name,
            spec.first_companion(measure).ok_or_else(|| anyhow::anyhow!("`first` measure `{}` has no companion min(timestamp) measure", measure.name))?.name
        ),
        (true, _, _) => format!("SUM({})", measure.name),
        (false, "count", None) => "COUNT(*)".to_string(),
        (false, "count", Some(column)) => format!("COUNT({column})"),
        (false, "tdigest", Some(column)) => format!("percentile_agg(CAST({column} AS DOUBLE))"),
        (false, "hll", Some(column)) => format!("hll_agg({column})"),
        // The FILTER appended below applies to value and companion alike, so both
        // describe the SAME row.
        (false, "first", Some(column)) => format!("first_value({column} ORDER BY timestamp)"),
        (false, aggregate, Some(column)) => format!("{}({column})", aggregate.to_uppercase()),
        (false, aggregate, None) => return Err(anyhow::anyhow!("{} measure `{}` needs a source column", aggregate, measure.name)),
    };
    let filter = if derived { None } else { measure.filter.as_deref() };
    Ok(format!("{} AS {}", filtered(expression, filter), measure.name))
}

/// `<expression> FILTER (WHERE …)`, or the bare expression. The build side, the
/// raw leg and the `HAVING` guard must all render a filter identically.
fn filtered(expression: String, filter: Option<&str>) -> String {
    if let Some(filter) = filter { format!("{expression} FILTER (WHERE {filter})") } else { expression }
}

/// The bucketed `timestamp`, dimensions and measures a rollup build selects,
/// plus the `, dim…` fragment its carried-forward leg reuses. The only spelling
/// of the bucket-floor formula.
fn bucketed_projection(spec: &RollupSpec, derived: bool) -> anyhow::Result<(String, String)> {
    let grain = spec.grain_micros().ok_or_else(|| anyhow::anyhow!("invalid rollup grain `{}`", spec.grain))?;
    let dimensions = spec.dimensions.join(", ");
    let select_dimensions = if dimensions.is_empty() { String::new() } else { format!(", {dimensions}") };
    let measures = spec.measures.iter().map(|measure| measure_projection(spec, measure, derived)).collect::<anyhow::Result<Vec<_>>>()?.join(", ");
    let projection = format!(
        "to_timestamp_micros(CAST(FLOOR(EXTRACT(EPOCH FROM timestamp) * 1000000 / {grain}) AS BIGINT) * {grain}) AS timestamp{select_dimensions}, {measures}"
    );
    Ok((select_dimensions, projection))
}

/// The partition's rows, rebuilt over `ranges` only and carried forward verbatim
/// from `target` everywhere else. Empty `ranges` means the whole day, from scratch.
pub(crate) fn build_partition_sql_ranges(
    spec: &RollupSpec, source: &str, from: &str, target: &str, project_id: &str, date: &str, ranges: &[(i64, i64)],
) -> anyhow::Result<String> {
    let (select_dimensions, projection) = bucketed_projection(spec, from != source)?;
    let source = from;
    let group_by = (1..=1 + spec.dimensions.len()).join(", ");

    let partition = format!("project_id = {} AND date = {}", sql_literal(project_id), sql_literal(date));
    let rebuilt = format!("SELECT {projection} FROM {source} WHERE {partition}");
    if ranges.is_empty() {
        return Ok(format!("{rebuilt} GROUP BY {group_by}"));
    }
    // Half-open bounds, and the SAME range list drives both legs: the rebuilt and
    // carried-forward hours must partition the day exactly or a bucket is double
    // counted or dropped.
    let dirty =
        ranges.iter().map(|(start, end)| format!("(timestamp >= to_timestamp_micros({start}) AND timestamp < to_timestamp_micros({end}))")).join(" OR ");
    let carried = spec.measures.iter().map(|measure| &measure.name).join(", ");
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
    let (_, projection) = bucketed_projection(spec, derived)?;
    let group_by = (1..=2 + spec.dimensions.len()).join(", ");
    let projects = project_ids.iter().map(|project| sql_literal(project)).join(", ");
    Ok(format!(
        "SELECT project_id, {projection} \
         FROM {from} WHERE project_id IN ({projects}) AND date = {} AND timestamp >= to_timestamp_micros({start}) AND timestamp < to_timestamp_micros({end}) \
         GROUP BY {group_by}",
        sql_literal(date)
    ))
}

/// FROZEN HASH: this IS the persisted bucket id in rollup slices, so changing
/// the hasher orphans every rollup ever written.
fn generated_bucket_id(bucket: i64, grain: i64, generation: &str, dimensions: &[datafusion::scalar::ScalarValue]) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    (bucket, grain, generation).hash(&mut hasher);
    dimensions.len().hash(&mut hasher);
    dimensions.iter().for_each(|dimension| format!("{dimension:?}").hash(&mut hasher));
    format!("{bucket}-{:016x}", hasher.finish())
}

/// Convert aggregate batches into rows for the generated rollup schema.
///
/// The aggregate output carries `timestamp`, the configured dimensions and the
/// measures; remaining target fields are identity or partition fields. Arrays
/// are cast only at the target boundary so digest state and non-string
/// dimensions keep their types.
pub fn to_rollup_batches(
    spec: &RollupSpec, source: &str, project_id: &str, date: &str, generation: &str, aggregated: &[arrow::record_batch::RecordBatch],
) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
    use arrow::{
        array::{Array, ArrayRef, BooleanArray, Date32Array, StringArray, TimestampMicrosecondArray},
        compute::kernels::cast::cast,
        datatypes::DataType,
    };
    use datafusion::{common::Result as DFResult, scalar::ScalarValue};
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
            let timestamps =
                timestamp.iter().map(|value| value.ok_or_else(|| anyhow::anyhow!("rollup aggregate timestamp is null"))).collect::<anyhow::Result<Vec<_>>>()?;
            let dimension_columns = spec
                .dimensions
                .iter()
                .map(|name| batch.column_by_name(name).ok_or_else(|| anyhow::anyhow!("rollup aggregate is missing dimension `{name}`")))
                .collect::<anyhow::Result<Vec<&ArrayRef>>>()?;
            let ids = (0..rows)
                .map(|row| {
                    let values = dimension_columns.iter().map(|column| ScalarValue::try_from_array(column, row)).collect::<DFResult<Vec<_>>>()?;
                    Ok(generated_bucket_id(timestamps[row], grain, generation, &values))
                })
                .collect::<DFResult<Vec<_>>>()?;

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
    spec: &RollupSpec, source: &str, date: &str, generations: &HashMap<String, String>, aggregated: &[arrow::record_batch::RecordBatch],
) -> anyhow::Result<HashMap<String, Vec<arrow::record_batch::RecordBatch>>> {
    use arrow::{
        array::{Array, StringArray},
        compute::cast,
        datatypes::DataType,
    };

    let mut grouped: HashMap<String, Vec<arrow::record_batch::RecordBatch>> = HashMap::new();
    for batch in aggregated.iter().filter(|batch| batch.num_rows() > 0) {
        let projects = batch.column_by_name("project_id").ok_or_else(|| anyhow::anyhow!("cohort aggregate is missing project_id"))?;
        let projects = cast(projects, &DataType::Utf8)?;
        let projects = projects.as_any().downcast_ref::<StringArray>().ok_or_else(|| anyhow::anyhow!("cohort project_id cannot cast to Utf8"))?;
        let rows_by_project: HashMap<&str, Vec<u32>> = (0..batch.num_rows())
            .map(|row| match projects.is_null(row) {
                true => Err(anyhow::anyhow!("cohort aggregate project_id is null")),
                false => Ok((projects.value(row), u32::try_from(row)?)),
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .into_group_map();
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
/// Serves both shapes: over measure columns when the rollup answers alone, and
/// over the union's state aliases when a raw leg is present. Sound only because
/// every variant is associative over a *partition* of the row set, which
/// [`interior`] guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Merge {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    TDigest,
    /// A distinct-count sketch. Like `TDigest`, the query's output IS the folded
    /// state; `distinct_count` reads the number out of it above the aggregate.
    Hll,
    /// Earliest value in the window. Two states — value and the timestamp of the
    /// row it came from — since "earliest" is not recoverable from the value alone.
    First,
}

impl Merge {
    /// State columns consumed, in order. `Avg` carries sum and count apart —
    /// an average is not a state, and the union would average two averages.
    const fn arity(self) -> usize {
        if matches!(self, Self::Avg | Self::First) { 2 } else { 1 }
    }

    /// The associative operator that folds one state column across legs.
    const fn partial_op(self) -> &'static str {
        match self {
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::TDigest => "tdigest_merge",
            Self::Hll => "hll_merge",
            // Exhaustive on purpose: a new state-carrying variant must name its
            // operator rather than silently folding with SUM.
            Self::Count | Self::Sum | Self::Avg | Self::First => "SUM",
        }
    }

    /// One partial-state expression per stored column, for the rollup leg.
    /// `First` is the exception: the value is picked BY the companion timestamp,
    /// and the companion is minimised.
    fn partial_states(self, columns: &[String]) -> Vec<String> {
        match (self, columns) {
            (Self::First, [value, at]) => vec![format!("first_value({value} ORDER BY {at} NULLS LAST)"), format!("MIN({at})")],
            // Same `arity()` contract `sql` enforces: folding a mis-arity First into SUM would
            // silently produce a different aggregate.
            (Self::First, _) => unreachable!("first needs (value, companion), got {}", columns.len()),
            _ => columns.iter().map(|column| format!("{}({column})", self.partial_op())).collect(),
        }
    }

    /// Combine `states` into the query's output value.
    fn sql(self, states: &[String]) -> String {
        match (self, states) {
            (Self::Count, [count]) => format!("COALESCE(SUM({count}), 0)"),
            // CAST the dividend, not the result: both states are Int64, so
            // dividing first truncates.
            (Self::Avg, [sum, count]) => {
                format!("CASE WHEN COALESCE(SUM({count}), 0) = 0 THEN CAST(NULL AS DOUBLE) ELSE CAST(SUM({sum}) AS DOUBLE) / CAST(SUM({count}) AS DOUBLE) END")
            }
            // Single-state variants fold with the same operator the rollup leg used.
            (Self::Sum | Self::Min | Self::Max | Self::TDigest | Self::Hll, [state]) => format!("{}({state})", self.partial_op()),
            // `NULLS LAST`: a leg that matched nothing contributes a NULL pair
            // and must lose to any leg that matched something.
            (Self::First, [value, at]) => format!("first_value({value} ORDER BY {at} NULLS LAST)"),
            // `arity()` fixes the state count per variant.
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
    /// Raw-leg aggregate SQL, one per state, in `merge` order.
    raw: Vec<String>,
}

/// A sliver of certified interior is worse than the raw plan it replaces: the
/// union costs a second scan, a second aggregation and a barrier.
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
/// endpoints snap to a grain boundary: a rollup row is indivisible, so off-grain
/// the legs double count a bucket or drop one, undetectably.
#[cfg(test)]
pub(crate) fn interior(lo: i64, hi: i64, grain: i64, horizon: i64) -> Option<(i64, i64)> {
    interiors(lo, hi, grain, horizon, &[(lo, hi)]).into_iter().next()
}

/// Every grain-aligned range the rollup may own inside `[lo, hi)`.
///
/// `covered` is the set of ranges whose coverage was proved current, ascending;
/// `horizon` caps them all, because a row still in the MemBuffer is missing from
/// EVERY rollup partition regardless of which dates are certified. Taking a set
/// rather than a prefix keeps one stale day from discarding every day after it.
pub(crate) fn interiors(lo: i64, hi: i64, grain: i64, horizon: i64, covered: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let capped = hi.min(horizon);
    let ranges: Vec<(i64, i64)> = covered
        .iter()
        .filter_map(|(start, end)| {
            let (start, end) = (ceil_grain((*start).max(lo), grain), floor_grain((*end).min(capped), grain));
            debug_assert!(start.rem_euclid(grain) == 0 && end.rem_euclid(grain) == 0, "interior endpoints must be grain-aligned");
            // A run too short to hold whole buckets goes back to the raw leg.
            (end.saturating_sub(start) >= MIN_INTERIOR_BUCKETS.saturating_mul(grain)).then_some((start, end))
        })
        .collect();
    // The floor applies to the TOTAL, not to each run, and is measured against
    // the window the QUERY asked for, never the horizon-capped one.
    let total: i64 = ranges.iter().map(|(start, end)| end - start).sum();
    if total < hi.saturating_sub(lo) / MIN_INTERIOR_FRACTION { Vec::new() } else { ranges }
}

/// The upper bound of a window the query left open. `i64::MAX` so it compares
/// above every real timestamp and `complement` needs no special case; `range_sql`
/// renders that one range without an upper bound.
pub(crate) const OPEN_END: i64 = i64::MAX;

/// The `num_records` sum over only the files lying wholly below `bound` — the
/// read half of `TAG_SOURCE_ROWS_BELOW`.
///
/// THE RULE MUST MATCH `partition_stats_bounded` EXACTLY: a file is excluded iff
/// its max timestamp is KNOWN and reaches the bound. A file with no statistics is
/// therefore counted on BOTH sides, and the two still agree; a straddler is
/// excluded wholesale on both sides for the same reason. Symmetry is the whole
/// soundness argument, so any drift between the two rules is a correctness bug,
/// not a tuning one.
pub(crate) fn rows_below(files: &[(Option<i64>, i64)], bound: i64) -> Option<u64> {
    u64::try_from(files.iter().filter(|(max_ts, _)| !max_ts.is_some_and(|hi| hi >= bound)).map(|(_, rows)| rows).sum::<i64>()).ok()
}

/// May a date's slice coverage be read from the tier at all?
///
/// `witnesses` is each covering slice's record of how many rows the DATE
/// partition held when it was built; `current` is how many it holds now. Both
/// must be `PartitionStats::rows` (the add-action `num_records` sum). Witnesses
/// are never SUMMED — each is a snapshot of the WHOLE date — and equality is
/// two-sided. A slice with no witness is refused.
pub(crate) fn slice_coverage_agrees(witnesses: &[Option<u64>], current: Option<u64>) -> bool {
    let files = current.map(|rows| [SourceFile { min_ts: None, max_ts: None, rows }]);
    let source = LiveSource { files: files.as_ref().map(|files| &files[..]), logical: None };
    !witnesses.is_empty() && witnesses.iter().all(|witness| verify_slice_witness(witness.map(SliceWitness::Physical), source) == WitnessVerdict::Valid)
}

/// One live file of a rollup SOURCE partition, as the Delta add-actions describe
/// it — not [`LiveFile`], which is a file of the tier the source feeds.
///
/// `min_ts`/`max_ts` are `None` when the file carries no timestamp statistic.
/// That is not "spans nothing": it cannot be placed relative to a slice bound,
/// so a bounded witness must refuse rather than guess.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SourceFile {
    pub min_ts: Option<i64>,
    /// INCLUSIVE, like a Delta file statistic and unlike `TimeSlice::end`.
    pub max_ts: Option<i64>,
    /// The add action's `num_records` — PHYSICAL, so it counts tombstones and
    /// superseded merge-on-read versions.
    pub rows: u64,
}

/// What a rollup slice recorded about its source partition at build time.
///
/// Versioned because the shapes are not comparable: a witness recorded under one
/// variant can only ever be re-proved under the same variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SliceWitness {
    /// v1, what `TAG_SOURCE_ROWS` holds: the WHOLE partition's `num_records` sum,
    /// unbounded — any ingest anywhere in the day moves it.
    Physical(u64),
    /// v2: `num_records` summed over the live files lying WHOLLY BELOW the
    /// slice's `covered_through`, so ingest past the bound does not perturb it.
    #[allow(dead_code)]
    PhysicalBelow { rows: u64, bound: i64 },
    /// v3: the exact LOGICAL row count of `[lo, hi)` — deduped and
    /// tombstone-aware. Invariant under rewrites that preserve logical content.
    #[allow(dead_code)]
    Logical { rows: u64, lo: i64, hi: i64 },
}

/// The live facts a witness may be re-proved against.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LiveSource<'a> {
    /// The source partition's live files. `None` when the Delta log could not be
    /// read, which is an absence of evidence, never a disagreement.
    pub files: Option<&'a [SourceFile]>,
    /// `(lo, hi, rows)` — an exact logical row count and the range it was taken
    /// over, when a logical-count index is resident and current for this
    /// partition. `None` when there is no index to ask.
    pub logical: Option<(i64, i64, u64)>,
}

/// Three-valued on purpose. `Stale` is a proven disagreement; `Unverifiable` is
/// an absence of evidence. Both keep the range off the tier, but they demand
/// opposite work: `Stale` needs a rebuild, `Unverifiable` needs the evidence to
/// exist.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WitnessVerdict {
    Valid,
    Stale,
    Unverifiable,
}

/// Re-prove one slice's witness against the source partition as it stands now.
///
/// Straddle rule, the soundness argument for `PhysicalBelow`: Delta statistics
/// are per FILE, so a file with rows on both sides of the bound cannot be split
/// by arithmetic and poisons the whole witness.
pub(crate) fn verify_slice_witness(witness: Option<SliceWitness>, source: LiveSource<'_>) -> WitnessVerdict {
    let verdict = |expected: u64, actual: u64| if expected == actual { WitnessVerdict::Valid } else { WitnessVerdict::Stale };
    let (Some(witness), Some(files)) = (witness, source.files) else { return WitnessVerdict::Unverifiable };
    match witness {
        SliceWitness::Physical(rows) => verdict(rows, files.iter().fold(0, |sum, file| sum.saturating_add(file.rows))),
        SliceWitness::PhysicalBelow { rows, bound } => files
            .iter()
            .try_fold(0u64, |below, file| match (file.min_ts, file.max_ts) {
                (Some(min_ts), Some(max_ts)) if !(min_ts < bound && max_ts >= bound) => {
                    Ok(if max_ts < bound { below.saturating_add(file.rows) } else { below })
                }
                _ => Err(WitnessVerdict::Unverifiable),
            })
            .map_or_else(std::convert::identity, |below| verdict(rows, below)),
        // The range must match: a count over a DIFFERENT window is not evidence.
        SliceWitness::Logical { rows, lo, hi } => match source.logical {
            Some((live_lo, live_hi, live)) if (live_lo, live_hi) == (lo, hi) => verdict(rows, live),
            _ => WitnessVerdict::Unverifiable,
        },
    }
}

/// `[lo, hi)` minus `ranges` — the raw leg's share.
///
/// `ranges` must be ascending and disjoint (`interiors` guarantees it). Bounds
/// are half-open and come from the SAME list that drives the rollup leg, so the
/// two partition `[lo, hi)` exactly.
pub(crate) fn complement(lo: i64, hi: i64, ranges: &[(i64, i64)]) -> Vec<(i64, i64)> {
    // The `(hi, hi)` sentinel emits the trailing gap.
    ranges
        .iter()
        .chain(std::iter::once(&(hi, hi)))
        .scan(lo, |cursor, &(start, end)| {
            let gap = (*cursor < start).then_some((*cursor, start));
            *cursor = (*cursor).max(end);
            Some(gap)
        })
        .flatten()
        .collect()
}

/// [`complement`] for a coverage set that is neither sorted nor disjoint —
/// slice coverage arrives in hash order and its ranges overlap freely.
pub(crate) fn uncovered(lo: i64, hi: i64, mut ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    ranges.sort_unstable();
    complement(lo, hi, &ranges)
}

pub(crate) fn hybrid_branch_count(lo: i64, hi: i64, ranges: &[(i64, i64)]) -> usize {
    ranges.len().saturating_add(complement(lo, hi, ranges).len())
}

/// Which projects the ROLLUP leg may answer for, and which must be read raw
/// across the whole window because their coverage was not proved. Splitting
/// rather than intersecting keeps one uncovered project from sending every
/// other project to a raw scan.
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
    /// project's rollup rows, so coverage must hold for every project with
    /// source data in the window.
    pub project_id: Option<String>,
    pub lo: i64,
    pub hi: i64,
    /// The query gave no upper bound and `hi` is a plan-time stand-in. The
    /// interior may use it; the trailing raw range may NOT, or the rewrite drops
    /// every row after it — the newest rows.
    open_end: bool,
    pub grain: i64,
    pub target: String,
    /// The `Aggregate` node this route replaces, verbatim; the caller substitutes
    /// the rewrite for exactly this node and leaves nodes above it alone.
    pub matched: datafusion::logical_expr::LogicalPlan,
    /// A `COUNT` over the promoted row filter, carried through the legs but NOT
    /// selected: it only powers the `HAVING` that reproduces group elimination.
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
/// Partitions really do hold several versions of one `id`; the query path
/// collapses them via `DedupExec`. A maintenance read registers the tier
/// directly rather than through the routing table, so it must spell the same
/// identity out.
pub(crate) fn rollup_tier_dedup(schema: &crate::schema::TableSchema) -> Option<(Vec<String>, &'static str, Option<&str>)> {
    let has = |name: &str| schema.fields.iter().any(|field| field.name == name);
    (has("timestamp") && has("id") && has("updated_at"))
        .then(|| (vec!["timestamp".to_owned(), "id".to_owned()], "updated_at", schema.tombstone_column.as_deref()))
}

/// A live tier file, as the publish path's replace-set sees it. `slice` is
/// `None` for a file carrying no `timefusion.slice_*` tags — e.g. one a
/// delta-rs OPTIMIZE rewrote, which keeps only its own `sort_by` tag.
pub(crate) struct LiveFile<'a> {
    pub slice: Option<(i64, i64)>,
    pub project: Option<&'a str>,
    pub partition: Option<(&'a str, &'a str)>,
    /// Inclusive timestamp bounds from the file's own Delta statistics, which
    /// survive tag loss.
    pub stats: Option<(i64, i64)>,
}

/// The publication a replace-set is being computed for.
pub(crate) struct SlicePublish<'a> {
    pub project_id: &'a str,
    pub date: &'a str,
    pub slice: (i64, i64),
    pub rows: u64,
    /// Tagged slice ranges LIVE in this partition once this commit lands, this
    /// slice included. Their union proves an untagged file redundant when no
    /// single slice contains it.
    pub covered: &'a [(i64, i64)],
}

/// Whether this publication retires `file`.
///
/// A TAGGED file is retired when this slice CONTAINS it (containment, not
/// equality). An UNTAGGED file is retired only when this partition provably
/// reproduces it, by any of three proofs, cheapest first:
///
/// - the slice spans a WHOLE partition day;
/// - the file's own statistics place it inside this slice;
/// - the union of live tagged slices covers the file's statistics range — the
///   only proof that reaches a day split into slices none of which contains it.
///
/// All three require `rows > 0` (an empty rebuild means the untagged file may be
/// the only copy) and the file to be in this very partition.
pub(crate) fn slice_retires(file: &LiveFile<'_>, publish: &SlicePublish<'_>) -> bool {
    let (start, end) = publish.slice;
    match file.slice {
        Some((file_start, file_end)) => file.project == Some(publish.project_id) && file_start >= start && file_end <= end,
        None => {
            publish.rows > 0
                && file.partition == Some((publish.project_id, publish.date))
                && (spans_whole_day(start, end) || file.stats.is_some_and(|(lo, hi)| (lo >= start && hi < end) || ranges_cover(publish.covered, (lo, hi))))
        }
    }
}

/// How many obsolete-generation refusals a derived unit must actually pay for.
///
/// A derived unit refuses a base file whose materialization generation is not
/// current. Demanding a rebuild for EVERY refusal livelocks when the refused
/// span is already reproduced by the selected files.
///
/// `refused` spans are INCLUSIVE of both ends (matching [`ranges_cover`]);
/// `None` is a file that will not say what it holds, and always counts.
///
/// ```
/// # use timefusion::rollup::unreproduced_refusals as unpaid;
/// const H: i64 = 3_600_000_000;
/// // One stale DAY-wide file over a day published as two halves: both halves
/// // are selected and tile the day, so refusing it costs nothing.
/// assert_eq!(unpaid(&[Some((0, 24 * H - 1))], &[(0, 12 * H), (12 * H, 24 * H)]), 0);
/// // A real hole: the selected files stop at noon, the refusal reaches past it.
/// assert_eq!(unpaid(&[Some((0, 24 * H - 1))], &[(0, 12 * H)]), 1);
/// // A gap anywhere in the middle is still a hole.
/// assert_eq!(unpaid(&[Some((0, 24 * H - 1))], &[(0, 6 * H), (7 * H, 24 * H)]), 1);
/// // A file that cannot say what it holds always counts — never assume empty.
/// assert_eq!(unpaid(&[None], &[(0, 24 * H)]), 1);
/// // Nothing selected reproduces nothing.
/// assert_eq!(unpaid(&[Some((0, H))], &[]), 1);
/// // Counts the refusals that are unpaid, not whether any refusal happened.
/// assert_eq!(unpaid(&[Some((0, H)), Some((20 * H, 30 * H))], &[(0, 24 * H)]), 1);
/// ```
pub fn unreproduced_refusals(refused: &[Option<(i64, i64)>], selected: &[(i64, i64)]) -> u64 {
    refused.iter().filter(|span| !span.is_some_and(|span| ranges_cover(selected, span))).count() as u64
}

/// Whether the union of `ranges` covers every instant in `[lo, hi]`.
///
/// `hi` is INCLUSIVE — it is a row's timestamp, straight from file statistics —
/// while a slice's end is exclusive, so a range must reach strictly past `hi`.
pub(crate) fn ranges_cover(ranges: &[(i64, i64)], (lo, hi): (i64, i64)) -> bool {
    // No range can reach strictly past `i64::MAX`, so that bound is never covered.
    let Some(bound) = hi.checked_add(1) else { return false };
    let clamped = ranges.iter().map(|&(start, end)| (start.max(lo).min(bound), end.max(lo).min(bound))).sorted().collect_vec();
    complement(lo, bound, &clamped).is_empty()
}

/// The sub-ranges of `untagged` that no live tagged slice covers — the exact
/// work that would close proof C for this partition. A partition with no tagged
/// ranges yields the untagged spans themselves.
///
/// Ends are EXCLUSIVE here, but a file's statistics `hi` is a row timestamp, so
/// callers pass `hi + 1`.
pub(crate) fn uncovered_gaps(untagged: &[(i64, i64)], tagged: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let covered = tagged.iter().copied().sorted().collect_vec();
    // Clamping to the span keeps `complement`'s precondition (ascending, inside
    // `[lo, hi)`) while leaving the gap set unchanged.
    let gaps = untagged
        .iter()
        .flat_map(|&(lo, hi)| {
            let clamped = covered.iter().map(|&(start, end)| (start.max(lo).min(hi), end.max(lo).min(hi))).collect_vec();
            complement(lo, hi, &clamped)
        })
        .collect();
    // Adjacent holes are ONE hole, else a day tiled by many files yields a unit
    // per file. The coordinator still bisects a merged hole that does not fit.
    crate::write::mem_buffer::merge_ranges(gaps)
}

/// The units `enqueue_untagged_rebuilds` should actually queue for one
/// partition: gaps snapped to the claimable floor, then merged.
///
/// ALIGN, then merge — the order matters. Gaps come from row statistics, so two
/// holes either side of one file's last row are milliseconds apart: too little
/// to matter, too much for `merge_ranges` to bridge.
///
/// Where a live tagged slice already CONTAINS a gap, that slice is queued
/// instead; publishing the contained span would be refused by `covered_by_wider`.
pub(crate) fn rebuild_slices(gaps: Vec<(i64, i64)>, tagged: &[(i64, i64)], day_start: i64, day_end: i64) -> Vec<(i64, i64)> {
    let floor = crate::maintenance_coordinator::MIN_SLICE_MICROS;
    let aligned = gaps
        .into_iter()
        .filter_map(|(start, end)| {
            let (start, end) = tagged
                .iter()
                .filter(|(covering_start, covering_end)| *covering_start <= start && *covering_end >= end)
                .min_by_key(|(covering_start, covering_end)| covering_end - covering_start)
                .copied()
                .unwrap_or((start, end));
            // Clamp to the partition: a slice overrunning the day would select
            // another partition's files.
            let start = start.max(day_start).div_euclid(floor) * floor;
            let end = end.min(day_end).saturating_add(floor - 1).div_euclid(floor) * floor;
            (end > start).then_some((start.max(day_start), end.min(day_end)))
        })
        .collect();
    crate::write::mem_buffer::merge_ranges(aligned)
}

/// A slice covering exactly one UTC calendar day — the partition granularity.
const fn spans_whole_day(start: i64, end: i64) -> bool {
    start.rem_euclid(crate::maintenance_coordinator::DAY_MICROS) == 0 && end.saturating_sub(start) == crate::maintenance_coordinator::DAY_MICROS
}

/// The declared measures a build actually MATERIALIZES, for `TAG_MEASURES`.
///
/// A measure counts only if it survives both ways of becoming an all-NULL
/// column: a missing INPUT (`slice_input_sql` projects `NULL AS …`) and a
/// missing TARGET column (`cast_record_batch` drops it).
///
/// On the DERIVED side a schema check is not enough — hence `base_evidence`: a
/// derived cell may claim a measure only if every base cell it read proved it.
pub(crate) fn materialized_measures(
    spec: &RollupSpec, derived: bool, present: &HashSet<String>, target: &arrow::datatypes::Schema, base_evidence: Option<&HashSet<String>>,
) -> Vec<String> {
    spec.measures
        .iter()
        .filter(|measure| {
            let input = if derived { Some(measure.name.as_str()) } else { measure.column.as_deref() };
            input.is_none_or(|column| present.contains(column))
                && (!derived || base_evidence.is_none_or(|proven| proven.contains(&measure.name)))
                && target.column_with_name(&measure.name).is_some()
        })
        .map(|measure| measure.name.clone())
        .collect()
}

/// What the base cells feeding a derived slice ALL prove they hold, for
/// `materialized_measures`.
///
/// `None` per cell is a cell with no `TAG_MEASURES` evidence, resolved as
/// `measures_available(None)` does — everything except
/// `MEASURES_ABSENT_FROM_LEGACY_CELLS`, since "proves nothing" would zero the
/// intersection for every derived cell over a legacy base.
///
/// `None` overall means no base cell overlapped the slice; vacuously permissive
/// because the base-coverage gate already retries on a real hole.
pub(crate) fn base_measure_evidence<'a>(spec: &RollupSpec, cells: impl Iterator<Item = Option<&'a HashSet<String>>>) -> Option<HashSet<String>> {
    cells
        .map(|held| {
            held.cloned().unwrap_or_else(|| {
                spec.measures.iter().map(|measure| measure.name.clone()).filter(|name| !MEASURES_ABSENT_FROM_LEGACY_CELLS.contains(&name.as_str())).collect()
            })
        })
        .reduce(|left, right| &left & &right)
}

/// The SELECT a rollup slice reads its input through, collapsing duplicates
/// when `dedup` says how.
///
/// `schema` describes whatever is registered as `raw` — for a DERIVED tier that
/// is the BASE TIER, not the raw source. `dedup = None` emits a bare
/// `SELECT *`, so the aggregate above would SUM a tier's superseded versions.
///
/// `present` is the PHYSICAL column set of the registered provider, not the
/// synthesized schema; a column it lacks is projected NULL.
pub(crate) fn slice_input_sql(
    schema: &crate::schema::TableSchema, dedup: Option<SliceDedup<'_>>, raw: &str, project_id: &str, (start, end): (i64, i64), shard_predicate: &str,
    present: Option<&HashSet<String>>,
) -> String {
    let window = format!(
        "WHERE project_id = {} AND timestamp >= to_timestamp_micros({start}) AND timestamp < to_timestamp_micros({end}){shard_predicate}",
        sql_literal(project_id)
    );
    let projected = |field: &crate::schema::FieldDef| match present {
        Some(present) if !present.contains(&field.name) => format!("NULL AS {}", quoted(&field.name)),
        _ => quoted(&field.name),
    };
    let missing = |name: &str| present.is_some_and(|present| !present.contains(name));
    let Some(dedup) = dedup.filter(|dedup| !dedup.keys.is_empty()) else {
        // `SELECT *` returns only what the provider has, so it cannot stand in
        // once anything is missing.
        return match schema.fields.iter().any(|field| missing(&field.name)) {
            true => format!("SELECT {} FROM {raw} {window}", schema.fields.iter().map(&projected).join(", ")),
            false => format!("SELECT * FROM {raw} {window}"),
        };
    };
    let inner = schema.fields.iter().map(&projected).join(", ");
    let columns = schema.fields.iter().map(|field| quoted(&field.name)).join(", ");
    let keys = dedup.keys.iter().map(|field| quoted(field)).join(", ");
    let order = dedup.tiebreak.map_or_else(|| keys.clone(), |field| format!("{} DESC NULLS LAST", quoted(field)));
    let tombstone = dedup.tombstone.map_or_else(String::new, |field| format!(" AND COALESCE({}, false) = false", quoted(field)));
    format!(
        "SELECT {columns} FROM (SELECT {inner}, ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {order}) AS __tf_rn FROM {raw} \
         {window}) WHERE __tf_rn = 1{tombstone}"
    )
}

/// Measures a cell WITHOUT `TAG_MEASURES` is known not to hold, and is refused for.
///
/// A bridge for cells written before `TAG_MEASURES` existed. ADD AN ENTRY
/// whenever a measure is declared on a spec before the cells predating it are
/// rebuilt; delete the constant once every cell carries the tag.
const MEASURES_ABSENT_FROM_LEGACY_CELLS: [&str; 2] = ["duration_digest", "service_name_hll"];

/// Measures refused on EVERY cell, tagged or not, because their stored state is
/// known empty and a merge over an empty state answers with a NUMBER rather than
/// declining (`distinct_count` of an empty HLL sketch is 0, not NULL).
///
/// Stronger than the list above: a tag proves the COLUMN was present, not that a
/// VALUE was written. Delete an entry only once an audit shows the measure reads
/// back over the window being served.
const MEASURES_NOT_YET_SERVABLE: [&str; 1] = ["service_name_hll"];

impl RoutedRollup {
    /// Every tier column this rewrite reads a state out of, the `HAVING` guard
    /// included — a cell missing the guard drops groups, not just a column.
    pub(crate) fn needed_measure_columns(&self) -> impl Iterator<Item = &str> {
        self.measures.iter().chain(self.guard.iter()).flat_map(|measure| measure.measures.iter().map(String::as_str))
    }

    /// Whether a cell whose materialized measures are `have` may serve this
    /// query. `None` is a legacy cell carrying no `TAG_MEASURES`.
    pub(crate) fn measures_available(&self, have: Option<&HashSet<String>>) -> bool {
        // Both arms: a tag cannot vouch for a state never written.
        if self.needed_measure_columns().any(|column| MEASURES_NOT_YET_SERVABLE.contains(&column)) {
            return false;
        }
        match have {
            Some(have) => self.needed_measure_columns().all(|column| have.contains(column)),
            None => self.needed_measure_columns().all(|column| !MEASURES_ABSENT_FROM_LEGACY_CELLS.contains(&column)),
        }
    }

    /// A half-open range predicate. Only `>=`/`<` are emitted: an inclusive bound
    /// on either side of a shared boundary double counts a whole bucket.
    /// `OPEN_END` is a sentinel, not a timestamp — it renders as no upper bound.
    fn range_sql(&(start, end): &(i64, i64)) -> String {
        if end == OPEN_END {
            return format!("(timestamp >= to_timestamp_micros({start}))");
        }
        format!("(timestamp >= to_timestamp_micros({start}) AND timestamp < to_timestamp_micros({end}))")
    }

    /// `GROUP BY 1, 2, …`, positional so it is valid whether the select list
    /// carries synthetic leg aliases or the query's own names.
    fn group_by(&self) -> String {
        if self.groups.is_empty() { String::new() } else { format!(" GROUP BY {}", (1..=self.groups.len()).join(", ")) }
    }

    /// One partial-aggregate SELECT over `ranges`. Aliases are synthetic
    /// (`__g0`, `__s1_0`) so a query alias can never collide with a state column.
    fn leg(&self, table: &str, ranges: &[(i64, i64)], extra: &str, projects: &str) -> String {
        let select = self
            .groups
            .iter()
            .enumerate()
            .map(|(index, (expression, _))| format!("{expression} AS __g{index}"))
            .chain(self.measures.iter().chain(self.guard.iter()).enumerate().flat_map(|(index, measure)| {
                let states = if table == self.target { measure.merge.partial_states(&measure.measures) } else { measure.raw.clone() };
                states.into_iter().enumerate().map(move |(state, sql)| format!("{sql} AS __s{index}_{state}"))
            }))
            .join(", ");
        let ranges = ranges.iter().map(Self::range_sql).join(" OR ");
        let row_filters = self.row_filters.iter().map(|filter| format!(" AND ({filter})")).collect::<String>();
        let group_by = self.group_by();
        format!("SELECT {select} FROM {table} WHERE {projects}({ranges}){extra}{row_filters}{group_by}")
    }

    /// `project_id IN (…) AND `, or `project_id = '…' AND ` when the split names
    /// no subset. Empty only when the query groups by project_id AND every
    /// project proved coverage.
    fn projects_in(&self, projects: Option<&[String]>) -> String {
        match projects {
            Some(list) => format!("project_id IN ({}) AND ", list.iter().map(|p| sql_literal(p)).join(", ")),
            None => self.project_id.as_deref().map_or_else(String::new, |project| format!("project_id = {} AND ", sql_literal(project))),
        }
    }

    /// The rewrite. `interiors` are the grain-aligned ranges the rollup leg owns;
    /// the raw leg owns their complement, so together they partition `[lo, hi)`
    /// with no gap and no overlap.
    pub fn sql(&self, generations: &[(String, String, String)], interiors: &[(i64, i64)], split: &ProjectSplit) -> String {
        let generations = format!(
            " AND ({})",
            generations
                .iter()
                // A generation id hashes the project, so a cross-project rewrite
                // names one per (project, date).
                .map(|(project, date, generation)| {
                    let prefix = if self.project_id.is_none() { format!("project_id = {} AND ", sql_literal(project)) } else { String::new() };
                    format!("({prefix}date = {} AND rollup_generation = {})", sql_literal(date), sql_literal(generation))
                })
                .join(" OR ")
        );
        // An open-ended window's raw leg must run to the sentinel, not the
        // stand-in `hi`, or the rewrite answers without the newest rows.
        let end = if self.open_end { OPEN_END } else { self.hi };
        let fringes = complement(self.lo, end, interiors);
        let rollup_projects = self.projects_in(split.covered.as_deref());
        // Unproved projects are read raw across the WHOLE window; with the covered
        // projects' interior and fringes that partitions (project x time) exactly.
        let raw_only_leg = (!split.raw_only.is_empty()).then(|| self.leg(&self.source, &[(self.lo, end)], "", &self.projects_in(Some(&split.raw_only))));
        if fringes.is_empty() && raw_only_leg.is_none() {
            // Single leg: the rollup rows ARE the partial states.
            let select = self
                .groups
                .iter()
                .map(|(expression, alias)| format!("{expression} AS {}", quoted(alias)))
                .chain(self.measures.iter().map(|measure| format!("{} AS {}", measure.merge.sql(&measure.measures), quoted(&measure.alias))))
                .join(", ");
            let row_filters = self.row_filters.iter().map(|filter| format!(" AND ({filter})")).collect::<String>();
            let group_by = self.group_by();
            let having = self.guard.as_ref().map_or_else(String::new, |guard| format!(" HAVING {} > 0", guard.merge.sql(&guard.measures)));
            return format!(
                "SELECT {select} FROM {} WHERE {rollup_projects}({}){generations}{row_filters}{group_by}{having}",
                self.target,
                interiors.iter().map(Self::range_sql).join(" OR "),
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
            .join(", ");
        let group_by = self.group_by();
        let having =
            self.guard.as_ref().map_or_else(String::new, |guard| format!(" HAVING {} > 0", guard.merge.sql(&[format!("__s{}_0", self.measures.len())])));
        let legs = std::iter::once(self.leg(&self.target, interiors, &generations, &rollup_projects))
            .chain((!fringes.is_empty()).then(|| self.leg(&self.source, &fringes, "", &rollup_projects)))
            .chain(raw_only_leg)
            .join(" UNION ALL ");
        format!("SELECT {outer} FROM ({legs}) AS rollup_union{group_by}{having}")
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
/// `CASE` before the matcher sees it. Narrow on purpose: exactly one `WHEN`,
/// whose predicate and result are the SAME column, and a string-literal
/// fallback. Anything else yields `None`.
fn coalesced_column(expr: &datafusion::logical_expr::Expr) -> Option<(&str, &str)> {
    use datafusion::logical_expr::Expr;
    match unaliased(expr) {
        Expr::ScalarFunction(function) if function.name().eq_ignore_ascii_case("coalesce") && function.args.len() == 2 => {
            Some((column_name(&function.args[0])?, string_literal(&function.args[1])?))
        }
        Expr::Case(case) => {
            let (probed, then) = null_guard_case(case)?;
            let column = column_name(probed)?;
            (column_name(then)? == column).then_some((column, string_literal(case.else_expr.as_ref()?)?))
        }
        _ => None,
    }
}

/// The `CASE WHEN <probed> IS NOT NULL THEN <then> …` shape DataFusion's
/// simplifier leaves where the query said `coalesce`, as `(<probed>, <then>)`.
/// The single spelling of the shape; how narrowly `probed` is accepted is the
/// caller's decision.
fn null_guard_case(case: &datafusion::logical_expr::Case) -> Option<(&datafusion::logical_expr::Expr, &datafusion::logical_expr::Expr)> {
    let [(when, then)] = &case.when_then_expr[..] else { return None };
    case.expr.is_none().then_some(())?;
    let datafusion::logical_expr::Expr::IsNotNull(probed) = when.as_ref() else { return None };
    Some((probed.as_ref(), then.as_ref()))
}

/// A retained dimension filter can prove a COALESCE fallback unreachable.
/// Only top-level AND terms establish this fact; an OR or a nullable comparison
/// must keep its fallback and decline if that column is absent from the rollup.
fn simplify_filtered_group(
    expression: &datafusion::logical_expr::Expr, predicates: &[datafusion::logical_expr::Expr], dimensions: &[String],
) -> datafusion::common::Result<datafusion::logical_expr::Expr> {
    use datafusion::{
        common::tree_node::{Transformed, TreeNode},
        logical_expr::{Expr, Operator, utils::split_conjunction},
    };
    let non_null: std::collections::HashSet<_> = predicates
        .iter()
        .flat_map(split_conjunction)
        .filter_map(|term| match term {
            Expr::IsNotNull(expr) => match expr.as_ref() {
                Expr::Column(column) => Some(column.name.as_str()),
                _ => None,
            },
            Expr::BinaryExpr(binary) if binary.op == Operator::Eq => match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(column), Expr::Literal(value, _)) | (Expr::Literal(value, _), Expr::Column(column)) if !value.is_null() => {
                    Some(column.name.as_str())
                }
                _ => None,
            },
            _ => None,
        })
        .filter(|column| dimensions.iter().any(|dimension| dimension == column))
        .collect();
    expression
        .clone()
        .transform_up(|node| {
            let replacement = match &node {
                Expr::ScalarFunction(function) if function.name() == "coalesce" => {
                    function.args.first().filter(|first| column_name(first).is_some_and(|column| non_null.contains(column))).cloned()
                }
                Expr::Case(case) => null_guard_case(case)
                    .filter(|&(probed, _)| matches!(probed, Expr::Column(column) if non_null.contains(column.name.as_str())))
                    .map(|(_, then)| then.clone()),
                _ => None,
            };
            Ok(replacement.map_or_else(|| Transformed::no(node), Transformed::yes))
        })
        .map(|result| result.data)
}

/// `extract(epoch from X)` — which DataFusion plans as `date_part('EPOCH', X)`
/// — optionally under an integer cast, returning `X` and the cast's SQL type.
///
/// Only `EPOCH` qualifies: every other field (`hour`, `dow`, …) is many-to-one
/// over buckets and would merge groups the raw path keeps apart. Only integer
/// casts qualify, being the ones whose SQL spelling reproduces exactly.
fn epoch_of(expr: &datafusion::logical_expr::Expr) -> Option<(&datafusion::logical_expr::Expr, Option<&'static str>)> {
    use datafusion::logical_expr::Expr;
    let expr = if let Expr::Alias(alias) = expr { alias.expr.as_ref() } else { expr };
    let (expr, cast) = match expr {
        Expr::Cast(inner) => (
            inner.expr.as_ref(),
            Some(match inner.field.data_type() {
                arrow::datatypes::DataType::Int32 => "INT",
                arrow::datatypes::DataType::Int64 => "BIGINT",
                _ => return None,
            }),
        ),
        expr => (expr, None),
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

/// The literal that `column = '…'` compares against, in either operand order;
/// `None` for any other shape (including `column = other_column`).
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
/// Sub-microsecond bounds must round UP: that is exact for both ends of a
/// half-open window on a microsecond-typed column; flooring shifts the window.
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

    // Render through the INNER value's `Display`; `ScalarValue`'s own spells its type.
    macro_rules! display_arms {
        ($($variant:ident),+) => {
            |value: &ScalarValue| match value {
                ScalarValue::Utf8(Some(value)) | ScalarValue::Utf8View(Some(value)) => Some(sql_literal(value)),
                $(ScalarValue::$variant(Some(value)) => Some(value.to_string()),)+
                ScalarValue::Null => Some("NULL".to_string()),
                _ => None,
            }
        };
    }
    let literal = display_arms!(Boolean, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64);
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
        // The three string scalars must collapse to ONE spelling, or identical
        // predicates spelled `Utf8`/`Utf8View` fail to match.
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
            // Idempotence at EVERY level, and `exactly_one` so `((X))` == `(X)`.
            let separator = if binary.op == Operator::And { " AND " } else { " OR " };
            operands.into_iter().map(canonical).sorted().dedup().exactly_one().unwrap_or_else(|mut terms| format!("({})", terms.join(separator)))
        }
        Expr::BinaryExpr(binary) => format!("({} {:?} {})", canonical(&binary.left), binary.op, canonical(&binary.right)),
        Expr::IsNotNull(expr) => format!("{} IS NOT NULL", canonical(expr)),
        Expr::ScalarFunction(function) => format!("{}({})", function.name(), function.args.iter().map(canonical).join(",")),
        expr => format!("{expr:?}"),
    }
}

/// Drop tantivy `text_match` accelerators from one AND level. They are added
/// beside the predicate they accelerate and carry no semantics, but the query
/// side and the declared measure filter do not receive the same hints.
///
/// Only a hint on a column this AND level already compares is dropped, so a
/// `text_match` the USER wrote against another column is preserved.
fn strip_index_hints(operands: &mut Vec<&datafusion::logical_expr::Expr>) {
    use datafusion::logical_expr::{Expr, Operator};
    fn hint_column(expr: &Expr) -> Option<String> {
        match unaliased(expr) {
            Expr::ScalarFunction(function) if function.name() == "text_match" => column_name(function.args.first()?).map(str::to_string),
            // The IN-list spelling is an OR subtree of per-item `text_match`
            // calls; only an OR whose EVERY leaf hints the same column is a hint.
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
    // `X AND X` must canonicalize to `X`: a pushed-down predicate is collected
    // both from the Filter node and from the TableScan.
    expressions.into_iter().map(canonical).sorted().dedup().join(" AND ")
}

fn parse_bucket_micros(value: &str) -> Option<i64> {
    let mut parts = value.split_whitespace();
    let value = parts.next()?.parse::<i64>().ok()?;
    let unit = match parts.next()?.trim_end_matches('s') {
        "second" | "sec" => 1_000_000,
        "minute" | "min" => 60_000_000,
        "hour" | "hr" => 3_600_000_000,
        "day" => 86_400_000_000,
        _ => return None,
    };
    value.checked_mul(unit)
}

/// The scanned table, or a description of the node that stopped the walk.
pub(crate) fn source_and_filters(plan: &datafusion::logical_expr::LogicalPlan, filters: &mut Vec<datafusion::logical_expr::Expr>) -> Result<String, String> {
    use datafusion::logical_expr::{Expr, LogicalPlan};
    match plan {
        // Only a rename-free projection may be walked through, or the matcher
        // reads a declared dimension off the wrong source column.
        LogicalPlan::Projection(projection) if projection.expr.iter().all(|expr| matches!(expr, Expr::Column(_))) => {
            source_and_filters(&projection.input, filters)
        }
        LogicalPlan::Filter(filter) => {
            filters.push(filter.predicate.clone());
            source_and_filters(&filter.input, filters)
        }
        LogicalPlan::TableScan(scan) => {
            filters.extend(scan.filters.clone());
            Ok(scan.table_name.table().to_string())
        }
        // Name only the exprs that disqualified the projection; `display()` on a
        // wide `SELECT *` node is a multi-KB log line.
        LogicalPlan::Projection(projection) => {
            Err(truncated(&format!("Projection: {}", projection.expr.iter().filter(|expr| !matches!(expr, Expr::Column(_))).take(4).join(", "))))
        }
        // A derived table or CTE. An alias re-qualifies but never renames, and
        // the chain admitted here holds exactly ONE TableScan, so a bare
        // `Column::name` still identifies one column.
        LogicalPlan::SubqueryAlias(alias) => {
            let source = source_and_filters(&alias.input, filters)?;
            // Unqualify so the whole chain has one spelling; otherwise the same
            // conjunct arrives twice and `canonical_and`'s dedup cannot cancel it.
            use datafusion::common::{
                Column,
                tree_node::{Transformed, TreeNode},
            };
            let unqualify = |node: Expr| {
                Ok(match node {
                    Expr::Column(column) => Transformed::yes(Expr::Column(Column::new_unqualified(column.name))),
                    node => Transformed::no(node),
                })
            };
            for filter in filters.iter_mut() {
                if let Ok(stripped) = filter.clone().transform_up(unqualify) {
                    *filter = stripped.data;
                }
            }
            Ok(source)
        }
        plan => Err(truncated(&plan.display().to_string())),
    }
}

/// One bounded log field: node descriptions are attacker-free but unbounded.
fn truncated(text: &str) -> String {
    const LIMIT: usize = 240;
    text.char_indices().nth(LIMIT).map_or_else(|| text.to_string(), |(cut, _)| format!("{}…", &text[..cut]))
}

/// Undo DataFusion's common-subexpression extraction, for MATCHING only:
/// substitutes each `__common_expr_N` back so the group expression again
/// resembles a dimension. Deliberately narrow — only CSE's own aliases are
/// inlined, and the rebuilt aggregate must keep the same field names and types.
pub(crate) fn inline_common_exprs(aggregate: &datafusion::logical_expr::Aggregate) -> Option<datafusion::logical_expr::Aggregate> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::logical_expr::{Aggregate, Expr, LogicalPlan};

    const CSE_PREFIX: &str = "__common_expr_";

    let LogicalPlan::Projection(projection) = aggregate.input.as_ref() else { return None };
    // A rename or a computed projection declines, as in `source_and_filters`.
    let definitions = projection.expr.iter().try_fold(std::collections::HashMap::new(), |mut definitions, expr| match expr {
        Expr::Alias(alias) if alias.name.starts_with(CSE_PREFIX) => {
            definitions.insert(alias.name.clone(), alias.expr.as_ref().clone());
            Some(definitions)
        }
        Expr::Column(_) => Some(definitions),
        _ => None,
    })?;
    (!definitions.is_empty()).then_some(())?;
    let inline = |expr: &Expr| {
        expr.clone()
            .transform_up(|node| {
                Ok(match &node {
                    Expr::Column(column) => definitions.get(&column.name).map_or(Transformed::no(node), |definition| Transformed::yes(definition.clone())),
                    _ => Transformed::no(node),
                })
            })
            .map(|transformed| transformed.data)
            .ok()
            // One bottom-up pass cannot resolve an alias defined via another
            // alias, so decline a half-substituted shape.
            .filter(|inlined| !inlined.column_refs().iter().any(|column| column.name.starts_with(CSE_PREFIX)))
    };
    let group_expr = aggregate.group_expr.iter().map(inline).collect::<Option<Vec<_>>>()?;
    let aggr_expr = aggregate.aggr_expr.iter().map(inline).collect::<Option<Vec<_>>>()?;
    Aggregate::try_new(projection.input.clone(), group_expr, aggr_expr)
        .ok()
        .filter(|rebuilt| rebuilt.schema.has_equivalent_names_and_types(&aggregate.schema).is_ok())
}

/// The table `plan` ultimately scans. Diagnostics only — a plan with two scans
/// yields the first, so this must never feed anything that answers rows.
fn scanned_table(plan: &datafusion::logical_expr::LogicalPlan) -> Option<String> {
    match plan {
        datafusion::logical_expr::LogicalPlan::TableScan(scan) => Some(scan.table_name.table().to_string()),
        plan => plan.inputs().into_iter().find_map(scanned_table),
    }
}

/// Does this predicate constrain ONLY `project_id`/`timestamp` — the two the
/// probe injects to satisfy the scan admission guard?
fn is_probe_scaffolding(expr: &datafusion::logical_expr::Expr) -> bool {
    let columns = expr.column_refs();
    !columns.is_empty() && columns.iter().all(|column| matches!(column.name.as_str(), "project_id" | "timestamp"))
}

/// Canonicalized declared filters, keyed by (source, spec, measure). Safe to
/// memoize for the process: the canonical form depends only on the declared
/// filter text and the schema's coercion rules.
static MEASURE_FILTERS: std::sync::OnceLock<dashmap::DashMap<(String, String, String), String>> = std::sync::OnceLock::new();

async fn measure_filters<'a>(
    session: &datafusion::execution::context::SessionState, source: &str, spec: &'a RollupSpec, project_id: &str, lo: i64, hi: i64,
) -> Result<Vec<(&'a RollupMeasure, String)>, MissReason> {
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
            // Must go through the SAME pipeline as the query side (optimize, then
            // split into conjuncts) or the two strings can never be equal.
            Some(filter) => {
                // `SELECT timestamp`, never `SELECT *`: `*` expands to a
                // projection `source_and_filters` refuses to walk. The project
                // and time bounds satisfy the scan admission guard and are
                // stripped back out below.
                let probe = format!(
                    "SELECT timestamp FROM {source} WHERE project_id = {} AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi}) AND ({filter})",
                    sql_literal(project_id)
                );
                // A failure here disqualifies every filtered measure, so the spec
                // stops routing at all — log it rather than discarding the error.
                let planned = async { session.optimize(&session.create_logical_plan(&probe).await?) }.await;
                let plan = planned.map_err(|error| {
                    tracing::warn!(event = "rollup_measure_probe_failed", source, measure = %measure.name, probe, %error, "a declared measure filter could not be planned");
                    MissReason::UnknownFilter
                })?;
                let mut filters = Vec::new();
                source_and_filters(&plan, &mut filters).map_err(|node| {
                    tracing::warn!(event = "rollup_measure_probe_unwalkable", source, measure = %measure.name, node, plan = %plan.display_indent(), "a declared measure filter planned to a shape the matcher cannot read");
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

/// The outermost `Aggregate`, wherever the optimizer put it. Nothing above it is
/// inspected or rebuilt — the rewrite is substituted in place. Do not match a
/// fixed grammar of parent nodes; the shape above depends on the analyzer rules.
fn outermost_aggregate(plan: &datafusion::logical_expr::LogicalPlan) -> Option<&datafusion::logical_expr::LogicalPlan> {
    match plan {
        datafusion::logical_expr::LogicalPlan::Aggregate(_) => Some(plan),
        plan => plan.inputs().into_iter().find_map(outermost_aggregate),
    }
}

/// Every rollup that could serve this aggregate, best first. The caller picks:
/// only it knows which tiers are actually built for the dates in the window.
pub(crate) async fn match_aggregates(
    plan: &datafusion::logical_expr::LogicalPlan, session: &datafusion::execution::context::SessionState,
) -> Result<Vec<RoutedRollup>, MissReason> {
    use datafusion::logical_expr::LogicalPlan;

    // Read paths only: the whole-tree search would otherwise reach an aggregate
    // nested inside a DML statement or an EXPLAIN, which may not be rewritten.
    if matches!(
        plan,
        LogicalPlan::Dml(_) | LogicalPlan::Ddl(_) | LogicalPlan::Copy(_) | LogicalPlan::Explain(_) | LogicalPlan::Analyze(_) | LogicalPlan::Statement(_)
    ) {
        return Ok(Vec::new());
    }
    let Some(matched) = outermost_aggregate(plan) else { return Ok(Vec::new()) };
    let LogicalPlan::Aggregate(original) = matched else { unreachable!("outermost_aggregate returns an Aggregate") };
    // Match against the pre-CSE shape where there is one; `matched` stays the
    // node actually in the tree, which is what the rewrite substitutes for.
    let inlined = inline_common_exprs(original);
    let aggregate = inlined.as_ref().unwrap_or(original);
    let shape = || matched.display_indent_schema().to_string().lines().take(6).join(" | ");
    let mut predicates = Vec::new();
    let source = match source_and_filters(&aggregate.input, &mut predicates) {
        Ok(source) => source,
        Err(node) => {
            // Count this only when a rollup-bearing table sits underneath; an
            // aggregate over a join or `pg_catalog` was never a candidate.
            if let Some(table) =
                scanned_table(&aggregate.input).filter(|table| crate::schema::get_schema(table).is_some_and(|schema| !schema.rollups.is_empty()))
            {
                crate::observability::record_rollup_miss(MissReason::UnwalkableSource);
                // Unconditional warn, not sampled: this class is too rare to
                // survive 1-in-64 sampling.
                tracing::warn!(
                    event = "rollup_declined_shape",
                    source = %table,
                    reason = MissReason::UnwalkableSource.label(),
                    node,
                    inlined_cse = inlined.is_some(),
                    plan = %shape(),
                    "the matcher could not walk from the aggregate down to the scan"
                );
            }
            return Ok(Vec::new());
        }
    };
    let Some(schema) = crate::schema::get_schema(&source).filter(|schema| !schema.rollups.is_empty()) else { return Ok(Vec::new()) };

    // Coarsest grain first (strictly fewer rows for the same answer); ties break
    // toward the narrower dimension set.
    let candidates = schema.rollups.iter().sorted_by_key(|spec| (std::cmp::Reverse(spec.grain_micros().unwrap_or(0)), spec.dimensions.len()));
    let (mut miss, mut grain_miss) = (None, None);
    let mut routes = Vec::new();
    for spec in candidates {
        match route_with_spec(spec, &source, &schema.table_name, &predicates, aggregate, session).await {
            // The rewrite replaces the node that is IN the tree, never the
            // inlined stand-in — `substitute` finds it by structural equality.
            Ok(route) => routes.push(RoutedRollup { matched: matched.clone(), ..route }),
            // Grain-only disqualifications are held back so they cannot mask an
            // actionable declared-schema gap; still reported if every spec
            // declined that way.
            Err(reason @ (MissReason::PartialBucket | MissReason::TinyInterior)) => grain_miss = Some(reason),
            Err(reason) => miss = Some(reason),
        }
    }
    // EVERY viable spec, not just the best on paper: coverage is per (spec,
    // date) and known only to the caller.
    if !routes.is_empty() {
        return Ok(routes);
    }
    let reason = miss.or(grain_miss).unwrap_or(MissReason::UnsupportedShape);
    let shape = shape();
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
/// `Ok(false)`: the term says nothing about `timestamp`. `Err`: it bounds
/// `timestamp` unreadably, which must never be ignored — a dropped bound widens
/// the window and would serve rows the query excluded.
fn narrow_timestamp(term: &datafusion::logical_expr::Expr, lo: &mut Option<i64>, hi: &mut Option<i64>) -> Result<bool, MissReason> {
    use datafusion::logical_expr::{Expr, Operator};
    // Bounds only ever tighten: max on the inclusive start, min on the exclusive end.
    let narrow = |bound: &mut Option<i64>, value: i64, tighten: fn(i64, i64) -> i64| *bound = Some(bound.map_or(value, |current| tighten(current, value)));
    // The exclusive end of an INCLUSIVE bound (`BETWEEN`, `<=`).
    let exclusive = |value: i64| value.checked_add(1).ok_or(MissReason::UnboundedTime);
    match term {
        Expr::Between(between) if !between.negated && column_name(&between.expr) == Some("timestamp") => {
            let (Some(lower), Some(upper)) = (timestamp_literal(&between.low), timestamp_literal(&between.high)) else {
                return Err(MissReason::UnboundedTime);
            };
            narrow(lo, lower, i64::max);
            narrow(hi, exclusive(upper)?, i64::min);
        }
        Expr::BinaryExpr(binary) if column_name(&binary.left) == Some("timestamp") => {
            let Some(value) = timestamp_literal(&binary.right) else { return Err(MissReason::UnboundedTime) };
            match binary.op {
                Operator::GtEq => narrow(lo, value, i64::max),
                Operator::Gt => narrow(lo, exclusive(value)?, i64::max),
                Operator::Lt => narrow(hi, value, i64::min),
                Operator::LtEq => narrow(hi, exclusive(value)?, i64::min),
                _ => return Err(MissReason::UnknownFilter),
            }
        }
        // `date_trunc(unit, timestamp) = X` bounds the window exactly as
        // `timestamp >= X AND timestamp < X + width(unit)` does.
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let Some((width, start)) = [(&binary.left, &binary.right), (&binary.right, &binary.left)]
                .into_iter()
                .find_map(|(truncated, literal)| Some((date_trunc_width(truncated)?, timestamp_literal(literal)?)))
            else {
                return Ok(false);
            };
            // No timestamp truncates to an unaligned instant, so this is
            // unsatisfiable rather than a window.
            if start.rem_euclid(width) != 0 {
                return Err(MissReason::UnknownFilter);
            }
            let end = start.checked_add(width).ok_or(MissReason::UnboundedTime)?;
            narrow(lo, start, i64::max);
            narrow(hi, end, i64::min);
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// The fixed width in microseconds of `date_trunc(unit, timestamp)`.
///
/// Only epoch-aligned, constant-width units qualify: `month`/`quarter`/`year`
/// have no fixed width, and `week` truncates to Monday while the epoch was a
/// Thursday, so the caller's alignment test would be wrong.
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
/// to, or `None` when it does not bound it on both sides. Only conjuncts narrow;
/// a disjunction leaves the window open, the safe direction for every caller.
pub(crate) fn timestamp_window(predicate: &datafusion::logical_expr::Expr) -> Option<(i64, i64)> {
    let (mut lo, mut hi) = (None, None);
    // An unreadable bound is not "no bound": treat it as unbounded.
    for term in datafusion::logical_expr::utils::split_conjunction(predicate) {
        narrow_timestamp(term, &mut lo, &mut hi).ok()?;
    }
    lo.zip(hi).filter(|(lo, hi)| lo < hi)
}

/// One output aggregate: the rollup-leg measure columns plus the raw-leg SQL,
/// one entry per state, in `merge` order. The raw leg reproduces each measure's
/// DECLARED filter text verbatim — exact, since the query's filter canonicalizes
/// to the same predicate. `COUNT(col)` whenever the measure carries a column:
/// `COUNT(*)` would keep an all-null bucket the rollup leg's `HAVING` drops.
fn routed_measure(alias: String, merge: Merge, resolved: &[&RollupMeasure]) -> RoutedMeasure {
    let raw = |measure: &RollupMeasure| {
        let expression = match (merge, measure.column.as_deref()) {
            (Merge::TDigest, Some(column)) => format!("percentile_agg(CAST({column} AS DOUBLE))"),
            (Merge::Hll, Some(column)) => format!("hll_agg({column})"),
            // Only the value state needs the ordered spelling; the companion is an ordinary `min`.
            (Merge::First, Some(column)) if measure.agg == "first" => format!("first_value({column} ORDER BY timestamp)"),
            (_, None) => "COUNT(*)".to_string(),
            (_, Some(column)) => format!("{}({column})", measure.agg.to_uppercase()),
        };
        filtered(expression, measure.filter.as_deref())
    };
    let measures = resolved.iter().map(|measure| measure.name.clone()).collect();
    RoutedMeasure { alias, merge, measures, raw: resolved.iter().copied().map(raw).collect() }
}

/// Resolve one query against one declared rollup spec. Every output is aliased
/// with the aggregate's OWN field name — the untouched nodes above reference it.
async fn route_with_spec(
    spec: &RollupSpec, source: &str, table_name: &str, predicates: &[datafusion::logical_expr::Expr], aggregate: &datafusion::logical_expr::Aggregate,
    session: &datafusion::execution::context::SessionState,
) -> Result<RoutedRollup, MissReason> {
    use datafusion::logical_expr::{Expr, utils::split_conjunction};

    let mut project_id = None;
    let (mut lo, mut hi) = (None, None);
    let mut row_filters = Vec::new();
    let mut promotable: Vec<&Expr> = Vec::new();
    // `col IS NOT NULL`, held apart from `promotable` and resolved as the guard.
    let mut null_guards: Vec<&str> = Vec::new();
    // Strip tantivy hints BEFORE classifying, or a hint is orphaned into
    // `promotable` once the predicate it accelerates is consumed as a dimension
    // filter, leaving a term no declared measure can match.
    let mut terms: Vec<&Expr> = predicates.iter().flat_map(split_conjunction).collect();
    strip_index_hints(&mut terms);
    for term in terms {
        if narrow_timestamp(term, &mut lo, &mut hi)? {
            continue;
        }
        // A predicate we can push into the rollup scan is a dimension filter; one
        // we cannot may NOT be answered by a measure pre-filtered the same way,
        // because the raw query also eliminates the groups where nothing matched
        // and re-aggregating resurrects them as 0/NULL rows.
        match (eq_literal(term, "project_id"), dimension_filter_sql(term, &spec.dimensions)) {
            // Two different literals cannot both hold; never keep the last.
            (Some(value), _) if project_id.as_deref().is_some_and(|current| current != value) => return Err(MissReason::MissingProject),
            (Some(value), _) => project_id = Some(value.to_string()),
            (None, Some(filter)) => row_filters.push(filter),
            // `col IS NOT NULL` over a non-dimension is expressed exactly by a
            // `count(col)` measure, so it becomes the guard.
            (None, None) => match unaliased(term) {
                Expr::IsNotNull(inner) if column_name(inner).is_some() => null_guards.push(column_name(inner).unwrap_or_default()),
                _ => promotable.push(term),
            },
        }
    }
    // The guard is a single measure, so two guarded columns must fail closed
    // rather than guard on one and ignore the other.
    if !null_guards.iter().all_equal() {
        return Err(MissReason::FilterMultipleNullGuards);
    }
    let null_guard = null_guards.first().copied();
    // A query that neither pins nor groups by project_id would fold every tenant
    // into one row.
    if project_id.is_none() && !aggregate.group_expr.iter().any(|expr| column_name(expr) == Some("project_id")) {
        return Err(MissReason::MissingProject);
    }
    // With no upper bound, `now` closes the window for the interior arithmetic
    // ONLY; `open_end` tells `sql` to leave the trailing raw range unbounded so
    // rows past `now` are still returned.
    let open_end = hi.is_none();
    let (lo, hi) = lo.zip(hi.or_else(|| Some(crate::support::now_micros()))).filter(|(lo, hi)| lo < hi).ok_or(MissReason::UnboundedTime)?;
    let grain = spec.grain_micros().ok_or(MissReason::UnsupportedShape)?;
    // A grain too coarse for the window yields no usable interior. Reject here
    // rather than at interior(), or a 1h tier shadows the 1m tier that CAN serve
    // a 10-minute window; re-checked after promotion so a residual filter is
    // reported as the more actionable diagnosis.
    let too_narrow = hi.saturating_sub(lo) < grain.saturating_mul(MIN_INTERIOR_BUCKETS);
    if too_narrow && promotable.is_empty() {
        return Err(MissReason::TinyInterior);
    }
    // Scaffolding the canonicalizer strips back out; any value satisfies the scan
    // admission guard's shape check.
    let probe_project = project_id.as_deref().unwrap_or("rollup-probe");
    let configured_filters = measure_filters(session, source, spec, probe_project, lo, hi).await?;
    let declared_measure = |aggregate: &str, column: Option<&str>, filter: &str| {
        configured_filters
            .iter()
            .find(|(measure, declared)| measure.agg == aggregate && measure.column.as_deref() == column && declared.as_str() == filter)
            .map(|(measure, _)| *measure)
    };

    // ROW-FILTER PROMOTION. A residual predicate is normally fatal, but when it
    // canonicalizes to exactly a DECLARED measure filter, a `HAVING` over a count
    // sharing that filter reproduces the raw query's group elimination exactly.
    // Without a null guard the count must have NO column (`count(col)` skips
    // nulls); with one, a single `{count, col, filter}` lookup covers the whole
    // conjunction — guarding on two counts separately is weaker than the raw query.
    let promoted = (!promotable.is_empty()).then(|| canonical_and(promotable.iter().copied()));
    let guard = match (promoted.as_deref(), null_guard) {
        (None, None) => None,
        (promoted, column) => Some(declared_measure("count", column, promoted.unwrap_or_default()).ok_or_else(|| {
            let declared = || {
                configured_filters
                    .iter()
                    .filter(|(measure, _)| measure.agg == "count" && measure.column.as_deref() == column)
                    .map(|(measure, filter)| format!("{}={filter}", measure.name))
                    .join(" | ")
            };
            // A residual constraining columns no declared filter mentions was
            // never a candidate, so it gets its own reason rather than inflating
            // `unknown_filter`.
            let guard_column_declared = column.is_some_and(|column| configured_filters.iter().any(|(measure, _)| measure.column.as_deref() == Some(column)));
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
            // A near-miss: the residual names the same columns a declared measure
            // filters on, yet did not match.
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
        })?),
    };

    if too_narrow {
        return Err(MissReason::TinyInterior);
    }

    // `project_id` is the partition column rather than a declared dimension, but
    // it groups exactly like one.
    let is_dimension = |column: &str| column == "project_id" || spec.dimensions.iter().any(|dimension| dimension == column);
    let groups = aggregate
        .group_expr
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            let alias = aggregate.schema.field(index).name();
            let simplified = simplify_filtered_group(expression, predicates, &spec.dimensions).map_err(|_| MissReason::UnsupportedShape)?;
            let expression = &simplified;
            // `extract(epoch from time_bucket(w, timestamp))::integer` is injective
            // on the bucket, so only the spelling differs. The wrapper must be
            // REPRODUCED, not dropped: the rewrite has to match the aggregate's
            // schema types.
            let epoch_wrapped = epoch_of(expression);
            let expression = match epoch_wrapped.map_or_else(|| unaliased(expression), |(inner, _)| inner) {
                Expr::Column(column) if is_dimension(&column.name) => column.name.clone(),
                Expr::ScalarFunction(function)
                    if function.name().eq_ignore_ascii_case("time_bucket")
                        && function.args.len() == 2
                        && column_name(&function.args[1]) == Some("timestamp") =>
                {
                    let interval = string_literal(&function.args[0]).ok_or(MissReason::UnsupportedShape)?;
                    let width = parse_bucket_micros(interval).ok_or(MissReason::UnsupportedShape)?;
                    // A width that is not a whole number of grains makes one rollup
                    // row straddle two output buckets; no state can be split.
                    if width < grain || width % grain != 0 {
                        return Err(MissReason::PartialBucket);
                    }
                    format!("time_bucket({}, timestamp)", sql_literal(interval))
                }
                // `COALESCE(dim, lit)` is a function of `dim`, so partitioning by
                // `dim` refines it and re-aggregating decomposable states over a
                // refinement equals aggregating the raw rows.
                other => {
                    // A bare column that is not a declared dimension keeps its own
                    // reason — it is the one shape an operator can act on.
                    let Some((column, fallback)) = coalesced_column(other).filter(|(column, _)| is_dimension(column)) else {
                        return Err(if matches!(other, Expr::Column(_)) { MissReason::UnknownGroupBy } else { MissReason::UnsupportedShape });
                    };
                    format!("COALESCE({column}, {})", sql_literal(fallback))
                }
            };
            // Only the bucket may wear the epoch wrapper; a dimension grouped by
            // `extract(epoch …)` is nonsense and must not be silently accepted.
            let expression = match epoch_wrapped {
                Some((_, cast)) if expression.starts_with("time_bucket(") => match cast {
                    Some(sql_type) => format!("CAST(date_part('EPOCH', {expression}) AS {sql_type})"),
                    None => format!("date_part('EPOCH', {expression})"),
                },
                Some(_) => return Err(MissReason::UnsupportedShape),
                None => expression,
            };
            Ok((expression, alias.to_string()))
        })
        .collect::<Result<Vec<_>, MissReason>>()?;

    let measures = aggregate
        .aggr_expr
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            let alias = aggregate.schema.field(aggregate.group_expr.len() + index).name().to_string();
            let Expr::AggregateFunction(function) = unaliased(expression) else { return Err(MissReason::NonDecomposableAggregate) };
            // An ORDER BY inside an aggregate makes it depend on row order, which no
            // partial state can carry — except `first_value(x ORDER BY timestamp)`,
            // which orders by the axis the rollup buckets on, so a (value,
            // timestamp) pair merges associatively.
            let ordered_by_timestamp = function.func.name().eq_ignore_ascii_case("first_value")
                && matches!(
                    function.params.order_by.as_slice(),
                    [sort] if column_name(&sort.expr) == Some("timestamp") && sort.asc
                );
            if function.params.distinct || (!function.params.order_by.is_empty() && !ordered_by_timestamp) {
                return Err(MissReason::NonDecomposableAggregate);
            }
            // The promoted conjuncts join the aggregate's own, so the pair resolves
            // to the measure declared as exactly that conjunction.
            let filter = canonical_and(function.params.filter.iter().flat_map(|filter| split_conjunction(filter.as_ref())).chain(promotable.iter().copied()));
            let name = function.func.name().to_ascii_lowercase();
            let column = function.params.args.first().and_then(column_name).map(str::to_string);
            // Under `col IS NOT NULL`, `count(*) ≡ count(col)`. The guard is set
            // aside rather than pushed, so neither leg re-applies it and the
            // rewritten measure skips exactly the rows the predicate excluded.
            let column = if name == "count" && column.is_none() { null_guard.map(str::to_string) } else { column };
            if null_guard.is_some() && column.as_deref() != null_guard {
                return Err(MissReason::FilterNullGuardMismatch);
            }
            let measure = |aggregate: &str, column: Option<&str>| declared_measure(aggregate, column, &filter);
            let one = |aggregate: &str| measure(aggregate, column.as_deref()).map(|measure| vec![measure]);
            let (merge, resolved) = match name.as_str() {
                "count" => (Merge::Count, one("count")),
                "sum" => (Merge::Sum, one("sum")),
                "min" => (Merge::Min, one("min")),
                "max" => (Merge::Max, one("max")),
                "avg" => (Merge::Avg, measure("sum", column.as_deref()).zip(measure("count", column.as_deref())).map(|(sum, count)| vec![sum, count])),
                "percentile_agg" => (Merge::TDigest, one("tdigest")),
                // Like `percentile_agg`: the aggregate yields the STATE and the
                // scalar reading a number out of it sits above, untouched.
                "hll_agg" => (Merge::Hll, one("hll")),
                // A PAIR like `avg`: the stored value plus the companion
                // `min(timestamp)` saying which row it came from. Declines under a
                // null guard — the guard reaches neither leg and `first_value`
                // does not skip nulls.
                "first_value" if null_guard.is_none() => {
                    (Merge::First, measure("first", column.as_deref()).zip(measure("min", Some("timestamp"))).map(|(first, at)| vec![first, at]))
                }
                _ => return Err(MissReason::NonDecomposableAggregate),
            };
            let resolved = resolved.ok_or(MissReason::MissingMeasure)?;
            debug_assert_eq!(resolved.len(), merge.arity(), "{merge:?} resolved the wrong number of measures");
            Ok(routed_measure(alias, merge, &resolved))
        })
        .collect::<Result<Vec<_>, MissReason>>()?;

    let guard = guard.map(|measure| routed_measure("__guard".to_string(), Merge::Count, &[measure]));

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

    /// `uncovered` must accept unsorted, overlapping coverage — callers pass
    /// `DashMap` iteration order and republish one range at several widths.
    #[test]
    fn uncovered_tolerates_unsorted_overlapping_coverage() {
        for (covered, want) in [
            (vec![], vec![(0, 100)]),
            (vec![(0, 100)], vec![]),
            (vec![(50, 100), (0, 60)], vec![]),
            (vec![(0, 25)], vec![(25, 100)]),
            (vec![(25, 50), (75, 100)], vec![(0, 25), (50, 75)]),
        ] {
            assert_eq!(uncovered(0, 100, covered.clone()), want, "coverage {covered:?}");
        }
    }

    /// `MissReason::label` feeds the `rollup_misses` counter: a telemetry contract.
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
                "filter_multiple_null_guards",
                "filter_null_guard_mismatch",
                "missing_measure",
                "non_decomposable",
                "unaligned_bucket_width",
                "not_built",
                "stale_coverage",
                "incomplete_coverage",
                "tiny_interior",
                "too_many_branches",
                "rewrite_schema_mismatch",
                "unwalkable_source",
                "measure_not_stored",
            ]
        );
    }

    use crate::maintenance_coordinator::DAY_MICROS;
    /// Any day-aligned instant; `slice_retires` takes the partition date as a label.
    const DAY: i64 = 20_683 * DAY_MICROS;
    const HOUR: i64 = HOUR_MICROS;

    /// A file is retired only when this partition provably reproduces it.
    #[test_case::test_case(Some((DAY, DAY + HOUR)), None, (DAY, DAY + DAY_MICROS), &[], 9, true; "tagged file contained by the slice")]
    #[test_case::test_case(Some((DAY - HOUR, DAY + DAY_MICROS)), None, (DAY, DAY + DAY_MICROS), &[], 9, false; "tagged file wider than the slice")]
    #[test_case::test_case(None, None, (DAY, DAY + DAY_MICROS), &[], 9, true; "untagged, whole-day slice needs no stats")]
    #[test_case::test_case(None, None, (DAY, DAY + HOUR), &[], 9, false; "untagged, sub-day slice with no stats proves nothing")]
    #[test_case::test_case(None, Some((DAY, DAY + HOUR - 1)), (DAY, DAY + HOUR), &[], 9, true; "untagged, stats inside a sub-day slice")]
    #[test_case::test_case(None, Some((DAY, DAY + HOUR)), (DAY, DAY + HOUR), &[], 9, false; "untagged, stats touch the exclusive end")]
    #[test_case::test_case(None, Some((DAY, DAY + HOUR - 1)), (DAY, DAY + HOUR), &[], 0, false; "empty rebuild may be the only copy left")]
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

    /// Tiers publish independently, so the one exported gauge slot must SUM
    /// per-tier counts rather than store the latest publish's.
    #[test]
    fn the_untagged_gauge_sums_tiers_rather_than_letting_one_mask_another() {
        let per_tier: dashmap::DashMap<String, u64> = dashmap::DashMap::new();
        let exported = || -> u64 { per_tier.iter().map(|entry| *entry.value()).sum() };

        per_tier.insert("otel_logs_and_spans_rollup_dashboard_1m_v3".to_owned(), 67);
        assert_eq!(exported(), 67);

        per_tier.insert("otel_logs_and_spans_rollup_dashboard_1h_v2".to_owned(), 0);
        assert_eq!(exported(), 67, "a clean tier's publish must not hide another tier's damage");

        // Zero only when EVERY tier is clean, which is what makes "alarm on > 0" sound.
        per_tier.insert("otel_logs_and_spans_rollup_dashboard_1m_v3".to_owned(), 0);
        assert_eq!(exported(), 0);
    }

    #[test]
    fn ranges_cover_needs_an_unbroken_run_past_the_inclusive_end() {
        assert!(ranges_cover(&[(DAY + HOUR, DAY + 2 * HOUR), (DAY, DAY + HOUR)], (DAY, DAY + 2 * HOUR - 1)));
        // Reaching exactly the inclusive end is NOT enough: a slice end is
        // exclusive, so a row AT `hi` would not be reproduced.
        assert!(!ranges_cover(&[(DAY, DAY + HOUR)], (DAY, DAY + HOUR)));
        assert!(!ranges_cover(&[], (DAY, DAY)));
        // A gap anywhere, even one covered by a LATER range, breaks the proof.
        assert!(!ranges_cover(&[(DAY, DAY + HOUR), (DAY + 2 * HOUR, DAY + 9 * HOUR)], (DAY, DAY + 3 * HOUR)));
    }

    /// The complement of the live tagged ranges is what must be REBUILT.
    #[test]
    fn uncovered_gaps_are_the_complement_of_the_live_tagged_ranges() {
        // Nothing tagged: the whole span is the gap.
        assert_eq!(uncovered_gaps(&[(DAY, DAY + 2 * HOUR)], &[]), vec![(DAY, DAY + 2 * HOUR)]);
        // One interior hole between two published slices.
        assert_eq!(uncovered_gaps(&[(DAY, DAY + 3 * HOUR)], &[(DAY, DAY + HOUR), (DAY + 2 * HOUR, DAY + 3 * HOUR)]), vec![(DAY + HOUR, DAY + 2 * HOUR)]);
        // Fully covered: the last range reaching exactly the exclusive end is enough.
        assert!(uncovered_gaps(&[(DAY, DAY + HOUR)], &[(DAY, DAY + HOUR)]).is_empty());
        // Ranges that overrun the span are clipped to it, out-of-order input is
        // fine, and two files sharing a hole report it once.
        assert_eq!(
            uncovered_gaps(&[(DAY + HOUR, DAY + 2 * HOUR), (DAY + HOUR, DAY + 2 * HOUR)], &[(DAY + 90 * 60_000_000, DAY + 9 * HOUR), (DAY, DAY + HOUR)]),
            vec![(DAY + HOUR, DAY + 90 * 60_000_000)]
        );
        // ADJACENT gaps are ONE hole: one unit per per-file statistics span would
        // shred the day into thousands of units.
        let minute = 60 * 1_000_000;
        assert_eq!(
            uncovered_gaps(&[(DAY, DAY + minute), (DAY + minute, DAY + 2 * minute), (DAY + 2 * minute, DAY + 3 * minute)], &[]),
            vec![(DAY, DAY + 3 * minute)],
            "three back-to-back holes are one hole"
        );
        assert_eq!(uncovered_gaps(&[(DAY, DAY + 2 * HOUR), (DAY + HOUR, DAY + 3 * HOUR)], &[]), vec![(DAY, DAY + 3 * HOUR)]);
        assert_eq!(
            uncovered_gaps(&[(DAY, DAY + HOUR), (DAY + 2 * HOUR, DAY + 3 * HOUR)], &[]),
            vec![(DAY, DAY + HOUR), (DAY + 2 * HOUR, DAY + 3 * HOUR)],
            "a real separation must NOT be merged away"
        );
    }

    /// `rebuild_slices` must align BEFORE merging, or a sub-minute separation
    /// survives the merge and aligns into two OVERLAPPING units.
    #[test]
    fn rebuild_slices_aligns_before_merging_so_a_sub_minute_separation_is_one_unit() {
        const DAY: i64 = 86_400_000_000;
        let floor = crate::maintenance_coordinator::MIN_SLICE_MICROS;
        let day_end = DAY + DAY;
        let near = vec![(DAY, DAY + floor / 3), (DAY + floor / 3 + 247_000, DAY + floor / 2)];
        assert_eq!(rebuild_slices(near.clone(), &[], DAY, day_end), vec![(DAY, DAY + floor)], "a 247ms separation is one minute of work");
        assert_eq!(crate::write::mem_buffer::merge_ranges(near).len(), 2);
        // A separation wider than the floor still yields two units, and a gap
        // contained by a live tagged slice is queued as that slice.
        assert_eq!(
            rebuild_slices(vec![(DAY, DAY + floor), (DAY + 3 * floor, DAY + 4 * floor)], &[], DAY, day_end),
            vec![(DAY, DAY + floor), (DAY + 3 * floor, DAY + 4 * floor)]
        );
        assert_eq!(rebuild_slices(vec![(DAY + floor, DAY + 2 * floor)], &[(DAY, DAY + 9 * floor)], DAY, day_end), vec![(DAY, DAY + 9 * floor)]);
        // Clamped to the partition: an overrunning gap must not select another
        // day's files.
        assert_eq!(rebuild_slices(vec![(day_end - floor, day_end + 5 * floor)], &[], DAY, day_end), vec![(day_end - floor, day_end)]);
    }

    #[test]
    fn hours_from_stats_json_bounds_the_hours_a_file_can_touch() {
        let day_start = chrono::NaiveDate::from_ymd_opt(2026, 8, 18).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
        let at = |hour: i64, minute: i64| day_start + hour * HOUR_MICROS + minute * 60 * 1_000_000;
        let bounds = |lo: i64, hi: i64| format!(r#"{{"minValues": {{"timestamp": {lo}}}, "maxValues": {{"timestamp": {hi}}}}}"#);

        // Epoch micros form: a file spanning 09:10..10:45 touches hours 9 and 10.
        let stats = format!(r#"{{"numRecords": 100, "minValues": {{"timestamp": {}}}, "maxValues": {{"timestamp": {}}}}}"#, at(9, 10), at(10, 45));
        assert_eq!(hours_from_stats_json(&stats, day_start), Some(0b11 << 9));

        // RFC 3339 form: a point file touches exactly one hour.
        let stats = r#"{"minValues": {"timestamp": "2026-08-18T03:00:00Z"}, "maxValues": {"timestamp": "2026-08-18T03:59:59.999999Z"}}"#;
        assert_eq!(hours_from_stats_json(stats, day_start), Some(1 << 3));

        // A whole-day file is honestly all hours.
        assert_eq!(hours_from_stats_json(&bounds(day_start, day_start + 24 * HOUR_MICROS - 1), day_start), Some(ALL_HOURS));

        // Bounds spilling outside the partition day are clamped, never trusted
        // into an empty mask.
        assert_eq!(hours_from_stats_json(&bounds(day_start - 5 * HOUR_MICROS, at(2, 0)), day_start), Some(0b111));
        let stats = bounds(day_start - 5 * HOUR_MICROS, day_start - HOUR_MICROS);
        assert_eq!(hours_from_stats_json(&stats, day_start), None, "a file wholly outside the day must read as unknown, not as nothing");

        // No stats, no timestamp bounds, garbage: all unknown.
        assert_eq!(hours_from_stats_json(r#"{"numRecords": 5}"#, day_start), None);
        assert_eq!(hours_from_stats_json("not json", day_start), None);
    }

    fn spec() -> RollupSpec {
        crate::schema::get_schema(SOURCE).expect("source schema").rollups.first().expect("declared rollup").clone()
    }

    /// APPEND-AND-MERGE EQUIVALENCE — the property the whole model rests on:
    /// partials over two disjoint halves, merged, must equal one rebuild over the
    /// union. A violation is silent, since both answers are well-formed.
    #[tokio::test]
    async fn partials_of_two_halves_merge_to_one_rebuild() {
        use arrow::array::Float64Array;
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("project_id", DataType::Utf8, false),
            // The aggregate SQL filters on the PARTITION columns, so a batch
            // handed to `rollup_partial_for_batches` must carry `date`.
            Field::new("date", DataType::Utf8, false),
            Field::new("svc", DataType::Utf8, true),
            Field::new("duration", DataType::Float64, true),
        ]));
        // Two services x two minutes, so the merge has to fold real groups.
        let rows = |from: i64, n: i64| -> RecordBatch {
            let times: Vec<i64> = (0..n).map(|i| (from + i % 2) * 60_000_000).collect();
            let svcs: Vec<&str> = (0..n).map(|i| if i % 3 == 0 { "api" } else { "web" }).collect();
            let durs: Vec<f64> = (0..n).map(|i| (i * 7 % 13) as f64).collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMicrosecondArray::from(times).with_timezone("UTC")),
                    Arc::new(StringArray::from(vec!["p"; n as usize])),
                    Arc::new(StringArray::from(vec!["1970-01-01"; n as usize])),
                    Arc::new(StringArray::from(svcs)),
                    Arc::new(Float64Array::from(durs)),
                ],
            )
            .expect("batch")
        };
        let spec = RollupSpec {
            grain: "1m".into(),
            name: Some("equiv".into()),
            dimensions: vec!["svc".into()],
            measures: vec![
                RollupMeasure { name: "c".into(), agg: "count".into(), column: None, filter: None },
                RollupMeasure { name: "s".into(), agg: "sum".into(), column: Some("duration".into()), filter: None },
                RollupMeasure { name: "lo".into(), agg: "min".into(), column: Some("duration".into()), filter: None },
                RollupMeasure { name: "hi".into(), agg: "max".into(), column: Some("duration".into()), filter: None },
            ],
            derive_from: None,
        };
        let ctx = datafusion::prelude::SessionContext::new();
        let (first, second) = (rows(0, 9), rows(0, 7));
        let window = (0, 10 * 60_000_000);
        let date = "1970-01-01";

        // APPEND side: each half aggregated on its own, exactly as a flush would.
        let mut appended = Vec::new();
        for half in [&first, &second] {
            appended.extend(rollup_partial_for_batches(&ctx, &spec, SOURCE, "p", date, std::slice::from_ref(half), window).await.expect("partial"));
        }
        assert!(appended.len() >= 2, "precondition: two independent partials, not one combined aggregate");

        // MERGE the appended partials the way the read path does.
        let states = format!("__states_{}", uuid::Uuid::new_v4().simple());
        let partial_schema = appended[0].schema();
        ctx.register_table(states.as_str(), Arc::new(datafusion::datasource::MemTable::try_new(partial_schema, vec![appended.clone()]).expect("states")))
            .expect("register states");
        let merged = ctx
            .sql(&format!("SELECT svc, SUM(c) AS c, SUM(s) AS s, MIN(lo) AS lo, MAX(hi) AS hi FROM {states} GROUP BY svc ORDER BY svc"))
            .await
            .expect("merge plan")
            .collect()
            .await
            .expect("merge");

        // REBUILD side: one aggregate over the union of the same rows.
        let raw = format!("__raw_{}", uuid::Uuid::new_v4().simple());
        ctx.register_table(raw.as_str(), Arc::new(datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![first, second]]).expect("raw")))
            .expect("register raw");
        let rebuilt = ctx
            .sql(&format!("SELECT svc, COUNT(*) AS c, SUM(duration) AS s, MIN(duration) AS lo, MAX(duration) AS hi FROM {raw} GROUP BY svc ORDER BY svc"))
            .await
            .expect("rebuild plan")
            .collect()
            .await
            .expect("rebuild");

        let show = |batches: &[RecordBatch]| arrow::util::pretty::pretty_format_batches(batches).expect("format").to_string();
        assert_eq!(show(&merged), show(&rebuilt), "appending partials and merging them must equal one rebuild over the same rows");
    }

    /// The two SQL arms `First` adds.
    #[test]
    fn first_picks_the_value_by_its_companion_on_both_legs() {
        let columns = ["landing_url".to_owned(), "landing_at".to_owned()];
        assert_eq!(
            Merge::First.partial_states(&columns),
            vec!["first_value(landing_url ORDER BY landing_at NULLS LAST)".to_owned(), "MIN(landing_at)".to_owned()],
            "the rollup leg picks the value BY the companion and minimises the companion itself"
        );
        let states = ["__s0_0".to_owned(), "__s0_1".to_owned()];
        assert_eq!(Merge::First.sql(&states), "first_value(__s0_0 ORDER BY __s0_1 NULLS LAST)");
        assert_eq!(Merge::First.arity(), columns.len());
    }

    /// A `first` measure and its companion: the first non-empty landing URL.
    fn first_spec() -> RollupSpec {
        let filter = Some("attributes___url___path <> ''".to_owned());
        RollupSpec {
            grain: "1m".into(),
            name: Some("first_test".into()),
            dimensions: vec!["kind".into()],
            measures: vec![
                RollupMeasure { name: "landing_url".into(), agg: "first".into(), column: Some("attributes___url___path".into()), filter: filter.clone() },
                RollupMeasure { name: "landing_at".into(), agg: "min".into(), column: Some("timestamp".into()), filter },
            ],
            derive_from: None,
        }
    }

    /// Value and companion must be selected by the same FILTER, or they describe
    /// different rows and the merge picks a value that was never first.
    #[test]
    fn a_first_measure_builds_value_and_companion_over_the_same_rows() {
        let sql = build_partition_sql(&first_spec(), SOURCE, "p", "2026-01-15").expect("builds");
        assert!(sql.contains("first_value(attributes___url___path ORDER BY timestamp) FILTER (WHERE attributes___url___path <> '') AS landing_url"), "{sql}");
        assert!(sql.contains("MIN(timestamp) FILTER (WHERE attributes___url___path <> '') AS landing_at"), "{sql}");
    }

    /// The coarse tier orders by the COMPANION with NULLS LAST, never by the base
    /// tier's bucket: a fine bucket that matched nothing stores NULLs and would
    /// otherwise win whenever it is the earliest.
    #[test]
    fn a_derived_first_orders_by_the_companion_with_nulls_last() {
        let mut coarse = first_spec();
        coarse.grain = "1h".into();
        coarse.name = Some("first_test_1h".into());
        coarse.derive_from = Some("first_test".into());
        let sql = build_partition_sql_from(&coarse, SOURCE, "src_rollup_first_test", "p", "2026-01-15").expect("builds");

        assert!(sql.contains("first_value(landing_url ORDER BY landing_at NULLS LAST) AS landing_url"), "{sql}");
        assert!(sql.contains("MIN(landing_at) AS landing_at"), "{sql}");
        // Ordering by the bucket is the bug this test exists to prevent.
        assert!(!sql.contains("ORDER BY timestamp"), "a derived first must not order by the base tier's bucket: {sql}");
    }

    /// `first_value` is the one aggregate allowed to carry an ORDER BY, and only
    /// by `timestamp` ascending.
    #[tokio::test]
    async fn only_first_value_ordered_by_timestamp_passes_the_order_by_gate() {
        let state = session().await;
        let route = async |order: &str| {
            let sql = format!(
                "SELECT first_value(name ORDER BY {order}) FROM {SOURCE} \
                 WHERE project_id = 'p' AND timestamp >= to_timestamp_micros(1786500000000000) GROUP BY kind"
            );
            route_for(&state, &sql).await
        };

        assert!(
            matches!(route("timestamp").await, Err(MissReason::MissingMeasure)),
            "ordering by timestamp must pass the gate and decline only for want of a measure"
        );
        for refused in ["duration", "timestamp DESC"] {
            assert!(matches!(route(refused).await, Err(MissReason::NonDecomposableAggregate)), "`ORDER BY {refused}` must still be refused outright");
        }
    }

    /// An open-ended window must route, and the trailing fringe must stay open
    /// or the rewrite drops the newest rows.
    #[tokio::test]
    async fn an_open_ended_window_routes_and_keeps_an_open_raw_tail() {
        let state = session().await;
        let sql = format!(
            "SELECT count(*) FROM {SOURCE} WHERE project_id = 'p' AND timestamp >= to_timestamp_micros(1786500000000000) GROUP BY resource___service___name"
        );
        let route = route_for(&state, &sql).await;
        assert!(route.is_ok(), "an open-ended window must route: {route:?}");
        let generated = hybrid_sql(&route.expect("route").expect("a route"), crate::support::now_micros());
        let tail = generated.rsplit("timestamp >=").next().expect("a trailing range");
        assert!(!tail.contains("timestamp <"), "the trailing raw range must stay open-ended, got: {generated}");
    }

    /// The interior and its complement must cover the window exactly once out to
    /// `OPEN_END`: a gap drops rows, an overlap doubles them, undetectably.
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

    /// A `text_match` hint whose own predicate was consumed as a DIMENSION filter
    /// must not be left behind in the promotable set.
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

    /// The IN-list spelling: `tantivy_rewriter` expands `kind IN (a, b)` into an
    /// OR of per-item `text_match` calls, so the leftover hint is an `Or` tree,
    /// invisible to a stripper that only looks at the top of each conjunct.
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

    /// `tantivy_rewriter` additively ANDs `text_match` hints beside a predicate it
    /// can accelerate, and not the same ones on both sides — so hint arity must
    /// not decide whether a panel routes.
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

    /// `AND` is idempotent, so a conjunct the optimizer leaves on both the Filter
    /// node and the TableScan's `partial_filters` must canonicalize once.
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
    #[test]
    fn a_merge_on_read_input_is_deduped_before_the_rollup_aggregate() {
        let base = crate::schema::get_schema("otel_logs_and_spans_rollup_dashboard_1m_v3").expect("the 1m tier is a declared rollup target");
        // `rollup_tier_dedup` exists because the maintenance read registers the
        // tier directly rather than through the routing table, so it cannot rely
        // on the planner having inserted a `DedupExec`.
        assert_eq!(base.dedup_keys, ["timestamp", "id"], "a rollup tier must declare its identity so reads collapse versions");
        let (keys, tiebreak, tombstone) = rollup_tier_dedup(base).expect("a generated tier carries timestamp/id/updated_at");

        let dedup = SliceDedup { keys: &keys, tiebreak: Some(tiebreak), tombstone };
        let sql = slice_input_sql(base, Some(dedup), "__raw", "p", (0, 60_000_000), "", None);
        assert!(sql.contains("ROW_NUMBER() OVER (PARTITION BY"), "a merge-on-read input must be collapsed to one row per key, got: {sql}");
        assert!(sql.contains("__tf_rn = 1"), "the collapse must keep exactly one version per key, got: {sql}");
        assert!(
            sql.contains("\"updated_at\" DESC NULLS LAST"),
            "it must keep the GREATEST version — keeping an arbitrary one is a different wrong answer, got: {sql}"
        );

        // No dedup means the aggregate above sums every version; kept as contrast.
        let undeduped = slice_input_sql(base, None, "__raw", "p", (0, 60_000_000), "", None);
        assert!(!undeduped.contains("__tf_rn"), "no dedup means a bare SELECT; this is the shape that over-counted");
    }

    /// A measure the SPEC declares but the physical table lacks must project
    /// NULL, not fail the unit — adding a measure to a spec does not evolve the
    /// existing rollup table, and no file holds a value for it anyway.
    #[test]
    fn a_measure_the_physical_table_lacks_projects_null_instead_of_failing() {
        let base = crate::schema::get_schema("otel_logs_and_spans_rollup_dashboard_1m_v3").expect("the 1m tier is a declared rollup target");
        let (keys, tiebreak, tombstone) = rollup_tier_dedup(base).expect("a generated tier carries timestamp/id/updated_at");
        let dedup = || SliceDedup { keys: &keys, tiebreak: Some(tiebreak), tombstone };
        // Everything except the measure added later.
        let present: std::collections::HashSet<String> = base.fields.iter().map(|field| field.name.clone()).filter(|name| name != "duration_digest").collect();
        assert!(base.fields.iter().any(|field| field.name == "duration_digest"), "precondition: the spec declares it");

        let sql = slice_input_sql(base, Some(dedup()), "__raw", "p", (0, 60_000_000), "", Some(&present));
        assert!(sql.contains("NULL AS \"duration_digest\""), "the absent measure must be projected NULL, got: {sql}");
        assert!(sql.contains("\"server_duration_digest\", ") || sql.contains(", \"server_duration_digest\""), "present columns must still be read, got: {sql}");
        assert!(!sql.contains("NULL AS \"server_duration_digest\""), "a column the table HAS must not be nulled, got: {sql}");
        // The outer projection names it, so downstream measure SQL resolves.
        assert!(sql.matches("\"duration_digest\"").count() >= 2, "the outer select must expose it too, got: {sql}");

        // Same for the undeduped path — `SELECT *` returns only physical columns,
        // so it cannot stand in once anything is missing.
        let undeduped = slice_input_sql(base, None, "__raw", "p", (0, 60_000_000), "", Some(&present));
        assert!(undeduped.contains("NULL AS \"duration_digest\""), "the bare-select path must project too, got: {undeduped}");

        // And when nothing is missing, the shape is unchanged.
        let all: std::collections::HashSet<String> = base.fields.iter().map(|field| field.name.clone()).collect();
        assert!(!slice_input_sql(base, None, "__raw", "p", (0, 60_000_000), "", Some(&all)).contains("NULL AS"));
        assert!(!slice_input_sql(base, Some(dedup()), "__raw", "p", (0, 60_000_000), "", Some(&all)).contains("NULL AS"));
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
        // Both tiers get the SAME identity, base and derived alike.
        assert_eq!(target.dedup_keys, ["timestamp", "id"], "every rollup tier declares the same identity");
        assert_eq!(target.dedup_tiebreak.as_deref(), Some("updated_at"), "keep-greatest needs the tiebreak");
    }

    #[test]
    fn slice_generation_is_stable_across_source_fingerprints() {
        let spec = spec();
        assert_eq!(generation_id(&spec, SOURCE, "p", "2026-08-15", 1, None), generation_id(&spec, SOURCE, "p", "2026-08-15", 2, None));
    }

    /// A cell built under `before` carries the whole-spec generation id; recovery
    /// recomputes it from `after` restricted to what the cell says it holds, so
    /// adding a measure must not orphan it while redefining one must.
    #[test]
    fn adding_a_measure_keeps_older_cells_current_and_redefining_one_does_not() {
        let mut before = spec();
        before.measures.pop().expect("the spec declares measures");
        let held: Vec<String> = before.measures.iter().map(|measure| measure.name.clone()).collect();
        let after = spec();
        assert_eq!(
            generation_id(&before, SOURCE, "p", "2026-08-15", 1, None),
            generation_id(&after, SOURCE, "p", "2026-08-15", 1, Some(&held)),
            "a cell built before the measure existed must stay current"
        );

        let mut redefined = after.clone();
        redefined.measures[0].filter = Some("status_code = 500".to_owned());
        assert_ne!(
            generation_id(&after, SOURCE, "p", "2026-08-15", 1, Some(&held)),
            generation_id(&redefined, SOURCE, "p", "2026-08-15", 1, Some(&held)),
            "redefining a measure the cell HOLDS must still orphan it"
        );
        let mut regrouped = after.clone();
        regrouped.dimensions.push("name".to_owned());
        assert_ne!(
            generation_id(&after, SOURCE, "p", "2026-08-15", 1, Some(&held)),
            generation_id(&regrouped, SOURCE, "p", "2026-08-15", 1, Some(&held)),
            "a different GROUP BY is a different row set, whatever the cell holds"
        );
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
    /// Getting the second wrong is silent: `SUM` over a Binary column is the
    /// default branch.
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

    /// `hll_agg` folds to the merged STATE; the `distinct_count` accessor stays in
    /// the projection above, as `approx_percentile` does over `percentile_agg`.
    #[test]
    fn the_hll_merge_folds_states_and_leaves_the_accessor_alone() {
        assert_eq!(Merge::Hll.arity(), 1);
        assert_eq!(Merge::Hll.partial_op(), "hll_merge");
        assert_eq!(Merge::Hll.sql(&["__s0_0".to_string()]), "hll_merge(__s0_0)");
    }

    const TARGET: &str = "otel_logs_and_spans_rollup_dashboard_1m_v3";
    /// Ten grains wide: a window narrower than `MIN_INTERIOR_BUCKETS` grains can
    /// never route, so it would exercise the rejection path, not the matcher.
    const WINDOW: &str = "timestamp >= to_timestamp_micros(60000000) AND timestamp < to_timestamp_micros(660000000)";
    const WIDE_HORIZON: i64 = 540_000_000;
    /// Exactly what the `server_*` measures declare.
    const SERVER: &str = "(kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http')";

    /// A session with the custom functions plus an empty MemTable per named schema.
    fn session_over(tables: impl IntoIterator<Item = String>) -> datafusion::execution::context::SessionState {
        let mut ctx = datafusion::prelude::SessionContext::new();
        crate::read::functions::register_custom_functions(&mut ctx).expect("functions register");
        for table_name in tables {
            let table = datafusion::datasource::MemTable::try_new(crate::schema::get_schema(&table_name).expect("schema").schema_ref(), vec![vec![]])
                .expect("empty table");
            ctx.register_table(table_name.as_str(), Arc::new(table)).expect("register table");
        }
        ctx.state()
    }

    /// Register the source AND its generated rollup so a route can be planned
    /// back into a real logical plan.
    async fn session() -> datafusion::execution::context::SessionState {
        // Every declared tier: specs are tried coarsest-first, so a session
        // missing the coarse table could not plan a rewrite the matcher chose.
        let targets = crate::schema::get_schema(SOURCE).expect("source schema").rollups.iter().map(|spec| spec.table_name(SOURCE)).collect::<Vec<_>>();
        session_over(std::iter::once(SOURCE.to_string()).chain(targets))
    }

    async fn optimized(state: &datafusion::execution::context::SessionState, sql: &str) -> datafusion::logical_expr::LogicalPlan {
        state.optimize(&state.create_logical_plan(sql).await.expect("parse")).expect("optimize")
    }

    async fn route_for(state: &datafusion::execution::context::SessionState, sql: &str) -> Result<Option<RoutedRollup>, MissReason> {
        match_aggregates(&optimized(state, sql).await, state).await.map(|routes| routes.into_iter().next())
    }

    /// For tests whose only interest is HOW a shape declines.
    async fn route_alone(sql: &str) -> Result<Option<RoutedRollup>, MissReason> {
        route_for(&session().await, sql).await
    }

    /// Route `sql`, then reassemble with `dml.rs`'s own two functions and assert
    /// the result still describes the query's own schema.
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

    /// Route `sql` — which must route — and return that single-leg rewrite.
    async fn routed_sql(state: &datafusion::execution::context::SessionState, sql: &str) -> String {
        generated_sql(&route_for(state, sql).await.expect("match").expect("declared rollup route"))
    }

    /// `sql` must route, every `want` must reach the single-leg rewrite verbatim,
    /// and the rewrite must reassemble into the query's own schema.
    async fn assert_rewrite_contains(sql: &str, wants: &[&str]) {
        let state = session().await;
        let (_, generated) = assert_substitutes(&state, sql, None).await;
        for want in wants {
            assert!(generated.contains(want), "`{want}` must reach the rewrite: {generated}");
        }
    }

    /// The rewrite when only part of the window is certified, so raw fringes and
    /// a live tail are unioned in.
    fn hybrid_sql(route: &RoutedRollup, horizon: i64) -> String {
        let interior = interior(route.lo, route.hi, route.grain, horizon).expect("a routable interior");
        route.sql(&[("project".into(), "1970-01-01".into(), "generation".into())], &[interior], &ProjectSplit::default())
    }

    /// An unfiltered percentile must read the unfiltered digest, never the
    /// `server`-filtered one. Cells predating a measure stay readable and are
    /// refused per query by `measures_available`.
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

        // A cell may hold the current generation and still not hold this column,
        // so the tag is the only proof; a cell that cannot show it is refused.
        assert!(!route.measures_available(None), "a legacy cell proves no digest and must not serve a percentile");
        assert!(
            !route.measures_available(Some(&["request_count".to_owned()].into_iter().collect())),
            "a cell that materialized other measures still cannot serve the digest"
        );
        assert!(
            route.measures_available(Some(&route.needed_measure_columns().map(str::to_owned).collect())),
            "a cell that materialized every needed column must serve it"
        );
    }

    /// What a derived build TAGS, joined to what the read gate then DOES with
    /// it: a base tier that cannot prove the digest must produce a derived cell
    /// the percentile is refused for, while the count chart keeps routing.
    #[tokio::test]
    async fn a_derived_tag_over_a_digestless_base_is_refused_for_the_percentile() {
        let source = crate::schema::get_schema(SOURCE).expect("source schema");
        let spec = source.rollups.iter().find(|spec| spec.derive_from.is_some()).expect("a derived tier");
        let declared = crate::schema::get_schema(&spec.table_name(SOURCE)).expect("tier schema");
        // Every column exists on both sides — only the base cells' own evidence
        // can refuse anything, which is the whole point.
        let present: std::collections::HashSet<String> = declared.fields.iter().map(|field| field.name.clone()).collect();
        let digestless: std::collections::HashSet<String> =
            spec.measures.iter().map(|measure| measure.name.clone()).filter(|name| name != "duration_digest").collect();
        let evidence = base_measure_evidence(spec, [Some(&present), Some(&digestless), None].into_iter());
        let tagged: std::collections::HashSet<String> =
            materialized_measures(spec, true, &present, declared.schema_ref().as_ref(), evidence.as_ref()).into_iter().collect();
        assert!(!tagged.contains("duration_digest"), "one base cell without the digest is enough to refuse it: {tagged:?}");
        assert!(tagged.contains("request_count"), "a measure every base cell proved must survive: {tagged:?}");

        let state = session().await;
        let percentile = format!(
            "SELECT time_bucket('1 hours', timestamp) AS tb, percentile_agg(CAST(duration AS DOUBLE PRECISION)) \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1"
        );
        let count = format!("SELECT time_bucket('1 hours', timestamp) AS tb, COUNT(*) AS c FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1");
        let route = async |sql: &str| route_for(&state, sql).await.expect("match").expect("declared rollup route");
        assert!(!route(&percentile).await.measures_available(Some(&tagged)), "the gate must refuse the percentile rather than answer it from an empty state");
        assert!(route(&count).await.measures_available(Some(&tagged)), "the same cell must keep serving the measures it did materialize");
    }

    /// The other half of the same rule: a legacy cell is not refused wholesale,
    /// or every count chart goes back to a raw scan.
    #[tokio::test]
    async fn a_legacy_cell_still_serves_a_count_chart() {
        let state = session().await;
        let sql = format!("SELECT time_bucket('1 hours', timestamp) AS tb, COUNT(*) AS c FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1");
        let route = route_for(&state, &sql).await.expect("match count").expect("declared rollup route");
        assert!(route.measures_available(None), "a count chart must keep routing off a cell written before TAG_MEASURES");
    }

    /// Shapes that must route, whose every `want` must reach the single-leg
    /// rewrite verbatim and whose rewrite must reassemble into the query's own
    /// schema.
    // Grouped charts emit `COALESCE(<dimension>, 'null')`, never the bare column, and group by bucket AND dimension together.
    #[test_case::test_case(
        &format!(
            "SELECT time_bucket('1 hours', timestamp) AS tb, COALESCE(resource___service___name, 'null') AS svc, COUNT(*) \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1, 2"
        ),
        &["COALESCE(resource___service___name, 'null')"] ; "a grouped chart coalescing its dimension routes")]
    // A bare-column coalesce is left alone by CSE; the `::text` cast repeats a COMPUTATION and gets lifted into `__common_expr_1`.
    #[test_case::test_case(
        &format!(
            "SELECT time_bucket('1 hours', timestamp) AS tb, COALESCE(status_code::text, 'null') AS sc, COUNT(*) \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1, 2"
        ),
        &["COALESCE(status_code, 'null')"] ; "a grouped chart casting its dimension routes despite cse")]
    // A real log-explorer count chart verbatim: `extract(epoch …)` over the bucket, the `::text` coalesce, `count(*)::float`.
    #[test_case::test_case(
        &format!(
            "SELECT extract(epoch from time_bucket('1 hours', timestamp))::integer, COALESCE(status_code::text, 'null'), count(*)::float AS count_ \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY time_bucket('1 hours', timestamp), COALESCE(status_code::text, 'null')"
        ),
        &["COALESCE(status_code, 'null')"] ; "the log explorer count chart routes verbatim")]
    // A bare `count(*)` reads Delta statistics, so the benchmark spells it `count(1) FROM (SELECT id …) t`; the walker must descend that `SubqueryAlias`.
    #[test_case::test_case(
        &format!("SELECT count(1) FROM (SELECT id FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}) t"),
        &["request_count"] ; "the benchmark count shape routes through its derived table")]
    // The qualifier proof: the aggregate groups by `t.status_code` while the rewrite can only produce an unqualified one, so a rewrite that did not
    // carry the `t` qualifier back fails to resolve rather than answering — the reassembly IS the assertion.
    #[test_case::test_case(
        &format!(
            "SELECT time_bucket('1 hours', t.timestamp) AS tb, t.status_code, count(*)::float AS count_ \
             FROM (SELECT timestamp, status_code, project_id FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}) t \
             GROUP BY 1, 2"
        ),
        &["status_code"] ; "a grouped derived table keeps its alias qualifier through the rewrite")]
    // The epoch wrapper AND its integer cast must survive, or the substituted rewrite will not match the aggregate's schema types.
    #[test_case::test_case(
        &format!(
            "SELECT extract(epoch from time_bucket('1 hours', timestamp))::integer AS tb, COUNT(*) \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1"
        ),
        &["date_part('EPOCH', time_bucket('1 hours', timestamp))", "CAST(date_part('EPOCH'"] ; "a chart grouping by extract epoch of a bucket routes")]
    #[test_case::test_case(
        &format!("SELECT COUNT(*) + 1 AS total FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}"),
        &[] ; "a scalar projection above the aggregate survives untouched")]
    // The promotion to a pre-filtered measure must carry a HAVING, or a bucket where nothing matched comes back as a 0 row instead of being absent.
    #[test_case::test_case(
        &format!(
            "SELECT time_bucket('1 hours', timestamp) AS bucket, COUNT(*) AS c \
             FROM {SOURCE} WHERE project_id = 'project' AND {SERVER} AND {WINDOW} GROUP BY 1 ORDER BY 1 DESC"
        ),
        &["server_request_count", "HAVING"] ; "a row filter that matches a declared measure filter routes with a having")]
    // The aggregate's own FILTER and the promoted row filter combine into exactly what `server_error_count` declares.
    #[test_case::test_case(
        &format!(
            "SELECT time_bucket('1 hours', timestamp) AS bucket, \
                    COUNT(*) FILTER (WHERE status_code = 'ERROR' OR COALESCE(attributes___http___response___status_code, 0) >= 500) AS errors \
             FROM {SOURCE} WHERE project_id = 'project' AND {SERVER} AND {WINDOW} GROUP BY 1"
        ),
        &["server_error_count"] ; "an aggregate filter combines with the promoted row filter")]
    // An aggregate inside a CTE is reachable because the matcher searches the tree instead of peeling the root.
    #[test_case::test_case(
        &format!(
            "WITH bucketed AS (SELECT time_bucket('1 hours', timestamp) AS t, avg(duration) AS mean \
                               FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1) \
             SELECT t, mean FROM bucketed ORDER BY 1 DESC"
        ),
        &[] ; "an aggregate inside a cte is reachable")]
    #[tokio::test]
    async fn a_dashboard_shape_routes_and_its_rewrite_reassembles(sql: &str, wants: &[&str]) {
        assert_rewrite_contains(sql, wants).await;
    }

    /// A measure on the not-yet-servable list is refused on a TAGGED cell, not
    /// merely an untagged one. The tag proves the COLUMN existed; it cannot prove
    /// a VALUE was written, and `distinct_count` of an empty sketch is 0 — so a
    /// merge over empty states answers with a number instead of declining.
    #[tokio::test]
    async fn a_not_yet_servable_measure_is_refused_even_when_the_cell_tags_it() {
        let state = session().await;
        let sql = format!(
            "SELECT time_bucket('1 hours', timestamp) AS tb, distinct_count(approx_count_distinct(resource___service___name)) \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1"
        );
        // Required, not optional: an early return would make the assertions below
        // vacuous.
        let route = route_for(&state, &sql).await.expect("match dcount").expect("the hll measure is declared, so it must route");
        let tagged: std::collections::HashSet<String> = ["service_name_hll".to_owned()].into_iter().collect();
        assert!(!route.measures_available(Some(&tagged)), "a TAGGED cell must not serve a measure whose stored state is known empty");
        assert!(!route.measures_available(None), "and an untagged cell must not either");
    }

    /// A group expression the matcher cannot serve must DECLINE, not vanish.
    /// `coalesce(status_code, level)` needs `level`, which no spec declares. The
    /// distinction under test is `Err(_)` versus a silent `Ok(None)`.
    #[tokio::test]
    async fn an_unservable_group_expression_is_counted_rather_than_silent() {
        let sql = format!(
            "SELECT time_bucket('1 hours', timestamp) AS tb, COALESCE(coalesce(status_code, level)::text, 'null') AS sc, COUNT(*) \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1, 2"
        );
        assert!(route_alone(&sql).await.is_err(), "an unservable group-by must report a reason, not fall through silently");
    }

    #[test_case::test_case("status_code = 'pickup_accepted'", true)]
    #[test_case::test_case("status_code IS NOT NULL", true)]
    #[test_case::test_case("status_code IS NULL", false)]
    #[test_case::test_case("status_code = 'pickup_accepted' OR status_code IS NULL", false)]
    #[tokio::test]
    async fn filtered_chart_only_drops_an_unreachable_level_fallback(predicate: &str, routes: bool) {
        let state = session().await;
        let sql = format!(
            "SELECT extract(epoch from time_bucket('2 hours', timestamp))::integer, \
            COALESCE(coalesce(status_code, level)::text, 'null'), count(*)::float AS count_ \
            FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} AND ({predicate}) \
            GROUP BY time_bucket('2 hours', timestamp), COALESCE(coalesce(status_code, level)::text, 'null')"
        );
        let route = route_for(&state, &sql).await;
        assert_eq!(matches!(route, Ok(Some(_))), routes, "{route:?}");
        if routes {
            assert_substitutes(&state, &sql, None).await;
        }
    }

    /// The shapes that make `source_and_filters` refuse. Each case asserts both
    /// that the miss is counted (it leaves through `Ok(Vec::new())`, so a counter
    /// is the only evidence) and that the refusal NAMES the node that stopped the
    /// walk.
    ///
    /// Deliberately absent: the `SELECT *` variant-wrap projection, which needs
    /// the Variant analyzer rule a bare `MemTable` session does not register.
    #[test_case::test_case(
        &format!("SELECT count(*) FROM (SELECT 1 FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} LIMIT 5000) s"),
        "Limit"; "a bounded existence probe over a derived table — monoscope safetyNetReprocess, BackgroundJobs.hs:2127")]
    #[test_case::test_case(
        &format!("WITH f AS (SELECT status_code AS sc, timestamp FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}) SELECT sc, count(*) FROM f GROUP BY sc"),
        "Projection"; "a CTE that RENAMES the dimension the outer aggregate groups by")]
    #[test_case::test_case(
        &format!(
            "WITH f AS (SELECT floor(extract(epoch from timestamp) / 60)::bigint AS bucket_idx FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}) \
             SELECT bucket_idx, count(*) FROM f GROUP BY bucket_idx"
        ),
        "Projection: CAST(floor"; "a CTE computing the bucket — monoscope endpointRequestStatsByProject and rollupServiceEdges")]
    #[test_case::test_case(
        &format!(
            "SELECT count(*) FROM (SELECT timestamp FROM {SOURCE} WHERE project_id = 'a' AND {WINDOW} \
             UNION ALL SELECT timestamp FROM {SOURCE} WHERE project_id = 'b' AND {WINDOW}) u"
        ),
        "Union"; "a UNION ALL of two scans — monoscope rollupServiceEdges hops")]
    #[test_case::test_case(
        &format!("SELECT count(DISTINCT status_code) FROM (SELECT DISTINCT status_code FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}) d"),
        "Aggregate"; "an inner DISTINCT, which plans as a second Aggregate")]
    #[tokio::test]
    async fn an_unwalkable_shape_is_counted_and_names_the_node_that_refused(sql: &str, expected_node: &str) {
        let state = session().await;
        let counter = &crate::observability::maintenance_stats().rollup_miss_unwalkable_source;
        let before = counter.load(std::sync::atomic::Ordering::Relaxed);
        let plan = optimized(&state, sql).await;

        assert_eq!(
            match_aggregates(&plan, &state).await.expect("an unwalkable plan declines, it does not error").len(),
            0,
            "an unwalkable plan must not route"
        );
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed) - before,
            1,
            "the miss must be counted exactly once — it leaves through Ok(Vec::new()), so nothing else records it: {}",
            plan.display_indent()
        );

        // Read off the same walk the matcher runs, so a test-only re-derivation
        // cannot drift from the log line.
        let Some(datafusion::logical_expr::LogicalPlan::Aggregate(aggregate)) = outermost_aggregate(&plan) else { panic!("an aggregate") };
        let refused = source_and_filters(aggregate.input.as_ref(), &mut Vec::new()).expect_err("the walk must refuse");
        assert!(refused.contains(expected_node), "the refusal must name the node that stopped the walk, got {refused:?} for {}", plan.display_indent());
    }

    /// Single-leg rewrites, checked for the states they must read and the ones
    /// they must not.
    // Under `col IS NOT NULL` — consumed above rather than pushed, so neither leg
    // re-applies it — `count(*)` is exactly `count(col)`, which `duration_count`
    // declares; `request_count` counts null-duration rows the guard excluded.
    #[test_case::test_case(
        &format!(
            "SELECT time_bucket('1 hours', timestamp) AS tb, COUNT(*) AS c \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} AND duration IS NOT NULL GROUP BY 1"
        ),
        &["duration_count"], &["request_count"] ; "a guarded count resolves to the guard column")]
    #[test_case::test_case(
        &format!("SELECT COUNT(*) FROM {SOURCE} WHERE project_id = 'project' AND kind = 'server' AND {WINDOW}"),
        &["AND (kind = 'server')"], &[] ; "matcher applies dimension predicates to the rollup scan")]
    // An `avg` over two Int64 measures divides as integers unless it casts first, and the CASE's DOUBLE only widens an already-truncated value.
    #[test_case::test_case(
        &format!("SELECT avg(duration) FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}"),
        &["CAST(SUM(duration_sum) AS DOUBLE) / CAST(SUM(duration_count) AS DOUBLE)"], &[] ; "avg casts before dividing so it does not truncate")]
    #[tokio::test]
    async fn the_single_leg_rewrite_reads_the_states_the_shape_requires(sql: &str, wants: &[&str], absent: &[&str]) {
        let generated = routed_sql(&session().await, sql).await;
        for want in wants {
            assert!(generated.contains(want), "`{want}` must reach the rewrite: {generated}");
        }
        for unwanted in absent {
            assert!(!generated.contains(unwanted), "`{unwanted}` must not reach the rewrite: {generated}");
        }
    }

    /// A guarded count beside a percentile: the match level resolves both states
    /// and the per-slice measure gate decides where they may be read from.
    #[tokio::test]
    async fn a_guarded_count_beside_a_percentile_resolves_both_states() {
        let state = session().await;
        let sql = format!(
            "SELECT time_bucket('1 hours', timestamp) AS tb, percentile_agg(CAST(duration AS DOUBLE PRECISION)) AS digest \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} AND duration IS NOT NULL GROUP BY 1 HAVING COUNT(*) > 0"
        );
        let (_, generated) = assert_substitutes(&state, &sql, None).await;
        assert!(generated.contains("duration_digest"), "the percentile must read the digest state: {generated}");
        assert!(generated.contains("duration_count"), "the guarded count(*) must resolve to count(duration): {generated}");
        assert!(!generated.contains("request_count"), "request_count counts null-duration rows the guard excluded: {generated}");
    }

    /// Shapes that must decline with exactly the reason named — the reason IS the
    /// `rollup_misses` label a dashboard is diagnosed from, so a wrong one is a
    /// silent misdiagnosis.
    // A spec disqualified by GRAIN ALONE must not report the miss, or the 1h tier's complaint masks the 1m tier's real reason. The window is day-wide on
    // purpose: the 1h tier must be disqualified by the BUCKET WIDTH (`PartialBucket`), not by a window too narrow to hold two of its grains.
    #[test_case::test_case(
        &format!(
            "SELECT time_bucket('30 minutes', timestamp) AS tb, status_message, COUNT(*) FROM {SOURCE} \
             WHERE project_id = 'project' AND timestamp >= to_timestamp_micros(0) AND timestamp < to_timestamp_micros(86400000000) GROUP BY 1, 2"
        ),
        MissReason::UnknownGroupBy ; "a sub hour bucket reports the group by not the grain")]
    // No timestamp truncates to an unaligned instant: inventing `[X, X+width)` would serve an hour of rows for a query that must return none.
    #[test_case::test_case(
        &format!(
            "SELECT project_id, COUNT(*) FROM {SOURCE} \
             WHERE timestamp >= to_timestamp_micros(0) AND timestamp < to_timestamp_micros(86400000000) \
             AND date_trunc('hour', timestamp) = to_timestamp_micros(90061000000) GROUP BY 1"
        ),
        MissReason::UnknownFilter ; "a date trunc equality against an unaligned literal is refused")]
    // Grouping by project_id is what makes a filterless query answerable; without a group key the rewrite would fold every project into one row.
    #[test_case::test_case(&format!("SELECT COUNT(*) FROM {SOURCE} WHERE {WINDOW}"), MissReason::MissingProject
        ; "a query with neither a project filter nor a project group is refused")]
    // `status_message` appears in no declared measure filter: the *not eligible* half of the split, which nothing could ever have answered.
    #[test_case::test_case(
        &format!(
            "SELECT COUNT(*) AS c FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} AND status_message = 'nope' \
             GROUP BY time_bucket('1 hours', timestamp)"
        ),
        MissReason::FilterNotEligible ; "a residual filter with no declared measure still refuses")]
    // The other half: a drifted panel whose residual constrains a column a declared measure DOES filter on is a near-miss worth its own counter.
    #[test_case::test_case(
        &format!(
            "SELECT COUNT(*) AS c FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} AND {SERVER} AND status_message = 'nope' \
             GROUP BY time_bucket('1 hours', timestamp)"
        ),
        MissReason::UnknownFilter ; "a near miss on a declared column is distinguished from an ineligible one")]
    // `name` is not a declared dimension, so it can only *select* a pre-filtered measure — and groups the raw query eliminates would come back as 0/NULL rows.
    #[test_case::test_case(
        &format!("SELECT COUNT(*) FROM {SOURCE} WHERE project_id = 'project' AND name = 'monoscope.http' AND {WINDOW}"),
        MissReason::UnknownFilter ; "a residual row filter refuses the route rather than inventing zero rows")]
    #[tokio::test]
    async fn a_shape_the_matcher_refuses_names_its_reason(sql: &str, reason: MissReason) {
        let miss = route_alone(sql).await.err();
        assert_eq!(miss, Some(reason), "expected {reason:?}, got {miss:?} for: {sql}");
    }

    /// Shapes that must decline rather than route to a column the matcher cannot
    /// prove is there. Declining is the invariant; WHICH reason each declines
    /// with is deliberately not.
    // `kind AS status_code` walked through would read the declared `status_code` dimension off `kind` and answer the wrong rows silently.
    #[test_case::test_case(
        &format!(
            "SELECT t.status_code, count(*) \
             FROM (SELECT kind AS status_code, timestamp, project_id FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}) t GROUP BY 1"
        ) ; "a derived table that renames a column still declines")]
    // The coalesce arm is deliberately narrow.
    #[test_case::test_case(
        &format!("SELECT COALESCE(status_message, 'null') AS g, COUNT(*) FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1")
        ; "a column that is not a declared dimension")]
    #[test_case::test_case(
        &format!("SELECT COALESCE(resource___service___name, name, 'null') AS g, COUNT(*) FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1")
        ; "three-argument coalesce")]
    #[test_case::test_case(
        &format!(
            "SELECT COALESCE(CONCAT(resource___service___name, 'x'), 'null') AS g, COUNT(*) FROM {SOURCE} \
             WHERE project_id = 'project' AND {WINDOW} GROUP BY 1"
        ) ; "coalesce over an expression, not a column")]
    // `extract(epoch …)` is accepted because it is 1:1 over buckets; every other field is many-to-one and would merge groups the raw path keeps apart.
    #[test_case::test_case(
        &format!(
            "SELECT extract(hour from time_bucket('1 hours', timestamp))::integer AS tb, COUNT(*) \
             FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} GROUP BY 1"
        ) ; "extracting any field but epoch from a bucket still declines")]
    #[tokio::test]
    async fn a_shape_the_matcher_cannot_prove_still_declines(sql: &str) {
        let route = route_alone(sql).await;
        assert!(matches!(route, Err(_) | Ok(None)), "must not route, got {route:?} for: {sql}");
    }

    #[tokio::test]
    async fn matcher_rewrites_a_certifiable_count_aggregate() {
        let state = session().await;
        let route = route_for(&state, &format!("SELECT COUNT(*) FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW}"))
            .await
            .expect("match count")
            .expect("declared rollup route");
        assert!(route.target.contains("_1m_"), "a minute-bucketed count must route to a 1m tier, got {}", route.target);
        assert_eq!(route.project_id.as_deref(), Some("project"));
        assert!(generated_sql(&route).contains("COALESCE(SUM(request_count), 0)"));
    }

    /// A cross-project overview GROUPS BY project_id instead of filtering on it.
    /// `project_id` is a real column on the rollup table (`to_rollup_batches`
    /// writes it), so grouping by it is answerable without an equality literal.
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

    /// `date_trunc(unit, timestamp) = X` is a WINDOW, not a dimension filter;
    /// treated as a residual it has no declared measure filter to match.
    #[tokio::test]
    async fn a_date_trunc_equality_narrows_the_window_instead_of_being_promoted() {
        let state = session().await;
        // A day-wide window plus the hour the panel wants.
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

    /// One project short of coverage must not refuse the query for every other
    /// project. The legs must PARTITION (project x time): covered projects read
    /// the rollup over the interior and raw over the fringes, uncovered projects
    /// read raw over the whole window.
    #[tokio::test]
    async fn an_uncovered_project_reads_raw_while_the_others_still_route() {
        let state = session().await;
        let sql = format!("SELECT project_id, COUNT(*) FROM {SOURCE} WHERE {WINDOW} GROUP BY 1");
        let route = route_for(&state, &sql).await.expect("match").expect("route");
        let split = ProjectSplit { covered: Some(vec!["good".into(), "fine".into()]), raw_only: vec!["lagging".into()] };
        let generated = route.sql(&[("good".into(), "1970-01-01".into(), "generation".into())], &[(route.lo, route.hi)], &split);

        // The rollup leg answers only for the projects that proved coverage...
        assert!(
            generated.contains(&format!("FROM {} WHERE project_id IN ('good', 'fine')", route.target)),
            "rollup leg must be restricted to covered projects: {generated}"
        );
        // ...and the uncovered one is read raw across the WHOLE window, not dropped.
        assert!(generated.contains(&format!("FROM {SOURCE} WHERE project_id IN ('lagging')")), "the uncovered project must still be read, raw: {generated}");
        assert!(generated.contains("UNION ALL"), "the two must be unioned: {generated}");
        assert!(generated.contains(&format!("timestamp >= to_timestamp_micros({})", route.lo)), "raw-only leg must start at the window start: {generated}");
    }

    /// With every project covered the rewrite must carry no `IN` list and no
    /// extra leg — this is the common case, and a needless predicate is a tax.
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

    /// The three ranges must PARTITION `[lo, hi)`, and both interior endpoints
    /// must be grain-aligned: a rollup row is indivisible, so half of one cannot
    /// be handed to a fringe.
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

    /// Microsecond-precision bounds ending at wall-clock `now` — the shape
    /// production sends — must emit a raw fringe and a live tail.
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
        // The query's own output name contains `avg(`, so only the union body can
        // be checked for a leg-level average.
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
    /// `metric_name`, selected with `metric_name IN (…)`.
    #[tokio::test]
    async fn a_metrics_panel_routes_with_an_in_filter_and_a_digest() {
        let state = session_over(["otel_metrics", "otel_metrics_rollup_metrics_1m_v2"].map(str::to_owned));
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

    /// `now()` folds to a NANOSECOND literal, so a microsecond-only matcher would
    /// refuse every `timestamp < now()` window.
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

    /// Coarsest-first selection must still respect the window: a 1h tier cannot
    /// answer a 10-minute one, and being tried FIRST it would shadow the 1m tier.
    #[tokio::test]
    async fn grain_selection_prefers_the_coarsest_tier_the_window_can_use() {
        let state = session().await;
        let wide = format!(
            "SELECT COUNT(*) FROM {SOURCE} WHERE project_id = 'project' \
               AND timestamp >= to_timestamp_micros(0) AND timestamp < to_timestamp_micros(864000000000) \
             GROUP BY time_bucket('1 hours', timestamp)"
        );
        // The GRAIN is what this pins, not which 1h tier wins: the candidate sort
        // breaks ties toward the narrower dimension set.
        let coarse = route_for(&state, &wide).await.expect("match").expect("route").target;
        assert!(coarse.contains("_1h_"), "a 10-day window bucketed hourly must use a coarse tier, got {coarse}");

        // Ten minutes: the 1h tier cannot cover it, so the 1m tier must win.
        let narrow = format!(
            "SELECT COUNT(*) FROM {SOURCE} WHERE project_id = 'project' \
               AND timestamp >= to_timestamp_micros(0) AND timestamp < to_timestamp_micros(600000000) \
             GROUP BY time_bucket('5 minutes', timestamp)"
        );
        let fine = route_for(&state, &narrow).await.expect("match").expect("route").target;
        assert!(fine.contains("_1m_"), "a 10-minute window must fall back to a fine tier, got {fine}");
    }

    /// Grouping by a bucket without SELECTing it is an ordinary dashboard shape
    /// ("count per hour, ordered by count"). The aggregate then has one more
    /// output than the projection above it, so the rewrite must be named for the
    /// AGGREGATE, not by absorbing the projection's names positionally.
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

    /// `ORDER BY <an aggregate> LIMIT n` puts the Sort BELOW the outer projection
    /// (the sort key must still exist when the sort runs, and is dropped after),
    /// giving `Projection(Sort(Projection(Aggregate)))`. The layers above an
    /// aggregate are whatever the session's analyzer rules produced, not a
    /// grammar, so the matcher searches rather than peels.
    #[tokio::test]
    async fn the_shape_that_defeated_every_peeling_matcher_routes() {
        let state = session().await;
        let sql = format!(
            "SELECT COUNT(*) AS c, avg(duration)::BIGINT AS m FROM {SOURCE} WHERE project_id = 'project' AND {WINDOW} \
             GROUP BY time_bucket('1 hours', timestamp) ORDER BY 1 DESC LIMIT 2"
        );
        let (rebuilt, _) = assert_substitutes(&state, &sql, None).await;
        // A routed query that lost its sort returns rows in rollup order and
        // silently truncates the wrong two.
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

    /// A rewrite is a SELECT, so its aliases are unqualified, while the
    /// aggregate's group-by column keeps its table qualifier. Nodes above resolve
    /// columns on `(qualifier, name)`, so the substitute must reproduce BOTH.
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
    /// arbitrary they are — never a name derived from the expression. The plan
    /// cache lifts literals to `$N` and substitutes the values back, leaving
    /// field names frozen as the template's, so a field can be named for an
    /// expression that is no longer there.
    #[tokio::test]
    async fn the_rewrite_is_aliased_with_the_aggregates_own_field_names() {
        let state = session().await;
        // `replace_params_with_values` aliases the new literal back to the
        // placeholder's name to keep the schema stable.
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

    /// `timestamp_window` decides how much rollup coverage a DML statement
    /// destroys, so a window narrower than the statement's true reach would leave
    /// coverage standing for a partition that changed.
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
    /// every day after it, and the rollup intervals plus the raw complement must
    /// still partition `[lo, hi)` exactly.
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

    /// The guard that stops the tier serving an aggregate built from a partition
    /// that has since moved.
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

    /// One live file, spelled the way a Delta add action does: `max_ts` inclusive.
    fn file(rows: u64, min_ts: i64, max_ts: i64) -> SourceFile {
        SourceFile { min_ts: Some(min_ts), max_ts: Some(max_ts), rows }
    }

    /// The scenario every case below is a transition of: a slice covering
    /// `[0, 600)` of a date whose live files at build time were `A[0,299]` and
    /// `B[300,599]`, 100 physical rows each, of which 180 are logically distinct.
    /// `C[700,900]` is later-in-the-day ingest no slice of this build claimed.
    const BOUND: i64 = 600;

    #[test_case::test_case(
        Some(SliceWitness::Physical(200)), &[file(100, 0, 299), file(100, 300, 599)], None, WitnessVerdict::Valid
        ; "v1 baseline: nothing moved")]
    #[test_case::test_case(
        Some(SliceWitness::Physical(200)), &[file(100, 0, 299), file(100, 300, 599), file(50, 700, 900)], None, WitnessVerdict::Stale
        ; "THE grew defect: hour-23 ingest voids an hour-00 slice under the v1 whole-partition witness")]
    #[test_case::test_case(
        Some(SliceWitness::PhysicalBelow { rows: 200, bound: BOUND }), &[file(100, 0, 299), file(100, 300, 599), file(50, 700, 900)], None, WitnessVerdict::Valid
        ; "and the bounded witness survives it — ingest past covered_through is not this slice's business")]
    #[test_case::test_case(
        Some(SliceWitness::Physical(200)), &[file(200, 0, 599)], None, WitnessVerdict::Valid
        ; "benign bin-pack compaction preserves num_records, so even v1 survives it")]
    #[test_case::test_case(
        Some(SliceWitness::PhysicalBelow { rows: 200, bound: BOUND }), &[file(200, 0, 599)], None, WitnessVerdict::Valid
        ; "benign bin-pack compaction under the bounded witness")]
    #[test_case::test_case(
        Some(SliceWitness::Physical(200)), &[file(180, 0, 599)], None, WitnessVerdict::Stale
        ; "benign DEDUP collapses 20 merge-on-read versions: physical witnesses call a correct slice stale")]
    #[test_case::test_case(
        Some(SliceWitness::Physical(180)), &[file(180, 0, 599)], None, WitnessVerdict::Valid
        ; "carry-forward: maintenance restamps the witness it knowingly invalidated, and the same slice is valid again")]
    #[test_case::test_case(
        Some(SliceWitness::Logical { rows: 180, lo: 0, hi: BOUND }), &[file(180, 0, 599)], Some((0, BOUND, 180)), WitnessVerdict::Valid
        ; "the logical witness needs no carry-forward: dedup cannot change what it counts")]
    #[test_case::test_case(
        Some(SliceWitness::PhysicalBelow { rows: 200, bound: BOUND }), &[file(100, 0, 299), file(100, 300, 599), file(10, 100, 200)], None, WitnessVerdict::Stale
        ; "genuine LATE ingest inside the covered range is stale, which is the whole point of keeping a witness")]
    #[test_case::test_case(
        Some(SliceWitness::Logical { rows: 180, lo: 0, hi: BOUND }), &[file(190, 0, 599)], Some((0, BOUND, 190)), WitnessVerdict::Stale
        ; "genuine late ingest under the logical witness")]
    #[test_case::test_case(
        Some(SliceWitness::PhysicalBelow { rows: 200, bound: BOUND }), &[file(90, 0, 299), file(100, 300, 599)], None, WitnessVerdict::Stale
        ; "a DML DELETE inside the covered range is stale under the bounded witness")]
    #[test_case::test_case(
        Some(SliceWitness::Logical { rows: 180, lo: 0, hi: BOUND }), &[file(170, 0, 599)], Some((0, BOUND, 170)), WitnessVerdict::Stale
        ; "a DML DELETE inside the covered range is stale under the logical witness")]
    #[test_case::test_case(
        Some(SliceWitness::PhysicalBelow { rows: 200, bound: BOUND }), &[file(100, 0, 299), file(150, 400, 800)], None, WitnessVerdict::Unverifiable
        ; "STRADDLE POISON: a file spanning the bound cannot be split by arithmetic, so it refuses instead of guessing")]
    #[test_case::test_case(
        Some(SliceWitness::PhysicalBelow { rows: 200, bound: BOUND }), &[file(250, 0, 900)], None, WitnessVerdict::Unverifiable
        ; "THE COST of that rule: a day packed to one file per date straddles every slice bound and reads unverifiable")]
    #[test_case::test_case(
        Some(SliceWitness::PhysicalBelow { rows: 200, bound: BOUND }), &[SourceFile { min_ts: None, max_ts: None, rows: 200 }], None, WitnessVerdict::Unverifiable
        ; "a file with no timestamp statistic cannot be placed against the bound")]
    #[test_case::test_case(
        Some(SliceWitness::Logical { rows: 180, lo: 0, hi: BOUND }), &[file(180, 0, 599)], None, WitnessVerdict::Unverifiable
        ; "a logical witness with no resident index is unverifiable, never assumed fresh")]
    #[test_case::test_case(
        Some(SliceWitness::Logical { rows: 180, lo: 0, hi: BOUND }), &[file(180, 0, 599)], Some((0, 300, 180)), WitnessVerdict::Unverifiable
        ; "a count over a DIFFERENT window is not evidence about this slice, even when the numbers agree")]
    #[test_case::test_case(None, &[file(200, 0, 599)], None, WitnessVerdict::Unverifiable ; "no witness at all")]
    #[test_case::test_case(Some(SliceWitness::Physical(200)), &[], None, WitnessVerdict::Stale ; "an emptied partition is a disagreement, not an absence")]
    fn witness_survives_benign_maintenance_and_still_catches_real_change(
        witness: Option<SliceWitness>, files: &[SourceFile], logical: Option<(i64, i64, u64)>, expected: WitnessVerdict,
    ) {
        assert_eq!(verify_slice_witness(witness, LiveSource { files: Some(files), logical }), expected);
    }

    #[test]
    fn hybrid_branch_count_includes_rollup_and_raw_ranges() {
        let ranges = vec![(10, 20), (30, 40), (50, 60)];
        assert_eq!(hybrid_branch_count(0, 70, &ranges), 7);
        assert_eq!(hybrid_branch_count(10, 60, &ranges), 5);
    }

    /// The buffer horizon caps EVERY interval, not just the last one: a row still
    /// in the MemBuffer is missing from every rollup partition.
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
    /// looks like a complete day.
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

    /// A filter's canonical form must not depend on which Arrow string scalar the
    /// planner happened to produce.
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

    /// A matcher that only accepts a bare aggregate root would never fire:
    /// `ORDER BY ... DESC` sits above the aggregate on real dashboard queries.
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

    /// `HAVING` plans to a `Filter` between the aggregate and the projection;
    /// dropping it would return the rows the query excluded.
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
        // The window is deliberately wide enough for every tier, or the reported
        // reason would be the coarsest tier's `TinyInterior` instead.
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

    /// `project_id = <column>` must not be consumed and dropped, or the rollup
    /// answers without a predicate the raw query enforces.
    #[tokio::test]
    async fn a_non_literal_project_id_predicate_is_not_silently_dropped() {
        let route = route_alone(&format!("SELECT COUNT(*) FROM {SOURCE} WHERE project_id = name AND project_id = 'project' AND {WINDOW}")).await;
        assert!(matches!(route, Err(MissReason::UnknownFilter) | Ok(None)), "must not route while ignoring `project_id = name`: {route:?}");
    }

    /// The aggregate batch a rollup build hands to the shapers: `timestamp` and
    /// every declared dimension and measure, one row per `(micros, dimension
    /// value, digest bytes, integer measure)`. `projects` prepends the
    /// `project_id` column the cohort shaper needs and the single-project one
    /// must not see.
    fn aggregate_batch(spec: &RollupSpec, projects: Option<&[&str]>, rows: &[(i64, &str, &[u8], i64)]) -> RecordBatch {
        let mut fields: Vec<Arc<Field>> = Vec::new();
        let mut columns: Vec<arrow::array::ArrayRef> = Vec::new();
        if let Some(projects) = projects {
            fields.push(Arc::new(Field::new("project_id", DataType::Utf8, false)));
            columns.push(Arc::new(StringArray::from(projects.iter().map(|project| Some(*project)).collect::<Vec<_>>())));
        }
        fields.push(Arc::new(Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false)));
        columns.push(Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|row| row.0).collect::<Vec<_>>()).with_timezone("UTC")));
        fields.extend(spec.dimensions.iter().map(|name| Arc::new(Field::new(name, DataType::Utf8, true))));
        let dims: Vec<Option<&str>> = rows.iter().map(|row| Some(row.1)).collect();
        columns.extend(spec.dimensions.iter().map(|_| Arc::new(StringArray::from(dims.clone())) as arrow::array::ArrayRef));
        fields.extend(spec.measures.iter().map(|measure| {
            Arc::new(Field::new(&measure.name, if measure.agg == "tdigest" { DataType::Binary } else { DataType::Int64 }, measure.agg != "count"))
        }));
        columns.extend(spec.measures.iter().map(|measure| {
            if measure.agg == "tdigest" {
                Arc::new(BinaryArray::from(rows.iter().map(|row| Some(row.2)).collect::<Vec<_>>())) as arrow::array::ArrayRef
            } else {
                Arc::new(Int64Array::from(rows.iter().map(|row| Some(row.3)).collect::<Vec<_>>())) as arrow::array::ArrayRef
            }
        }));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("aggregate batch")
    }

    /// The first value of a shaped batch's `column`, which the shapers always
    /// write as a `StringViewArray`.
    fn utf8(batch: &RecordBatch, column: &str) -> String {
        batch.column_by_name(column).expect("column").as_any().downcast_ref::<StringViewArray>().expect("utf8 column").value(0).to_owned()
    }

    #[test]
    fn shaped_batches_preserve_binary_measure_and_generation() {
        let spec = spec();
        let input = aggregate_batch(&spec, None, &[(1_000_000, "value", &[1_u8, 2, 3][..], 1)]);
        let output = to_rollup_batches(&spec, SOURCE, "project", "1970-01-01", "generation-a", std::slice::from_ref(&input)).expect("shape rollup");
        let other = to_rollup_batches(&spec, SOURCE, "project", "1970-01-01", "generation-b", &[input]).expect("shape rollup");
        let batch = output.first().expect("one output batch");
        assert_eq!(utf8(batch, "rollup_generation"), "generation-a");
        let digest = batch.column_by_name("server_duration_digest").expect("digest");
        assert_eq!(digest.data_type(), &DataType::Binary);
        assert_eq!(digest.as_any().downcast_ref::<BinaryArray>().expect("binary digest").value(0), &[1, 2, 3]);
        assert_ne!(utf8(batch, "id"), utf8(&other[0], "id"), "generations must not share a dedup identity");
    }

    #[test]
    fn cohort_batch_shaping_preserves_project_generations() {
        let spec = spec();
        let input = aggregate_batch(&spec, Some(&["project-b", "project-a"]), &[(2_000_000, "b", &[2_u8][..], 2), (1_000_000, "a", &[1_u8][..], 1)]);
        let generations =
            std::collections::HashMap::from([("project-a".to_string(), "generation-a".to_string()), ("project-b".to_string(), "generation-b".to_string())]);
        let output = to_rollup_batches_by_project(&spec, SOURCE, "1970-01-01", &generations, &[input]).expect("shape cohort");
        for (project, generation) in [("project-a", "generation-a"), ("project-b", "generation-b")] {
            assert_eq!(utf8(&output[project][0], "project_id"), project);
            assert_eq!(utf8(&output[project][0], "rollup_generation"), generation);
        }
    }
}

#[cfg(test)]
mod rows_below_tests {
    use super::rows_below;

    /// The rescue's whole soundness argument is that this rule matches
    /// `partition_stats_bounded` exactly: excluded iff the max timestamp is KNOWN
    /// and reaches the bound. Prod measured 96.8% of rollup staleness as ingest
    /// past the build's own bound — rows this sum, by construction, cannot see.
    #[test_case::test_case(&[(Some(500), 10), (Some(1_500), 90)], 1_000 => Some(10) ; "a tail append past the bound is invisible")]
    #[test_case::test_case(&[(Some(999), 10)], 1_000 => Some(10) ; "a file ending just below the bound counts")]
    #[test_case::test_case(&[(Some(1_000), 10)], 1_000 => Some(0) ; "max == bound is excluded: the write side tests hi >= bound")]
    #[test_case::test_case(&[(None, 10), (Some(500), 5)], 1_000 => Some(15) ; "a file with no statistics is COUNTED, matching the write side")]
    #[test_case::test_case(&[], 1_000 => Some(0) ; "no files sum to zero, which still compares")]
    fn the_read_rule_matches_the_write_rule(files: &[(Option<i64>, i64)], bound: i64) -> Option<u64> {
        rows_below(files, bound)
    }

    /// A straddler is excluded WHOLESALE — never split by arithmetic — which is
    /// what lets compaction across the bound read as stale (a rebuild) rather
    /// than as a silently wrong partial count.
    #[test]
    fn a_straddling_file_is_excluded_wholesale() {
        // min far below the bound is irrelevant: only max decides.
        assert_eq!(rows_below(&[(Some(2_000), 100), (Some(500), 7)], 1_000), Some(7));
    }
}
