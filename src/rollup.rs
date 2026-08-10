//! Dashboard rollups: a pre-aggregated sibling of `otel_logs_and_spans`.
//!
//! Wide dashboard panels do not fail because the engine is slow, they fail
//! because they read raw rows at all. Measured on prod 2026-08-09, a 7-day
//! Overview panel decoded 15.97 M rows across 105 objects to return 35.89 K —
//! and at 14 days the same shape hit the 2 GiB dedup cap and errored. Reading a
//! 1-minute rollup instead turns that into a few hundred KB, which is the only
//! change here that removes the work rather than making it cheaper.
//!
//! Two halves, and the second is the one that gets skipped:
//!
//! * [`build_partition`] computes buckets from a source partition;
//! * [`route`] PROVES a query is answerable from them, and refuses otherwise.
//!
//! Correctness under merge-on-read is why the build trigger is not a timer. A
//! bucket's contents change after the fact — enrichment appends new versions and
//! keep-greatest decides the winner — so a rollup computed over a bin that is
//! later deduped is simply wrong. Buckets are therefore built ONLY from a
//! partition the sweep has certified clean, the same signal
//! `dedup_skip_allowed` uses, and a bin re-entering the dirty queue invalidates
//! them.

use crate::{metrics, schema_loader::RollupSpec};

/// The source table this rollup summarizes. Spans only — `otel_metrics` has
/// none of these columns and would need its own rollup, dimension set and
/// decomposability argument.
pub const SOURCE_TABLE: &str = "otel_logs_and_spans";

/// The table [`build_partition`] writes and [`route`] reads.
///
/// NAMING RULE, pinned by `rollup_table_is_named_after_its_source`:
/// `{SOURCE_TABLE}_rollup_{grain}`. This was `otel_rollup_1m`, which reads like
/// a rollup of all OTel data when it is a rollup of ONE table — every dimension
/// and measure here is span-shaped (`kind`, `status_code`, `duration`, and an
/// error predicate on HTTP status). A second rollup over a different source
/// must carry its own source's name for the same reason.
pub const ROLLUP_TABLE: &str = "otel_logs_and_spans_rollup_1m";

/// The grain suffix in [`ROLLUP_TABLE`], kept beside [`GRAIN_MICROS`] so a
/// change to one that is not mirrored in the other fails the naming test
/// rather than shipping a table whose name lies about its resolution.
pub const GRAIN_SUFFIX: &str = "1m";

/// Base grain, in microseconds. Coarser grains are derived by re-aggregating
/// this one (1m -> 1h -> 1d): the merge is associative, so there is no second
/// pipeline and no second correctness argument.
pub const GRAIN_MICROS: i64 = 60 * 1_000_000;

/// Columns a query may GROUP BY or FILTER on and still be answerable.
///
/// Filter columns constrain the design exactly as hard as group-by columns, and
/// that is the part a rollup design usually gets wrong: rows for a
/// non-dimension are already summed together and cannot be subtracted back out,
/// so a filter on one cannot be applied after aggregation. Both sets are
/// checked against this list.
pub const DIMENSIONS: [&str; 3] = ["resource___service___name", "kind", "status_code"];

/// Aggregates that survive re-aggregation across buckets and across collapsed
/// dimensions. `avg` is admissible as `sum/count` and is expanded by the
/// planner before it reaches here.
pub const DECOMPOSABLE: [&str; 4] = ["count", "sum", "min", "max"];

/// Why a query could not be served from the rollup.
///
/// Carried rather than collapsed to a bool because without the reason there is
/// no feedback loop telling us which dimension to add next — a rollup that
/// silently serves 20% of traffic looks identical to one that serves 90%.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissReason {
    /// A GROUP BY key that is not a dimension.
    UnknownGroupBy,
    /// A FILTER on a column that is not a dimension — the one that gets missed.
    UnknownFilter,
    /// `count(distinct)`, an exact percentile: cannot be re-aggregated.
    NonDecomposableAggregate,
    /// The requested bucket width is finer than [`GRAIN_MICROS`], or the range
    /// does not cover whole buckets.
    PartialBucket,
}

impl MissReason {
    /// Stable label for the `rollup_miss` counter.
    pub const fn label(self) -> &'static str {
        match self {
            Self::UnknownGroupBy => "unknown_group_by",
            Self::UnknownFilter => "unknown_filter",
            Self::NonDecomposableAggregate => "non_decomposable_aggregate",
            Self::PartialBucket => "partial_bucket",
        }
    }
}

/// What a query asks for, reduced to the four things routing depends on.
#[derive(Debug, Clone)]
pub struct Ask<'a> {
    pub group_by: &'a [String],
    pub filtered: &'a [String],
    pub aggregates: &'a [String],
    /// Requested bucket width in micros, if the query buckets by time at all.
    pub bucket_micros: Option<i64>,
}

/// Decide whether `ask` can be answered from the rollup, and record the verdict.
///
/// All four conditions must hold; any failure falls through to the raw scan,
/// which is always correct and merely slower. Silent fallthrough is deliberate —
/// a query that cannot route must still return the right answer — but it is
/// never UNRECORDED, which is what makes the dimension set improvable.
pub fn route(spec: &RollupSpec, ask: &Ask<'_>) -> Result<(), MissReason> {
    let grain_micros = spec.grain_micros().unwrap_or(GRAIN_MICROS);
    let known = |c: &String| spec.dimensions.iter().any(|d| d == c);
    let reason = if !ask.group_by.iter().all(known) {
        Some(MissReason::UnknownGroupBy)
    } else if !ask.filtered.iter().all(known) {
        Some(MissReason::UnknownFilter)
    } else if !ask.aggregates.iter().all(|a| DECOMPOSABLE.contains(&a.as_str())) {
        Some(MissReason::NonDecomposableAggregate)
    } else if ask.bucket_micros.is_none_or(|w| w < grain_micros || w % grain_micros != 0) {
        // Finer than the grain cannot be reconstructed, and a width that is not
        // a whole multiple would split a stored bucket across two output rows.
        Some(MissReason::PartialBucket)
    } else {
        None
    };
    match reason {
        Some(r) => {
            metrics::record_rollup_miss(r.label());
            Err(r)
        }
        None => {
            metrics::record_rollup_hit();
            Ok(())
        }
    }
}

/// Bucket start containing `ts`, floored to the base grain.
///
/// Floor, not truncate-toward-zero: pre-epoch timestamps are not expected here,
/// but a bucket that jumps forward for negative input would silently misplace
/// rows rather than fail, and that is the class of bug this table cannot afford.
pub const fn bucket_start(ts_micros: i64) -> i64 {
    ts_micros.div_euclid(GRAIN_MICROS) * GRAIN_MICROS
}

/// Stable identity for one rollup row.
///
/// Deterministic in (bucket, dimensions) so rebuilding a partition produces the
/// SAME id and replaces the previous row through the table's dedup keys, rather
/// than doubling every measure. That property is what lets a re-certified bin be
/// rebuilt without a delete pass.
pub fn bucket_id(bucket_micros: i64, dims: &[Option<&str>]) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bucket_micros.hash(&mut h);
    // Hash the arity too: ["a", None] and ["a"] must not collide.
    dims.len().hash(&mut h);
    for d in dims {
        d.hash(&mut h);
    }
    format!("{bucket_micros}-{:016x}", h.finish())
}

/// The SQL that computes one partition's buckets from the source table.
///
/// Expressed as SQL rather than hand-rolled Arrow because the engine already
/// does this well and the aggregate list is the part that must stay obviously
/// aligned with [`DIMENSIONS`] and the schema's measure columns.
///
/// `date` is the partition being rebuilt; the caller is responsible for having
/// certified it clean first (see the module docs).
pub fn build_partition_sql(spec: &RollupSpec, source: &str, project_id: &str, date: &str) -> String {
    let grain = spec.grain_micros().unwrap_or(GRAIN_MICROS);
    let dims = spec.dimensions.join(", ");
    // Measures come from the spec in declaration order, which is the same order
    // `synthesize` appended them to the schema — that alignment is what lets
    // `to_rollup_batches` map column i of the aggregate onto measure i.
    let measures = spec
        .measures
        .iter()
        .map(|m| {
            let inner = match (m.agg.as_str(), m.column.as_deref()) {
                ("count", _) => "COUNT(*)".to_string(),
                (a, Some(c)) => format!("{}({c})", a.to_uppercase()),
                (a, None) => format!("{}(1)", a.to_uppercase()),
            };
            match &m.filter {
                Some(f) => format!("{inner} FILTER (WHERE {f}) AS {}", m.name),
                None => format!("{inner} AS {}", m.name),
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT \
           to_timestamp_micros(CAST(FLOOR(EXTRACT(EPOCH FROM timestamp) * 1000000 / {grain}) AS BIGINT) * {grain}) AS timestamp, \
           {dims}, {measures} \
         FROM {source} \
         WHERE project_id = '{project_id}' AND date = '{date}' \
         GROUP BY 1, {dims}"
    )
}

/// Shape [`build_partition_sql`]'s output into rows of the rollup schema.
///
/// The aggregate produces `timestamp`, the dimensions, then the measures; this
/// adds the identity columns the table needs — a deterministic `id` (so a
/// rebuild replaces rather than doubles), the partition `date`, and the
/// `updated_at`/`deleted` pair every table here carries.
pub fn to_rollup_batches(
    spec: &RollupSpec, source: &str, project_id: &str, date: &str, aggregated: &[arrow::record_batch::RecordBatch],
) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
    use arrow::array::{Array, ArrayRef, BooleanArray, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use std::sync::Arc;

    let target = spec.table_name(source);
    let schema = crate::schema_loader::get_schema(&target).ok_or_else(|| anyhow::anyhow!("{target} schema missing"))?.schema_ref();
    // Days since epoch; the partition column is what routes reads to this date.
    let date_days =
        chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")?.signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
    let now = crate::clock::now_micros();

    let text = |b: &arrow::record_batch::RecordBatch, name: &str| -> Option<Vec<Option<String>>> {
        let idx = b.schema().index_of(name).ok()?;
        let col = arrow::compute::kernels::cast::cast(b.column(idx), &arrow::datatypes::DataType::Utf8).ok()?;
        let col = col.as_any().downcast_ref::<StringArray>()?.clone();
        Some((0..col.len()).map(|i| col.is_valid(i).then(|| col.value(i).to_string())).collect())
    };
    let ints = |b: &arrow::record_batch::RecordBatch, name: &str| -> Vec<Option<i64>> {
        b.schema()
            .index_of(name)
            .ok()
            .and_then(|i| arrow::compute::kernels::cast::cast(b.column(i), &arrow::datatypes::DataType::Int64).ok())
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>().cloned())
            .map_or_else(|| vec![None; b.num_rows()], |c| (0..c.len()).map(|i| c.is_valid(i).then(|| c.value(i))).collect())
    };

    let mut out = Vec::new();
    for b in aggregated.iter().filter(|b| b.num_rows() > 0) {
        let n = b.num_rows();
        let ts: Vec<i64> = b
            .schema()
            .index_of("timestamp")
            .ok()
            .and_then(|i| {
                arrow::compute::kernels::cast::cast(
                    b.column(i),
                    &arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
                )
                .ok()
            })
            .and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>().cloned())
            .map_or_else(|| vec![0; n], |c| (0..c.len()).map(|i| c.value(i)).collect());
        let dims: Vec<Vec<Option<String>>> = spec.dimensions.iter().map(|d| text(b, d).unwrap_or_else(|| vec![None; n])).collect();
        let ids: Vec<String> = (0..n)
            .map(|r| {
                let row: Vec<Option<&str>> = dims.iter().map(|d| d[r].as_deref()).collect();
                bucket_id(ts[r], &row)
            })
            .collect();

        let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for f in schema.fields() {
            let col: ArrayRef = match f.name().as_str() {
                "project_id" => Arc::new(StringArray::from(vec![Some(project_id); n])),
                "timestamp" => Arc::new(TimestampMicrosecondArray::from(ts.clone()).with_timezone("UTC")),
                "date" => Arc::new(Date32Array::from(vec![date_days; n])),
                "id" => Arc::new(StringArray::from(ids.clone())),
                "updated_at" => Arc::new(TimestampMicrosecondArray::from(vec![now; n]).with_timezone("UTC")),
                "deleted" => Arc::new(BooleanArray::from(vec![Some(false); n])),
                name if DIMENSIONS.contains(&name) => {
                    let i = DIMENSIONS.iter().position(|d| *d == name).unwrap();
                    Arc::new(StringArray::from(dims[i].clone()))
                }
                // Counts are never null — an absent count is a zero bucket, not
                // an unknown one — while the duration measures are genuinely
                // absent when every row in the bucket had a null duration.
                name @ ("request_count" | "error_count") => Arc::new(Int64Array::from(ints(b, name).into_iter().map(|v| v.unwrap_or(0)).collect::<Vec<_>>())),
                name => Arc::new(Int64Array::from(ints(b, name))),
            };
            // Build in the natural Arrow type, then cast to whatever the loaded
            // schema actually uses — string columns materialize as `Utf8View`
            // here, and hand-matching each field's type would drift the moment
            // the schema changes.
            cols.push(match col.data_type() == f.data_type() {
                true => col,
                false => arrow::compute::kernels::cast::cast(&col, f.data_type())?,
            });
        }
        out.push(arrow::record_batch::RecordBatch::try_new(Arc::clone(&schema), cols)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {

    /// The rollup must be DEFINED BY CONFIG, not by code. This is the property
    /// that makes it a continuous-aggregate feature rather than one hardcoded
    /// table: the schema registry has to contain a rollup table synthesized
    /// from the source's `rollups:` block, with the dimensions and measures
    /// that block declares and types taken from the source's own columns.
    #[test]
    fn the_rollup_table_is_generated_from_the_source_tables_config() {
        let src = crate::schema_loader::get_schema(SOURCE_TABLE).expect("source schema");
        let spec = src.rollups.first().expect("otel_logs_and_spans must declare a rollup");
        let generated = crate::schema_loader::get_schema(&spec.table_name(SOURCE_TABLE)).expect("the rollup table must be registered without a hand-written yaml");

        let has = |n: &str| generated.fields.iter().any(|f| f.name == n);
        for d in &spec.dimensions {
            assert!(has(d), "declared dimension `{d}` missing from the generated table");
        }
        for m in &spec.measures {
            assert!(has(&m.name), "declared measure `{}` missing from the generated table", m.name);
        }
        // Types are DERIVED from the source, which is the anti-drift property:
        // a sum over an i64 column cannot silently become a string.
        let src_ty = |n: &str| src.fields.iter().find(|f| f.name == n).map(|f| f.data_type.clone());
        for m in spec.measures.iter().filter(|m| m.agg != "count") {
            let col = m.column.as_deref().expect("non-count measure declares a column");
            assert_eq!(generated.fields.iter().find(|f| f.name == m.name).map(|f| f.data_type.clone()), src_ty(col), "measure `{}` must keep the source column's type", m.name);
        }
        // Partitioning is inherited, so multi-tenant routing and date pruning
        // work on the rollup exactly as on the source.
        assert_eq!(generated.partitions, src.partitions);
        // And a rollup is never version_append: buckets are rebuilt wholesale.
        assert!(!generated.version_append);
    }

    /// The spec as DECLARED on the source table — tests exercise the real
    /// configuration rather than a fixture that could drift from it.
    fn declared_spec() -> crate::schema_loader::RollupSpec {
        crate::schema_loader::get_schema(SOURCE_TABLE).expect("source schema").rollups.first().expect("a declared rollup").clone()
    }

    use super::*;

    fn ask<'a>(group_by: &'a [String], filtered: &'a [String], aggregates: &'a [String], bucket: Option<i64>) -> Ask<'a> {
        Ask { group_by, filtered, aggregates, bucket_micros: bucket }
    }
    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    /// The dashboard's Traffic panel: count by hour, filtered to server spans.
    /// This is the shape the whole table exists for, so if it stops routing the
    /// rollup is worthless.
    /// A rollup's name must say WHAT it rolls up. `otel_rollup_1m` did not: it
    /// reads like a rollup of all OTel data while summarizing exactly one
    /// table, and every dimension and measure in it is span-shaped. Adding a
    /// second rollup over a different source under a vague name is the failure
    /// this pins.
    #[test]
    fn rollup_table_is_named_after_its_source() {
        assert_eq!(ROLLUP_TABLE, format!("{SOURCE_TABLE}_rollup_{GRAIN_SUFFIX}"), "a rollup table must be named {{source}}_rollup_{{grain}}");
        // And the grain in the NAME must be the grain it actually stores.
        assert_eq!(GRAIN_MICROS, 60 * 1_000_000, "GRAIN_SUFFIX says 1m; keep it and GRAIN_MICROS in step");
        // The schema registry must agree, or the build writes to a table the
        // reader never looks for.
        let schema = crate::schema_loader::get_schema(ROLLUP_TABLE).expect("rollup schema must be registered under its own name");
        assert_eq!(schema.table_name, ROLLUP_TABLE);
    }

    #[test]
    fn the_traffic_panel_routes() {
        let (g, f, a) = (s(&[]), s(&["kind"]), s(&["count"]));
        assert_eq!(route(&declared_spec(), &ask(&g, &f, &a, Some(3_600_000_000))), Ok(()));
    }

    /// A filter on a NON-dimension must refuse, even though every GROUP BY key
    /// is fine. Those rows are already summed together and cannot be subtracted
    /// back out, so serving this from the rollup would silently over-count.
    #[test]
    fn a_filter_on_a_non_dimension_refuses_even_when_the_group_by_is_clean() {
        let (g, f, a) = (s(&["kind"]), s(&["attributes___url___path"]), s(&["count"]));
        assert_eq!(route(&declared_spec(), &ask(&g, &f, &a, Some(GRAIN_MICROS))), Err(MissReason::UnknownFilter));
    }

    #[test]
    fn a_non_dimension_group_by_refuses() {
        let (g, f, a) = (s(&["name"]), s(&[]), s(&["count"]));
        assert_eq!(route(&declared_spec(), &ask(&g, &f, &a, Some(GRAIN_MICROS))), Err(MissReason::UnknownGroupBy));
    }

    /// Exact percentiles do not re-aggregate. Answering them from stored
    /// min/max/sum would be a different, wrong number returned silently.
    #[test]
    fn a_non_decomposable_aggregate_refuses() {
        let (g, f, a) = (s(&[]), s(&[]), s(&["approx_percentile"]));
        assert_eq!(route(&declared_spec(), &ask(&g, &f, &a, Some(GRAIN_MICROS))), Err(MissReason::NonDecomposableAggregate));
    }

    /// Finer than the grain cannot be reconstructed, and a width that is not a
    /// whole multiple would split a stored bucket across two output rows.
    #[test]
    fn a_bucket_finer_than_the_grain_or_not_a_multiple_of_it_refuses() {
        let (g, f, a) = (s(&[]), s(&[]), s(&["count"]));
        assert_eq!(route(&declared_spec(), &ask(&g, &f, &a, Some(30_000_000))), Err(MissReason::PartialBucket), "30s is finer than the 1m grain");
        assert_eq!(route(&declared_spec(), &ask(&g, &f, &a, Some(90_000_000))), Err(MissReason::PartialBucket), "90s is not a whole number of buckets");
        assert_eq!(route(&declared_spec(), &ask(&g, &f, &a, None)), Err(MissReason::PartialBucket), "an unbucketed aggregate has no whole-bucket guarantee");
        assert_eq!(route(&declared_spec(), &ask(&g, &f, &a, Some(GRAIN_MICROS))), Ok(()), "the grain itself routes");
    }

    /// Rebuilding a partition must REPLACE its rows, not double every measure.
    /// The id is the only thing that makes the rewrite idempotent.
    #[test]
    fn a_rebuilt_bucket_keeps_its_identity_and_distinct_dimensions_do_not_collide() {
        let b = bucket_start(1_786_000_123_456_789);
        assert_eq!(
            bucket_id(b, &[Some("api"), Some("server")]),
            bucket_id(b, &[Some("api"), Some("server")]),
            "a rebuild must collide with the row it replaces"
        );
        assert_ne!(bucket_id(b, &[Some("api"), Some("server")]), bucket_id(b, &[Some("api"), Some("client")]), "different dimensions are different rows");
        assert_ne!(bucket_id(b, &[Some("api"), None]), bucket_id(b, &[Some("api")]), "arity must be part of the identity");
        assert_ne!(bucket_id(b, &[Some("api")]), bucket_id(b + GRAIN_MICROS, &[Some("api")]), "different buckets are different rows");
    }

    #[test]
    fn a_bucket_start_floors_and_is_stable_within_the_grain() {
        assert_eq!(bucket_start(GRAIN_MICROS), GRAIN_MICROS);
        assert_eq!(bucket_start(GRAIN_MICROS + 1), GRAIN_MICROS);
        assert_eq!(bucket_start(GRAIN_MICROS * 2 - 1), GRAIN_MICROS);
        // Floor, not truncate-toward-zero: a negative input must not jump into
        // the following bucket and silently misplace its rows.
        assert_eq!(bucket_start(-1), -GRAIN_MICROS);
    }

    /// The build must aggregate exactly the measures the schema stores, and
    /// group by exactly the dimensions routing promises. A drift between these
    /// two lists is a wrong answer, not a compile error.
    #[test]
    fn the_build_sql_covers_every_dimension_and_every_stored_measure() {
        let sql = build_partition_sql(&declared_spec(), SOURCE_TABLE, "proj", "2026-08-01");
        for d in DIMENSIONS {
            assert!(sql.contains(d), "dimension {d} missing from the build");
        }
        for m in ["request_count", "error_count", "duration_sum", "duration_min", "duration_max"] {
            assert!(sql.contains(m), "measure {m} missing from the build");
        }
        assert!(sql.contains("project_id = 'proj'") && sql.contains("date = '2026-08-01'"), "the build must be scoped to one certified partition");
    }
}
