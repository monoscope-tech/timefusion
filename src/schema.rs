use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, OnceLock},
};

use arrow::datatypes::{DataType as ArrowDataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use deltalake::{
    datafusion::parquet::file::metadata::SortingColumn,
    kernel::{ArrayType, DataType as DeltaDataType, PrimitiveType, StructField},
};
use include_dir::{Dir, include_dir};
use serde::{Deserialize, Serialize};

/// One continuous-aggregate rollup, declared on the SOURCE table.
///
/// TimescaleDB's continuous aggregates in TF's shape: you declare the grain,
/// the dimensions and the measures, and the rollup table is SYNTHESIZED from
/// this plus the source schema (`RollupSpec::synthesize`). Nothing is
/// hand-written, so a rollup column's type cannot drift from the source column
/// it aggregates, and adding a rollup is a config change rather than a new
/// YAML file plus a hardcoded constant.
///
/// The refresh policy is TF's own and is better than a timer: the build fires
/// at the point a partition is CERTIFIED duplicate-free, because a bucket
/// computed over a bin that is later deduped is simply wrong.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RollupSpec {
    /// Bucket width, e.g. `1m`, `1h`, `1d`. The default table-name suffix, so
    /// the name cannot disagree with the resolution.
    pub grain: String,
    /// Distinguishing suffix, for a SECOND rollup at the same grain.
    ///
    /// Needed only when two rollups share a grain but group differently:
    /// different dimensions mean a different GROUP BY, hence a different row
    /// set, hence a genuinely different table. If instead you want another
    /// MEASURE at a grain you already roll up, add it to that rollup's
    /// `measures:` — same grain and same dimensions produce the identical row
    /// set, so a second table would duplicate every identity and dimension
    /// column and force a query wanting both measures to read two tables.
    #[serde(default)]
    pub name: Option<String>,
    /// Columns a query may GROUP BY **or FILTER on** and still be answerable.
    /// Filters constrain the design exactly as hard as group-bys — rows for a
    /// non-dimension are already summed together and cannot be subtracted back
    /// out — which is the part a rollup design usually gets wrong.
    pub dimensions: Vec<String>,
    pub measures: Vec<RollupMeasure>,
    /// Build this rollup from ANOTHER rollup on the same source rather than from
    /// raw rows, naming that spec's `name`.
    ///
    /// A coarse tier is what makes a 30-day window cheap, but computing it from
    /// raw means a second full scan of every partition — on the big table that
    /// doubles the cost of every certification. Re-aggregating the fine tier
    /// instead reads a table that is already orders of magnitude smaller, and it
    /// is exact: every measure here is associative, so folding 1m states into 1h
    /// states gives the same answer as folding raw rows straight to 1h.
    #[serde(default)]
    pub derive_from: Option<String>,
}

/// One stored measure. Only DECOMPOSABLE aggregates are expressible: count/sum/
/// min/max re-aggregate across buckets and across collapsed dimensions. `avg`
/// is admissible as sum/count and is expanded before it gets here; exact
/// percentiles and exact count(distinct) are not, and are refused rather than
/// answered approximately without being asked.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RollupMeasure {
    /// Column name in the rollup table.
    pub name: String,
    /// `count` | `sum` | `min` | `max`.
    pub agg: String,
    /// Source column to aggregate. Omitted for `count`.
    #[serde(default)]
    pub column: Option<String>,
    /// Optional `FILTER (WHERE …)` predicate, for things like an error count.
    #[serde(default)]
    pub filter: Option<String>,
}

impl RollupSpec {
    /// `{source}_rollup_{name|grain}` — see `rollup_table_is_named_after_its_source`.
    pub fn table_name(&self, source: &str) -> String {
        format!("{source}_rollup_{}", self.name.as_deref().unwrap_or(&self.grain))
    }

    /// Grain in microseconds, parsed from the suffix. `None` for an
    /// unparseable grain, which `validate` rejects at load rather than at the
    /// first build.
    pub fn grain_micros(&self) -> Option<i64> {
        let (n, unit) = self.grain.split_at(self.grain.len().checked_sub(1)?);
        let n: i64 = n.parse().ok()?;
        let mult = match unit {
            "s" => 1_000_000,
            "m" => 60 * 1_000_000,
            "h" => 3_600 * 1_000_000,
            "d" => 86_400 * 1_000_000,
            _ => return None,
        };
        (n > 0).then(|| n.checked_mul(mult)).flatten()
    }

    fn validate(&self, source: &TableSchema) -> anyhow::Result<()> {
        const IDENTITY_FIELDS: [&str; 7] = ["project_id", "timestamp", "date", "id", "updated_at", "deleted", "rollup_generation"];
        let field = |name: &str| source.fields.iter().find(|f| f.name == name);
        let is_ident = |name: &str| {
            let mut chars = name.chars();
            matches!(chars.next(), Some('a'..='z' | 'A'..='Z' | '_')) && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        };

        anyhow::ensure!(self.grain_micros().is_some(), "rollup {}: invalid grain `{}`", self.table_name(&source.table_name), self.grain);
        anyhow::ensure!(self.name.as_deref().is_none_or(is_ident), "rollup {}: name must be an SQL identifier", self.table_name(&source.table_name));
        let mut names = HashSet::new();
        for dimension in &self.dimensions {
            anyhow::ensure!(field(dimension).is_some(), "rollup {}: unknown dimension `{dimension}`", self.table_name(&source.table_name));
            anyhow::ensure!(names.insert(dimension), "rollup {}: duplicate dimension `{dimension}`", self.table_name(&source.table_name));
            anyhow::ensure!(
                !IDENTITY_FIELDS.contains(&dimension.as_str()),
                "rollup {}: dimension `{dimension}` collides with an identity field",
                self.table_name(&source.table_name)
            );
        }
        anyhow::ensure!(!self.measures.is_empty(), "rollup {}: needs at least one measure", self.table_name(&source.table_name));
        if let Some(base) = &self.derive_from {
            let target = self.table_name(&source.table_name);
            let base_spec = source
                .rollups
                .iter()
                .find(|spec| spec.name.as_deref() == Some(base.as_str()))
                .ok_or_else(|| anyhow::anyhow!("rollup {target}: `derive_from: {base}` names no rollup on this table"))?;
            anyhow::ensure!(base_spec.derive_from.is_none(), "rollup {target}: `derive_from` may not chain — `{base}` is itself derived");
            let (Some(fine), Some(coarse)) = (base_spec.grain_micros(), self.grain_micros()) else {
                anyhow::bail!("rollup {target}: cannot compare grains with `{base}`")
            };
            // A base bucket must fall entirely inside one of ours, or its state
            // would have to be split across two output buckets — which no
            // aggregate state can do.
            anyhow::ensure!(
                coarse > fine && coarse % fine == 0,
                "rollup {target}: grain must be a whole multiple of `{base}`'s ({} vs {})",
                self.grain,
                base_spec.grain
            );
            for dimension in &self.dimensions {
                anyhow::ensure!(
                    base_spec.dimensions.contains(dimension),
                    "rollup {target}: dimension `{dimension}` is absent from `{base}`, so it cannot be derived from it"
                );
            }
            for measure in &self.measures {
                let base_measure = base_spec
                    .measures
                    .iter()
                    .find(|candidate| candidate.name == measure.name)
                    .ok_or_else(|| anyhow::anyhow!("rollup {target}: measure `{}` is absent from `{base}`", measure.name))?;
                // Re-aggregating states only works if both sides mean the same
                // thing; a `sum` folded out of a `min` is silently wrong.
                anyhow::ensure!(
                    base_measure.agg == measure.agg && base_measure.column == measure.column && base_measure.filter == measure.filter,
                    "rollup {target}: measure `{}` must match `{base}`'s definition exactly to be derived from it",
                    measure.name
                );
            }
        }
        for measure in &self.measures {
            anyhow::ensure!(is_ident(&measure.name), "rollup {}: measure `{}` must be an SQL identifier", self.table_name(&source.table_name), measure.name);
            anyhow::ensure!(names.insert(&measure.name), "rollup {}: duplicate or colliding measure `{}`", self.table_name(&source.table_name), measure.name);
            anyhow::ensure!(
                matches!(measure.agg.as_str(), "count" | "sum" | "min" | "max" | "tdigest" | "hll"),
                "rollup {}: unsupported aggregate `{}`",
                self.table_name(&source.table_name),
                measure.agg
            );
            match (&measure.agg[..], measure.column.as_deref()) {
                ("count", None) => {}
                ("count", Some(column)) | ("sum" | "min" | "max" | "hll", Some(column)) => {
                    anyhow::ensure!(field(column).is_some(), "rollup {}: unknown column `{column}`", self.table_name(&source.table_name));
                }
                ("tdigest", Some(column)) => {
                    let Some(data_type) = field(column).map(|f| f.data_type.as_str()) else {
                        anyhow::bail!("rollup {}: unknown column `{column}`", self.table_name(&source.table_name))
                    };
                    anyhow::ensure!(
                        matches!(data_type, "Int32" | "Int64" | "UInt32" | "UInt64" | "Float64"),
                        "rollup {}: tdigest column `{column}` must be numeric",
                        self.table_name(&source.table_name)
                    );
                }
                (_, None) => {
                    anyhow::bail!("rollup {}: `{}` measure `{}` needs a source column", self.table_name(&source.table_name), measure.agg, measure.name)
                }
                (aggregate, Some(_)) => anyhow::bail!("rollup {}: unsupported aggregate `{aggregate}`", self.table_name(&source.table_name)),
            }
            anyhow::ensure!(
                measure.filter.as_deref().is_none_or(|f| !f.trim().is_empty()),
                "rollup {}: measure `{}` has an empty filter",
                self.table_name(&source.table_name),
                measure.name
            );
        }
        Ok(())
    }

    /// The rollup's `TableSchema`, derived from the source so column types
    /// cannot drift. Identity columns mirror what every table here carries;
    /// dimensions keep the source's own type and nullability; measures are
    /// Int64 (count) or the source column's type (sum/min/max).
    pub fn synthesize(&self, source: &TableSchema) -> anyhow::Result<TableSchema> {
        let src_field = |n: &str| {
            source
                .fields
                .iter()
                .find(|f| f.name == n)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("rollup {}: unknown column `{n}`", self.table_name(&source.table_name)))
        };
        let plain = |name: &str, data_type: &str, nullable: bool| FieldDef {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable,
            tantivy: None,
            dictionary: None,
            bloom_filter: false,
        };
        let mut fields = vec![
            plain("project_id", "Utf8", true),
            plain("timestamp", "Timestamp(Microsecond, Some(\"UTC\"))", false),
            plain("date", "Date32", false),
            plain("id", "Utf8", false),
            plain("updated_at", "Timestamp(Microsecond, Some(\"UTC\"))", false),
            plain("deleted", "Boolean", true),
            plain("rollup_generation", "Utf8", false),
        ];
        for d in &self.dimensions {
            let f = src_field(d)?;
            // Always nullable in the rollup: GROUP BY emits a NULL group for
            // rows missing the dimension, even when the source column is not.
            fields.push(FieldDef { nullable: true, ..f });
        }
        for m in &self.measures {
            let ty = match (m.agg.as_str(), &m.column) {
                ("count", _) => "Int64".to_string(),
                // `src_field` for its error, not its type: a sketch column is
                // always Binary, but naming a column that does not exist must
                // still fail HERE rather than synthesize a phantom field that
                // only breaks when the build SQL runs.
                ("tdigest" | "hll", Some(c)) => src_field(c).map(|_| "Binary".to_string())?,
                (_, Some(c)) => src_field(c)?.data_type,
                (a, None) => anyhow::bail!("rollup {}: `{a}` measure `{}` needs a source column", self.table_name(&source.table_name), m.name),
            };
            // A measure over an empty group is NULL, and count is never NULL.
            fields.push(plain(&m.name, &ty, m.agg != "count"));
        }
        Ok(TableSchema {
            table_name: self.table_name(&source.table_name),
            // Same partitioning as the source, so ProjectRoutingTable gives
            // multi-tenant isolation and date pruning for free.
            partitions: source.partitions.clone(),
            sorting_columns: vec![SortingColumnDef { name: "timestamp".into(), descending: true, nulls_first: true }],
            z_order_columns: vec![],
            fields,
            time_column: Some("timestamp".into()),
            // NO read-time dedup. A bucket is identified by time + dimension
            // tuple hashed into `id`, but duplicates are impossible by
            // construction: rollup waves commit Remove actions
            // for every existing file in the (project, date) partition together
            // with the new ones, so a partition only ever holds one build, and
            // reads additionally pin `rollup_generation`.
            //
            // Declaring keys here made every read of a rollup plan a `DedupExec`
            // over them — and the rewrite projects only dimensions and measures,
            // so planning died with "DedupExec key `id` not in input schema" and
            // every routed query fell back to a raw scan that then failed. Unit
            // tests could not see it: they register the rollup as a `MemTable`,
            // which has no dedup layer at all.
            dedup_keys: vec![],
            dedup_tiebreak: None,
            tombstone_column: Some("deleted".into()),
            // Never version_append: buckets are rebuilt wholesale, so there are
            // no superseded versions and the read path pays no dedup ordering.
            version_append: false,
            // A rollup of a rollup is a real design (1m -> 1h -> 1d) but it is
            // not this change: declaring it here would recurse at load.
            rollups: vec![],
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableSchema {
    pub table_name: String,
    /// Continuous-aggregate rollups derived FROM this table. Declared here
    /// rather than as separate schema files so a rollup cannot drift from its
    /// source; the rollup's own `TableSchema` is synthesized at load
    /// (`RollupSpec::synthesize`) and registered under `{table}_rollup_{grain}`.
    #[serde(default)]
    pub rollups: Vec<RollupSpec>,
    pub partitions: Vec<String>,
    pub sorting_columns: Vec<SortingColumnDef>,
    pub z_order_columns: Vec<String>,
    pub fields: Vec<FieldDef>,
    /// Column the optimizer should rewrite into a `date` partition filter.
    /// Defaults to `"timestamp"` for back-compat with existing schemas.
    #[serde(default)]
    pub time_column: Option<String>,
    /// Composite key for last-write-wins dedup at flush time. Empty = no dedup
    /// (append-only). E.g. `[id, timestamp]`. Variant columns rejected at load.
    /// Only collapses dupes inside one bucket; cross-bucket dupes need the
    /// read-side row_number() rewrite.
    #[serde(default)]
    pub dedup_keys: Vec<String>,
    /// Tie-breaker column for dedup: when rows share `dedup_keys`, keep the one
    /// with the greatest value here (ties → last seen, the back-compat default;
    /// NULL sorts lowest, so an un-stamped legacy row always loses).
    /// On a [`Self::version_append`] table the tiebreak is TF-OWNED: every write
    /// stamps it from a per-table monotonic clock (`insert_coerce::stamp_version`),
    /// so the newest version of a row wins deterministically. Everywhere else it
    /// is client-supplied and TF never writes it. `None` = keep-last by position.
    #[serde(default)]
    pub dedup_tiebreak: Option<String>,
    /// Nullable `Boolean` column marking a row version as a DELETION of its
    /// `dedup_keys` tuple (merge-on-read). `None` = the table has no tombstones
    /// and every mechanism below is a no-op. NULL and `false` both mean live —
    /// only `true` is a tombstone — so a table can declare the column before a
    /// single tombstone exists (and before any backfill) with zero effect.
    ///
    /// Independent of [`Self::version_append`] on purpose: read-side filtering
    /// and the sweep's version collapse key off this column alone, so they come
    /// alive (as no-ops) without waiting for the write path.
    #[serde(default)]
    pub tombstone_column: Option<String>,
    /// Merge-on-read WRITE path: `UPDATE`/`DELETE` append a new row version
    /// (with a fresh `dedup_tiebreak`, and `tombstone_column = true` for a
    /// delete) instead of planning a Delta MERGE. Per-table opt-in; false =
    /// today's in-place mutation. Requires `dedup_keys`, `dedup_tiebreak` and
    /// `tombstone_column`.
    ///
    /// Read-side fast paths that are only *wrong* once a key has more than one
    /// version on disk gate on THIS flag — e.g. the order-preserving union in
    /// `ProjectRoutingTable::scan`, which exists only to make `DedupExec`'s
    /// keep-greatest engage. Declaring the column is inert, so gating those on
    /// the column alone costs a blocking sort + k-way merge for a feature that
    /// cannot yet have written anything.
    ///
    /// [`Self::tombstones_possible`] deliberately does NOT gate on this flag:
    /// versions already written outlive the flag being turned off. Anything
    /// whose correctness depends on what is IN STORAGE must key off the column.
    ///
    /// 2026-08-02: enabling this on `otel_logs_and_spans` / `otel_metrics` first
    /// made recent-window reads unusable (1h ~13s, 3h timing out). Dedup becomes
    /// mandatory, bounded dedup needed ordered input, and the appended version
    /// carries the row's ORIGINAL timestamp into a NEW file — so Delta files
    /// overlap in time, no ordering can be declared, and the planner inserted a
    /// query-time `SortExec` that exhausted the 27.5GB query pool.
    ///
    /// Fixed in `read_dedup`, not by giving up the feature: keep-greatest now
    /// runs WITHOUT a bound (buffering to end-of-stream), so nothing has to
    /// manufacture an ordering. `mor_delta_leg_sorts` staying 0 is the signal
    /// that this is holding.
    #[serde(default)]
    pub version_append: bool,
}

// ─── ADDING A COLUMN TO A SHIPPED TABLE (read this before editing any YAML) ───
//
// A NEW table may declare whatever columns it likes — `mor_versioned` declares
// `updated_at` and `deleted` from birth, and nothing is wrong with that because
// no Delta table with the old column set exists anywhere.
//
// An EXISTING shipped table MUST NOT gain a column without an explicit migration
// path. `otel_logs_and_spans` gained `updated_at` + `deleted` in 7d68f01 and
// prod took 268 flush failures and rejected pgwire INSERTs within minutes:
//
//     Arrow error: Invalid argument error: number of columns(94) must match
//     number of fields(92)
//
// The YAML is only one of TWO schemas. The other is the one physically stored in
// each live Delta table's transaction log, and it is NOT derived from the YAML —
// it is whatever was there when the table was created, months ago, per project.
// Editing the YAML makes the write path build batches to the new shape while
// every existing Delta table still declares the old one, and nullability buys
// nothing: the mismatch is arity, not nulls.
//
// Every local suite passes anyway, because tests create their tables from
// scratch and the two schemas therefore always agree. That is precisely the
// blind spot — a green test run is NOT evidence that a column addition is safe.
// See `dedup_compaction_test::adding_a_column_to_an_existing_table_is_caught`,
// which creates a table at an old column set and then writes the new one.
//
// So: a column addition to a shipped table needs a migration that evolves the
// stored Delta schema of every live table (all projects, unified + custom)
// BEFORE the binary that writes the wider batch is deployed — verified against a
// pre-existing table, not a fresh one.

impl TableSchema {
    pub fn time_column_name(&self) -> &str {
        self.time_column.as_deref().unwrap_or("timestamp")
    }

    /// Arrow type + nullability of one declared field, without building the
    /// whole `schema_ref()` (which allocates ~100 fields per call).
    pub fn field_def(&self, name: &str) -> Option<(ArrowDataType, bool)> {
        let f = self.fields.iter().find(|f| f.name == name)?;
        Some((parse_arrow_data_type(&f.data_type).ok()?, f.nullable))
    }

    /// Can a tombstone row EXIST in this table's storage? True as soon as the
    /// column is declared, regardless of the write path.
    ///
    /// The old rule was `column && version_append`, guarded by a warning to
    /// "never flip the flag off after tombstones were written". That warning was
    /// correct and the hazard was real: disabling merge-on-read on
    /// `otel_logs_and_spans` (2026-08-02) would have instantly re-enabled
    /// COUNT(*)-from-stats over files still holding tombstoned rows, counting
    /// deleted rows as live. Rather than leave a flag whose safety depends on
    /// never being turned off, this now tracks STORAGE: a declared column means
    /// a tombstone may be down there, whatever the writer is doing today.
    ///
    /// The cost is that a table which declares the column but has never
    /// tombstoned anything gives up the stats fast path. That is the right side
    /// to err on — the alternative silently over-counts. A table that genuinely
    /// wants the fast path simply must not declare a `tombstone_column`.
    pub fn tombstones_possible(&self) -> bool {
        // Deliberately does NOT consult `version_append`. Whether a tombstone can
        // be sitting in STORAGE is a property of the data, not of whether the
        // write path happens to be enabled right now — and `version_append` is a
        // flag that gets toggled. It was `tombstone_column.is_some() &&
        // version_append`, which meant turning merge-on-read OFF instantly
        // re-enabled COUNT(*)-from-stats pushdown over files that still contain
        // tombstoned rows, silently counting deleted rows as live (2026-08-02).
        // Being conservative here costs a dormant table the stats fast path;
        // being wrong resurrects deleted rows in every COUNT.
        self.tombstone_column.is_some()
    }

    fn validate(&self) -> anyhow::Result<()> {
        let field = |role: &str, name: &str| {
            self.fields
                .iter()
                .find(|f| f.name == name)
                .ok_or_else(|| anyhow::anyhow!("schema `{}`: {role} references unknown field `{}`", self.table_name, name))
        };
        self.dedup_keys.iter().map(|k| ("dedup_keys", k)).chain(self.dedup_tiebreak.iter().map(|tb| ("dedup_tiebreak", tb))).try_for_each(
            |(role, name)| -> anyhow::Result<()> {
                anyhow::ensure!(field(role, name)?.data_type != "Variant", "schema `{}`: {role} cannot be a Variant column `{}`", self.table_name, name);
                Ok(())
            },
        )?;
        if let Some(tc) = &self.tombstone_column {
            let f = field("tombstone_column", tc)?;
            // Nullable Boolean is load-bearing: NULL must be a legal "live"
            // encoding so existing rows need no backfill.
            anyhow::ensure!(f.data_type == "Boolean" && f.nullable, "schema `{}`: tombstone_column `{}` must be a nullable Boolean field", self.table_name, tc);
        }
        anyhow::ensure!(
            !self.version_append || (!self.dedup_keys.is_empty() && self.dedup_tiebreak.is_some() && self.tombstone_column.is_some()),
            "schema `{}`: version_append requires dedup_keys, dedup_tiebreak and tombstone_column",
            self.table_name
        );
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SortingColumnDef {
    pub name: String,
    pub descending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FieldDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    #[serde(default)]
    pub tantivy: Option<TantivyFieldConfig>,
    /// Opt-out for dictionary encoding. Default on. Set false for high-entropy
    /// free-text columns (stacktraces, raw queries, full URLs) where dict just
    /// builds a useless 8MB before falling back to PLAIN — wasted writer pass.
    #[serde(default)]
    pub dictionary: Option<bool>,
    /// Per-column bloom filter opt-in. Default off. Enable for high-cardinality
    /// equality-lookup columns (ids, trace_ids, span_ids, session_ids).
    #[serde(default)]
    pub bloom_filter: bool,
}

/// Per-column tantivy index configuration. Drives `tantivy_index::schema`.
///
/// `tokenizer`: "raw" (exact match keyword) or "default" (tokenized text).
/// `flatten`: for Variant columns — "json" (value-only text) or "kv" (key:value tokens).
///
/// User fields are always indexed-only — the real data lives in Delta/parquet.
/// Only the reserved `_timestamp` and `_id` reserved fields are stored, and only
/// because the reader needs them to produce `(timestamp, id)` prefilter hits for
/// the Delta-side join.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct TantivyFieldConfig {
    #[serde(default)]
    pub indexed: bool,
    #[serde(default)]
    pub tokenizer: Option<String>,
    #[serde(default)]
    pub flatten: Option<String>,
}

impl TableSchema {
    pub fn fields(&self) -> anyhow::Result<Vec<FieldRef>> {
        self.fields
            .iter()
            .map(|f| {
                let field = Field::new(&f.name, parse_arrow_data_type(&f.data_type)?, f.nullable);
                // Without the ExtensionType marker fresh tables (variant_bench)
                // crash on the first INSERT — see `VARIANT_EXT_KEY`.
                Ok(Arc::new(match f.data_type.as_str() {
                    "Variant" => field.with_metadata(HashMap::from([(VARIANT_EXT_KEY.to_string(), VARIANT_EXT_VALUE.to_string())])),
                    _ => field,
                }) as FieldRef)
            })
            .collect()
    }

    pub fn columns(&self) -> anyhow::Result<Vec<StructField>> {
        self.fields.iter().map(|f| Ok(StructField::new(&f.name, parse_delta_data_type(&f.data_type)?, f.nullable))).collect()
    }

    pub fn schema_ref(&self) -> SchemaRef {
        // Partition columns move to the end to match Delta Lake's output order,
        // order preserved within each group.
        let all_fields = self.fields().unwrap_or_else(|e| panic!("Failed to build schema for table {}: {e:?}", self.table_name));
        let partition_set = self.partition_set();
        let (partition_fields, data_fields): (Vec<_>, Vec<_>) = all_fields.into_iter().partition(|f| partition_set.contains(f.name().as_str()));
        Arc::new(Schema::new(data_fields.into_iter().chain(partition_fields).collect::<Vec<_>>()))
    }

    fn partition_set(&self) -> HashSet<&str> {
        self.partitions.iter().map(String::as_str).collect()
    }

    pub fn sorting_columns(&self) -> Vec<SortingColumn> {
        // Parquet data files omit partition columns (they live in the path), so
        // the `SortingColumn.column_idx` the footer records must be the column's
        // position among the *non-partition* fields — the physical parquet leaf
        // order the reader (`ordering_from_parquet_metadata`) indexes into. Using
        // the raw fields-list index over-counts by every partition column that
        // precedes a sort key (e.g. `date` at field 0), so the footer points at
        // the wrong column and the sort-order pushdown silently never fires.
        let partition_set = self.partition_set();
        let data_cols: Vec<&str> = self.fields.iter().map(|f| f.name.as_str()).filter(|n| !partition_set.contains(n)).collect();
        self.sorting_columns
            .iter()
            .filter_map(|col| {
                data_cols.iter().position(|n| *n == col.name).map(|idx| SortingColumn {
                    column_idx: idx as i32,
                    descending: col.descending,
                    nulls_first: col.nulls_first,
                })
            })
            .collect()
    }
}

fn parse_arrow_data_type(s: &str) -> anyhow::Result<ArrowDataType> {
    Ok(match s {
        // Use Utf8View for better performance with zero-copy string operations
        "Utf8" => ArrowDataType::Utf8View,
        "Date32" => ArrowDataType::Date32,
        "Boolean" => ArrowDataType::Boolean,
        "Int32" => ArrowDataType::Int32,
        "Int64" => ArrowDataType::Int64,
        "Float64" => ArrowDataType::Float64,
        "Binary" => ArrowDataType::Binary,
        "UInt32" => ArrowDataType::UInt32,
        "UInt64" => ArrowDataType::UInt64,
        "List(Utf8)" => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8View, true))),
        "List(Int64)" => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int64, true))),
        "List(Float64)" => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Float64, true))),
        "Timestamp(Microsecond, None)" => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        "Timestamp(Microsecond, Some(\"UTC\"))" => ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        // Variant: declare the inner buffers as Binary to match
        // `delta_kernel::unshredded_variant()`. delta-rs's kernel rejects
        // schema mismatches at scan validation time even when no data
        // files exist (e.g. fresh DELETE on an empty table). Both
        // MemBuffer and Delta reads end up as Binary because:
        //   - the parquet reader honors `schema_force_view_types=false`
        //     (set in our session and in `delta_session_from` for DML);
        //   - `convert_variant_columns` casts VariantArrayBuilder's
        //     BinaryView output to Binary before MemBuffer ever sees it.
        // The ExtensionType marker (`VARIANT_EXT_KEY`) is added to the Field's
        // metadata in `fields()`.
        "Variant" => ArrowDataType::Struct(
            vec![
                Arc::new(Field::new(VARIANT_METADATA_FIELD, ArrowDataType::Binary, false)),
                Arc::new(Field::new(VARIANT_VALUE_FIELD, ArrowDataType::Binary, false)),
            ]
            .into(),
        ),
        _ => anyhow::bail!("Unknown type: {}", s),
    })
}

fn parse_delta_data_type(s: &str) -> anyhow::Result<DeltaDataType> {
    use PrimitiveType::*;
    Ok(match s {
        "Utf8" => DeltaDataType::Primitive(String),
        "Date32" => DeltaDataType::Primitive(Date),
        "Boolean" => DeltaDataType::Primitive(Boolean),
        "Int32" | "UInt32" => DeltaDataType::Primitive(Integer),
        "Int64" | "UInt64" => DeltaDataType::Primitive(Long),
        "Float64" => DeltaDataType::Primitive(Double),
        "Binary" => DeltaDataType::Primitive(Binary),
        "List(Utf8)" => DeltaDataType::Array(Box::new(ArrayType::new(DeltaDataType::Primitive(String), true))),
        "List(Int64)" => DeltaDataType::Array(Box::new(ArrayType::new(DeltaDataType::Primitive(Long), true))),
        "List(Float64)" => DeltaDataType::Array(Box::new(ArrayType::new(DeltaDataType::Primitive(Double), true))),
        "Variant" => DeltaDataType::unshredded_variant(),
        _ if s.starts_with("Timestamp") => DeltaDataType::Primitive(Timestamp),
        _ => anyhow::bail!("Unknown type: {}", s),
    })
}

// Include all YAML files from schemas directory at compile time
static SCHEMAS_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/schemas");

pub struct SchemaRegistry {
    schemas: HashMap<String, TableSchema>,
}

impl SchemaRegistry {
    fn new() -> Self {
        let mut schemas: HashMap<String, TableSchema> = SCHEMAS_DIR
            .files()
            .filter(|f| f.path().extension().and_then(|s| s.to_str()) == Some("yaml"))
            .map(|file| {
                let content = file.contents_utf8().expect("Schema file should be UTF-8");
                let schema: TableSchema = serde_yaml::from_str(content).unwrap_or_else(|e| panic!("Failed to parse schema {:?}: {}", file.path(), e));
                schema.validate().unwrap_or_else(|e| panic!("Invalid schema {:?}: {}", file.path(), e));
                schema.rollups.iter().try_for_each(|rollup| rollup.validate(&schema)).unwrap_or_else(|e| panic!("Invalid rollup on {:?}: {}", file.path(), e));
                (schema.table_name.clone(), schema)
            })
            .collect();
        // Synthesize a table for every declared rollup. Generated rather than
        // hand-written so a rollup column's type cannot drift from the source
        // column it aggregates, and so adding one is a config change.
        // Two rollups generating one name is a config error, and WHICH error it
        // is depends on whether they group the same way — so say so, rather
        // than making the operator infer it from a name collision.
        for src in schemas.values() {
            for (i, a) in src.rollups.iter().enumerate() {
                if let Some(b) = src.rollups[i + 1..].iter().find(|b| b.table_name(&src.table_name) == a.table_name(&src.table_name)) {
                    let name = a.table_name(&src.table_name);
                    assert!(
                        a.dimensions != b.dimensions,
                        "{}: two rollups both generate `{name}` with the SAME dimensions. Same grain + same dimensions is the same GROUP BY, so \
                         add the extra measures to the existing rollup instead of declaring a second one — a second table would duplicate every \
                         identity and dimension column and make a query wanting both measures read two tables.",
                        src.table_name
                    );
                    panic!(
                        "{}: two rollups both generate `{name}` but group differently ({:?} vs {:?}). Different dimensions ARE different tables; \
                         give one of them a `name:` to distinguish it.",
                        src.table_name, a.dimensions, b.dimensions
                    );
                }
            }
        }
        let synthesized: Vec<TableSchema> = schemas
            .values()
            .flat_map(|src| src.rollups.iter().map(move |spec| (src, spec)))
            .map(|(src, spec)| {
                let rollup = spec.synthesize(src).unwrap_or_else(|e| panic!("Invalid rollup on {}: {e}", src.table_name));
                rollup.validate().unwrap_or_else(|e| panic!("Invalid synthesized rollup {}: {e}", rollup.table_name));
                rollup
            })
            .collect();
        for r in synthesized {
            let name = r.table_name.clone();
            if schemas.insert(name.clone(), r).is_some() {
                // A hand-written file under a generated name would silently win
                // or lose depending on iteration order. (Same-source rollup
                // collisions are already reported above, with a better message.)
                panic!("rollup table `{name}` collides with a hand-written schema file of the same name");
            }
        }
        // Migration aliases remain queryable while v3/v2 slice generations
        // shadow-build and canary. They are read-only schema aliases: source
        // rollup declarations point only at the new targets, so maintenance
        // cannot accidentally keep writing the retired generations.
        for (current, legacy) in [
            ("otel_logs_and_spans_rollup_dashboard_1m_v3", "otel_logs_and_spans_rollup_dashboard_1m_v2"),
            ("otel_logs_and_spans_rollup_dashboard_1h_v2", "otel_logs_and_spans_rollup_dashboard_1h_v1"),
            ("otel_metrics_rollup_metrics_1m_v2", "otel_metrics_rollup_metrics_1m_v1"),
            ("otel_metrics_rollup_metrics_1h_v2", "otel_metrics_rollup_metrics_1h_v1"),
        ] {
            if let Some(mut schema) = schemas.get(current).cloned() {
                schema.table_name = legacy.to_owned();
                schemas.entry(legacy.to_owned()).or_insert(schema);
            }
        }
        Self { schemas }
    }

    pub fn get(&self, table_name: &str) -> Option<&TableSchema> {
        self.schemas.get(table_name)
    }

    // otel_logs_and_spans predates multi-schema, so it's the back-compat default.
    pub fn get_default(&self) -> Option<&TableSchema> {
        self.schemas.get("otel_logs_and_spans").or_else(|| self.schemas.values().next())
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }
}

// Global registry instance.
//
// IMPORTANT: The registry is loaded once via `include_dir!` and `OnceLock`,
// so schemas are immutable for the lifetime of the process. Several
// downstream caches rely on this invariant for correctness (not just perf):
//   - `optimizers::indexed_columns_for` (per-table tokenizer map)
//   - `plan_cache::PlanCacheHook` (LogicalPlan embeds SchemaRef at parse time)
// If hot-reload of YAML schemas is ever added, those caches must gain a
// schema-version token in their key (or be flushed on reload).
static SCHEMA_REGISTRY: OnceLock<SchemaRegistry> = OnceLock::new();

pub fn registry() -> &'static SchemaRegistry {
    SCHEMA_REGISTRY.get_or_init(SchemaRegistry::new)
}

pub fn get_schema(table_name: &str) -> Option<&'static TableSchema> {
    registry().get(table_name)
}

pub fn get_default_schema() -> &'static TableSchema {
    registry().get_default().expect("No schemas available in registry")
}

/// Inner field names of the unshredded Variant struct
/// (`delta_kernel::unshredded_variant()`). Centralized here so any writer or
/// validator that constructs a Variant struct uses the same names; if
/// delta-kernel ever renames these, only this file changes.
pub const VARIANT_METADATA_FIELD: &str = "metadata";
pub const VARIANT_VALUE_FIELD: &str = "value";

/// Arrow ExtensionType marker every Variant field must carry, or
/// `Field::try_extension_type::<VariantType>()` (delta-rs, parquet-variant-compute)
/// panics with "Extension type name missing".
pub const VARIANT_EXT_KEY: &str = "ARROW:extension:name";
pub const VARIANT_EXT_VALUE: &str = "arrow.parquet.variant";

/// Returns true if the given Arrow DataType structurally matches a Variant
/// (Struct with `metadata` + `value` binary/binaryview fields).
pub fn is_variant_type(data_type: &ArrowDataType) -> bool {
    let ArrowDataType::Struct(fields) = data_type else { return false };
    let binary_named = |name: &str| fields.iter().any(|f| f.name() == name && matches!(f.data_type(), ArrowDataType::Binary | ArrowDataType::BinaryView));
    fields.len() == 2 && binary_named(VARIANT_METADATA_FIELD) && binary_named(VARIANT_VALUE_FIELD)
}

/// Replaces Variant fields with Utf8View on a schema. This is the schema we hand to the
/// SQL planner via `TableProvider::schema()` whenever the table contains Variant columns.
///
/// Background: `INSERT INTO t (v) VALUES ('{"a":1}')` fails inside
/// `LogicalPlanBuilder::values` because `arrow_cast::can_cast_types(Utf8, Struct{Binary,Binary})`
/// is false. The check is hardcoded in datafusion-expr; there is no extension hook to
/// register a Utf8→Variant coercion (datafusion exposes `ExprPlanner` for binary ops,
/// field access, etc., but not for the values-type check). Patching arrow-cast or
/// datafusion-expr is the only "fundamental" fix and is out of scope.
///
/// So we keep two views of the schema:
/// - SQL-facing view (this function): Utf8View for variant cols → planner accepts JSON literals.
/// - Storage view (`real_schema()`): the actual Struct{Binary, Binary} variant type.
///
/// `DataSink::write_all` converts inbound Utf8/Utf8View → Variant struct (via
/// `parquet_variant_compute::VariantArrayBuilder`) before the Delta write.
pub fn create_insert_compatible_schema(schema: &SchemaRef) -> SchemaRef {
    // `tf.pg_type = jsonb`: pgwire Describe derives RowDescription from the
    // *unanalyzed* plan, where Variant cols carry this Utf8View view. Without
    // the tag, bare Variant columns surface text OID 25 and strict drivers
    // (hasql) reject the row (expected jsonb 3802). vendor/arrow-pg maps the
    // tag to OID 3802 + the 0x01 binary jsonb version byte.
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .map(|f| {
            if is_variant_type(f.data_type()) {
                Arc::new(
                    Field::new(f.name(), ArrowDataType::Utf8View, f.is_nullable())
                        .with_metadata(HashMap::from([("tf.pg_type".to_string(), "jsonb".to_string())])),
                )
            } else {
                f.clone()
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE_YAML: &str = "table_name: t\npartitions: []\nsorting_columns: []\nz_order_columns: []\ndedup_keys: [id]\ndedup_tiebreak: updated_at\n";
    const FIELDS_YAML: &str = "fields:\n  - {name: id, data_type: Utf8, nullable: false}\n  - {name: updated_at, data_type: 'Timestamp(Microsecond, None)', nullable: true}\n  - {name: deleted, data_type: Boolean, nullable: true}\n";

    fn parse_schema(extra: &str, fields: &str) -> TableSchema {
        serde_yaml::from_str(&format!("{BASE_YAML}{extra}{fields}")).expect("yaml parses")
    }

    #[test]
    fn synthesized_rollup_stores_a_generation_and_tdigest() {
        let source = get_schema("otel_logs_and_spans").expect("source schema");
        let spec = RollupSpec {
            grain: "1m".into(),
            name: Some("digest_test".into()),
            dimensions: vec!["kind".into()],
            measures: vec![RollupMeasure { name: "digest".into(), agg: "tdigest".into(), column: Some("duration".into()), filter: None }],
            derive_from: None,
        };

        let rollup = spec.synthesize(source).expect("valid rollup");
        assert_eq!(rollup.field_def("rollup_generation"), Some((ArrowDataType::Utf8View, false)));
        assert_eq!(rollup.field_def("digest"), Some((ArrowDataType::Binary, true)));
    }

    #[test]
    fn legacy_rollup_generations_remain_readable_during_migration() {
        for name in [
            "otel_logs_and_spans_rollup_dashboard_1m_v2",
            "otel_logs_and_spans_rollup_dashboard_1h_v1",
            "otel_metrics_rollup_metrics_1m_v1",
            "otel_metrics_rollup_metrics_1h_v1",
        ] {
            assert!(get_schema(name).is_some(), "legacy rollup schema {name} must remain registered during canary");
        }
    }

    /// An `hll` measure stores a sketch, exactly as `tdigest` does, and unlike
    /// every other aggregate it must accept a NON-numeric source column — the
    /// columns anyone wants a distinct count of (trace id, user id, service
    /// name) are all strings.
    #[test]
    fn an_hll_measure_stores_a_sketch_over_any_column_type() {
        let source = get_schema("otel_logs_and_spans").expect("source schema");
        let spec = |column: &str| RollupSpec {
            grain: "1m".into(),
            name: Some("hll_test".into()),
            dimensions: vec!["kind".into()],
            measures: vec![RollupMeasure { name: "traces".into(), agg: "hll".into(), column: Some(column.into()), filter: None }],
            derive_from: None,
        };
        for column in ["context___trace_id", "duration"] {
            let rollup = spec(column).synthesize(source).expect("valid rollup");
            assert_eq!(rollup.field_def("traces"), Some((ArrowDataType::Binary, true)), "column {column}");
        }
        assert!(spec("no_such_column").synthesize(source).is_err(), "an unknown column must still be rejected");
    }

    #[test]
    fn invalid_rollup_aggregate_fails_validation() {
        let source = get_schema("otel_logs_and_spans").expect("source schema");
        let spec = RollupSpec {
            grain: "1m".into(),
            name: None,
            dimensions: vec![],
            measures: vec![RollupMeasure { name: "bad".into(), agg: "median".into(), column: Some("duration".into()), filter: None }],
            derive_from: None,
        };

        assert!(spec.validate(source).unwrap_err().to_string().contains("unsupported aggregate"));
    }

    // Regression: parquet `SortingColumn.column_idx` must index the non-partition
    // (physical parquet) columns, not the raw fields list. `date` is a partition
    // at field index 0, so the raw index over-counts by 1 and the footer points
    // at the wrong column — which made `ordering_from_parquet_metadata` resolve
    // nothing and the timestamp-ordering pushdown silently never fire.
    #[test]
    fn sorting_columns_index_excludes_partitions() {
        let schema = get_schema("otel_logs_and_spans").expect("otel schema registered");
        let scs = schema.sorting_columns();
        // Build the expected physical (non-partition) column order.
        let partition_set = schema.partition_set();
        let data_cols: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).filter(|n| !partition_set.contains(n)).collect();
        // timestamp is the lead sort key and the first physical column → index 0.
        assert_eq!(data_cols[0], "timestamp");
        let ts = scs.first().expect("at least one sorting column");
        assert_eq!(ts.column_idx, 0, "timestamp must map to physical parquet column 0, got {}", ts.column_idx);
        assert!(ts.descending, "timestamp is sorted newest-first (DESC)");
        // Every declared sort column resolves to its non-partition position.
        for (sc, def) in scs.iter().zip(&schema.sorting_columns) {
            let want = data_cols.iter().position(|n| *n == def.name).unwrap() as i32;
            assert_eq!(sc.column_idx, want, "sort col `{}` column_idx mismatch", def.name);
        }
    }

    /// The tombstone column must be nullable Boolean (NULL = live, so no
    /// backfill) and named by the schema — nothing hard-codes it. Exercised on
    /// `mor_versioned`, the from-scratch fixture. (It was once the ONLY table
    /// allowed to declare these columns; the shipped tables joined it after the
    /// 2026-08-02 migration — see `shipped_mor_tables_declare_the_migrated_columns_last`.)
    #[test]
    fn mor_versioned_tombstone_column_is_nullable_boolean() {
        let schema = get_schema("mor_versioned").expect("fixture registered");
        assert_eq!(schema.tombstone_column.as_deref(), Some("deleted"));
        assert_eq!(schema.field_def("deleted"), Some((ArrowDataType::Boolean, true)));
        assert_eq!(schema.dedup_tiebreak.as_deref(), Some("updated_at"));
    }

    /// The SHIPPED merge-on-read tables, post-migration (2026-08-02). This guard
    /// used to assert these tables had NO merge-on-read columns, because adding
    /// one to a table that already holds live Delta data broke prod (7d68f01):
    /// the stored transaction log kept the old column set while the write path
    /// built batches to the YAML's, giving `number of columns(94) must match
    /// number of fields(92)`.
    ///
    /// The migration has since been run and verified against prod
    /// (`migrate-columns`, commit edb2fd2), so the invariant is no longer
    /// "absent" but "declared in the SAME SHAPE AND ORDER the stored schema was
    /// widened in" — the two columns last, nullable, and never before any
    /// pre-existing field. Reordering them here re-creates 7d68f01 exactly.
    #[test]
    fn shipped_mor_tables_declare_the_migrated_columns_last() {
        for name in ["otel_logs_and_spans", "otel_metrics"] {
            let schema = get_schema(name).unwrap_or_else(|| panic!("{name} registered"));
            // `version_append` is OFF since 2026-08-02 (it forced a query-time
            // SortExec that exhausted the query pool), but the migrated columns
            // and the tombstone/tiebreak declarations MUST stay: tombstones and
            // multi-version rows written while it was on are still in storage,
            // and DedupExec + the tombstone filter are what keep them correct.
            assert!(schema.version_append, "{name} ships merge-on-read");
            assert_eq!(schema.tombstone_column.as_deref(), Some("deleted"), "{name} tombstone column");
            // Nullable, so the migration needed no backfill: NULL reads as live.
            assert_eq!(schema.field_def("deleted"), Some((ArrowDataType::Boolean, true)), "{name}.deleted must be nullable Boolean");
            assert!(matches!(schema.field_def("updated_at"), Some((ArrowDataType::Timestamp(..), true))), "{name}.updated_at must be a nullable timestamp");

            // ORDER is the load-bearing part: `migrate-columns` APPENDED these,
            // so they must be the final two fields, updated_at then deleted.
            let tail: Vec<&str> = schema.fields.iter().rev().take(2).map(|f| f.name.as_str()).collect();
            assert_eq!(tail, vec!["deleted", "updated_at"], "{name}: the migrated columns must be the LAST two fields, in migration order (7d68f01)");
        }
        // The tiebreak MUST be the TF-owned column, not the client's:
        // `insert_coerce::stamp_version` OVERWRITES whatever this names, so
        // pointing it back at `observed_timestamp` / `ingested_at` would destroy
        // client data on every write.
        for name in ["otel_logs_and_spans", "otel_metrics"] {
            assert_eq!(get_schema(name).unwrap().dedup_tiebreak.as_deref(), Some("updated_at"), "{name} must break ties on the TF-owned stamp");
        }
    }

    #[test]
    fn validate_rejects_bad_tombstone_and_version_append_declarations() {
        let parse = |extra: &str, fields: &str| parse_schema(extra, fields).validate();

        parse("tombstone_column: deleted\nversion_append: true\n", FIELDS_YAML).expect("well-formed version-append table");
        assert!(parse("tombstone_column: missing\n", FIELDS_YAML).unwrap_err().to_string().contains("unknown field"));
        assert!(parse("tombstone_column: id\n", FIELDS_YAML).unwrap_err().to_string().contains("nullable Boolean"));
        let non_null = FIELDS_YAML.replace("deleted, data_type: Boolean, nullable: true", "deleted, data_type: Boolean, nullable: false");
        assert!(parse("tombstone_column: deleted\n", &non_null).unwrap_err().to_string().contains("nullable Boolean"));
        assert!(parse("version_append: true\n", FIELDS_YAML).unwrap_err().to_string().contains("version_append requires"));
    }

    /// A declared-but-dormant tombstone column must NOT read as "tombstones can
    /// exist" — that verdict is what the COUNT(*) stats pushdown gates on, and
    /// reading it off the column alone cost every `COUNT(*)` on the main table
    /// its fast path (pre-deploy review, 2026-07-31).
    #[test]
    fn tombstones_possible_tracks_storage_not_the_write_path() {
        let parse = |extra: &str| parse_schema(extra, FIELDS_YAML);

        assert!(!parse("").tombstones_possible(), "no tombstone column at all");
        // A declared column means a tombstone MAY sit in storage, whatever the
        // write path is doing now — `version_append` is a flag that gets toggled,
        // and turning it off does not delete the tombstones already written.
        // Consulting it here let COUNT(*)-from-stats count deleted rows as live
        // the moment merge-on-read was disabled (2026-08-02).
        assert!(parse("tombstone_column: deleted\n").tombstones_possible(), "declared column ⇒ tombstones may exist even with the write path off");
        assert!(parse("tombstone_column: deleted\nversion_append: true\n").tombstones_possible(), "write path on → tombstones can exist");
        // The live schemas. otel has `version_append: false` since 2026-08-02 but
        // KEEPS its tombstone column, so it must stay conservative.
        assert!(get_schema("otel_logs_and_spans").unwrap().tombstones_possible(), "otel ships merge-on-read");
        assert!(get_schema("mor_versioned").unwrap().tombstones_possible());
        // `mor_dormant` declares a tiebreak but NO tombstone column, so nothing
        // could ever have tombstoned it — the stats fast path stays available.
        assert!(!get_schema("mor_dormant").unwrap().tombstones_possible());
    }

    #[test]
    fn otel_metrics_schema_supports_native_metric_values() {
        let schema = get_schema("otel_metrics").expect("metrics schema registered");
        assert_eq!(schema.partitions, ["project_id", "date"]);
        let fields = schema.fields().expect("metrics fields parse");
        assert!(matches!(fields.iter().find(|f| f.name() == "value").map(|f| f.data_type()), Some(ArrowDataType::Float64)));
        assert!(matches!(fields.iter().find(|f| f.name() == "hist_bucket_counts").map(|f| f.data_type()), Some(ArrowDataType::List(_))));
    }
}
