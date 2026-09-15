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
use itertools::Itertools;
use serde::{Deserialize, Serialize};

/// One continuous-aggregate rollup, declared on the SOURCE table. The rollup
/// table is synthesized from this plus the source schema
/// (`RollupSpec::synthesize`); the build fires when a partition is certified
/// duplicate-free, since a bucket over a bin that is later deduped is wrong.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RollupSpec {
    /// Bucket width, e.g. `1m`, `1h`, `1d`. The default table-name suffix, so
    /// the name cannot disagree with the resolution.
    pub grain: String,
    /// Distinguishing suffix, needed only when two rollups share a grain but
    /// group differently. To add a MEASURE at a grain you already roll up, add
    /// it to that rollup's `measures:` instead of declaring a second spec.
    #[serde(default)]
    pub name: Option<String>,
    /// Columns a query may GROUP BY **or FILTER on** and still be answerable.
    /// Filters constrain the design as hard as group-bys: rows for a
    /// non-dimension are already summed together and cannot be subtracted out.
    pub dimensions: Vec<String>,
    pub measures: Vec<RollupMeasure>,
    /// Build this rollup from ANOTHER rollup on the same source rather than from
    /// raw rows, naming that spec's `name`. Exact because every measure here is
    /// associative.
    #[serde(default)]
    pub derive_from: Option<String>,
}

/// One stored measure. Only DECOMPOSABLE aggregates are expressible, so they
/// re-aggregate across buckets and across collapsed dimensions. `avg` is
/// expanded to sum/count before it gets here; exact percentiles and exact
/// count(distinct) are refused.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct RollupMeasure {
    /// Column name in the rollup table.
    pub name: String,
    /// `count` | `sum` | `min` | `max` | `tdigest` | `hll` | `first`.
    ///
    /// `first` additionally requires a companion `{agg: min, column: timestamp}`
    /// measure carrying the same filter — see `validate`.
    pub agg: String,
    /// Source column to aggregate. Omitted for `count`.
    #[serde(default)]
    pub column: Option<String>,
    /// Optional `FILTER (WHERE …)` predicate, for things like an error count.
    #[serde(default)]
    pub filter: Option<String>,
}

/// `(name, data_type, nullable)` of the columns every rollup tier carries.
/// `synthesize` builds them and `validate` refuses a dimension colliding with
/// one.
const IDENTITY_FIELDS: [(&str, &str, bool); 7] = [
    ("project_id", "Utf8", true),
    ("timestamp", "Timestamp(Microsecond, Some(\"UTC\"))", false),
    ("date", "Date32", false),
    ("id", "Utf8", false),
    ("updated_at", "Timestamp(Microsecond, Some(\"UTC\"))", false),
    ("deleted", "Boolean", true),
    ("rollup_generation", "Utf8", false),
];

impl RollupSpec {
    /// `{source}_rollup_{name|grain}`.
    pub fn table_name(&self, source: &str) -> String {
        format!("{source}_rollup_{}", self.name.as_deref().unwrap_or(&self.grain))
    }

    /// Grain in microseconds, parsed from the suffix. `None` if unparseable,
    /// which `validate` rejects at load.
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

    /// The declared `min(timestamp)` measure that says WHICH row a `first`
    /// measure's value came from; without it the coarse tier cannot merge. The
    /// filter must match the measure's, since "earliest row matching the filter"
    /// is a different row. `validate` refuses a spec without one, so `None` here
    /// means "never validated".
    pub(crate) fn first_companion(&self, measure: &RollupMeasure) -> Option<&RollupMeasure> {
        self.measures.iter().find(|c| c.agg == "min" && c.column.as_deref() == Some("timestamp") && c.filter == measure.filter)
    }

    fn validate(&self, source: &TableSchema) -> anyhow::Result<()> {
        let target = self.table_name(&source.table_name);
        let is_ident = |name: &str| {
            let mut chars = name.chars();
            matches!(chars.next(), Some('a'..='z' | 'A'..='Z' | '_')) && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        };

        anyhow::ensure!(self.grain_micros().is_some(), "rollup {target}: invalid grain `{}`", self.grain);
        anyhow::ensure!(self.name.as_deref().is_none_or(is_ident), "rollup {target}: name must be an SQL identifier");
        let mut names = HashSet::new();
        for dimension in &self.dimensions {
            anyhow::ensure!(source.field(dimension).is_some(), "rollup {target}: unknown dimension `{dimension}`");
            anyhow::ensure!(names.insert(dimension), "rollup {target}: duplicate dimension `{dimension}`");
            anyhow::ensure!(
                !IDENTITY_FIELDS.iter().any(|(name, ..)| *name == dimension.as_str()),
                "rollup {target}: dimension `{dimension}` collides with an identity field"
            );
        }
        anyhow::ensure!(!self.measures.is_empty(), "rollup {target}: needs at least one measure");
        if let Some(base) = &self.derive_from {
            let base_spec = source
                .rollups
                .iter()
                .find(|spec| spec.name.as_deref() == Some(base.as_str()))
                .ok_or_else(|| anyhow::anyhow!("rollup {target}: `derive_from: {base}` names no rollup on this table"))?;
            anyhow::ensure!(base_spec.derive_from.is_none(), "rollup {target}: `derive_from` may not chain — `{base}` is itself derived");
            let (Some(fine), Some(coarse)) = (base_spec.grain_micros(), self.grain_micros()) else {
                anyhow::bail!("rollup {target}: cannot compare grains with `{base}`")
            };
            // A base bucket must fall entirely inside one of ours; no aggregate
            // state can be split across two output buckets.
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
                // Re-aggregation is only correct if both sides mean the same
                // thing. Names already match, so struct equality IS
                // agg+column+filter.
                anyhow::ensure!(
                    base_measure == measure,
                    "rollup {target}: measure `{}` must match `{base}`'s definition exactly to be derived from it",
                    measure.name
                );
            }
        }
        for measure in &self.measures {
            let (name, agg) = (&measure.name, &measure.agg);
            anyhow::ensure!(is_ident(name), "rollup {target}: measure `{name}` must be an SQL identifier");
            anyhow::ensure!(names.insert(name), "rollup {target}: duplicate or colliding measure `{name}`");
            anyhow::ensure!(
                matches!(agg.as_str(), "count" | "sum" | "min" | "max" | "tdigest" | "hll" | "first"),
                "rollup {target}: unsupported aggregate `{agg}`"
            );
            anyhow::ensure!(
                agg != "first" || self.first_companion(measure).is_some(),
                "rollup {target}: `first` measure `{name}` needs a companion `{{agg: min, column: timestamp}}` measure carrying the same filter"
            );
            match measure.column.as_deref() {
                None => anyhow::ensure!(agg == "count", "rollup {target}: `{agg}` measure `{name}` needs a source column"),
                Some(column) => {
                    let Some(data_type) = source.field(column).map(|f| f.data_type.as_str()) else {
                        anyhow::bail!("rollup {target}: unknown column `{column}`")
                    };
                    anyhow::ensure!(
                        agg != "tdigest" || matches!(data_type, "Int32" | "Int64" | "UInt32" | "UInt64" | "Float64"),
                        "rollup {target}: tdigest column `{column}` must be numeric"
                    );
                }
            }
            anyhow::ensure!(measure.filter.as_deref().is_none_or(|f| !f.trim().is_empty()), "rollup {target}: measure `{name}` has an empty filter");
        }
        Ok(())
    }

    /// The rollup's `TableSchema`, derived from the source so column types
    /// cannot drift. Identity columns mirror what every table here carries;
    /// dimensions keep the source's own type and nullability; measures are
    /// Int64 (count) or the source column's type (sum/min/max).
    pub fn synthesize(&self, source: &TableSchema) -> anyhow::Result<TableSchema> {
        let target = self.table_name(&source.table_name);
        let src_field = |n: &str| source.field(n).cloned().ok_or_else(|| anyhow::anyhow!("rollup {target}: unknown column `{n}`"));
        let plain = |name: &str, data_type: &str, nullable: bool| FieldDef {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable,
            // A tier is rebuilt wholesale, never UPDATEd.
            ..Default::default()
        };
        let fields = IDENTITY_FIELDS
            .into_iter()
            .map(|(name, data_type, nullable)| Ok(plain(name, data_type, nullable)))
            // Dimensions are always nullable here: GROUP BY emits a NULL group
            // for rows missing the dimension even when the source column is not.
            // `tantivy: None` is deliberate — inheriting the source's index
            // config turns a dimension equality into a much slower `text_match`.
            .chain(self.dimensions.iter().map(|d| Ok(FieldDef { nullable: true, tantivy: None, ..src_field(d)? })))
            .chain(self.measures.iter().map(|m| {
                let ty = match (m.agg.as_str(), &m.column) {
                    ("count", _) => "Int64".to_string(),
                    // `src_field` for its error, not its type: a sketch column is
                    // always Binary, but an unknown column must still fail here.
                    ("tdigest" | "hll", Some(c)) => src_field(c).map(|_| "Binary".to_string())?,
                    (_, Some(c)) => src_field(c)?.data_type,
                    (a, None) => anyhow::bail!("rollup {target}: `{a}` measure `{}` needs a source column", m.name),
                };
                // A measure over an empty group is NULL, and count is never NULL.
                Ok(plain(&m.name, &ty, m.agg != "count"))
            }))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(TableSchema {
            table_name: target.clone(),
            // Same partitioning as the source, so ProjectRoutingTable gives
            // multi-tenant isolation and date pruning for free.
            partitions: source.partitions.clone(),
            sorting_columns: vec![SortingColumnDef { name: "timestamp".into(), descending: true, nulls_first: true }],
            z_order_columns: vec![],
            fields,
            time_column: Some("timestamp".into()),
            // A tier declares its identity so reads collapse superseded
            // versions; every tier declares the SAME keys. This is a safety net
            // under maintenance repair, not a substitute for it — dedup over a
            // partition with one version per key is near-free, over eight it is
            // not.
            dedup_keys: vec!["timestamp".into(), "id".into()],
            dedup_tiebreak: Some("updated_at".into()),
            tombstone_column: Some("deleted".into()),
            // A tier is rebuilt wholesale, never UPDATEd in place.
            version_append: false,
            // Declaring rollups on a synthesized tier would recurse at load.
            rollups: vec![],
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableSchema {
    pub table_name: String,
    /// Continuous-aggregate rollups derived FROM this table. Each rollup's own
    /// `TableSchema` is synthesized at load (`RollupSpec::synthesize`) and
    /// registered under `{table}_rollup_{grain}`.
    #[serde(default)]
    pub rollups: Vec<RollupSpec>,
    pub partitions: Vec<String>,
    pub sorting_columns: Vec<SortingColumnDef>,
    pub z_order_columns: Vec<String>,
    pub fields: Vec<FieldDef>,
    /// Column the optimizer should rewrite into a `date` partition filter.
    /// Defaults to `"timestamp"`.
    #[serde(default)]
    pub time_column: Option<String>,
    /// Composite key for last-write-wins dedup at flush time. Empty = no dedup
    /// (append-only). Variant columns are rejected at load. Only collapses dupes
    /// inside one bucket; cross-bucket dupes need the read-side rewrite.
    #[serde(default)]
    pub dedup_keys: Vec<String>,
    /// Tie-breaker column for dedup: when rows share `dedup_keys`, keep the one
    /// with the greatest value here (ties → last seen; NULL sorts lowest, so an
    /// un-stamped legacy row always loses). On a [`Self::version_append`] table
    /// this column is TF-OWNED — every write stamps it
    /// (`insert_coerce::stamp_version`); elsewhere it is client-supplied and TF
    /// never writes it. `None` = keep-last by position.
    #[serde(default)]
    pub dedup_tiebreak: Option<String>,
    /// Nullable `Boolean` column marking a row version as a DELETION of its
    /// `dedup_keys` tuple (merge-on-read). NULL and `false` both mean live, so a
    /// table can declare the column before any tombstone exists, with no
    /// backfill and no effect. Independent of [`Self::version_append`]: read-side
    /// filtering and the sweep's version collapse key off this column alone.
    #[serde(default)]
    pub tombstone_column: Option<String>,
    /// Merge-on-read WRITE path: `UPDATE`/`DELETE` append a new row version
    /// (fresh `dedup_tiebreak`, `tombstone_column = true` for a delete) instead
    /// of planning a Delta MERGE. Requires `dedup_keys`, `dedup_tiebreak` and
    /// `tombstone_column`.
    ///
    /// Read-side fast paths that are only wrong once a key has more than one
    /// version on disk gate on THIS flag, not on the column — declaring the
    /// column is inert, and gating on it would pay for a feature that cannot yet
    /// have written anything. Conversely, anything whose correctness depends on
    /// what is IN STORAGE (e.g. [`Self::tombstones_possible`]) must key off the
    /// column, because versions already written outlive the flag.
    #[serde(default)]
    pub version_append: bool,
}

// Adding a column to a shipped table needs an explicit migration that evolves
// the stored Delta schema of every live table (all projects, unified + custom)
// BEFORE the binary writing the wider batch is deployed. Delta schemas are not
// derived from YAML, and new-table tests cannot detect an upgrade mismatch.

impl TableSchema {
    pub fn time_column_name(&self) -> &str {
        self.time_column.as_deref().unwrap_or("timestamp")
    }

    pub fn field(&self, name: &str) -> Option<&FieldDef> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Arrow type + nullability of one declared field, without building the
    /// whole `schema_ref()` (which allocates ~100 fields per call).
    pub fn field_def(&self, name: &str) -> Option<(ArrowDataType, bool)> {
        let f = self.field(name)?;
        Some((parse_arrow_data_type(&f.data_type).ok()?, f.nullable))
    }

    /// Can a tombstone row EXIST in this table's storage? True as soon as the
    /// column is declared, regardless of the write path. A table that declares
    /// the column but never tombstones anything gives up the COUNT(*)-from-stats
    /// fast path; that is the safe side to err on.
    pub fn tombstones_possible(&self) -> bool {
        // Deliberately does NOT consult `version_append`: that flag can be
        // toggled off, but the tombstones already written stay in storage, and
        // counting them as live silently over-counts every COUNT(*).
        self.tombstone_column.is_some()
    }

    fn validate(&self) -> anyhow::Result<()> {
        let table = &self.table_name;
        for (field, config) in self.fields.iter().filter_map(|f| Some((f, f.tantivy.as_ref()?))).filter(|(_, c)| c.list_mode == TantivyListMode::Elements) {
            anyhow::ensure!(
                config.indexed
                    && config.tokenizer.as_deref() == Some("raw")
                    && config.flatten.is_none()
                    && matches!(parse_arrow_data_type(&field.data_type)?, ArrowDataType::List(inner) if matches!(inner.data_type(), ArrowDataType::Utf8 | ArrowDataType::Utf8View)),
                "schema `{table}`: element index `{}` requires an indexed string list, raw tokenizer and no flattening",
                field.name
            );
        }
        let field = |role: &str, name: &str| self.field(name).ok_or_else(|| anyhow::anyhow!("schema `{table}`: {role} references unknown field `{name}`"));
        for (role, name) in self.dedup_keys.iter().map(|k| ("dedup_keys", k)).chain(self.dedup_tiebreak.iter().map(|tb| ("dedup_tiebreak", tb))) {
            anyhow::ensure!(field(role, name)?.data_type != "Variant", "schema `{table}`: {role} cannot be a Variant column `{name}`");
        }
        if let Some(tc) = &self.tombstone_column {
            let f = field("tombstone_column", tc)?;
            // Nullable Boolean is load-bearing: NULL must be a legal "live"
            // encoding so existing rows need no backfill.
            anyhow::ensure!(f.data_type == "Boolean" && f.nullable, "schema `{table}`: tombstone_column `{tc}` must be a nullable Boolean field");
        }
        anyhow::ensure!(
            !self.version_append || (!self.dedup_keys.is_empty() && self.dedup_tiebreak.is_some() && self.tombstone_column.is_some()),
            "schema `{table}`: version_append requires dedup_keys, dedup_tiebreak and tombstone_column"
        );
        Ok(())
    }

    pub fn fields(&self) -> anyhow::Result<Vec<FieldRef>> {
        self.fields
            .iter()
            .map(|f| {
                let field = Field::new(&f.name, parse_arrow_data_type(&f.data_type)?, f.nullable);
                // Without the ExtensionType marker a fresh table crashes on the
                // first INSERT — see `VARIANT_EXT_KEY`.
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
        // `SortingColumn.column_idx` indexes physical parquet LEAVES among the
        // NON-partition fields (partition columns live in the path, not the
        // file), and a leaf is not a field: a Variant/struct column occupies one
        // leaf per child. Counting fields instead makes the footer name an
        // unrelated column, so the reader advertises an ordering the data does
        // not have.
        fn leaves(data_type: &ArrowDataType) -> i32 {
            use arrow::datatypes::DataType::*;
            match data_type {
                Struct(fields) => fields.iter().map(|f| leaves(f.data_type())).sum(),
                List(f) | LargeList(f) | FixedSizeList(f, _) | ListView(f) | LargeListView(f) => leaves(f.data_type()),
                Map(entries, _) => leaves(entries.data_type()),
                RunEndEncoded(_, values) => leaves(values.data_type()),
                _ => 1,
            }
        }
        let partition_set = self.partition_set();
        let Ok(fields) = self.fields() else { return Vec::new() };
        // Filter BEFORE the scan: partition columns must not advance the leaf
        // counter.
        let leaf_of: HashMap<&str, i32> = self
            .fields
            .iter()
            .zip(&fields)
            .filter(|(d, _)| !partition_set.contains(d.name.as_str()))
            .scan(0i32, |next, (declared, field)| {
                let idx = *next;
                *next += leaves(field.data_type());
                Some((declared.name.as_str(), idx))
            })
            .collect();
        self.sorting_columns
            .iter()
            .filter_map(|col| {
                leaf_of.get(col.name.as_str()).map(|&column_idx| SortingColumn { column_idx, descending: col.descending, nulls_first: col.nulls_first })
            })
            .collect()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SortingColumnDef {
    pub name: String,
    pub descending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct FieldDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    #[serde(default)]
    pub tantivy: Option<TantivyFieldConfig>,
    /// Opt-out for dictionary encoding. Default on. Set false for high-entropy
    /// free-text columns (stacktraces, raw queries, full URLs), where the
    /// dictionary is built and then discarded for PLAIN.
    #[serde(default)]
    pub dictionary: Option<bool>,
    /// Per-column bloom filter opt-in. Default off. Enable for high-cardinality
    /// equality-lookup columns (ids, trace_ids, span_ids, session_ids).
    #[serde(default)]
    pub bloom_filter: bool,
    /// Declares that an UPDATE may change this column, so versions of one row
    /// can disagree on its value.
    ///
    /// **Columns are immutable by default**, and that is load-bearing: a filter
    /// on an immutable column can be pushed BELOW the merge-on-read `DedupExec`
    /// (every version agrees, so it keeps or drops a key group whole), while a
    /// filter on a MUTABLE column must stay above it or a stale version could
    /// match a predicate the winning version no longer satisfies. Marking
    /// everything mutable strands point-lookup predicates above the dedup and
    /// materialises the whole window.
    ///
    /// Enforced, not trusted: `extract_dml_info` refuses at plan time any UPDATE
    /// assigning a column this does not mark. The version tiebreak and tombstone
    /// columns are always treated as mutable without declaring it.
    #[serde(default)]
    pub mutable: bool,
}

/// Per-column tantivy index configuration. Drives `tantivy_index::schema`.
///
/// `tokenizer`: "raw" (exact match keyword) or "default" (tokenized text).
/// `flatten`: for Variant columns — "json" (value-only text) or "kv" (key:value tokens).
///
/// User fields are always indexed-only; the real data lives in Delta/parquet.
/// Only the reserved `_timestamp` and `_id` fields are stored, because the
/// reader needs them to produce `(timestamp, id)` prefilter hits.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct TantivyFieldConfig {
    #[serde(default)]
    pub indexed: bool,
    #[serde(default)]
    pub tokenizer: Option<String>,
    #[serde(default)]
    pub flatten: Option<String>,
    #[serde(default)]
    pub list_mode: TantivyListMode,
}

/// Representation of string arrays in the search index: joined full text, or
/// one term per element (exact tag boundaries).
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TantivyListMode {
    #[default]
    JoinedText,
    Elements,
}

fn parse_arrow_data_type(s: &str) -> anyhow::Result<ArrowDataType> {
    Ok(match s {
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
        // Inner buffers must be Binary (not BinaryView) to match
        // `delta_kernel::unshredded_variant()`; the kernel rejects schema
        // mismatches at scan validation even when no data files exist. The
        // ExtensionType marker is added to the Field metadata in `fields()`.
        "Variant" => ArrowDataType::Struct([VARIANT_METADATA_FIELD, VARIANT_VALUE_FIELD].map(|n| Arc::new(Field::new(n, ArrowDataType::Binary, false))).into()),
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
        // The two spellings `parse_arrow_data_type` accepts — the decoders must agree on the set.
        "Timestamp(Microsecond, None)" | "Timestamp(Microsecond, Some(\"UTC\"))" => DeltaDataType::Primitive(Timestamp),
        _ => anyhow::bail!("Unknown type: {}", s),
    })
}

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
                let schema: TableSchema = serde_yaml::from_str(file.contents_utf8().expect("Schema file should be UTF-8"))
                    .unwrap_or_else(|e| panic!("Failed to parse schema {:?}: {}", file.path(), e));
                schema.validate().unwrap_or_else(|e| panic!("Invalid schema {:?}: {}", file.path(), e));
                schema.rollups.iter().try_for_each(|rollup| rollup.validate(&schema)).unwrap_or_else(|e| panic!("Invalid rollup on {:?}: {}", file.path(), e));
                (schema.table_name.clone(), schema)
            })
            .collect();
        // Two rollups generating one table name is a config error; which error
        // depends on whether they group the same way.
        if let Some((src, a, b)) = schemas
            .values()
            .flat_map(|src| src.rollups.iter().tuple_combinations().map(move |(a, b)| (src, a, b)))
            .find(|(src, a, b)| a.table_name(&src.table_name) == b.table_name(&src.table_name))
        {
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
            // A hand-written file under a generated name would win or lose by
            // iteration order.
            assert!(!schemas.contains_key(&r.table_name), "rollup table `{}` collides with a hand-written schema file of the same name", r.table_name);
            schemas.insert(r.table_name.clone(), r);
        }
        // Read-only aliases keeping retired rollup generations queryable while
        // the new ones shadow-build: source declarations point only at the new
        // targets, so maintenance never writes the retired generations.
        for (current, legacy) in [
            ("otel_logs_and_spans_rollup_dashboard_1m_v3", "otel_logs_and_spans_rollup_dashboard_1m_v2"),
            ("otel_logs_and_spans_rollup_dashboard_1h_v2", "otel_logs_and_spans_rollup_dashboard_1h_v1"),
            ("otel_metrics_rollup_metrics_1m_v2", "otel_metrics_rollup_metrics_1m_v1"),
            ("otel_metrics_rollup_metrics_1h_v2", "otel_metrics_rollup_metrics_1h_v1"),
        ] {
            if let Some(schema) = schemas.get(current).cloned() {
                schemas.entry(legacy.to_owned()).or_insert(TableSchema { table_name: legacy.to_owned(), ..schema });
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

// Schemas are immutable for the process lifetime, and downstream caches
// (`optimizers::indexed_columns_for`, `plan_cache::PlanCacheHook`) depend on
// that for CORRECTNESS. Adding hot-reload means giving those caches a
// schema-version token, or flushing them on reload.
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

/// `get_schema(table_name)`, falling back to the default schema.
pub fn schema_or_default(table_name: &str) -> &'static TableSchema {
    get_schema(table_name).unwrap_or_else(get_default_schema)
}

/// Inner field names of the unshredded Variant struct
/// (`delta_kernel::unshredded_variant()`).
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

/// Replaces Variant fields with Utf8View, giving the SQL-facing view of a table
/// with Variant columns (`TableProvider::schema()`); `real_schema()` keeps the
/// storage view.
///
/// Needed because `INSERT ... VALUES ('{"a":1}')` fails in
/// `LogicalPlanBuilder::values`: `can_cast_types(Utf8, Struct{Binary,Binary})` is
/// false and datafusion exposes no hook to register a Utf8→Variant coercion for
/// that check. `DataSink::write_all` converts inbound Utf8/Utf8View to the
/// Variant struct before the Delta write.
pub fn create_insert_compatible_schema(schema: &SchemaRef) -> SchemaRef {
    // `tf.pg_type = jsonb` is required: pgwire Describe derives RowDescription
    // from the *unanalyzed* plan, so without the tag Variant columns surface as
    // text OID 25 and strict drivers reject the row. vendor/arrow-pg maps the tag
    // to OID 3802 + the 0x01 binary jsonb version byte.
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|f| match is_variant_type(f.data_type()) {
                true => Arc::new(
                    Field::new(f.name(), ArrowDataType::Utf8View, f.is_nullable()).with_metadata(HashMap::from([("tf.pg_type".into(), "jsonb".into())])),
                ),
                false => f.clone(),
            })
            .collect::<Vec<FieldRef>>(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    const BASE_YAML: &str = "table_name: t\npartitions: []\nsorting_columns: []\nz_order_columns: []\ndedup_keys: [id]\ndedup_tiebreak: updated_at\n";
    const FIELDS_YAML: &str = "fields:\n  - {name: id, data_type: Utf8, nullable: false}\n  - {name: updated_at, data_type: 'Timestamp(Microsecond, None)', nullable: true}\n  - {name: deleted, data_type: Boolean, nullable: true}\n";

    fn parse_schema(extra: &str, fields: &str) -> TableSchema {
        serde_yaml::from_str(&format!("{BASE_YAML}{extra}{fields}")).expect("yaml parses")
    }

    fn source() -> &'static TableSchema {
        get_schema("otel_logs_and_spans").expect("source schema")
    }

    fn measure(name: &str, agg: &str, column: Option<&str>, filter: Option<&str>) -> RollupMeasure {
        RollupMeasure { name: name.into(), agg: agg.into(), column: column.map(str::to_owned), filter: filter.map(str::to_owned) }
    }

    /// A 1m rollup over `kind` — the shape every spec test varies.
    fn spec(name: &str, measures: Vec<RollupMeasure>) -> RollupSpec {
        RollupSpec { grain: "1m".into(), name: Some(name.into()), dimensions: vec!["kind".into()], measures, derive_from: None }
    }

    /// The merge-on-read triple every tombstoned table must declare identically.
    /// The tiebreak MUST name the TF-owned column: `insert_coerce::stamp_version`
    /// overwrites whatever it names, so pointing it at a client column would
    /// destroy client data on every write.
    #[test_case("mor_versioned")]
    fn assert_tombstone_shape(name: &str) {
        let schema = get_schema(name).unwrap_or_else(|| panic!("{name} registered"));
        assert_eq!(schema.tombstone_column.as_deref(), Some("deleted"), "{name} tombstone column");
        // Nullable so no backfill is needed: NULL reads as live.
        assert_eq!(schema.field_def("deleted"), Some((ArrowDataType::Boolean, true)), "{name}.deleted must be nullable Boolean");
        assert_eq!(schema.dedup_tiebreak.as_deref(), Some("updated_at"), "{name} must break ties on the TF-owned stamp");
    }

    #[test_case("List(Utf8)", "indexed: true, tokenizer: raw, list_mode: elements" => true ; "exact string list")]
    #[test_case("Utf8", "indexed: true, tokenizer: raw, list_mode: elements" => false ; "scalar column")]
    #[test_case("List(Int64)", "indexed: true, tokenizer: raw, list_mode: elements" => false ; "non-string elements")]
    #[test_case("List(Utf8)", "indexed: true, list_mode: elements" => false ; "tokenizer must be raw")]
    #[test_case("List(Utf8)", "indexed: false, tokenizer: raw, list_mode: elements" => false ; "not indexed")]
    #[test_case("List(Utf8)", "indexed: true, tokenizer: raw, list_mode: elements, flatten: json" => false ; "flatten is incompatible")]
    #[test_case("List(Utf8)", "indexed: true" => true ; "plain indexed list")]
    fn element_index_configuration_requires_exact_string_lists(data_type: &str, options: &str) -> bool {
        let fields = format!("{FIELDS_YAML}  - name: hashes\n    data_type: {data_type}\n    tantivy: {{{options}}}\n    nullable: true\n");
        parse_schema("", &fields).validate().is_ok()
    }

    #[test]
    fn synthesized_rollup_stores_a_generation_and_tdigest() {
        let rollup = spec("digest_test", vec![measure("digest", "tdigest", Some("duration"), None)]).synthesize(source()).expect("valid rollup");
        assert_eq!(rollup.field_def("rollup_generation"), Some((ArrowDataType::Utf8View, false)));
        // `kind` is tantivy-indexed on the source and must NOT inherit it here —
        // see `synthesize`.
        assert!(rollup.fields.iter().all(|f| f.tantivy.is_none()), "no rollup field may carry a tantivy config");
    }

    /// A `first` measure is only re-aggregable if a companion `min(timestamp)`
    /// measure records which row its value came from, so a spec without one is
    /// refused at load.
    #[test]
    fn a_first_measure_is_refused_without_its_companion() {
        let source = source();
        let landing = |filter: Option<&str>| measure("landing_url", "first", Some("attributes___url___path"), filter);
        let companion = |filter: Option<&str>| measure("at", "min", Some("timestamp"), filter);
        let spec = |measures| spec("first_test", measures);

        assert!(spec(vec![landing(None)]).validate(source).is_err(), "a `first` measure alone must not validate");
        assert!(spec(vec![landing(None), companion(None)]).validate(source).is_ok(), "the companion makes it valid");

        // "earliest row" and "earliest row MATCHING the filter" are different
        // rows, so a companion with a different filter cannot order this measure.
        let filter = "attributes___url___path <> ''";
        assert!(spec(vec![landing(Some(filter)), companion(None)]).validate(source).is_err(), "the companion's filter must match the measure's");
        assert!(spec(vec![landing(Some(filter)), companion(Some(filter))]).validate(source).is_ok());

        // The stored value keeps the source column's type: it IS a value from
        // that column, not a sketch over it.
        let rollup = spec(vec![landing(None), companion(None)]).synthesize(source).expect("valid rollup");
        assert_eq!(rollup.field_def("landing_url").map(|(ty, _)| ty), Some(ArrowDataType::Utf8View));
    }

    /// A sketch measure stores Binary regardless of aggregate, and `hll` — alone
    /// among the aggregates — must accept a NON-numeric source column.
    #[test_case("tdigest", "duration" => Some((ArrowDataType::Binary, true)) ; "tdigest over a numeric column")]
    #[test_case("hll", "context___trace_id" => Some((ArrowDataType::Binary, true)) ; "hll over a string column")]
    #[test_case("hll", "duration" => Some((ArrowDataType::Binary, true)) ; "hll over a numeric column")]
    fn a_sketch_measure_stores_a_binary_sketch(agg: &str, column: &str) -> Option<(ArrowDataType, bool)> {
        spec("sketch_test", vec![measure("sketch", agg, Some(column), None)]).synthesize(source()).expect("valid rollup").field_def("sketch")
    }

    #[test]
    fn unresolvable_rollup_measures_are_refused() {
        let bad = spec("bad_agg", vec![measure("bad", "median", Some("duration"), None)]);
        assert!(bad.validate(source()).unwrap_err().to_string().contains("unsupported aggregate"));
        let unknown = spec("hll_test", vec![measure("traces", "hll", Some("no_such_column"), None)]);
        assert!(unknown.synthesize(source()).is_err(), "an unknown column must still be rejected");
    }

    #[test_case("otel_logs_and_spans_rollup_dashboard_1m_v2")]
    #[test_case("otel_logs_and_spans_rollup_dashboard_1h_v1")]
    #[test_case("otel_metrics_rollup_metrics_1m_v1")]
    #[test_case("otel_metrics_rollup_metrics_1h_v1")]
    fn legacy_rollup_generations_remain_readable_during_migration(name: &str) {
        assert!(get_schema(name).is_some(), "legacy rollup schema {name} must remain registered during canary");
    }

    /// Migrated columns must be declared in the SAME SHAPE AND ORDER the stored
    /// Delta schema was widened in — last, nullable, never before a pre-existing
    /// field. Otherwise the write path builds batches the transaction log's
    /// column set does not match, and every write fails.
    /// `migrate-columns` APPENDS, so a later migration must extend a row's list
    /// at the end, never insert mid-list.
    #[test_case("otel_logs_and_spans", &["updated_at", "deleted", "attributes___http___route"] ; "otel_logs_and_spans")]
    #[test_case("otel_metrics", &["updated_at", "deleted"] ; "otel_metrics")]
    fn shipped_mor_tables_declare_the_migrated_columns_last(name: &str, migrated: &[&str]) {
        let schema = get_schema(name).unwrap_or_else(|| panic!("{name} registered"));
        assert!(schema.version_append, "{name} ships merge-on-read");
        assert_tombstone_shape(name);
        assert!(matches!(schema.field_def("updated_at"), Some((ArrowDataType::Timestamp(..), true))), "{name}.updated_at must be a nullable timestamp");
        let tail: Vec<&str> = schema.fields.iter().rev().take(migrated.len()).map(|f| f.name.as_str()).rev().collect();
        assert_eq!(tail, migrated, "{name}: migrated columns must be the LAST fields, in migration order (7d68f01)");
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

    /// `tombstones_possible` is what the COUNT(*) stats pushdown gates on, so it
    /// must track the declared column rather than the write-path flag: turning
    /// `version_append` off does not delete tombstones already written.
    #[test_case("" => false ; "no tombstone column at all")]
    #[test_case("tombstone_column: deleted\n" => true ; "declared column alone means tombstones may exist with the write path off")]
    #[test_case("tombstone_column: deleted\nversion_append: true\n" => true ; "write path on")]
    fn tombstones_possible_tracks_storage_not_the_write_path(extra: &str) -> bool {
        parse_schema(extra, FIELDS_YAML).tombstones_possible()
    }

    /// `mor_dormant` declares a tiebreak but NO tombstone column, so nothing could
    /// ever have tombstoned it — the stats fast path stays available.
    #[test_case("otel_logs_and_spans" => true ; "otel ships merge-on-read")]
    #[test_case("mor_versioned" => true ; "mor_versioned")]
    #[test_case("mor_dormant" => false ; "tiebreak without a tombstone column")]
    fn shipped_schemas_report_tombstones_only_when_declared(name: &str) -> bool {
        get_schema(name).unwrap().tombstones_possible()
    }

    #[test]
    fn otel_metrics_schema_supports_native_metric_values() {
        let schema = get_schema("otel_metrics").expect("metrics schema registered");
        assert_eq!(schema.partitions, ["project_id", "date"]);
        let fields = schema.fields().expect("metrics fields parse");
        assert!(matches!(fields.iter().find(|f| f.name() == "value").map(|f| f.data_type()), Some(ArrowDataType::Float64)));
        assert!(matches!(fields.iter().find(|f| f.name() == "hist_bucket_counts").map(|f| f.data_type()), Some(ArrowDataType::List(_))));
    }

    /// A footer `SortingColumn.column_idx` indexes parquet LEAVES, and a
    /// Variant/struct column is several leaves, so each recorded index must
    /// resolve back to the column it names. Asserting hardcoded indices would
    /// just re-encode a miscount.
    #[test]
    fn footer_sort_indices_resolve_to_the_columns_they_name() {
        fn leaf_paths(name: &str, data_type: &ArrowDataType, out: &mut Vec<String>) {
            match data_type {
                ArrowDataType::Struct(fields) => fields.iter().for_each(|f| leaf_paths(f.name(), f.data_type(), out)),
                ArrowDataType::List(f) | ArrowDataType::LargeList(f) | ArrowDataType::FixedSizeList(f, _) => leaf_paths(name, f.data_type(), out),
                ArrowDataType::Map(entries, _) => leaf_paths(name, entries.data_type(), out),
                _ => out.push(name.to_owned()),
            }
        }
        for table in ["otel_logs_and_spans", "otel_metrics"] {
            let schema = get_schema(table).expect("shipped schema");
            let partitions = schema.partition_set();
            let fields = schema.fields().expect("arrow fields");
            let mut leaves = Vec::new();
            for (declared, field) in schema.fields.iter().zip(&fields).filter(|(d, _)| !partitions.contains(d.name.as_str())) {
                leaf_paths(&declared.name, field.data_type(), &mut leaves);
            }
            let recorded = schema.sorting_columns();
            assert_eq!(recorded.len(), schema.sorting_columns.len(), "{table}: every declared sorting column must be recorded");
            for (column, sorting) in schema.sorting_columns.iter().zip(&recorded) {
                let idx = usize::try_from(sorting.column_idx).expect("non-negative leaf index");
                assert_eq!(
                    leaves.get(idx).map(String::as_str),
                    Some(column.name.as_str()),
                    "{table}: `{}` was recorded as leaf {idx}, which is `{:?}` ({} leaves total)",
                    column.name,
                    leaves.get(idx),
                    leaves.len()
                );
            }
            assert!(leaves.len() >= fields.len(), "{table}: leaves cannot be fewer than fields, or this test is measuring nothing");
            // Partition columns live in the path, not the file, so they consume
            // no leaf: `timestamp` at leaf 0 proves `date` (declared first) was
            // excluded.
            assert_eq!(leaves.first().map(String::as_str), Some("timestamp"), "{table}: the lead sort key must be leaf 0");
            for partition in &schema.partitions {
                assert!(!leaves.contains(partition), "{table}: partition `{partition}` must not occupy a parquet leaf");
            }
        }
    }
}
