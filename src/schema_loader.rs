use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use arrow::datatypes::{DataType as ArrowDataType, Field, FieldRef, Schema, SchemaRef};
use deltalake::{
    datafusion::parquet::file::metadata::SortingColumn,
    kernel::{ArrayType, DataType as DeltaDataType, PrimitiveType, StructField},
};
use include_dir::{Dir, include_dir};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableSchema {
    pub table_name: String,
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
    /// A `Timestamp(Microsecond, _)` tiebreak is TF-owned: every write stamps it
    /// from a per-table monotonic clock (`insert_coerce::stamp_version`), so the
    /// newest version of a row wins deterministically. `None` = keep-last by position.
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
    /// version on disk gate on THIS flag, not on `tombstone_column`: see
    /// [`Self::tombstones_possible`] (COUNT(*)-from-stats pushdown) and the
    /// order-preserving union in `ProjectRoutingTable::scan` (which exists only
    /// to make `DedupExec`'s keep-greatest engage). Declaring the column is
    /// inert, so gating those on the column alone costs a full scan / a blocking
    /// sort + k-way merge for a feature that cannot yet have written anything.
    #[serde(default)]
    pub version_append: bool,
}

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

    /// Can a tombstone row EXIST in this table's storage? Only if the column is
    /// declared AND the write path that emits tombstones is enabled — a declared
    /// column with `version_append: false` is a no-op (see `tombstone_column`).
    ///
    /// ORDERING INVARIANT: `version_append` is the flag that *enables* writing a
    /// tombstone, so it is necessarily true strictly before the first tombstone
    /// exists; a reader that trusts it therefore never counts one it hasn't
    /// accounted for. Never add a write path that appends a tombstone (or any
    /// second row version) without gating it on `version_append`, and never flip
    /// the flag off after tombstones were written — either breaks this and turns
    /// the COUNT(*) stats pushdown into a silent over-count.
    pub fn tombstones_possible(&self) -> bool {
        self.tombstone_column.is_some() && self.version_append
    }

    fn validate(&self) -> anyhow::Result<()> {
        for k in &self.dedup_keys {
            let f = self
                .fields
                .iter()
                .find(|f| f.name == *k)
                .ok_or_else(|| anyhow::anyhow!("schema `{}`: dedup_keys references unknown field `{}`", self.table_name, k))?;
            if f.data_type == "Variant" {
                anyhow::bail!("schema `{}`: dedup_keys cannot include Variant column `{}`", self.table_name, k);
            }
        }
        if let Some(tb) = &self.dedup_tiebreak {
            let f = self
                .fields
                .iter()
                .find(|f| f.name == *tb)
                .ok_or_else(|| anyhow::anyhow!("schema `{}`: dedup_tiebreak references unknown field `{}`", self.table_name, tb))?;
            if f.data_type == "Variant" {
                anyhow::bail!("schema `{}`: dedup_tiebreak cannot be a Variant column `{}`", self.table_name, tb);
            }
        }
        if let Some(tc) = &self.tombstone_column {
            let f = self
                .fields
                .iter()
                .find(|f| f.name == *tc)
                .ok_or_else(|| anyhow::anyhow!("schema `{}`: tombstone_column references unknown field `{}`", self.table_name, tc))?;
            // Nullable Boolean is load-bearing: NULL must be a legal "live"
            // encoding so existing rows need no backfill.
            if f.data_type != "Boolean" || !f.nullable {
                anyhow::bail!("schema `{}`: tombstone_column `{}` must be a nullable Boolean field", self.table_name, tc);
            }
        }
        if self.version_append && (self.dedup_keys.is_empty() || self.dedup_tiebreak.is_none() || self.tombstone_column.is_none()) {
            anyhow::bail!("schema `{}`: version_append requires dedup_keys, dedup_tiebreak and tombstone_column", self.table_name);
        }
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
                let data_type = parse_arrow_data_type(&f.data_type)?;
                let mut field = Field::new(&f.name, data_type, f.nullable);
                // Mark Variant fields with the Arrow ExtensionType key so
                // downstream code that does `Field::try_extension_type::<VariantType>()`
                // (delta-rs main, parquet-variant-compute) doesn't panic
                // with "Extension type name missing". Without this, fresh
                // tables (variant_bench) crash on the first INSERT.
                if f.data_type == "Variant" {
                    use std::collections::HashMap;
                    let mut md: HashMap<String, String> = field.metadata().clone();
                    md.insert("ARROW:extension:name".into(), "arrow.parquet.variant".into());
                    field = field.with_metadata(md);
                }
                Ok(Arc::new(field) as FieldRef)
            })
            .collect()
    }

    pub fn columns(&self) -> anyhow::Result<Vec<StructField>> {
        self.fields
            .iter()
            .map(|f| {
                let data_type = parse_delta_data_type(&f.data_type)?;
                Ok(StructField::new(&f.name, data_type, f.nullable))
            })
            .collect()
    }

    pub fn schema_ref(&self) -> SchemaRef {
        // Return schema with partition columns moved to the end to match Delta Lake's output order
        let all_fields = self.fields().unwrap_or_else(|e| panic!("Failed to build schema for table {}: {e:?}", self.table_name));

        let partition_set: std::collections::HashSet<&str> = self.partitions.iter().map(|s| s.as_str()).collect();

        // Separate non-partition and partition fields, maintaining order within each group
        let mut non_partition_fields = Vec::new();
        let mut partition_fields = Vec::new();

        for field in all_fields {
            if partition_set.contains(field.name().as_str()) {
                partition_fields.push(field);
            } else {
                non_partition_fields.push(field);
            }
        }

        // Combine: non-partition fields first, then partition fields at the end
        non_partition_fields.extend(partition_fields);
        Arc::new(Schema::new(non_partition_fields))
    }

    pub fn sorting_columns(&self) -> Vec<SortingColumn> {
        // Parquet data files omit partition columns (they live in the path), so
        // the `SortingColumn.column_idx` the footer records must be the column's
        // position among the *non-partition* fields — the physical parquet leaf
        // order the reader (`ordering_from_parquet_metadata`) indexes into. Using
        // the raw fields-list index over-counts by every partition column that
        // precedes a sort key (e.g. `date` at field 0), so the footer points at
        // the wrong column and the sort-order pushdown silently never fires.
        let partition_set: std::collections::HashSet<&str> = self.partitions.iter().map(|s| s.as_str()).collect();
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
        "UInt32" => ArrowDataType::UInt32,
        "UInt64" => ArrowDataType::UInt64,
        "List(Utf8)" => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8View, true))),
        "List(Int64)" => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int64, true))),
        "List(Float64)" => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Float64, true))),
        "Timestamp(Microsecond, None)" => ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        "Timestamp(Microsecond, Some(\"UTC\"))" => ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
        // Variant: declare the inner buffers as Binary to match
        // `delta_kernel::unshredded_variant()`. delta-rs's kernel rejects
        // schema mismatches at scan validation time even when no data
        // files exist (e.g. fresh DELETE on an empty table). Both
        // MemBuffer and Delta reads end up as Binary because:
        //   - the parquet reader honors `schema_force_view_types=false`
        //     (set in our session and in `delta_session_from` for DML);
        //   - `convert_variant_columns` casts VariantArrayBuilder's
        //     BinaryView output to Binary before MemBuffer ever sees it.
        // The ExtensionType marker (`ARROW:extension:name = arrow.parquet.variant`)
        // is added to the Field's metadata in `fields()` below.
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
        let mut schemas = HashMap::new();

        // Load all YAML schemas from the directory
        for file in SCHEMAS_DIR.files() {
            if file.path().extension().and_then(|s| s.to_str()) == Some("yaml") {
                let content = file.contents_utf8().expect("Schema file should be UTF-8");
                match serde_yaml::from_str::<TableSchema>(content) {
                    Ok(schema) => {
                        if let Err(e) = schema.validate() {
                            panic!("Invalid schema {:?}: {}", file.path(), e);
                        }
                        schemas.insert(schema.table_name.clone(), schema);
                    }
                    Err(e) => {
                        panic!("Failed to parse schema {:?}: {}", file.path(), e);
                    }
                }
            }
        }

        Self { schemas }
    }

    pub fn get(&self, table_name: &str) -> Option<&TableSchema> {
        self.schemas.get(table_name)
    }

    pub fn get_default(&self) -> Option<&TableSchema> {
        // Return the first schema as default (for backward compatibility)
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
//   - `optimizers::tantivy_rewriter::indexed_columns_for` (per-table tokenizer map)
//   - `plan_cache::PlanCacheHook` (LogicalPlan embeds SchemaRef at parse time)
// If hot-reload of YAML schemas is ever added, those caches must gain a
// schema-version token in their key (or be flushed on reload).
static SCHEMA_REGISTRY: OnceLock<SchemaRegistry> = OnceLock::new();

pub fn registry() -> &'static SchemaRegistry {
    SCHEMA_REGISTRY.get_or_init(SchemaRegistry::new)
}

// Convenience function to get a schema by name
pub fn get_schema(table_name: &str) -> Option<&'static TableSchema> {
    registry().get(table_name)
}

// Get the default schema (for backward compatibility)
pub fn get_default_schema() -> &'static TableSchema {
    registry().get_default().expect("No schemas available in registry")
}

/// Inner field names of the unshredded Variant struct
/// (`delta_kernel::unshredded_variant()`). Centralized here so any writer or
/// validator that constructs a Variant struct uses the same names; if
/// delta-kernel ever renames these, only this file changes.
pub const VARIANT_METADATA_FIELD: &str = "metadata";
pub const VARIANT_VALUE_FIELD: &str = "value";

/// Returns true if the given Arrow DataType structurally matches a Variant
/// (Struct with `metadata` + `value` binary/binaryview fields).
pub fn is_variant_type(data_type: &ArrowDataType) -> bool {
    match data_type {
        ArrowDataType::Struct(fields) if fields.len() == 2 => {
            fields.iter().any(|f| f.name() == VARIANT_METADATA_FIELD && matches!(f.data_type(), ArrowDataType::Binary | ArrowDataType::BinaryView))
                && fields.iter().any(|f| f.name() == VARIANT_VALUE_FIELD && matches!(f.data_type(), ArrowDataType::Binary | ArrowDataType::BinaryView))
        }
        _ => false,
    }
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
    let new_fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .map(|f| {
            if is_variant_type(f.data_type()) {
                // `tf.pg_type = jsonb`: pgwire Describe derives RowDescription from the
                // *unanalyzed* plan, where Variant cols carry this Utf8View view. Without
                // the tag, bare Variant columns surface text OID 25 and strict drivers
                // (hasql) reject the row (expected jsonb 3802). vendor/arrow-pg maps the
                // tag to OID 3802 + the 0x01 binary jsonb version byte.
                let md = [("tf.pg_type".to_string(), "jsonb".to_string())].into_iter().collect();
                Arc::new(Field::new(f.name(), ArrowDataType::Utf8View, f.is_nullable()).with_metadata(md))
            } else {
                f.clone()
            }
        })
        .collect();
    Arc::new(Schema::new(new_fields))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let partition_set: std::collections::HashSet<&str> = schema.partitions.iter().map(|s| s.as_str()).collect();
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

    /// The tombstone column must be appended LAST (no existing column's physical
    /// parquet index may shift), nullable Boolean (NULL = live, so no backfill),
    /// and named by the schema — nothing hard-codes it. `version_append` stays
    /// off: it gates only the phase-3 write path, and decoupling it from
    /// `tombstone_column` keeps rollback from resurrecting deleted rows.
    #[test]
    fn otel_tombstone_column_is_last_and_nullable_boolean() {
        let schema = get_schema("otel_logs_and_spans").expect("otel schema registered");
        assert_eq!(schema.tombstone_column.as_deref(), Some("deleted"));
        assert!(!schema.version_append, "write-path flag must stay off until the version-append path lands");
        let last = schema.fields.last().expect("fields non-empty");
        assert_eq!(last.name, "deleted", "tombstone column must be appended LAST");
        assert_eq!(schema.field_def("deleted"), Some((ArrowDataType::Boolean, true)));
    }

    /// Tables that declare no tombstone column are untouched by any of it.
    #[test]
    fn otel_metrics_has_no_tombstone_column() {
        let schema = get_schema("otel_metrics").expect("metrics schema registered");
        assert_eq!(schema.tombstone_column, None);
        assert!(!schema.version_append);
        assert_eq!(schema.dedup_tiebreak.as_deref(), Some("ingested_at"));
    }

    #[test]
    fn validate_rejects_bad_tombstone_and_version_append_declarations() {
        let base = "table_name: t\npartitions: []\nsorting_columns: []\nz_order_columns: []\ndedup_keys: [id]\ndedup_tiebreak: updated_at\n";
        let fields = "fields:\n  - {name: id, data_type: Utf8, nullable: false}\n  - {name: updated_at, data_type: 'Timestamp(Microsecond, None)', nullable: true}\n  - {name: deleted, data_type: Boolean, nullable: true}\n";
        let parse = |extra: &str, fields: &str| serde_yaml::from_str::<TableSchema>(&format!("{base}{extra}{fields}")).expect("yaml parses").validate();

        parse("tombstone_column: deleted\nversion_append: true\n", fields).expect("well-formed version-append table");
        assert!(parse("tombstone_column: missing\n", fields).unwrap_err().to_string().contains("unknown field"));
        assert!(parse("tombstone_column: id\n", fields).unwrap_err().to_string().contains("nullable Boolean"));
        let non_null = fields.replace("deleted, data_type: Boolean, nullable: true", "deleted, data_type: Boolean, nullable: false");
        assert!(parse("tombstone_column: deleted\n", &non_null).unwrap_err().to_string().contains("nullable Boolean"));
        assert!(parse("version_append: true\n", fields).unwrap_err().to_string().contains("version_append requires"));
    }

    /// A declared-but-dormant tombstone column must NOT read as "tombstones can
    /// exist" — that verdict is what the COUNT(*) stats pushdown gates on, and
    /// reading it off the column alone cost every `COUNT(*)` on the main table
    /// its fast path (pre-deploy review, 2026-07-31).
    #[test]
    fn tombstones_possible_needs_the_write_path_too() {
        let base = "table_name: t\npartitions: []\nsorting_columns: []\nz_order_columns: []\ndedup_keys: [id]\ndedup_tiebreak: updated_at\n";
        let fields = "fields:\n  - {name: id, data_type: Utf8, nullable: false}\n  - {name: updated_at, data_type: 'Timestamp(Microsecond, None)', nullable: true}\n  - {name: deleted, data_type: Boolean, nullable: true}\n";
        let parse = |extra: &str| serde_yaml::from_str::<TableSchema>(&format!("{base}{extra}{fields}")).expect("yaml parses");

        assert!(!parse("").tombstones_possible(), "no tombstone column at all");
        assert!(!parse("tombstone_column: deleted\n").tombstones_possible(), "declared but dormant → no tombstone can exist");
        assert!(parse("tombstone_column: deleted\nversion_append: true\n").tombstones_possible(), "write path on → tombstones can exist");
        // The live schemas, as shipped.
        assert!(!get_schema("otel_logs_and_spans").unwrap().tombstones_possible());
        assert!(get_schema("mor_versioned").unwrap().tombstones_possible());
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
