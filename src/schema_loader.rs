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
    /// version on disk gate on THIS flag, not on `tombstone_column`: see
    /// [`Self::tombstones_possible`] (COUNT(*)-from-stats pushdown) and the
    /// order-preserving union in `ProjectRoutingTable::scan` (which exists only
    /// to make `DedupExec`'s keep-greatest engage). Declaring the column is
    /// inert, so gating those on the column alone costs a full scan / a blocking
    /// sort + k-way merge for a feature that cannot yet have written anything.
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
        let schemas = SCHEMAS_DIR
            .files()
            .filter(|f| f.path().extension().and_then(|s| s.to_str()) == Some("yaml"))
            .map(|file| {
                let content = file.contents_utf8().expect("Schema file should be UTF-8");
                let schema: TableSchema = serde_yaml::from_str(content).unwrap_or_else(|e| panic!("Failed to parse schema {:?}: {}", file.path(), e));
                schema.validate().unwrap_or_else(|e| panic!("Invalid schema {:?}: {}", file.path(), e));
                (schema.table_name.clone(), schema)
            })
            .collect();
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
//   - `optimizers::tantivy_rewriter::indexed_columns_for` (per-table tokenizer map)
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
    /// `mor_versioned`, the from-scratch fixture: it is the ONLY table allowed to
    /// declare these columns (see the migration note above `TableSchema`).
    #[test]
    fn mor_versioned_tombstone_column_is_nullable_boolean() {
        let schema = get_schema("mor_versioned").expect("fixture registered");
        assert_eq!(schema.tombstone_column.as_deref(), Some("deleted"));
        assert_eq!(schema.field_def("deleted"), Some((ArrowDataType::Boolean, true)));
        assert_eq!(schema.dedup_tiebreak.as_deref(), Some("updated_at"));
    }

    /// The SHIPPED tables must not have gained merge-on-read columns. Adding one
    /// to a table that already has live Delta data broke prod (7d68f01): every
    /// existing Delta table still stores the old column set, and the write path
    /// builds batches to the YAML's. Guards the specific columns that did it.
    #[test]
    fn shipped_tables_declare_no_merge_on_read_columns() {
        for name in ["otel_logs_and_spans", "otel_metrics"] {
            let schema = get_schema(name).unwrap_or_else(|| panic!("{name} registered"));
            assert_eq!(schema.tombstone_column, None, "{name} is a shipped table — a tombstone column needs a Delta-side migration first");
            assert!(!schema.version_append, "{name} is a shipped table — version_append needs the columns, which need a migration");
            for col in ["updated_at", "deleted"] {
                assert!(schema.field_def(col).is_none(), "{name} must not declare `{col}`: prod's Delta tables have no such field (7d68f01)");
            }
        }
        // The tiebreaks these tables actually ship with — client-supplied, and
        // therefore never stamped (see `insert_coerce::stamp_column`).
        assert_eq!(get_schema("otel_logs_and_spans").unwrap().dedup_tiebreak.as_deref(), Some("observed_timestamp"));
        assert_eq!(get_schema("otel_metrics").unwrap().dedup_tiebreak.as_deref(), Some("ingested_at"));
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
    fn tombstones_possible_needs_the_write_path_too() {
        let parse = |extra: &str| parse_schema(extra, FIELDS_YAML);

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
