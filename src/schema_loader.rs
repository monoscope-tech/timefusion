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
}

impl TableSchema {
    pub fn time_column_name(&self) -> &str {
        self.time_column.as_deref().unwrap_or("timestamp")
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
        self.sorting_columns
            .iter()
            .filter_map(|col| {
                self.fields.iter().position(|f| f.name == col.name).map(|idx| SortingColumn {
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
        "UInt32" => ArrowDataType::UInt32,
        "UInt64" => ArrowDataType::UInt64,
        "List(Utf8)" => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8View, true))),
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
        "List(Utf8)" => DeltaDataType::Array(Box::new(ArrayType::new(DeltaDataType::Primitive(String), true))),
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
            fields
                .iter()
                .any(|f| f.name() == VARIANT_METADATA_FIELD && matches!(f.data_type(), ArrowDataType::Binary | ArrowDataType::BinaryView))
                && fields
                    .iter()
                    .any(|f| f.name() == VARIANT_VALUE_FIELD && matches!(f.data_type(), ArrowDataType::Binary | ArrowDataType::BinaryView))
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
