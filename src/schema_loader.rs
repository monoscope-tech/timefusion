use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};
use deltalake::datafusion::parquet::file::metadata::SortingColumn;
use deltalake::kernel::{ArrayType, DataType as DeltaDataType, PrimitiveType, StructField};
use include_dir::{Dir, include_dir};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableSchema {
    pub table_name: String,
    pub partitions: Vec<String>,
    pub sorting_columns: Vec<SortingColumnDef>,
    pub z_order_columns: Vec<String>,
    pub fields: Vec<FieldDef>,
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
}

impl TableSchema {
    pub fn fields(&self) -> anyhow::Result<Vec<FieldRef>> {
        self.fields
            .iter()
            .map(|f| {
                let data_type = parse_arrow_data_type(&f.data_type)?;
                Ok(Arc::new(Field::new(&f.name, data_type, f.nullable)) as FieldRef)
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
        let all_fields = self.fields().unwrap_or_else(|e| {
            log::error!("Failed to get fields: {:?}", e);
            Vec::new()
        });

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
        "Int32" => ArrowDataType::Int32,
        "Int64" => ArrowDataType::Int64,
        "UInt32" => ArrowDataType::UInt32,
        "UInt64" => ArrowDataType::UInt64,
        "List(Utf8)" => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8View, true))),
        "Timestamp(Microsecond, None)" => ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        "Timestamp(Microsecond, Some(\"UTC\"))" => ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
        // Variant Binary Encoding: must use Binary (not BinaryView) to match
        // delta_kernel's unshredded_variant() representation.
        "Variant" => ArrowDataType::Struct(
            vec![
                Arc::new(Field::new("metadata", ArrowDataType::Binary, false)),
                Arc::new(Field::new("value", ArrowDataType::Binary, false)),
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

// Global registry instance
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

/// Returns true if the given Arrow DataType structurally matches a Variant
/// (Struct with `metadata` + `value` binary/binaryview fields).
pub fn is_variant_type(data_type: &ArrowDataType) -> bool {
    match data_type {
        ArrowDataType::Struct(fields) if fields.len() == 2 => {
            fields.iter().any(|f| f.name() == "metadata" && matches!(f.data_type(), ArrowDataType::Binary | ArrowDataType::BinaryView))
                && fields.iter().any(|f| f.name() == "value" && matches!(f.data_type(), ArrowDataType::Binary | ArrowDataType::BinaryView))
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
                Arc::new(Field::new(f.name(), ArrowDataType::Utf8View, f.is_nullable()))
            } else {
                f.clone()
            }
        })
        .collect();
    Arc::new(Schema::new(new_fields))
}

