use std::sync::Arc;
use std::collections::HashMap;
use std::sync::OnceLock;
use arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};
use arrow::datatypes::DataType as ArrowDataType;
use delta_kernel::parquet::format::SortingColumn;
use deltalake::kernel::{StructField, DataType as DeltaDataType, PrimitiveType, ArrayType};
use serde::{Deserialize, Serialize};

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
        let fields = self.fields().unwrap_or_else(|e| {
            log::error!("Failed to get fields: {:?}", e);
            Vec::new()
        });
        Arc::new(Schema::new(fields))
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
        "Utf8" => ArrowDataType::Utf8,
        "Date32" => ArrowDataType::Date32,
        "Int32" => ArrowDataType::Int32,
        "Int64" => ArrowDataType::Int64,
        "UInt32" => ArrowDataType::UInt32,
        "UInt64" => ArrowDataType::UInt64,
        "List(Utf8)" => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8, true))),
        "Timestamp(Microsecond, None)" => ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        "Timestamp(Microsecond, Some(\"UTC\"))" => ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
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
        _ if s.starts_with("Timestamp") => DeltaDataType::Primitive(Timestamp),
        _ => anyhow::bail!("Unknown type: {}", s),
    })
}

// Include all schema YAML files at compile time
macro_rules! include_schemas {
    () => {{
        vec![
            ("otel_logs_and_spans", include_str!("../schemas/otel_logs_and_spans.yaml")),
            ("metrics", include_str!("../schemas/metrics.yaml")),
            ("events", include_str!("../schemas/events.yaml")),
            // Add more schemas here as they are added to the schemas directory
        ]
    }};
}

pub struct SchemaRegistry {
    schemas: HashMap<String, TableSchema>,
}

impl SchemaRegistry {
    fn new() -> Self {
        let mut schemas = HashMap::new();
        
        // Load all schemas at compile time
        for (name, yaml_content) in include_schemas!() {
            match serde_yaml::from_str::<TableSchema>(yaml_content) {
                Ok(schema) => {
                    schemas.insert(schema.table_name.clone(), schema);
                }
                Err(e) => {
                    panic!("Failed to parse schema {}: {}", name, e);
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
        self.schemas.get("otel_logs_and_spans")
            .or_else(|| self.schemas.values().next())
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
    registry().get_default()
        .expect("No schemas available in registry")
}