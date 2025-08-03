use std::sync::Arc;
use arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};
use arrow::datatypes::DataType as ArrowDataType;
use delta_kernel::parquet::format::SortingColumn;
use deltalake::kernel::{StructField, DataType as DeltaDataType, PrimitiveType, ArrayType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    pub partitions: Vec<String>,
    pub sorting_columns: Vec<SortingColumnDef>,
    pub z_order_columns: Vec<String>,
    pub fields: Vec<FieldDef>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SortingColumnDef {
    pub name: String,
    pub descending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Serialize, Deserialize)]
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

fn parse_arrow_data_type(type_str: &str) -> anyhow::Result<ArrowDataType> {
    match type_str {
        "Utf8" => Ok(ArrowDataType::Utf8),
        "Date32" => Ok(ArrowDataType::Date32),
        "Int32" => Ok(ArrowDataType::Int32),
        "Int64" => Ok(ArrowDataType::Int64),
        "UInt32" => Ok(ArrowDataType::UInt32),
        "UInt64" => Ok(ArrowDataType::UInt64),
        "List(Utf8)" => Ok(ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8, true)))),
        "Timestamp(Microsecond, None)" => Ok(ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)),
        "Timestamp(Microsecond, Some(\"UTC\"))" => Ok(ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))),
        _ => Err(anyhow::anyhow!("Unknown data type: {}", type_str)),
    }
}

fn parse_delta_data_type(type_str: &str) -> anyhow::Result<DeltaDataType> {
    match type_str {
        "Utf8" => Ok(DeltaDataType::Primitive(PrimitiveType::String)),
        "Date32" => Ok(DeltaDataType::Primitive(PrimitiveType::Date)),
        "Int32" => Ok(DeltaDataType::Primitive(PrimitiveType::Integer)),
        "Int64" => Ok(DeltaDataType::Primitive(PrimitiveType::Long)),
        "UInt32" => Ok(DeltaDataType::Primitive(PrimitiveType::Integer)),
        "UInt64" => Ok(DeltaDataType::Primitive(PrimitiveType::Long)),
        "List(Utf8)" => Ok(DeltaDataType::Array(Box::new(ArrayType::new(
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        )))),
        "Timestamp(Microsecond, None)" => Ok(DeltaDataType::Primitive(PrimitiveType::Timestamp)),
        "Timestamp(Microsecond, Some(\"UTC\"))" => Ok(DeltaDataType::Primitive(PrimitiveType::Timestamp)),
        _ => Err(anyhow::anyhow!("Unknown data type: {}", type_str)),
    }
}

#[macro_export]
macro_rules! load_schema {
    ($path:literal) => {{
        const YAML_CONTENT: &str = include_str!($path);
        serde_yaml::from_str::<$crate::schema_loader::TableSchema>(YAML_CONTENT).expect("Failed to parse schema YAML")
    }};
}