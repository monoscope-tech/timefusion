use arrow::datatypes::FieldRef;
use arrow_schema::SchemaRef;
use delta_kernel::parquet::format::SortingColumn;
use deltalake::kernel::StructField;
use log::debug;
use std::sync::OnceLock;

use crate::schema_loader::TableSchema;
use crate::load_schema;

static OTEL_SCHEMA: OnceLock<TableSchema> = OnceLock::new();

pub fn get_otel_schema() -> &'static TableSchema {
    OTEL_SCHEMA.get_or_init(|| load_schema!("../schemas/otel_logs_and_spans.yaml"))
}

// Helper struct for accessing schema information
pub struct OtelLogsAndSpans;

impl OtelLogsAndSpans {
    pub fn table_name() -> String {
        get_otel_schema().table_name.clone()
    }

    #[allow(dead_code)]
    pub fn fields() -> anyhow::Result<Vec<FieldRef>> {
        get_otel_schema().fields()
    }

    pub fn columns() -> anyhow::Result<Vec<StructField>> {
        let columns = get_otel_schema().columns()?;
        debug!("schema_field columns {:?}", columns);
        Ok(columns)
    }

    pub fn schema_ref() -> SchemaRef {
        get_otel_schema().schema_ref()
    }

    pub fn partitions() -> Vec<String> {
        get_otel_schema().partitions.clone()
    }

    pub fn sorting_columns() -> Vec<SortingColumn> {
        get_otel_schema().sorting_columns()
    }

    pub fn z_order_columns() -> Vec<String> {
        get_otel_schema().z_order_columns.clone()
    }
}

