pub mod test_helpers {
    use crate::schema_loader::get_default_schema;
    use crate::variant_utils::{VARIANT_COLUMNS, convert_batch_json_to_variant, variant_data_type};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_json::ReaderBuilder;
    use datafusion::arrow::record_batch::RecordBatch;
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::sync::Arc;

    pub fn json_to_batch(records: Vec<Value>) -> anyhow::Result<RecordBatch> {
        let schema = get_default_schema().schema_ref();
        // Create a parsing schema with Utf8 for Variant columns (arrow_json can't parse Variant)
        let parse_schema = Arc::new(Schema::new(
            schema.fields().iter().map(|f| {
                if VARIANT_COLUMNS.contains(&f.name().as_str()) && *f.data_type() == variant_data_type() {
                    Arc::new(Field::new(f.name(), DataType::Utf8, f.is_nullable()))
                } else {
                    f.clone()
                }
            }).collect::<Vec<_>>()
        ));
        let json_data = records.into_iter().map(|v| v.to_string()).collect::<Vec<_>>().join("\n");

        let batch = ReaderBuilder::new(parse_schema)
            .build(std::io::Cursor::new(json_data.as_bytes()))?
            .next()
            .ok_or_else(|| anyhow::anyhow!("Failed to read batch"))??;

        // Convert Utf8 JSON columns to Variant
        convert_batch_json_to_variant(batch).map_err(|e| anyhow::anyhow!("{}", e))
    }

    pub fn create_default_record() -> HashMap<String, Value> {
        get_default_schema()
            .fields
            .iter()
            .map(|field| {
                let value = if field.data_type == "List(Utf8)" { json!([]) } else { Value::Null };
                (field.name.clone(), value)
            })
            .collect()
    }

    pub fn test_span(id: &str, name: &str, project_id: &str) -> Value {
        json!({
            "timestamp": chrono::Utc::now().timestamp_micros(),
            "id": id,
            "name": name,
            "project_id": project_id,
            "date": chrono::Utc::now().date_naive().to_string(),
            "hashes": [],
            "summary": vec![format!("Test span: {}", name)]
        })
    }
}
