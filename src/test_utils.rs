pub mod test_helpers {
    use crate::schema_loader::get_default_schema;
    use arrow_json::ReaderBuilder;
    use datafusion::arrow::record_batch::RecordBatch;
    use serde_json::{Value, json};
    use std::collections::HashMap;

    pub fn json_to_batch(records: Vec<Value>) -> anyhow::Result<RecordBatch> {
        let schema = get_default_schema().schema_ref();
        let json_data = records.into_iter().map(|v| v.to_string()).collect::<Vec<_>>().join("\n");

        ReaderBuilder::new(schema.clone())
            .build(std::io::Cursor::new(json_data.as_bytes()))?
            .next()
            .ok_or_else(|| anyhow::anyhow!("Failed to read batch"))?
            .map_err(Into::into)
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
