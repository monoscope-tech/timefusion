#[cfg(test)]
pub mod test_helpers {
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use crate::persistent_queue::get_otel_schema;
    use serde_json::Value;
    use std::collections::HashMap;
    
    /// Create a RecordBatch from JSON values
    /// Each JSON object should have field names matching the schema
    pub fn json_to_batch(records: Vec<Value>) -> anyhow::Result<RecordBatch> {
        if records.is_empty() {
            return Err(anyhow::anyhow!("Cannot create batch from empty records"));
        }
        
        let schema = get_otel_schema();
        let arrow_schema = schema.schema_ref();
        let num_records = records.len();
        
        // Build arrays for each column
        let mut arrays: Vec<Arc<dyn Array>> = vec![];
        
        for field in arrow_schema.fields() {
            let array: Arc<dyn Array> = match field.data_type() {
                DataType::Utf8 => {
                    let mut builder = StringBuilder::new();
                    for record in &records {
                        if let Some(value) = record.get(field.name()) {
                            match value {
                                Value::String(s) => builder.append_value(s),
                                Value::Null => builder.append_null(),
                                _ => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                },
                DataType::Int32 => {
                    let mut builder = Int32Builder::new();
                    for record in &records {
                        if let Some(value) = record.get(field.name()) {
                            match value {
                                Value::Number(n) => {
                                    if let Some(i) = n.as_i64() {
                                        builder.append_value(i as i32);
                                    } else {
                                        builder.append_null();
                                    }
                                },
                                Value::Null => builder.append_null(),
                                _ => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                },
                DataType::Int64 => {
                    let mut builder = Int64Builder::new();
                    for record in &records {
                        if let Some(value) = record.get(field.name()) {
                            match value {
                                Value::Number(n) => {
                                    if let Some(i) = n.as_i64() {
                                        builder.append_value(i);
                                    } else {
                                        builder.append_null();
                                    }
                                },
                                Value::Null => builder.append_null(),
                                _ => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                },
                DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                    let mut builder = TimestampMicrosecondBuilder::new();
                    for record in &records {
                        if let Some(value) = record.get(field.name()) {
                            match value {
                                Value::Number(n) => {
                                    if let Some(i) = n.as_i64() {
                                        builder.append_value(i);
                                    } else {
                                        builder.append_null();
                                    }
                                },
                                Value::Null => builder.append_null(),
                                _ => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish().with_timezone_opt(tz.clone()))
                },
                DataType::Date32 => {
                    let mut builder = Date32Builder::new();
                    for record in &records {
                        if let Some(value) = record.get(field.name()) {
                            match value {
                                Value::Number(n) => {
                                    if let Some(i) = n.as_i64() {
                                        builder.append_value(i as i32);
                                    } else {
                                        builder.append_null();
                                    }
                                },
                                Value::String(date_str) => {
                                    // Parse date string and convert to days since epoch
                                    if let Ok(date) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                        let days = (date - epoch).num_days() as i32;
                                        builder.append_value(days);
                                    } else {
                                        builder.append_null();
                                    }
                                },
                                Value::Null => builder.append_null(),
                                _ => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                },
                DataType::List(_) => {
                    let mut builder = ListBuilder::new(StringBuilder::new());
                    for record in &records {
                        if let Some(value) = record.get(field.name()) {
                            match value {
                                Value::Array(arr) => {
                                    for item in arr {
                                        if let Value::String(s) = item {
                                            builder.values().append_value(s);
                                        }
                                    }
                                    builder.append(true);
                                },
                                Value::Null => builder.append(true),
                                _ => builder.append(true),
                            }
                        } else {
                            builder.append(true);
                        }
                    }
                    Arc::new(builder.finish())
                },
                _ => Arc::new(NullArray::new(num_records)),
            };
            arrays.push(array);
        }
        
        RecordBatch::try_new(arrow_schema, arrays).map_err(Into::into)
    }
    
    /// Helper to create a default record with all fields set to null/default values
    pub fn create_default_record() -> HashMap<String, Value> {
        let mut record = HashMap::new();
        let schema = get_otel_schema();
        
        for field in &schema.fields {
            let value = match field.data_type.as_str() {
                "List(Utf8)" => Value::Array(vec![]),
                _ => Value::Null,
            };
            record.insert(field.name.clone(), value);
        }
        
        record
    }
    
    /// Helper to set timestamp as microseconds
    pub fn set_timestamp_micros(record: &mut HashMap<String, Value>, field: &str, timestamp: chrono::DateTime<chrono::Utc>) {
        record.insert(field.to_string(), Value::Number(timestamp.timestamp_micros().into()));
    }
    
    /// Helper to set a date field
    pub fn set_date(record: &mut HashMap<String, Value>, field: &str, date: chrono::NaiveDate) {
        record.insert(field.to_string(), Value::String(date.to_string()));
    }
}