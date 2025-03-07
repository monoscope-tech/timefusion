// src/utils.rs
use anyhow::Result;
use datafusion::arrow::array::{Array, StringArray, TimestampMicrosecondArray};
use chrono::{LocalResult, TimeZone, Utc};

pub fn prepare_sql(query: &str) -> Result<String> {
    let query_lower = query.trim().to_lowercase();
    if query_lower.starts_with("insert") || query_lower.starts_with("select") {
        Ok(query.replace("\"table\"", "\"table_events\""))
    } else {
        Ok(query.to_string())
    }
}

pub fn value_to_string(array: &dyn Array, index: usize) -> String {
    if array.is_null(index) {
        return "NULL".to_string();
    }
    if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
        return string_array.value(index).to_string();
    }
    if let Some(ts_array) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let micros = ts_array.value(index);
        match Utc.timestamp_micros(micros) {
            LocalResult::Single(dt) => dt.to_rfc3339(),
            _ => "Invalid timestamp".to_string(),
        }
    } else {
        format!("{:?}", array.to_data().buffers()[0].as_slice())
    }
}
