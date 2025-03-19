use anyhow::Result;
use chrono::{LocalResult, TimeZone, Utc};
use datafusion::arrow::array::{Array, StringArray, TimestampMicrosecondArray};

/// In our updated application the table name is fixed to "telemetry.events",
pub fn prepare_sql(query: &str) -> Result<String> {
    Ok(query.to_string())
}

/// Converts an Arrow array value to a string.
/// For null values, returns "NULL". Timestamps are converted using RFC3339.
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
