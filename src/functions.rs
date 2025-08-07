use anyhow::Result;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, StringBuilder, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DataFusionError, not_impl_err};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarFunctionImplementation, ScalarUDF, ScalarUDFImpl, Signature, Volatility, create_udf};
use serde_json::{Value as JsonValue, json};
use std::any::Any;
use std::sync::Arc;

/// Register all custom PostgreSQL-compatible functions
pub fn register_custom_functions(ctx: &mut datafusion::execution::context::SessionContext) -> Result<()> {
    // Register to_char function
    ctx.register_udf(create_to_char_udf());

    // Register AT TIME ZONE function
    ctx.register_udf(create_at_time_zone_udf());

    // Register jsonb_array_elements function (if not already available)
    ctx.register_udf(create_jsonb_array_elements_udf());

    // Register json_build_array function
    ctx.register_udf(create_json_build_array_udf());

    // Register to_json function
    ctx.register_udf(create_to_json_udf());

    // Register extract_epoch function for fractional seconds
    ctx.register_udf(create_extract_epoch_udf());

    Ok(())
}

/// Create the to_char UDF for PostgreSQL-compatible timestamp formatting
fn create_to_char_udf() -> ScalarUDF {
    let to_char_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> {
        if args.len() != 2 {
            return Err(DataFusionError::Execution(
                "to_char requires exactly 2 arguments: timestamp and format string".to_string(),
            ));
        }

        // Extract timestamp array
        let timestamp_array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        // Extract format string
        let format_str = match &args[1] {
            ColumnarValue::Scalar(scalar) => match scalar {
                datafusion::scalar::ScalarValue::Utf8(Some(s)) => s.clone(),
                _ => return Err(DataFusionError::Execution("Format string must be a UTF8 string".to_string())),
            },
            ColumnarValue::Array(_) => {
                return Err(DataFusionError::Execution("Format string must be a scalar value".to_string()));
            }
        };

        // Convert timestamps to formatted strings
        let result = format_timestamps(&timestamp_array, &format_str)?;

        Ok(ColumnarValue::Array(result))
    });

    create_udf(
        "to_char",
        vec![DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        to_char_fn,
    )
}

/// Format timestamps according to PostgreSQL format patterns
fn format_timestamps(timestamp_array: &ArrayRef, format_str: &str) -> datafusion::error::Result<ArrayRef> {
    // Try to handle both microsecond and nanosecond timestamps
    let mut builder = StringBuilder::new();

    if let Some(timestamps) = timestamp_array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        for i in 0..timestamps.len() {
            if timestamps.is_null(i) {
                builder.append_null();
            } else {
                let timestamp_us = timestamps.value(i);
                let datetime =
                    DateTime::<Utc>::from_timestamp_micros(timestamp_us).ok_or_else(|| DataFusionError::Execution("Invalid timestamp".to_string()))?;

                // Convert PostgreSQL format to chrono format
                let chrono_format = postgres_to_chrono_format(format_str);
                let formatted = datetime.format(&chrono_format).to_string();

                builder.append_value(&formatted);
            }
        }
    } else if let Some(timestamps) = timestamp_array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        for i in 0..timestamps.len() {
            if timestamps.is_null(i) {
                builder.append_null();
            } else {
                let timestamp_ns = timestamps.value(i);
                let datetime = DateTime::<Utc>::from_timestamp_nanos(timestamp_ns);

                // Convert PostgreSQL format to chrono format
                let chrono_format = postgres_to_chrono_format(format_str);
                let formatted = datetime.format(&chrono_format).to_string();

                builder.append_value(&formatted);
            }
        }
    } else {
        return Err(DataFusionError::Execution("First argument must be a timestamp".to_string()));
    }

    Ok(Arc::new(builder.finish()))
}

/// Convert PostgreSQL format patterns to chrono format patterns
fn postgres_to_chrono_format(pg_format: &str) -> String {
    // This is a simplified conversion - a full implementation would handle all PostgreSQL patterns
    // Order matters! Longer patterns should be replaced first
    pg_format
        .replace("YYYY", "%Y")
        .replace("Month", "%B") // Full month name (must come before MM)
        .replace("Mon", "%b") // Abbreviated month name (must come before MM)
        .replace("MM", "%m") // Month number
        .replace("DD", "%d")
        .replace("HH24", "%H")
        .replace("HH", "%I")
        .replace("MI", "%M")
        .replace("SS", "%S")
        .replace("US", "%6f") // Microseconds (6 digits)
        .replace("MS", "%3f") // Milliseconds (3 digits)
        .replace("TZ", "%Z")
        .replace("Day", "%A")
}

/// Create the AT TIME ZONE UDF for timezone conversion
fn create_at_time_zone_udf() -> ScalarUDF {
    let at_time_zone_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> {
        if args.len() != 2 {
            return Err(DataFusionError::Execution(
                "AT TIME ZONE requires exactly 2 arguments: timestamp and timezone".to_string(),
            ));
        }

        // Extract timestamp array
        let timestamp_array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        // Extract timezone string
        let tz_str = match &args[1] {
            ColumnarValue::Scalar(scalar) => match scalar {
                datafusion::scalar::ScalarValue::Utf8(Some(s)) => s.clone(),
                _ => return Err(DataFusionError::Execution("Timezone must be a UTF8 string".to_string())),
            },
            ColumnarValue::Array(_) => {
                return Err(DataFusionError::Execution("Timezone must be a scalar value".to_string()));
            }
        };

        // Convert timestamps to the specified timezone
        let result = convert_timezone(&timestamp_array, &tz_str)?;

        Ok(ColumnarValue::Array(result))
    });

    create_udf(
        "at_time_zone",
        vec![DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), DataType::Utf8],
        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        Volatility::Immutable,
        at_time_zone_fn,
    )
}

/// Convert timestamps to a different timezone
fn convert_timezone(timestamp_array: &ArrayRef, tz_str: &str) -> datafusion::error::Result<ArrayRef> {
    // Parse timezone
    let tz: Tz = tz_str.parse().map_err(|_| DataFusionError::Execution(format!("Invalid timezone: {}", tz_str)))?;

    // Handle microsecond timestamps (which is what we're using)
    if let Some(timestamps) = timestamp_array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let mut builder = TimestampMicrosecondArray::builder(timestamps.len());

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) {
                builder.append_null();
            } else {
                let timestamp_us = timestamps.value(i);
                let datetime =
                    DateTime::<Utc>::from_timestamp_micros(timestamp_us).ok_or_else(|| DataFusionError::Execution("Invalid timestamp".to_string()))?;

                // Convert to target timezone (keeping the same instant in time)
                let converted = datetime.with_timezone(&tz);

                // Convert back to UTC timestamp for storage
                builder.append_value(converted.timestamp_micros());
            }
        }

        Ok(Arc::new(builder.finish()))
    } else if let Some(timestamps) = timestamp_array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        let mut builder = TimestampNanosecondArray::builder(timestamps.len());

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) {
                builder.append_null();
            } else {
                let timestamp_ns = timestamps.value(i);
                let datetime = DateTime::<Utc>::from_timestamp_nanos(timestamp_ns);

                // Convert to target timezone (keeping the same instant in time)
                let converted = datetime.with_timezone(&tz);

                // Convert back to UTC timestamp for storage
                builder.append_value(converted.timestamp_nanos_opt().unwrap_or(timestamp_ns));
            }
        }

        Ok(Arc::new(builder.finish()))
    } else {
        Err(DataFusionError::Execution("First argument must be a timestamp".to_string()))
    }
}

/// Create the jsonb_array_elements UDF to unnest JSON arrays
fn create_jsonb_array_elements_udf() -> ScalarUDF {
    // Note: This is a placeholder implementation
    // A full implementation would require table function support in DataFusion
    // For now, we'll create a function that extracts array elements as a string
    let jsonb_array_elements_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> {
        if args.len() != 1 {
            return Err(DataFusionError::Execution("jsonb_array_elements requires exactly 1 argument".to_string()));
        }

        // For now, return a not implemented error
        // A proper implementation would require table function support
        not_impl_err!("jsonb_array_elements is not yet fully implemented - requires table function support")
    });

    create_udf(
        "jsonb_array_elements",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        jsonb_array_elements_fn,
    )
}

/// Create the json_build_array UDF for building JSON arrays
fn create_json_build_array_udf() -> ScalarUDF {
    ScalarUDF::from(JsonBuildArrayUDF::new())
}

#[derive(Debug)]
struct JsonBuildArrayUDF {
    signature: Signature,
}

impl JsonBuildArrayUDF {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonBuildArrayUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_build_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        if args.is_empty() {
            // Empty array case
            let mut builder = StringBuilder::with_capacity(1, 1024);
            builder.append_value("[]");
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }

        // Determine the number of rows
        let num_rows = match &args[0] {
            ColumnarValue::Array(array) => array.len(),
            ColumnarValue::Scalar(_) => 1,
        };

        let mut builder = StringBuilder::with_capacity(num_rows, 1024);

        for row_idx in 0..num_rows {
            let mut row_values = Vec::new();

            for arg in &args {
                let value = match arg {
                    ColumnarValue::Array(array) => {
                        let json_values = array_to_json_values(array)?;
                        json_values[row_idx].clone()
                    }
                    ColumnarValue::Scalar(scalar) => {
                        let array = scalar.to_array()?;
                        let json_values = array_to_json_values(&array)?;
                        json_values[0].clone()
                    }
                };
                row_values.push(value);
            }

            let json_array = JsonValue::Array(row_values);
            builder.append_value(json_array.to_string());
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Create the to_json UDF for converting values to JSON
fn create_to_json_udf() -> ScalarUDF {
    ScalarUDF::from(ToJsonUDF::new())
}

#[derive(Debug)]
struct ToJsonUDF {
    signature: Signature,
}

impl ToJsonUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToJsonUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        if args.len() != 1 {
            return Err(DataFusionError::Execution("to_json requires exactly 1 argument".to_string()));
        }

        let array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let json_values = array_to_json_values(&array)?;
        let mut builder = StringBuilder::with_capacity(json_values.len(), 1024);

        for value in json_values {
            builder.append_value(value.to_string());
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Create the extract_epoch UDF for extracting epoch time with fractional seconds
fn create_extract_epoch_udf() -> ScalarUDF {
    ScalarUDF::from(ExtractEpochUDF::new())
}

#[derive(Debug)]
struct ExtractEpochUDF {
    signature: Signature,
}

impl ExtractEpochUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ExtractEpochUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "extract_epoch"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        if args.len() != 1 {
            return Err(DataFusionError::Execution("extract_epoch requires exactly 1 argument".to_string()));
        }

        let array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let result = if let Some(timestamps) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            let mut builder = Float64Array::builder(timestamps.len());
            for i in 0..timestamps.len() {
                if timestamps.is_null(i) {
                    builder.append_null();
                } else {
                    let timestamp_us = timestamps.value(i);
                    let epoch_seconds = timestamp_us as f64 / 1_000_000.0;
                    builder.append_value(epoch_seconds);
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        } else if let Some(timestamps) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
            let mut builder = Float64Array::builder(timestamps.len());
            for i in 0..timestamps.len() {
                if timestamps.is_null(i) {
                    builder.append_null();
                } else {
                    let timestamp_ns = timestamps.value(i);
                    let epoch_seconds = timestamp_ns as f64 / 1_000_000_000.0;
                    builder.append_value(epoch_seconds);
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        } else {
            return Err(DataFusionError::Execution("extract_epoch requires a timestamp argument".to_string()));
        };

        Ok(ColumnarValue::Array(result))
    }
}

/// Convert Arrow array to JSON values
fn array_to_json_values(array: &ArrayRef) -> datafusion::error::Result<Vec<JsonValue>> {
    let mut values = Vec::with_capacity(array.len());

    match array.data_type() {
        DataType::Utf8 => {
            let string_array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Execution("Failed to downcast to StringArray".to_string()))?;
            for i in 0..string_array.len() {
                if string_array.is_null(i) {
                    values.push(JsonValue::Null);
                } else {
                    values.push(JsonValue::String(string_array.value(i).to_string()));
                }
            }
        }
        DataType::Int64 => {
            let int_array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Execution("Failed to downcast to Int64Array".to_string()))?;
            for i in 0..int_array.len() {
                if int_array.is_null(i) {
                    values.push(JsonValue::Null);
                } else {
                    values.push(json!(int_array.value(i)));
                }
            }
        }
        DataType::Float64 => {
            let float_array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("Failed to downcast to Float64Array".to_string()))?;
            for i in 0..float_array.len() {
                if float_array.is_null(i) {
                    values.push(JsonValue::Null);
                } else {
                    values.push(json!(float_array.value(i)));
                }
            }
        }
        DataType::Boolean => {
            let bool_array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| DataFusionError::Execution("Failed to downcast to BooleanArray".to_string()))?;
            for i in 0..bool_array.len() {
                if bool_array.is_null(i) {
                    values.push(JsonValue::Null);
                } else {
                    values.push(json!(bool_array.value(i)));
                }
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let timestamp_array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| DataFusionError::Execution("Failed to downcast to TimestampMicrosecondArray".to_string()))?;
            for i in 0..timestamp_array.len() {
                if timestamp_array.is_null(i) {
                    values.push(JsonValue::Null);
                } else {
                    let timestamp_us = timestamp_array.value(i);
                    let datetime =
                        DateTime::<Utc>::from_timestamp_micros(timestamp_us).ok_or_else(|| DataFusionError::Execution("Invalid timestamp".to_string()))?;
                    values.push(JsonValue::String(datetime.to_rfc3339()));
                }
            }
        }
        _ => {
            // For other types, try to convert to string
            let string_array = datafusion::arrow::compute::cast(array, &DataType::Utf8)?;
            return array_to_json_values(&string_array);
        }
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_to_chrono_format() {
        assert_eq!(postgres_to_chrono_format("YYYY-MM-DD"), "%Y-%m-%d");
        assert_eq!(postgres_to_chrono_format("YYYY-MM-DD HH24:MI:SS"), "%Y-%m-%d %H:%M:%S");
        assert_eq!(postgres_to_chrono_format("Day, DD Mon YYYY"), "%A, %d %b %Y");
    }
}
