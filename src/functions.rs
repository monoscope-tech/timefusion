use anyhow::Result;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray, StringBuilder, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DataFusionError, not_impl_err, ScalarValue};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, ColumnarValue, ScalarFunctionArgs, 
    ScalarFunctionImplementation, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, 
    Volatility, create_udf, create_udaf
};
use serde_json::{Value as JsonValue, json};
use std::any::Any;
use std::sync::Arc;
use tdigests::TDigest;

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

    // Register time_bucket function for time-series bucketing
    ctx.register_udf(create_time_bucket_udf());

    // Register percentile_agg aggregate function
    ctx.register_udaf(create_percentile_agg_udaf());

    // Register approx_percentile scalar function
    ctx.register_udf(create_approx_percentile_udf());

    // Register array_element function
    ctx.register_udf(create_array_element_udf());

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

/// Create the time_bucket UDF for time-series bucketing (similar to TimescaleDB)
fn create_time_bucket_udf() -> ScalarUDF {
    let time_bucket_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> {
        if args.len() != 2 {
            return Err(DataFusionError::Execution(
                "time_bucket requires exactly 2 arguments: interval and timestamp".to_string(),
            ));
        }

        // Extract interval string
        let interval_str = match &args[0] {
            ColumnarValue::Scalar(scalar) => match scalar {
                datafusion::scalar::ScalarValue::Utf8(Some(s)) => s.clone(),
                _ => return Err(DataFusionError::Execution("Interval must be a UTF8 string".to_string())),
            },
            ColumnarValue::Array(_) => {
                return Err(DataFusionError::Execution("Interval must be a scalar value".to_string()));
            }
        };

        // Parse the interval to get bucket size in microseconds
        let bucket_size_micros = parse_interval_to_micros(&interval_str)?;

        // Extract timestamp array
        let timestamp_array = match &args[1] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        // Bucket the timestamps
        let result = bucket_timestamps(&timestamp_array, bucket_size_micros)?;

        Ok(ColumnarValue::Array(result))
    });

    create_udf(
        "time_bucket",
        vec![DataType::Utf8, DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))],
        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        Volatility::Immutable,
        time_bucket_fn,
    )
}

/// Parse interval string to microseconds
fn parse_interval_to_micros(interval_str: &str) -> datafusion::error::Result<i64> {
    let trimmed = interval_str.trim();
    
    // Try to parse with whitespace first (e.g., "30 minutes")
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    
    let (value, unit) = if parts.len() == 2 {
        // Format: "30 minutes"
        let value = parts[0].parse::<i64>()
            .map_err(|_| DataFusionError::Execution("Invalid interval value".to_string()))?;
        (value, parts[1].to_lowercase())
    } else if parts.len() == 1 {
        // Try to parse format without space (e.g., "30m")
        let part = parts[0];
        
        // Find where the number ends and the unit begins
        let split_pos = part.chars()
            .position(|c| c.is_alphabetic())
            .ok_or_else(|| DataFusionError::Execution(
                "Invalid interval format. Expected format: 'N unit' (e.g., '5 minutes' or '5m')".to_string()
            ))?;
        
        let (num_str, unit_str) = part.split_at(split_pos);
        
        let value = num_str.parse::<i64>()
            .map_err(|_| DataFusionError::Execution("Invalid interval value".to_string()))?;
            
        (value, unit_str.to_lowercase())
    } else {
        return Err(DataFusionError::Execution(
            "Invalid interval format. Expected format: 'N unit' (e.g., '5 minutes' or '5m')".to_string(),
        ));
    };

    let micros_per_unit = match unit.as_str() {
        "second" | "seconds" | "sec" | "secs" | "s" => 1_000_000,
        "minute" | "minutes" | "min" | "mins" | "m" => 60 * 1_000_000,
        "hour" | "hours" | "hr" | "hrs" | "h" => 3600 * 1_000_000,
        "day" | "days" | "d" => 86400 * 1_000_000,
        "week" | "weeks" | "w" => 7 * 86400 * 1_000_000,
        _ => {
            return Err(DataFusionError::Execution(format!(
                "Unsupported time unit: {}. Supported units: second(s), minute(s), hour(s), day(s), week(s)",
                unit
            )));
        }
    };

    Ok(value * micros_per_unit)
}

/// Bucket timestamps to the nearest bucket boundary
fn bucket_timestamps(timestamp_array: &ArrayRef, bucket_size_micros: i64) -> datafusion::error::Result<ArrayRef> {
    if let Some(timestamps) = timestamp_array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let mut builder = TimestampMicrosecondArray::builder(timestamps.len());

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) {
                builder.append_null();
            } else {
                let timestamp_us = timestamps.value(i);
                // Calculate the bucket: floor(timestamp / bucket_size) * bucket_size
                let bucket = (timestamp_us / bucket_size_micros) * bucket_size_micros;
                builder.append_value(bucket);
            }
        }

        Ok(Arc::new(builder.finish()))
    } else if let Some(timestamps) = timestamp_array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        let mut builder = TimestampNanosecondArray::builder(timestamps.len());
        let bucket_size_nanos = bucket_size_micros * 1000;

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) {
                builder.append_null();
            } else {
                let timestamp_ns = timestamps.value(i);
                // Calculate the bucket: floor(timestamp / bucket_size) * bucket_size
                let bucket = (timestamp_ns / bucket_size_nanos) * bucket_size_nanos;
                builder.append_value(bucket);
            }
        }

        Ok(Arc::new(builder.finish()))
    } else {
        Err(DataFusionError::Execution("Argument must be a timestamp".to_string()))
    }
}

/// Create the percentile_agg UDAF for building t-digest summaries
fn create_percentile_agg_udaf() -> AggregateUDF {
    create_udaf(
        "percentile_agg",
        vec![DataType::Float64],
        Arc::new(DataType::Binary),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(PercentileAccumulator::new()))),
        Arc::new(vec![DataType::Float64]),
    )
}

/// Wrapper for TDigest with accumulated values
#[derive(Debug, Clone)]
struct TDigestWrapper {
    values: Vec<f64>,
}

impl TDigestWrapper {
    fn new() -> Self {
        Self {
            values: Vec::new(),
        }
    }

    fn insert(&mut self, value: f64) {
        self.values.push(value);
    }

    fn merge(&mut self, other: &TDigestWrapper) {
        self.values.extend(&other.values);
    }

    fn to_digest(&self) -> Option<TDigest> {
        if self.values.is_empty() {
            None
        } else {
            Some(TDigest::from_values(self.values.clone()))
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self.values).unwrap_or_else(|_| Vec::new())
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let values: Vec<f64> = bincode::deserialize(bytes)
            .map_err(|e| format!("Failed to deserialize: {}", e))?;
        Ok(Self { values })
    }
}

/// Accumulator for percentile_agg that builds a t-digest
#[derive(Debug)]
struct PercentileAccumulator {
    digest: TDigestWrapper,
}

impl PercentileAccumulator {
    fn new() -> Self {
        Self {
            digest: TDigestWrapper::new(),
        }
    }
}

impl Accumulator for PercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::error::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];
        let float_array = array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("percentile_agg expects Float64 values".to_string()))?;

        for i in 0..float_array.len() {
            if !float_array.is_null(i) {
                let value = float_array.value(i);
                self.digest.insert(value);
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
        // Serialize the t-digest wrapper to binary
        let bytes = self.digest.to_bytes();
        Ok(ScalarValue::Binary(Some(bytes)))
    }

    fn size(&self) -> usize {
        // Estimate size based on values vector
        std::mem::size_of::<TDigestWrapper>() + self.digest.values.len() * std::mem::size_of::<f64>()
    }

    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        // Return the serialized state
        self.evaluate().map(|v| vec![v])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let array = &states[0];
        let binary_array = array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected binary array for merge".to_string()))?;

        for i in 0..binary_array.len() {
            if !binary_array.is_null(i) {
                let bytes = binary_array.value(i);
                let other_digest = TDigestWrapper::from_bytes(bytes)
                    .map_err(|e| DataFusionError::Execution(e))?;
                
                self.digest.merge(&other_digest);
            }
        }

        Ok(())
    }
}

/// Create the approx_percentile UDF for extracting percentiles from t-digest
fn create_approx_percentile_udf() -> ScalarUDF {
    let udf = ApproxPercentileUDF::new();
    ScalarUDF::new_from_impl(udf)
}

/// UDF implementation for approx_percentile
#[derive(Debug)]
struct ApproxPercentileUDF {
    signature: Signature,
}

impl ApproxPercentileUDF {
    fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Float64, DataType::Binary]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ApproxPercentileUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "approx_percentile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(DataFusionError::Execution(
                "approx_percentile requires exactly 2 arguments: percentile and t-digest".to_string(),
            ));
        }

        let percentile_array = match &args.args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
        };

        let digest_array = match &args.args[1] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(percentile_array.len())?,
        };

        let percentile_values = percentile_array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("First argument must be a percentile (Float64)".to_string()))?;

        let digest_values = digest_array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| DataFusionError::Execution("Second argument must be a t-digest (Binary)".to_string()))?;

        let mut builder = Float64Array::builder(percentile_array.len());

        for i in 0..percentile_array.len() {
            if percentile_values.is_null(i) || digest_values.is_null(i) {
                builder.append_null();
            } else {
                let percentile = percentile_values.value(i);
                
                // Validate percentile is between 0 and 1
                if percentile < 0.0 || percentile > 1.0 {
                    return Err(DataFusionError::Execution(
                        format!("Percentile must be between 0 and 1, got {}", percentile),
                    ));
                }

                let digest_bytes = digest_values.value(i);
                let wrapper = TDigestWrapper::from_bytes(digest_bytes)
                    .map_err(|e| DataFusionError::Execution(e))?;
                
                match wrapper.to_digest() {
                    Some(digest) => {
                        let value = digest.estimate_quantile(percentile);
                        builder.append_value(value);
                    }
                    None => {
                        // No values in the digest, return NULL
                        builder.append_null();
                    }
                }
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Create array_element UDF for PostgreSQL-compatible array access
fn create_array_element_udf() -> ScalarUDF {
    let udf = ArrayElementUDF::new();
    ScalarUDF::new_from_impl(udf)
}

/// UDF implementation for array_element
#[derive(Debug)]
struct ArrayElementUDF {
    signature: Signature,
}

impl ArrayElementUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayElementUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_element"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Execution(
                "array_element requires exactly 2 arguments".to_string(),
            ));
        }

        match &arg_types[0] {
            DataType::List(field) => Ok(field.data_type().clone()),
            _ => Err(DataFusionError::Execution(
                "First argument must be an array".to_string(),
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(DataFusionError::Execution(
                "array_element requires exactly 2 arguments: array and index".to_string(),
            ));
        }

        let array_arg = &args.args[0];
        let index_arg = &args.args[1];

        // Convert to arrays
        let list_array = match array_arg {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
        };

        let index_array = match index_arg {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(list_array.len())?,
        };

        // Get the list array
        let list_array = list_array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::ListArray>()
            .ok_or_else(|| DataFusionError::Execution("First argument must be a list array".to_string()))?;

        let index_array = index_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("Second argument must be an integer".to_string()))?;

        // Get the data type of list elements
        let element_type = match list_array.data_type() {
            DataType::List(field) => field.data_type(),
            _ => return Err(DataFusionError::Execution("Expected list data type".to_string())),
        };

        // Create a builder for the result based on element type
        let result = match element_type {
            DataType::Float32 => {
                let mut builder = datafusion::arrow::array::Float32Array::builder(list_array.len());
                for i in 0..list_array.len() {
                    if list_array.is_null(i) || index_array.is_null(i) {
                        builder.append_null();
                    } else {
                        let idx = index_array.value(i) as usize;
                        // PostgreSQL uses 1-based indexing
                        if idx == 0 || idx > list_array.value(i).len() {
                            builder.append_null();
                        } else {
                            let values = list_array.value(i);
                            let float_values = values
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::Float32Array>()
                                .ok_or_else(|| DataFusionError::Execution("Expected Float32 array elements".to_string()))?;
                            builder.append_value(float_values.value((idx - 1) as usize));
                        }
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Float64 => {
                let mut builder = Float64Array::builder(list_array.len());
                for i in 0..list_array.len() {
                    if list_array.is_null(i) || index_array.is_null(i) {
                        builder.append_null();
                    } else {
                        let idx = index_array.value(i) as usize;
                        // PostgreSQL uses 1-based indexing
                        if idx == 0 || idx > list_array.value(i).len() {
                            builder.append_null();
                        } else {
                            let values = list_array.value(i);
                            let float_values = values
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .ok_or_else(|| DataFusionError::Execution("Expected Float64 array elements".to_string()))?;
                            builder.append_value(float_values.value((idx - 1) as usize));
                        }
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::new();
                for i in 0..list_array.len() {
                    if list_array.is_null(i) || index_array.is_null(i) {
                        builder.append_null();
                    } else {
                        let idx = index_array.value(i) as usize;
                        // PostgreSQL uses 1-based indexing
                        if idx == 0 || idx > list_array.value(i).len() {
                            builder.append_null();
                        } else {
                            let values = list_array.value(i);
                            let string_values = values
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .ok_or_else(|| DataFusionError::Execution("Expected String array elements".to_string()))?;
                            builder.append_value(string_values.value((idx - 1) as usize));
                        }
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Int32 => {
                let mut builder = datafusion::arrow::array::Int32Array::builder(list_array.len());
                for i in 0..list_array.len() {
                    if list_array.is_null(i) || index_array.is_null(i) {
                        builder.append_null();
                    } else {
                        let idx = index_array.value(i) as usize;
                        // PostgreSQL uses 1-based indexing
                        if idx == 0 || idx > list_array.value(i).len() {
                            builder.append_null();
                        } else {
                            let values = list_array.value(i);
                            let int_values = values
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                                .ok_or_else(|| DataFusionError::Execution("Expected Int32 array elements".to_string()))?;
                            builder.append_value(int_values.value((idx - 1) as usize));
                        }
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Int64 => {
                let mut builder = Int64Array::builder(list_array.len());
                for i in 0..list_array.len() {
                    if list_array.is_null(i) || index_array.is_null(i) {
                        builder.append_null();
                    } else {
                        let idx = index_array.value(i) as usize;
                        // PostgreSQL uses 1-based indexing
                        if idx == 0 || idx > list_array.value(i).len() {
                            builder.append_null();
                        } else {
                            let values = list_array.value(i);
                            let int_values = values
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .ok_or_else(|| DataFusionError::Execution("Expected Int64 array elements".to_string()))?;
                            builder.append_value(int_values.value((idx - 1) as usize));
                        }
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            _ => {
                return Err(DataFusionError::Execution(
                    format!("Unsupported array element type: {:?}", element_type),
                ));
            }
        };

        Ok(ColumnarValue::Array(result))
    }
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

    #[test]
    fn test_empty_tdigest_wrapper() {
        // Test that empty TDigestWrapper doesn't panic
        let wrapper = TDigestWrapper::new();
        assert!(wrapper.to_digest().is_none());
        
        // Test with values
        let mut wrapper_with_values = TDigestWrapper::new();
        wrapper_with_values.insert(10.0);
        wrapper_with_values.insert(20.0);
        assert!(wrapper_with_values.to_digest().is_some());
    }

    #[test]
    fn test_parse_interval_to_micros() {
        // Test format with spaces
        assert_eq!(parse_interval_to_micros("1 second").unwrap(), 1_000_000);
        assert_eq!(parse_interval_to_micros("5 seconds").unwrap(), 5_000_000);
        assert_eq!(parse_interval_to_micros("1 minute").unwrap(), 60_000_000);
        assert_eq!(parse_interval_to_micros("5 minutes").unwrap(), 300_000_000);
        assert_eq!(parse_interval_to_micros("1 hour").unwrap(), 3_600_000_000);
        assert_eq!(parse_interval_to_micros("2 hours").unwrap(), 7_200_000_000);
        assert_eq!(parse_interval_to_micros("1 day").unwrap(), 86_400_000_000);
        assert_eq!(parse_interval_to_micros("1 week").unwrap(), 604_800_000_000);

        // Test different unit formats with spaces
        assert_eq!(parse_interval_to_micros("5 min").unwrap(), 300_000_000);
        assert_eq!(parse_interval_to_micros("5 mins").unwrap(), 300_000_000);
        assert_eq!(parse_interval_to_micros("5 m").unwrap(), 300_000_000);
        
        // Test format without spaces
        assert_eq!(parse_interval_to_micros("1second").unwrap(), 1_000_000);
        assert_eq!(parse_interval_to_micros("5seconds").unwrap(), 5_000_000);
        assert_eq!(parse_interval_to_micros("1minute").unwrap(), 60_000_000);
        assert_eq!(parse_interval_to_micros("5minutes").unwrap(), 300_000_000);
        assert_eq!(parse_interval_to_micros("30m").unwrap(), 1_800_000_000);
        assert_eq!(parse_interval_to_micros("1h").unwrap(), 3_600_000_000);
        assert_eq!(parse_interval_to_micros("2h").unwrap(), 7_200_000_000);
        assert_eq!(parse_interval_to_micros("1d").unwrap(), 86_400_000_000);
        assert_eq!(parse_interval_to_micros("1w").unwrap(), 604_800_000_000);
        assert_eq!(parse_interval_to_micros("5min").unwrap(), 300_000_000);
        assert_eq!(parse_interval_to_micros("5mins").unwrap(), 300_000_000);
        assert_eq!(parse_interval_to_micros("5s").unwrap(), 5_000_000);

        // Test error cases
        assert!(parse_interval_to_micros("invalid").is_err());
        assert!(parse_interval_to_micros("5").is_err());
        assert!(parse_interval_to_micros("abc minutes").is_err());
        assert!(parse_interval_to_micros("m5").is_err()); // unit before number
    }
}
