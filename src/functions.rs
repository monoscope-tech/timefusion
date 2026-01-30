use anyhow::Result;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, ListArray, StringArray, StringViewArray, StringViewBuilder,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DFSchema, DataFusionError, ExprSchema, ScalarValue, not_impl_err};
use datafusion::logical_expr::ExprSchemable;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, ColumnarValue, Expr, ScalarFunctionArgs, ScalarFunctionImplementation, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility, create_udaf, create_udf,
    expr::{Alias, ScalarFunction},
    planner::{ExprPlanner, PlannerResult, RawBinaryExpr},
};
use datafusion::sql::sqlparser::ast::BinaryOperator;
use serde_json::{Value as JsonValue, json};
use std::any::Any;
use std::sync::Arc;
use tdigests::TDigest;

use crate::schema_loader::is_variant_type;

/// Extract a String from any ScalarValue string type (Utf8, Utf8View, LargeUtf8)
fn scalar_to_string(scalar: &ScalarValue) -> Option<String> {
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

// ============================================================================
// Variant-Aware Expression Planner
// ============================================================================

/// ExprPlanner that intercepts -> and ->> operators on Variant columns
/// and rewrites them to efficient variant_get calls with flattened dot-paths.
#[derive(Debug, Default)]
pub struct VariantAwareExprPlanner;

/// Path component for building variant_get paths
#[derive(Debug, Clone)]
enum PathComponent {
    Field(String),
    Index(i64),
}

impl ExprPlanner for VariantAwareExprPlanner {
    fn plan_binary_op(&self, expr: RawBinaryExpr, schema: &DFSchema) -> datafusion::error::Result<PlannerResult<RawBinaryExpr>> {
        let is_long_arrow = match &expr.op {
            BinaryOperator::Arrow => false,
            BinaryOperator::LongArrow => true,
            _ => return Ok(PlannerResult::Original(expr)),
        };

        // Recursively collect path components from chained operators
        let (base_expr, mut path_parts) = collect_arrow_chain(&expr.left);
        if let Some(component) = extract_path_component(&expr.right) {
            path_parts.push(component);
        } else {
            return Ok(PlannerResult::Original(expr));
        }

        // Check if base column is Variant type
        if !is_variant_column(&base_expr, schema) {
            return Ok(PlannerResult::Original(expr)); // Let JSON planner handle
        }

        // Build dot-path: ["user", "name"] → "user.name", ["items", Index(0)] → "items[0]"
        let full_path = build_variant_path(&path_parts);

        // Create variant_get function call
        let variant_get_udf = ScalarUDF::from(datafusion_variant::VariantGetUdf::default());
        let path_literal = Expr::Literal(ScalarValue::Utf8(Some(full_path.clone())), None);
        let variant_get_call = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(variant_get_udf),
            args: vec![base_expr.clone(), path_literal],
        });

        // For ->> wrap with variant_to_json for text output
        let result = if is_long_arrow {
            let variant_to_json_udf = ScalarUDF::from(datafusion_variant::VariantToJsonUdf::default());
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(variant_to_json_udf),
                args: vec![variant_get_call],
            })
        } else {
            variant_get_call
        };

        // Create alias to preserve original SQL representation
        let op_str = if is_long_arrow { "->>" } else { "->" };
        let alias_name = format!("{} {} {}", expr_repr(&base_expr), op_str, path_repr(&path_parts));
        Ok(PlannerResult::Planned(Expr::Alias(Alias::new(result, None::<&str>, alias_name))))
    }
}

/// Recursively collect chained arrow expressions into base + path components
fn collect_arrow_chain(expr: &Expr) -> (Expr, Vec<PathComponent>) {
    match expr {
        Expr::BinaryExpr(binary) if matches!(binary.op, datafusion::logical_expr::Operator::Arrow) => {
            let (base, mut parts) = collect_arrow_chain(&binary.left);
            if let Some(component) = extract_path_component(&binary.right) {
                parts.push(component);
            }
            (base, parts)
        }
        Expr::Alias(alias) => collect_arrow_chain(&alias.expr),
        _ => (expr.clone(), vec![]),
    }
}

/// Extract path component from expression (string literal or integer)
fn extract_path_component(expr: &Expr) -> Option<PathComponent> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Some(PathComponent::Field(s.clone())),
        Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Some(PathComponent::Field(s.clone())),
        Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Some(PathComponent::Field(s.clone())),
        Expr::Literal(ScalarValue::Int64(Some(i)), _) => Some(PathComponent::Index(*i)),
        Expr::Literal(ScalarValue::Int32(Some(i)), _) => Some(PathComponent::Index(*i as i64)),
        Expr::Literal(ScalarValue::UInt64(Some(i)), _) => Some(PathComponent::Index(*i as i64)),
        Expr::Literal(ScalarValue::UInt32(Some(i)), _) => Some(PathComponent::Index(*i as i64)),
        _ => None,
    }
}

/// Check if expression evaluates to a Variant type
fn is_variant_column(expr: &Expr, schema: &DFSchema) -> bool {
    match expr {
        // Direct column reference
        Expr::Column(col) => schema.field_from_column(col).map(|f| is_variant_type(f.data_type())).unwrap_or(false),
        // Unwrap aliases
        Expr::Alias(alias) => is_variant_column(&alias.expr, schema),
        // Check if it's a call to a variant-producing function
        Expr::ScalarFunction(func) => {
            let name = func.func.name();
            matches!(
                name,
                "json_to_variant"
                    | "variant_get"
                    | "cast_to_variant"
                    | "variant_object_construct"
                    | "variant_list_construct"
                    | "variant_object_insert"
                    | "variant_list_insert"
            )
        }
        // Try to get the type for other expressions
        _ => expr.get_type(schema).map(|dt| is_variant_type(&dt)).unwrap_or(false),
    }
}

/// Build variant_get path string from components
fn build_variant_path(parts: &[PathComponent]) -> String {
    let mut path = String::new();
    for (i, part) in parts.iter().enumerate() {
        match part {
            PathComponent::Field(name) => {
                if i > 0 {
                    path.push('.');
                }
                path.push_str(name);
            }
            PathComponent::Index(idx) => {
                path.push('[');
                path.push_str(&idx.to_string());
                path.push(']');
            }
        }
    }
    path
}

/// Generate SQL-like representation for expression (for alias)
fn expr_repr(expr: &Expr) -> String {
    match expr {
        Expr::Column(col) => col.name.clone(),
        Expr::Alias(alias) => alias.name.clone(),
        _ => "expr".to_string(),
    }
}

/// Generate path representation for alias
fn path_repr(parts: &[PathComponent]) -> String {
    parts
        .iter()
        .map(|p| match p {
            PathComponent::Field(s) => format!("'{}'", s),
            PathComponent::Index(i) => i.to_string(),
        })
        .collect::<Vec<_>>()
        .join("->")
}

/// Register all custom PostgreSQL-compatible functions
pub fn register_custom_functions(ctx: &mut datafusion::execution::context::SessionContext) -> Result<()> {
    // Register Variant-aware expr planner (must be before JSON planner for priority)
    datafusion::execution::FunctionRegistry::register_expr_planner(ctx, Arc::new(VariantAwareExprPlanner))?;

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

    // Register variant functions from datafusion-variant
    ctx.register_udf(ScalarUDF::from(datafusion_variant::JsonToVariantUdf::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::VariantToJsonUdf::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::VariantGetUdf::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::CastToVariantUdf::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::IsVariantNullUdf::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::VariantPretty::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::VariantListConstruct::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::VariantListInsert::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::VariantObjectConstruct::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::VariantObjectInsert::default()));

    // Register jsonb_path_exists for JSONPath queries on Variant columns
    ctx.register_udf(create_jsonb_path_exists_udf());

    Ok(())
}

/// Create the to_char UDF for PostgreSQL-compatible timestamp formatting
fn create_to_char_udf() -> ScalarUDF {
    ScalarUDF::from(ToCharUDF::new())
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct ToCharUDF {
    signature: Signature,
}

impl ToCharUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToCharUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_char"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
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
            ColumnarValue::Scalar(scalar) => {
                scalar_to_string(scalar).ok_or_else(|| DataFusionError::Execution("Format string must be a UTF8 string".to_string()))?
            }
            ColumnarValue::Array(arr) => {
                if let Some(str_arr) = arr.as_any().downcast_ref::<StringViewArray>() {
                    if str_arr.len() == 1 && !str_arr.is_null(0) {
                        str_arr.value(0).to_string()
                    } else {
                        return Err(DataFusionError::Execution("Format string must be a scalar value".to_string()));
                    }
                } else if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                    if str_arr.len() == 1 && !str_arr.is_null(0) {
                        str_arr.value(0).to_string()
                    } else {
                        return Err(DataFusionError::Execution("Format string must be a scalar value".to_string()));
                    }
                } else {
                    return Err(DataFusionError::Execution("Format string must be a UTF8 string".to_string()));
                }
            }
        };

        let result = format_timestamps(&timestamp_array, &format_str)?;
        Ok(ColumnarValue::Array(result))
    }
}

/// Format timestamps according to PostgreSQL format patterns
fn format_timestamps(timestamp_array: &ArrayRef, format_str: &str) -> datafusion::error::Result<ArrayRef> {
    let chrono_format = postgres_to_chrono_format(format_str);
    let mut builder = StringViewBuilder::new();

    let format_fn = |timestamp_us: i64| -> datafusion::error::Result<String> {
        DateTime::<Utc>::from_timestamp_micros(timestamp_us)
            .ok_or_else(|| DataFusionError::Execution("Invalid timestamp".to_string()))
            .map(|dt| dt.format(&chrono_format).to_string())
    };

    match timestamp_array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        Some(timestamps) => {
            for i in 0..timestamps.len() {
                if timestamps.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(&format_fn(timestamps.value(i))?);
                }
            }
        }
        None => match timestamp_array.as_any().downcast_ref::<TimestampNanosecondArray>() {
            Some(timestamps) => {
                for i in 0..timestamps.len() {
                    if timestamps.is_null(i) {
                        builder.append_null();
                    } else {
                        let timestamp_us = timestamps.value(i) / 1000; // Convert nanos to micros
                        builder.append_value(&format_fn(timestamp_us)?);
                    }
                }
            }
            None => return Err(DataFusionError::Execution("First argument must be a timestamp".to_string())),
        },
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
    ScalarUDF::from(AtTimeZoneUDF::new())
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct AtTimeZoneUDF {
    signature: Signature,
}

impl AtTimeZoneUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AtTimeZoneUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "at_time_zone"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        match &arg_types[0] {
            DataType::Timestamp(unit, _) => Ok(DataType::Timestamp(*unit, None)),
            _ => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
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
            ColumnarValue::Scalar(scalar) => {
                scalar_to_string(scalar).ok_or_else(|| DataFusionError::Execution("Timezone must be a UTF8 string".to_string()))?
            }
            ColumnarValue::Array(arr) => {
                if let Some(str_arr) = arr.as_any().downcast_ref::<StringViewArray>() {
                    if str_arr.len() == 1 && !str_arr.is_null(0) {
                        str_arr.value(0).to_string()
                    } else {
                        return Err(DataFusionError::Execution("Timezone must be a scalar string value".to_string()));
                    }
                } else if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                    if str_arr.len() == 1 && !str_arr.is_null(0) {
                        str_arr.value(0).to_string()
                    } else {
                        return Err(DataFusionError::Execution("Timezone must be a scalar string value".to_string()));
                    }
                } else {
                    return Err(DataFusionError::Execution("Timezone must be a UTF8 string".to_string()));
                }
            }
        };

        let result = convert_timezone(&timestamp_array, &tz_str)?;
        Ok(ColumnarValue::Array(result))
    }
}

/// Convert timestamps to a different timezone
/// This adjusts the timestamp so that when formatted as UTC, it displays the local time
fn convert_timezone(timestamp_array: &ArrayRef, tz_str: &str) -> datafusion::error::Result<ArrayRef> {
    use chrono::Offset;

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

                // Get the local time in target timezone
                let local_time = datetime.with_timezone(&tz);
                // Get the offset from UTC in seconds
                let offset_secs = local_time.offset().fix().local_minus_utc() as i64;
                // Adjust the timestamp so that when formatted as UTC, it shows local time
                let adjusted_us = timestamp_us + (offset_secs * 1_000_000);
                builder.append_value(adjusted_us);
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

                // Get the local time in target timezone
                let local_time = datetime.with_timezone(&tz);
                // Get the offset from UTC in seconds
                let offset_secs = local_time.offset().fix().local_minus_utc() as i64;
                // Adjust the timestamp so that when formatted as UTC, it shows local time
                let adjusted_ns = timestamp_ns + (offset_secs * 1_000_000_000);
                builder.append_value(adjusted_ns);
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
        vec![DataType::Utf8View],
        DataType::Utf8View,
        Volatility::Immutable,
        jsonb_array_elements_fn,
    )
}

/// Create the json_build_array UDF for building JSON arrays
fn create_json_build_array_udf() -> ScalarUDF {
    ScalarUDF::from(JsonBuildArrayUDF::new())
}

#[derive(Debug, Hash, Eq, PartialEq)]
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
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        if args.is_empty() {
            // Empty array case
            let mut builder = StringViewBuilder::with_capacity(1);
            builder.append_value("[]");
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }

        // Determine the number of rows
        let num_rows = match &args[0] {
            ColumnarValue::Array(array) => array.len(),
            ColumnarValue::Scalar(_) => 1,
        };

        let mut builder = StringViewBuilder::with_capacity(num_rows);

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

#[derive(Debug, Hash, Eq, PartialEq)]
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
        Ok(DataType::Utf8View)
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
        let mut builder = StringViewBuilder::with_capacity(json_values.len());

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

#[derive(Debug, Hash, Eq, PartialEq)]
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
        DataType::Utf8View => {
            let string_array = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| DataFusionError::Execution("Failed to downcast to StringViewArray".to_string()))?;
            for i in 0..string_array.len() {
                if string_array.is_null(i) {
                    values.push(JsonValue::Null);
                } else {
                    let s = string_array.value(i);
                    // Try to parse as JSON if it looks like JSON
                    let val = if (s.starts_with('{') && s.ends_with('}')) || (s.starts_with('[') && s.ends_with(']')) {
                        serde_json::from_str(s).unwrap_or_else(|_| JsonValue::String(s.to_string()))
                    } else {
                        JsonValue::String(s.to_string())
                    };
                    values.push(val);
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
        DataType::List(_) => {
            let list_array = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| DataFusionError::Execution("Failed to downcast to ListArray".to_string()))?;

            for i in 0..list_array.len() {
                if list_array.is_null(i) {
                    values.push(JsonValue::Null);
                } else {
                    let array_ref = list_array.value(i);
                    let inner_values = array_to_json_values(&array_ref)?;
                    values.push(JsonValue::Array(inner_values));
                }
            }
        }
        _ => {
            // For other types, try to convert to string
            let string_array = datafusion::arrow::compute::cast(array, &DataType::Utf8View)?;
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
            ColumnarValue::Scalar(scalar) => {
                scalar_to_string(scalar).ok_or_else(|| DataFusionError::Execution("Interval must be a UTF8 string".to_string()))?
            }
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
        vec![DataType::Utf8View, DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))],
        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        Volatility::Immutable,
        time_bucket_fn,
    )
}

/// Parse interval string to microseconds
fn parse_interval_to_micros(interval_str: &str) -> datafusion::error::Result<i64> {
    let trimmed = interval_str.trim();
    let parts: Vec<&str> = trimmed.split_whitespace().collect();

    let (value, unit) = match parts.as_slice() {
        [value_str, unit_str] => {
            let value = value_str.parse::<i64>().map_err(|_| DataFusionError::Execution("Invalid interval value".to_string()))?;
            (value, unit_str.to_lowercase())
        }
        [combined] => {
            let split_pos = combined
                .chars()
                .position(|c| c.is_alphabetic())
                .ok_or_else(|| DataFusionError::Execution("Invalid interval format. Expected format: 'N unit' (e.g., '5 minutes' or '5m')".to_string()))?;

            let (num_str, unit_str) = combined.split_at(split_pos);
            let value = num_str.parse::<i64>().map_err(|_| DataFusionError::Execution("Invalid interval value".to_string()))?;
            (value, unit_str.to_lowercase())
        }
        _ => {
            return Err(DataFusionError::Execution(
                "Invalid interval format. Expected format: 'N unit' (e.g., '5 minutes' or '5m')".to_string(),
            ));
        }
    };

    let micros_per_unit = match unit.as_str() {
        "second" | "seconds" | "sec" | "secs" | "s" => 1_000_000,
        "minute" | "minutes" | "min" | "mins" | "m" => 60_000_000,
        "hour" | "hours" | "hr" | "hrs" | "h" => 3_600_000_000,
        "day" | "days" | "d" => 86_400_000_000,
        "week" | "weeks" | "w" => 604_800_000_000,
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
        let mut builder = TimestampMicrosecondArray::builder(timestamps.len()).with_timezone("UTC");

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
        let mut builder = TimestampNanosecondArray::builder(timestamps.len()).with_timezone("UTC");
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
        Arc::new(vec![DataType::Binary]), // State type should match return type
    )
}

/// Wrapper for TDigest with accumulated values
#[derive(Debug, Clone)]
struct TDigestWrapper {
    values: Vec<f64>,
}

impl TDigestWrapper {
    fn new() -> Self {
        Self { values: Vec::new() }
    }

    fn insert(&mut self, value: f64) {
        self.values.push(value);
    }

    fn merge(&mut self, other: &TDigestWrapper) {
        self.values.extend(&other.values);
    }

    fn to_digest(&self) -> Option<TDigest> {
        if self.values.is_empty() { None } else { Some(TDigest::from_values(self.values.clone())) }
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::encode_to_vec(&self.values, bincode::config::standard()).unwrap_or_else(|_| Vec::new())
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let values: Vec<f64> = bincode::decode_from_slice(bytes, bincode::config::standard()).map_err(|e| format!("Failed to deserialize: {}", e))?.0;
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
        Self { digest: TDigestWrapper::new() }
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
                let other_digest = TDigestWrapper::from_bytes(bytes).map_err(DataFusionError::Execution)?;

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
#[derive(Debug, Hash, Eq, PartialEq)]
struct ApproxPercentileUDF {
    signature: Signature,
}

impl ApproxPercentileUDF {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Exact(vec![DataType::Float64, DataType::Binary]), Volatility::Immutable),
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

        // Determine the result size based on the digest array (which comes from GROUP BY)
        let digest_size = match &args.args[1] {
            ColumnarValue::Array(array) => array.len(),
            ColumnarValue::Scalar(_) => 1,
        };

        let percentile_array = match &args.args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(digest_size)?,
        };

        let digest_array = match &args.args[1] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(digest_size)?,
        };

        let percentile_values = percentile_array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("First argument must be a percentile (Float64)".to_string()))?;

        let digest_values = digest_array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| DataFusionError::Execution("Second argument must be a t-digest (Binary)".to_string()))?;

        // Ensure we process the correct number of rows
        let num_rows = digest_array.len();
        let mut builder = Float64Array::builder(num_rows);

        for i in 0..num_rows {
            if percentile_values.is_null(i) || digest_values.is_null(i) {
                builder.append_null();
            } else {
                let percentile = percentile_values.value(i);

                // Validate percentile is between 0 and 1
                if !(0.0..=1.0).contains(&percentile) {
                    return Err(DataFusionError::Execution(format!("Percentile must be between 0 and 1, got {}", percentile)));
                }

                let digest_bytes = digest_values.value(i);
                let wrapper = TDigestWrapper::from_bytes(digest_bytes).map_err(DataFusionError::Execution)?;

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

// ============================================================================
// jsonb_path_exists UDF for JSONPath queries on Variant/JSON columns
// ============================================================================

/// Create the jsonb_path_exists UDF for PostgreSQL-compatible JSONPath queries
fn create_jsonb_path_exists_udf() -> ScalarUDF {
    ScalarUDF::from(JsonbPathExistsUDF::new())
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct JsonbPathExistsUDF {
    signature: Signature,
}

impl JsonbPathExistsUDF {
    fn new() -> Self {
        Self {
            // Accept Variant struct or JSON string as first arg, path string as second
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonbPathExistsUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "jsonb_path_exists"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(DataFusionError::Execution(
                "jsonb_path_exists requires exactly 2 arguments: json/variant and jsonpath".to_string(),
            ));
        }

        let json_array = match &args.args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let path_str = match &args.args[1] {
            ColumnarValue::Scalar(scalar) => scalar_to_string(scalar).ok_or_else(|| DataFusionError::Execution("JSONPath must be a string".to_string()))?,
            ColumnarValue::Array(_) => {
                return Err(DataFusionError::Execution("JSONPath must be a scalar string".to_string()));
            }
        };

        // Parse the JSONPath expression
        let json_path = serde_json_path::JsonPath::parse(&path_str).map_err(|e| DataFusionError::Execution(format!("Invalid JSONPath: {}", e)))?;

        // Process based on input type
        let result = if is_variant_type(json_array.data_type()) {
            // Handle Variant struct type
            evaluate_jsonpath_on_variant(&json_array, &json_path)?
        } else {
            // Handle JSON string type
            evaluate_jsonpath_on_json_string(&json_array, &json_path)?
        };

        Ok(ColumnarValue::Array(result))
    }
}

/// Convert parquet_variant::Variant to serde_json::Value
fn variant_to_serde_json(variant: &parquet_variant::Variant) -> JsonValue {
    use base64::Engine;
    use parquet_variant::Variant;

    match variant {
        Variant::Null => JsonValue::Null,
        Variant::BooleanTrue => JsonValue::Bool(true),
        Variant::BooleanFalse => JsonValue::Bool(false),
        Variant::Int8(v) => json!(*v),
        Variant::Int16(v) => json!(*v),
        Variant::Int32(v) => json!(*v),
        Variant::Int64(v) => json!(*v),
        Variant::Float(v) => json!(*v),
        Variant::Double(v) => json!(*v),
        Variant::Decimal4(d) => json!(d.to_string()),
        Variant::Decimal8(d) => json!(d.to_string()),
        Variant::Decimal16(d) => json!(d.to_string()),
        Variant::Date(v) => json!(*v),
        Variant::Time(v) => json!(*v),
        Variant::Uuid(v) => json!(v.to_string()),
        Variant::TimestampMicros(v) => json!(*v),
        Variant::TimestampNtzMicros(v) => json!(*v),
        Variant::TimestampNanos(v) => json!(*v),
        Variant::TimestampNtzNanos(v) => json!(*v),
        Variant::Binary(bytes) => json!(base64::engine::general_purpose::STANDARD.encode(bytes)),
        Variant::String(s) => JsonValue::String(s.to_string()),
        Variant::ShortString(s) => JsonValue::String(s.as_str().to_string()),
        Variant::Object(obj) => {
            let mut map = serde_json::Map::new();
            for (key, value) in obj.iter() {
                map.insert(key.to_string(), variant_to_serde_json(&value));
            }
            JsonValue::Object(map)
        }
        Variant::List(list) => {
            let items: Vec<JsonValue> = list.iter().map(|v| variant_to_serde_json(&v)).collect();
            JsonValue::Array(items)
        }
    }
}

/// Evaluate JSONPath on a Variant (Struct) array
fn evaluate_jsonpath_on_variant(array: &ArrayRef, json_path: &serde_json_path::JsonPath) -> datafusion::error::Result<ArrayRef> {
    use datafusion::arrow::array::StructArray;
    use parquet_variant::Variant;

    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Execution("Expected Variant struct array".to_string()))?;

    let metadata_col = struct_array
        .column_by_name("metadata")
        .ok_or_else(|| DataFusionError::Execution("Variant missing metadata column".to_string()))?;
    let value_col = struct_array
        .column_by_name("value")
        .ok_or_else(|| DataFusionError::Execution("Variant missing value column".to_string()))?;

    let metadata_binary = metadata_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BinaryViewArray>()
        .ok_or_else(|| DataFusionError::Execution("Variant metadata not BinaryView".to_string()))?;
    let value_binary = value_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BinaryViewArray>()
        .ok_or_else(|| DataFusionError::Execution("Variant value not BinaryView".to_string()))?;

    let mut builder = BooleanArray::builder(struct_array.len());

    for i in 0..struct_array.len() {
        if struct_array.is_null(i) {
            builder.append_null();
            continue;
        }

        let metadata = metadata_binary.value(i);
        let value = value_binary.value(i);

        // Decode Variant to JSON
        let variant = Variant::new(metadata, value);
        let json_value = variant_to_serde_json(&variant);

        // Apply JSONPath and check if any matches exist
        let matches = json_path.query(&json_value);
        builder.append_value(!matches.is_empty());
    }

    Ok(Arc::new(builder.finish()))
}

/// Evaluate JSONPath on a JSON string array
fn evaluate_jsonpath_on_json_string(array: &ArrayRef, json_path: &serde_json_path::JsonPath) -> datafusion::error::Result<ArrayRef> {
    let mut builder = BooleanArray::builder(array.len());

    // Handle different string types
    if let Some(string_array) = array.as_any().downcast_ref::<StringViewArray>() {
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                builder.append_null();
            } else {
                let json_str = string_array.value(i);
                let result = match serde_json::from_str::<JsonValue>(json_str) {
                    Ok(json_value) => !json_path.query(&json_value).is_empty(),
                    Err(_) => false, // Invalid JSON returns false
                };
                builder.append_value(result);
            }
        }
    } else if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                builder.append_null();
            } else {
                let json_str = string_array.value(i);
                let result = match serde_json::from_str::<JsonValue>(json_str) {
                    Ok(json_value) => !json_path.query(&json_value).is_empty(),
                    Err(_) => false,
                };
                builder.append_value(result);
            }
        }
    } else {
        return Err(DataFusionError::Execution(
            "jsonb_path_exists requires JSON string or Variant input".to_string(),
        ));
    }

    Ok(Arc::new(builder.finish()))
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
