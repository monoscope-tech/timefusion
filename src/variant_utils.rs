//! Variant type utilities for converting between JSON and Variant,
//! and providing UDFs that work with both Variant and Utf8 inputs.
//!
//! This module uses the parquet-variant crate for proper Variant binary encoding
//! as specified in the Parquet Variant specification.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use arrow::array::{BinaryArray, BinaryViewArray, StringBuilder};
use parquet_variant::Variant;
use parquet_variant_compute::json_to_variant;
use parquet_variant_json::VariantToJson;

/// Columns that should be stored as Variant type
pub const VARIANT_COLUMNS: &[&str] = &["body", "context", "events", "links", "attributes", "resource", "errors"];

/// Get the Arrow DataType for Variant (Struct with metadata and value BinaryView fields)
/// This matches the parquet-variant-compute output
pub fn variant_data_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("metadata", DataType::BinaryView, false)),
        Arc::new(Field::new("value", DataType::BinaryView, false)),
    ]))
}

/// Check if an array is a Variant type (Struct with metadata and value fields)
pub fn is_variant_array(array: &ArrayRef) -> bool {
    if let DataType::Struct(fields) = array.data_type() {
        fields.len() == 2
            && fields.iter().any(|f| f.name() == "metadata" && matches!(f.data_type(), DataType::BinaryView | DataType::Binary))
            && fields.iter().any(|f| f.name() == "value" && matches!(f.data_type(), DataType::BinaryView | DataType::Binary))
    } else {
        false
    }
}

/// Check if a ColumnarValue is a Variant type
pub fn is_variant_columnar(value: &ColumnarValue) -> bool {
    match value {
        ColumnarValue::Array(arr) => is_variant_array(arr),
        ColumnarValue::Scalar(ScalarValue::Struct(arr)) => is_variant_array(&(arr.clone() as ArrayRef)),
        _ => false,
    }
}

/// Convert a JSON string array to a Variant array using proper parquet-variant encoding
pub fn json_to_variant_array(json_array: &StringArray) -> Result<ArrayRef, DataFusionError> {
    let array_ref: ArrayRef = Arc::new(json_array.clone());
    let variant_array = json_to_variant(&array_ref)
        .map_err(|e| DataFusionError::Execution(format!("Failed to convert JSON to Variant: {}", e)))?;
    Ok(Arc::new(variant_array.into_inner()))
}

/// Convert a Variant array to a JSON string array
/// Handles both Binary and BinaryView field types
pub fn variant_to_json_array(variant_array: &StructArray) -> Result<ArrayRef, DataFusionError> {
    let metadata_col = variant_array.column_by_name("metadata")
        .ok_or_else(|| DataFusionError::Execution("Missing metadata field in Variant".to_string()))?;
    let value_col = variant_array.column_by_name("value")
        .ok_or_else(|| DataFusionError::Execution("Missing value field in Variant".to_string()))?;

    // Helper to get bytes from either Binary or BinaryView array
    fn get_bytes<'a>(arr: &'a ArrayRef, idx: usize) -> Option<&'a [u8]> {
        if let Some(binary) = arr.as_any().downcast_ref::<BinaryArray>() {
            if binary.is_null(idx) { None } else { Some(binary.value(idx)) }
        } else if let Some(binary_view) = arr.as_any().downcast_ref::<BinaryViewArray>() {
            if binary_view.is_null(idx) { None } else { Some(binary_view.value(idx)) }
        } else {
            None
        }
    }

    let mut builder = StringBuilder::new();
    for i in 0..variant_array.len() {
        if variant_array.is_null(i) {
            builder.append_null();
        } else {
            let metadata = get_bytes(metadata_col, i)
                .ok_or_else(|| DataFusionError::Execution("Missing metadata bytes".to_string()))?;
            let value = get_bytes(value_col, i)
                .ok_or_else(|| DataFusionError::Execution("Missing value bytes".to_string()))?;
            let variant = Variant::new(metadata, value);
            let json_str = variant.to_json_string()
                .map_err(|e| DataFusionError::Execution(format!("Failed to convert Variant to JSON: {}", e)))?;
            builder.append_value(&json_str);
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Convert a ColumnarValue to JSON string if it's a Variant, otherwise pass through
pub fn ensure_json_columnar(value: &ColumnarValue) -> Result<ColumnarValue, DataFusionError> {
    if !is_variant_columnar(value) {
        return Ok(value.clone());
    }

    match value {
        ColumnarValue::Array(arr) => {
            let struct_arr = arr.as_any().downcast_ref::<StructArray>().ok_or_else(|| DataFusionError::Execution("Expected StructArray".to_string()))?;
            let json_arr = variant_to_json_array(struct_arr)?;
            Ok(ColumnarValue::Array(json_arr))
        }
        ColumnarValue::Scalar(scalar) => {
            if let ScalarValue::Struct(arr) = scalar {
                let json_arr = variant_to_json_array(arr)?;
                let json_str = json_arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| DataFusionError::Execution("Expected StringArray".to_string()))?;
                if json_str.is_null(0) {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(json_str.value(0).to_string()))))
                }
            } else {
                Ok(value.clone())
            }
        }
    }
}

/// Convert a RecordBatch, transforming JSON string columns (Utf8) to Variant for specified columns
pub fn convert_batch_json_to_variant(batch: RecordBatch) -> Result<RecordBatch, DataFusionError> {
    let schema = batch.schema();
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let mut new_fields: Vec<Arc<Field>> = Vec::with_capacity(batch.num_columns());

    for (i, field) in schema.fields().iter().enumerate() {
        let column = batch.column(i);

        if VARIANT_COLUMNS.contains(&field.name().as_str()) && matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            // Convert UTF8 JSON string to Variant using proper parquet-variant encoding
            let string_array = column.as_any().downcast_ref::<StringArray>().ok_or_else(|| DataFusionError::Execution("Expected StringArray".to_string()))?;
            let variant_array = json_to_variant_array(string_array)?;
            let variant_field = Arc::new(Field::new(field.name(), variant_data_type(), field.is_nullable()));
            new_columns.push(variant_array);
            new_fields.push(variant_field);
        } else {
            new_columns.push(column.clone());
            new_fields.push(field.clone());
        }
    }

    let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns).map_err(|e| DataFusionError::Execution(format!("Failed to create batch: {}", e)))
}

// ============================================================================
// Replacement UDFs that work with both Variant and Utf8 inputs
// ============================================================================

/// Create the json_get UDF (-> operator) that works with both Variant and Utf8
pub fn create_json_get_udf() -> ScalarUDF {
    ScalarUDF::from(VariantAwareJsonGetUDF::new())
}

/// Create the json_get_str UDF (->> operator) that works with both Variant and Utf8
pub fn create_json_get_str_udf() -> ScalarUDF {
    ScalarUDF::from(VariantAwareJsonGetStrUDF::new())
}

/// Create the json_length UDF that works with both Variant and Utf8
pub fn create_json_length_udf() -> ScalarUDF {
    ScalarUDF::from(VariantAwareJsonLengthUDF::new())
}

/// Create the json_contains UDF that works with both Variant and Utf8
pub fn create_json_contains_udf() -> ScalarUDF {
    ScalarUDF::from(VariantAwareJsonContainsUDF::new())
}

// ============================================================================
// UDF Implementations
// ============================================================================

#[derive(Debug, Hash, Eq, PartialEq)]
struct VariantAwareJsonGetUDF {
    signature: Signature,
}

impl VariantAwareJsonGetUDF {
    fn new() -> Self {
        Self { signature: Signature::variadic_any(Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for VariantAwareJsonGetUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_get"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        if args.args.len() < 2 {
            return Err(DataFusionError::Execution("json_get requires at least 2 arguments".to_string()));
        }

        // Convert first argument from Variant to JSON if needed
        let json_arg = ensure_json_columnar(&args.args[0])?;

        // Call the underlying datafusion-functions-json implementation
        let mut new_args = args.args.clone();
        new_args[0] = json_arg;

        // Use the json_get UDF from datafusion-functions-json
        datafusion_functions_json::udfs::json_get_udf().invoke_with_args(ScalarFunctionArgs { args: new_args, ..args })
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct VariantAwareJsonGetStrUDF {
    signature: Signature,
}

impl VariantAwareJsonGetStrUDF {
    fn new() -> Self {
        Self { signature: Signature::variadic_any(Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for VariantAwareJsonGetStrUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_get_str"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        if args.args.len() < 2 {
            return Err(DataFusionError::Execution("json_get_str requires at least 2 arguments".to_string()));
        }

        let json_arg = ensure_json_columnar(&args.args[0])?;
        let mut new_args = args.args.clone();
        new_args[0] = json_arg;

        datafusion_functions_json::udfs::json_get_str_udf().invoke_with_args(ScalarFunctionArgs { args: new_args, ..args })
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct VariantAwareJsonLengthUDF {
    signature: Signature,
}

impl VariantAwareJsonLengthUDF {
    fn new() -> Self {
        Self { signature: Signature::variadic_any(Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for VariantAwareJsonLengthUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        if args.args.is_empty() {
            return Err(DataFusionError::Execution("json_length requires at least 1 argument".to_string()));
        }

        let json_arg = ensure_json_columnar(&args.args[0])?;
        let mut new_args = args.args.clone();
        new_args[0] = json_arg;

        datafusion_functions_json::udfs::json_length_udf().invoke_with_args(ScalarFunctionArgs { args: new_args, ..args })
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct VariantAwareJsonContainsUDF {
    signature: Signature,
}

impl VariantAwareJsonContainsUDF {
    fn new() -> Self {
        Self { signature: Signature::variadic_any(Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for VariantAwareJsonContainsUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        if args.args.len() < 2 {
            return Err(DataFusionError::Execution("json_contains requires at least 2 arguments".to_string()));
        }

        let json_arg = ensure_json_columnar(&args.args[0])?;
        let mut new_args = args.args.clone();
        new_args[0] = json_arg;

        datafusion_functions_json::udfs::json_contains_udf().invoke_with_args(ScalarFunctionArgs { args: new_args, ..args })
    }
}

/// Register all variant-aware JSON functions in the session context
pub fn register_variant_json_functions(ctx: &mut datafusion::execution::context::SessionContext) {
    // Register our variant-aware replacements that override the datafusion-functions-json ones
    ctx.register_udf(create_json_get_udf());
    ctx.register_udf(create_json_get_str_udf());
    ctx.register_udf(create_json_length_udf());
    ctx.register_udf(create_json_contains_udf());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variant_data_type() {
        let dt = variant_data_type();
        assert!(matches!(dt, DataType::Struct(_)));
    }

    #[test]
    fn test_json_to_variant_roundtrip() {
        let json_data = vec![Some(r#"{"name": "test", "value": 123}"#), None, Some(r#"[1, 2, 3]"#)];
        let json_array = StringArray::from(json_data);

        // Convert to variant
        let variant_arr = json_to_variant_array(&json_array).unwrap();

        // Convert back to JSON
        let struct_arr = variant_arr.as_any().downcast_ref::<StructArray>().unwrap();
        let json_back = variant_to_json_array(struct_arr).unwrap();
        let json_str = json_back.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(json_str.len(), 3);
        assert!(!json_str.is_null(0));
        assert!(json_str.is_null(1));
        assert!(!json_str.is_null(2));

        // Verify content (JSON may be reordered but should parse to same value)
        let parsed_original: serde_json::Value = serde_json::from_str(r#"{"name": "test", "value": 123}"#).unwrap();
        let parsed_roundtrip: serde_json::Value = serde_json::from_str(json_str.value(0)).unwrap();
        assert_eq!(parsed_original, parsed_roundtrip);

        let parsed_array: serde_json::Value = serde_json::from_str(json_str.value(2)).unwrap();
        assert_eq!(parsed_array, serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn test_is_variant_array() {
        let json_array = StringArray::from(vec![Some(r#"{"key": "value"}"#)]);
        let variant_arr = json_to_variant_array(&json_array).unwrap();

        assert!(is_variant_array(&variant_arr));

        // Non-variant array should return false
        let string_arr: ArrayRef = Arc::new(StringArray::from(vec!["test"]));
        assert!(!is_variant_array(&string_arr));
    }
}
