use std::{any::Any, sync::Arc};

use anyhow::Result;
use chrono::{DateTime, Datelike, Utc};
use chrono_tz::Tz;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, ListArray, StringArray, StringViewArray, StringViewBuilder,
            TimestampMicrosecondArray, TimestampNanosecondArray,
        },
        datatypes::{DataType, TimeUnit},
    },
    common::{DFSchema, DataFusionError, ExprSchema, ScalarValue, not_impl_err},
    logical_expr::{
        Accumulator, AggregateUDF, ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarFunctionImplementation, ScalarUDF, ScalarUDFImpl, Signature,
        TypeSignature, Volatility, create_udaf, create_udf,
        expr::{Alias, ScalarFunction},
        planner::{ExprPlanner, PlannerResult, RawBinaryExpr},
    },
    sql::sqlparser::ast::BinaryOperator,
};
use serde_json::{Value as JsonValue, json};
use tdigests::TDigest;

use crate::schema_loader::is_variant_type;

/// Extract a String from any ScalarValue string type (Utf8, Utf8View, LargeUtf8)
fn scalar_to_string(scalar: &ScalarValue) -> Option<String> {
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Pull a single UTF-8 string out of a scalar-or-length-1-array argument.
/// Used by UDFs whose Nth argument is a constant string (format, timezone,
/// etc.). `label` names the argument in error messages.
fn extract_scalar_string(arg: &ColumnarValue, label: &str) -> datafusion::error::Result<String> {
    let not_utf8 = || DataFusionError::Execution(format!("{label} must be a UTF8 string"));
    let not_scalar = || DataFusionError::Execution(format!("{label} must be a scalar value"));
    match arg {
        ColumnarValue::Scalar(scalar) => scalar_to_string(scalar).ok_or_else(not_utf8),
        ColumnarValue::Array(arr) => {
            // `&&` short-circuits so is_null(0) is never called on an empty array.
            if let Some(a) = arr.as_any().downcast_ref::<StringViewArray>() {
                if a.len() == 1 && !a.is_null(0) { Ok(a.value(0).to_string()) } else { Err(not_scalar()) }
            } else if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                if a.len() == 1 && !a.is_null(0) { Ok(a.value(0).to_string()) } else { Err(not_scalar()) }
            } else {
                Err(not_utf8())
            }
        }
    }
}

/// Emits the three boilerplate `ScalarUDFImpl` methods (`as_any`, `name`,
/// `signature`) shared by every UDF in this module that stores its `Signature`
/// in a `signature` field. `return_type` / `invoke_with_args` stay per-impl.
macro_rules! scalar_udf_boilerplate {
    ($name:literal) => {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            $name
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
    };
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

        // Build the variant_get(base, '<path>') call. For `->` we return the
        // Variant leaf so chained `->` keeps working. For `->>` we'd previously
        // ask variant_get to project as Utf8, but that returns NULL for
        // numeric/boolean leaves (parquet_variant_compute doesn't stringify).
        // Postgres `->>` text semantics need numeric/bool → text, JSON null →
        // SQL NULL, and string → unquoted. Compose:
        //   variant_get(col, path) → Variant
        //   variant_to_json(...)   → JSON-encoded text (Utf8)
        //   json_to_pg_text(...)   → Postgres ->> text
        let variant_get_udf = ScalarUDF::from(VariantGetExtUdf::default());
        let path_literal = Expr::Literal(ScalarValue::Utf8(Some(full_path.clone())), None);
        let get_args = vec![base_expr.clone(), path_literal];
        let variant_leaf = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(variant_get_udf),
            args: get_args,
        });
        let result = if is_long_arrow {
            let to_json = Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(ScalarUDF::from(VariantToJsonExtUdf::default())),
                args: vec![variant_leaf],
            });
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(ScalarUDF::from(JsonToPgTextUdf::default())),
                args: vec![to_json],
            })
        } else {
            variant_leaf
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

/// `json_to_pg_text(utf8) → utf8`: convert JSON-encoded text to Postgres `->>` text.
///
/// - JSON string `"Alice"` → `Alice` (parsed, so escape sequences resolve correctly)
/// - JSON null → SQL NULL
/// - JSON number / boolean → its literal text (`42`, `true`)
/// - JSON object / array → returned as-is (Postgres `->>` does the same)
///
/// Bridges `parquet_variant_compute::variant_get`'s NULL-on-non-string-cast
/// behavior to the Postgres `->>` contract.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonToPgTextUdf {
    signature: Signature,
}

impl Default for JsonToPgTextUdf {
    fn default() -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonToPgTextUdf {
    scalar_udf_boilerplate!("json_to_pg_text");
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::compute::cast;
        let arr = match args.args.into_iter().next().unwrap() {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
        };
        // Cast once to Utf8 — collapses Utf8/Utf8View/LargeUtf8 to a single
        // concrete shape, single pass over rows.
        let utf8 = cast(&arr, &DataType::Utf8).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let strs = utf8
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("json_to_pg_text: cast to Utf8 failed".into()))?;
        let mut b = datafusion::arrow::array::StringBuilder::with_capacity(strs.len(), strs.value_data().len());
        for i in 0..strs.len() {
            if strs.is_null(i) {
                b.append_null();
                continue;
            }
            // Parse via serde_json so escape sequences resolve correctly and
            // false-positive shapes like '"a"+"b"' don't trigger naive unquoting.
            // JSON null → SQL NULL; JSON string → its raw text; anything else
            // (number, bool, object, array) → its JSON literal text (per Postgres ->>).
            let s = strs.value(i);
            match serde_json::from_str::<JsonValue>(s) {
                Ok(JsonValue::Null) => b.append_null(),
                Ok(JsonValue::String(inner)) => b.append_value(&inner),
                Ok(_) | Err(_) => b.append_value(s),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish())))
    }
}

/// `datafusion-variant`'s UDFs call `try_field_as_variant_array(field)` on
/// their first arg and bail with "Extension type name missing" when the
/// field lacks the `ARROW:extension:name = arrow.parquet.variant` marker.
/// That marker survives in the LogicalPlan's `projected_schema` (set by
/// `VariantSelectRewriter::patch_table_scan` and by `SchemaRegistry`'s
/// `fields()`), but is stripped on the way to the physical executor's
/// per-row Field — so any SELECT touching a Variant column would panic at
/// execution time. We re-stamp the marker here right before delegating.
fn stamp_variant_field(f: &Arc<datafusion::arrow::datatypes::Field>) -> Arc<datafusion::arrow::datatypes::Field> {
    const EXT_KEY: &str = "ARROW:extension:name";
    const EXT_VAL: &str = "arrow.parquet.variant";
    if !is_variant_type(f.data_type()) || f.metadata().get(EXT_KEY).map(String::as_str) == Some(EXT_VAL) {
        return f.clone();
    }
    let mut md = f.metadata().clone();
    md.insert(EXT_KEY.into(), EXT_VAL.into());
    Arc::new(f.as_ref().clone().with_metadata(md))
}

/// Wrap a `datafusion-variant` UDF so its arg fields get the Variant
/// extension marker re-stamped before delegation. Generic over the inner
/// UDF type so `VariantToJsonUdf` and `VariantGetUdf` share one impl.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct VariantExtWrapper<U: ScalarUDFImpl + Default + Hash + PartialEq + Eq + 'static> {
    inner: U,
}

impl<U: ScalarUDFImpl + Default + Hash + PartialEq + Eq + 'static> Default for VariantExtWrapper<U> {
    fn default() -> Self {
        Self { inner: U::default() }
    }
}

impl<U: ScalarUDFImpl + Default + Hash + PartialEq + Eq + 'static> ScalarUDFImpl for VariantExtWrapper<U> {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        self.inner.return_type(arg_types)
    }
    // VariantGetUdf in particular panics in `return_type` and instead
    // computes the output Field shape from arg types via this method, so
    // we must forward it rather than rely on the default that calls
    // return_type.
    fn return_field_from_args(&self, args: datafusion::logical_expr::ReturnFieldArgs) -> datafusion::error::Result<datafusion::arrow::datatypes::FieldRef> {
        self.inner.return_field_from_args(args)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        args.arg_fields = args.arg_fields.iter().map(stamp_variant_field).collect();
        self.inner.invoke_with_args(args)
    }
}

use std::hash::Hash;
pub type VariantToJsonExtUdf = VariantExtWrapper<datafusion_variant::VariantToJsonUdf>;
pub type VariantGetExtUdf = VariantExtWrapper<datafusion_variant::VariantGetUdf>;

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

    // Bridges variant -> Postgres ->> text semantics (numeric/bool/null → text/NULL).
    ctx.register_udf(ScalarUDF::from(JsonToPgTextUdf::default()));

    // Register variant functions from datafusion-variant
    ctx.register_udf(ScalarUDF::from(datafusion_variant::JsonToVariantUdf::default()));
    ctx.register_udf(ScalarUDF::from(VariantToJsonExtUdf::default()));
    ctx.register_udf(ScalarUDF::from(VariantGetExtUdf::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::CastToVariantUdf::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::IsVariantNullUdf::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::VariantPretty::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::VariantListConstruct::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::VariantListInsert::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::VariantObjectConstruct::default()));
    ctx.register_udf(ScalarUDF::from(datafusion_variant::VariantObjectInsert::default()));

    // Register jsonb_path_exists for JSONPath queries on Variant columns
    ctx.register_udf(create_jsonb_path_exists_udf());

    // Register text_match(col, 'query') for tantivy-accelerated full-text search.
    // Naive substring fallback ensures correctness when tantivy is disabled or
    // when post-filtering MemBuffer rows; see [[tantivy_index/udf]].
    ctx.register_udf(crate::tantivy_index::udf::text_match_udf());

    // Test-only clock UDFs. Gated behind TIMEFUSION_ENABLE_TEST_UDFS so a
    // production deployment can't have its eviction/flush clock yanked by
    // a stray SQL session. Required by the long-duration bench harness in
    // `bench/timeseries_lifecycle.py` to simulate hours in seconds.
    if std::env::var("TIMEFUSION_ENABLE_TEST_UDFS").map(|v| v == "true" || v == "1").unwrap_or(false) {
        ctx.register_udf(create_set_clock_udf());
        ctx.register_udf(create_advance_clock_udf());
        ctx.register_udf(create_now_micros_udf());
        tracing::warn!("TIMEFUSION_ENABLE_TEST_UDFS=true; clock UDFs registered. Do NOT enable in production.");
    }

    Ok(())
}

pub type FnRegistry = dyn datafusion::execution::FunctionRegistry + Send + Sync;

/// Process-wide Arc'd FunctionRegistry pre-populated with all custom UDFs.
/// Lazy-init via OnceLock so test/bench harnesses that build many layers don't
/// re-register UDFs 20× per test. Production builds it once at startup either
/// way.
pub fn function_registry() -> Result<Arc<FnRegistry>> {
    static CELL: std::sync::OnceLock<Arc<FnRegistry>> = std::sync::OnceLock::new();
    if let Some(reg) = CELL.get() {
        return Ok(Arc::clone(reg));
    }
    let mut ctx = datafusion::execution::context::SessionContext::new();
    register_custom_functions(&mut ctx)?;
    let arc: Arc<FnRegistry> = Arc::new(ctx.state());
    // First-write-wins; if a parallel test won the race we just discard ours.
    let _ = CELL.set(Arc::clone(&arc));
    Ok(arc)
}

/// `timefusion_set_clock(rfc3339_text)` → bigint micros-since-epoch.
fn create_set_clock_udf() -> ScalarUDF {
    use datafusion::arrow::{
        array::{Int64Array, StringArray},
        datatypes::DataType,
    };
    let fun: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        let arr = match &args[0] {
            ColumnarValue::Array(a) => a.clone(),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };
        let s = arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("timefusion_set_clock expects Utf8".into()))?;
        let mut b = Int64Array::builder(s.len());
        for i in 0..s.len() {
            if s.is_null(i) {
                b.append_null();
                continue;
            }
            let t = chrono::DateTime::parse_from_rfc3339(s.value(i))
                .map_err(|e| DataFusionError::Execution(format!("invalid rfc3339: {e}")))?
                .timestamp_micros();
            b.append_value(crate::clock::set_micros(t));
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish())))
    });
    create_udf("timefusion_set_clock", vec![DataType::Utf8], DataType::Int64, Volatility::Volatile, fun)
}

/// `timefusion_advance_clock(delta_micros)` → new bigint micros.
fn create_advance_clock_udf() -> ScalarUDF {
    use datafusion::arrow::{array::Int64Array, datatypes::DataType};
    let fun: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        let arr = match &args[0] {
            ColumnarValue::Array(a) => a.clone(),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };
        let d = arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("timefusion_advance_clock expects Int64".into()))?;
        let mut b = Int64Array::builder(d.len());
        for i in 0..d.len() {
            if d.is_null(i) {
                b.append_null();
            } else {
                b.append_value(crate::clock::advance_micros(d.value(i)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish())))
    });
    create_udf("timefusion_advance_clock", vec![DataType::Int64], DataType::Int64, Volatility::Volatile, fun)
}

/// `timefusion_now_micros()` → current clock value (frozen or wall).
fn create_now_micros_udf() -> ScalarUDF {
    use datafusion::arrow::{array::Int64Array, datatypes::DataType};
    let fun: ScalarFunctionImplementation = Arc::new(move |_args: &[ColumnarValue]| {
        let v = crate::clock::now_micros();
        Ok(ColumnarValue::Array(Arc::new(Int64Array::from(vec![v]))))
    });
    create_udf("timefusion_now_micros", vec![], DataType::Int64, Volatility::Volatile, fun)
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
    scalar_udf_boilerplate!("to_char");

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
        let format_str = extract_scalar_string(&args[1], "Format string")?;

        let result = format_timestamps(&timestamp_array, &format_str)?;
        Ok(ColumnarValue::Array(result))
    }
}

/// Format timestamps according to PostgreSQL format patterns
fn format_timestamps(timestamp_array: &ArrayRef, format_str: &str) -> datafusion::error::Result<ArrayRef> {
    let parts = parse_pg_format(format_str);
    let mut builder = StringViewBuilder::new();

    let format_fn = |timestamp_us: i64| -> datafusion::error::Result<String> {
        DateTime::<Utc>::from_timestamp_micros(timestamp_us)
            .ok_or_else(|| DataFusionError::Execution("Invalid timestamp".to_string()))
            .map(|dt| render_pg_format(&parts, &dt))
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

/// One segment of a parsed Postgres format string. Most tokens collapse to a
/// `Chrono` spec; `PgD` / `PgDY` exist because chrono has no exact equivalent
/// for the Postgres day-of-week semantics (Sun=1..Sat=7 / uppercase abbrev).
#[derive(Debug, PartialEq)]
enum FmtPart {
    /// A chrono strftime spec (e.g. `"%Y"`) or escaped-literal slice.
    Chrono(String),
    /// Postgres `D`: day of week, Sunday=1..Saturday=7.
    PgD,
    /// Postgres `DY`: uppercase abbreviated weekday name (e.g. `"WED"`).
    PgDY,
}

/// Render a parsed Postgres format against a `DateTime<Utc>`.
fn render_pg_format(parts: &[FmtPart], dt: &DateTime<Utc>) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for part in parts {
        match part {
            FmtPart::Chrono(spec) => {
                let _ = write!(out, "{}", dt.format(spec));
            }
            FmtPart::PgD => {
                // chrono `num_days_from_sunday` is 0=Sun..6=Sat; Postgres `D` is 1..7.
                let _ = write!(out, "{}", dt.weekday().num_days_from_sunday() + 1);
            }
            FmtPart::PgDY => {
                // Abbreviated English weekday is ASCII-only, so to_ascii_uppercase suffices
                // and avoids the locale-aware Unicode case-folding overhead of to_uppercase.
                let mut s = dt.format("%a").to_string();
                s.make_ascii_uppercase();
                out.push_str(&s);
            }
        }
    }
    out
}

/// Parse a PostgreSQL `to_char` format string into a sequence of render parts.
///
/// Honors Postgres literal-escape syntax: text inside `"..."` is copied verbatim
/// (with `""` standing for a literal `"`). Outside literals, the longest matching
/// token is replaced with its chrono equivalent.
///
/// **Known divergences from real Postgres** (intentional):
/// - `Month` / `Day` output is unpadded; real Postgres pads to 9 chars. E2E
///   callers rely on the unpadded form. Re-add padding behind a custom
///   formatter only if a caller asks.
/// - Token matching is case-sensitive. Real Postgres `to_char` is case-insensitive
///   (e.g. `yyyy == YYYY`). Has been true since the original chained-replace
///   implementation; not a regression.
/// - Unterminated `"..."` literals are accepted (the remainder is copied
///   verbatim). Real Postgres errors. Lenient behaviour matches the
///   chained-replace predecessor.
/// - `HH` aliases `HH12` (12-hour clock with leading zero), matching Postgres.
///   Do not "fix" it to `%H` — Postgres `HH` is *not* `HH24`.
///
/// **Not yet implemented** (silently pass through as literal text — same as the
/// chained-replace predecessor): `Q`, `WW`, `IW`, `CC`, `J`, `OF`, `TZH`, `TZM`,
/// rare numeric tokens, locale-affected text tokens. Add to `TOKENS` (or as new
/// `FmtPart` variants for cases with no chrono equivalent) when a caller needs them.
fn parse_pg_format(pg_format: &str) -> Vec<FmtPart> {
    // (Postgres token, chrono spec). Longest-prefix wins, so order matters within
    // groups that share a prefix (HH24 before HH, Month before Mon before MM, etc.).
    const TOKENS: &[(&str, &str)] = &[
        ("YYYY", "%Y"),
        ("YY", "%y"),
        // Note: Postgres pads `Month` / `Day` output to 9 chars; chrono's %B / %A do not.
        // E2E callers rely on the unpadded form, so we keep the divergence — re-add padding
        // only if a caller asks for it.
        ("Month", "%B"),
        ("Mon", "%b"),
        ("MM", "%m"),
        ("DD", "%d"),
        ("Day", "%A"),
        ("Dy", "%a"),
        // `D` and `DY` are handled below as PgD / PgDY because chrono has no
        // exact equivalent for Postgres's Sun=1..Sat=7 numbering or its
        // uppercase abbreviated weekday name. They must be matched before the
        // single-char fallback below; they aren't in this table so we test for
        // them explicitly in the loop.
        ("HH24", "%H"),
        ("HH12", "%I"),
        ("HH", "%I"),
        ("MI", "%M"),
        ("SS", "%S"),
        ("US", "%6f"),
        ("MS", "%3f"),
        ("TZ", "%Z"),
        ("AM", "%p"),
        ("PM", "%p"),
    ];

    // All token keys are ASCII so byte-prefix matching is sound, but the non-token
    // pass-through path must walk UTF-8 char boundaries — a multi-byte char in a `"..."`
    // literal would otherwise be split into separate `char`s and produce mojibake.
    let bytes = pg_format.as_bytes();
    let mut parts: Vec<FmtPart> = Vec::new();
    // Accumulate chrono spec / literal text into a buffer; flush on a non-chrono
    // boundary (PgD / PgDY) or at the end so the resulting Vec stays compact.
    let mut buf = String::with_capacity(pg_format.len());
    let flush = |parts: &mut Vec<FmtPart>, buf: &mut String| {
        if !buf.is_empty() {
            parts.push(FmtPart::Chrono(std::mem::take(buf)));
        }
    };
    let push_passthrough = |buf: &mut String, s: &str, i: &mut usize| {
        let c = s[*i..].chars().next().expect("loop invariant: i < s.len()");
        // chrono treats `%` as a format-spec start; double it to emit a literal.
        if c == '%' {
            buf.push('%');
        }
        buf.push(c);
        *i += c.len_utf8();
    };
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            // Literal section: copy until matching `"`. `""` inside is an escaped quote.
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        buf.push('"');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                push_passthrough(&mut buf, pg_format, &mut i);
            }
            continue;
        }
        // `DY` must be matched before bare `D` (longest-prefix). Neither is in TOKENS.
        // Same trailing-alpha guard as `D` below so a future `DYY`-style token doesn't
        // get greedily consumed as `DY` + leftover `Y`.
        if bytes[i..].starts_with(b"DY") && !bytes.get(i + 2).is_some_and(|b| b.is_ascii_alphabetic()) {
            flush(&mut parts, &mut buf);
            parts.push(FmtPart::PgDY);
            i += 2;
            continue;
        }
        if bytes[i] == b'D' && !bytes.get(i + 1).is_some_and(|b| b.is_ascii_alphabetic()) {
            // Bare `D` only — guarded so `D<letter>` (e.g. a future token starting with D)
            // doesn't get consumed here. `Day`, `Dy`, `DD` are caught by TOKENS / DY above.
            flush(&mut parts, &mut buf);
            parts.push(FmtPart::PgD);
            i += 1;
            continue;
        }
        let matched = TOKENS.iter().find(|(pg, _)| bytes[i..].starts_with(pg.as_bytes()));
        if let Some((pg, chrono)) = matched {
            buf.push_str(chrono);
            i += pg.len();
            continue;
        }
        push_passthrough(&mut buf, pg_format, &mut i);
    }
    flush(&mut parts, &mut buf);
    parts
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
    scalar_udf_boilerplate!("at_time_zone");

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
        let tz_str = extract_scalar_string(&args[1], "Timezone")?;

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
    aliases:   Vec<String>,
}

impl JsonBuildArrayUDF {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            // `jsonb_build_array` is Postgres-only; TimeFusion stores JSON as Utf8View either way.
            aliases:   vec!["jsonb_build_array".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonBuildArrayUDF {
    scalar_udf_boilerplate!("json_build_array");

    fn aliases(&self) -> &[String] {
        &self.aliases
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
    aliases:   Vec<String>,
}

impl ToJsonUDF {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            // `to_jsonb` is Postgres-only; TimeFusion stores JSON as Utf8View either way.
            aliases:   vec!["to_jsonb".to_string()],
        }
    }
}

impl ScalarUDFImpl for ToJsonUDF {
    scalar_udf_boilerplate!("to_json");

    fn aliases(&self) -> &[String] {
        &self.aliases
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
    scalar_udf_boilerplate!("extract_epoch");

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

/// Downcast `array` to a primitive Arrow array and push each element into
/// `values` as `json!(value)`, mapping nulls to `JsonValue::Null`.
macro_rules! push_json_primitive {
    ($array:expr, $values:expr, $ty:ty, $tyname:literal) => {{
        let arr = $array
            .as_any()
            .downcast_ref::<$ty>()
            .ok_or_else(|| DataFusionError::Execution(concat!("Failed to downcast to ", $tyname).to_string()))?;
        for i in 0..arr.len() {
            if arr.is_null(i) {
                $values.push(JsonValue::Null);
            } else {
                $values.push(json!(arr.value(i)));
            }
        }
    }};
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
        DataType::Int64 => push_json_primitive!(array, values, Int64Array, "Int64Array"),
        DataType::Float64 => push_json_primitive!(array, values, Float64Array, "Float64Array"),
        DataType::Boolean => push_json_primitive!(array, values, BooleanArray, "BooleanArray"),
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
    scalar_udf_boilerplate!("approx_percentile");

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
    scalar_udf_boilerplate!("jsonb_path_exists");

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
            evaluate_jsonpath_on_variant(&json_array, &json_path, &path_str)?
        } else {
            // Handle JSON string type
            evaluate_jsonpath_on_json_string(&json_array, &json_path)?
        };

        Ok(ColumnarValue::Array(result))
    }
}

const MAX_VARIANT_DEPTH: usize = 100;

/// Convert parquet_variant::Variant to serde_json::Value with depth limit to prevent stack overflow
fn variant_to_serde_json(variant: &parquet_variant::Variant, depth: usize) -> Result<JsonValue, DataFusionError> {
    use base64::Engine;
    use parquet_variant::Variant;

    if depth > MAX_VARIANT_DEPTH {
        return Err(DataFusionError::Execution(format!(
            "Variant nesting depth exceeds limit of {}",
            MAX_VARIANT_DEPTH
        )));
    }

    Ok(match variant {
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
        Variant::ShortString(s) => JsonValue::String(s.to_string()),
        Variant::Object(obj) => {
            let mut map = serde_json::Map::new();
            for (key, value) in obj.iter() {
                map.insert(key.to_string(), variant_to_serde_json(&value, depth + 1)?);
            }
            JsonValue::Object(map)
        }
        Variant::List(list) => {
            let items: Vec<JsonValue> = list.iter().map(|v| variant_to_serde_json(&v, depth + 1)).collect::<Result<_, _>>()?;
            JsonValue::Array(items)
        }
    })
}

/// Accessor that uniformly reads bytes from either `BinaryArray` or `BinaryViewArray`.
/// Delta-rs/Parquet may yield either representation depending on
/// `schema_force_view_types`, so variant decoding handles both transparently.
enum BinaryAccessor<'a> {
    Binary(&'a datafusion::arrow::array::BinaryArray),
    View(&'a datafusion::arrow::array::BinaryViewArray),
}

impl<'a> BinaryAccessor<'a> {
    fn try_new(col: &'a ArrayRef, field: &str) -> datafusion::error::Result<Self> {
        if let Some(a) = col.as_any().downcast_ref::<datafusion::arrow::array::BinaryArray>() {
            Ok(Self::Binary(a))
        } else if let Some(a) = col.as_any().downcast_ref::<datafusion::arrow::array::BinaryViewArray>() {
            Ok(Self::View(a))
        } else {
            Err(DataFusionError::Execution(format!(
                "Variant {field} column is not Binary or BinaryView (got {:?})",
                col.data_type()
            )))
        }
    }

    fn value(&self, i: usize) -> &[u8] {
        match self {
            Self::Binary(a) => a.value(i),
            Self::View(a) => a.value(i),
        }
    }
}

/// Evaluate JSONPath on a Variant (Struct) array
fn evaluate_jsonpath_on_variant(array: &ArrayRef, json_path: &serde_json_path::JsonPath, raw_path: &str) -> datafusion::error::Result<ArrayRef> {
    // Fast path: simple `$.a.b.c[N].d` style paths translate cleanly to a
    // parquet_variant_compute::VariantPath and we can use the vectorized
    // `variant_get` kernel, which walks the Variant binary directly without
    // ever materializing the full JsonValue. Path existence = result is
    // non-null per row.
    if let Some(variant_path) = simple_path_to_variant_path(raw_path) {
        use parquet_variant_compute::{GetOptions, variant_get};
        let opts = GetOptions::new_with_path(variant_path);
        let extracted = variant_get(array, opts).map_err(|e| DataFusionError::Execution(format!("variant_get failed: {e}")))?;
        // Path exists ↔ extracted row is non-null. is_null/is_not_null arrays
        // honor underlying null buffer cheaply (no per-row decode).
        let mut builder = BooleanArray::builder(extracted.len());
        for i in 0..extracted.len() {
            builder.append_value(!extracted.is_null(i));
        }
        return Ok(Arc::new(builder.finish()));
    }

    // Fallback: complex JSONPath (filters, recursive descent, etc.) — fall
    // back to the slow path that walks the Variant binary into a JsonValue
    // and runs serde_json_path. Avoided when the path is simple.
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
    let metadata_binary = BinaryAccessor::try_new(metadata_col, "metadata")?;
    let value_binary = BinaryAccessor::try_new(value_col, "value")?;
    let mut builder = BooleanArray::builder(struct_array.len());
    for i in 0..struct_array.len() {
        if struct_array.is_null(i) {
            builder.append_null();
            continue;
        }
        let variant = Variant::new(metadata_binary.value(i), value_binary.value(i));
        let json_value = variant_to_serde_json(&variant, 0)?;
        builder.append_value(!json_path.query(&json_value).is_empty());
    }
    Ok(Arc::new(builder.finish()))
}

/// Convert a simple JSONPath (`$.a.b[0].c`) to a `parquet_variant::VariantPath`.
/// Returns `None` for any path that uses filters, recursive descent, slices,
/// wildcards, or other features that don't map to direct field/index access —
/// those fall back to the slow JsonValue path.
fn simple_path_to_variant_path(raw: &str) -> Option<parquet_variant::VariantPath<'_>> {
    use parquet_variant::{VariantPath, VariantPathElement};
    let s = raw.strip_prefix('$').unwrap_or(raw);
    let mut elements: Vec<VariantPathElement> = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'.' => {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i] != b'.' && bytes[i] != b'[' {
                    if !(bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                        return None;
                    }
                    i += 1;
                }
                if i == start {
                    return None;
                }
                elements.push(VariantPathElement::field(std::borrow::Cow::Borrowed(&s[start..i])));
            }
            b'[' => {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i] != b']' {
                    if !bytes[i].is_ascii_digit() {
                        return None;
                    }
                    i += 1;
                }
                if i >= bytes.len() || i == start {
                    return None;
                }
                let idx: usize = s[start..i].parse().ok()?;
                elements.push(VariantPathElement::index(idx));
                i += 1; // skip ']'
            }
            _ => return None,
        }
    }
    Some(VariantPath::new(elements))
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
    fn test_parse_pg_format() {
        // Helper: assert the parse collapses to a single Chrono part with the given spec.
        let chrono_only = |fmt: &str, expected: &str| {
            assert_eq!(parse_pg_format(fmt), vec![FmtPart::Chrono(expected.to_string())], "fmt: {fmt}");
        };
        chrono_only("YYYY-MM-DD", "%Y-%m-%d");
        chrono_only("YYYY-MM-DD HH24:MI:SS", "%Y-%m-%d %H:%M:%S");
        chrono_only("Day, DD Mon YYYY", "%A, %d %b %Y");
        // Postgres-style "..." literal escapes: ISO-8601 with T separator and Z suffix.
        chrono_only(r#"YYYY-MM-DD"T"HH24:MI:SS.US"Z""#, "%Y-%m-%dT%H:%M:%S.%6fZ");
        // Tokens inside a literal stay literal.
        chrono_only(r#""YYYY=" YYYY"#, "YYYY= %Y");
        // "" inside a literal is an escaped quote.
        chrono_only(r#""a""b""#, "a\"b");
        // A bare % outside tokens is escaped to chrono's literal-%.
        chrono_only("100%", "100%%");
        // Unterminated literal: copy the remainder verbatim, don't panic.
        chrono_only(r#"YYYY "tail"#, "%Y tail");

        // D / DY split the buffer (no chrono equivalent).
        assert_eq!(parse_pg_format("D"), vec![FmtPart::PgD]);
        assert_eq!(parse_pg_format("DY"), vec![FmtPart::PgDY]);
        assert_eq!(parse_pg_format("YYYY-D"), vec![FmtPart::Chrono("%Y-".to_string()), FmtPart::PgD]);
        assert_eq!(parse_pg_format("DY YYYY"), vec![FmtPart::PgDY, FmtPart::Chrono(" %Y".to_string())]);
    }

    /// End-to-end UDF parity with Postgres/TimescaleDB `to_char`. Expected outputs
    /// captured from real Postgres 16 with `SELECT to_char(TIMESTAMP '2026-06-10 08:10:52.422355', fmt)`.
    #[tokio::test]
    async fn test_to_char_postgres_parity() {
        use datafusion::prelude::SessionContext;
        let mut ctx = SessionContext::new();
        register_custom_functions(&mut ctx).unwrap();
        let ts = "TIMESTAMP '2026-06-10 08:10:52.422355'";
        let cases: &[(&str, &str)] = &[
            ("YYYY-MM-DD", "2026-06-10"),
            ("YYYY-MM-DD HH24:MI:SS", "2026-06-10 08:10:52"),
            // Monoscope's ISO-8601 target — the bug this fix addresses.
            (r#"YYYY-MM-DD"T"HH24:MI:SS.US"Z""#, "2026-06-10T08:10:52.422355Z"),
            (r#"YYYY-MM-DD"T"HH24:MI:SS.MS"Z""#, "2026-06-10T08:10:52.422Z"),
            ("DD/MM/YYYY", "10/06/2026"),
            ("Mon DD, YYYY", "Jun 10, 2026"),
            ("Day, Mon DD YYYY", "Wednesday, Jun 10 2026"),
            ("HH12:MI", "08:10"),
            ("YY", "26"),
            // Literal containing characters that look like tokens.
            (r#""YYYY=" YYYY"#, "YYYY= 2026"),
            // Non-ASCII bytes inside a literal must survive intact (UTF-8 boundary walk).
            (r#""· "YYYY"#, "· 2026"),
            // AM/PM and Dy round-out token coverage.
            ("HH12:MI AM", "08:10 AM"),
            ("Dy", "Wed"),
            // Postgres-specific tokens with no exact chrono equivalent.
            // 2026-06-10 is a Wednesday: Postgres D=4 (Sun=1), DY="WED".
            ("D", "4"),
            ("DY", "WED"),
            // Order-of-parsing check: DY must beat bare D.
            ("DY-D", "WED-4"),
        ];
        for (fmt, expected) in cases {
            let sql = format!("SELECT to_char({ts}, '{fmt}') AS s");
            let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
            let col = batches[0].column(0).as_any().downcast_ref::<datafusion::arrow::array::StringViewArray>().unwrap();
            assert_eq!(col.value(0), *expected, "format `{fmt}`");
        }
        // Separate PM-timestamp case to actually exercise the PM output of %p.
        let pm_sql = "SELECT to_char(TIMESTAMP '2026-06-10 20:10:52', 'HH12:MI PM') AS s";
        let pm_batches = ctx.sql(pm_sql).await.unwrap().collect().await.unwrap();
        let pm_col = pm_batches[0].column(0).as_any().downcast_ref::<datafusion::arrow::array::StringViewArray>().unwrap();
        assert_eq!(pm_col.value(0), "08:10 PM");
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
