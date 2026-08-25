use std::{
    hash::Hash,
    sync::{Arc, LazyLock},
};

use anyhow::Result;
use chrono::{DateTime, Datelike, Utc};
use chrono_tz::Tz;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Float64Array, Int64Array, StringArray, StringViewArray, TimestampMicrosecondArray,
            TimestampNanosecondArray,
        },
        datatypes::{DataType, Field, FieldRef, TimeUnit},
    },
    common::{DFSchema, DataFusionError, ExprSchema, ScalarValue, not_impl_err},
    logical_expr::{
        Accumulator, AggregateUDF, ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarFunctionImplementation, ScalarUDF, ScalarUDFImpl, Signature,
        TypeSignature, Volatility, create_udaf, create_udf,
        expr::{Alias, ScalarFunction},
        planner::{ExprPlanner, PlannerResult, RawBinaryExpr, TypePlanner},
    },
    sql::sqlparser::ast::{BinaryOperator, DataType as SqlDataType},
};
use serde_json::{Value as JsonValue, json};
use tdigests::TDigest;

use crate::{observability::arrow_err, read::optimizers::extract_utf8_string, schema::is_variant_type};

/// Extracts a UTF-8 constant argument.
fn extract_scalar_string(arg: &ColumnarValue, label: &str) -> datafusion::error::Result<String> {
    let not_utf8 = || DataFusionError::Execution(format!("{label} must be a UTF8 string"));
    match arg {
        ColumnarValue::Scalar(scalar) => extract_utf8_string(scalar).ok_or_else(not_utf8),
        ColumnarValue::Array(arr) if arr.len() != 1 || arr.is_null(0) => Err(DataFusionError::Execution(format!("{label} must be a scalar value"))),
        ColumnarValue::Array(arr) => extract_utf8_string(&ScalarValue::try_from_array(arr, 0)?).ok_or_else(not_utf8),
    }
}

/// Materializes scalars as one-element arrays.
fn as_array(v: &ColumnarValue) -> datafusion::error::Result<ArrayRef> {
    match v {
        ColumnarValue::Array(a) => Ok(a.clone()),
        ColumnarValue::Scalar(s) => s.to_array(),
    }
}

macro_rules! scalar_udf_boilerplate {
    ($name:literal) => {
        fn name(&self) -> &str {
            $name
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
    };
}

/// Resolves PostgreSQL types that DataFusion does not model natively as text.
/// The planner runs for simple and extended protocol casts alike.
#[derive(Debug, Default)]
pub struct PostgresTypePlanner;

impl TypePlanner for PostgresTypePlanner {
    fn plan_type_field(&self, sql_type: &SqlDataType) -> datafusion::error::Result<Option<FieldRef>> {
        Ok(match sql_type {
            SqlDataType::Custom(name, _) if matches!(name.to_string().to_ascii_lowercase().as_str(), "jsonpath" | "regproc" | "pg_catalog.regproc") => {
                Some(Arc::new(Field::new("", DataType::Utf8, true)))
            }
            _ => None,
        })
    }
}

/// Rewrites Variant `->` and `->>` operators to `variant_get` calls.
#[derive(Debug, Default)]
pub struct VariantAwareExprPlanner;

#[derive(Debug, Clone, PartialEq)]
enum PathComponent {
    Field(String),
    Index(i64),
}

impl ExprPlanner for VariantAwareExprPlanner {
    fn plan_binary_op(&self, expr: RawBinaryExpr, schema: &DFSchema) -> datafusion::error::Result<PlannerResult<RawBinaryExpr>> {
        // PG array overlap: `a && b` → array_has_any(a, b). DataFusion's
        // NestedFunctionPlanner rewrites `@>`/`<@` but not `&&`, so it errored
        // as "Unsupported binary operator: PGOverlap". Only rewrite when both
        // sides are lists; anything else falls through unchanged.
        if matches!(expr.op, BinaryOperator::PGOverlap)
            && matches!(expr.left.get_type(schema)?, DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..))
            && matches!(expr.right.get_type(schema)?, DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..))
        {
            return Ok(PlannerResult::Planned(datafusion::functions_nested::expr_fn::array_has_any(expr.left, expr.right)));
        }

        // `#>`/`#>>` address the same leaves as `->`/`->>`, but take the whole
        // path as one text[] literal instead of a chain. Postgres callers reach
        // for them whenever the path is built programmatically, so a store that
        // supports only the arrow forms silently forces every such query into a
        // per-store branch on the client.
        let (is_long_arrow, path_is_array) = match &expr.op {
            BinaryOperator::Arrow => (false, false),
            BinaryOperator::LongArrow => (true, false),
            BinaryOperator::HashArrow => (false, true),
            BinaryOperator::HashLongArrow => (true, true),
            _ => return Ok(PlannerResult::Original(expr)),
        };

        let (base_expr, mut path_parts) = if path_is_array { (unalias(&expr.left), vec![]) } else { collect_arrow_chain(&expr.left) };
        let Some(components) = (if path_is_array { extract_path_array(&expr.right) } else { extract_path_component(&expr.right).map(|c| vec![c]) }) else {
            return Ok(PlannerResult::Original(expr));
        };
        path_parts.extend(components);

        if !is_variant_column(&base_expr, schema) {
            return Ok(PlannerResult::Original(expr)); // Let JSON planner handle
        }

        // `variant_get` cannot stringify numeric/boolean leaves. Compose through
        // JSON text to preserve PostgreSQL `->>` semantics.
        let path_literal = Expr::Literal(ScalarValue::Utf8(Some(build_variant_path(&path_parts))), None);
        let base_repr = expr_repr(&base_expr);
        let variant_leaf = Expr::ScalarFunction(ScalarFunction { func: variant_get_udf(), args: vec![base_expr, path_literal] });
        let result = if is_long_arrow {
            let to_json = Expr::ScalarFunction(ScalarFunction { func: variant_to_json_udf(), args: vec![variant_leaf] });
            Expr::ScalarFunction(ScalarFunction { func: json_to_pg_text_udf(), args: vec![to_json] })
        } else {
            variant_leaf
        };

        let op_str = match (path_is_array, is_long_arrow) {
            (false, false) => "->",
            (false, true) => "->>",
            (true, false) => "#>",
            (true, true) => "#>>",
        };
        let alias_name = format!("{base_repr} {op_str} {}", path_repr(&path_parts));
        Ok(PlannerResult::Planned(Expr::Alias(Alias::new(result, None::<&str>, alias_name))))
    }
}

fn unalias(expr: &Expr) -> Expr {
    match expr {
        Expr::Alias(alias) => unalias(&alias.expr),
        expr => expr.clone(),
    }
}

/// Path operand of `#>`/`#>>`. Postgres spells it `text[]`, which reaches the
/// planner either as the unparsed literal `{a,b,c}` or as an already-built list.
/// Quoted elements (`{"a b",c}`) are unquoted, matching Postgres array-literal
/// parsing; an empty path is rejected so `#>> '{}'` falls through unchanged
/// rather than silently addressing the whole document.
fn extract_path_array(expr: &Expr) -> Option<Vec<PathComponent>> {
    let expr = match expr {
        Expr::Cast(cast) => cast.expr.as_ref(),
        expr => expr,
    };
    let parts: Vec<PathComponent> = match expr {
        Expr::Literal(v, _) => {
            let raw = extract_utf8_string(v)?;
            let inner = raw.strip_prefix('{')?.strip_suffix('}')?;
            if inner.is_empty() {
                return None;
            }
            inner
                .split(',')
                .map(|part| {
                    let part = part.trim();
                    PathComponent::Field(part.strip_prefix('"').and_then(|p| p.strip_suffix('"')).unwrap_or(part).to_string())
                })
                .collect()
        }
        Expr::ScalarFunction(func) if func.func.name() == "make_array" => func.args.iter().filter_map(extract_path_component).collect(),
        _ => return None,
    };
    (!parts.is_empty()).then_some(parts)
}

fn collect_arrow_chain(expr: &Expr) -> (Expr, Vec<PathComponent>) {
    match expr {
        Expr::BinaryExpr(binary) if matches!(binary.op, datafusion::logical_expr::Operator::Arrow) => {
            let (base, mut parts) = collect_arrow_chain(&binary.left);
            parts.extend(extract_path_component(&binary.right)); // Option is an iterator
            (base, parts)
        }
        Expr::Alias(alias) => collect_arrow_chain(&alias.expr),
        _ => (expr.clone(), vec![]),
    }
}

fn extract_path_component(expr: &Expr) -> Option<PathComponent> {
    // `::` binds tighter than `->>`, so monoscope's `attributes->>'route'::text`
    // parses as `attributes ->> CAST('route' AS text)` — the cast lands on the
    // PATH, not the extracted value. Matching only a bare literal made this
    // return `None`, `VariantAwareExprPlanner` fall through to
    // datafusion-functions-json, and the query fail to PLAN against a Variant
    // column: "Unexpected argument type to 'json_as_text' … got Struct([metadata,
    // value])". Prod 2026-08-25: 17 of those in 90 minutes, one per chart load —
    // the panel errors outright rather than rendering slowly. A cast around a
    // path literal cannot change which field is addressed, so unwrap it.
    let expr = match expr {
        Expr::Cast(cast) => cast.expr.as_ref(),
        expr => expr,
    };
    let Expr::Literal(v, _) = expr else { return None };
    extract_utf8_string(v).map(PathComponent::Field).or_else(|| {
        Some(PathComponent::Index(match v {
            ScalarValue::Int64(Some(i)) => *i,
            ScalarValue::Int32(Some(i)) => (*i).into(),
            ScalarValue::UInt32(Some(i)) => (*i).into(),
            ScalarValue::UInt64(Some(i)) => i64::try_from(*i).ok()?,
            _ => return None,
        }))
    })
}

/// Check if expression evaluates to a Variant type
fn is_variant_column(expr: &Expr, schema: &DFSchema) -> bool {
    match expr {
        // Direct column reference. The SQL-facing schema un-types Variant columns to
        // Utf8View (see `create_insert_compatible_schema`) and tags them
        // `tf.pg_type=jsonb`, so by the time the planner runs `plan_binary_op` the real
        // Struct type is gone. Detect the marker too, else `->`/`->>` fall through to
        // datafusion-functions-json (json_get/json_as_text) and blow up once the
        // analyzer restores the Variant struct. On a base column the tag is only ever
        // set on Variant columns (UDF-output `tf.pg_type` tags live on expressions).
        Expr::Column(col) => {
            schema.field_from_column(col).is_ok_and(|f| is_variant_type(f.data_type()) || f.metadata().get("tf.pg_type").is_some_and(|v| v == "jsonb"))
        }
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
        _ => expr.get_type(schema).is_ok_and(|dt| is_variant_type(&dt)),
    }
}

/// Build variant_get path string from components:
/// `["user", "name"]` → `user.name`, `["items", Index(0)]` → `items[0]`.
fn build_variant_path(parts: &[PathComponent]) -> String {
    parts
        .iter()
        .enumerate()
        .map(|(i, part)| match part {
            PathComponent::Field(name) if i > 0 => format!(".{name}"),
            PathComponent::Field(name) => name.clone(),
            PathComponent::Index(idx) => format!("[{idx}]"),
        })
        .collect()
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
            PathComponent::Field(s) => format!("'{s}'"),
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
        Self { signature: Signature::uniform(1, vec![DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for JsonToPgTextUdf {
    scalar_udf_boilerplate!("json_to_pg_text");
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return Err(DataFusionError::Execution("json_to_pg_text requires exactly 1 argument".into()));
        };
        let arr = arg.to_array(args.number_rows)?;
        // Cast once to Utf8 — collapses Utf8/Utf8View/LargeUtf8 to a single
        // concrete shape, single pass over rows.
        let utf8 = datafusion::arrow::compute::cast(&arr, &DataType::Utf8).map_err(arrow_err)?;
        let strs = utf8.as_any().downcast_ref::<StringArray>().ok_or_else(|| DataFusionError::Execution("json_to_pg_text: cast to Utf8 failed".into()))?;
        // Builder (not a `collect()` into StringArray) so the common non-string
        // case appends the borrowed `&str` without a per-row String alloc.
        let mut b = datafusion::arrow::array::StringBuilder::with_capacity(strs.len(), strs.value_data().len());
        // Parse via serde_json so escape sequences resolve correctly and false-positive
        // shapes like '"a"+"b"' don't trigger naive unquoting. JSON null → SQL NULL;
        // JSON string → its raw text; anything else → its JSON literal text (per PG ->>).
        strs.iter().for_each(|opt| match opt.map(|s| (s, serde_json::from_str::<JsonValue>(s))) {
            None | Some((_, Ok(JsonValue::Null))) => b.append_null(),
            Some((_, Ok(JsonValue::String(inner)))) => b.append_value(&inner),
            Some((s, _)) => b.append_value(s),
        });
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
fn stamp_variant_field(f: &FieldRef) -> FieldRef {
    use crate::schema::{VARIANT_EXT_KEY as EXT_KEY, VARIANT_EXT_VALUE as EXT_VAL};
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
/// `JSONB_OUT` tags the output Field with `tf.pg_type = jsonb` so bare
/// Variant columns (wrapped by VariantPgwireRootWrap) surface PG OID 3802
/// over the wire instead of text — strict drivers (hasql) reject text.
#[derive(Debug, Hash, PartialEq, Eq, Default)]
pub struct VariantExtWrapper<U: ScalarUDFImpl + Default + Hash + PartialEq + Eq + 'static, const JSONB_OUT: bool = false> {
    inner: U,
}

impl<U: ScalarUDFImpl + Default + Hash + PartialEq + Eq + 'static, const JSONB_OUT: bool> ScalarUDFImpl for VariantExtWrapper<U, JSONB_OUT> {
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
    fn return_field_from_args(&self, args: datafusion::logical_expr::ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        let f = self.inner.return_field_from_args(args)?;
        if !JSONB_OUT {
            return Ok(f);
        }
        let mut md = f.metadata().clone();
        md.insert("tf.pg_type".into(), "jsonb".into());
        Ok(Arc::new(f.as_ref().clone().with_metadata(md)))
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::compute::cast;
        // The official datafusion-variant UDFs declare a BinaryView Variant output but
        // pass the input `metadata` buffer through unchanged. TF stores Variants as
        // Struct(Binary, Binary) (delta-kernel / delta-rs fork requirement), so a
        // Binary-input Variant makes the inner UDF's *declared* (BinaryView) and
        // *actual* (Binary metadata) output types disagree → the DataFusion
        // "result_data_type == expected_type" assertion fires. Coerce Variant args to
        // BinaryView here so the inner UDF is internally consistent; TF's on-disk /
        // MemBuffer representation stays Binary.
        // Indexed loop: rewrites `args.args[i]` / `args.arg_fields[i]` in place.
        for i in 0..args.args.len() {
            let field = args.arg_fields[i].clone();
            let DataType::Struct(inner) = field.data_type() else { continue };
            if !is_variant_type(field.data_type()) || !inner.iter().any(|f| matches!(f.data_type(), DataType::Binary)) {
                continue;
            }
            let bv_fields: datafusion::arrow::datatypes::Fields = inner
                .iter()
                .map(|f| {
                    let dt = if matches!(f.data_type(), DataType::Binary) { DataType::BinaryView } else { f.data_type().clone() };
                    Arc::new(Field::new(f.name(), dt, f.is_nullable()))
                })
                .collect();
            let bv = DataType::Struct(bv_fields);
            let arr = args.args[i].to_array(args.number_rows)?;
            let casted = cast(&arr, &bv).map_err(|e| datafusion::error::DataFusionError::Execution(format!("variant BinaryView coerce: {e}")))?;
            args.args[i] = ColumnarValue::Array(casted);
            args.arg_fields[i] = Arc::new(Field::new(field.name(), bv, field.is_nullable()).with_metadata(field.metadata().clone()));
        }
        args.arg_fields = args.arg_fields.iter().map(stamp_variant_field).collect();
        self.inner.invoke_with_args(args)
    }
}

pub type VariantToJsonExtUdf = VariantExtWrapper<datafusion_variant::VariantToJsonUdf, true>;
pub type VariantGetExtUdf = VariantExtWrapper<datafusion_variant::VariantGetUdf>;

/// Process-wide singletons for the Variant UDFs the analyzer rules splice into
/// plans. They are stateless, so the rules clone one `Arc` per plan instead of
/// allocating a fresh `ScalarUDF` per rewritten expression.
macro_rules! shared_udf {
    ($(#[$m:meta])* $vis:vis $name:ident: $ty:ty) => {
        $(#[$m])*
        $vis fn $name() -> Arc<ScalarUDF> {
            static UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| Arc::new(ScalarUDF::from(<$ty>::default())));
            Arc::clone(&UDF)
        }
    };
}

shared_udf!(pub variant_to_json_udf: VariantToJsonExtUdf);
shared_udf!(pub variant_get_udf: VariantGetExtUdf);
shared_udf!(pub json_to_variant_udf: datafusion_variant::JsonToVariantUdf);
shared_udf!(pub json_to_pg_text_udf: JsonToPgTextUdf);

/// Register all custom PostgreSQL-compatible functions
/// Collapse the repetitive `ctx.register_udf(ScalarUDF::from(T))` calls for
/// UDFs built straight from a unit/default struct.
macro_rules! reg_from {
    ($ctx:expr, $($udf:expr),+ $(,)?) => { $( $ctx.register_udf(ScalarUDF::from($udf)); )+ };
}

pub fn register_custom_functions(ctx: &mut datafusion::execution::context::SessionContext) -> Result<()> {
    // Register Variant-aware expr planner (must be before JSON planner for priority)
    datafusion::execution::FunctionRegistry::register_expr_planner(ctx, Arc::new(VariantAwareExprPlanner))?;

    // PgCoalesceUdf: PG parity coalesce that type-checks `coalesce(list_col, '{}')`,
    // replacing the built-in under the same name; see PgArrayLiteralRewriter.
    // JsonToPgTextUdf bridges variant -> Postgres ->> text semantics (numeric/bool/null → text/NULL).
    reg_from!(
        ctx,
        crate::read::optimizers::PgCoalesceUdf::default(),
        ToCharUDF::new(),
        AtTimeZoneUDF::new(),
        JsonBuildArrayUDF::new(),
        JsonbBuildArrayUDF::new(),
        ToJsonbUDF::new(),
        ToJsonUDF::new(),
        ExtractEpochUDF::new(),
        JsonToPgTextUdf::default(),
        datafusion_variant::JsonToVariantUdf::default(),
        VariantToJsonExtUdf::default(),
        VariantGetExtUdf::default(),
        datafusion_variant::CastToVariantUdf::default(),
        datafusion_variant::IsVariantNullUdf::default(),
        datafusion_variant::VariantPretty::default(),
        datafusion_variant::VariantListConstruct::default(),
        datafusion_variant::VariantListInsert::default(),
        datafusion_variant::VariantObjectConstruct::default(),
        datafusion_variant::VariantObjectInsert::default(),
        JsonbPathExistsUDF::new(),
        ApproxPercentileUDF::new(),
    );

    // create_udf-based UDFs that carry construction logic.
    ctx.register_udf(create_jsonb_array_elements_udf());
    ctx.register_udf(create_time_bucket_udf());
    ctx.register_udaf(create_percentile_agg_udaf());
    ctx.register_udaf(create_tdigest_merge_udaf());
    ctx.register_udaf(AggregateUDF::from(HllAggUDF::default()));
    ctx.register_udaf(create_hll_merge_udaf());
    ctx.register_udf(create_hll_count_udf());
    ctx.register_udf(hash_bucket_udf());

    // text_match(col, 'query') for tantivy-accelerated full-text search. Naive
    // substring fallback keeps correctness when tantivy is disabled or when
    // post-filtering MemBuffer rows; see [[tantivy_index/udf]].
    ctx.register_udf(crate::tantivy::udf::text_match_udf());

    // Test-only clock UDFs. Gated behind TIMEFUSION_ENABLE_TEST_UDFS so a
    // production deployment can't have its eviction/flush clock yanked by
    // a stray SQL session. Required by the long-duration bench harness in
    // `bench/timeseries_lifecycle.py` to simulate hours in seconds.
    if std::env::var("TIMEFUSION_ENABLE_TEST_UDFS").is_ok_and(|v| v == "true" || v == "1") {
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
    let fun: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        let arr = as_array(&args[0])?;
        let s = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| DataFusionError::Execution("timefusion_set_clock expects Utf8".into()))?;
        let out: Int64Array = s
            .iter()
            .map(|v| {
                v.map(|s| {
                    chrono::DateTime::parse_from_rfc3339(s)
                        .map(|t| crate::support::set_micros(t.timestamp_micros()))
                        .map_err(|e| DataFusionError::Execution(format!("invalid rfc3339: {e}")))
                })
                .transpose()
            })
            .collect::<datafusion::error::Result<_>>()?;
        Ok(ColumnarValue::Array(Arc::new(out)))
    });
    create_udf("timefusion_set_clock", vec![DataType::Utf8], DataType::Int64, Volatility::Volatile, fun)
}

/// `timefusion_advance_clock(delta_micros)` → new bigint micros.
fn create_advance_clock_udf() -> ScalarUDF {
    let fun: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        let arr = as_array(&args[0])?;
        let d = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| DataFusionError::Execution("timefusion_advance_clock expects Int64".into()))?;
        let out: Int64Array = d.iter().map(|v| v.map(crate::support::advance_micros)).collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    });
    create_udf("timefusion_advance_clock", vec![DataType::Int64], DataType::Int64, Volatility::Volatile, fun)
}

/// `timefusion_now_micros()` → current clock value (frozen or wall).
fn create_now_micros_udf() -> ScalarUDF {
    let fun: ScalarFunctionImplementation =
        Arc::new(move |_args: &[ColumnarValue]| Ok(ColumnarValue::Array(Arc::new(Int64Array::from(vec![crate::support::now_micros()])))));
    create_udf("timefusion_now_micros", vec![], DataType::Int64, Volatility::Volatile, fun)
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct ToCharUDF {
    signature: Signature,
}

impl ToCharUDF {
    fn new() -> Self {
        Self { signature: Signature::any(2, Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for ToCharUDF {
    scalar_udf_boilerplate!("to_char");

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let [ts, fmt] = args.args.as_slice() else {
            return Err(DataFusionError::Execution("to_char requires exactly 2 arguments: timestamp and format string".to_string()));
        };
        let format_str = extract_scalar_string(fmt, "Format string")?;
        Ok(ColumnarValue::Array(format_timestamps(&as_array(ts)?, &format_str)?))
    }
}

/// Raw timestamp ticks and the array's ticks-per-second — see `timestamp_ticks`.
type TimestampTicks<'a> = (Box<dyn Iterator<Item = Option<i64>> + 'a>, i64);

/// Downcast a µs/ns timestamp array to its raw ticks plus the array's
/// ticks-per-second, so callers stay unit-agnostic. `label` names the argument
/// in the error message.
fn timestamp_ticks<'a>(array: &'a ArrayRef, label: &str) -> datafusion::error::Result<TimestampTicks<'a>> {
    if let Some(ts) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        Ok((Box::new(ts.iter()), 1_000_000))
    } else if let Some(ts) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        Ok((Box::new(ts.iter()), 1_000_000_000))
    } else {
        Err(DataFusionError::Execution(format!("{label} must be a timestamp")))
    }
}

/// Map each tick of a timestamp array through `f(tick, ticks_per_second)` and
/// rebuild a timestamp array of the same unit, carrying `tz` (None = naive).
fn map_timestamps(
    array: &ArrayRef, tz: Option<&str>, label: &str, f: impl Fn(i64, i64) -> datafusion::error::Result<i64>,
) -> datafusion::error::Result<ArrayRef> {
    let (ticks, per_sec) = timestamp_ticks(array, label)?;
    let raw: Int64Array = ticks.map(|v| v.map(|v| f(v, per_sec)).transpose()).collect::<datafusion::error::Result<_>>()?;
    let unit = if per_sec == 1_000_000 { TimeUnit::Microsecond } else { TimeUnit::Nanosecond };
    datafusion::arrow::compute::cast(&raw, &DataType::Timestamp(unit, tz.map(Arc::from))).map_err(arrow_err)
}

/// Format timestamps according to PostgreSQL format patterns
fn format_timestamps(timestamp_array: &ArrayRef, format_str: &str) -> datafusion::error::Result<ArrayRef> {
    let parts = parse_pg_format(format_str);
    let (ticks, per_sec) = timestamp_ticks(timestamp_array, "First argument")?;
    let per_micro = per_sec / 1_000_000;
    let out: StringViewArray = ticks
        .map(|v| {
            v.map(|t| {
                DateTime::<Utc>::from_timestamp_micros(t / per_micro)
                    .ok_or_else(|| DataFusionError::Execution("Invalid timestamp".to_string()))
                    .map(|dt| render_pg_format(&parts, &dt))
            })
            .transpose()
        })
        .collect::<datafusion::error::Result<_>>()?;
    Ok(Arc::new(out))
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
    parts
        .iter()
        .map(|part| match part {
            FmtPart::Chrono(spec) => dt.format(spec).to_string(),
            // chrono `num_days_from_sunday` is 0=Sun..6=Sat; Postgres `D` is 1..7.
            FmtPart::PgD => (dt.weekday().num_days_from_sunday() + 1).to_string(),
            // Abbreviated English weekday is ASCII-only, so to_ascii_uppercase suffices
            // and avoids the locale-aware Unicode case-folding overhead of to_uppercase.
            FmtPart::PgDY => {
                let mut s = dt.format("%a").to_string();
                s.make_ascii_uppercase();
                s
            }
        })
        .collect()
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
    // ORDER IS LOAD-BEARING: every entry must come before any entry that is one of its
    // prefixes. E.g. YYYY before YY, HH24/HH12 before HH, Month before Mon before MM.
    // The loop below uses linear `find` so a misordering would silently match the
    // shorter token first. Note: `D` and `DY` are handled below as PgD / PgDY (no
    // chrono equivalent), not here.
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
        // Our timestamps are stored UTC, so `TZ` always renders as "UTC".
        ("TZ", "%Z"),
        ("AM", "%p"),
        ("PM", "%p"),
        // Lowercase forms — Postgres `am`/`pm` emit lowercase output (chrono `%P`).
        ("am", "%P"),
        ("pm", "%P"),
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
        // No trailing-alpha guard here: Postgres consumes `DY` greedily, so `DYY` is
        // `DY` + leftover `Y`. The bare-`D` guard below is still needed because
        // `Day`/`Dy`/`DD` are alpha-prefix conflicts; no such conflict exists for `DY`.
        if bytes[i..].starts_with(b"DY") {
            flush(&mut parts, &mut buf);
            parts.push(FmtPart::PgDY);
            i += 2;
            continue;
        }
        // Alphanumeric guard (vs just alpha) so a future `D1`-style token can't be
        // greedily consumed as bare `D` + leftover `1` before getting added to TOKENS.
        if bytes[i] == b'D' && !bytes.get(i + 1).is_some_and(|b| b.is_ascii_alphanumeric()) {
            // Bare `D` only — guarded so `D<letter>` (e.g. a future token starting with D)
            // doesn't get consumed here. `Day`, `Dy`, `DD` are caught by TOKENS; `DY` is
            // caught by its own check above.
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

#[derive(Debug, Hash, Eq, PartialEq)]
struct AtTimeZoneUDF {
    signature: Signature,
}

impl AtTimeZoneUDF {
    fn new() -> Self {
        Self { signature: Signature::any(2, Volatility::Immutable) }
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
        let [ts, tz] = args.args.as_slice() else {
            return Err(DataFusionError::Execution("AT TIME ZONE requires exactly 2 arguments: timestamp and timezone".to_string()));
        };
        let tz_str = extract_scalar_string(tz, "Timezone")?;
        Ok(ColumnarValue::Array(convert_timezone(&as_array(ts)?, &tz_str)?))
    }
}

/// Convert timestamps to a different timezone: shift each value by the target
/// zone's UTC offset so that rendering it as UTC displays the local time.
fn convert_timezone(timestamp_array: &ArrayRef, tz_str: &str) -> datafusion::error::Result<ArrayRef> {
    use chrono::Offset;
    let tz: Tz = tz_str.parse().map_err(|_| DataFusionError::Execution(format!("Invalid timezone: {tz_str}")))?;
    // `per_sec` is the array's ticks-per-second, so the same shift works for µs and ns.
    map_timestamps(timestamp_array, None, "First argument", |v, per_sec| {
        let dt =
            DateTime::<Utc>::from_timestamp_micros(v / (per_sec / 1_000_000)).ok_or_else(|| DataFusionError::Execution("Invalid timestamp".to_string()))?;
        Ok(v + dt.with_timezone(&tz).offset().fix().local_minus_utc() as i64 * per_sec)
    })
}

/// `jsonb_array_elements` placeholder: unnesting a JSON array into rows needs
/// DataFusion table-function support, so the UDF exists only to give callers a
/// clear "not implemented" instead of "unknown function".
fn create_jsonb_array_elements_udf() -> ScalarUDF {
    let stub: ScalarFunctionImplementation =
        Arc::new(move |_: &[ColumnarValue]| not_impl_err!("jsonb_array_elements is not yet fully implemented - requires table function support"));
    create_udf("jsonb_array_elements", vec![DataType::Utf8View], DataType::Utf8View, Volatility::Immutable, stub)
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct JsonBuildArrayUDF {
    signature: Signature,
}

impl JsonBuildArrayUDF {
    fn new() -> Self {
        Self { signature: Signature::variadic_any(Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for JsonBuildArrayUDF {
    scalar_udf_boilerplate!("json_build_array");

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        let num_rows = args
            .iter()
            .find_map(|a| match a {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);

        // Convert each argument column ONCE up front. Converting inside the
        // row loop is O(rows² × args) — observed as ~0.6ms/row × millions of
        // rows, enough to OOM prod on a wide-window span-list query.
        let cols = args.iter().map(|arg| array_to_json_values(&as_array(arg)?)).collect::<datafusion::error::Result<Vec<_>>>()?;

        let out = StringViewArray::from_iter_values((0..num_rows).map(|row_idx| {
            // len-1 columns are broadcast scalars
            let row: Vec<JsonValue> = cols.iter().map(|c| c[if c.len() == 1 { 0 } else { row_idx }].clone()).collect();
            JsonValue::Array(row).to_string()
        }));
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct ToJsonUDF {
    signature: Signature,
    aliases: Vec<String>,
}

impl ToJsonUDF {
    fn new() -> Self {
        // PG's `row_to_json(record)` is `to_json` over a row. pgAdmin's
        // dashboard polls `row_to_json(t)` over a subquery alias every 5s.
        Self { signature: Signature::any(1, Volatility::Immutable), aliases: vec!["row_to_json".to_string()] }
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
        let [arg] = args.args.as_slice() else {
            return Err(DataFusionError::Execution("to_json requires exactly 1 argument".to_string()));
        };
        let out = StringViewArray::from_iter_values(array_to_json_values(&as_array(arg)?)?.iter().map(JsonValue::to_string));
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

// JSONB-tagged wrappers around the JSON UDFs. Output stays Utf8View, but the
// returned Field carries `tf.pg_type = jsonb` so the patched vendor/arrow-pg
// surfaces PG OID 3802 and prepends the 0x01 binary jsonb version byte.
fn jsonb_tagged_field() -> FieldRef {
    let meta = [("tf.pg_type".to_string(), "jsonb".to_string())].into_iter().collect();
    Arc::new(Field::new("", DataType::Utf8View, true).with_metadata(meta))
}

macro_rules! jsonb_wrapper {
    ($wrap:ident, $inner:ident, $pg_name:expr) => {
        #[derive(Debug, Hash, Eq, PartialEq)]
        struct $wrap {
            inner: $inner,
        }
        impl $wrap {
            fn new() -> Self {
                Self { inner: $inner::new() }
            }
        }
        impl ScalarUDFImpl for $wrap {
            fn name(&self) -> &str {
                $pg_name
            }
            fn signature(&self) -> &Signature {
                self.inner.signature()
            }
            fn return_type(&self, a: &[DataType]) -> datafusion::error::Result<DataType> {
                self.inner.return_type(a)
            }
            fn return_field_from_args(&self, _: datafusion::logical_expr::ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
                Ok(jsonb_tagged_field())
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
                self.inner.invoke_with_args(args)
            }
        }
    };
}
jsonb_wrapper!(JsonbBuildArrayUDF, JsonBuildArrayUDF, "jsonb_build_array");
jsonb_wrapper!(ToJsonbUDF, ToJsonUDF, "to_jsonb");

#[derive(Debug, Hash, Eq, PartialEq)]
struct ExtractEpochUDF {
    signature: Signature,
}

impl ExtractEpochUDF {
    fn new() -> Self {
        Self { signature: Signature::any(1, Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for ExtractEpochUDF {
    scalar_udf_boilerplate!("extract_epoch");

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return Err(DataFusionError::Execution("extract_epoch requires exactly 1 argument".to_string()));
        };
        let array = as_array(arg)?;
        // Divide in the array's own unit so nanosecond inputs keep sub-µs precision.
        let (ticks, per_sec) = timestamp_ticks(&array, "extract_epoch argument")?;
        let secs: Float64Array = ticks.map(|v| v.map(|t| t as f64 / per_sec as f64)).collect();
        Ok(ColumnarValue::Array(Arc::new(secs)))
    }
}

/// Downcast `array` to a primitive Arrow array and map each element to
/// `json!(value)`, nulls to `JsonValue::Null`.
macro_rules! json_primitives {
    ($array:expr, $ty:ty) => {{
        let arr = $array.as_any().downcast_ref::<$ty>().ok_or_else(|| DataFusionError::Execution(format!("Failed to downcast to {}", stringify!($ty))))?;
        arr.iter().map(|v| v.map_or(JsonValue::Null, |x| json!(x))).collect()
    }};
}

/// Convert Arrow array to JSON values
fn array_to_json_values(array: &ArrayRef) -> datafusion::error::Result<Vec<JsonValue>> {
    array_to_json_values_inner(array, true)
}

/// `sniff_json` parses Utf8 values that look like JSON into real JSON. PG parity
/// requires it only at the top level — Variant/Utf8 columns holding JSON
/// (attributes, events, links) need it — while list elements must stay JSON
/// strings (`to_jsonb(text[])`), so list recursion always passes `false`.
fn array_to_json_values_inner(array: &ArrayRef, sniff_json: bool) -> datafusion::error::Result<Vec<JsonValue>> {
    Ok(match array.data_type() {
        DataType::Utf8View => {
            let strs = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| DataFusionError::Execution("Failed to downcast to StringViewArray".to_string()))?;
            // Sniff JSON only at the top level: Variant/Utf8 columns holding JSON
            // (attributes, events) must surface as real JSON. Inside List(Utf8)
            // (e.g. summary text[]) PG keeps elements as JSON *strings*.
            let looks_json = |s: &str| (s.starts_with('{') && s.ends_with('}')) || (s.starts_with('[') && s.ends_with(']'));
            strs.iter()
                .map(|v| match v {
                    None => JsonValue::Null,
                    Some(s) if sniff_json && looks_json(s) => serde_json::from_str(s).unwrap_or_else(|_| JsonValue::String(s.to_string())),
                    Some(s) => JsonValue::String(s.to_string()),
                })
                .collect()
        }
        DataType::Int64 => json_primitives!(array, Int64Array),
        DataType::Float64 => json_primitives!(array, Float64Array),
        DataType::Boolean => json_primitives!(array, BooleanArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let ts = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| DataFusionError::Execution("Failed to downcast to TimestampMicrosecondArray".to_string()))?;
            ts.iter()
                .map(|v| match v {
                    None => Ok(JsonValue::Null),
                    Some(us) => DateTime::<Utc>::from_timestamp_micros(us)
                        .map(|dt| JsonValue::String(dt.to_rfc3339()))
                        .ok_or_else(|| DataFusionError::Execution("Invalid timestamp".to_string())),
                })
                .collect::<datafusion::error::Result<_>>()?
        }
        // A record renders as a JSON object keyed by field name — PG's
        // `row_to_json(t)`. Without this the generic fallback below tries to cast
        // Struct to Utf8View and fails outright.
        DataType::Struct(fields) => {
            let columns = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StructArray>()
                .ok_or_else(|| DataFusionError::Execution("Failed to downcast to StructArray".to_string()))?;
            // Field values are converted column-wise, then transposed per row.
            let per_field = fields
                .iter()
                .zip(columns.columns())
                .map(|(field, column)| array_to_json_values_inner(column, false).map(|values| (field.name().clone(), values)))
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            (0..array.len())
                .map(|row| {
                    if columns.is_null(row) {
                        return JsonValue::Null;
                    }
                    per_field.iter().map(|(name, values)| (name.clone(), values[row].clone())).collect::<serde_json::Map<_, _>>().into()
                })
                .collect()
        }
        DataType::List(_) => list_to_json_values::<i32>(array)?,
        DataType::LargeList(_) => list_to_json_values::<i64>(array)?,
        DataType::FixedSizeList(field, _) => list_to_json_values::<i32>(&datafusion::arrow::compute::cast(array, &DataType::List(field.clone()))?)?,
        // Anything else: render through its string form.
        _ => return array_to_json_values_inner(&datafusion::arrow::compute::cast(array, &DataType::Utf8View)?, sniff_json),
    })
}

fn list_to_json_values<O: datafusion::arrow::array::OffsetSizeTrait>(array: &ArrayRef) -> datafusion::error::Result<Vec<JsonValue>> {
    let list_array = array
        .as_any()
        .downcast_ref::<datafusion::arrow::array::GenericListArray<O>>()
        .ok_or_else(|| DataFusionError::Execution("Failed to downcast to list array".to_string()))?;
    // Always sniff_json=false: PG's to_jsonb(text[]) keeps elements as JSON strings.
    (0..list_array.len())
        .map(|i| if list_array.is_null(i) { Ok(JsonValue::Null) } else { array_to_json_values_inner(&list_array.value(i), false).map(JsonValue::Array) })
        .collect()
}

/// Create the time_bucket UDF for time-series bucketing (similar to TimescaleDB)
fn create_time_bucket_udf() -> ScalarUDF {
    let time_bucket_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> {
        let [interval, ts] = args else {
            return Err(DataFusionError::Execution("time_bucket requires exactly 2 arguments: interval and timestamp".to_string()));
        };
        let bucket_size_micros = parse_interval_to_micros(&extract_scalar_string(interval, "Interval")?)?;
        Ok(ColumnarValue::Array(bucket_timestamps(&as_array(ts)?, bucket_size_micros)?))
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
    let bad_format = || DataFusionError::Execution("Invalid interval format. Expected format: 'N unit' (e.g., '5 minutes' or '5m')".to_string());
    let parts: Vec<&str> = interval_str.split_whitespace().collect();
    let (num_str, unit_str) = match *parts.as_slice() {
        [value, unit] => (value, unit),
        // `find` yields a byte index, so `split_at` stays on a char boundary.
        [combined] => combined.split_at(combined.find(char::is_alphabetic).ok_or_else(bad_format)?),
        _ => return Err(bad_format()),
    };

    let value = num_str.parse::<i64>().map_err(|_| DataFusionError::Execution("Invalid interval value".to_string()))?;
    let micros_per_unit = match unit_str.to_lowercase().as_str() {
        "second" | "seconds" | "sec" | "secs" | "s" => 1_000_000,
        "minute" | "minutes" | "min" | "mins" | "m" => 60_000_000,
        "hour" | "hours" | "hr" | "hrs" | "h" => 3_600_000_000,
        "day" | "days" | "d" => 86_400_000_000,
        "week" | "weeks" | "w" => 604_800_000_000,
        unit => {
            return Err(DataFusionError::Execution(format!("Unsupported time unit: {unit}. Supported units: second(s), minute(s), hour(s), day(s), week(s)")));
        }
    };
    value.checked_mul(micros_per_unit).ok_or_else(|| DataFusionError::Execution(format!("Interval '{interval_str}' overflows")))
}

/// Bucket timestamps to the nearest bucket boundary
fn bucket_timestamps(timestamp_array: &ArrayRef, bucket_size_micros: i64) -> datafusion::error::Result<ArrayRef> {
    // floor(timestamp / bucket_size) * bucket_size, in the array's own unit.
    map_timestamps(timestamp_array, Some("UTC"), "Argument", |v, per_sec| {
        let size = bucket_size_micros * (per_sec / 1_000_000);
        Ok((v / size) * size)
    })
}

/// Create the percentile_agg UDAF for building t-digest summaries
fn create_percentile_agg_udaf() -> AggregateUDF {
    create_udaf(
        "percentile_agg",
        vec![DataType::Float64],
        Arc::new(DataType::Binary),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::<PercentileAccumulator>::default())),
        Arc::new(vec![DataType::Binary]),
    )
}

fn create_tdigest_merge_udaf() -> AggregateUDF {
    create_udaf(
        "tdigest_merge",
        vec![DataType::Binary],
        Arc::new(DataType::Binary),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::<TDigestMergeAccumulator>::default())),
        Arc::new(vec![DataType::Binary]),
    )
}

const TDIGEST_MAX_CENTROIDS: usize = 200;

/// Wrapper for the bounded, mergeable t-digest state exchanged between partial
/// and final aggregates. Its binary representation contains centroids, never
/// the raw input values.
#[derive(Debug, Default)]
struct TDigestWrapper {
    digest: Option<TDigest>,
}

impl TDigestWrapper {
    fn insert_batch(&mut self, values: impl IntoIterator<Item = f64>) {
        let values: Vec<f64> = values.into_iter().filter(|v| v.is_finite()).collect();
        if values.is_empty() {
            return;
        }
        let mut digest = TDigest::from_values(values);
        digest.compress(TDIGEST_MAX_CENTROIDS);
        self.merge_digest(&digest);
    }

    fn merge(&mut self, other: &TDigestWrapper) {
        if let Some(digest) = &other.digest {
            self.merge_digest(digest);
        }
    }

    fn merge_digest(&mut self, digest: &TDigest) {
        // Only clones when this wrapper is still empty.
        let mut merged = self.digest.as_ref().map_or_else(|| digest.clone(), |current| current.merge(digest));
        merged.compress(TDIGEST_MAX_CENTROIDS);
        self.digest = Some(merged);
    }

    fn to_digest(&self) -> Option<TDigest> {
        self.digest.clone()
    }

    fn to_bytes(&self) -> datafusion::error::Result<Vec<u8>> {
        let centroids: Vec<(f64, f64)> = self.digest.iter().flat_map(|d| d.centroids().iter().map(|c| (c.mean, c.weight))).collect();
        // Never swallow the encode failure: an empty payload would silently become an
        // empty digest downstream and skew every percentile.
        bincode::encode_to_vec(centroids, bincode::config::standard()).map_err(|e| DataFusionError::Execution(format!("Failed to serialize t-digest: {e}")))
    }

    fn from_bytes(bytes: &[u8]) -> datafusion::error::Result<Self> {
        let centroids: Vec<(f64, f64)> = bincode::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|e| DataFusionError::Execution(format!("Failed to deserialize t-digest: {e}")))?
            .0;
        let centroids: Vec<tdigests::Centroid> = centroids
            .into_iter()
            .filter(|(mean, weight)| mean.is_finite() && *weight > 0.0)
            .map(|(mean, weight)| tdigests::Centroid::new(mean, weight))
            .collect();
        if centroids.is_empty() {
            return Ok(Self::default());
        }
        let mut digest = TDigest::from_centroids(centroids);
        digest.compress(TDIGEST_MAX_CENTROIDS);
        Ok(Self { digest: Some(digest) })
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.digest.as_ref().map_or(0, |digest| std::mem::size_of_val(digest.centroids()))
    }
}

fn merge_tdigest_batch(digest: &mut TDigestWrapper, arrays: &[ArrayRef]) -> datafusion::error::Result<()> {
    let Some(array) = arrays.first() else { return Ok(()) };
    let binary = array.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| DataFusionError::Execution("tdigest_merge expects Binary values".to_string()))?;
    binary.iter().flatten().try_for_each(|bytes| {
        digest.merge(&TDigestWrapper::from_bytes(bytes)?);
        Ok(())
    })
}

/// Accumulator for percentile_agg that builds a t-digest
#[derive(Debug, Default)]
struct PercentileAccumulator {
    digest: TDigestWrapper,
}

impl Accumulator for PercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::error::Result<()> {
        let Some(array) = values.first() else { return Ok(()) };
        let floats =
            array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| DataFusionError::Execution("percentile_agg expects Float64 values".to_string()))?;
        self.digest.insert_batch(floats.iter().flatten());
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.digest.to_bytes()?)))
    }

    fn size(&self) -> usize {
        self.digest.size()
    }

    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        self.evaluate().map(|v| vec![v])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        merge_tdigest_batch(&mut self.digest, states)
    }
}

#[derive(Debug, Default)]
struct TDigestMergeAccumulator {
    digest: TDigestWrapper,
}

impl Accumulator for TDigestMergeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::error::Result<()> {
        merge_tdigest_batch(&mut self.digest, values)
    }

    fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.digest.to_bytes()?)))
    }

    fn size(&self) -> usize {
        self.digest.size()
    }

    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        self.evaluate().map(|value| vec![value])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        merge_tdigest_batch(&mut self.digest, states)
    }
}

/// UDF implementation for approx_percentile: extracts a percentile from a t-digest.
#[derive(Debug, Hash, Eq, PartialEq)]
struct ApproxPercentileUDF {
    signature: Signature,
}

impl ApproxPercentileUDF {
    fn new() -> Self {
        Self { signature: Signature::new(TypeSignature::Exact(vec![DataType::Float64, DataType::Binary]), Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for ApproxPercentileUDF {
    scalar_udf_boilerplate!("approx_percentile");

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let [pct_arg, digest_arg] = args.args.as_slice() else {
            return Err(DataFusionError::Execution("approx_percentile requires exactly 2 arguments: percentile and t-digest".to_string()));
        };
        // Result size follows the digest column (which comes from GROUP BY).
        let num_rows = match digest_arg {
            ColumnarValue::Array(array) => array.len(),
            ColumnarValue::Scalar(_) => 1,
        };
        let percentile_array = pct_arg.to_array(num_rows)?;
        let digest_array = digest_arg.to_array(num_rows)?;

        let percentiles = percentile_array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("First argument must be a percentile (Float64)".to_string()))?;
        let digests = digest_array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| DataFusionError::Execution("Second argument must be a t-digest (Binary)".to_string()))?;

        // None → SQL NULL (null input, or a digest that saw no values).
        let out: Float64Array = percentiles
            .iter()
            .zip(digests.iter())
            .map(|(pct, bytes)| match (pct, bytes) {
                (Some(pct), Some(bytes)) => {
                    if !(0.0..=1.0).contains(&pct) {
                        return Err(DataFusionError::Execution(format!("Percentile must be between 0 and 1, got {pct}")));
                    }
                    Ok(TDigestWrapper::from_bytes(bytes)?.to_digest().map(|d| d.estimate_quantile(pct)))
                }
                _ => Ok(None),
            })
            .collect::<datafusion::error::Result<_>>()?;
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

// ============================================================================
// HyperLogLog: `hll_agg` / `hll_merge` / `hll_count`
// ============================================================================
//
// The distinct-count analogue of `percentile_agg` / `tdigest_merge` /
// `approx_percentile` above, and it exists for the same reason: DataFusion's
// own `approx_distinct` computes a fine estimate but gives no way to STORE the
// sketch and fold it later, so a rollup cannot carry a distinct count. (Its
// `HyperLogLog` type is `pub(crate)`, so it cannot be reused here either.)
//
// These three are the rollup's storage layer, not a second user-facing API:
// queries keep asking for `approx_distinct(x)`, and the rollup matcher answers
// it with `hll_count(hll_merge(state))` when a measure covers it.

/// Feed one array's non-null values into a sketch.
///
/// Strings and binaries are hashed in place; anything else is cast to Utf8View
/// first, which preserves distinctness for every primitive type at the cost of
/// a formatting pass that only non-string columns pay.
fn hll_insert_array(sketch: &mut crate::read::Hll, array: &ArrayRef) -> datafusion::error::Result<()> {
    macro_rules! feed {
        ($ty:ty) => {{
            let typed = array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| DataFusionError::Execution("hll_agg: array does not match its own data type".to_string()))?;
            typed.iter().flatten().for_each(|value| sketch.insert_hash(crate::read::hash_bytes(AsRef::<[u8]>::as_ref(&value))));
            return Ok(());
        }};
    }
    match array.data_type() {
        DataType::Utf8View => feed!(StringViewArray),
        DataType::Utf8 => feed!(StringArray),
        DataType::LargeUtf8 => feed!(datafusion::arrow::array::LargeStringArray),
        DataType::BinaryView => feed!(BinaryViewArray),
        DataType::Binary => feed!(BinaryArray),
        DataType::LargeBinary => feed!(datafusion::arrow::array::LargeBinaryArray),
        _ => hll_insert_array(sketch, &datafusion::arrow::compute::cast(array, &DataType::Utf8View)?),
    }
}

/// Decode and fold a column of serialized sketches.
fn hll_merge_array(sketch: &mut crate::read::Hll, arrays: &[ArrayRef]) -> datafusion::error::Result<()> {
    let Some(array) = arrays.first() else { return Ok(()) };
    let binary = array.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| DataFusionError::Execution("hll_merge expects Binary values".to_string()))?;
    binary.iter().flatten().try_for_each(|bytes| {
        sketch.merge(&crate::read::Hll::from_bytes(bytes).map_err(DataFusionError::Execution)?);
        Ok(())
    })
}

/// Shared by both UDAFs: they differ only in what `update_batch` feeds in.
#[derive(Debug, Default)]
struct HllAccumulator {
    sketch: crate::read::Hll,
    /// `hll_agg` hashes raw values; `hll_merge` folds stored sketches.
    merging: bool,
}

impl Accumulator for HllAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::error::Result<()> {
        match values.first() {
            Some(array) if !self.merging => hll_insert_array(&mut self.sketch, array),
            Some(_) => hll_merge_array(&mut self.sketch, values),
            None => Ok(()),
        }
    }

    fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.sketch.to_bytes())))
    }

    fn size(&self) -> usize {
        self.sketch.size()
    }

    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        self.evaluate().map(|state| vec![state])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        hll_merge_array(&mut self.sketch, states)
    }
}

/// `hll_agg(any) -> Binary`, also spelled `approx_count_distinct`.
///
/// Both names are Timescale Toolkit's: there, `approx_count_distinct(x)` builds
/// a `hyperloglog` and `distinct_count(sketch)` reads the number out of it. TF
/// mirrors that split exactly, so monoscope emits ONE
/// `distinct_count(approx_count_distinct(x))` for both backends — the same trick
/// `percentile_agg`/`approx_percentile` already play for percentiles. (Getting
/// this wrong is easy: Toolkit's `approx_count_distinct` returns a SKETCH, not a
/// count, and `approx_count_distinct(x)::float` is a type error there.)
///
/// The split is also what makes the rollup work: the aggregate's output IS the
/// storable state, so a measure holds it and the scalar reads it back at query
/// time.
///
/// A hand-written `AggregateUDFImpl` rather than `create_udaf` because the
/// argument is deliberately untyped: a distinct count is meaningful over every
/// column type, and an `Exact` signature would make `hll_agg(duration)` a
/// planning error instead of a cast.
#[derive(Debug, Hash, Eq, PartialEq)]
struct HllAggUDF {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for HllAggUDF {
    fn default() -> Self {
        Self { signature: Signature::any(1, Volatility::Immutable), aliases: vec!["approx_count_distinct".to_string()] }
    }
}

impl datafusion::logical_expr::AggregateUDFImpl for HllAggUDF {
    fn name(&self) -> &str {
        "hll_agg"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn state_fields(&self, _args: datafusion::logical_expr::function::StateFieldsArgs) -> datafusion::error::Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new("sketch", DataType::Binary, true))])
    }

    fn accumulator(&self, _acc_args: datafusion::logical_expr::function::AccumulatorArgs) -> datafusion::error::Result<Box<dyn Accumulator>> {
        Ok(Box::new(HllAccumulator::default()))
    }
}

fn create_hll_merge_udaf() -> AggregateUDF {
    create_udaf(
        "hll_merge",
        vec![DataType::Binary],
        Arc::new(DataType::Binary),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(HllAccumulator { merging: true, ..Default::default() }) as Box<dyn Accumulator>)),
        Arc::new(vec![DataType::Binary]),
    )
}

/// `hll_count(Binary) -> Int64`, also spelled `distinct_count`. Int64 because
/// that is PG `bigint`, which is what Timescale Toolkit's `distinct_count`
/// returns.
fn create_hll_count_udf() -> ScalarUDF {
    create_udf(
        "hll_count",
        vec![DataType::Binary],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let array = as_array(args.first().ok_or_else(|| DataFusionError::Execution("hll_count requires one argument".to_string()))?)?;
            let binary =
                array.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| DataFusionError::Execution("hll_count expects a Binary sketch".to_string()))?;
            // NULL in, NULL out: a group that never saw the measure has no sketch,
            // which is not the same claim as "zero distinct values".
            let counts: Int64Array = binary
                .iter()
                .map(|bytes| {
                    bytes.map(|bytes| crate::read::Hll::from_bytes(bytes).map(|s| s.estimate() as i64).map_err(DataFusionError::Execution)).transpose()
                })
                .collect::<datafusion::error::Result<_>>()?;
            Ok(ColumnarValue::Array(Arc::new(counts)))
        }) as ScalarFunctionImplementation,
    )
    .with_aliases(["distinct_count"])
}

/// `hash_bucket(text, n)` — a stable, evenly-spread bucket in `[0, n)`.
///
/// The sharded dedup rewrite used `substr(md5(…), 1, 2)` for this. A live CPU
/// profile on 2026-08-18 put `md5::compress` at **5.71% of all CPU**, larger
/// than the ZSTD decompression it exists to serve, because each of K passes
/// hashes every row to keep 1/K of them. Bucketing needs an even, stable spread
/// — not a cryptographic digest — so this uses the same non-cryptographic mixer
/// the HLL sketches already hash with.
///
/// NULL hashes as the empty string rather than to NULL: a NULL bucket satisfies
/// neither `>= lo` nor `< hi`, so such a row would silently fall out of every
/// shard.
pub fn hash_bucket_udf() -> ScalarUDF {
    create_udf(
        "hash_bucket",
        vec![DataType::Utf8View, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let [value, buckets] = args else {
                return Err(DataFusionError::Execution("hash_bucket requires exactly 2 arguments: value and bucket count".to_string()));
            };
            let buckets = match buckets {
                ColumnarValue::Scalar(scalar) => i64::try_from(scalar.clone()).unwrap_or(0),
                _ => return Err(DataFusionError::Execution("hash_bucket's bucket count must be a literal".to_string())),
            };
            let buckets = u64::try_from(buckets).ok().filter(|n| *n > 0).ok_or_else(|| {
                // A zero count would divide by zero; a negative one is a caller bug.
                DataFusionError::Execution("hash_bucket's bucket count must be positive".to_string())
            })?;
            let array = as_array(value)?;
            let strings = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| DataFusionError::Execution(format!("hash_bucket expects a Utf8View value, got {}", array.data_type())))?;
            let buckets: Int64Array =
                strings.iter().map(|value| Some((crate::read::hash_bytes(value.unwrap_or_default().as_bytes()) % buckets) as i64)).collect();
            Ok(ColumnarValue::Array(Arc::new(buckets)))
        }) as ScalarFunctionImplementation,
    )
}

#[cfg(test)]
mod hash_bucket_tests {
    use datafusion::prelude::SessionContext;

    /// Bucketing is only correct if it PARTITIONS: every row lands in exactly one
    /// bucket of `[0, n)`, and equal keys always land together — that is what lets
    /// the dedup rewrite process one shard at a time without splitting a key's
    /// copies across passes.
    #[tokio::test]
    async fn hash_bucket_partitions_and_keeps_equal_keys_together() {
        let ctx = SessionContext::new();
        ctx.register_udf(super::hash_bucket_udf());
        let one = |sql: &str| {
            let ctx = ctx.clone();
            let sql = sql.to_string();
            async move {
                let batches = ctx.sql(&sql).await.expect("plan").collect().await.expect("run");
                datafusion::arrow::util::pretty::pretty_format_batches(&batches).expect("format").to_string()
            }
        };
        // In range, and the same input always gives the same bucket.
        let rendered = one("SELECT hash_bucket(arrow_cast(v, 'Utf8View'), 256) AS b FROM (VALUES ('a'), ('a'), ('b')) AS t(v)").await;
        let buckets: Vec<i64> = rendered.lines().filter_map(|line| line.trim_matches(|c: char| c == '|' || c.is_whitespace()).parse::<i64>().ok()).collect();
        assert_eq!(buckets.len(), 3, "three rows: {rendered}");
        assert!(buckets.iter().all(|b| (0..256).contains(b)), "every bucket in range: {buckets:?}");
        assert_eq!(buckets[0], buckets[1], "equal keys must share a bucket");
        // NULL must not vanish: it buckets as the empty string, not as NULL.
        let rendered = one("SELECT hash_bucket(arrow_cast(NULL, 'Utf8View'), 256) AS b").await;
        assert!(!rendered.contains("NULL"), "NULL must bucket, not propagate: {rendered}");
    }

    /// A row that hashes outside every shard's range is a row the rewrite never
    /// reads and never rewrites — silent data loss that the conservation checks
    /// would only catch after the work was done. Spread matters too: a skewed
    /// bucketing makes one shard carry the memory the split existed to avoid.
    #[tokio::test]
    async fn hash_bucket_spreads_evenly_enough_to_shard_on() {
        let ctx = SessionContext::new();
        ctx.register_udf(super::hash_bucket_udf());
        let batches = ctx
            .sql(
                "SELECT count(*) AS n, count(DISTINCT hash_bucket(arrow_cast(v, 'Utf8View'), 256)) AS distinct_buckets, \
                 min(hash_bucket(arrow_cast(v, 'Utf8View'), 256)) AS lo, max(hash_bucket(arrow_cast(v, 'Utf8View'), 256)) AS hi \
                 FROM (SELECT CAST(i AS VARCHAR) AS v FROM generate_series(1, 5000) AS t(i))",
            )
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        let rendered = datafusion::arrow::util::pretty::pretty_format_batches(&batches).expect("format").to_string();
        // The one data row renders as `| n | distinct | lo | hi |`; every other
        // line is a border or the header.
        let row = rendered
            .lines()
            .find_map(|line| {
                let cells: Vec<i64> = line.split('|').filter_map(|cell| cell.trim().parse::<i64>().ok()).collect();
                (cells.len() == 4).then_some(cells)
            })
            .unwrap_or_else(|| panic!("one four-column data row: {rendered}"));
        assert_eq!(row[0], 5000, "all rows counted: {rendered}");
        assert_eq!(row[1], 256, "5000 keys must reach every one of 256 buckets: {rendered}");
        assert!((0..256).contains(&row[2]) && (0..256).contains(&row[3]), "bounds inside [0, 256): {rendered}");
    }
}

#[cfg(test)]
mod hll_tests {
    use datafusion::prelude::SessionContext;

    use super::*;

    async fn scalar(sql: &str) -> u64 {
        let mut ctx = SessionContext::new();
        register_custom_functions(&mut ctx).unwrap();
        let batches = ctx.sql(sql).await.expect("plan").collect().await.expect("execute");
        let column = batches[0].column(0);
        ScalarValue::try_from_array(column, 0).unwrap().cast_to(&DataType::UInt64).unwrap().to_string().parse().unwrap()
    }

    /// A `series % n` source gives a known distinct count, and 200k rows over
    /// the default target-partition count guarantees the partial/final split —
    /// so this also proves the state survives repartitioning.
    #[tokio::test]
    async fn hll_count_of_hll_agg_matches_the_true_cardinality() {
        for (n, tolerance) in [(1u64, 0.0), (500, 0.0), (5_000, 0.05), (200_000, 0.05)] {
            let estimate = scalar(&format!("SELECT hll_count(hll_agg(v)) FROM (SELECT value % {n} AS v FROM generate_series(1, 200000) t(value))")).await;
            let error = (estimate as f64 - n as f64).abs() / n as f64;
            assert!(error <= tolerance, "n={n}: estimated {estimate}, error {:.2}% > {:.0}%", error * 100.0, tolerance * 100.0);
        }
    }

    /// The rollup property: sketches built per group and folded afterwards must
    /// agree with one built over everything at once. Without this a 30-day tile
    /// cannot be answered from 1-minute buckets.
    #[tokio::test]
    async fn merging_per_bucket_sketches_equals_one_pass_over_all_rows() {
        let rows = "SELECT value % 30000 AS v, value % 7 AS bucket FROM generate_series(1, 300000) t(value)";
        let merged = scalar(&format!("SELECT hll_count(hll_merge(s)) FROM (SELECT hll_agg(v) AS s FROM ({rows}) GROUP BY bucket)")).await;
        let one_pass = scalar(&format!("SELECT hll_count(hll_agg(v)) FROM ({rows})")).await;
        assert_eq!(merged, one_pass, "folding per-bucket states must equal a single pass");
    }

    /// Distinct counts are asked of every column type, and NULL is not a value.
    #[tokio::test]
    async fn non_string_columns_work_and_nulls_are_ignored() {
        assert_eq!(scalar("SELECT hll_count(hll_agg(v)) FROM (VALUES (1),(2),(2),(NULL)) t(v)").await, 2);
        assert_eq!(scalar("SELECT hll_count(hll_agg(v)) FROM (VALUES (1.5),(2.5),(1.5)) t(v)").await, 2);
        assert_eq!(scalar("SELECT hll_count(hll_agg(v)) FROM (VALUES ('a'),('b'),('a')) t(v)").await, 2);
        assert_eq!(scalar("SELECT hll_count(hll_agg(v)) FROM (VALUES (arrow_cast(1, 'Timestamp(Microsecond, None)'))) t(v)").await, 1);
    }

    /// The exact SQL monoscope sends to BOTH backends. Verified 2026-08-13
    /// against prod Timescale: `distinct_count(approx_count_distinct(v))::float`
    /// returns 2 for (1,2,2,NULL). Toolkit's `approx_count_distinct` builds a
    /// SKETCH, not a count — `approx_count_distinct(x)::float` is a type error
    /// there — so TF has to split the same way or the one query text cannot run
    /// on both.
    #[tokio::test]
    async fn the_timescale_toolkit_spelling_runs_unchanged() {
        assert_eq!(scalar("SELECT distinct_count(approx_count_distinct(v)) FROM (VALUES (1),(2),(2),(NULL)) t(v)").await, 2);
        let rows = "SELECT value % 30000 AS v, value % 7 AS bucket FROM generate_series(1, 300000) t(value)";
        let direct = scalar(&format!("SELECT distinct_count(approx_count_distinct(v)) FROM ({rows})")).await;
        // Folding per-bucket sketches is the rewrite a rollup substitutes; it
        // must answer the same number as the raw fallback it replaces.
        let via_rollup = scalar(&format!("SELECT hll_count(hll_merge(s)) FROM (SELECT hll_agg(v) AS s FROM ({rows}) GROUP BY bucket)")).await;
        assert_eq!(direct, via_rollup, "the rewrite must not change the answer");
        let error = (direct as f64 - 30_000.0).abs() / 30_000.0;
        assert!(error < 0.05, "estimated {direct}, want ~30000");
    }

    /// The session query wraps the sketch in `FILTER (WHERE …)`; verified on
    /// prod Timescale in the same shape, so the one text runs on both.
    #[tokio::test]
    async fn the_filtered_form_the_session_query_sends_runs_here_too() {
        let sql = "SELECT distinct_count(approx_count_distinct(v) FILTER (WHERE v IS NOT NULL))::BIGINT FROM (VALUES (1),(2),(2),(NULL)) t(v)";
        assert_eq!(scalar(sql).await, 2);
    }

    /// `hll_count(NULL)` is NULL, not 0: a rollup row that never saw the measure
    /// makes no claim about its cardinality.
    #[tokio::test]
    async fn a_missing_sketch_is_null_not_zero() {
        let mut ctx = SessionContext::new();
        register_custom_functions(&mut ctx).unwrap();
        let batches = ctx.sql("SELECT hll_count(arrow_cast(NULL, 'Binary'))").await.unwrap().collect().await.unwrap();
        assert!(batches[0].column(0).is_null(0));
    }
}

// ============================================================================
// jsonb_path_exists UDF for JSONPath queries on Variant/JSON columns
// ============================================================================

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
        let [json, path] = args.args.as_slice() else {
            return Err(DataFusionError::Execution("jsonb_path_exists requires exactly 2 arguments: json/variant and jsonpath".to_string()));
        };
        let json_array = as_array(json)?;
        let path_str = match path {
            ColumnarValue::Scalar(scalar) => extract_utf8_string(scalar).ok_or_else(|| DataFusionError::Execution("JSONPath must be a string".to_string()))?,
            ColumnarValue::Array(_) => return Err(DataFusionError::Execution("JSONPath must be a scalar string".to_string())),
        };

        // PG SQL/JSON-path dialect.
        let json_path = sql_json_path::JsonPath::new(&path_str).map_err(|e| DataFusionError::Execution(format!("Invalid JSONPath: {e}")))?;
        let result = if is_variant_type(json_array.data_type()) {
            evaluate_jsonpath_on_variant(&json_array, &json_path, &path_str)?
        } else {
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
        return Err(DataFusionError::Execution(format!("Variant nesting depth exceeds limit of {MAX_VARIANT_DEPTH}")));
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
        Variant::Object(obj) => JsonValue::Object(
            obj.iter().map(|(key, value)| Ok((key.to_string(), variant_to_serde_json(&value, depth + 1)?))).collect::<Result<_, DataFusionError>>()?,
        ),
        Variant::List(list) => JsonValue::Array(list.iter().map(|v| variant_to_serde_json(&v, depth + 1)).collect::<Result<_, _>>()?),
    })
}

/// Accessor that uniformly reads bytes from either `BinaryArray` or `BinaryViewArray`.
/// Delta-rs/Parquet may yield either representation depending on
/// `schema_force_view_types`, so variant decoding handles both transparently.
enum BinaryAccessor<'a> {
    Binary(&'a BinaryArray),
    View(&'a BinaryViewArray),
}

impl<'a> BinaryAccessor<'a> {
    fn try_new(col: &'a ArrayRef, field: &str) -> datafusion::error::Result<Self> {
        if let Some(a) = col.as_any().downcast_ref::<BinaryArray>() {
            Ok(Self::Binary(a))
        } else if let Some(a) = col.as_any().downcast_ref::<BinaryViewArray>() {
            Ok(Self::View(a))
        } else {
            Err(DataFusionError::Execution(format!("Variant {field} column is not Binary or BinaryView (got {:?})", col.data_type())))
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
fn evaluate_jsonpath_on_variant(array: &ArrayRef, json_path: &sql_json_path::JsonPath, raw_path: &str) -> datafusion::error::Result<ArrayRef> {
    // Fast path: simple `$.a.b.c[N].d` style paths translate cleanly to a
    // parquet_variant_compute::VariantPath and we can use the vectorized
    // `variant_get` kernel, which walks the Variant binary directly without
    // ever materializing the full JsonValue. Path existence = result is
    // non-null per row.
    //
    // Parity caveat: variant_get resolves like PG *strict* mode — it does NOT
    // perform PG lax auto-unwrapping (`.a` on an array, `[i]` on a scalar). So
    // for a filter-free path over an array-shaped-where-object-expected value
    // this can yield a false negative vs. the PG (lax) engine used by the
    // fallback below. Monoscope's queries are unaffected — they always carry a
    // `? (...)` filter, which is not a simple path and takes the fallback. A
    // lax-correct variant-native evaluator is the deferred Phase 3 fix.
    if let Some(variant_path) = simple_path_to_variant_path(raw_path) {
        use parquet_variant_compute::{GetOptions, variant_get};
        let opts = GetOptions::new_with_path(variant_path);
        let extracted = variant_get(array, opts).map_err(|e| DataFusionError::Execution(format!("variant_get failed: {e}")))?;
        // A NULL input row → NULL (SQL semantics, matches the fallback path and PG);
        // a present row → path exists ↔ extracted is non-null. `extracted.is_null(i)`
        // alone can't tell the two apart, so gate on the input's null buffer.
        let out: BooleanArray = (0..extracted.len()).map(|i| (!array.is_null(i)).then(|| !extracted.is_null(i))).collect();
        return Ok(Arc::new(out));
    }

    // Fallback: complex JSONPath (filters, recursive descent, etc.) — walk the
    // Variant binary into a JsonValue and run the PG jsonpath engine. Avoided
    // when the path is simple (filter-free) via the variant_get fast lane above.
    //
    // Deferred optimization: evaluate the filter against the Variant binary
    // directly (no per-row JsonValue). The clean route — impl sql_json_path's
    // JsonRef over parquet_variant::Variant so the SAME engine (hence identical
    // PG semantics) walks the binary — is blocked because Variant/VariantList/
    // VariantObject are Clone, not the Copy that JsonRef requires. It also buys
    // little for the dominant `$[*] ? (@ == x)` shape: the `[*]` prefix needs
    // the whole (small) column anyway, so only a JsonValue alloc is saved.
    // Revisit if a profile shows this materialization is a real hot spot.
    use datafusion::arrow::array::StructArray;
    use parquet_variant::Variant;
    let struct_array = array.as_any().downcast_ref::<StructArray>().ok_or_else(|| DataFusionError::Execution("Expected Variant struct array".to_string()))?;
    let metadata_col = struct_array.column_by_name("metadata").ok_or_else(|| DataFusionError::Execution("Variant missing metadata column".to_string()))?;
    let value_col = struct_array.column_by_name("value").ok_or_else(|| DataFusionError::Execution("Variant missing value column".to_string()))?;
    let metadata_binary = BinaryAccessor::try_new(metadata_col, "metadata")?;
    let value_binary = BinaryAccessor::try_new(value_col, "value")?;
    // Lax mode (PG default): a data-dependent eval error is an empty match, not a query failure.
    let out: BooleanArray = (0..struct_array.len())
        .map(|i| {
            if struct_array.is_null(i) {
                Ok(None)
            } else {
                variant_to_serde_json(&Variant::new(metadata_binary.value(i), value_binary.value(i)), 0)
                    .map(|json| Some(json_path.exists(&json).unwrap_or(false)))
            }
        })
        .collect::<datafusion::error::Result<_>>()?;
    Ok(Arc::new(out))
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
fn evaluate_jsonpath_on_json_string(array: &ArrayRef, json_path: &sql_json_path::JsonPath) -> datafusion::error::Result<ArrayRef> {
    // Path exists per row; invalid JSON or a lax-mode eval error → false (PG parity).
    let eval = |s: &str| serde_json::from_str::<JsonValue>(s).ok().and_then(|v| json_path.exists(&v).ok()).unwrap_or(false);
    let iter: Box<dyn Iterator<Item = Option<&str>>> = if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
        Box::new(a.iter())
    } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        Box::new(a.iter())
    } else {
        return Err(DataFusionError::Execution("jsonb_path_exists requires JSON string or Variant input".to_string()));
    };
    Ok(Arc::new(iter.map(|opt| opt.map(eval)).collect::<BooleanArray>()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `::` binds tighter than `->>`, so monoscope emits a cast on the PATH.
    /// Both spellings address the same field and must plan to the same
    /// `variant_get`; before the cast was unwrapped the second one did not
    /// plan at all against a Variant column.
    #[test_case::test_case(false ; "bare path literal")]
    #[test_case::test_case(true ; "cast path literal, the shape monoscope emits")]
    fn an_arrow_path_literal_addresses_the_same_field_through_a_cast(cast: bool) {
        let literal = datafusion::prelude::lit("route");
        let expr = if cast { Expr::Cast(datafusion::logical_expr::Cast::new(Box::new(literal), arrow::datatypes::DataType::Utf8)) } else { literal };
        assert_eq!(
            extract_path_component(&expr),
            Some(PathComponent::Field("route".to_string())),
            "a cast around a path literal cannot change which field is addressed"
        );
    }

    #[test]
    fn percentile_agg_state_is_bounded() {
        let mut digest = TDigestWrapper::default();
        digest.insert_batch((0..100_000).map(|value| value as f64));
        assert!(digest.to_bytes().unwrap().len() < 10_000, "percentile state must not grow with input rows");
    }

    #[test]
    fn percentile_agg_merge_preserves_tail_estimate() {
        let mut left = TDigestWrapper::default();
        let mut right = TDigestWrapper::default();
        left.insert_batch((0..50_000).map(|value| value as f64));
        right.insert_batch((50_000..100_000).map(|value| value as f64));
        left.merge(&right);

        assert!(left.to_bytes().unwrap().len() < 10_000);
        assert!((left.to_digest().unwrap().estimate_quantile(0.95) - 95_000.0).abs() < 1_000.0);
    }

    #[tokio::test]
    async fn tdigest_merge_merges_serialized_percentile_states() {
        let mut ctx = datafusion::prelude::SessionContext::new();
        register_custom_functions(&mut ctx).expect("functions register");
        let batches = ctx
            .sql(
                "SELECT approx_percentile(0.95, tdigest_merge(digest)) FROM (\
                   SELECT percentile_agg(value) AS digest FROM (VALUES (1.0), (2.0), (3.0)) AS low(value) \
                   UNION ALL \
                   SELECT percentile_agg(value) AS digest FROM (VALUES (100.0), (101.0), (102.0)) AS high(value)\
                 )",
            )
            .await
            .expect("plan tdigest merge")
            .collect()
            .await
            .expect("run tdigest merge");
        let values = batches[0].column(0).as_any().downcast_ref::<Float64Array>().expect("percentile output");
        assert!(values.value(0) > 90.0);
    }

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
            // AM/PM, Dy, bare HH round-out token coverage.
            ("HH12:MI AM", "08:10 AM"),
            ("HH:MI:SS", "08:10:52"),   // bare HH aliases HH12 (12-hour clock with leading zero).
            ("HH12:MI am", "08:10 am"), // lowercase am token emits lowercase output.
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

    /// PG parity: `to_jsonb(text[])` produces an array of JSON *strings*. Elements
    /// that happen to look like JSON (log bodies, attributes payloads in monoscope's
    /// `summary` column) must NOT be re-parsed into objects/arrays — that broke the
    /// log explorer's row renderer ("e.indexOf is not a function").
    #[tokio::test]
    async fn test_to_jsonb_text_array_elements_stay_strings() {
        use datafusion::prelude::SessionContext;
        let mut ctx = SessionContext::new();
        register_custom_functions(&mut ctx).unwrap();
        let sql = r#"SELECT to_jsonb(make_array('{"a":1}', '[1,2]', 'plain', '123')) AS s"#;
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0).as_any().downcast_ref::<datafusion::arrow::array::StringViewArray>().unwrap();
        assert_eq!(col.value(0), r#"["{\"a\":1}","[1,2]","plain","123"]"#);
        // Independent of serialisation format: every element must be a JSON *string*.
        let parsed: serde_json::Value = serde_json::from_str(col.value(0)).unwrap();
        assert!(parsed.as_array().unwrap().iter().all(serde_json::Value::is_string), "elements must stay strings: {parsed}");
        // Top-level Utf8 scalars keep the JSON sniff: Variant/Utf8 columns holding
        // JSON (attributes, events, links) rely on it to surface as real JSON.
        let sql = r#"SELECT to_jsonb('{"a":1}') AS s"#;
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0).as_any().downcast_ref::<datafusion::arrow::array::StringViewArray>().unwrap();
        assert_eq!(col.value(0), r#"{"a":1}"#);
        // to_json shares array_to_json_values, so the same rule applies — monoscope's
        // selectChildSpansAndLogs emits to_json(summary).
        let sql = r#"SELECT to_json(make_array('{"a":1}')) AS s"#;
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0).as_any().downcast_ref::<datafusion::arrow::array::StringViewArray>().unwrap();
        assert_eq!(col.value(0), r#"["{\"a\":1}"]"#);
    }

    /// LargeList and FixedSizeList must keep list structure (they used to fall
    /// through to the cast-to-string arm) and follow the same no-sniff rule.
    #[test]
    fn test_large_and_fixed_size_list_to_json_values() {
        use datafusion::arrow::array::{FixedSizeListBuilder, GenericListBuilder, StringViewBuilder};
        let expected = vec![serde_json::json!([r#"{"a":1}"#, "plain"])];
        let mut b = GenericListBuilder::<i64, _>::new(StringViewBuilder::new());
        b.values().append_value(r#"{"a":1}"#);
        b.values().append_value("plain");
        b.append(true);
        let arr: ArrayRef = Arc::new(b.finish());
        assert_eq!(array_to_json_values(&arr).unwrap(), expected);

        let mut b = FixedSizeListBuilder::new(StringViewBuilder::new(), 2);
        b.values().append_value(r#"{"a":1}"#);
        b.values().append_value("plain");
        b.append(true);
        let arr: ArrayRef = Arc::new(b.finish());
        assert_eq!(array_to_json_values(&arr).unwrap(), expected);
    }

    /// Regression guard: json_build_array used to call array_to_json_values
    /// per row per arg — O(rows² × args). At one 8192-row batch that's ~10s;
    /// linear is <100ms. Also pins mixed scalar+array broadcast (a scalar
    /// first arg used to clamp num_rows to 1).
    #[test]
    fn test_json_build_array_linear_and_broadcast() {
        use datafusion::{
            arrow::array::{Int64Array, StringViewArray},
            logical_expr::ScalarFunctionArgs,
        };
        let n = 8192;
        let ids: ArrayRef = Arc::new(StringViewArray::from_iter_values((0..n).map(|i| format!("id-{i}"))));
        let nums: ArrayRef = Arc::new(Int64Array::from_iter_values(0..n as i64));
        let scalar = ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some("tag".into())));
        let args = ScalarFunctionArgs {
            args: vec![scalar, ColumnarValue::Array(ids), ColumnarValue::Array(nums)],
            arg_fields: vec![],
            number_rows: n,
            return_field: Arc::new(Field::new("", DataType::Utf8View, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let start = std::time::Instant::now();
        let ColumnarValue::Array(out) = JsonBuildArrayUDF::new().invoke_with_args(args).unwrap() else { panic!("expected array output") };
        assert!(start.elapsed() < std::time::Duration::from_secs(2), "quadratic regression: took {:?}", start.elapsed());
        let out = out.as_any().downcast_ref::<datafusion::arrow::array::StringViewArray>().unwrap();
        assert_eq!(out.len(), n);
        assert_eq!(out.value(7), r#"["tag","id-7",7]"#);
    }

    #[test]
    fn test_empty_tdigest_wrapper() {
        assert!(TDigestWrapper::default().to_digest().is_none());
        let mut wrapper = TDigestWrapper::default();
        wrapper.insert_batch(vec![10.0, 20.0]);
        assert!(wrapper.to_digest().is_some());
    }

    #[test]
    fn test_parse_interval_to_micros() {
        // Both spellings — `N unit` and the space-less `Nunit` — for every unit alias.
        let cases: &[(&str, i64)] = &[
            ("1 second", 1_000_000),
            ("5 seconds", 5_000_000),
            ("1 minute", 60_000_000),
            ("5 minutes", 300_000_000),
            ("1 hour", 3_600_000_000),
            ("2 hours", 7_200_000_000),
            ("1 day", 86_400_000_000),
            ("1 week", 604_800_000_000),
            ("5 min", 300_000_000),
            ("5 mins", 300_000_000),
            ("5 m", 300_000_000),
            ("1second", 1_000_000),
            ("5seconds", 5_000_000),
            ("1minute", 60_000_000),
            ("5minutes", 300_000_000),
            ("30m", 1_800_000_000),
            ("1h", 3_600_000_000),
            ("2h", 7_200_000_000),
            ("1d", 86_400_000_000),
            ("1w", 604_800_000_000),
            ("5min", 300_000_000),
            ("5mins", 300_000_000),
            ("5s", 5_000_000),
        ];
        for (input, expected) in cases {
            assert_eq!(parse_interval_to_micros(input).unwrap(), *expected, "interval: {input}");
        }
        // No unit, no number, non-numeric value, unit-before-number, and overflow.
        for bad in ["invalid", "5", "abc minutes", "m5", "9223372036854 weeks"] {
            assert!(parse_interval_to_micros(bad).is_err(), "expected error for: {bad}");
        }
    }
}

#[cfg(test)]
mod row_to_json_tests {
    use super::*;

    /// `row_to_json` is PG's `to_json` over a record. Struct support also fixes
    /// `to_json`/`to_jsonb` of a struct column, which previously failed with
    /// "Cast error: Casting from Struct(...) to Utf8View not supported".
    ///
    /// Keys come out SORTED, matching PG's `jsonb`, not `json` (which preserves
    /// column order). Both share this code path, and serde_json's Map is a
    /// BTreeMap unless the crate-wide `preserve_order` feature is on — not worth
    /// flipping globally for key order no caller depends on.
    #[tokio::test]
    async fn row_to_json_renders_a_struct_as_a_json_object() {
        use datafusion::prelude::SessionContext;
        let mut ctx = SessionContext::new();
        register_custom_functions(&mut ctx).unwrap();
        for sql in ["SELECT row_to_json(named_struct('total', 1, 'active', 2)) AS d", "SELECT to_json(named_struct('total', 1, 'active', 2)) AS d"] {
            let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            let column = batches[0].column(0).as_any().downcast_ref::<StringViewArray>().unwrap();
            assert_eq!(column.value(0), r#"{"active":2,"total":1}"#, "sql: {sql}");
        }
    }

    /// Documented limitation. PG lets `row_to_json(t)` name a whole row; DataFusion
    /// rejects the bare relation alias during SQL PLANNING ("No field named t"),
    /// before any analyzer rule can rewrite it — so pgAdmin's dashboard charts
    /// still fail. Fixing it needs an AST-level rewrite that reads the derived
    /// table's column aliases; the struct form above is what works today.
    #[tokio::test]
    async fn bare_relation_alias_is_still_unsupported() {
        use datafusion::prelude::SessionContext;
        let mut ctx = SessionContext::new();
        register_custom_functions(&mut ctx).unwrap();
        let error = ctx.sql("SELECT row_to_json(t) FROM (SELECT 1 AS total) t").await.unwrap_err().to_string();
        assert!(error.contains("No field named t"), "unexpected error: {error}");
    }
}
