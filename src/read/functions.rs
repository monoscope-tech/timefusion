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
            Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
            StringArray, StringViewArray, TimestampMicrosecondArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        },
        datatypes::{DataType, Field, FieldRef, IntervalUnit, TimeUnit},
    },
    common::{DFSchema, DataFusionError, ExprSchema, ScalarValue, not_impl_err},
    functions::regex::expr_fn::regexp_match,
    functions_nested::expr_fn::array_element,
    logical_expr::{
        Accumulator, AggregateUDF, ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarFunctionImplementation, ScalarUDF, ScalarUDFImpl, Signature,
        TypeSignature, Volatility, create_udaf, create_udf,
        expr::{Alias, ScalarFunction},
        planner::{ExprPlanner, PlannerResult, RawBinaryExpr, TypePlanner},
        sort_properties::{ExprProperties, SortProperties},
    },
    prelude::lit,
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

/// Exactly-N argument destructuring. `what` is the trailing
/// ": <arg description>" of the error message (empty for one-argument forms).
fn args_n<'a, const N: usize>(args: &'a [ColumnarValue], name: &str, what: &str) -> datafusion::error::Result<&'a [ColumnarValue; N]> {
    args.try_into().map_err(|_| DataFusionError::Execution(format!("{name} requires exactly {N} argument{}{what}", if N == 1 { "" } else { "s" })))
}

/// Materializes scalars as one-element arrays.
fn as_array(v: &ColumnarValue) -> datafusion::error::Result<ArrayRef> {
    match v {
        ColumnarValue::Array(a) => Ok(a.clone()),
        ColumnarValue::Scalar(s) => s.to_array(),
    }
}

/// Downcasts an Arrow array to its concrete type, failing with `msg`.
fn downcast<T: 'static>(array: &dyn Array, msg: impl std::fmt::Display) -> datafusion::error::Result<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| DataFusionError::Execution(msg.to_string()))
}

/// `name`/`signature` accessors, plus — when given — a constant `return_type`
/// and the `aliases()` accessor.
macro_rules! udf_boilerplate {
    ($name:literal) => {
        fn name(&self) -> &str {
            $name
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
    };
    ($name:literal, $ret:expr) => {
        udf_boilerplate!($name);
        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok($ret)
        }
    };
    ($name:literal, $ret:expr, aliases) => {
        udf_boilerplate!($name, $ret);
        fn aliases(&self) -> &[String] {
            &self.aliases
        }
    };
}

/// Declare a UDF struct whose only state is its `Signature` (plus, optionally,
/// its `aliases()` list) together with the `Default` that builds it.
macro_rules! udf_struct {
    ($(#[$m:meta])* $ty:ident, $sig:expr) => {
        $(#[$m])*
        #[derive(Debug, Hash, Eq, PartialEq)]
        struct $ty {
            signature: Signature,
        }
        impl Default for $ty {
            fn default() -> Self {
                Self { signature: $sig }
            }
        }
    };
    ($(#[$m:meta])* $ty:ident, $sig:expr, aliases: $aliases:expr) => {
        $(#[$m])*
        #[derive(Debug, Hash, Eq, PartialEq)]
        struct $ty {
            signature: Signature,
            aliases: Vec<String>,
        }
        impl Default for $ty {
            fn default() -> Self {
                Self { signature: $sig, aliases: $aliases }
            }
        }
    };
}

/// A cast around a path/format literal cannot change what it addresses, so every
/// literal extractor below looks through one.
fn uncast(expr: &Expr) -> &Expr {
    match expr {
        Expr::Cast(cast) => cast.expr.as_ref(),
        expr => expr,
    }
}

fn udf_call(func: Arc<ScalarUDF>, args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction { func, args })
}

/// Resolves PostgreSQL types that DataFusion does not model natively as text.
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
pub(super) enum PathComponent {
    Field(String),
    Index(i64),
}

impl ExprPlanner for VariantAwareExprPlanner {
    fn plan_binary_op(&self, expr: RawBinaryExpr, schema: &DFSchema) -> datafusion::error::Result<PlannerResult<RawBinaryExpr>> {
        // PG array overlap: `a && b` → array_has_any(a, b), which DataFusion's
        // NestedFunctionPlanner does not cover. Only when both sides are lists.
        if matches!(expr.op, BinaryOperator::PGOverlap)
            && matches!(expr.left.get_type(schema)?, DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..))
            && matches!(expr.right.get_type(schema)?, DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..))
        {
            return Ok(PlannerResult::Planned(datafusion::functions_nested::expr_fn::array_has_any(expr.left, expr.right)));
        }

        // `#>`/`#>>` address the same leaves as `->`/`->>`, but take the whole
        // path as one text[] literal instead of a chain.
        let (is_long_arrow, path_is_array) = match &expr.op {
            BinaryOperator::Arrow => (false, false),
            BinaryOperator::LongArrow => (true, false),
            BinaryOperator::HashArrow => (false, true),
            BinaryOperator::HashLongArrow => (true, true),
            _ => return Ok(PlannerResult::Original(expr)),
        };

        // The empty path addresses the whole document: PG's `x #> '{}'` is `x`, and
        // `x #>> '{}'` is `x` rendered as text (a JSON string losing its quotes).
        if path_is_array && is_empty_path_array(&expr.right) {
            let base = unalias(&expr.left);
            let json = if is_variant_column(&base, schema) { udf_call(variant_to_json_udf(), vec![base]) } else { base };
            return Ok(PlannerResult::Planned(if is_long_arrow { udf_call(json_to_pg_text_udf(), vec![json]) } else { json }));
        }

        let (base_expr, prefix) = if path_is_array { (unalias(&expr.left), vec![]) } else { collect_arrow_chain(&expr.left) };
        let Some(components) = (if path_is_array { extract_path_array(&expr.right) } else { extract_path_component(&expr.right).map(|c| vec![c]) }) else {
            return Ok(PlannerResult::Original(expr));
        };
        let path_parts: Vec<_> = prefix.into_iter().chain(components).collect();

        if !is_variant_column(&base_expr, schema) {
            return Ok(PlannerResult::Original(expr)); // Let JSON planner handle
        }

        // `variant_get` cannot stringify numeric/boolean leaves. Compose through
        // JSON text to preserve PostgreSQL `->>` semantics.
        let path_literal = Expr::Literal(ScalarValue::Utf8(Some(build_variant_path(&path_parts))), None);
        let base_repr = expr_repr(&base_expr);
        let variant_leaf = udf_call(variant_get_udf(), vec![base_expr, path_literal]);
        let result = if is_long_arrow { udf_call(json_to_pg_text_udf(), vec![udf_call(variant_to_json_udf(), vec![variant_leaf])]) } else { variant_leaf };

        let op_str = match (path_is_array, is_long_arrow) {
            (false, false) => "->",
            (false, true) => "->>",
            (true, false) => "#>",
            (true, true) => "#>>",
        };
        let alias_name = format!("{base_repr} {op_str} {}", path_repr(&path_parts));
        Ok(PlannerResult::Planned(Expr::Alias(Alias::new(result, None::<&str>, alias_name))))
    }

    // PG's `substring(string FROM pattern)`: a string FROM operand is a POSIX
    // regex and the result is the matched text (first capture group, else the
    // whole match). `regexp_match` element 1 has exactly those semantics.
    // sqlparser lowers this and the offset spelling to the same 2-arg form.
    fn plan_substring(&self, args: Vec<Expr>) -> datafusion::error::Result<PlannerResult<Vec<Expr>>> {
        // Only a string LITERAL routes. A column-typed operand is genuinely
        // ambiguous, and an offset must keep reaching the default planner.
        let [value, pattern @ Expr::Literal(ScalarValue::Utf8(Some(_)) | ScalarValue::Utf8View(Some(_)) | ScalarValue::LargeUtf8(Some(_)), _)] =
            args.as_slice()
        else {
            return Ok(PlannerResult::Original(args));
        };
        Ok(PlannerResult::Planned(array_element(regexp_match(value.clone(), pattern.clone(), None), lit(1i64))))
    }
}

fn unalias(expr: &Expr) -> Expr {
    match expr {
        Expr::Alias(alias) => unalias(&alias.expr),
        expr => expr.clone(),
    }
}

/// `{}` — the empty `text[]` path, addressing the whole document. Handled by its
/// own arm in the planner above; `extract_path_array` rejects it.
fn is_empty_path_array(expr: &Expr) -> bool {
    matches!(uncast(expr), Expr::Literal(v, _) if extract_utf8_string(v).is_some_and(|raw| raw.trim() == "{}"))
}

/// Path operand of `#>`/`#>>`: a `text[]`, reaching the planner either as the
/// unparsed literal `{a,b,c}` or as an already-built list. Quoted elements
/// (`{"a b",c}`) are unquoted; an empty path returns `None`.
fn extract_path_array(expr: &Expr) -> Option<Vec<PathComponent>> {
    let parts: Vec<PathComponent> = match uncast(expr) {
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

pub(super) fn extract_path_component(expr: &Expr) -> Option<PathComponent> {
    // `::` binds tighter than `->>`, so `attributes->>'route'::text` puts the cast
    // on the PATH literal. A cast cannot change which field is addressed: unwrap it,
    // else this returns None and the expr falls through to datafusion-functions-json,
    // which cannot plan against a Variant column.
    let Expr::Literal(v, _) = uncast(expr) else { return None };
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

/// UDFs whose result is a Variant, so `->`/`->>` applied to one must route to
/// `variant_get` rather than fall through to datafusion-functions-json.
const VARIANT_PRODUCING_UDFS: [&str; 7] =
    ["json_to_variant", "variant_get", "cast_to_variant", "variant_object_construct", "variant_list_construct", "variant_object_insert", "variant_list_insert"];

/// Check if expression evaluates to a Variant type
fn is_variant_column(expr: &Expr, schema: &DFSchema) -> bool {
    match expr {
        // The SQL-facing schema un-types Variant columns to Utf8View and tags them
        // `tf.pg_type=jsonb`, so the Struct type is gone by planning time — the tag
        // must be detected too. On a base column the tag is only ever set on Variants.
        Expr::Column(col) => {
            schema.field_from_column(col).is_ok_and(|f| is_variant_type(f.data_type()) || f.metadata().get("tf.pg_type").is_some_and(|v| v == "jsonb"))
        }
        Expr::Alias(alias) => is_variant_column(&alias.expr, schema),
        Expr::ScalarFunction(func) => VARIANT_PRODUCING_UDFS.iter().any(|name| *name == func.func.name()),
        _ => expr.get_type(schema).is_ok_and(|dt| is_variant_type(&dt)),
    }
}

/// Build a `variant_get` path string from components:
/// `["user", "name"]` → `['user']['name']`, `["items", Index(0)]` → `['items'][0]`.
///
/// Field names are ALWAYS bracket-quoted, never bare dot notation: `VariantPath`
/// reads `a.b` as a two-element nested path, but Postgres `->>` takes the whole
/// literal as ONE key (e.g. `attributes ->> 'http.request.method'`). Inside the
/// brackets `\` and `]` are backslash-escaped, as `parquet_variant` expects.
pub(super) fn build_variant_path(parts: &[PathComponent]) -> String {
    parts
        .iter()
        .map(|part| match part {
            PathComponent::Field(name) => format!("['{}']", name.replace('\\', "\\\\").replace(']', "\\]")),
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

udf_struct!(
    /// `json_to_pg_text(utf8) → utf8`: convert JSON-encoded text to Postgres `->>` text.
    ///
    /// - JSON string `"Alice"` → `Alice` (parsed, so escape sequences resolve correctly)
    /// - JSON null → SQL NULL
    /// - JSON number / boolean → its literal text (`42`, `true`)
    /// - JSON object / array → returned as-is (Postgres `->>` does the same)
    JsonToPgTextUdf,
    Signature::uniform(1, vec![DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8], Volatility::Immutable)
);

impl ScalarUDFImpl for JsonToPgTextUdf {
    udf_boilerplate!("json_to_pg_text", DataType::Utf8);
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let [arg] = args_n::<1>(&args.args, "json_to_pg_text", "")?;
        let arr = arg.to_array(args.number_rows)?;
        // Cast once to Utf8 — collapses Utf8/Utf8View/LargeUtf8 to one concrete shape.
        let utf8 = datafusion::arrow::compute::cast(&arr, &DataType::Utf8).map_err(arrow_err)?;
        let strs: &StringArray = downcast(&utf8, "json_to_pg_text: cast to Utf8 failed")?;
        let mut b = datafusion::arrow::array::StringBuilder::with_capacity(strs.len(), strs.value_data().len());
        // Parse via serde_json so escape sequences resolve and shapes like '"a"+"b"'
        // don't trigger naive unquoting. JSON null → SQL NULL; JSON string → its raw
        // text; anything else → its JSON literal text (per PG ->>).
        strs.iter().for_each(|opt| match opt.map(|s| (s, serde_json::from_str::<JsonValue>(s))) {
            None | Some((_, Ok(JsonValue::Null))) => b.append_null(),
            Some((_, Ok(JsonValue::String(inner)))) => b.append_value(&inner),
            Some((s, _)) => b.append_value(s),
        });
        Ok(ColumnarValue::Array(Arc::new(b.finish())))
    }
}

/// Rebuild `f` carrying one extra metadata key.
fn with_meta(f: &Field, key: &str, val: &str) -> FieldRef {
    let mut md = f.metadata().clone();
    md.insert(key.into(), val.into());
    Arc::new(f.clone().with_metadata(md))
}

/// Re-stamp the `ARROW:extension:name = arrow.parquet.variant` marker, which the
/// logical plan carries but the physical executor's per-row Field drops.
/// `datafusion-variant`'s UDFs fail without it ("Extension type name missing").
fn stamp_variant_field(f: &FieldRef) -> FieldRef {
    use crate::schema::{VARIANT_EXT_KEY as EXT_KEY, VARIANT_EXT_VALUE as EXT_VAL};
    if !is_variant_type(f.data_type()) || f.metadata().get(EXT_KEY).map(String::as_str) == Some(EXT_VAL) {
        return f.clone();
    }
    with_meta(f, EXT_KEY, EXT_VAL)
}

/// Wrap a `datafusion-variant` UDF so its arg fields get the Variant extension
/// marker re-stamped before delegation. `JSONB_OUT` tags the output Field with
/// `tf.pg_type = jsonb` so it surfaces over the wire as PG OID 3802, not text.
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
    // Must be forwarded: `VariantGetUdf` panics in `return_type` and computes its
    // output Field here instead, so the default impl would not do.
    fn return_field_from_args(&self, args: datafusion::logical_expr::ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        let f = self.inner.return_field_from_args(args)?;
        Ok(if JSONB_OUT { with_meta(&f, "tf.pg_type", "jsonb") } else { f })
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::compute::cast;
        // datafusion-variant's UDFs declare a BinaryView Variant output but pass the
        // input `metadata` buffer through unchanged; TF stores Variants as
        // Struct(Binary, Binary), so a Binary input makes the inner UDF's declared and
        // actual output types disagree. Coerce Variant args to BinaryView here; the
        // on-disk / MemBuffer representation stays Binary.
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

/// Process-wide singleton accessor for a stateless UDF, so analyzer rules clone
/// one `Arc` instead of allocating a `ScalarUDF` per rewritten expression.
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

/// `ctx.register_udf(ScalarUDF::from(T))` for each UDF built from a default struct.
macro_rules! reg_from {
    ($ctx:expr, $($udf:expr),+ $(,)?) => { $( $ctx.register_udf(ScalarUDF::from($udf)); )+ };
}

pub fn register_custom_functions(ctx: &mut datafusion::execution::context::SessionContext) -> Result<()> {
    // Must be registered before the JSON planner, which would otherwise win.
    datafusion::execution::FunctionRegistry::register_expr_planner(ctx, Arc::new(VariantAwareExprPlanner))?;

    reg_from!(
        ctx,
        crate::read::optimizers::PgCoalesceUdf::default(),
        ToCharUDF::default(),
        AtTimeZoneUDF::default(),
        JsonBuildArrayUDF::default(),
        JsonbBuildArrayUDF::default(),
        ToJsonbUDF::default(),
        ToJsonUDF::default(),
        ExtractEpochUDF::default(),
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
        JsonbPathExistsUDF::default(),
        JsonbPathQueryFirstUDF::default(),
        ApproxPercentileUDF::default(),
    );

    // create_udf-based UDFs that carry construction logic.
    ctx.register_udf(create_jsonb_array_elements_udf());
    ctx.register_udf(ScalarUDF::from(TimeBucketUDF::default()));
    ctx.register_udaf(binary_state_udaf("percentile_agg", DataType::Float64, Arc::new(|_| Ok(Box::<SketchAccumulator<TDigestWrapper>>::default()))));
    ctx.register_udaf(binary_state_udaf("tdigest_merge", DataType::Binary, Arc::new(|_| Ok(SketchAccumulator::<TDigestWrapper>::merging()))));
    ctx.register_udaf(AggregateUDF::from(HllAggUDF::default()));
    ctx.register_udaf(binary_state_udaf("hll_merge", DataType::Binary, Arc::new(|_| Ok(SketchAccumulator::<crate::read::Hll>::merging()))));
    ctx.register_udf(create_hll_count_udf());
    ctx.register_udf(hash_bucket_udf());

    // text_match(col, 'query'): tantivy-accelerated full-text search, with a naive
    // substring fallback when tantivy is off or when post-filtering MemBuffer rows.
    ctx.register_udf(crate::tantivy::udf::text_match_udf());

    // Test-only clock UDFs, gated so a production deployment can't have its
    // eviction/flush clock yanked by a stray SQL session.
    if std::env::var("TIMEFUSION_ENABLE_TEST_UDFS").is_ok_and(|v| v == "true" || v == "1") {
        ctx.register_udf(create_set_clock_udf());
        ctx.register_udf(create_advance_clock_udf());
        ctx.register_udf(create_now_micros_udf());
        tracing::warn!("TIMEFUSION_ENABLE_TEST_UDFS=true; clock UDFs registered. Do NOT enable in production.");
    }

    Ok(())
}

pub type FnRegistry = dyn datafusion::execution::FunctionRegistry + Send + Sync;

/// Process-wide Arc'd FunctionRegistry pre-populated with all custom UDFs,
/// built once on first call.
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
        let s: &StringArray = downcast(&arr, "timefusion_set_clock expects Utf8")?;
        let parse = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .map(|t| crate::support::set_micros(t.timestamp_micros()))
                .map_err(|e| DataFusionError::Execution(format!("invalid rfc3339: {e}")))
        };
        let out: Int64Array = s.iter().map(|v| v.map(parse).transpose()).collect::<datafusion::error::Result<_>>()?;
        Ok(ColumnarValue::Array(Arc::new(out)))
    });
    create_udf("timefusion_set_clock", vec![DataType::Utf8], DataType::Int64, Volatility::Volatile, fun)
}

/// `timefusion_advance_clock(delta_micros)` → new bigint micros.
fn create_advance_clock_udf() -> ScalarUDF {
    let fun: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        let arr = as_array(&args[0])?;
        let d: &Int64Array = downcast(&arr, "timefusion_advance_clock expects Int64")?;
        Ok(ColumnarValue::Array(Arc::new(d.iter().map(|v| v.map(crate::support::advance_micros)).collect::<Int64Array>())))
    });
    create_udf("timefusion_advance_clock", vec![DataType::Int64], DataType::Int64, Volatility::Volatile, fun)
}

/// `timefusion_now_micros()` → current clock value (frozen or wall).
fn create_now_micros_udf() -> ScalarUDF {
    let fun: ScalarFunctionImplementation =
        Arc::new(move |_args: &[ColumnarValue]| Ok(ColumnarValue::Array(Arc::new(Int64Array::from(vec![crate::support::now_micros()])))));
    create_udf("timefusion_now_micros", vec![], DataType::Int64, Volatility::Volatile, fun)
}

udf_struct!(ToCharUDF, Signature::any(2, Volatility::Immutable));

impl ScalarUDFImpl for ToCharUDF {
    udf_boilerplate!("to_char", DataType::Utf8View);

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let [ts, fmt] = args_n::<2>(&args.args, "to_char", ": timestamp and format string")?;
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

/// A raw tick plus its array's ticks-per-second as a UTC instant.
fn tick_to_utc(tick: i64, per_sec: i64) -> datafusion::error::Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_micros(tick / (per_sec / 1_000_000)).ok_or_else(|| DataFusionError::Execution("Invalid timestamp".to_string()))
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
    let out: StringViewArray =
        ticks.map(|v| v.map(|t| tick_to_utc(t, per_sec).map(|dt| render_pg_format(&parts, &dt))).transpose()).collect::<datafusion::error::Result<_>>()?;
    Ok(Arc::new(out))
}

/// One segment of a parsed Postgres format string. Most tokens collapse to a
/// `Chrono` spec; `PgD` / `PgDY` have no exact chrono equivalent.
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
/// **Deliberate divergences from real Postgres:** `Month`/`Day` are unpadded (PG
/// pads to 9 chars); token matching is case-sensitive; unterminated `"..."`
/// literals are accepted rather than an error. `HH` aliases `HH12` — do NOT
/// "fix" it to `%H`, Postgres `HH` is *not* `HH24`.
///
/// Unsupported tokens (`Q`, `WW`, `IW`, `CC`, `J`, `OF`, `TZH`, `TZM`, …) pass
/// through as literal text; add them to `TOKENS` when a caller needs them.
fn parse_pg_format(pg_format: &str) -> Vec<FmtPart> {
    // ORDER IS LOAD-BEARING: every entry must come before any entry that is one of its
    // prefixes (YYYY before YY, HH24/HH12 before HH, Month before Mon before MM). The
    // loop below uses linear `find`, so a misordering silently matches the shorter
    // token. `D` and `DY` are handled in the loop as PgD / PgDY, not here.
    const TOKENS: &[(&str, &str)] = &[
        ("YYYY", "%Y"),
        ("YY", "%y"),
        ("Month", "%B"),
        ("Mon", "%b"),
        ("MM", "%m"),
        ("DD", "%d"),
        ("Day", "%A"),
        ("Dy", "%a"),
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
        ("am", "%P"),
        ("pm", "%P"),
    ];

    // All token keys are ASCII so byte-prefix matching is sound, but the pass-through
    // path must walk UTF-8 char boundaries or multi-byte chars produce mojibake.
    let bytes = pg_format.as_bytes();
    let mut parts: Vec<FmtPart> = Vec::new();
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
        // Postgres consumes `DY` greedily, so `DYY` is `DY` + leftover `Y`.
        if bytes[i..].starts_with(b"DY") {
            flush(&mut parts, &mut buf);
            parts.push(FmtPart::PgDY);
            i += 2;
            continue;
        }
        // Bare `D` only: the alphanumeric guard stops `D<alnum>` (`Day`, `Dy`, `DD`, or a
        // future `D1` token) being consumed here before its own rule gets a chance.
        if bytes[i] == b'D' && !bytes.get(i + 1).is_some_and(|b| b.is_ascii_alphanumeric()) {
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

udf_struct!(AtTimeZoneUDF, Signature::any(2, Volatility::Immutable));

impl ScalarUDFImpl for AtTimeZoneUDF {
    udf_boilerplate!("at_time_zone");

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        match &arg_types[0] {
            DataType::Timestamp(unit, _) => Ok(DataType::Timestamp(*unit, None)),
            _ => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let [ts, tz] = args_n::<2>(&args.args, "AT TIME ZONE", ": timestamp and timezone")?;
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
        let dt = tick_to_utc(v, per_sec)?;
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

udf_struct!(JsonBuildArrayUDF, Signature::variadic_any(Volatility::Immutable));

impl ScalarUDFImpl for JsonBuildArrayUDF {
    udf_boilerplate!("json_build_array", DataType::Utf8View);

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let num_rows = args.number_rows;
        let args = args.args;
        // Convert each argument column ONCE up front; converting inside the row loop
        // below would be O(rows² × args).
        let cols = args.iter().map(|arg| array_to_json_values(&as_array(arg)?)).collect::<datafusion::error::Result<Vec<_>>>()?;

        let out = StringViewArray::from_iter_values((0..num_rows).map(|row_idx| {
            // len-1 columns are broadcast scalars
            let row: Vec<JsonValue> = cols.iter().map(|c| c[if c.len() == 1 { 0 } else { row_idx }].clone()).collect();
            JsonValue::Array(row).to_string()
        }));
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

udf_struct!(
    /// PG's `to_json`, aliased as `row_to_json(record)`.
    ToJsonUDF,
    Signature::any(1, Volatility::Immutable),
    aliases: vec!["row_to_json".to_string()]
);

impl ScalarUDFImpl for ToJsonUDF {
    udf_boilerplate!("to_json", DataType::Utf8View, aliases);

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let [arg] = args_n::<1>(&args.args, "to_json", "")?;
        let out = StringViewArray::from_iter_values(array_to_json_values(&as_array(arg)?)?.iter().map(JsonValue::to_string));
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

// JSONB-tagged wrappers around the JSON UDFs. Output stays Utf8View, but the
// returned Field carries `tf.pg_type = jsonb`, which vendor/arrow-pg turns into
// PG OID 3802 plus the leading 0x01 binary jsonb version byte.
fn jsonb_tagged_field() -> FieldRef {
    with_meta(&Field::new("", DataType::Utf8View, true), "tf.pg_type", "jsonb")
}

macro_rules! jsonb_wrapper {
    ($wrap:ident, $inner:ident, $pg_name:expr) => {
        #[derive(Debug, Hash, Eq, PartialEq, Default)]
        struct $wrap {
            inner: $inner,
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

udf_struct!(ExtractEpochUDF, Signature::any(1, Volatility::Immutable));

impl ScalarUDFImpl for ExtractEpochUDF {
    udf_boilerplate!("extract_epoch", DataType::Float64);

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let [arg] = args_n::<1>(&args.args, "extract_epoch", "")?;
        let array = as_array(arg)?;
        // Divide in the array's own unit so nanosecond inputs keep sub-µs precision.
        let (ticks, per_sec) = timestamp_ticks(&array, "extract_epoch argument")?;
        let secs: Float64Array = ticks.map(|v| v.map(|t| t as f64 / per_sec as f64)).collect();
        Ok(ColumnarValue::Array(Arc::new(secs)))
    }
}

/// Map each element of a primitive Arrow array to `json!(value)`, nulls to `JsonValue::Null`.
macro_rules! json_primitives {
    ($array:expr, $ty:ty) => {
        json_primitives!($array, $ty, |x| json!(x))
    };
    ($array:expr, $ty:ty, $convert:expr) => {{
        let arr: &$ty = downcast($array, concat!("Failed to downcast to ", stringify!($ty)))?;
        arr.iter().map(|v| v.map_or(JsonValue::Null, $convert)).collect()
    }};
}

// JSON has no non-finite numbers. PostgreSQL retains them as strings, unlike
// serde_json's default float serializer, which converts them to null.
macro_rules! json_floats {
    ($array:expr, $ty:ty) => {
        json_primitives!($array, $ty, |x| {
            if x.is_nan() {
                json!("NaN")
            } else if x.is_infinite() {
                json!(if x.is_sign_positive() { "Infinity" } else { "-Infinity" })
            } else {
                json!(x)
            }
        })
    };
}

/// Convert Arrow array to JSON values
fn array_to_json_values(array: &ArrayRef) -> datafusion::error::Result<Vec<JsonValue>> {
    array_to_json_values_inner(array, true)
}

/// `sniff_json` parses Utf8 values that look like JSON into real JSON. PG parity
/// wants it only at the top level: list elements must stay JSON strings
/// (`to_jsonb(text[])`), so list/struct recursion always passes `false`.
fn array_to_json_values_inner(array: &ArrayRef, sniff_json: bool) -> datafusion::error::Result<Vec<JsonValue>> {
    Ok(match array.data_type() {
        DataType::Utf8View => {
            let strs: &StringViewArray = downcast(array, "Failed to downcast to StringViewArray")?;
            let looks_json = |s: &str| (s.starts_with('{') && s.ends_with('}')) || (s.starts_with('[') && s.ends_with(']'));
            strs.iter()
                .map(|v| match v {
                    None => JsonValue::Null,
                    Some(s) if sniff_json && looks_json(s) => serde_json::from_str(s).unwrap_or_else(|_| JsonValue::String(s.to_string())),
                    Some(s) => JsonValue::String(s.to_string()),
                })
                .collect()
        }
        DataType::Int8 => json_primitives!(array, Int8Array),
        DataType::Int16 => json_primitives!(array, Int16Array),
        DataType::Int32 => json_primitives!(array, Int32Array),
        DataType::Int64 => json_primitives!(array, Int64Array),
        DataType::UInt8 => json_primitives!(array, UInt8Array),
        DataType::UInt16 => json_primitives!(array, UInt16Array),
        DataType::UInt32 => json_primitives!(array, UInt32Array),
        DataType::UInt64 => json_primitives!(array, UInt64Array),
        DataType::Float32 => json_floats!(array, Float32Array),
        DataType::Float64 => json_floats!(array, Float64Array),
        DataType::Boolean => json_primitives!(array, BooleanArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let ts: &TimestampMicrosecondArray = downcast(array, "Failed to downcast to TimestampMicrosecondArray")?;
            ts.iter()
                .map(|v| match v {
                    None => Ok(JsonValue::Null),
                    Some(us) => DateTime::<Utc>::from_timestamp_micros(us)
                        .map(|dt| JsonValue::String(dt.to_rfc3339()))
                        .ok_or_else(|| DataFusionError::Execution("Invalid timestamp".to_string())),
                })
                .collect::<datafusion::error::Result<_>>()?
        }
        // A record renders as a JSON object keyed by field name — PG's `row_to_json(t)`.
        DataType::Struct(fields) => {
            let columns: &datafusion::arrow::array::StructArray = downcast(array, "Failed to downcast to StructArray")?;
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
    let list_array: &datafusion::arrow::array::GenericListArray<O> = downcast(array, "Failed to downcast to list array")?;
    // Always sniff_json=false: PG's to_jsonb(text[]) keeps elements as JSON strings.
    (0..list_array.len())
        .map(|i| if list_array.is_null(i) { Ok(JsonValue::Null) } else { array_to_json_values_inner(&list_array.value(i), false).map(JsonValue::Array) })
        .collect()
}

udf_struct!(
    /// TimescaleDB's `time_bucket`, accepting BOTH spellings of the bucket width:
    /// a string (`time_bucket('5 minutes', ts)`) and an INTERVAL.
    TimeBucketUDF,
    {
        let ts = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
        Signature::one_of(
            vec![TypeSignature::Exact(vec![DataType::Utf8View, ts.clone()]), TypeSignature::Exact(vec![DataType::Interval(IntervalUnit::MonthDayNano), ts])],
            Volatility::Immutable,
        )
    }
);

impl ScalarUDFImpl for TimeBucketUDF {
    udf_boilerplate!("time_bucket", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))));

    fn output_ordering(&self, inputs: &[ExprProperties]) -> datafusion::error::Result<SortProperties> {
        // Like date_bin, a constant positive width preserves timestamp ordering.
        // A row-dependent width does not establish an ordering of the buckets.
        Ok(match inputs {
            [width, timestamp] if width.sort_properties == SortProperties::Singleton => timestamp.sort_properties,
            _ => SortProperties::Unordered,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let [width, ts] = args_n::<2>(&args.args, "time_bucket", ": interval and timestamp")?;
        let micros = match width {
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(i))) => interval_to_micros(i.months, i.days, i.nanoseconds)?,
            _ => parse_interval_to_micros(&extract_scalar_string(width, "Interval")?)?,
        };
        // floor(timestamp / bucket_size) * bucket_size, in the array's own unit.
        let bucketed = map_timestamps(&as_array(ts)?, Some("UTC"), "Argument", |v, per_sec| {
            let size =
                micros.checked_mul(per_sec / 1_000_000).ok_or_else(|| DataFusionError::Execution("time_bucket width overflows the timestamp unit".into()))?;
            v.div_euclid(size).checked_mul(size).ok_or_else(|| DataFusionError::Execution("time_bucket result is outside the timestamp range".into()))
        })?;
        Ok(ColumnarValue::Array(bucketed))
    }
}

/// Width of an `INTERVAL` bucket in microseconds.
///
/// Months are REJECTED, not approximated: a month is 28-31 days, so folding it to
/// a fixed width would silently mis-bucket rows. Days and nanoseconds are exact.
pub(crate) fn interval_to_micros(months: i32, days: i32, nanoseconds: i64) -> datafusion::error::Result<i64> {
    if months != 0 {
        return Err(DataFusionError::Execution(
            "time_bucket does not support month or year intervals (a month is not a fixed width); use days or smaller, e.g. INTERVAL '30 days'".to_string(),
        ));
    }
    i64::from(days)
        .checked_mul(86_400_000_000)
        .and_then(|d| d.checked_add(nanoseconds / 1_000))
        .filter(|m| *m > 0)
        .ok_or_else(|| DataFusionError::Execution("time_bucket interval must be a positive, representable width".to_string()))
}

/// Parse interval string to microseconds
pub(crate) fn parse_interval_to_micros(interval_str: &str) -> datafusion::error::Result<i64> {
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
    value
        .checked_mul(micros_per_unit)
        .filter(|width| *width > 0)
        .ok_or_else(|| DataFusionError::Execution(format!("Interval '{interval_str}' must be positive and representable")))
}

/// A UDAF whose partial state is exactly its output: one serialized Binary sketch.
fn binary_state_udaf(name: &str, input: DataType, factory: datafusion::logical_expr::function::AccumulatorFactoryFunction) -> AggregateUDF {
    create_udaf(name, vec![input], Arc::new(DataType::Binary), Volatility::Immutable, factory, Arc::new(vec![DataType::Binary]))
}

/// A mergeable sketch whose serialized form IS the aggregate's partial state.
/// `encode`/`heap_size` are named apart from the inherent `to_bytes`/`size` an
/// implementor may already have, so the trait never shadows them.
trait Sketch: Default + std::fmt::Debug + Send + Sync + 'static {
    /// Message when the `*_merge` state column is not Binary.
    const MERGE_ERR: &'static str;
    /// Hash/digest one array's non-null values.
    fn insert_array(&mut self, array: &ArrayRef) -> datafusion::error::Result<()>;
    /// Fold one serialized sketch in.
    fn merge_bytes(&mut self, bytes: &[u8]) -> datafusion::error::Result<()>;
    fn encode(&self) -> datafusion::error::Result<Vec<u8>>;
    fn heap_size(&self) -> usize;
}

/// Backs every sketch UDAF pair: `*_agg` digests raw values, `*_merge` folds
/// already-serialized sketches.
#[derive(Debug, Default)]
struct SketchAccumulator<S: Sketch> {
    sketch: S,
    merging: bool,
}

impl<S: Sketch> SketchAccumulator<S> {
    fn merging() -> Box<dyn Accumulator> {
        Box::new(Self { sketch: S::default(), merging: true })
    }

    fn fold_states(&mut self, arrays: &[ArrayRef]) -> datafusion::error::Result<()> {
        let Some(array) = arrays.first() else { return Ok(()) };
        let binary: &BinaryArray = downcast(array, S::MERGE_ERR)?;
        binary.iter().flatten().try_for_each(|bytes| self.sketch.merge_bytes(bytes))
    }
}

impl<S: Sketch> Accumulator for SketchAccumulator<S> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::error::Result<()> {
        match values.first() {
            Some(array) if !self.merging => self.sketch.insert_array(array),
            Some(_) => self.fold_states(values),
            None => Ok(()),
        }
    }

    fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.sketch.encode()?)))
    }

    fn size(&self) -> usize {
        self.sketch.heap_size()
    }

    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        self.evaluate().map(|v| vec![v])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        self.fold_states(states)
    }
}

const TDIGEST_MAX_CENTROIDS: usize = 200;

/// Bounded, mergeable t-digest state. Its binary representation contains
/// centroids, never the raw input values.
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
        let mut merged = self.digest.as_ref().map_or_else(|| digest.clone(), |current| current.merge(digest));
        merged.compress(TDIGEST_MAX_CENTROIDS);
        self.digest = Some(merged);
    }

    fn to_bytes(&self) -> datafusion::error::Result<Vec<u8>> {
        let centroids: Vec<(f64, f64)> = self.digest.iter().flat_map(|d| d.centroids().iter().map(|c| (c.mean, c.weight))).collect();
        // Never swallow the encode failure: an empty payload decodes as an empty digest.
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
}

/// `percentile_agg` digests raw Float64 values; `tdigest_merge` folds serialized digests.
impl Sketch for TDigestWrapper {
    const MERGE_ERR: &'static str = "tdigest_merge expects Binary values";

    fn insert_array(&mut self, array: &ArrayRef) -> datafusion::error::Result<()> {
        let floats: &Float64Array = downcast(array, "percentile_agg expects Float64 values")?;
        self.insert_batch(floats.iter().flatten());
        Ok(())
    }

    fn merge_bytes(&mut self, bytes: &[u8]) -> datafusion::error::Result<()> {
        self.merge(&Self::from_bytes(bytes)?);
        Ok(())
    }

    fn encode(&self) -> datafusion::error::Result<Vec<u8>> {
        self.to_bytes()
    }

    fn heap_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.digest.as_ref().map_or(0, |digest| std::mem::size_of_val(digest.centroids()))
    }
}

udf_struct!(
    /// `approx_percentile(pct, digest)`: extracts a percentile from a t-digest.
    ApproxPercentileUDF,
    Signature::new(TypeSignature::Exact(vec![DataType::Float64, DataType::Binary]), Volatility::Immutable)
);

impl ScalarUDFImpl for ApproxPercentileUDF {
    udf_boilerplate!("approx_percentile", DataType::Float64);

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let [pct_arg, digest_arg] = args_n::<2>(&args.args, "approx_percentile", ": percentile and t-digest")?;
        // Result size follows the digest column (which comes from GROUP BY).
        let num_rows = if let ColumnarValue::Array(array) = digest_arg { array.len() } else { 1 };
        let percentile_array = pct_arg.to_array(num_rows)?;
        let digest_array = digest_arg.to_array(num_rows)?;

        let percentiles: &Float64Array = downcast(&percentile_array, "First argument must be a percentile (Float64)")?;
        let digests: &BinaryArray = downcast(&digest_array, "Second argument must be a t-digest (Binary)")?;

        // None → SQL NULL (null input, or a digest that saw no values).
        let out: Float64Array = percentiles
            .iter()
            .zip(digests.iter())
            .map(|(pct, bytes)| match (pct, bytes) {
                (Some(pct), Some(bytes)) => {
                    if !(0.0..=1.0).contains(&pct) {
                        return Err(DataFusionError::Execution(format!("Percentile must be between 0 and 1, got {pct}")));
                    }
                    Ok(TDigestWrapper::from_bytes(bytes)?.digest.map(|d| d.estimate_quantile(pct)))
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
// `approx_percentile`: unlike DataFusion's `approx_distinct`, the sketch itself
// is storable, so a rollup measure can carry it and fold it later.

/// `hll_agg` hashes raw values; `hll_merge` folds stored sketches.
impl Sketch for crate::read::Hll {
    const MERGE_ERR: &'static str = "hll_merge expects Binary values";

    /// Strings and binaries are hashed in place; anything else is cast to
    /// Utf8View first, which preserves distinctness for every primitive type.
    fn insert_array(&mut self, array: &ArrayRef) -> datafusion::error::Result<()> {
        macro_rules! feed {
            ($ty:ty) => {{
                let typed: &$ty = downcast(array, "hll_agg: array does not match its own data type")?;
                typed.iter().flatten().for_each(|value| self.insert_hash(crate::read::hash_bytes(AsRef::<[u8]>::as_ref(&value))));
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
            _ => self.insert_array(&datafusion::arrow::compute::cast(array, &DataType::Utf8View)?),
        }
    }

    fn merge_bytes(&mut self, bytes: &[u8]) -> datafusion::error::Result<()> {
        self.merge(&Self::from_bytes(bytes).map_err(DataFusionError::Execution)?);
        Ok(())
    }

    fn encode(&self) -> datafusion::error::Result<Vec<u8>> {
        Ok(self.to_bytes())
    }

    fn heap_size(&self) -> usize {
        self.size()
    }
}

udf_struct!(
    /// `hll_agg(any) -> Binary`, also spelled `approx_count_distinct`. Returns a
    /// SKETCH, not a count — read it with `hll_count`/`distinct_count`, matching
    /// Timescale Toolkit's split. The signature is deliberately untyped so a
    /// distinct count over any column type plans as a cast, not an error.
    HllAggUDF,
    Signature::any(1, Volatility::Immutable),
    aliases: vec!["approx_count_distinct".to_string()]
);

impl datafusion::logical_expr::AggregateUDFImpl for HllAggUDF {
    udf_boilerplate!("hll_agg");

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn state_fields(&self, _args: datafusion::logical_expr::function::StateFieldsArgs) -> datafusion::error::Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new("sketch", DataType::Binary, true))])
    }

    fn accumulator(&self, _acc_args: datafusion::logical_expr::function::AccumulatorArgs) -> datafusion::error::Result<Box<dyn Accumulator>> {
        Ok(Box::new(SketchAccumulator::<crate::read::Hll>::default()))
    }
}

/// `hll_count(Binary) -> Int64`, also spelled `distinct_count`. Int64 to match
/// PG `bigint`.
fn create_hll_count_udf() -> ScalarUDF {
    create_udf(
        "hll_count",
        vec![DataType::Binary],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let array = as_array(args.first().ok_or_else(|| DataFusionError::Execution("hll_count requires one argument".to_string()))?)?;
            let binary: &BinaryArray = downcast(&array, "hll_count expects a Binary sketch")?;
            // NULL in, NULL out: no sketch is not the same claim as zero distinct values.
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

/// `hash_bucket(text, n)` — a stable, evenly-spread bucket in `[0, n)`, using the
/// same non-cryptographic mixer as the HLL sketches.
///
/// NULL hashes as the empty string rather than to NULL: a NULL bucket satisfies
/// neither `>= lo` nor `< hi`, so such a row would fall out of every shard.
pub fn hash_bucket_udf() -> ScalarUDF {
    create_udf(
        "hash_bucket",
        vec![DataType::Utf8View, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let [value, buckets] = args_n::<2>(args, "hash_bucket", ": value and bucket count")?;
            let ColumnarValue::Scalar(count) = buckets else {
                return Err(DataFusionError::Execution("hash_bucket's bucket count must be a literal".to_string()));
            };
            // A zero count would divide by zero; a negative one is a caller bug.
            let buckets = i64::try_from(count.clone())
                .ok()
                .and_then(|n| u64::try_from(n).ok())
                .filter(|n| *n > 0)
                .ok_or_else(|| DataFusionError::Execution("hash_bucket's bucket count must be positive".to_string()))?;
            let array = as_array(value)?;
            let strings: &StringViewArray = downcast(&array, format!("hash_bucket expects a Utf8View value, got {}", array.data_type()))?;
            let buckets: Int64Array =
                strings.iter().map(|value| Some((crate::read::hash_bytes(value.unwrap_or_default().as_bytes()) % buckets) as i64)).collect();
            Ok(ColumnarValue::Array(Arc::new(buckets)))
        }) as ScalarFunctionImplementation,
    )
}

#[cfg(test)]
mod hash_bucket_tests {
    use datafusion::prelude::SessionContext;

    /// Runs `sql` with `hash_bucket` registered, returning the integer cells of each
    /// rendered data row plus the raw render.
    async fn int_rows(sql: &str) -> (Vec<Vec<i64>>, String) {
        let ctx = SessionContext::new();
        ctx.register_udf(super::hash_bucket_udf());
        let batches = ctx.sql(sql).await.expect("plan").collect().await.expect("run");
        let rendered = datafusion::arrow::util::pretty::pretty_format_batches(&batches).expect("format").to_string();
        let rows = rendered
            .lines()
            .map(|line| line.split('|').filter_map(|cell| cell.trim().parse::<i64>().ok()).collect::<Vec<i64>>())
            .filter(|cells| !cells.is_empty())
            .collect();
        (rows, rendered)
    }

    /// Bucketing must PARTITION: every row lands in exactly one bucket of `[0, n)`,
    /// and equal keys always land together.
    #[tokio::test]
    async fn hash_bucket_partitions_and_keeps_equal_keys_together() {
        // In range, and the same input always gives the same bucket.
        let (rows, rendered) = int_rows("SELECT hash_bucket(arrow_cast(v, 'Utf8View'), 256) AS b FROM (VALUES ('a'), ('a'), ('b')) AS t(v)").await;
        let buckets: Vec<i64> = rows.iter().map(|cells| cells[0]).collect();
        assert_eq!(buckets.len(), 3, "three rows: {rendered}");
        assert!(buckets.iter().all(|b| (0..256).contains(b)), "every bucket in range: {buckets:?}");
        assert_eq!(buckets[0], buckets[1], "equal keys must share a bucket");
        // NULL must not vanish: it buckets as the empty string, not as NULL.
        let (_, rendered) = int_rows("SELECT hash_bucket(arrow_cast(NULL, 'Utf8View'), 256) AS b").await;
        assert!(!rendered.contains("NULL"), "NULL must bucket, not propagate: {rendered}");
    }

    /// Every key must land inside `[0, n)` and the spread must reach every bucket —
    /// a row outside every shard's range is a row the sharded rewrite never sees.
    #[tokio::test]
    async fn hash_bucket_spreads_evenly_enough_to_shard_on() {
        // The one data row renders as `| n | distinct | lo | hi |`.
        let (rows, rendered) = int_rows(
            "SELECT count(*) AS n, count(DISTINCT hash_bucket(arrow_cast(v, 'Utf8View'), 256)) AS distinct_buckets, \
             min(hash_bucket(arrow_cast(v, 'Utf8View'), 256)) AS lo, max(hash_bucket(arrow_cast(v, 'Utf8View'), 256)) AS hi \
             FROM (SELECT CAST(i AS VARCHAR) AS v FROM generate_series(1, 5000) AS t(i))",
        )
        .await;
        let row = rows.iter().find(|cells| cells.len() == 4).unwrap_or_else(|| panic!("one four-column data row: {rendered}"));
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

    /// 200k rows forces the partial/final split, so this also pins that the sketch
    /// state survives repartitioning.
    #[tokio::test]
    async fn hll_count_of_hll_agg_matches_the_true_cardinality() {
        for (n, tolerance) in [(1u64, 0.0), (500, 0.0), (5_000, 0.05), (200_000, 0.05)] {
            let estimate = scalar(&format!("SELECT hll_count(hll_agg(v)) FROM (SELECT value % {n} AS v FROM generate_series(1, 200000) t(value))")).await;
            let error = (estimate as f64 - n as f64).abs() / n as f64;
            assert!(error <= tolerance, "n={n}: estimated {estimate}, error {:.2}% > {:.0}%", error * 100.0, tolerance * 100.0);
        }
    }

    /// The rollup property: sketches built per group and folded afterwards must
    /// agree with one built over everything at once, in both spellings.
    #[tokio::test]
    async fn merging_per_bucket_sketches_equals_one_pass_and_the_toolkit_spelling() {
        let rows = "SELECT value % 30000 AS v, value % 7 AS bucket FROM generate_series(1, 300000) t(value)";
        let merged = scalar(&format!("SELECT hll_count(hll_merge(s)) FROM (SELECT hll_agg(v) AS s FROM ({rows}) GROUP BY bucket)")).await;
        let one_pass = scalar(&format!("SELECT hll_count(hll_agg(v)) FROM ({rows})")).await;
        let toolkit = scalar(&format!("SELECT distinct_count(approx_count_distinct(v)) FROM ({rows})")).await;
        assert_eq!(merged, one_pass, "folding per-bucket states must equal a single pass");
        assert_eq!(toolkit, one_pass, "the rewrite must not change the answer");
        let error = (one_pass as f64 - 30_000.0).abs() / 30_000.0;
        assert!(error < 0.05, "estimated {one_pass}, want ~30000");
    }

    /// Distinct counts are asked of every column type, and NULL is not a value.
    /// The `distinct_count(approx_count_distinct(…))` cases pin Timescale Toolkit
    /// parity, so one query text runs on both backends.
    #[test_case::test_case("SELECT hll_count(hll_agg(v)) FROM (VALUES (1),(2),(2),(NULL)) t(v)" => 2 ; "ints, and NULL is not a value")]
    #[test_case::test_case("SELECT hll_count(hll_agg(v)) FROM (VALUES (1.5),(2.5),(1.5)) t(v)" => 2 ; "floats")]
    #[test_case::test_case("SELECT hll_count(hll_agg(v)) FROM (VALUES ('a'),('b'),('a')) t(v)" => 2 ; "strings")]
    #[test_case::test_case("SELECT hll_count(hll_agg(v)) FROM (VALUES (arrow_cast(1, 'Timestamp(Microsecond, None)'))) t(v)" => 1 ; "timestamps")]
    #[test_case::test_case("SELECT distinct_count(approx_count_distinct(v)) FROM (VALUES (1),(2),(2),(NULL)) t(v)" => 2 ; "the timescale toolkit spelling runs unchanged")]
    #[test_case::test_case(
        "SELECT distinct_count(approx_count_distinct(v) FILTER (WHERE v IS NOT NULL))::BIGINT FROM (VALUES (1),(2),(2),(NULL)) t(v)" => 2
        ; "the filtered form the session query sends runs here too"
    )]
    #[tokio::test]
    async fn an_exact_distinct_count(sql: &str) -> u64 {
        scalar(sql).await
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

// Accept a Variant struct or a JSON string as first arg, a path string as second.
udf_struct!(JsonbPathExistsUDF, Signature::any(2, Volatility::Immutable));

impl ScalarUDFImpl for JsonbPathExistsUDF {
    udf_boilerplate!("jsonb_path_exists");

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let (json_array, path_str, json_path) = json_path_args(&args.args, "jsonb_path_exists")?;
        if is_variant_type(json_array.data_type())
            && let Some(fast) = variant_get_exists(&json_array, &path_str)?
        {
            return Ok(ColumnarValue::Array(fast));
        }
        // Lax mode (PG default): an eval error or unparseable JSON is no match, not a failure.
        json_path_eval::<bool, BooleanArray>(&json_array, "jsonb_path_exists", |json| Some(json.is_some_and(|json| json_path.exists(json).unwrap_or(false))))
    }
}

/// `(json_array, raw_path, compiled_path)` for the two `jsonb_path_*` UDFs. The
/// path must be a scalar: it compiles once per invocation, not once per row.
fn json_path_args(args: &[ColumnarValue], name: &str) -> datafusion::error::Result<(ArrayRef, String, sql_json_path::JsonPath)> {
    let [json, path] = args_n::<2>(args, name, ": json/variant and jsonpath")?;
    let ColumnarValue::Scalar(scalar) = path else {
        return Err(DataFusionError::Execution("JSONPath must be a scalar string".to_string()));
    };
    let path_str = extract_utf8_string(scalar).ok_or_else(|| DataFusionError::Execution("JSONPath must be a string".to_string()))?;
    let json_path = sql_json_path::JsonPath::new(&path_str).map_err(|e| DataFusionError::Execution(format!("Invalid JSONPath: {e}")))?;
    Ok((as_array(json)?, path_str, json_path))
}

/// Run a compiled JSONPath over either a Variant-struct or a JSON-string column,
/// keeping NULL rows NULL. `f` sees `None` for a JSON-string row that failed to
/// parse — the two UDFs disagree on what that means (no match vs. false).
fn json_path_eval<T, A: Array + FromIterator<Option<T>> + 'static>(
    array: &ArrayRef, name: &str, f: impl Fn(Option<&JsonValue>) -> Option<T>,
) -> datafusion::error::Result<ColumnarValue> {
    let out: A = if is_variant_type(array.data_type()) {
        map_variant_rows(array, |json| f(Some(json)))?
    } else {
        map_utf8_rows(array, name, |s| f(serde_json::from_str::<JsonValue>(s).ok().as_ref()))?
    };
    Ok(ColumnarValue::Array(Arc::new(out)))
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

/// Row accessor reading bytes from either `BinaryArray` or `BinaryViewArray` —
/// Parquet yields either depending on `schema_force_view_types`.
fn binary_values<'a>(col: &'a ArrayRef, field: &str) -> datafusion::error::Result<Box<dyn Fn(usize) -> &'a [u8] + 'a>> {
    if let Some(a) = col.as_any().downcast_ref::<BinaryArray>() {
        Ok(Box::new(move |i| a.value(i)))
    } else if let Some(a) = col.as_any().downcast_ref::<BinaryViewArray>() {
        Ok(Box::new(move |i| a.value(i)))
    } else {
        Err(DataFusionError::Execution(format!("Variant {field} column is not Binary or BinaryView (got {:?})", col.data_type())))
    }
}

/// Fast lane for `jsonb_path_exists` on a Variant (Struct) array: simple
/// `$.a.b.c[N].d` paths use the vectorized `variant_get` kernel, which walks the
/// Variant binary without materializing a JsonValue. Returns `None` for anything
/// that is not a simple path, so the caller takes the JsonValue fallback.
///
/// Parity caveat: `variant_get` resolves like PG *strict* mode — no lax
/// auto-unwrapping (`.a` on an array, `[i]` on a scalar) — so a filter-free path
/// over an array-shaped value can be a false negative versus the lax fallback.
fn variant_get_exists(array: &ArrayRef, raw_path: &str) -> datafusion::error::Result<Option<ArrayRef>> {
    use parquet_variant_compute::{GetOptions, variant_get};
    let Some(variant_path) = simple_path_to_variant_path(raw_path) else { return Ok(None) };
    let extracted = variant_get(array, GetOptions::new_with_path(variant_path)).map_err(|e| DataFusionError::Execution(format!("variant_get failed: {e}")))?;
    // `extracted.is_null(i)` cannot distinguish a NULL input row from a missing path,
    // so gate on the input's null buffer: NULL in → NULL out.
    let out: BooleanArray = (0..extracted.len()).map(|i| (!array.is_null(i)).then(|| !extracted.is_null(i))).collect();
    Ok(Some(Arc::new(out)))
}

/// Decode each row of a Variant struct array into a `JsonValue` and map it
/// through `f`; NULL rows stay NULL without being decoded.
fn map_variant_rows<T, A: FromIterator<Option<T>>>(array: &ArrayRef, f: impl Fn(&JsonValue) -> Option<T>) -> datafusion::error::Result<A> {
    use datafusion::arrow::array::StructArray;
    let struct_array = array.as_any().downcast_ref::<StructArray>().ok_or_else(|| DataFusionError::Execution("Expected Variant struct array".to_string()))?;
    let missing = |name: &str| DataFusionError::Execution(format!("Variant missing {name} column"));
    let metadata_binary = binary_values(struct_array.column_by_name("metadata").ok_or_else(|| missing("metadata"))?, "metadata")?;
    let value_binary = binary_values(struct_array.column_by_name("value").ok_or_else(|| missing("value"))?, "value")?;
    (0..struct_array.len())
        .map(|i| {
            (!struct_array.is_null(i))
                .then(|| variant_to_serde_json(&parquet_variant::Variant::new(metadata_binary(i), value_binary(i)), 0).map(|json| f(&json)))
                .transpose()
                .map(Option::flatten)
        })
        .collect()
}

/// Convert a simple JSONPath (`$.a.b[0].c`) to a `parquet_variant::VariantPath`.
/// Returns `None` for any path that uses filters, recursive descent, slices,
/// wildcards, or other features that don't map to direct field/index access —
/// those fall back to the slow JsonValue path.
fn simple_path_to_variant_path(raw: &str) -> Option<parquet_variant::VariantPath<'_>> {
    use parquet_variant::{VariantPath, VariantPathElement};
    let mut rest = raw.strip_prefix('$').unwrap_or(raw);
    let mut elements: Vec<VariantPathElement> = Vec::new();
    while !rest.is_empty() {
        rest = match rest.as_bytes()[0] {
            b'.' => {
                let body = &rest[1..];
                let (name, tail) = body.split_at(body.find(['.', '[']).unwrap_or(body.len()));
                if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                    return None;
                }
                elements.push(VariantPathElement::field(std::borrow::Cow::Borrowed(name)));
                tail
            }
            b'[' => {
                let (digits, tail) = rest[1..].split_once(']')?;
                if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                elements.push(VariantPathElement::index(digits.parse().ok()?));
                tail
            }
            _ => return None,
        };
    }
    Some(VariantPath::new(elements))
}

// ============================================================================
// jsonb_path_query_first: the matched VALUE, where jsonb_path_exists returns
// only whether one existed.
// ============================================================================

udf_struct!(JsonbPathQueryFirstUDF, Signature::any(2, Volatility::Immutable));

impl ScalarUDFImpl for JsonbPathQueryFirstUDF {
    udf_boilerplate!("jsonb_path_query_first");

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8View)
    }

    // Tagged jsonb: the tag is what makes `#>> '{}'` on the result unwrap the document to text.
    fn return_field_from_args(&self, _: datafusion::logical_expr::ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        Ok(jsonb_tagged_field())
    }

    /// First match as JSON text, or NULL. No `variant_get` fast lane here: filtered
    /// paths are never simple paths, and it would repeat the strict/lax hazard
    /// documented on `variant_get_exists`.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let (json_array, _, json_path) = json_path_args(&args.args, "jsonb_path_query_first")?;
        // Lax mode (PG default): an eval error is no match, not a query failure.
        json_path_eval::<String, StringViewArray>(&json_array, "jsonb_path_query_first", |json| {
            json.and_then(|json| json_path.query_first(json).ok().flatten()).map(|found| found.to_string())
        })
    }
}

/// Map each non-NULL row of a Utf8/Utf8View array through `f`, keeping NULLs.
fn map_utf8_rows<T, A: FromIterator<Option<T>>>(array: &ArrayRef, label: &str, f: impl Fn(&str) -> Option<T>) -> datafusion::error::Result<A> {
    let iter: Box<dyn Iterator<Item = Option<&str>>> = if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
        Box::new(a.iter())
    } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        Box::new(a.iter())
    } else {
        return Err(DataFusionError::Execution(format!("{label} requires JSON string or Variant input")));
    };
    Ok(iter.map(|opt| opt.and_then(&f)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The timestamp every `to_char` parity case is captured against.
    const TS: &str = "TIMESTAMP '2026-06-10 08:10:52.422355'";

    /// `::` binds tighter than `->>`, so a cast can land on the PATH literal. Both
    /// spellings must address the same field.
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

    /// A field name is ONE key whatever characters it holds — dot notation cannot
    /// say that, so the encoder brackets and escapes and must round-trip exactly.
    #[test_case::test_case("plain")]
    #[test_case::test_case("http.request.method" ; "dots are part of the OTel key")]
    #[test_case::test_case("a[0]" ; "brackets")]
    #[test_case::test_case("back\\slash")]
    #[test_case::test_case("quo'te")]
    #[test_case::test_case("dq\"uote")]
    #[test_case::test_case("123" ; "digits stay a field, not an index")]
    #[test_case::test_case("")]
    fn a_field_name_round_trips_through_the_variant_path_encoding(name: &str) {
        use parquet_variant::{VariantPath, VariantPathElement};
        let encoded = build_variant_path(&[PathComponent::Field(name.to_string())]);
        assert_eq!(
            VariantPath::try_from(encoded.as_str()).unwrap_or_else(|e| panic!("{name:?} encoded as {encoded:?}: {e}")).as_ref(),
            [VariantPathElement::field(name)],
        );
    }

    #[test]
    fn an_index_and_a_field_compose_into_one_path() {
        let path = build_variant_path(&[PathComponent::Field("items".into()), PathComponent::Index(2), PathComponent::Field("name".into())]);
        assert_eq!(path, "['items'][2]['name']");
        assert_eq!(parquet_variant::VariantPath::try_from(path.as_str()).unwrap().len(), 3);
    }

    /// A t-digest holding `values`.
    fn digest(values: impl IntoIterator<Item = f64>) -> TDigestWrapper {
        let mut wrapper = TDigestWrapper::default();
        wrapper.insert_batch(values);
        wrapper
    }

    #[test]
    fn percentile_agg_state_is_bounded_and_merge_preserves_the_tail() {
        assert!(TDigestWrapper::default().digest.is_none(), "no digest until a batch arrives");
        assert!(digest(vec![10.0, 20.0]).digest.is_some(), "a batch creates the digest");
        assert!(digest((0..100_000).map(|value| value as f64)).to_bytes().unwrap().len() < 10_000, "percentile state must not grow with input rows");

        let mut left = digest((0..50_000).map(|value| value as f64));
        left.merge(&digest((50_000..100_000).map(|value| value as f64)));
        assert!(left.to_bytes().unwrap().len() < 10_000, "a merged state stays bounded too");
        assert!((left.digest.as_ref().unwrap().estimate_quantile(0.95) - 95_000.0).abs() < 1_000.0, "merge preserves the p95 tail estimate");
    }

    #[tokio::test]
    async fn tdigest_merge_merges_serialized_percentile_states() {
        let batches = udf_ctx()
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

    /// A session with the real registered UDFs — the fixture every query test starts from.
    fn udf_ctx() -> datafusion::prelude::SessionContext {
        let mut ctx = datafusion::prelude::SessionContext::new();
        register_custom_functions(&mut ctx).expect("functions register");
        ctx
    }

    /// The one UTF-8 string a query returns, through the real registered UDFs.
    async fn text(sql: &str) -> String {
        let batches = udf_ctx().sql(sql).await.unwrap().collect().await.unwrap();
        batches[0].column(0).as_any().downcast_ref::<StringViewArray>().unwrap().value(0).to_string()
    }

    fn chrono_only(spec: &str) -> Vec<FmtPart> {
        vec![FmtPart::Chrono(spec.to_string())]
    }

    #[test_case::test_case("YYYY-MM-DD" => chrono_only("%Y-%m-%d") ; "date")]
    #[test_case::test_case("YYYY-MM-DD HH24:MI:SS" => chrono_only("%Y-%m-%d %H:%M:%S") ; "date and 24h time")]
    #[test_case::test_case("Day, DD Mon YYYY" => chrono_only("%A, %d %b %Y") ; "names")]
    #[test_case::test_case(r#"YYYY-MM-DD"T"HH24:MI:SS.US"Z""# => chrono_only("%Y-%m-%dT%H:%M:%S.%6fZ") ; "iso 8601 literal escapes")]
    #[test_case::test_case(r#""YYYY=" YYYY"# => chrono_only("YYYY= %Y") ; "tokens inside a literal stay literal")]
    #[test_case::test_case(r#""a""b""# => chrono_only("a\"b") ; "doubled quote inside a literal is an escaped quote")]
    #[test_case::test_case("100%" => chrono_only("100%%") ; "a bare percent is escaped to chrono literal-percent")]
    #[test_case::test_case(r#"YYYY "tail"# => chrono_only("%Y tail") ; "unterminated literal copies the remainder verbatim")]
    // D / DY have no chrono equivalent, so they split the buffer.
    #[test_case::test_case("D" => vec![FmtPart::PgD] ; "pg D alone")]
    #[test_case::test_case("DY" => vec![FmtPart::PgDY] ; "pg DY alone")]
    #[test_case::test_case("YYYY-D" => vec![FmtPart::Chrono("%Y-".to_string()), FmtPart::PgD] ; "pg D splits the chrono buffer")]
    #[test_case::test_case("DY YYYY" => vec![FmtPart::PgDY, FmtPart::Chrono(" %Y".to_string())] ; "pg DY splits the chrono buffer")]
    fn test_parse_pg_format(fmt: &str) -> Vec<FmtPart> {
        parse_pg_format(fmt)
    }

    /// End-to-end `to_char` parity; expected outputs captured from Postgres 16.
    #[test_case::test_case(TS, "YYYY-MM-DD" => "2026-06-10" ; "date")]
    #[test_case::test_case(TS, "YYYY-MM-DD HH24:MI:SS" => "2026-06-10 08:10:52" ; "date and 24h time")]
    #[test_case::test_case(TS, r#"YYYY-MM-DD"T"HH24:MI:SS.US"Z""# => "2026-06-10T08:10:52.422355Z" ; "monoscope iso 8601 micros")]
    #[test_case::test_case(TS, r#"YYYY-MM-DD"T"HH24:MI:SS.MS"Z""# => "2026-06-10T08:10:52.422Z" ; "iso 8601 millis")]
    #[test_case::test_case(TS, "DD/MM/YYYY" => "10/06/2026" ; "day first")]
    #[test_case::test_case(TS, "Mon DD, YYYY" => "Jun 10, 2026" ; "short month name")]
    #[test_case::test_case(TS, "Day, Mon DD YYYY" => "Wednesday, Jun 10 2026" ; "long day name")]
    #[test_case::test_case(TS, "HH12:MI" => "08:10" ; "12h time")]
    #[test_case::test_case(TS, "YY" => "26" ; "two digit year")]
    #[test_case::test_case(TS, r#""YYYY=" YYYY"# => "YYYY= 2026" ; "literal that looks like a token")]
    #[test_case::test_case(TS, r#""· "YYYY"# => "· 2026" ; "non ascii literal survives the utf8 boundary walk")]
    #[test_case::test_case(TS, "HH12:MI AM" => "08:10 AM" ; "am token")]
    #[test_case::test_case(TS, "HH:MI:SS" => "08:10:52" ; "bare HH aliases HH12, 12 hour clock with leading zero")]
    #[test_case::test_case(TS, "HH12:MI am" => "08:10 am" ; "lowercase am token emits lowercase output")]
    #[test_case::test_case(TS, "Dy" => "Wed" ; "abbreviated day name")]
    // 2026-06-10 is a Wednesday: Postgres D=4 (Sun=1), DY="WED".
    #[test_case::test_case(TS, "D" => "4" ; "pg D is 1 based from sunday")]
    #[test_case::test_case(TS, "DY" => "WED" ; "pg DY is upper case")]
    #[test_case::test_case(TS, "DY-D" => "WED-4" ; "DY must beat bare D")]
    #[test_case::test_case("TIMESTAMP '2026-06-10 20:10:52'", "HH12:MI PM" => "08:10 PM" ; "pm token on an afternoon timestamp")]
    #[tokio::test]
    async fn test_to_char_postgres_parity(ts: &str, fmt: &str) -> String {
        text(&format!("SELECT to_char({ts}, '{fmt}') AS s")).await
    }

    /// The rendered text already pins that elements are JSON strings; this pins that
    /// it still *parses back* as an array of strings, not objects/arrays.
    #[tokio::test]
    async fn test_to_jsonb_text_array_elements_stay_strings() {
        let array = text(r#"SELECT to_jsonb(make_array('{"a":1}', '[1,2]', 'plain', '123')) AS s"#).await;
        let parsed: serde_json::Value = serde_json::from_str(&array).unwrap();
        assert!(parsed.as_array().unwrap().iter().all(serde_json::Value::is_string), "elements must stay strings: {parsed}");
    }

    /// LargeList and FixedSizeList must keep list structure and follow the same
    /// no-sniff rule.
    #[test]
    fn test_large_and_fixed_size_list_to_json_values() {
        use datafusion::arrow::array::{FixedSizeListBuilder, GenericListBuilder, StringViewBuilder};
        let elements = [r#"{"a":1}"#, "plain"];
        let expected = vec![serde_json::json!(elements)];

        let mut b = GenericListBuilder::<i64, _>::new(StringViewBuilder::new());
        elements.into_iter().for_each(|value| b.values().append_value(value));
        b.append(true);
        let arr: ArrayRef = Arc::new(b.finish());
        assert_eq!(array_to_json_values(&arr).unwrap(), expected, "LargeList");

        let mut b = FixedSizeListBuilder::new(StringViewBuilder::new(), 2);
        elements.into_iter().for_each(|value| b.values().append_value(value));
        b.append(true);
        let arr: ArrayRef = Arc::new(b.finish());
        assert_eq!(array_to_json_values(&arr).unwrap(), expected, "FixedSizeList");
    }

    /// `json_build_array` must stay linear in rows (not O(rows² × args)) and must
    /// broadcast a scalar arg without clamping the output to one row.
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
        let ColumnarValue::Array(out) = JsonBuildArrayUDF::default().invoke_with_args(args).unwrap() else { panic!("expected array output") };
        assert!(start.elapsed() < std::time::Duration::from_secs(2), "quadratic regression: took {:?}", start.elapsed());
        let out = out.as_any().downcast_ref::<datafusion::arrow::array::StringViewArray>().unwrap();
        assert_eq!(out.len(), n);
        assert_eq!(out.value(7), r#"["tag","id-7",7]"#);
    }

    #[test]
    fn test_parse_interval_to_micros() {
        // Every spelling of one unit — `N unit`, the space-less `Nunit`, and each alias.
        let cases: &[(i64, &[&str])] = &[
            (1_000_000, &["1 second", "1second"]),
            (5_000_000, &["5 seconds", "5seconds", "5s"]),
            (60_000_000, &["1 minute", "1minute"]),
            (300_000_000, &["5 minutes", "5minutes", "5 min", "5min", "5 mins", "5mins", "5 m"]),
            (1_800_000_000, &["30m"]),
            (3_600_000_000, &["1 hour", "1h"]),
            (7_200_000_000, &["2 hours", "2h"]),
            (86_400_000_000, &["1 day", "1d"]),
            (604_800_000_000, &["1 week", "1w"]),
        ];
        for (expected, spellings) in cases {
            for input in *spellings {
                assert_eq!(parse_interval_to_micros(input).unwrap(), *expected, "interval: {input}");
            }
        }
        // No unit, no number, non-numeric value, unit-before-number, and overflow.
        for bad in ["invalid", "5", "abc minutes", "m5", "9223372036854 weeks"] {
            assert!(parse_interval_to_micros(bad).is_err(), "expected error for: {bad}");
        }
    }

    /// PG parity for JSON rendering. `to_jsonb(text[])` yields an array of JSON
    /// *strings* — elements that happen to look like JSON must NOT be re-parsed.
    /// `row_to_json` is PG's `to_json` over a record; its keys come out SORTED (PG
    /// `jsonb` order, not `json` column order) because serde_json's Map is a BTreeMap.
    #[test_case::test_case(r#"SELECT to_jsonb(make_array('{"a":1}', '[1,2]', 'plain', '123')) AS s"# => r#"["{\"a\":1}","[1,2]","plain","123"]"# ; "text array elements stay strings")]
    #[test_case::test_case(r#"SELECT to_jsonb('{"a":1}') AS s"# => r#"{"a":1}"# ; "a top level utf8 scalar keeps the JSON sniff")]
    #[test_case::test_case(r#"SELECT to_json(make_array('{"a":1}')) AS s"# => r#"["{\"a\":1}"]"# ; "to_json shares array_to_json_values")]
    #[test_case::test_case("SELECT row_to_json(named_struct('total', 1, 'active', 2)) AS d" => r#"{"active":2,"total":1}"# ; "row_to_json of a struct")]
    #[test_case::test_case("SELECT to_json(named_struct('total', 1, 'active', 2)) AS d" => r#"{"active":2,"total":1}"# ; "to_json of a struct column")]
    #[tokio::test]
    async fn json_rendering_matches_postgres(sql: &str) -> String {
        text(sql).await
    }

    /// Documented limitation: PG lets `row_to_json(t)` name a whole row, but
    /// DataFusion rejects the bare relation alias during SQL planning, before any
    /// analyzer rule could rewrite it. Use the struct form above.
    #[tokio::test]
    async fn bare_relation_alias_is_still_unsupported() {
        let error = udf_ctx().sql("SELECT row_to_json(t) FROM (SELECT 1 AS total) t").await.unwrap_err().to_string();
        assert!(error.contains("No field named t"), "unexpected error: {error}");
    }
}

#[cfg(test)]
mod time_bucket_streaming_tests {
    use super::*;
    use datafusion::{
        arrow::{
            datatypes::{Schema, SchemaRef},
            record_batch::RecordBatch,
        },
        catalog::streaming::StreamingTable,
        execution::{TaskContext, context::SessionContext},
        physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter, streaming::PartitionStream},
        prelude::{SessionConfig, col},
    };
    use futures::{StreamExt, stream};

    #[derive(Debug)]
    struct PausedInput {
        batch: RecordBatch,
        release: Arc<tokio::sync::Notify>,
    }
    impl PartitionStream for PausedInput {
        fn schema(&self) -> &SchemaRef {
            self.batch.schema_ref()
        }
        fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
            let (batch, schema, release) = (self.batch.clone(), self.batch.schema(), self.release.clone());
            let tail_schema = schema.clone();
            let rows = stream::once(async move { Ok(batch) }).chain(stream::once(async move {
                release.notified().await;
                Ok(RecordBatch::new_empty(tail_schema))
            }));
            Box::pin(RecordBatchStreamAdapter::new(schema, rows))
        }
    }

    #[tokio::test]
    async fn time_bucket_emits_completed_groups_before_input_finishes() -> Result<()> {
        let mut ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        register_custom_functions(&mut ctx)?;
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(TimestampMicrosecondArray::from(vec![0, 500_000, 1_000_000, 2_000_000]).with_timezone("UTC"))])?;
        let release = Arc::new(tokio::sync::Notify::new());
        let source = Arc::new(PausedInput { batch, release: release.clone() });
        ctx.register_table("ordered_input", Arc::new(StreamingTable::try_new(schema, vec![source])?.with_sort_order(vec![col("ts").sort(true, false)])))?;
        let mut rows = ctx.sql("SELECT time_bucket('1 second', ts), count(*) FROM ordered_input GROUP BY 1 ORDER BY 1").await?.execute_stream().await?;
        let first = tokio::time::timeout(std::time::Duration::from_secs(2), rows.next()).await?.expect("first completed group")?;
        assert!(first.num_rows() > 0, "a completed group must be emitted before the input finishes");
        assert_eq!(first.column(1).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 2);
        release.notify_one();
        let mut count = first.num_rows();
        while let Some(batch) = rows.next().await {
            count += batch?.num_rows();
        }
        assert_eq!(count, 3);
        Ok(())
    }

    #[tokio::test]
    async fn time_bucket_matches_date_bin_and_rejects_invalid_widths() -> Result<()> {
        let mut ctx = SessionContext::new();
        register_custom_functions(&mut ctx)?;
        let rows = ctx.sql("SELECT time_bucket('1 second', to_timestamp_micros(n)) IS NOT DISTINCT FROM date_bin(INTERVAL '1 second', to_timestamp_micros(n), TIMESTAMP '1970-01-01') FROM (VALUES (-1000001), (-1), (0), (999999), (1000000), (NULL)) t(n)").await?.collect().await?;
        for batch in rows {
            let equal = batch.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(equal.iter().all(|value| value == Some(true)), "time_bucket must agree with date_bin on every pinned boundary: {equal:?}");
        }
        for width in ["'0 seconds'", "'-1 second'", "'9223372036854775807 weeks'", "INTERVAL '0 seconds'", "INTERVAL '1 month'"] {
            // A planning error and an execution error both count as a rejection.
            let ran: datafusion::error::Result<_> =
                async { ctx.sql(&format!("SELECT time_bucket({width}, TIMESTAMP '2026-01-01')")).await?.collect().await }.await;
            assert!(ran.is_err(), "invalid width {width}");
        }
        Ok(())
    }
}
