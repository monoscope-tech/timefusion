//! Cross-connection LRU cache for parsed `LogicalPlan`s.
//!
//! Background. `datafusion-postgres` already caches per-connection prepared
//! statements via the pgwire `PortalStore`, so a well-behaved client (psql,
//! hasql, pgbench) parses each prepared statement once per connection. The
//! cost we still pay:
//!   1. Short-lived connections (PgBouncer transaction pooling, monoscope's
//!      hasql pool when it rotates) — every new connection re-parses the
//!      same `INSERT INTO otel_logs_and_spans ...` statement, which is
//!      ~hundreds of µs of sqlparser + datafusion analyzer work.
//!   2. Anonymous prepared statements (Parse with empty name): the portal
//!      store doesn't persist them, so each Bind round-trips the planner.
//!
//! This hook short-circuits `parse_sql` by returning a cloned `LogicalPlan`
//! from an LRU keyed on the *canonical* statement text. We only cache
//! parameterised DML / SELECT statements — anything containing a literal
//! value would explode the cache. The `to_string()` we key on is produced
//! by sqlparser AFTER its own normalization, so `INSERT INTO t VALUES ($1)`
//! and `insert into t values ($1)` collapse to one entry.
//!
//! Schema-staleness invariant. `LogicalPlan` embeds the table's `SchemaRef`
//! at parse time. Caching across schema changes would silently serve plans
//! built against the old shape. We rely on the fact that timefusion's
//! `schema_loader::registry()` is loaded via `include_dir!` at compile time
//! and is therefore immutable for the lifetime of the process — see
//! `optimizers/tantivy_rewriter::indexed_columns_for` which makes the same
//! assumption. If we ever add hot-reload of YAML schemas, this cache must
//! also gain a schema-version token in the key (e.g. an `Arc<AtomicU64>`
//! bumped on each reload) or a full flush on reload.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    arrow::{array::UInt64Array, datatypes::Schema, record_batch::RecordBatch},
    common::{
        ParamValues,
        tree_node::{Transformed, TreeNode},
    },
    logical_expr::{Cast, Expr, LogicalPlan, dml::WriteOp},
    prelude::SessionContext,
    scalar::ScalarValue,
    sql::{parser::Statement as DfStatement, sqlparser::ast::Statement},
};
use datafusion_postgres::{
    hooks::{HookClient, QueryHook},
    pgwire::{
        api::{
            ClientInfo,
            results::{Response, Tag},
        },
        error::{PgWireError, PgWireResult},
    },
};
use tracing::debug;

use crate::errors::arrow_err;

/// Walk a plan and replace every `CAST(Literal(v), T)` with `Literal(cast(v, T))`.
///
/// After `replace_params_with_values` substitutes `$N → literal`, the `CAST`
/// wrappers `insert_coerce` puts around every placeholder turn into per-cell
/// `CAST(Literal, T)` exprs inside `ValuesExec`. Executing those casts at
/// query time, once per (row, column), is responsible for ~9–10 ms/row of
/// pgwire-INSERT overhead at the 88-col schema (measured). The cast values
/// are constant so we can fold them once, at substitution time, and let
/// `ValuesExec` see plain literals.
fn fold_literal_casts(plan: LogicalPlan) -> datafusion::error::Result<LogicalPlan> {
    plan.transform_up(|node| {
        let mut changed = false;
        let new_exprs: Vec<Expr> = node
            .expressions()
            .into_iter()
            .map(|expr| {
                let t = expr.transform_up(|e| {
                    if let Expr::Cast(Cast { expr, field }) = &e
                        && let Expr::Literal(value, metadata) = expr.as_ref()
                    {
                        let data_type = field.data_type();
                        return match value.cast_to(data_type) {
                            Ok(folded) => Ok(Transformed::yes(Expr::Literal(folded, metadata.clone()))),
                            // If a particular literal can't be cast (e.g. lossy
                            // string-→-number), leave it alone — the executor's
                            // cast will surface a clear error. Trace-level so a
                            // run-time spike is visible without polluting the
                            // hot path.
                            Err(err) => {
                                tracing::trace!(target: "plan_cache", %err, ?value, ?data_type, "fold_literal_casts: cast_to failed, leaving CAST for executor");
                                Ok(Transformed::no(e))
                            }
                        };
                    }
                    Ok(Transformed::no(e))
                })?;
                changed |= t.transformed;
                Ok(t.data)
            })
            .collect::<datafusion::error::Result<_>>()?;
        // Only rebuild when a cast was actually folded. `with_new_exprs` rejects
        // some nodes whose `expressions()`/`with_new_exprs` round-trip isn't
        // identity — notably `Unnest`, whose `expressions()` returns its
        // `exec_columns` but `with_new_exprs` asserts an empty expr list (DF54).
        // monoscope's `UPDATE … FROM (SELECT unnest($1::text[]) …)` dual-write
        // carries exactly such an `Unnest`; rebuilding it unconditionally tripped
        // `Internal error: Assertion failed: expr.is_empty()`.
        if !changed {
            return Ok(Transformed::no(node));
        }
        let rebuilt = node.with_new_exprs(new_exprs, node.inputs().into_iter().cloned().collect())?;
        Ok(Transformed::yes(rebuilt))
    })
    .map(|t| t.data)
}

/// pgwire-INSERT bypass: recognise `Dml(Insert) → [Projection →] Values(literals)`
/// and short-circuit the whole DataFusion executor by building the RecordBatch
/// directly from the literals and calling `ProjectRoutingTable.fast_insert_batch`.
/// Skips `ValuesExec`, `DataSinkExec`, and the per-row `replace_params_with_values`
/// walk that together account for ~5-6 ms/row of overhead at the 88-col schema.
///
/// Returns `Ok(Some(rows))` on success, `Ok(None)` if the plan shape isn't
/// the supported fast-path INSERT (caller should fall back to the regular
/// `execute_logical_plan` path).
async fn try_fast_path_insert(plan: &LogicalPlan, session_context: &SessionContext) -> datafusion::error::Result<Option<u64>> {
    use datafusion::logical_expr::dml::DmlStatement;

    let LogicalPlan::Dml(DmlStatement { table_name, op: WriteOp::Insert(_), input, .. }) = plan else {
        return Ok(None);
    };

    // Input is either `Projection → Values` (the common case for INSERT INTO
    // t (cols) VALUES …) or `Values` directly. We use the Values schema for
    // building the RecordBatch (row width must match cell count), and where
    // there is a Projection above, we require each projection expr to be a
    // simple column rename: `column1 AS target_col`. After `fold_literal_casts`
    // this is true for parameterised pgwire INSERTs where `insert_coerce`
    // already pushed the target type down into each placeholder. Anything
    // more complex (default expressions, computed columns) falls back to the
    // executor.
    // For Projection above Values, each output column is either `Column(..)`
    // or `Alias(Column(..), name)`. We capture (values_input_idx, output_name)
    // per projection position so we can reorder Values columns to match the
    // table's expected layout. Anything more complex (computed cols, casts
    // we didn't fold, sub-exprs) falls back to the executor.
    // Each output column maps either to a Values column position (column refs
    // — the common case) or to a constant the optimizer folded into the
    // projection (NULL defaults for unspecified columns, etc.). We record
    // (source, name) per output column so the build step can pull from the
    // right place.
    enum ColumnSource {
        Values(usize),
        Constant(ScalarValue),
    }
    let (column_plan, values): (Option<Vec<(ColumnSource, String)>>, &datafusion::logical_expr::Values) = match input.as_ref() {
        LogicalPlan::Projection(p) => {
            let LogicalPlan::Values(v) = p.input.as_ref() else {
                return Ok(None);
            };
            let mut plan: Vec<(ColumnSource, String)> = Vec::with_capacity(p.expr.len());
            for (i, e) in p.expr.iter().enumerate() {
                let (inner, name) = match e {
                    Expr::Alias(a) => (a.expr.as_ref(), a.name.clone()),
                    other => (other, p.schema.field(i).name().to_string()),
                };
                match inner {
                    Expr::Column(c) => {
                        let Some(input_idx) = v.schema.fields().iter().position(|f| f.name() == &c.name) else {
                            return Ok(None);
                        };
                        plan.push((ColumnSource::Values(input_idx), name));
                    }
                    Expr::Literal(val, _) => {
                        plan.push((ColumnSource::Constant(val.clone()), name));
                    }
                    _ => return Ok(None),
                }
            }
            (Some(plan), v)
        }
        LogicalPlan::Values(v) => (None, v),
        _ => return Ok(None),
    };
    let target_schema = values.schema.clone();

    // Every cell must be a literal — possibly wrapped in an Alias from the
    // pgwire `$N` placeholder name retained after substitution. Anything else
    // legitimately needs the full executor (subqueries, function calls,
    // correlated refs, etc.).
    fn cell_as_literal(e: &Expr) -> Option<&ScalarValue> {
        match e {
            Expr::Literal(v, _) => Some(v),
            Expr::Alias(a) => cell_as_literal(&a.expr),
            _ => None,
        }
    }
    if !values.values.iter().all(|row| row.iter().all(|e| cell_as_literal(e).is_some())) {
        return Ok(None);
    }

    let values_schema: Arc<Schema> = Arc::new(target_schema.as_arrow().clone());
    let num_values_cols = values_schema.fields().len();
    let num_rows = values.values.len();

    // Build one array per Values column (in Values' native order).
    let mut values_columns: Vec<datafusion::arrow::array::ArrayRef> = Vec::with_capacity(num_values_cols);
    for col_idx in 0..num_values_cols {
        let target_ty = values_schema.field(col_idx).data_type();
        if num_rows == 0 {
            values_columns.push(datafusion::arrow::array::new_empty_array(target_ty));
            continue;
        }
        let scalars: Vec<ScalarValue> = values
            .values
            .iter()
            .map(|row| {
                cell_as_literal(&row[col_idx])
                    .cloned()
                    .ok_or_else(|| datafusion::error::DataFusionError::Internal(format!("try_fast_path_insert: expected Literal, got {:?}", row[col_idx])))
            })
            .collect::<datafusion::error::Result<_>>()?;
        let arr = ScalarValue::iter_to_array(scalars)?;
        // `iter_to_array` may return a different concrete type than the
        // Values column declares (e.g. all-NULL columns come back as Null).
        // Cast back to target so the downstream MemBuffer schema check sees
        // exactly what the table expects.
        let arr = if arr.data_type() == target_ty { arr } else { datafusion::arrow::compute::cast(&arr, target_ty).map_err(arrow_err)? };
        values_columns.push(arr);
    }

    // Apply the projection: pull Values columns by index, or materialize a
    // constant array for projection cells the optimizer folded to a literal
    // (NULL defaults for columns not in the INSERT, etc.).
    let (final_schema, columns) = if let Some(plan) = column_plan {
        let n = num_rows.max(1);
        let mut fields: Vec<Arc<datafusion::arrow::datatypes::Field>> = Vec::with_capacity(plan.len());
        let mut cols: Vec<datafusion::arrow::array::ArrayRef> = Vec::with_capacity(plan.len());
        for (src, name) in &plan {
            match src {
                ColumnSource::Values(idx) => {
                    let f = values_schema.field(*idx);
                    fields.push(Arc::new(datafusion::arrow::datatypes::Field::new(name, f.data_type().clone(), f.is_nullable())));
                    cols.push(values_columns[*idx].clone());
                }
                ColumnSource::Constant(val) => {
                    let arr = val.to_array_of_size(n)?;
                    fields.push(Arc::new(datafusion::arrow::datatypes::Field::new(name, arr.data_type().clone(), true)));
                    cols.push(arr);
                }
            }
        }
        (Arc::new(Schema::new(fields)), cols)
    } else {
        (values_schema, values_columns)
    };
    let batch = RecordBatch::try_new(final_schema, columns).map_err(arrow_err)?;

    let provider = session_context.table_provider(table_name.clone()).await?;
    let Some(routing) = provider.downcast_ref::<crate::database::ProjectRoutingTable>() else {
        return Ok(None);
    };
    let rows = routing.fast_insert_batch(batch).await?;
    Ok(Some(rows))
}

/// Mirror of `datafusion_postgres::handlers::dml_completion`,
/// which is `pub(super)` and so unreachable from outside the crate.
///
/// **Re-sync checklist.** When bumping the patched `datafusion-postgres` git dep
/// (apitoolkit/datafusion-postgres @ `timefusion-df54`, see the `[patch.crates-io]`
/// in Cargo.toml), diff its `handlers.rs::dml_completion` against this
/// implementation — upstream changes to the tag format ("INSERT 0 N" oid +
/// count), the `count` column name, or the count column's Arrow type are silent
/// divergence here (no compile error, wrong wire response). Search for the
/// `RE-SYNC-DML-COMPLETION` marker below and confirm parity.
// RE-SYNC-DML-COMPLETION: keep in sync with apitoolkit/datafusion-postgres@timefusion-df54 src/handlers.rs.
async fn dml_completion(df: datafusion::dataframe::DataFrame) -> PgWireResult<Option<Response>> {
    let tag = match df.logical_plan() {
        LogicalPlan::Dml(d) => match d.op {
            WriteOp::Insert(_) => Tag::new("INSERT").with_oid(0),
            WriteOp::Update => Tag::new("UPDATE"),
            WriteOp::Delete => Tag::new("DELETE"),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    let batches = df.collect().await.map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    let rows =
        batches.first().and_then(|b| b.column_by_name("count")).and_then(|c| c.as_any().downcast_ref::<UInt64Array>()).map_or(0, |a| a.value(0) as usize);
    Ok(Some(Response::Execution(tag.with_rows(rows))))
}

// Fallback when config isn't initialized (test-only factory paths). Prod reads
// `memory.timefusion_plan_cache_capacity`. See config.rs.
const DEFAULT_PLAN_CACHE_CAPACITY: usize = 1024;

/// Singleton handle so `timefusion_stats` can read the same cache the
/// pgwire factory writes to without plumbing an Arc through the database
/// constructor.
static GLOBAL: std::sync::OnceLock<std::sync::Arc<PlanCacheHook>> = std::sync::OnceLock::new();

pub fn set_global(cache: std::sync::Arc<PlanCacheHook>) {
    let _ = GLOBAL.set(cache);
}

pub fn global() -> Option<std::sync::Arc<PlanCacheHook>> {
    GLOBAL.get().cloned()
}

/// Lock-free plan cache.
///
/// The Mutex<LruCache> design was a serialization point on the hot read path:
/// every query — even on a cache hit — took the mutex to update LRU order.
/// At 50+ concurrent readers that became the dominant bottleneck.
///
/// OLAP workloads churn through a small set of templates (the harness's prod
/// replay sees ~5 unique canonical plans across millions of queries), so we
/// drop LRU entirely. DashMap gives us lock-free reads and a soft size cap
/// that just clears the cache once exceeded — cheap, correct, and never holds
/// a lock across the await in `handle_simple_query`.
pub struct PlanCacheHook {
    cache: dashmap::DashMap<String, LogicalPlan>,
    capacity: usize,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
    /// Shape cache for LITERAL-bearing SELECTs (generated dashboard SQL that
    /// never repeats verbatim): keyed by the statement with every string
    /// literal replaced by `$N`, storing the pre-optimized placeholder plan +
    /// inferred parameter types. A hit clones the plan and substitutes the
    /// query's actual literals (cast to the inferred types) — skipping parse,
    /// analyze, AND optimize. `None` = negative entry: this shape failed to
    /// plan/parameterize once; don't retry it per query.
    shapes: dashmap::DashMap<String, Option<ShapeEntry>>,
    /// Canonical texts we served a pre-optimized substituted plan for, so
    /// `was_pre_optimized` can tell the handler to skip `state.optimize()`.
    /// Literal-bearing texts are one-shot (next dashboard refresh has new
    /// literals), so recency semantics with a soft cap are enough — a false
    /// `false` after eviction merely re-optimizes an optimized plan.
    served: dashmap::DashMap<String, ()>,
    shape_hits: std::sync::atomic::AtomicU64,
    shape_skips: std::sync::atomic::AtomicU64,
    /// When true, `now()`-bearing SELECTs go through the shape cache with the
    /// time function parameterized (fresh instant substituted per query) instead
    /// of being bypassed. Off by default — it's the hot dashboard path, so enable
    /// deliberately (TIMEFUSION_PLAN_CACHE_TIME_FNS=1) after canarying.
    time_fn_shapes: bool,
}

#[derive(Clone)]
struct ShapeEntry {
    plan: LogicalPlan,
    /// Inferred DataType per `$N` (index 0 = `$1`); substituted literals are
    /// cast to these so the plan's expression types stay exact.
    param_types: Vec<Option<datafusion::arrow::datatypes::DataType>>,
}

/// Statements whose optimized plan embeds the QUERY START TIME must never be
/// cached: DataFusion const-folds these Stable functions during
/// `state.optimize()` (SimplifyExpressions reads query_execution_start_time),
/// so a cached plan would freeze `now()` at first-build time and serve stale
/// windows forever. Applies to BOTH the `$N` template cache and the shape
/// cache — such statements re-plan per query instead.
fn contains_plan_time_folded_fn(stmt: &Statement) -> bool {
    // Union of both classes; TIME_FNS == PARAMETERIZABLE ∪ UNPARAMETERIZABLE.
    stmt_uses_fn(stmt, PARAMETERIZABLE_TIME_FNS) || stmt_uses_fn(stmt, UNPARAMETERIZABLE_TIME_FNS)
}

/// The timestamp-returning time fns we can safely parameterize (replace the call
/// with a `$N` placeholder bound to the current instant, so the plan is reusable
/// and time stays fresh).
const PARAMETERIZABLE_TIME_FNS: &[&str] = &["now", "current_timestamp", "statement_timestamp", "transaction_timestamp", "clock_timestamp", "localtimestamp"];

/// Date/Time-returning time fns — different result type, riskier substitution —
/// so a query using any of these stays on the bypass path.
const UNPARAMETERIZABLE_TIME_FNS: &[&str] = &["current_date", "today", "current_time", "localtime"];

fn is_parameterizable_time_fn(name: &str) -> bool {
    PARAMETERIZABLE_TIME_FNS.contains(&name.to_lowercase().as_str())
}

fn contains_unparameterizable_time_fn(stmt: &Statement) -> bool {
    stmt_uses_fn(stmt, UNPARAMETERIZABLE_TIME_FNS)
}

/// True if `stmt` calls any function named in `names` (case-insensitive, matching
/// the last name segment). Shared AST-visitor for the time-fn classifiers.
fn stmt_uses_fn(stmt: &Statement, names: &[&str]) -> bool {
    use std::ops::ControlFlow;

    use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, visit_expressions};
    let mut found = false;
    let _: ControlFlow<()> = visit_expressions(stmt, |e: &SqlExpr| {
        if let SqlExpr::Function(f) = e
            && let Some(name) = f.name.0.last()
            && let Some(ident) = name.as_ident()
            && names.contains(&ident.value.to_lowercase().as_str())
        {
            found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    found
}

/// Highest client-supplied `$N` placeholder index already in `stmt` (0 if none).
/// Lets the mixed now()+`$N` path number its injected time-fn placeholders
/// above the client's so the two numbering spaces don't collide.
fn max_placeholder_index(stmt: &Statement) -> usize {
    use std::ops::ControlFlow;

    use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Value, visit_expressions};
    let mut max = 0usize;
    let _: ControlFlow<()> = visit_expressions(stmt, |e: &SqlExpr| {
        if let SqlExpr::Value(vs) = e
            && let Value::Placeholder(p) = &vs.value
            && let Ok(n) = p.trim_start_matches('$').parse::<usize>()
        {
            max = max.max(n);
        }
        ControlFlow::Continue(())
    });
    max
}

/// Replace string literals (when `include_strings`) and parameterizable time
/// fns in a SELECT with `$N` placeholders numbered `base + walk_position`,
/// returning the parameterized statement + the extracted values in `$` order.
/// `None` when nothing was extracted. Numbers/booleans stay inline — they steer
/// plan shape (LIMIT, bucket sizes) and vary little. `base > 0` leaves the
/// client's `$1..$base` binds untouched (mixed now()+`$N` path); `include_strings`
/// is off there because a prepared statement's literals are fixed across binds.
fn parameterize_statement(stmt: &Statement, base: usize, include_strings: bool) -> Option<(Statement, Vec<ScalarValue>)> {
    use std::ops::ControlFlow;

    use datafusion::sql::sqlparser::{
        ast::{Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, Value, ValueWithSpan, visit_expressions_mut},
        tokenizer::Span,
    };
    let mut stmt = stmt.clone();
    let mut values: Vec<ScalarValue> = Vec::new();

    // Parameterize a numeric literal ONLY when reached as a value-context child
    // (function arg, comparison operand, CASE/BETWEEN/cast). A bare
    // `Expr::Value(Number)` — which is exactly what GROUP BY / ORDER BY ordinals
    // and LIMIT / OFFSET are — is never a child of these containers, so ordinals
    // keep their positional meaning at every nesting level. Numbers that can't be
    // parsed (or would lose precision) stay inline. This is the fix for the
    // dashboard shape fragmentation: time_bucket(60,…), approx_percentile(0.95,…),
    // `duration <= 500`, epoch bounds all differ only by numeric literals.
    fn take_number(e: &mut SqlExpr, base: usize, values: &mut Vec<ScalarValue>) {
        if let SqlExpr::Value(vs) = e
            && let Value::Number(n, _) = &vs.value
            && let Some(sv) =
                n.parse::<i64>().map(|i| ScalarValue::Int64(Some(i))).ok().or_else(|| n.parse::<f64>().ok().map(|f| ScalarValue::Float64(Some(f))))
        {
            values.push(sv);
            vs.value = Value::Placeholder(format!("${}", base + values.len()));
        }
    }
    // Capture "now" once so every now()/current_timestamp in the statement
    // substitutes to the same fresh instant (matching SQL's single-evaluation
    // semantics). Timezone-aware nanosecond mirrors DataFusion's native now();
    // the caller casts to the placeholder's inferred type.
    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let _: ControlFlow<()> = visit_expressions_mut(&mut stmt, |e: &mut SqlExpr| {
        match e {
            // PG array literals ('{}', '{a,b}') must stay inline: PgArrayLiteralRewriter
            // rewrites them to typed list literals during analysis, and it only matches
            // Expr::Literal — a `$N` placeholder slips past it and gets mis-cast to a
            // single-element list (COALESCE(list_col, '{a,b}') → ['{a,b}'] instead of
            // ['a','b']). Cheap to skip: array-literal COALESCE is not a hot cached path.
            SqlExpr::Value(vs) => {
                if include_strings
                    && let Value::SingleQuotedString(s) = &vs.value
                    && !s.trim_start().starts_with('{')
                {
                    values.push(ScalarValue::Utf8(Some(s.clone())));
                    vs.value = Value::Placeholder(format!("${}", base + values.len()));
                }
            }
            // now()/current_timestamp/… → placeholder bound to the captured instant,
            // so the optimized plan is reusable across dashboard refreshes while the
            // time window stays fresh (never frozen to plan-build time).
            SqlExpr::Function(f) if f.name.0.last().and_then(|n| n.as_ident()).is_some_and(|i| is_parameterizable_time_fn(&i.value)) => {
                values.push(ScalarValue::TimestampNanosecond(Some(now_ns), Some("+00:00".into())));
                *e = SqlExpr::Value(ValueWithSpan { value: Value::Placeholder(format!("${}", base + values.len())), span: Span::empty() });
            }
            // Value-context containers: parameterize their direct numeric-literal
            // children. See take_number — this deliberately never touches a
            // standalone Number (ordinals / LIMIT / OFFSET). Gated on
            // `include_strings`: the mixed now()+`$N` execute path calls with
            // `false` and binds only time-fn placeholders positionally, so it must
            // not gain extra numeric placeholders.
            SqlExpr::BinaryOp { left, right, .. } if include_strings => {
                take_number(left, base, &mut values);
                take_number(right, base, &mut values);
            }
            SqlExpr::UnaryOp { expr, .. } | SqlExpr::Nested(expr) | SqlExpr::Cast { expr, .. } if include_strings => take_number(expr, base, &mut values),
            SqlExpr::Between { expr, low, high, .. } if include_strings => {
                take_number(expr, base, &mut values);
                take_number(low, base, &mut values);
                take_number(high, base, &mut values);
            }
            SqlExpr::InList { expr, list, .. } if include_strings => {
                take_number(expr, base, &mut values);
                list.iter_mut().for_each(|e| take_number(e, base, &mut values));
            }
            SqlExpr::Case { operand, conditions, else_result, .. } if include_strings => {
                if let Some(op) = operand {
                    take_number(op, base, &mut values);
                }
                for w in conditions.iter_mut() {
                    take_number(&mut w.condition, base, &mut values);
                    take_number(&mut w.result, base, &mut values);
                }
                if let Some(er) = else_result {
                    take_number(er, base, &mut values);
                }
            }
            SqlExpr::Function(f) if include_strings => {
                if let FunctionArguments::List(list) = &mut f.args {
                    for arg in list.args.iter_mut() {
                        let ex = match arg {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                            FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. } | FunctionArg::ExprNamed { arg: FunctionArgExpr::Expr(e), .. } => Some(e),
                            _ => None,
                        };
                        if let Some(e) = ex {
                            take_number(e, base, &mut values);
                        }
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    });
    (!values.is_empty()).then_some((stmt, values))
}

impl Default for PlanCacheHook {
    fn default() -> Self {
        let cfg = crate::config::try_config().map(|c| &c.memory);
        let capacity = cfg.map_or(DEFAULT_PLAN_CACHE_CAPACITY, |m| m.timefusion_plan_cache_capacity);
        let time_fn_shapes = cfg.is_some_and(|m| m.timefusion_plan_cache_time_fns);
        Self::new(capacity, time_fn_shapes)
    }
}

impl PlanCacheHook {
    pub fn new(capacity: usize, time_fn_shapes: bool) -> Self {
        Self {
            cache: dashmap::DashMap::new(),
            capacity: capacity.max(1),
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
            shapes: dashmap::DashMap::new(),
            served: dashmap::DashMap::new(),
            shape_hits: std::sync::atomic::AtomicU64::new(0),
            shape_skips: std::sync::atomic::AtomicU64::new(0),
            time_fn_shapes,
        }
    }

    /// Returns (hits, misses) for stats observability.
    pub fn counters(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (self.hits.load(Relaxed), self.misses.load(Relaxed))
    }

    /// Returns (shape_hits, shape_skips) for stats observability.
    pub fn shape_counters(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (self.shape_hits.load(Relaxed), self.shape_skips.load(Relaxed))
    }

    /// Shape-cache path for literal-bearing SELECTs. Returns a fully
    /// substituted, pre-optimized plan, or `None` to fall back to the normal
    /// parse→optimize pipeline. Every failure installs a negative entry so a
    /// shape that can't parameterize is only attempted once.
    async fn try_shape_cached_plan(&self, statement: &Statement, canonical: &str, session_context: &SessionContext) -> Option<LogicalPlan> {
        use std::sync::atomic::Ordering::Relaxed;
        if !matches!(statement, Statement::Query(_)) {
            return None;
        }
        let (param_stmt, values) = parameterize_statement(statement, 0, true)?;
        let shape_key = param_stmt.to_string();
        let entry = self.get_or_build_shape(&shape_key, param_stmt, values.len(), session_context).await?;

        // Substitute this query's literals, cast to the inferred types.
        let cast_values: Vec<ScalarValue> = values
            .into_iter()
            .zip(entry.param_types.iter())
            .map(|(v, ty)| match ty {
                Some(t) => v.cast_to(t).unwrap_or(v),
                None => v,
            })
            .collect();
        let plan = entry.plan.clone().replace_params_with_values(&ParamValues::List(cast_values.into_iter().map(Into::into).collect())).ok()?;
        let plan = fold_literal_casts(plan).ok()?;
        self.shape_hits.fetch_add(1, Relaxed);
        // Mark so `was_pre_optimized` lets the handler skip state.optimize().
        if self.served.len() >= 4096 {
            self.served.retain(|_, _| fastrand::bool());
        }
        self.served.insert(canonical.to_string(), ());
        Some(plan)
    }

    /// Get or build+optimize+cache the placeholder template for `shape_key`.
    /// `value_count` = how many leading `$N` types to record for the caller to
    /// cast its client literals against (0 for the mixed path, which binds its
    /// injected params by inferred type at execute). `None` = negative entry:
    /// this shape failed to plan once; don't retry per query.
    async fn get_or_build_shape(&self, shape_key: &str, param_stmt: Statement, value_count: usize, session_context: &SessionContext) -> Option<ShapeEntry> {
        use std::sync::atomic::Ordering::Relaxed;
        if let Some(e) = self.shapes.get(shape_key) {
            return e.value().clone(); // Some(entry) hit / None negative
        }
        // Build the placeholder plan once for this shape.
        let state = session_context.state();
        let built = match state.statement_to_plan(DfStatement::Statement(Box::new(param_stmt))).await {
            Ok(p) => state.optimize(&p).ok(),
            Err(_) => None,
        }
        .and_then(|plan| {
            if value_count == 0 {
                return Some(ShapeEntry { plan, param_types: Vec::new() });
            }
            let types = plan.get_parameter_types().ok()?;
            let param_types = (1..=value_count).map(|i| types.get(&format!("${i}")).cloned().flatten()).collect();
            Some(ShapeEntry { plan, param_types })
        });
        if built.is_none() {
            self.shape_skips.fetch_add(1, Relaxed);
            debug!(target: "plan_cache", "shape negative-cached: {shape_key}");
        }
        // Soft cap shares the template cache's philosophy: clear half on
        // overflow (shape variety should be tiny in steady state).
        if self.shapes.len() >= self.capacity {
            let cap = self.capacity;
            self.shapes.retain(|_, _| fastrand::usize(..cap) >= cap / 2);
        }
        self.shapes.insert(shape_key.to_string(), built.clone());
        built
    }

    /// Mixed now()+client-`$N` path: cache an OPTIMIZED template whose time-fn
    /// placeholders are numbered above the client's binds and whose client
    /// placeholders stay open. Returns the template unsubstituted —
    /// `extra_execute_params` supplies a fresh instant for the time-fn
    /// placeholders on every execute, so the window never freezes, even for a
    /// reused (named) prepared statement whose parse hook runs only once.
    async fn try_mixed_time_fn_plan(&self, statement: &Statement, canonical: &str, session_context: &SessionContext) -> Option<LogicalPlan> {
        let base = max_placeholder_index(statement);
        let (param_stmt, _) = parameterize_statement(statement, base, false)?;
        let shape_key = param_stmt.to_string();
        let plan = self.get_or_build_shape(&shape_key, param_stmt, 0, session_context).await?.plan;
        self.shape_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.served.len() >= 4096 {
            self.served.retain(|_, _| fastrand::bool());
        }
        self.served.insert(canonical.to_string(), ());
        Some(plan)
    }

    /// Cheap pre-check on the AST kind. Skipping non-DML before paying for
    /// `Statement::to_string()` avoids serializing the AST on every Parse
    /// message regardless of cacheability.
    fn kind_is_cacheable(stmt: &Statement) -> bool {
        matches!(stmt, Statement::Insert(_) | Statement::Query(_) | Statement::Update { .. } | Statement::Delete(_))
    }

    /// Only cache statements with at least one placeholder. Without a
    /// placeholder, the canonical text contains literal values (timestamps,
    /// UUIDs, etc.) which would never recur — caching that just pollutes the
    /// LRU and increases lock contention.
    fn has_placeholder(sql: &str) -> bool {
        // Naive `contains('$')` would false-positive on dollar-quoted literals
        // like '$100' and cache statements with embedded literal values.
        sql.as_bytes().windows(2).any(|w| w[0] == b'$' && w[1].is_ascii_digit())
    }
}

#[async_trait]
impl QueryHook for PlanCacheHook {
    /// Deliberately a no-op for simple queries. The cache only fires on the
    /// extended-query path (parameterised SQL with `$N` placeholders); ad-hoc
    /// `psql` simple queries get one-shot literals so caching them by
    /// canonical text would never produce a hit. The vendored
    /// datafusion-postgres `SimpleQueryHandler::do_query` still runs
    /// `state.optimize()` per call because `was_pre_optimized` returns false
    /// for canonical SQL not present in our cache — see the optimize-skip patch
    /// on apitoolkit/datafusion-postgres@timefusion-df54.
    async fn handle_simple_query(
        &self, _statement: &Statement, _session_context: &SessionContext, _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }

    /// Trailing placeholders the mixed path injects — the Parse/Describe path
    /// hides these from the client's ParameterDescription. Equals the number of
    /// values `extra_execute_params` appends for the same statement.
    fn injected_param_count(&self, statement: &Statement) -> usize {
        self.extra_execute_params(statement).len()
    }

    /// Fresh instant(s) for the time-fn placeholders the mixed now()+`$N` path
    /// injected at parse (numbered above the client's binds). Appended to the
    /// client's params before substitution, so `now()` is re-evaluated on every
    /// execute. Empty for the pure path (M=0, substituted at parse) and for any
    /// statement we didn't shape-cache — surplus is ignored by the executor.
    fn extra_execute_params(&self, statement: &Statement) -> Vec<ScalarValue> {
        if !self.time_fn_shapes
            || !matches!(statement, Statement::Query(_))
            || contains_unparameterizable_time_fn(statement)
            || !stmt_uses_fn(statement, PARAMETERIZABLE_TIME_FNS)
        {
            return Vec::new();
        }
        let base = max_placeholder_index(statement);
        if base == 0 {
            return Vec::new(); // pure path substitutes at parse
        }
        parameterize_statement(statement, base, false).map_or(Vec::new(), |(_, values)| values)
    }

    async fn handle_extended_parse_query(
        &self, statement: &Statement, session_context: &SessionContext, _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        // Cheap AST-variant check first; only then pay for to_string() and
        // the placeholder scan.
        if !Self::kind_is_cacheable(statement) {
            return None;
        }
        // now()/current_date/... are const-folded by the optimizer using the
        // query start time — a verbatim-cached optimized plan would freeze them.
        // With time-fn shape caching on, route now()-bearing SELECTs to the shape
        // path (which parameterizes the time fn → fresh instant per query);
        // otherwise, and for unparameterizable date/time fns, plan fresh.
        if contains_plan_time_folded_fn(statement) {
            if self.time_fn_shapes && matches!(statement, Statement::Query(_)) && !contains_unparameterizable_time_fn(statement) {
                let canonical = statement.to_string();
                if !Self::has_placeholder(&canonical) {
                    // Pure now()/literals: parameterize + substitute at parse.
                    return self.try_shape_cached_plan(statement, &canonical, session_context).await.map(Ok);
                } else if stmt_uses_fn(statement, PARAMETERIZABLE_TIME_FNS) {
                    // Mixed now()+client `$N`: cache a template that keeps BOTH the
                    // client placeholders and the time-fn placeholders open; the
                    // fresh instant is injected per-execute by extra_execute_params
                    // (correct even for reused prepared statements).
                    return self.try_mixed_time_fn_plan(statement, &canonical, session_context).await.map(Ok);
                }
            }
            return None;
        }
        let canonical = statement.to_string();
        if !Self::has_placeholder(&canonical) {
            // Literal-bearing SELECT: try the shape cache (parameterize →
            // cached placeholder plan → substitute). Falls back to the
            // normal pipeline on any miss/failure.
            return self.try_shape_cached_plan(statement, &canonical, session_context).await.map(Ok);
        }

        // Lock-free read: DashMap.get returns a guard that just locks the
        // single shard's reader, not the whole cache.
        if let Some(plan) = self.cache.get(&canonical) {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            debug!(target: "plan_cache", "hit: {}", canonical);
            return Some(Ok(plan.clone()));
        }

        // Miss: build the plan, install it, hand a clone back to caller.
        self.misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let state = session_context.state();
        let plan = match state.statement_to_plan(DfStatement::Statement(Box::new(statement.clone()))).await {
            Ok(p) => p,
            Err(e) => return Some(Err(PgWireError::ApiError(Box::new(e)))),
        };
        // Multi-row INSERT placeholder coercion: wraps `$N` placeholders inside
        // Values rows with `CAST($N AS <col_type>)` so pgwire param-type
        // inference returns the right type per placeholder (otherwise row-1
        // types leak across to row-2+ placeholders by position).
        let plan = crate::insert_coerce::rewrite_plan(plan);
        // Pre-optimize at cache-miss time, not per-query. With OLAP traffic
        // (one-time plan build, millions of executions) this turns a per-
        // query 30ms cost into a one-time amortization. The patched
        // datafusion-postgres skips its `state.optimize()` call when the hook
        // returns Some — see apitoolkit/datafusion-postgres@timefusion-df54 src/handlers.rs.
        // The plan still goes through `replace_params_with_values` at exec
        // time, but optimization rules that aren't constant-fold are
        // parameter-independent and stay valid across all bound values.
        let plan = match state.optimize(&plan) {
            Ok(p) => p,
            Err(e) => return Some(Err(PgWireError::ApiError(Box::new(e)))),
        };
        // Soft size cap: when exceeded, evict half the entries (random shard
        // sweep) rather than clearing the whole cache. With the OLAP workload
        // we measured (~5 unique templates) this branch never fires; under
        // pattern-diverse load (ad-hoc debug sessions) it avoids the thrash
        // a full clear would cause when one burst of distinct queries
        // displaces the hot templates.
        //
        // Under a burst of novel queries all missing simultaneously (300
        // connections in an ad-hoc debug session), N threads could each
        // cross the threshold and try to sweep. Without coordination
        // they'd each take per-shard write locks in series, stalling
        // concurrent readers. The static AtomicBool below gates the
        // sweep so only the first thread per overflow crossing runs
        // `retain`; the rest skip and re-insert as if the cap hadn't
        // been crossed yet (their entries will be evicted by the next
        // sweep). Cheap single-atomic and converges the stampede.
        // AtomicBool gate so only one sweep runs per overflow crossing.
        // Cheap (one atomic) and converts the multi-thread retain
        // stampede into a single sweep + a cohort of fast-path skips.
        // `cache.retain` with this closure can't panic, so the guard can
        // be a flat `.store(false)` at the end of the block rather than
        // a Drop-based scopeguard.
        // `cache.len()` walks every DashMap shard under a read lock — O(shards).
        // With ~5 templates in the OLAP target the threshold never trips, so
        // this is a per-miss O(16) atomic chain that mostly returns false
        // before the second clause even fires. If `DEFAULT_PLAN_CACHE_CAPACITY`
        // ever grows to a number where `len()` shows up on the profile,
        // mirror the size with a separate `AtomicUsize` bumped/decremented
        // alongside `cache.insert`/`retain` and gate on that instead — the
        // `EVICTING` AtomicBool already prevents double-counting on the
        // sweep path.
        static EVICTING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if self.cache.len() >= self.capacity && !EVICTING.swap(true, std::sync::atomic::Ordering::AcqRel) {
            let target = self.capacity / 2;
            // Operator-visible signal: this branch firing means the workload
            // is pushing more distinct plan templates than the cache can
            // hold. With the steady-state OLAP target (~5 templates) this
            // should never fire in prod. If it does, expect the next ~256
            // queries to thunder-herd into state.optimize().
            tracing::warn!(
                target = "plan_cache",
                size = self.cache.len(),
                capacity = self.capacity,
                "plan_cache exceeded capacity — evicting ~half. Subsequent queries on evicted plans will re-pay the optimize cost. If this fires steadily, the workload's plan-template variety has grown past the cache budget."
            );
            self.cache.retain(|_, _| {
                // DashMap iterates per-shard so this is roughly random by
                // hash distribution. Cheaper than an LRU clock and adequate
                // when the steady-state working set fits well under capacity.
                // Caveat: `retain` takes a write-lock on each shard in turn.
                // Under a pathological burst of distinct queries that
                // crosses the cap (~256 unique templates) this briefly
                // stalls every concurrent reader on the same shards. For
                // the OLAP workload measured here (~5 unique templates)
                // this branch never fires; if it ever does in prod, swap
                // for a try-lock-and-skip pattern.
                fastrand::usize(..self.capacity) >= target
            });
            EVICTING.store(false, std::sync::atomic::Ordering::Release);
        }
        self.cache.insert(canonical, plan.clone());
        Some(Ok(plan))
    }

    async fn handle_extended_query(
        &self, _statement: &Statement, logical_plan: &LogicalPlan, params: &ParamValues, session_context: &SessionContext, _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        // Only intercept DML — for SELECTs the vendored path is fine.
        // The win here is post-substitution constant folding of `CAST(Literal, T)`
        // exprs that `insert_coerce` puts around every placeholder; folding them
        // before `ValuesExec` evaluates the plan saves ~9–10 ms per inserted
        // row on the 88-col schema (measured).
        if !matches!(logical_plan, LogicalPlan::Dml(_)) {
            return None;
        }
        let substituted = match logical_plan.clone().replace_params_with_values(params) {
            Ok(p) => p,
            Err(e) => return Some(Err(PgWireError::ApiError(Box::new(e)))),
        };
        let folded = match fold_literal_casts(substituted) {
            Ok(p) => p,
            Err(e) => return Some(Err(PgWireError::ApiError(Box::new(e)))),
        };
        // Fast-path: `Dml(Insert) → [Projection →] Values(literals)` skips the
        // executor entirely and writes the batch straight into the buffered
        // layer. Saves the ~5-6 ms/row that `ValuesExec` + `DataSinkExec`
        // were costing at the 88-col schema.
        match try_fast_path_insert(&folded, session_context).await {
            Ok(Some(rows)) => return Some(Ok(Response::Execution(Tag::new("INSERT").with_oid(0).with_rows(rows as usize)))),
            Ok(None) => {} // fall through to the normal executor
            Err(e) => return Some(Err(PgWireError::ApiError(Box::new(e)))),
        }
        let df = match session_context.execute_logical_plan(folded).await {
            Ok(df) => df,
            Err(e) => return Some(Err(PgWireError::ApiError(Box::new(e)))),
        };
        Some(match dml_completion(df).await {
            Ok(Some(resp)) => Ok(resp),
            Ok(None) => Err(PgWireError::ApiError("internal error: DML plan returned non-DML completion".to_string().into())),
            Err(e) => Err(e),
        })
    }

    /// Signal to the do_query path that any plan we returned is already
    /// optimized — so `state.optimize()` can be skipped. Plans only land
    /// in `self.cache` after `state.optimize()` ran inside
    /// `handle_extended_parse_query`, so a cache lookup here is the
    /// authoritative answer.
    ///
    /// TOCTOU note: between this lookup and the handler calling
    /// `replace_params_with_values`, the capacity-limit sweep can evict the
    /// entry. In that case we falsely return `false` and the handler will
    /// re-optimize the plan it has in hand — specifically, the
    /// pre-optimised `LogicalPlan` stored on the `Portal` at parse time
    /// (which IS the optimised plan our hook installed; the eviction only
    /// removed our memo of having installed it, not the plan itself).
    /// Re-running `state.optimize()` on an already-optimized plan is a
    /// near-no-op (analyzer/optimizer rules detect inapplicability and
    /// short-circuit) — at most a few hundred microseconds of redundant
    /// work, well below the per-query budget. No correctness risk.
    fn was_pre_optimized(&self, canonical_sql: &str) -> bool {
        self.cache.contains_key(canonical_sql) || self.served.contains_key(canonical_sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> Statement {
        use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
        Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap().remove(0)
    }

    #[test]
    fn parameterize_extracts_strings_and_value_context_numbers_in_walk_order() {
        let stmt = parse("SELECT id FROM t WHERE project_id = 'p1' AND ts > '2026-07-01' AND n = 5 LIMIT 100");
        let (param, values) = parameterize_statement(&stmt, 0, true).expect("has literals");
        let text = param.to_string();
        assert!(text.contains("$1") && text.contains("$2") && text.contains("$3"), "strings + the comparison number become placeholders: {text}");
        assert!(text.contains("LIMIT 100"), "LIMIT stays inline (not an ordinal-safe value context): {text}");
        // Walk order: 'p1', '2026-07-01', then the numeric 5 from `n = 5`.
        assert_eq!(
            values,
            vec![ScalarValue::Utf8(Some("p1".into())), ScalarValue::Utf8(Some("2026-07-01".into())), ScalarValue::Int64(Some(5))],
            "values extracted in walk order"
        );
        // Same shape with different literals (incl. the number) → identical shape key.
        let stmt2 = parse("SELECT id FROM t WHERE project_id = 'p2' AND ts > '2026-07-04' AND n = 9 LIMIT 100");
        let (param2, _) = parameterize_statement(&stmt2, 0, true).unwrap();
        assert_eq!(text, param2.to_string(), "shape key must be literal-insensitive");
    }

    #[test]
    fn parameterize_none_without_any_literals() {
        // No string/number/time-fn literals to lift → nothing to cache-generalize.
        assert!(parameterize_statement(&parse("SELECT count(*) FROM t"), 0, true).is_none());
    }

    #[test]
    fn numeric_literals_in_value_contexts_parameterize() {
        // The 2026-07-20 plateau fix: dashboard shapes that differ only by numeric
        // literals (bucket size, percentile, duration thresholds, epoch bounds) must
        // collapse to one cached shape instead of replanning every refresh.
        let a = "SELECT time_bucket(60, timestamp), approx_percentile(0.95, duration) FROM t WHERE project_id = 'p' AND duration <= 500 AND timestamp >= 1721000000000000";
        let b = "SELECT time_bucket(300, timestamp), approx_percentile(0.99, duration) FROM t WHERE project_id = 'p' AND duration <= 900 AND timestamp >= 1722000000000000";
        let (pa, va) = parameterize_statement(&parse(a), 0, true).expect("numbers parameterize");
        let (pb, _) = parameterize_statement(&parse(b), 0, true).unwrap();
        let ta = pa.to_string();
        assert!(!ta.contains("60") && !ta.contains("0.95") && !ta.contains("500"), "numerics replaced: {ta}");
        assert_eq!(ta, pb.to_string(), "shape identical across differing numeric literals");
        // 4 numbers + the 'p' string all captured.
        assert_eq!(va.len(), 5, "captured {:?}", va);
    }

    #[test]
    fn ordinals_and_limit_stay_inline() {
        // SAFETY regression: GROUP BY / ORDER BY ordinals and LIMIT/OFFSET are bare
        // Number nodes; parameterizing them would change ORDER BY 1 into ordering by
        // a constant (wrong results). Only the 'p' string may be lifted.
        let stmt = parse("SELECT status_code, count(*) FROM t WHERE project_id = 'p' GROUP BY 1 ORDER BY 1 LIMIT 100 OFFSET 20");
        let (param, values) = parameterize_statement(&stmt, 0, true).expect("'p' parameterizes");
        let text = param.to_string();
        assert!(text.contains("GROUP BY 1"), "group-by ordinal inline: {text}");
        assert!(text.contains("ORDER BY 1"), "order-by ordinal inline: {text}");
        assert!(text.contains("LIMIT 100") && text.contains("OFFSET 20"), "limit/offset inline: {text}");
        assert_eq!(values, vec![ScalarValue::Utf8(Some("p".into()))], "only the string lifted, no ordinals");
    }

    #[test]
    fn parameterize_keeps_pg_array_literals_inline() {
        // Regression: parameterizing '{}'/'{a,b}' into a $N placeholder hides them
        // from PgArrayLiteralRewriter (matches Expr::Literal only), so they got
        // mis-cast to single-element lists (COALESCE(list_col, '{a,b}') → ['{a,b}']
        // instead of ['a','b']; edge_cases.slt:172). Array literals must stay inline.
        let stmt = parse("SELECT ARRAY_LENGTH(COALESCE(parent_id, '{a,b}')) FROM t WHERE project_id = 'p'");
        let (param, values) = parameterize_statement(&stmt, 0, true).expect("the 'p' literal still parameterizes");
        let text = param.to_string();
        assert!(text.contains("'{a,b}'"), "PG array literal stays inline: {text}");
        assert_eq!(values, vec![ScalarValue::Utf8(Some("p".into()))], "only the non-array string is extracted");
    }

    #[test]
    fn time_functions_disqualify_caching() {
        // Optimizer const-folds these from the query start time — caching the
        // optimized plan would freeze the window (2026-07-05 review finding).
        for sql in [
            "SELECT id FROM t WHERE project_id = 'p' AND ts > now()",
            "SELECT id FROM t WHERE project_id = 'p' AND d = current_date",
            "SELECT id FROM t WHERE project_id = 'p' AND ts > NOW() - INTERVAL '1 hour'",
        ] {
            assert!(contains_plan_time_folded_fn(&parse(sql)), "{sql}");
        }
        assert!(!contains_plan_time_folded_fn(&parse("SELECT id FROM t WHERE project_id = 'p' AND ts > '2026-07-01'")));
        // A column merely NAMED now must not disqualify.
        assert!(!contains_plan_time_folded_fn(&parse("SELECT now FROM t WHERE project_id = 'p'")));
    }

    #[test]
    fn parameterize_replaces_now_with_fresh_timestamp_placeholder() {
        // now()/current_timestamp become $N bound to a fresh instant so the
        // optimized plan is reusable while the window stays current (D2).
        let before = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        let stmt = parse("SELECT id FROM t WHERE project_id = 'p' AND ts > now() - INTERVAL '1 hour'");
        let (param, values) = parameterize_statement(&stmt, 0, true).expect("now() parameterizes");
        let after = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        let text = param.to_string();
        assert!(!text.to_lowercase().contains("now("), "now() replaced by placeholder: {text}");
        assert!(text.contains("$1") && text.contains("$2"), "project_id + now() both placeholders: {text}");
        // Second value is the timestamp bound to the captured instant.
        match values[1] {
            ScalarValue::TimestampNanosecond(Some(ns), Some(_)) => assert!(before <= ns && ns <= after, "fresh instant"),
            ref v => panic!("expected tz-aware nanosecond timestamp, got {v:?}"),
        }
        // Shape is literal-insensitive: two refreshes yield the same placeholder text.
        let (param2, _) = parameterize_statement(&parse("SELECT id FROM t WHERE project_id = 'q' AND ts > now() - INTERVAL '1 hour'"), 0, true).unwrap();
        assert_eq!(text, param2.to_string(), "reusable shape key across refreshes");
    }

    #[test]
    fn max_placeholder_index_finds_highest_client_bind() {
        assert_eq!(max_placeholder_index(&parse("SELECT id FROM t WHERE project_id = $1 AND n = $3")), 3);
        assert_eq!(max_placeholder_index(&parse("SELECT id FROM t WHERE project_id = 'p'")), 0);
    }

    #[test]
    fn mixed_parameterizes_time_fns_above_client_binds_only() {
        // Mixed now()+$N: time-fn numbered above the client's max ($1 → now() = $2),
        // client $1 untouched, string literals left inline (fixed across binds).
        let stmt = parse("SELECT id FROM t WHERE project_id = $1 AND level = 'error' AND ts > now() - INTERVAL '1 hour'");
        let (param, values) = parameterize_statement(&stmt, 1, false).expect("now() parameterizes");
        let text = param.to_string();
        assert!(!text.to_lowercase().contains("now("), "now() replaced: {text}");
        assert!(text.contains("$1") && text.contains("$2"), "client $1 kept, now() → $2: {text}");
        assert!(text.contains("'error'"), "string literal stays inline in mixed path: {text}");
        assert_eq!(values.len(), 1, "only the time-fn is extracted");
        assert!(matches!(values[0], ScalarValue::TimestampNanosecond(Some(_), Some(_))));
    }

    #[test]
    fn extra_execute_params_supplies_fresh_instant_for_mixed_only() {
        let hook = PlanCacheHook::new(64, true);
        let mixed = parse("SELECT id FROM t WHERE project_id = $1 AND ts > now() - INTERVAL '1 hour'");
        // Two executes → two fresh instants (never frozen), one value each ($2).
        let a = hook.extra_execute_params(&mixed);
        let b = hook.extra_execute_params(&mixed);
        assert_eq!(a.len(), 1);
        match (&a[0], &b[0]) {
            (ScalarValue::TimestampNanosecond(Some(x), _), ScalarValue::TimestampNanosecond(Some(y), _)) => assert!(y >= x, "monotonic fresh instant"),
            _ => panic!("expected tz-aware nanosecond timestamps"),
        }
        // Pure path (no client bind) substitutes at parse → no execute-time extras.
        assert!(hook.extra_execute_params(&parse("SELECT id FROM t WHERE project_id = 'p' AND ts > now()")).is_empty());
        // No time fn → nothing to inject.
        assert!(hook.extra_execute_params(&parse("SELECT id FROM t WHERE project_id = $1")).is_empty());
        // Flag off → feature disabled entirely.
        assert!(PlanCacheHook::new(64, false).extra_execute_params(&mixed).is_empty());
    }

    #[test]
    fn date_time_fns_stay_unparameterizable() {
        // Date/Time-returning fns must NOT take the shape path (type risk).
        assert!(contains_unparameterizable_time_fn(&parse("SELECT id FROM t WHERE d = current_date")));
        assert!(!contains_unparameterizable_time_fn(&parse("SELECT id FROM t WHERE ts > now()")));
    }
}
