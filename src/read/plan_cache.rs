//! Cross-connection cache for parsed `LogicalPlan`s.
//!
//! Cached plans embed schemas: safe only while the compile-time schema registry
//! is immutable — schema hot reload must invalidate this cache.

use std::{
    cmp::Reverse,
    ops::ControlFlow,
    sync::{
        Arc, OnceLock,
        atomic::{
            AtomicBool, AtomicU64, AtomicUsize,
            Ordering::{AcqRel, Relaxed, Release},
        },
    },
};

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::{
    arrow::{
        array::{ArrayRef, UInt64Array, new_empty_array},
        compute::cast,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    common::{
        ParamValues,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    error::Result as DfResult,
    logical_expr::{Cast, Expr, LogicalPlan, Values, dml::WriteOp},
    prelude::SessionContext,
    scalar::ScalarValue,
    sql::{
        parser::Statement as DfStatement,
        sqlparser::{
            ast::{
                CastKind, DataType as SqlDataType, Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, OrderByKind, SelectItem,
                SetExpr, Statement, TimezoneInfo, Value, ValueWithSpan, visit_expressions, visit_expressions_mut,
            },
            tokenizer::Span,
        },
    },
};
use datafusion_postgres::{
    arrow_pg::encode_dataframe,
    hooks::{HookClient, QueryHook},
    pgwire::{
        api::{
            ClientInfo,
            portal::Format,
            results::{Response, Tag},
        },
        error::{ErrorInfo, PgWireError, PgWireResult},
        messages::response::TransactionStatus,
        types::format::FormatOptions,
    },
};
use itertools::Itertools;
use tracing::{debug, warn};

use crate::observability::{api_err, arrow_err};

/// Approximate retained bytes per expression node.
const PLAN_BYTES_PER_EXPR: usize = 384;

/// Retained plan budget per cache slot.
const PLAN_CACHE_PLAN_BYTES_PER_SLOT: usize = 128 * 1024;

/// Sweep to half the budget to avoid repeated boundary crossings.
const SWEEP_LOW_WATER_NUM: usize = 1;
const SWEEP_LOW_WATER_DEN: usize = 2;

/// Estimated retained size of a plan, in bytes.
fn plan_bytes(plan: &LogicalPlan) -> usize {
    let mut nodes = 0usize;
    let _ = plan.apply_with_subqueries(|p| {
        p.apply_expressions(|e| {
            e.apply(|_| {
                nodes += 1;
                Ok(TreeNodeRecursion::Continue)
            })
        })
    });
    nodes.saturating_mul(PLAN_BYTES_PER_EXPR)
}

/// A map bounded by entry count and retained weight.
struct WeighedMap<V> {
    map: DashMap<String, (V, usize)>,
    capacity: usize,
    max_bytes: usize,
    bytes: AtomicUsize,
    /// Prevents concurrent shard-locking sweeps.
    sweeping: AtomicBool,
}

impl<V: Clone> WeighedMap<V> {
    fn new(capacity: usize, bytes_per_slot: usize) -> Self {
        let capacity = capacity.max(1);
        Self { map: DashMap::new(), capacity, max_bytes: capacity.saturating_mul(bytes_per_slot), bytes: AtomicUsize::new(0), sweeping: AtomicBool::new(false) }
    }

    fn get(&self, key: &str) -> Option<V> {
        self.map.get(key).map(|e| e.value().0.clone())
    }

    fn bytes(&self) -> usize {
        self.bytes.load(Relaxed)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.len()
    }

    fn contains_key(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    /// Admit `value` at `weight`, sweeping first if either bound is crossed.
    /// An entry heavier than the whole budget is not admitted: it would evict
    /// everything else and still not survive.
    fn insert(&self, key: String, value: V, weight: usize, label: &'static str) {
        if (self.map.len() >= self.capacity || self.bytes() >= self.max_bytes) && !self.sweeping.swap(true, AcqRel) {
            self.sweep(label);
            self.sweeping.store(false, Release);
        }
        if weight < self.max_bytes {
            self.bytes.fetch_add(weight, Relaxed);
            if let Some((_, prev)) = self.map.insert(key, (value, weight)) {
                self.bytes.fetch_sub(prev, Relaxed);
            }
        }
    }

    /// Evict heaviest-first down to the low-water mark: the bytes are owed by a
    /// few bulk INSERTs while the population is mostly small SELECTs, so a
    /// random half would keep the pressure and drop the hot templates.
    fn sweep(&self, label: &'static str) {
        let low_water = self.max_bytes / SWEEP_LOW_WATER_DEN * SWEEP_LOW_WATER_NUM;
        warn!(
            target: "plan_cache",
            cache = label,
            size = self.map.len(),
            capacity = self.capacity,
            bytes = self.bytes(),
            max_bytes = self.max_bytes,
            low_water,
            "plan_cache over budget — evicting heaviest-first down to the low-water mark. If this fires steadily, the workload's plan-template variety has grown past the cache budget."
        );
        // `sorted_unstable_by_key` must stay EAGER: it releases every DashMap
        // shard guard before the loop calls `remove`. A lazy sort deadlocks.
        let by_weight = self.map.iter().map(|e| (e.value().1, e.key().clone())).sorted_unstable_by_key(|&(weight, _)| Reverse(weight));
        // Both bounds are swept in one pass: drop while EITHER is exceeded.
        let mut held = self.bytes();
        let mut len = self.map.len();
        for (weight, key) in by_weight {
            if held <= low_water && len < self.capacity {
                break;
            }
            if self.map.remove(&key).is_some() {
                held = held.saturating_sub(weight);
                len -= 1;
            }
        }
        self.bytes.store(held, Relaxed);
    }
}

/// Walk a plan and replace every `CAST(Literal(v), T)` with `Literal(cast(v, T))`,
/// so `ValuesExec` does not re-evaluate a constant cast per (row, column).
fn fold_literal_casts(plan: LogicalPlan) -> DfResult<LogicalPlan> {
    plan.transform_up(|node| {
        let folded: Vec<Transformed<Expr>> = node
            .expressions()
            .into_iter()
            .map(|expr| {
                expr.transform_up(|e| {
                    let Expr::Cast(Cast { expr, field }) = &e else { return Ok(Transformed::no(e)) };
                    let Expr::Literal(value, metadata) = expr.as_ref() else { return Ok(Transformed::no(e)) };
                    let data_type = field.data_type();
                    match value.cast_to(data_type) {
                        Ok(folded) => Ok(Transformed::yes(Expr::Literal(folded, metadata.clone()))),
                        // Uncastable literal stays put; the executor's cast reports it.
                        Err(err) => {
                            tracing::trace!(target: "plan_cache", %err, ?value, ?data_type, "fold_literal_casts: cast_to failed, leaving CAST for executor");
                            Ok(Transformed::no(e))
                        }
                    }
                })
            })
            .collect::<DfResult<_>>()?;
        // Only rebuild when a cast was folded: some nodes do not round-trip
        // through `expressions()`/`with_new_exprs` — notably `Unnest`, which
        // asserts an empty expr list (DF54).
        if !folded.iter().any(|t| t.transformed) {
            return Ok(Transformed::no(node));
        }
        let exprs = folded.into_iter().map(|t| t.data).collect();
        node.with_new_exprs(exprs, node.inputs().into_iter().cloned().collect()).map(Transformed::yes)
    })
    .map(|t| t.data)
}

/// pgwire-INSERT bypass: recognise `Dml(Insert) → [Projection →] Values(literals)`
/// and short-circuit the whole DataFusion executor by building the RecordBatch
/// directly from the literals and calling `ProjectRoutingTable.fast_insert_batch`.
///
/// Returns `Ok(Some(rows))` on success, `Ok(None)` if the plan shape isn't
/// the supported fast-path INSERT (caller should fall back to the regular
/// `execute_logical_plan` path).
async fn try_fast_path_insert(plan: &LogicalPlan, session_context: &SessionContext) -> DfResult<Option<u64>> {
    use datafusion::logical_expr::dml::DmlStatement;

    let LogicalPlan::Dml(DmlStatement { table_name, op: WriteOp::Insert(_), input, .. }) = plan else {
        return Ok(None);
    };

    // Input is either `Projection → Values` or `Values` directly. Each projected
    // column must resolve to a Values column or to a constant the optimizer
    // folded in; anything more complex falls back to the executor.
    enum ColumnSource {
        Values(usize),
        Constant(ScalarValue),
    }
    let (column_plan, values): (Option<Vec<(ColumnSource, String)>>, &Values) = match input.as_ref() {
        LogicalPlan::Projection(p) => {
            let LogicalPlan::Values(v) = p.input.as_ref() else {
                return Ok(None);
            };
            let plan: Option<Vec<_>> = p
                .expr
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    let (inner, name) = match e {
                        Expr::Alias(a) => (a.expr.as_ref(), a.name.clone()),
                        other => (other, p.schema.field(i).name().to_string()),
                    };
                    let src = match inner {
                        Expr::Column(c) => ColumnSource::Values(v.schema.fields().iter().position(|f| f.name() == &c.name)?),
                        Expr::Literal(val, _) => ColumnSource::Constant(val.clone()),
                        _ => return None,
                    };
                    Some((src, name))
                })
                .collect();
            let Some(plan) = plan else { return Ok(None) };
            (Some(plan), v)
        }
        LogicalPlan::Values(v) => (None, v),
        _ => return Ok(None),
    };

    // Every cell must be a literal, possibly wrapped in an Alias left over from
    // the `$N` placeholder name. `None` means "not the fast path".
    fn cell_as_literal(e: &Expr) -> Option<&ScalarValue> {
        match e {
            Expr::Literal(v, _) => Some(v),
            Expr::Alias(a) => cell_as_literal(&a.expr),
            _ => None,
        }
    }

    let values_schema: Arc<Schema> = Arc::new(values.schema.as_arrow().clone());
    let num_rows = values.values.len();

    // One array per Values column, in Values' native order.
    let Some(values_columns) = values_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(col_idx, field)| {
            let target_ty = field.data_type();
            let Some(scalars) = values.values.iter().map(|row| cell_as_literal(&row[col_idx]).cloned()).collect::<Option<Vec<_>>>() else {
                return Ok(None);
            };
            if scalars.is_empty() {
                return Ok(Some(new_empty_array(target_ty)));
            }
            let arr = ScalarValue::iter_to_array(scalars)?;
            // `iter_to_array` may return a different concrete type than declared
            // (all-NULL columns come back as Null); cast back so the downstream
            // MemBuffer schema check sees what the table expects.
            Ok(Some(if arr.data_type() == target_ty { arr } else { cast(&arr, target_ty).map_err(arrow_err)? }))
        })
        .collect::<DfResult<Vec<Option<ArrayRef>>>>()?
        .into_iter()
        .collect::<Option<Vec<ArrayRef>>>()
    else {
        return Ok(None);
    };

    // Apply the projection: pull Values columns by index, or materialize a
    // constant array for projection cells the optimizer folded to a literal.
    let (final_schema, columns) = match column_plan {
        Some(plan) => {
            let (fields, cols) = plan
                .iter()
                .map(|(src, name)| match src {
                    ColumnSource::Values(idx) => {
                        let f = values_schema.field(*idx);
                        Ok((Arc::new(Field::new(name, f.data_type().clone(), f.is_nullable())), values_columns[*idx].clone()))
                    }
                    ColumnSource::Constant(val) => {
                        let arr = val.to_array_of_size(num_rows)?;
                        Ok((Arc::new(Field::new(name, arr.data_type().clone(), true)), arr))
                    }
                })
                .collect::<DfResult<(Vec<Arc<Field>>, Vec<ArrayRef>)>>()?;
            (Arc::new(Schema::new(fields)), cols)
        }
        None => (values_schema, values_columns),
    };
    let batch = RecordBatch::try_new(final_schema, columns).map_err(arrow_err)?;

    let provider = session_context.table_provider(table_name.clone()).await?;
    let Some(routing) = provider.downcast_ref::<crate::database::ProjectRoutingTable>() else {
        return Ok(None);
    };
    let rows = routing.fast_insert_batch(batch).await?;
    Ok(Some(rows))
}

fn non_dml_err() -> PgWireError {
    PgWireError::ApiError("internal error: DML plan returned non-DML completion".into())
}

/// Mirror of `datafusion_postgres::handlers::dml_completion`, which is
/// `pub(super)` and so unreachable from outside that crate. Re-check parity when
/// bumping the patched dep: a changed tag format, `count` column name, or count
/// Arrow type diverges silently (no compile error, wrong wire response).
// RE-SYNC-DML-COMPLETION: keep in sync with datafusion-postgres src/handlers.rs.
async fn dml_completion(df: datafusion::dataframe::DataFrame) -> PgWireResult<Response> {
    let tag = match df.logical_plan() {
        LogicalPlan::Dml(d) => match d.op {
            WriteOp::Insert(_) => Tag::new("INSERT").with_oid(0),
            WriteOp::Update => Tag::new("UPDATE"),
            WriteOp::Delete => Tag::new("DELETE"),
            _ => return Err(non_dml_err()),
        },
        _ => return Err(non_dml_err()),
    };
    let batches = df.collect().await.map_err(api_err)?;
    let rows = batches
        .first()
        .and_then(|b| b.column_by_name("count"))
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .filter(|a| !a.is_empty()) // an empty count batch would make value(0) panic
        .map_or(0, |a| a.value(0) as usize);
    Ok(Response::Execution(tag.with_rows(rows)))
}

/// Execute an already-planned simple-protocol query under the client's
/// statement timeout, encoding rows (or the DML completion tag) for the wire.
async fn run_simple_query(
    plan: LogicalPlan, session_context: &SessionContext, timeout: Option<std::time::Duration>, format_options: Arc<FormatOptions>,
) -> PgWireResult<Response> {
    let exec = session_context.execute_logical_plan(plan);
    let df = match timeout {
        Some(d) => tokio::time::timeout(d, exec).await.map_err(|_| {
            PgWireError::UserError(Box::new(ErrorInfo::new("ERROR".into(), "57014".into(), "canceling statement due to statement timeout".into())))
        })?,
        None => exec.await,
    }
    .map_err(api_err)?;
    match df.logical_plan() {
        LogicalPlan::Dml(_) => dml_completion(df).await,
        _ => encode_dataframe(df, &Format::UnifiedText, Some(format_options)).await.map(Response::Query),
    }
}

/// Substitute + constant-fold a prepared DML plan, then take the fast INSERT
/// path if the shape allows it, else the regular executor.
async fn run_extended_dml(logical_plan: &LogicalPlan, params: &ParamValues, session_context: &SessionContext) -> PgWireResult<Response> {
    let substituted = logical_plan.clone().replace_params_with_values(params).map_err(api_err)?;
    let folded = fold_literal_casts(substituted).map_err(api_err)?;
    if let Some(rows) = try_fast_path_insert(&folded, session_context).await.map_err(api_err)? {
        return Ok(Response::Execution(Tag::new("INSERT").with_oid(0).with_rows(rows as usize)));
    }
    dml_completion(session_context.execute_logical_plan(folded).await.map_err(api_err)?).await
}

/// Plan → `insert_coerce::rewrite_plan` → optimize: the full miss-path build
/// shared by the verbatim cache and the `count(*)`-normalized fallback.
async fn plan_and_optimize(statement: Statement, session_context: &SessionContext) -> DfResult<LogicalPlan> {
    let state = session_context.state();
    state.statement_to_plan(DfStatement::Statement(Box::new(statement))).await.map(crate::write::rewrite_plan).and_then(|p| state.optimize(&p))
}

// Fallback when config isn't initialized; otherwise `memory.timefusion_plan_cache_capacity`.
const DEFAULT_PLAN_CACHE_CAPACITY: usize = 1024;

/// Soft cap on the `served` memo (one-shot literal-bearing texts).
const SERVED_CAP: usize = 4096;

/// Singleton handle so `timefusion_stats` can read the same cache the pgwire
/// factory writes to.
static GLOBAL: OnceLock<Arc<PlanCacheHook>> = OnceLock::new();

pub fn set_global(cache: Arc<PlanCacheHook>) {
    let _ = GLOBAL.set(cache);
}

pub fn global() -> Option<Arc<PlanCacheHook>> {
    GLOBAL.get().cloned()
}

/// Lock-free plan cache. No LRU ordering: OLAP workloads churn through a small
/// set of templates, and DashMap never holds a lock across the await in
/// `handle_simple_query`.
pub struct PlanCacheHook {
    cache: WeighedMap<LogicalPlan>,
    hits: AtomicU64,
    misses: AtomicU64,
    /// Shape cache for literal-bearing SELECTs that never repeat verbatim: keyed
    /// by the statement with literals replaced by `$N`, storing the pre-optimized
    /// placeholder plan + inferred parameter types. `None` = negative entry: this
    /// shape failed to plan/parameterize once; don't retry per query.
    /// Bounded by weight as well as count — it holds `LogicalPlan`s like `cache`.
    shapes: WeighedMap<Option<ShapeEntry>>,
    /// Canonical texts we served a pre-optimized substituted plan for, so
    /// `was_pre_optimized` can tell the handler to skip `state.optimize()`.
    /// A false `false` after eviction merely re-optimizes an optimized plan.
    served: DashMap<String, ()>,
    shape_hits: AtomicU64,
    shape_skips: AtomicU64,
    /// When true, `now()`-bearing SELECTs go through the shape cache with the
    /// time function parameterized (fresh instant per query) instead of being
    /// bypassed (`TIMEFUSION_PLAN_CACHE_TIME_FNS`).
    time_fn_shapes: bool,
}

#[derive(Clone)]
struct ShapeEntry {
    plan: LogicalPlan,
    /// Inferred DataType per `$N` (index 0 = `$1`); substituted literals are
    /// cast to these so the plan's expression types stay exact.
    param_types: Vec<Option<DataType>>,
}

/// What a shape-cache attempt lifts to `$N`, and whether it substitutes at parse.
#[derive(Clone, Copy)]
enum ShapeMode {
    /// Lift every literal (and any time fn) and substitute at parse.
    AllLiterals,
    /// Lift ONLY the time fn, leaving every other literal inline — required for
    /// now()-bearing queries, where lifting strings/numbers makes `INTERVAL $n`
    /// unplannable — then substitute at parse.
    TimeFnOnly,
    /// Mixed now()+client-`$N`: number the injected time-fn placeholders above
    /// the client's binds and return the template UNSUBSTITUTED, so
    /// `extra_execute_params` supplies a fresh instant on every execute and the
    /// window never freezes even for a reused named prepared statement.
    MixedWithClientBinds,
}

/// True if the optimized plan would embed the query start time: DataFusion
/// const-folds these Stable fns in `state.optimize()`, so caching the result
/// would freeze `now()` at first-build time. Such statements re-plan per query.
fn contains_plan_time_folded_fn(stmt: &Statement) -> bool {
    stmt_uses_fn(stmt, PARAMETERIZABLE_TIME_FNS) || stmt_uses_fn(stmt, UNPARAMETERIZABLE_TIME_FNS)
}

/// The timestamp-returning time fns we can safely parameterize (replace the call
/// with a `$N` placeholder bound to the current instant, so the plan is reusable
/// and time stays fresh).
const PARAMETERIZABLE_TIME_FNS: &[&str] = &["now", "current_timestamp", "statement_timestamp", "transaction_timestamp", "clock_timestamp", "localtimestamp"];

/// Date/Time-returning time fns — different result type, riskier substitution —
/// so a query using any of these stays on the bypass path.
const UNPARAMETERIZABLE_TIME_FNS: &[&str] = &["current_date", "today", "current_time", "localtime"];

/// True if `e` is exactly the bare `count(*)` idiom.
fn is_count_star(e: &SqlExpr) -> bool {
    matches!(e, SqlExpr::Function(f)
        if fn_name_is_one_of(f, &["count"])
            && matches!(&f.args, FunctionArguments::List(l) if matches!(l.args.as_slice(), [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)])))
}

/// Rewrite `count(*)` to `count(1)`, but ONLY for a statement DataFusion rejects
/// today: one whose `ORDER BY <ordinal>` points at a select item that wraps
/// `count(*)` in a larger expression. `None` for everything else.
///
/// The narrowness is a correctness requirement: `count(*)` and `count(1)` do not
/// NAME the same column, so rewriting a query that already plans would change its
/// wire-visible column name. A query that errors has no output contract to break.
/// A bare `count(*) … ORDER BY 1` already resolves and must not get the rewrite.
fn normalize_count_star(stmt: &Statement) -> Option<Statement> {
    let Statement::Query(query) = stmt else { return None };
    let OrderByKind::Expressions(order_exprs) = &query.order_by.as_ref()?.kind else { return None };
    let SetExpr::Select(select) = &*query.body else { return None };

    // An ordinal is 1-based and only an integer literal counts; `ORDER BY x`
    // resolves by name and never hits this bug.
    let points_at_wrapped_count_star = order_exprs.iter().any(|o| {
        let SqlExpr::Value(v) = &o.expr else { return false };
        let Value::Number(n, _) = &v.value else { return false };
        let Some(item) = n.parse::<usize>().ok().filter(|i| *i > 0).and_then(|i| select.projection.get(i - 1)) else { return false };
        let (SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. }) = item else { return false };
        // Bare `count(*)` already resolves; only a WRAPPED one breaks.
        !is_count_star(e)
            && visit_expressions(e, |inner: &SqlExpr| if is_count_star(inner) { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }).is_break()
    });
    if !points_at_wrapped_count_star {
        return None;
    }

    let mut out = stmt.clone();
    let _: ControlFlow<()> = visit_expressions_mut(&mut out, |e: &mut SqlExpr| {
        if is_count_star(e)
            && let SqlExpr::Function(f) = e
            && let FunctionArguments::List(list) = &mut f.args
            && let [FunctionArg::Unnamed(arg)] = list.args.as_mut_slice()
        {
            *arg = FunctionArgExpr::Expr(SqlExpr::Value(ValueWithSpan { value: Value::Number("1".into(), false), span: Span::empty() }));
        }
        ControlFlow::Continue(())
    });
    Some(out)
}

/// Case-insensitive match of a call's last name segment against `names`.
fn fn_name_is_one_of(f: &Function, names: &[&str]) -> bool {
    f.name.0.last().and_then(|n| n.as_ident()).is_some_and(|i| names.iter().any(|n| n.eq_ignore_ascii_case(&i.value)))
}

fn contains_unparameterizable_time_fn(stmt: &Statement) -> bool {
    stmt_uses_fn(stmt, UNPARAMETERIZABLE_TIME_FNS)
}

/// True if `stmt` calls any function named in `names`. Shared AST-visitor for
/// the time-fn classifiers.
fn stmt_uses_fn(stmt: &Statement, names: &[&str]) -> bool {
    visit_expressions(stmt, |e: &SqlExpr| match e {
        SqlExpr::Function(f) if fn_name_is_one_of(f, names) => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

/// Highest client-supplied `$N` placeholder index already in `stmt` (0 if none),
/// so injected time-fn placeholders can be numbered above the client's.
fn max_placeholder_index(stmt: &Statement) -> usize {
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
    // A regex `SUBSTRING(x FROM 'pat')` must keep its pattern inline: lifted to
    // `$N` it becomes an untyped placeholder that coerces to substr's declared
    // Int64 arg 2 and the query dies in `simplify_expressions`. The whole
    // statement opts out of shape caching so the literal reaches the planner.
    // The offset forms carry `Value::Number` and are unaffected.
    let has_regex_substring = visit_expressions(stmt, |e: &SqlExpr| match e {
        SqlExpr::Substring { substring_from: Some(from), .. } if matches!(&**from, SqlExpr::Value(vs) if matches!(vs.value, Value::SingleQuotedString(_))) => {
            ControlFlow::Break(())
        }
        _ => ControlFlow::Continue(()),
    })
    .is_break();
    if has_regex_substring {
        return None;
    }

    let mut stmt = stmt.clone();
    let mut values: Vec<ScalarValue> = Vec::new();

    // Push `v` and return the `$N` placeholder referencing its position.
    //
    // An identical literal must REUSE its placeholder: DataFusion requires a
    // SELECT expression to appear in GROUP BY as the *same* expression, so
    // giving two occurrences of one literal distinct placeholders makes e.g.
    // `time_bucket(60, ts)` in SELECT and GROUP BY stop matching. Reuse is
    // semantics-preserving; it only narrows the template.
    fn placeholder_for(values: &mut Vec<ScalarValue>, base: usize, v: ScalarValue) -> Value {
        let idx = values.iter().position(|existing| *existing == v).unwrap_or_else(|| {
            values.push(v);
            values.len() - 1
        });
        Value::Placeholder(format!("${}", base + idx + 1))
    }

    // Parameterize a numeric literal ONLY when reached as a value-context child
    // (function arg, comparison operand, CASE/BETWEEN/cast). A bare
    // `Expr::Value(Number)` — exactly what GROUP BY / ORDER BY ordinals and
    // LIMIT / OFFSET are — is never a child of these containers, so ordinals keep
    // their positional meaning. Unparseable numbers stay inline.
    fn take_numbers<'a>(exprs: impl IntoIterator<Item = &'a mut SqlExpr>, base: usize, values: &mut Vec<ScalarValue>) {
        for e in exprs {
            if let SqlExpr::Value(vs) = e
                && let Value::Number(n, _) = &vs.value
                && let Some(sv) =
                    n.parse::<i64>().map(|i| ScalarValue::Int64(Some(i))).ok().or_else(|| n.parse::<f64>().ok().map(|f| ScalarValue::Float64(Some(f))))
            {
                vs.value = placeholder_for(values, base, sv);
            }
        }
    }
    // Capture "now" once so every now()/current_timestamp substitutes to the same
    // instant (SQL's single-evaluation semantics). Microseconds, the timestamp
    // columns' precision: a finer instant makes coercion cast the column instead.
    let now_us = chrono::Utc::now().timestamp_micros();
    let _: ControlFlow<()> = visit_expressions_mut(&mut stmt, |e: &mut SqlExpr| {
        match e {
            // PG array literals ('{}', '{a,b}') must stay inline: PgArrayLiteralRewriter
            // matches only Expr::Literal, so a `$N` placeholder slips past it and is
            // mis-cast to a single-element list.
            SqlExpr::Value(vs) if include_strings => {
                if let Value::SingleQuotedString(s) = &vs.value
                    && !s.trim_start().starts_with('{')
                {
                    vs.value = placeholder_for(&mut values, base, ScalarValue::Utf8(Some(s.clone())));
                }
            }
            // now()/current_timestamp/… → placeholder bound to the captured instant,
            // so the optimized plan is reusable while the time window stays fresh.
            SqlExpr::Function(f) if fn_name_is_one_of(f, PARAMETERIZABLE_TIME_FNS) => {
                let value = placeholder_for(&mut values, base, ScalarValue::TimestampMicrosecond(Some(now_us), Some("+00:00".into())));
                let placeholder = SqlExpr::Value(ValueWithSpan { value, span: Span::empty() });
                // The CAST is required: a bare placeholder is untyped, so
                // `now() - INTERVAL '1h'` cannot infer a common argument type.
                *e = SqlExpr::Cast {
                    kind: CastKind::Cast,
                    expr: Box::new(placeholder),
                    data_type: SqlDataType::Timestamp(Some(6), TimezoneInfo::Tz),
                    format: None,
                    array: false,
                };
            }
            // Every arm BELOW this sentinel requires `include_strings`; the time-fn
            // arm above deliberately does not. The mixed now()+`$N` execute path
            // binds time-fn placeholders positionally and must gain no others.
            _ if !include_strings => {}
            SqlExpr::BinaryOp { left, right, .. } => take_numbers([&mut **left, &mut **right], base, &mut values),
            SqlExpr::UnaryOp { expr, .. } | SqlExpr::Nested(expr) | SqlExpr::Cast { expr, .. } => take_numbers([&mut **expr], base, &mut values),
            SqlExpr::Between { expr, low, high, .. } => take_numbers([&mut **expr, &mut **low, &mut **high], base, &mut values),
            SqlExpr::InList { expr, list, .. } => take_numbers(std::iter::once(&mut **expr).chain(list.iter_mut()), base, &mut values),
            // Walk order (operand → conditions → else) fixes `$N` numbering; keep it.
            SqlExpr::Case { operand, conditions, else_result, .. } => take_numbers(
                operand
                    .iter_mut()
                    .map(|e| &mut **e)
                    .chain(conditions.iter_mut().flat_map(|w| [&mut w.condition, &mut w.result]))
                    .chain(else_result.iter_mut().map(|e| &mut **e)),
                base,
                &mut values,
            ),
            SqlExpr::Function(Function { args: FunctionArguments::List(list), .. }) => take_numbers(
                list.args.iter_mut().filter_map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. }
                    | FunctionArg::ExprNamed { arg: FunctionArgExpr::Expr(e), .. } => Some(e),
                    _ => None,
                }),
                base,
                &mut values,
            ),
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
            cache: WeighedMap::new(capacity, PLAN_CACHE_PLAN_BYTES_PER_SLOT),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            shapes: WeighedMap::new(capacity, PLAN_CACHE_PLAN_BYTES_PER_SLOT),
            served: DashMap::new(),
            shape_hits: AtomicU64::new(0),
            shape_skips: AtomicU64::new(0),
            time_fn_shapes,
        }
    }

    /// Returns (hits, misses) for stats observability.
    pub fn counters(&self) -> (u64, u64) {
        (self.hits.load(Relaxed), self.misses.load(Relaxed))
    }

    /// Returns (shape_hits, shape_skips) for stats observability.
    pub fn shape_counters(&self) -> (u64, u64) {
        (self.shape_hits.load(Relaxed), self.shape_skips.load(Relaxed))
    }

    /// Shape-cache path for literal-bearing SELECTs. Returns a pre-optimized
    /// plan — fully substituted, or (for `MixedWithClientBinds`) the template
    /// with the client's `$N` still open — or `None` to fall back to the normal
    /// parse→optimize pipeline. Every failure installs a negative entry so a
    /// shape that can't parameterize is only attempted once.
    async fn try_shape_cached_plan(&self, statement: &Statement, canonical: &str, session_context: &SessionContext, mode: ShapeMode) -> Option<LogicalPlan> {
        if !matches!(statement, Statement::Query(_)) {
            return None;
        }
        let mixed = matches!(mode, ShapeMode::MixedWithClientBinds);
        let base = if mixed { self.mixed_time_fn_base(statement)? } else { 0 };
        let (param_stmt, values) = parameterize_statement(statement, base, matches!(mode, ShapeMode::AllLiterals))?;
        let shape_key = param_stmt.to_string();
        // The mixed path binds its injected params at execute, so it records no
        // leading `$N` types to cast against.
        let entry = self.get_or_build_shape(&shape_key, param_stmt, if mixed { 0 } else { values.len() }, session_context).await?;

        let plan = if mixed {
            entry.plan
        } else {
            // Substitute this query's literals, cast to the inferred types.
            let cast_values: Vec<ScalarValue> =
                values.into_iter().zip(entry.param_types.iter()).map(|(v, ty)| ty.as_ref().and_then(|t| v.cast_to(t).ok()).unwrap_or(v)).collect();
            let plan = entry.plan.replace_params_with_values(&ParamValues::List(cast_values.into_iter().map(Into::into).collect())).ok()?;
            fold_literal_casts(plan).ok()?
        };
        self.mark_served(canonical);
        Some(plan)
    }

    /// Record a shape hit and memo the canonical text so `was_pre_optimized`
    /// tells the handler to skip `state.optimize()`.
    fn mark_served(&self, canonical: &str) {
        self.shape_hits.fetch_add(1, Relaxed);
        // Soft cap: these texts are one-shot, so losing a memo only costs a
        // redundant re-optimize.
        if self.served.len() >= SERVED_CAP {
            self.served.retain(|_, _| fastrand::bool());
        }
        self.served.insert(canonical.to_string(), ());
    }

    /// Get or build+optimize+cache the placeholder template for `shape_key`.
    /// `value_count` = how many leading `$N` types to record for the caller to
    /// cast its client literals against (0 for the mixed path, which binds its
    /// injected params by inferred type at execute). `None` = negative entry:
    /// this shape failed to plan once; don't retry per query.
    async fn get_or_build_shape(&self, shape_key: &str, param_stmt: Statement, value_count: usize, session_context: &SessionContext) -> Option<ShapeEntry> {
        if let Some(e) = self.shapes.get(shape_key) {
            return e; // Some(entry) hit / None negative
        }
        // Build the placeholder plan once; log rather than swallow the error so a
        // negative-caching shape is diagnosable.
        let state = session_context.state();
        let built = state
            .statement_to_plan(DfStatement::Statement(Box::new(param_stmt)))
            .await
            .and_then(|p| state.optimize(&p))
            .inspect_err(|e| warn!(target: "plan_cache", "shape build failed: {shape_key} — {e}"))
            .ok()
            .and_then(|plan| {
                let types = if value_count == 0 { Default::default() } else { plan.get_parameter_types().ok()? };
                Some(ShapeEntry { plan, param_types: (1..=value_count).map(|i| types.get(&format!("${i}")).cloned().flatten()).collect() })
            });
        if built.is_none() {
            self.shape_skips.fetch_add(1, Relaxed);
        }
        // A negative entry weighs only its key.
        let weight = built.as_ref().map_or(0, |e| plan_bytes(&e.plan)) + shape_key.len();
        self.shapes.insert(shape_key.to_string(), built.clone(), weight, "shape");
        built
    }

    /// Base index for the mixed now()+client-`$N` path: the client's highest
    /// `$N`, when this statement is one we inject time-fn placeholders above.
    /// `None` = not a mixed-path statement, so nothing to inject.
    ///
    /// Filtering out base 0 is load-bearing: `has_placeholder` is a TEXT scan, so
    /// a `'$1'` string literal routes a bind-less statement here and the template
    /// would be served (and cached) with an unsubstituted `$1`. The AST decides.
    fn mixed_time_fn_base(&self, stmt: &Statement) -> Option<usize> {
        (self.time_fn_shapes
            && matches!(stmt, Statement::Query(_))
            && !contains_unparameterizable_time_fn(stmt)
            && stmt_uses_fn(stmt, PARAMETERIZABLE_TIME_FNS))
        .then(|| max_placeholder_index(stmt))
        .filter(|&base| base > 0)
    }

    /// The cached-plan lookup shared by BOTH protocol paths: cheap AST-kind
    /// gate, the time-fn guards, then the shape / verbatim caches. `None` =
    /// not cacheable, caller falls back to the normal parse→optimize pipeline.
    /// Also normalizes `count(*)` before planning.
    ///
    /// The normalization must be an AST rewrite, not a text rewrite: over raw SQL
    /// it would also hit INSERT payloads that contain `count(*)` as data.
    ///
    /// A rewritten statement must never return `None`: the caller would then fall
    /// through to a planner that re-plans the ORIGINAL (broken) statement. So when
    /// the normal path declines a rewritten statement, plan it here.
    async fn cached_plan(&self, statement: &Statement, session_context: &SessionContext) -> Option<PgWireResult<LogicalPlan>> {
        // Cheap AST-variant gate first: avoids `Statement::to_string()` on every
        // Parse message regardless of cacheability.
        if !matches!(statement, Statement::Insert(_) | Statement::Query(_) | Statement::Update { .. } | Statement::Delete(_)) {
            return None;
        }
        let Some(normalized) = normalize_count_star(statement) else {
            return self.cached_plan_normalized(statement, session_context).await;
        };
        match self.cached_plan_normalized(&normalized, session_context).await {
            Some(result) => Some(result),
            None => Some(plan_and_optimize(normalized, session_context).await.map_err(api_err)),
        }
    }

    async fn cached_plan_normalized(&self, statement: &Statement, session_context: &SessionContext) -> Option<PgWireResult<LogicalPlan>> {
        // now()/current_date/... are const-folded against the query start time, so
        // a verbatim-cached optimized plan would freeze them. With time-fn shape
        // caching on, route now()-bearing SELECTs to the shape path; otherwise,
        // and for unparameterizable date/time fns, plan fresh.
        if contains_plan_time_folded_fn(statement) {
            if self.time_fn_shapes && matches!(statement, Statement::Query(_)) && !contains_unparameterizable_time_fn(statement) {
                let canonical = statement.to_string();
                // Mixed now()+client `$N`: template keeps both open; the fresh
                // instant is injected per-execute by extra_execute_params.
                // Otherwise pure now()-bearing: lift ONLY now(), keeping other
                // literals inline so INTERVAL/time_bucket plan.
                let mode = if Self::has_placeholder(&canonical) { ShapeMode::MixedWithClientBinds } else { ShapeMode::TimeFnOnly };
                return self.try_shape_cached_plan(statement, &canonical, session_context, mode).await.map(Ok);
            }
            return None;
        }
        let canonical = statement.to_string();
        if !Self::has_placeholder(&canonical) {
            // Literal-bearing SELECT (no now()): lift all literals.
            return self.try_shape_cached_plan(statement, &canonical, session_context, ShapeMode::AllLiterals).await.map(Ok);
        }

        if let Some(plan) = self.cache.get(&canonical) {
            self.hits.fetch_add(1, Relaxed);
            debug!(target: "plan_cache", %canonical, "plan cache hit");
            return Some(Ok(plan));
        }

        // Miss: build the plan, install it, hand a clone back to caller.
        self.misses.fetch_add(1, Relaxed);
        // The stored plan is already optimized: the patched datafusion-postgres
        // skips its own `state.optimize()` when the hook returns Some. Safe because
        // non-constant-fold rules are parameter-independent, and time-folding
        // statements never reach here.
        let plan = match plan_and_optimize(statement.clone(), session_context).await {
            Ok(p) => p,
            Err(e) => return Some(Err(api_err(e))),
        };
        // Admit at the plan's own estimated weight, not the length of the SQL that
        // produced it — the two differ by orders of magnitude for a bulk INSERT.
        let weight = plan_bytes(&plan) + canonical.len();
        self.cache.insert(canonical, plan.clone(), weight, "template");
        Some(Ok(plan))
    }

    fn has_placeholder(sql: &str) -> bool {
        // A bare `contains('$')` false-positives on literals like '$100'.
        sql.as_bytes().windows(2).any(|w| w[0] == b'$' && w[1].is_ascii_digit())
    }
}

#[async_trait]
impl QueryHook for PlanCacheHook {
    /// Serve simple-protocol queries from the same caches the extended path uses.
    /// Ad-hoc SQL arrives with literals inline, so the *shape* cache is what fires
    /// here; `cached_plan` returning `None` falls through to the vendored
    /// `session_context.sql()` path unchanged.
    async fn handle_simple_query(
        &self, statement: &Statement, session_context: &SessionContext, client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        // The TransactionStatementHook runs AFTER us and is what rejects
        // statements inside a failed transaction block; answering here would
        // silently execute them. Defer to it.
        if client.transaction_status() == TransactionStatus::Error {
            return None;
        }
        // On a plan-build error, fall through: the vendored path produces the same
        // error with its own context.
        let plan = self.cached_plan(statement, session_context).await?.ok()?;
        let timeout = client.metadata().get("statement_timeout_ms").and_then(|s| s.parse::<u64>().ok()).map(std::time::Duration::from_millis);
        let format_options = Arc::new(FormatOptions::from_client_metadata(client.metadata()));
        Some(run_simple_query(plan, session_context, timeout, format_options).await)
    }

    /// Trailing placeholders the mixed path injects — the Parse/Describe path
    /// hides these from the client's ParameterDescription. Equals the number of
    /// values `extra_execute_params` appends for the same statement.
    fn injected_param_count(&self, statement: Option<&Statement>) -> usize {
        self.extra_execute_params(statement).len()
    }

    /// Fresh instant(s) for the time-fn placeholders the mixed now()+`$N` path
    /// injected at parse (numbered above the client's binds). Appended to the
    /// client's params before substitution, so `now()` is re-evaluated on every
    /// execute. Empty for the pure path (M=0, substituted at parse) and for any
    /// statement we didn't shape-cache — surplus is ignored by the executor.
    fn extra_execute_params(&self, statement: Option<&Statement>) -> Vec<ScalarValue> {
        // `None` statement = an AST the portal store no longer pins; `None` base =
        // the pure path or a statement we never shape-cached. Nothing to inject.
        let Some(statement) = statement else { return Vec::new() };
        self.mixed_time_fn_base(statement).and_then(|base| parameterize_statement(statement, base, false)).map_or_else(Vec::new, |(_, values)| values)
    }

    async fn handle_extended_parse_query(
        &self, statement: &Statement, session_context: &SessionContext, _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        self.cached_plan(statement, session_context).await
    }

    async fn handle_extended_query(
        &self, _statement: Option<&Statement>, logical_plan: &LogicalPlan, params: &ParamValues, session_context: &SessionContext, _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        // Only intercept DML — for SELECTs the vendored path is fine. The win is
        // post-substitution constant folding of the per-placeholder `CAST`s.
        if !matches!(logical_plan, LogicalPlan::Dml(_)) {
            return None;
        }
        Some(run_extended_dml(logical_plan, params, session_context).await)
    }

    /// Signal to the do_query path that any plan we returned is already optimized,
    /// so `state.optimize()` can be skipped. A sweep between this lookup and the
    /// handler's use of the plan can make this falsely return `false`; that only
    /// costs a redundant re-optimize of an already-optimized plan.
    fn was_pre_optimized(&self, canonical_sql: &str) -> bool {
        self.cache.contains_key(canonical_sql) || self.served.contains_key(canonical_sql)
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    fn parse(sql: &str) -> Statement {
        use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
        Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap().remove(0)
    }

    /// The pure shape path (no client binds): rendered shape key + lifted values.
    fn shape(sql: &str) -> (String, Vec<ScalarValue>) {
        let (param, values) = parameterize_statement(&parse(sql), 0, true).expect("has literals");
        (param.to_string(), values)
    }

    fn utf8(s: &str) -> ScalarValue {
        ScalarValue::Utf8(Some(s.into()))
    }

    /// A lifted time fn may only ever be a tz-aware microsecond timestamp.
    fn ts_micros(v: &ScalarValue) -> i64 {
        ts_micros_opt(v).unwrap_or_else(|| panic!("expected tz-aware microsecond timestamp, got {v:?}"))
    }

    fn ts_micros_opt(v: &ScalarValue) -> Option<i64> {
        match v {
            ScalarValue::TimestampMicrosecond(Some(us), Some(_)) => Some(*us),
            _ => None,
        }
    }

    proptest::proptest! {
        /// The premise of the cross-connection cache: the shape key depends on the
        /// AST's SHAPE only, never on the literal values it carries.
        #[test]
        fn shape_key_ignores_literal_values(p in "[a-z]{1,8}", q in "[a-z]{1,8}", n in 0i64..10_000, m in 0i64..10_000) {
            let sql = |proj: &str, num: i64| format!("SELECT id FROM t WHERE project_id = '{proj}' AND n = {num} LIMIT 10");
            proptest::prop_assert_eq!(shape(&sql(&p, n)).0, shape(&sql(&q, m)).0);
        }
    }

    /// One literal, three `time_bucket` calls across SELECT / GROUP BY / ORDER BY:
    /// they must lift to the SAME placeholder or the GROUP BY stops matching the
    /// SELECT and the shape build fails.
    #[test]
    fn one_literal_lifts_to_one_placeholder_so_group_by_still_matches_select() {
        let (text, values) = shape(
            "SELECT EXTRACT(EPOCH FROM time_bucket(60, timestamp)), count(*) FROM t \
             WHERE project_id = 'p' GROUP BY time_bucket(60, timestamp) ORDER BY time_bucket(60, timestamp) DESC",
        );
        assert_eq!(values, vec![ScalarValue::Int64(Some(60)), utf8("p")], "the repeated 60 is stored once");
        assert_eq!(text.matches("time_bucket($1,").count(), 3, "all three time_bucket calls share one placeholder: {text}");
        assert!(!text.contains("$3"), "no placeholder beyond the two distinct literals: {text}");
    }

    /// `SUBSTRING(x FROM 'pat')` is PG regex extraction and the pattern must reach
    /// the planner as a literal, so the statement opts out of shape caching.
    #[test_case("SELECT substring('abc-def' FROM '^[a-z]+')" => matches None ; "a regex substring pattern is never lifted to a placeholder")]
    #[test_case("SELECT substring(body FROM 'HTTP/[0-9.]+') FROM t WHERE project_id = 'p'" => matches None ; "regex substring over a real table opts out too")]
    // No string/number/time-fn literals to lift → nothing to cache-generalize.
    #[test_case("SELECT count(*) FROM t" => matches None ; "parameterize none without any literals")]
    fn parameterize_declines_shape_caching(sql: &str) -> Option<(Statement, Vec<ScalarValue>)> {
        parameterize_statement(&parse(sql), 0, true)
    }

    /// Literals that are NOT ordinal-safe value contexts must survive verbatim in
    /// the shape key; in each case only the `'p'` project id may be lifted. Where a
    /// `twin` is given it differs from `sql` ONLY in its literals, so both must
    /// render the identical shape key.
    // Substring OFFSETs carry a number, so the statement still caches.
    #[test_case("SELECT substring(body FROM 3) FROM t WHERE project_id = 'p'", None::<&str>, &["FROM 3"], &[] => vec![utf8("p")] ; "an offset substring still caches, offset inline")]
    // Ordinals and LIMIT/OFFSET are bare Number nodes; parameterizing them would
    // turn ORDER BY 1 into ordering by a constant.
    #[test_case("SELECT status_code, count(*) FROM t WHERE project_id = 'p' GROUP BY 1 ORDER BY 1 LIMIT 100 OFFSET 20", None::<&str>, &["GROUP BY 1", "ORDER BY 1", "LIMIT 100", "OFFSET 20"], &[]
        => vec![utf8("p")] ; "ordinals and limit stay inline")]
    // A $N placeholder hides an array literal from PgArrayLiteralRewriter (which
    // matches Expr::Literal only), mis-casting it to a single-element list.
    #[test_case("SELECT ARRAY_LENGTH(COALESCE(parent_id, '{a,b}')) FROM t WHERE project_id = 'p'", None::<&str>, &["'{a,b}'"], &[] => vec![utf8("p")] ; "parameterize keeps pg array literals inline")]
    // Strings and the comparison number lift, in walk order; LIMIT is not an
    // ordinal-safe value context so it stays inline.
    #[test_case("SELECT id FROM t WHERE project_id = 'p1' AND ts > '2026-07-01' AND n = 5 LIMIT 100", Some("SELECT id FROM t WHERE project_id = 'p2' AND ts > '2026-07-04' AND n = 9 LIMIT 100"),
        &["$1", "$2", "$3", "LIMIT 100"], &[]
        => vec![utf8("p1"), utf8("2026-07-01"), ScalarValue::Int64(Some(5))] ; "parameterize extracts strings and value context numbers in walk order")]
    // Shapes differing only by numeric literals must collapse to one entry.
    #[test_case("SELECT time_bucket(60, timestamp), approx_percentile(0.95, duration) FROM t WHERE project_id = 'p' AND duration <= 500 AND timestamp >= 1721000000000000",
        Some("SELECT time_bucket(300, timestamp), approx_percentile(0.99, duration) FROM t WHERE project_id = 'p' AND duration <= 900 AND timestamp >= 1722000000000000"),
        &[], &["60", "0.95", "500"]
        => with |v: Vec<ScalarValue>| assert_eq!(v.len(), 5, "4 numbers + the 'p' string all captured: {v:?}") ; "numeric literals in value contexts parameterize")]
    fn only_the_unsafe_literals_stay_inline(sql: &str, twin: Option<&str>, inline: &[&str], replaced: &[&str]) -> Vec<ScalarValue> {
        let (text, values) = shape(sql);
        for frag in inline {
            assert!(text.contains(frag), "{frag} must stay inline: {text}");
        }
        for frag in replaced {
            assert!(!text.contains(frag), "{frag} must be replaced by a placeholder: {text}");
        }
        if let Some(twin) = twin {
            assert_eq!(text, shape(twin).0, "shape key must be literal-insensitive");
        }
        values
    }

    /// Only statements DataFusion rejects today may be rewritten: a working query
    /// must keep its exact output column names. The INSERT case pins that this is
    /// an AST rewrite, so SQL payload data containing `count(*)` is untouched.
    #[test_case("SELECT COUNT(*)::int8 FROM t" => matches None ; "no order by at all")]
    #[test_case("SELECT src, COUNT(*) FROM t GROUP BY src ORDER BY 2 DESC" => matches None ; "ordinal resolves to a bare count(*)")]
    #[test_case("SELECT src, COUNT(*)::int8 FROM t GROUP BY src ORDER BY 1 ASC" => matches None ; "ordinal points elsewhere")]
    #[test_case("SELECT src, COUNT(*)::int8 AS c FROM t GROUP BY src ORDER BY c DESC" => matches None ; "order by alias resolves")]
    #[test_case("SELECT COUNT(id)::int8 FROM t ORDER BY 1" => matches None ; "counts a column")]
    #[test_case("SELECT COUNT(DISTINCT level)::int8 FROM t ORDER BY 1" => matches None ; "count distinct")]
    #[test_case("SELECT SUM(d)::int8 FROM t ORDER BY 1" => matches None ; "not a count at all")]
    #[test_case("SELECT COUNT(t.*)::int8 FROM t ORDER BY 1" => matches None ; "qualified wildcard is not the idiom")]
    #[test_case("INSERT INTO t (body) VALUES ('the query was count(*) over spans')" => matches None ; "insert data containing the text count star is never rewritten")]
    #[test_case("SELECT src, COUNT(*)::int8 FROM t GROUP BY src ORDER BY 2 DESC" => with |o: Option<String>| {
        let out = o.expect("rewritten");
        assert!(out.contains("COUNT(1)"), "count(*) becomes count(1): {out}");
        assert!(!out.contains("COUNT(*)"), "no wildcard call survives: {out}");
    } ; "count star is normalized so an order by ordinal can resolve it")]
    #[test_case("SELECT a, CAST(COUNT(*) AS int8), 'count(*)' FROM s GROUP BY a ORDER BY 2 DESC" => with |o: Option<String>| {
        let out = o.expect("rewritten");
        assert!(out.contains("COUNT(1)") && out.contains("'count(*)'"), "call rewritten, neighbouring literal preserved: {out}");
    } ; "a string literal beside a genuine rewrite survives untouched")]
    fn normalize_count_star_rewrites_only_the_broken_ordinal_shape(sql: &str) -> Option<String> {
        normalize_count_star(&parse(sql)).map(|s| s.to_string())
    }

    /// The optimizer const-folds time fns from the query start time, so caching
    /// the optimized plan would freeze the window. The Date/Time-returning subset
    /// must additionally stay off the shape path.
    #[test_case("SELECT id FROM t WHERE project_id = 'p' AND ts > now()" => (true, false) ; "now() folds but parameterizes")]
    #[test_case("SELECT id FROM t WHERE ts > now()" => (true, false) ; "now() folds, bare predicate")]
    #[test_case("SELECT id FROM t WHERE project_id = 'p' AND ts > NOW() - INTERVAL '1 hour'" => (true, false) ; "now() inside an interval arithmetic")]
    #[test_case("SELECT id FROM t WHERE project_id = 'p' AND d = current_date" => (true, true) ; "date fns stay unparameterizable")]
    #[test_case("SELECT id FROM t WHERE d = current_date" => (true, true) ; "current_date, bare predicate")]
    #[test_case("SELECT id FROM t WHERE project_id = 'p' AND ts > '2026-07-01'" => (false, false) ; "a timestamp literal is not a time fn")]
    // A column merely NAMED now must not disqualify.
    #[test_case("SELECT now FROM t WHERE project_id = 'p'" => (false, false) ; "a column named now is not a call")]
    fn time_fn_classification(sql: &str) -> (bool, bool) {
        let stmt = parse(sql);
        (contains_plan_time_folded_fn(&stmt), contains_unparameterizable_time_fn(&stmt))
    }

    #[test_case("SELECT id FROM t WHERE project_id = $1 AND n = $3" => 3 ; "highest client bind wins")]
    #[test_case("SELECT id FROM t WHERE project_id = 'p'" => 0 ; "no binds at all")]
    fn max_placeholder_index_finds_highest_client_bind(sql: &str) -> usize {
        max_placeholder_index(&parse(sql))
    }

    /// `now()` lifts to a placeholder bound to a FRESH instant (caching a folded
    /// one would freeze the window) — on the pure path and on the mixed path,
    /// where the generated index must land above the client's highest `$N`.
    // Pure path: strings lift too, and the `twin` proves the key is reusable across refreshes.
    #[test_case("SELECT id FROM t WHERE project_id = 'p' AND ts > now() - INTERVAL '1 hour'", 0, true, &["$1", "$2"],
        Some("SELECT id FROM t WHERE project_id = 'q' AND ts > now() - INTERVAL '1 hour'") => 3 ; "pure path lifts project_id, now() and the interval string")]
    // Mixed path: client $1 kept, now() becomes $2, and the string stays inline.
    #[test_case("SELECT id FROM t WHERE project_id = $1 AND level = 'error' AND ts > now() - INTERVAL '1 hour'", 1, false, &["$1", "$2", "'error'"],
        None::<&str> => 1 ; "mixed parameterizes time fns above client binds only")]
    fn now_lifts_to_a_fresh_timestamp_placeholder(sql: &str, base: usize, include_strings: bool, contains: &[&str], twin: Option<&str>) -> usize {
        let render = |sql: &str| {
            let before = chrono::Utc::now().timestamp_micros();
            let (param, values) = parameterize_statement(&parse(sql), base, include_strings).expect("now() parameterizes");
            let after = chrono::Utc::now().timestamp_micros();
            // Find the lifted instant by TYPE, not position: with `include_strings`
            // the INTERVAL literal lifts after the time fn, so it is not always last.
            let us = values.iter().find_map(ts_micros_opt).expect("a tz-aware instant was lifted");
            assert!(before <= us && us <= after, "the lifted instant is fresh, not frozen");
            (param.to_string(), values)
        };
        let (text, values) = render(sql);
        assert!(!text.to_lowercase().contains("now("), "now() replaced by a placeholder: {text}");
        for frag in contains {
            assert!(text.contains(frag), "{frag} expected in: {text}");
        }
        if let Some(twin) = twin {
            assert_eq!(text, render(twin).0, "reusable shape key across refreshes");
        }
        values.len()
    }

    /// The lifted instant must share the column's precision: a nanosecond one makes
    /// coercion cast the COLUMN, which CSE then hoists into a projection that the
    /// rollup matcher cannot walk, so no now()-relative query could route.
    #[tokio::test]
    async fn a_lifted_now_does_not_cast_the_timestamp_column() {
        let hook = PlanCacheHook::new(64, true);
        let sql = "SELECT count(*) FROM t WHERE project_id = 'p' AND ts >= now() - INTERVAL '4 hours' AND ts < now() - INTERVAL '3 hours'";
        let plan = hook.cached_plan(&parse(sql), &test_ctx()).await.expect("cacheable").expect("plans");
        let text = plan.display_indent().to_string();
        assert!(!text.contains("CAST(t.ts"), "the column must not be cast to the bound instant's type:\n{text}");
    }

    #[test]
    fn extra_execute_params_supplies_fresh_instant_for_mixed_only() {
        let hook = PlanCacheHook::new(64, true);
        let mixed = parse("SELECT id FROM t WHERE project_id = $1 AND ts > now() - INTERVAL '1 hour'");
        // Two executes → two fresh instants (never frozen), one value each ($2).
        let a = hook.extra_execute_params(Some(&mixed));
        let b = hook.extra_execute_params(Some(&mixed));
        assert_eq!(a.len(), 1);
        assert!(ts_micros(&b[0]) >= ts_micros(&a[0]), "monotonic fresh instant");
        // Pure path (no client bind) substitutes at parse → no execute-time extras.
        assert!(hook.extra_execute_params(Some(&parse("SELECT id FROM t WHERE project_id = 'p' AND ts > now()"))).is_empty());
        // No time fn → nothing to inject.
        assert!(hook.extra_execute_params(Some(&parse("SELECT id FROM t WHERE project_id = $1"))).is_empty());
        // Flag off → feature disabled entirely.
        assert!(PlanCacheHook::new(64, false).extra_execute_params(Some(&mixed)).is_empty());
        // A dropped AST (bulk statements do not pin theirs) must not panic.
        assert!(hook.extra_execute_params(None).is_empty());
        assert_eq!(hook.injected_param_count(None), 0);
    }

    /// SessionContext with one in-memory table, enough to plan the SELECTs the
    /// simple-query path exercises.
    fn test_ctx() -> SessionContext {
        use datafusion::{
            arrow::datatypes::{DataType, Field},
            datasource::MemTable,
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("project_id", DataType::Utf8, true),
            Field::new("ts", DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())), true),
        ]));
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap())).unwrap();
        ctx
    }

    /// The shape that failed to plan now plans, and the shapes we decline keep
    /// their exact wire-visible column name.
    #[tokio::test]
    async fn the_broken_ordinal_shape_plans_and_working_shapes_keep_their_names() {
        let ctx = test_ctx();
        let plan = async |stmt: Statement| ctx.state().statement_to_plan(DfStatement::Statement(Box::new(stmt))).await;

        let broken = parse("SELECT project_id, COUNT(*)::int8 FROM t GROUP BY project_id ORDER BY 2 DESC");
        assert!(plan(broken.clone()).await.is_err(), "the bug this fixes must still be reproducible without the rewrite");
        let fixed = normalize_count_star(&broken).expect("rewritten");
        assert!(plan(fixed).await.is_ok(), "normalized ordinal resolves");

        // A query we decline is byte-identical, so its column name cannot move.
        for sql in ["SELECT COUNT(*) FROM t", "SELECT COUNT(*)::int8 FROM t"] {
            assert!(normalize_count_star(&parse(sql)).is_none(), "declined: {sql}");
        }
        assert_eq!(plan(parse("SELECT COUNT(*) FROM t")).await.expect("plans").schema().field(0).name(), "count(*)");
    }

    #[tokio::test]
    async fn simple_query_path_caches_by_shape_and_bypasses_date_fns() {
        let hook = PlanCacheHook::new(64, true);
        let ctx = test_ctx();
        let plan_for = async |sql: &str| hook.cached_plan(&parse(sql), &ctx).await.map(|r| r.expect("plan"));

        // The shape is built once; the second identical query is a pure hit.
        assert!(plan_for("SELECT id FROM t WHERE project_id = 'p'").await.is_some());
        assert_eq!(hook.shape_counters(), (1, 0));
        assert!(plan_for("SELECT id FROM t WHERE project_id = 'p'").await.is_some());
        assert_eq!(hook.shape_counters(), (2, 0));
        assert_eq!(hook.shapes.len(), 1, "identical query reuses the one shape");

        // Same shape, different literal → still one shape entry, another hit.
        assert!(plan_for("SELECT id FROM t WHERE project_id = 'q'").await.is_some());
        assert_eq!(hook.shape_counters(), (3, 0));
        assert_eq!(hook.shapes.len(), 1, "literals don't multiply shapes");

        // Unparameterizable time fn keeps bypassing: no plan, no shape.
        assert!(hook.cached_plan(&parse("SELECT current_date"), &ctx).await.is_none());
        // Non-cacheable AST kind bypasses too.
        assert!(hook.cached_plan(&parse("SET TIME ZONE 'UTC'"), &ctx).await.is_none());
        assert_eq!(hook.shape_counters(), (3, 0));
    }

    /// Varying the INSERT batch size must not grow the cache without bound: every
    /// distinct batch size is a distinct entry whose `Values` plan holds one
    /// `CAST` per placeholder, so entry COUNT bounds nothing.
    #[tokio::test]
    async fn varying_insert_batch_sizes_cannot_grow_the_plan_cache_past_its_byte_budget() {
        // Fewer statements than slots, so only the byte budget can bound this.
        const STATEMENTS: usize = 40;
        let hook = PlanCacheHook::new(64, false);
        let ctx = test_ctx();

        // Each statement is a bulk INSERT longer than the last.
        let bulk_insert = |batch: usize| {
            let values = (0..batch * 50).map(|r| format!("(${}, ${})", r * 2 + 1, r * 2 + 2)).collect::<Vec<_>>().join(",");
            parse(&format!("INSERT INTO t (id, project_id) VALUES {values}"))
        };
        for batch in 1..=STATEMENTS {
            assert!(hook.cached_plan(&bulk_insert(batch), &ctx).await.is_some(), "the INSERT must still plan and be served");
        }

        // The sweep runs BEFORE the admission that follows it, so the steady-state
        // bound is the budget plus one statement.
        let largest = hook.cache.map.iter().map(|e| e.value().1).max().unwrap_or(0);
        let retained: usize = hook.cache.map.iter().map(|e| e.value().1).sum();
        assert!(retained <= hook.cache.max_bytes + largest, "cache retains {retained} plan bytes, over its {} budget", hook.cache.max_bytes);
        assert_eq!(hook.cache.bytes(), retained, "the byte counter drifted from the map it bounds");
        assert!(hook.cache.len() < STATEMENTS, "the sweep must have dropped entries; {} of {STATEMENTS} retained", hook.cache.len());
        assert!(hook.cache.len() < hook.cache.capacity, "the entry cap must not be what bounded this");

        // Evicting largest-first is what makes the bound affordable: the small hot
        // SELECT sharing the cache with this flood must survive it.
        let select = "SELECT id FROM t WHERE project_id = $1";
        assert!(hook.cached_plan(&parse(select), &ctx).await.is_some());
        for batch in 1..=STATEMENTS {
            let _ = hook.cached_plan(&bulk_insert(batch), &ctx).await;
        }
        assert!(hook.cache.contains_key(&parse(select).to_string()), "the flood evicted the small hot SELECT instead of the bulk INSERTs paying for the bytes");
    }

    /// A sweep must leave headroom: evicting to exactly the cap lets the very next
    /// insert re-cross it, and every Parse then pays a full sort of the map.
    #[test]
    fn a_sweep_leaves_headroom_so_the_next_insert_cannot_re_trigger_it() {
        // 8 slots x 1 KiB = 8 KiB of budget, entries of 1 KiB each.
        let map: WeighedMap<u8> = WeighedMap::new(8, 1024);
        for i in 0..8 {
            map.insert(format!("k{i}"), 0, 1024, "test");
        }
        assert!(map.bytes() >= map.max_bytes, "the budget must actually be reached to arm the sweep");

        // This insert sweeps; the map must then sit at or under the low water mark
        // plus the one entry just admitted.
        map.insert("trigger".into(), 0, 1024, "test");
        let low_water = map.max_bytes / SWEEP_LOW_WATER_DEN * SWEEP_LOW_WATER_NUM;
        assert!(map.bytes() <= low_water + 1024, "swept to {} bytes, expected <= {} — no headroom left", map.bytes(), low_water + 1024);
        assert!(map.bytes() < map.max_bytes, "a sweep that lands ON the cap re-arms itself on the next insert");
    }

    /// An entry heavier than the entire budget is served but never admitted:
    /// caching it would evict everything else and still not survive.
    #[test]
    fn an_entry_bigger_than_the_budget_is_not_admitted() {
        let map: WeighedMap<u8> = WeighedMap::new(4, 1024);
        map.insert("huge".into(), 0, 4 * 1024, "test");
        assert_eq!(map.len(), 0);
        assert_eq!(map.bytes(), 0, "a rejected entry must not be charged to the budget");
    }

    /// A `'$1'` string literal makes the text-based `has_placeholder` fire while
    /// the AST holds no bind; it must plan through the pure-now() path rather than
    /// caching an unsubstituted template.
    #[tokio::test]
    async fn string_literal_that_looks_like_a_placeholder_does_not_poison_the_cache() {
        let hook = PlanCacheHook::new(64, true);
        let ctx = test_ctx();
        let stmt = parse("SELECT now(), '$1'");
        for _ in 0..2 {
            let plan = hook.cached_plan(&stmt, &ctx).await;
            if let Some(r) = plan {
                let plan = r.expect("planned");
                assert!(plan.get_parameter_types().expect("param types").is_empty(), "no placeholder may survive into the served plan: {plan:?}");
            }
        }
        // A real mixed now()+$N query still takes the mixed path.
        assert!(hook.cached_plan(&parse("SELECT id, now() FROM t WHERE project_id = $1"), &ctx).await.is_some());
    }
}
