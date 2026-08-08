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

use std::sync::{
    Arc, OnceLock,
    atomic::{
        AtomicBool, AtomicU64,
        Ordering::{AcqRel, Relaxed, Release},
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
        tree_node::{Transformed, TreeNode},
    },
    error::Result as DfResult,
    logical_expr::{Cast, Expr, LogicalPlan, Values, dml::WriteOp},
    prelude::SessionContext,
    scalar::ScalarValue,
    sql::{parser::Statement as DfStatement, sqlparser::ast::Statement},
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
use tracing::{debug, warn};

use crate::errors::{api_err, arrow_err};

/// Soft size cap: once `cap` is reached, drop ~half the entries at random.
/// Cheaper than an LRU clock and adequate while the steady-state working set
/// fits well under `cap`.
fn soft_cap<K: Eq + std::hash::Hash, V>(map: &DashMap<K, V>, cap: usize) {
    if map.len() >= cap {
        map.retain(|_, _| fastrand::bool());
    }
}

/// Walk a plan and replace every `CAST(Literal(v), T)` with `Literal(cast(v, T))`.
///
/// After `replace_params_with_values` substitutes `$N → literal`, the `CAST`
/// wrappers `insert_coerce` puts around every placeholder turn into per-cell
/// `CAST(Literal, T)` exprs inside `ValuesExec`. Executing those casts at
/// query time, once per (row, column), is responsible for ~9–10 ms/row of
/// pgwire-INSERT overhead at the 88-col schema (measured). The cast values
/// are constant so we can fold them once, at substitution time, and let
/// `ValuesExec` see plain literals.
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
                        // A literal that can't be cast (e.g. lossy string-→-number)
                        // stays put — the executor's cast surfaces a clear error.
                        Err(err) => {
                            tracing::trace!(target: "plan_cache", %err, ?value, ?data_type, "fold_literal_casts: cast_to failed, leaving CAST for executor");
                            Ok(Transformed::no(e))
                        }
                    }
                })
            })
            .collect::<DfResult<_>>()?;
        // Only rebuild when a cast was actually folded. `with_new_exprs` rejects
        // some nodes whose `expressions()`/`with_new_exprs` round-trip isn't
        // identity — notably `Unnest`, whose `expressions()` returns its
        // `exec_columns` but `with_new_exprs` asserts an empty expr list (DF54).
        // monoscope's `UPDATE … FROM (SELECT unnest($1::text[]) …)` dual-write
        // carries exactly such an `Unnest`; rebuilding it unconditionally tripped
        // `Internal error: Assertion failed: expr.is_empty()`.
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
/// Skips `ValuesExec`, `DataSinkExec`, and the per-row `replace_params_with_values`
/// walk that together account for ~5-6 ms/row of overhead at the 88-col schema.
///
/// Returns `Ok(Some(rows))` on success, `Ok(None)` if the plan shape isn't
/// the supported fast-path INSERT (caller should fall back to the regular
/// `execute_logical_plan` path).
async fn try_fast_path_insert(plan: &LogicalPlan, session_context: &SessionContext) -> DfResult<Option<u64>> {
    use datafusion::logical_expr::dml::DmlStatement;

    let LogicalPlan::Dml(DmlStatement { table_name, op: WriteOp::Insert(_), input, .. }) = plan else {
        return Ok(None);
    };

    // Input is either `Projection → Values` (INSERT INTO t (cols) VALUES …) or
    // `Values` directly. Each projected output column must resolve either to a
    // Values column (a `Column`/`Alias(Column)` ref — the common case, which
    // also tells us how to reorder Values columns into the table's layout) or
    // to a constant the optimizer folded in (NULL defaults for unspecified
    // columns). Anything more complex (computed cols, unfolded casts,
    // sub-exprs) falls back to the executor.
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

    // Every cell must be a literal — possibly wrapped in an Alias from the
    // pgwire `$N` placeholder name retained after substitution. Anything else
    // legitimately needs the full executor (subqueries, function calls,
    // correlated refs, etc.) — `None` propagates out as "not the fast path".
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
    let columns_or_bail: Vec<Option<ArrayRef>> = values_schema
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
            // `iter_to_array` may return a different concrete type than the
            // Values column declares (e.g. all-NULL columns come back as Null).
            // Cast back to target so the downstream MemBuffer schema check sees
            // exactly what the table expects.
            Ok(Some(if arr.data_type() == target_ty { arr } else { cast(&arr, target_ty).map_err(arrow_err)? }))
        })
        .collect::<DfResult<_>>()?;
    let Some(values_columns) = columns_or_bail.into_iter().collect::<Option<Vec<ArrayRef>>>() else {
        return Ok(None);
    };

    // Apply the projection: pull Values columns by index, or materialize a
    // constant array for projection cells the optimizer folded to a literal.
    let (final_schema, columns) = match column_plan {
        Some(plan) => {
            let (fields, cols): (Vec<Arc<Field>>, Vec<ArrayRef>) = plan
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
                .collect::<DfResult<Vec<_>>>()?
                .into_iter()
                .unzip();
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
    PgWireError::ApiError("internal error: DML plan returned non-DML completion".to_string().into())
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
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "57014".to_string(),
                "canceling statement due to statement timeout".to_string(),
            )))
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
    // Fast-path: `Dml(Insert) → [Projection →] Values(literals)` skips the
    // executor entirely and writes the batch straight into the buffered layer.
    // Saves the ~5-6 ms/row that `ValuesExec` + `DataSinkExec` were costing at
    // the 88-col schema.
    if let Some(rows) = try_fast_path_insert(&folded, session_context).await.map_err(api_err)? {
        return Ok(Response::Execution(Tag::new("INSERT").with_oid(0).with_rows(rows as usize)));
    }
    dml_completion(session_context.execute_logical_plan(folded).await.map_err(api_err)?).await
}

// Fallback when config isn't initialized (test-only factory paths). Prod reads
// `memory.timefusion_plan_cache_capacity`. See config.rs.
const DEFAULT_PLAN_CACHE_CAPACITY: usize = 1024;

/// Soft cap on the `served` memo (one-shot literal-bearing texts).
const SERVED_CAP: usize = 4096;

/// Singleton handle so `timefusion_stats` can read the same cache the
/// pgwire factory writes to without plumbing an Arc through the database
/// constructor.
static GLOBAL: OnceLock<Arc<PlanCacheHook>> = OnceLock::new();

pub fn set_global(cache: Arc<PlanCacheHook>) {
    let _ = GLOBAL.set(cache);
}

pub fn global() -> Option<Arc<PlanCacheHook>> {
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
    cache: DashMap<String, LogicalPlan>,
    capacity: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    /// Shape cache for LITERAL-bearing SELECTs (generated dashboard SQL that
    /// never repeats verbatim): keyed by the statement with every string
    /// literal replaced by `$N`, storing the pre-optimized placeholder plan +
    /// inferred parameter types. A hit clones the plan and substitutes the
    /// query's actual literals (cast to the inferred types) — skipping parse,
    /// analyze, AND optimize. `None` = negative entry: this shape failed to
    /// plan/parameterize once; don't retry it per query.
    shapes: DashMap<String, Option<ShapeEntry>>,
    /// Canonical texts we served a pre-optimized substituted plan for, so
    /// `was_pre_optimized` can tell the handler to skip `state.optimize()`.
    /// Literal-bearing texts are one-shot (next dashboard refresh has new
    /// literals), so recency semantics with a soft cap are enough — a false
    /// `false` after eviction merely re-optimizes an optimized plan.
    served: DashMap<String, ()>,
    shape_hits: AtomicU64,
    shape_skips: AtomicU64,
    /// When true, `now()`-bearing SELECTs go through the shape cache with the
    /// time function parameterized (fresh instant substituted per query) instead
    /// of being bypassed. Off by default — it's the hot dashboard path, so enable
    /// deliberately (TIMEFUSION_PLAN_CACHE_TIME_FNS=1) after canarying.
    time_fn_shapes: bool,
    /// Single-flight guard for the capacity sweep — see `cached_plan`.
    evicting: AtomicBool,
}

#[derive(Clone)]
struct ShapeEntry {
    plan: LogicalPlan,
    /// Inferred DataType per `$N` (index 0 = `$1`); substituted literals are
    /// cast to these so the plan's expression types stay exact.
    param_types: Vec<Option<DataType>>,
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

/// True if `e` is exactly the bare `count(*)` idiom.
fn is_count_star(e: &datafusion::sql::sqlparser::ast::Expr) -> bool {
    use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments};
    matches!(e, SqlExpr::Function(f)
        if fn_name_is_one_of(f, &["count"])
            && matches!(&f.args, FunctionArguments::List(l) if matches!(l.args.as_slice(), [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)])))
}

/// Rewrite `count(*)` to `count(1)` — but ONLY for a statement that DataFusion
/// rejects today, i.e. one whose `ORDER BY <ordinal>` points at a select item
/// that wraps `count(*)` in a larger expression. `None` for everything else.
///
/// The narrowness is the point, and it is a correctness requirement rather than
/// caution. `count(*)` and `count(1)` compute the same thing, but they do not
/// NAME the same thing: the wire-visible column for `SELECT count(*)` is
/// `count(*)`, and the shape cache lifts the injected `1` into a placeholder on
/// top of that, so a blanket rewrite renamed the column to `count($1)` for every
/// caller (caught by
/// `normalizing_count_star_leaves_the_output_column_name_alone`). Restricting
/// the rewrite to statements that currently fail to plan at all means no
/// working query can change shape or name — a query that errors has no output
/// contract to break.
///
/// A bare `SELECT count(*) ... ORDER BY 1` is deliberately excluded: DataFusion
/// already resolves that ordinal, so it does not need — and must not get — the
/// rewrite.
fn normalize_count_star(stmt: &Statement) -> Option<Statement> {
    use std::ops::ControlFlow;

    use datafusion::sql::sqlparser::ast::{
        Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, OrderByKind, SelectItem, SetExpr, Value, ValueWithSpan,
        visit_expressions, visit_expressions_mut,
    };

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
        !is_count_star(e) && visit_expressions(e, |inner: &SqlExpr| if is_count_star(inner) { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }).is_break()
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
            *arg = FunctionArgExpr::Expr(SqlExpr::Value(ValueWithSpan {
                value: Value::Number("1".into(), false),
                span: datafusion::sql::sqlparser::tokenizer::Span::empty(),
            }));
        }
        ControlFlow::Continue(())
    });
    Some(out)
}

/// Case-insensitive match of a call's last name segment against `names`.
fn fn_name_is_one_of(f: &datafusion::sql::sqlparser::ast::Function, names: &[&str]) -> bool {
    f.name.0.last().and_then(|n| n.as_ident()).is_some_and(|i| names.iter().any(|n| n.eq_ignore_ascii_case(&i.value)))
}

fn contains_unparameterizable_time_fn(stmt: &Statement) -> bool {
    stmt_uses_fn(stmt, UNPARAMETERIZABLE_TIME_FNS)
}

/// True if `stmt` calls any function named in `names`. Shared AST-visitor for
/// the time-fn classifiers.
fn stmt_uses_fn(stmt: &Statement, names: &[&str]) -> bool {
    use std::ops::ControlFlow;

    use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, visit_expressions};
    visit_expressions(stmt, |e: &SqlExpr| match e {
        SqlExpr::Function(f) if fn_name_is_one_of(f, names) => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
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
        ast::{
            CastKind, DataType as SqlDataType, Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, TimezoneInfo, Value, ValueWithSpan,
            visit_expressions_mut,
        },
        tokenizer::Span,
    };
    let mut stmt = stmt.clone();
    let mut values: Vec<ScalarValue> = Vec::new();

    // Push `v` and return the `$N` placeholder referencing its new position —
    // the bookkeeping shared by every literal-lifting site below.
    fn placeholder_for(values: &mut Vec<ScalarValue>, base: usize, v: ScalarValue) -> Value {
        values.push(v);
        Value::Placeholder(format!("${}", base + values.len()))
    }

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
            vs.value = placeholder_for(values, base, sv);
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
                    vs.value = placeholder_for(&mut values, base, ScalarValue::Utf8(Some(s.clone())));
                }
            }
            // now()/current_timestamp/… → placeholder bound to the captured instant,
            // so the optimized plan is reusable across dashboard refreshes while the
            // time window stays fresh (never frozen to plan-build time).
            SqlExpr::Function(f) if fn_name_is_one_of(f, PARAMETERIZABLE_TIME_FNS) => {
                let value = placeholder_for(&mut values, base, ScalarValue::TimestampNanosecond(Some(now_ns), Some("+00:00".into())));
                let placeholder = SqlExpr::Value(ValueWithSpan { value, span: Span::empty() });
                // Wrap in CAST(... AS TIMESTAMPTZ): a BARE placeholder is untyped, so
                // `now() - INTERVAL '1h'` (every dashboard time window) failed to
                // optimize with "Cannot infer common argument type Timestamp >=
                // Interval" → the shape negative-cached → 0 shape hits in prod
                // (2026-07-20). Typing the placeholder lets the arithmetic infer.
                *e = SqlExpr::Cast {
                    kind: CastKind::Cast,
                    expr: Box::new(placeholder),
                    data_type: SqlDataType::Timestamp(None, TimezoneInfo::Tz),
                    format: None,
                    array: false,
                };
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
                // Walk order (operand → conditions → else) fixes `$N` numbering; keep it.
                operand.iter_mut().for_each(|e| take_number(e, base, &mut values));
                conditions.iter_mut().for_each(|w| {
                    take_number(&mut w.condition, base, &mut values);
                    take_number(&mut w.result, base, &mut values);
                });
                else_result.iter_mut().for_each(|e| take_number(e, base, &mut values));
            }
            SqlExpr::Function(f) if include_strings => {
                if let FunctionArguments::List(list) = &mut f.args {
                    list.args
                        .iter_mut()
                        .filter_map(|arg| match arg {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                            | FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. }
                            | FunctionArg::ExprNamed { arg: FunctionArgExpr::Expr(e), .. } => Some(e),
                            _ => None,
                        })
                        .for_each(|e| take_number(e, base, &mut values));
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
            cache: DashMap::new(),
            capacity: capacity.max(1),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            shapes: DashMap::new(),
            served: DashMap::new(),
            shape_hits: AtomicU64::new(0),
            shape_skips: AtomicU64::new(0),
            time_fn_shapes,
            evicting: AtomicBool::new(false),
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

    /// Shape-cache path for literal-bearing SELECTs. Returns a fully
    /// substituted, pre-optimized plan, or `None` to fall back to the normal
    /// parse→optimize pipeline. Every failure installs a negative entry so a
    /// shape that can't parameterize is only attempted once.
    /// `include_strings=false` lifts ONLY the time fn (now()) and leaves every
    /// other literal inline — used for now()-bearing queries, where lifting
    /// strings/numbers breaks planning (INTERVAL '…' → INTERVAL $n is
    /// unplannable, and the same time_bucket('1m',…) in GROUP BY and ORDER BY
    /// would get distinct placeholders → "ORDER BY must be in GROUP BY").
    /// Repeated dashboard refreshes send identical SQL except now(), so the shape
    /// key is still stable → hits. `true` (pure literal SELECTs) lifts all.
    async fn try_shape_cached_plan(
        &self, statement: &Statement, canonical: &str, session_context: &SessionContext, include_strings: bool,
    ) -> Option<LogicalPlan> {
        if !matches!(statement, Statement::Query(_)) {
            return None;
        }
        let (param_stmt, values) = parameterize_statement(statement, 0, include_strings)?;
        let shape_key = param_stmt.to_string();
        let entry = self.get_or_build_shape(&shape_key, param_stmt, values.len(), session_context).await?;

        // Substitute this query's literals, cast to the inferred types.
        let cast_values: Vec<ScalarValue> =
            values.into_iter().zip(entry.param_types.iter()).map(|(v, ty)| ty.as_ref().and_then(|t| v.cast_to(t).ok()).unwrap_or(v)).collect();
        let plan = entry.plan.clone().replace_params_with_values(&ParamValues::List(cast_values.into_iter().map(Into::into).collect())).ok()?;
        let plan = fold_literal_casts(plan).ok()?;
        self.mark_served(canonical);
        Some(plan)
    }

    /// Record a shape hit and memo the canonical text so `was_pre_optimized`
    /// tells the handler to skip `state.optimize()`.
    fn mark_served(&self, canonical: &str) {
        self.shape_hits.fetch_add(1, Relaxed);
        soft_cap(&self.served, SERVED_CAP);
        self.served.insert(canonical.to_string(), ());
    }

    /// Get or build+optimize+cache the placeholder template for `shape_key`.
    /// `value_count` = how many leading `$N` types to record for the caller to
    /// cast its client literals against (0 for the mixed path, which binds its
    /// injected params by inferred type at execute). `None` = negative entry:
    /// this shape failed to plan once; don't retry per query.
    async fn get_or_build_shape(&self, shape_key: &str, param_stmt: Statement, value_count: usize, session_context: &SessionContext) -> Option<ShapeEntry> {
        if let Some(e) = self.shapes.get(shape_key) {
            return e.value().clone(); // Some(entry) hit / None negative
        }
        // Build the placeholder plan once for this shape. The error is logged
        // rather than swallowed so we can see WHY a shape negative-caches in prod
        // (2026-07-20: dashboard now()+$N shapes all failed to build → 0 hits).
        let state = session_context.state();
        let built = state
            .statement_to_plan(DfStatement::Statement(Box::new(param_stmt)))
            .await
            .and_then(|p| state.optimize(&p))
            .inspect_err(|e| warn!(target: "plan_cache", "shape build failed: {shape_key} — {e}"))
            .ok()
            .and_then(|plan| match value_count {
                0 => Some(ShapeEntry { plan, param_types: Vec::new() }),
                n => {
                    let types = plan.get_parameter_types().ok()?;
                    let param_types = (1..=n).map(|i| types.get(&format!("${i}")).cloned().flatten()).collect();
                    Some(ShapeEntry { plan, param_types })
                }
            });
        if built.is_none() {
            self.shape_skips.fetch_add(1, Relaxed);
        }
        // Shape variety should be tiny in steady state, so the same soft cap the
        // template cache uses is enough.
        soft_cap(&self.shapes, self.capacity);
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
        let base = self.mixed_time_fn_base(statement)?;
        let (param_stmt, _) = parameterize_statement(statement, base, false)?;
        let shape_key = param_stmt.to_string();
        let plan = self.get_or_build_shape(&shape_key, param_stmt, 0, session_context).await?.plan;
        self.mark_served(canonical);
        Some(plan)
    }

    /// Base index for the mixed now()+client-`$N` path: the client's highest
    /// `$N`, when this statement is one we inject time-fn placeholders above.
    /// `None` = not a mixed-path statement, so nothing to inject.
    ///
    /// A base of 0 also covers the `'$1'`-string-literal trap: `has_placeholder`
    /// is a TEXT scan, so a literal containing `$1` routes a bind-less statement
    /// here; the template would then be served with an unsubstituted `$1` ("no
    /// value for placeholder $1" — and cached). The AST is authoritative.
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
    /// Normalize `count(*)` before planning, then plan.
    ///
    /// `count(*)` and `count(1)` are exactly equivalent (`1` is never NULL), but
    /// DataFusion plans the aggregate as `count(Int64(1))` while an `ORDER BY`
    /// *ordinal* resolves to the expression as WRITTEN in the select list. When
    /// that select item wraps the call — `count(*)::int8` — the two never match
    /// and planning fails with "Column in ORDER BY must be in GROUP BY or an
    /// aggregate function". Postgres accepts it. monoscope's service graph is
    /// exactly this shape and it was the single largest error source on prod
    /// (~5500/hour, 2026-08-08).
    ///
    /// This has to happen at the AST level, not as a text rewrite: the same
    /// rewrite over raw SQL would also hit INSERTs, and monoscope inserts
    /// arbitrary span bodies and log text that can contain `count(*)` as data.
    ///
    /// A rewritten statement must never return `None`, because the caller then
    /// falls through to a planner that re-plans the ORIGINAL statement — which
    /// is the broken one. So when the normal path declines a rewritten
    /// statement, plan it here instead of handing back the un-normalized form.
    async fn cached_plan(&self, statement: &Statement, session_context: &SessionContext) -> Option<PgWireResult<LogicalPlan>> {
        // Cheap AST-variant gate first: skipping non-DML here avoids paying for
        // `Statement::to_string()` on every Parse message regardless of
        // cacheability.
        if !matches!(statement, Statement::Insert(_) | Statement::Query(_) | Statement::Update { .. } | Statement::Delete(_)) {
            return None;
        }
        let Some(normalized) = normalize_count_star(statement) else {
            return self.cached_plan_normalized(statement, session_context).await;
        };
        match self.cached_plan_normalized(&normalized, session_context).await {
            Some(result) => Some(result),
            None => {
                let state = session_context.state();
                Some(
                    state
                        .statement_to_plan(DfStatement::Statement(Box::new(normalized)))
                        .await
                        .map(crate::insert_coerce::rewrite_plan)
                        .and_then(|plan| state.optimize(&plan))
                        .map_err(api_err),
                )
            }
        }
    }

    async fn cached_plan_normalized(&self, statement: &Statement, session_context: &SessionContext) -> Option<PgWireResult<LogicalPlan>> {
        // now()/current_date/... are const-folded by the optimizer using the
        // query start time — a verbatim-cached optimized plan would freeze them.
        // With time-fn shape caching on, route now()-bearing SELECTs to the shape
        // path (which parameterizes the time fn → fresh instant per query);
        // otherwise, and for unparameterizable date/time fns, plan fresh.
        if contains_plan_time_folded_fn(statement) {
            if self.time_fn_shapes && matches!(statement, Statement::Query(_)) && !contains_unparameterizable_time_fn(statement) {
                let canonical = statement.to_string();
                return if Self::has_placeholder(&canonical) {
                    // Mixed now()+client `$N`: cache a template that keeps BOTH the
                    // client placeholders and the time-fn placeholders open; the
                    // fresh instant is injected per-execute by extra_execute_params
                    // (correct even for reused prepared statements).
                    self.try_mixed_time_fn_plan(statement, &canonical, session_context).await
                } else {
                    // Pure now()-bearing: lift ONLY now() (include_strings=false),
                    // keep other literals inline so INTERVAL/time_bucket plan.
                    self.try_shape_cached_plan(statement, &canonical, session_context, false).await
                }
                .map(Ok);
            }
            return None;
        }
        let canonical = statement.to_string();
        if !Self::has_placeholder(&canonical) {
            // Literal-bearing SELECT (no now()): lift all literals for a
            // literal-insensitive shape.
            return self.try_shape_cached_plan(statement, &canonical, session_context, true).await.map(Ok);
        }

        // Lock-free read: DashMap.get returns a guard that just locks the
        // single shard's reader, not the whole cache.
        if let Some(plan) = self.cache.get(&canonical) {
            self.hits.fetch_add(1, Relaxed);
            debug!(target: "plan_cache", %canonical, "plan cache hit");
            return Some(Ok(plan.clone()));
        }

        // Miss: build the plan, install it, hand a clone back to caller.
        self.misses.fetch_add(1, Relaxed);
        let state = session_context.state();
        // `insert_coerce::rewrite_plan` wraps `$N` placeholders inside Values rows
        // with `CAST($N AS <col_type>)` so pgwire param-type inference returns the
        // right type per placeholder (otherwise row-1 types leak across to row-2+
        // placeholders by position).
        //
        // Pre-optimizing at cache-miss time turns a per-query ~30ms cost into a
        // one-time amortization: the patched datafusion-postgres skips its own
        // `state.optimize()` when the hook returns Some (see
        // apitoolkit/datafusion-postgres@timefusion-df54 src/handlers.rs). The plan
        // still goes through `replace_params_with_values` at exec time, but
        // non-constant-fold rules are parameter-independent and stay valid across
        // all bound values.
        let built = state
            .statement_to_plan(DfStatement::Statement(Box::new(statement.clone())))
            .await
            .map(crate::insert_coerce::rewrite_plan)
            .and_then(|p| state.optimize(&p));
        let plan = match built {
            Ok(p) => p,
            Err(e) => return Some(Err(api_err(e))),
        };
        // Overflow sweep. Only the first thread per crossing runs `retain` (the
        // rest re-insert as if the cap hadn't been crossed and get swept next
        // time): `retain` write-locks each shard in turn, so an unsynchronized
        // stampede of missing threads would serialize every concurrent reader
        // behind N sweeps. `retain` can't panic here, so a flat store suffices.
        if self.cache.len() >= self.capacity && !self.evicting.swap(true, AcqRel) {
            // Operator-visible: the workload has more distinct plan templates than
            // the cache can hold (~5 in the OLAP steady state, so this should never
            // fire in prod). Expect the next queries to re-pay state.optimize().
            warn!(
                target: "plan_cache",
                size = self.cache.len(),
                capacity = self.capacity,
                "plan_cache exceeded capacity — evicting ~half. Subsequent queries on evicted plans will re-pay the optimize cost. If this fires steadily, the workload's plan-template variety has grown past the cache budget."
            );
            soft_cap(&self.cache, self.capacity);
            self.evicting.store(false, Release);
        }
        self.cache.insert(canonical, plan.clone());
        Some(Ok(plan))
    }

    fn has_placeholder(sql: &str) -> bool {
        // Naive `contains('$')` would false-positive on dollar-quoted literals
        // like '$100' and cache statements with embedded literal values.
        sql.as_bytes().windows(2).any(|w| w[0] == b'$' && w[1].is_ascii_digit())
    }
}

#[async_trait]
impl QueryHook for PlanCacheHook {
    /// Serve simple-protocol queries from the same caches the extended path
    /// uses. `psql`/ad-hoc SQL arrives with literals inline, so the *shape*
    /// cache is what fires here (literal-insensitive template + per-query
    /// re-binding of the lifted literals) — a verbatim hit only happens for
    /// repeated identical text. Everything the extended path bypasses
    /// (unparameterizable time fns, non-DML kinds, unparameterizable ASTs)
    /// still bypasses: `cached_plan` returning `None` falls through to the
    /// vendored `session_context.sql()` path unchanged.
    async fn handle_simple_query(
        &self, statement: &Statement, session_context: &SessionContext, client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        // The TransactionStatementHook runs AFTER us and is what rejects
        // statements inside a failed transaction block; answering here would
        // silently execute them. Defer to it.
        if client.transaction_status() == TransactionStatus::Error {
            return None;
        }
        // On a plan-build error, fall through rather than surfacing it: the
        // vendored path will produce the same error with its own context.
        let plan = self.cached_plan(statement, session_context).await?.ok()?;
        // Mirror the vendored do_query: the statement timeout covers planning
        // + DataFrame construction, and rows are encoded in unified text.
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
        // `None` statement = a bulk data statement whose AST the portal store
        // no longer pins; `None` base = the pure path (substituted at parse) or
        // a statement we never shape-cached. Nothing to inject either way.
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
        // Only intercept DML — for SELECTs the vendored path is fine.
        // The win here is post-substitution constant folding of `CAST(Literal, T)`
        // exprs that `insert_coerce` puts around every placeholder; folding them
        // before `ValuesExec` evaluates the plan saves ~9–10 ms per inserted
        // row on the 88-col schema (measured).
        if !matches!(logical_plan, LogicalPlan::Dml(_)) {
            return None;
        }
        Some(run_extended_dml(logical_plan, params, session_context).await)
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

    /// The prod-breaking shape: monoscope's service graph, ~5500 planning
    /// failures/hour. `count(*)` must reach the planner as `count(1)` so the
    /// `ORDER BY 6` ordinal resolves against the same expression.
    #[test]
    fn count_star_is_normalized_so_an_order_by_ordinal_can_resolve_it() {
        let stmt = parse("SELECT src, COUNT(*)::int8 FROM t GROUP BY src ORDER BY 2 DESC");
        let out = normalize_count_star(&stmt).expect("rewritten").to_string();
        assert!(out.contains("COUNT(1)"), "count(*) becomes count(1): {out}");
        assert!(!out.contains("COUNT(*)"), "no wildcard call survives: {out}");
    }

    /// Only statements DataFusion rejects today may be rewritten — a working
    /// query must keep its exact output column names. See `normalize_count_star`.
    #[test]
    fn normalize_count_star_never_touches_a_query_that_already_works() {
        for sql in [
            // No ORDER BY at all.
            "SELECT COUNT(*)::int8 FROM t",
            // Ordinal resolves to a BARE count(*), which DataFusion handles.
            "SELECT src, COUNT(*) FROM t GROUP BY src ORDER BY 2 DESC",
            // Ordinal points at a different select item than the wrapped count.
            "SELECT src, COUNT(*)::int8 FROM t GROUP BY src ORDER BY 1 ASC",
            // ORDER BY by name/alias already resolves.
            "SELECT src, COUNT(*)::int8 AS c FROM t GROUP BY src ORDER BY c DESC",
        ] {
            assert!(normalize_count_star(&parse(sql)).is_none(), "must decline: {sql}");
        }
    }

    /// A statement with nothing to rewrite must return `None` — that is what
    /// keeps it on the ordinary cache path instead of forcing a fresh plan.
    #[test]
    fn normalize_count_star_declines_when_there_is_no_wildcard_count() {
        for sql in [
            "SELECT COUNT(id)::int8 FROM t ORDER BY 1",
            "SELECT COUNT(DISTINCT level)::int8 FROM t ORDER BY 1",
            "SELECT SUM(d)::int8 FROM t ORDER BY 1",
            // A qualified wildcard is not the count(*) idiom and is left alone.
            "SELECT COUNT(t.*)::int8 FROM t ORDER BY 1",
        ] {
            assert!(normalize_count_star(&parse(sql)).is_none(), "must decline: {sql}");
        }
    }

    /// The reason this is an AST rewrite and not a text rewrite: monoscope
    /// inserts arbitrary span bodies and log text, and a regex over raw SQL
    /// would corrupt any row whose DATA contains `count(*)`.
    #[test]
    fn insert_data_containing_the_text_count_star_is_never_rewritten() {
        let stmt = parse("INSERT INTO t (body) VALUES ('the query was count(*) over spans')");
        assert!(normalize_count_star(&stmt).is_none(), "a string literal is data, not a call");
        // A neighbouring string literal survives a genuine rewrite untouched.
        let mixed = parse("SELECT a, CAST(COUNT(*) AS int8), 'count(*)' FROM s GROUP BY a ORDER BY 2 DESC");
        let out = normalize_count_star(&mixed).expect("rewritten").to_string();
        assert!(out.contains("COUNT(1)") && out.contains("'count(*)'"), "call rewritten, literal preserved: {out}");
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
        let a = hook.extra_execute_params(Some(&mixed));
        let b = hook.extra_execute_params(Some(&mixed));
        assert_eq!(a.len(), 1);
        match (&a[0], &b[0]) {
            (ScalarValue::TimestampNanosecond(Some(x), _), ScalarValue::TimestampNanosecond(Some(y), _)) => assert!(y >= x, "monotonic fresh instant"),
            _ => panic!("expected tz-aware nanosecond timestamps"),
        }
        // Pure path (no client bind) substitutes at parse → no execute-time extras.
        assert!(hook.extra_execute_params(Some(&parse("SELECT id FROM t WHERE project_id = 'p' AND ts > now()"))).is_empty());
        // No time fn → nothing to inject.
        assert!(hook.extra_execute_params(Some(&parse("SELECT id FROM t WHERE project_id = $1"))).is_empty());
        // Flag off → feature disabled entirely.
        assert!(PlanCacheHook::new(64, false).extra_execute_params(Some(&mixed)).is_empty());
        // Bulk data statements no longer pin their AST past Parse; nothing to
        // inject into one, so a dropped AST must not panic or over-count.
        assert!(hook.extra_execute_params(None).is_empty());
        assert_eq!(hook.injected_param_count(None), 0);
    }

    #[test]
    fn date_time_fns_stay_unparameterizable() {
        // Date/Time-returning fns must NOT take the shape path (type risk).
        assert!(contains_unparameterizable_time_fn(&parse("SELECT id FROM t WHERE d = current_date")));
        assert!(!contains_unparameterizable_time_fn(&parse("SELECT id FROM t WHERE ts > now()")));
    }

    /// SessionContext with one in-memory table, enough to plan the SELECTs the
    /// simple-query path exercises.
    fn test_ctx() -> SessionContext {
        use datafusion::{
            arrow::datatypes::{DataType, Field},
            datasource::MemTable,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true), Field::new("project_id", DataType::Utf8, true)]));
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(MemTable::try_new(schema.clone(), vec![vec![]]).unwrap())).unwrap();
        ctx
    }

    /// The end-to-end property: the shape that failed to plan now plans, and the
    /// shapes we decline keep their exact wire-visible column name.
    #[tokio::test]
    async fn the_broken_ordinal_shape_plans_and_working_shapes_keep_their_names() {
        let ctx = test_ctx();
        let plan = async |stmt: Statement| ctx.state().statement_to_plan(DfStatement::Statement(Box::new(stmt))).await;

        // Before: this is the monoscope service-graph shape and it does not plan.
        let broken = parse("SELECT project_id, COUNT(*)::int8 FROM t GROUP BY project_id ORDER BY 2 DESC");
        assert!(plan(broken.clone()).await.is_err(), "the bug this fixes must still be reproducible without the rewrite");
        // After: normalized, it plans.
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

        // 1st and 2nd identical simple queries both come back cached (the shape
        // is built once — the second is a pure hit, no new shape entry).
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

    /// A `'$1'` STRING LITERAL makes the text-based `has_placeholder` fire while
    /// the AST holds no bind. Routing that into the mixed-time-fn path returned
    /// an unsubstituted template — "no value for placeholder $1", cached. It
    /// must plan through the pure-now() path instead.
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
