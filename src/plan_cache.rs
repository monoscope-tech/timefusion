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

    let LogicalPlan::Dml(DmlStatement {
        table_name,
        op: WriteOp::Insert(_),
        input,
        ..
    }) = plan
    else {
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
        let arr = if arr.data_type() == target_ty {
            arr
        } else {
            datafusion::arrow::compute::cast(&arr, target_ty).map_err(arrow_err)?
        };
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
// Takes the already-collected DML output batches (the hook builds the physical
// plan itself to skip a redundant re-optimize, so there is no DataFrame to
// collect) and derives the CommandComplete tag + affected-row count.
fn dml_response(plan: &LogicalPlan, batches: &[RecordBatch]) -> Option<Response> {
    let tag = match plan {
        LogicalPlan::Dml(d) => match d.op {
            WriteOp::Insert(_) => Tag::new("INSERT").with_oid(0),
            WriteOp::Update => Tag::new("UPDATE"),
            WriteOp::Delete => Tag::new("DELETE"),
            _ => return None,
        },
        _ => return None,
    };
    let rows = batches
        .first()
        .and_then(|b| b.column_by_name("count"))
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .map_or(0, |a| a.value(0) as usize);
    Some(Response::Execution(tag.with_rows(rows)))
}

const DEFAULT_PLAN_CACHE_CAPACITY: usize = 256;

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
    /// INSERT executes that took `try_fast_path_insert` (`fast_path_hits`) vs.
    /// those that fell through to the executor (`fast_path_fallthrough`). The
    /// fast path silently stops matching when the client's INSERT shape drifts
    /// (e.g. monoscope's 2026-07-05 switch to `INSERT…SELECT…unnest`), so a
    /// hit-rate collapse here is the earliest signal that the write path
    /// regressed to full planning.
    fast_path_hits: std::sync::atomic::AtomicU64,
    fast_path_fallthrough: std::sync::atomic::AtomicU64,
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
    use std::ops::ControlFlow;

    use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, visit_expressions};
    const TIME_FNS: &[&str] = &[
        "now",
        "current_timestamp",
        "current_date",
        "current_time",
        "localtimestamp",
        "localtime",
        "statement_timestamp",
        "transaction_timestamp",
        "clock_timestamp",
        "today",
    ];
    let mut found = false;
    let _: ControlFlow<()> = visit_expressions(stmt, |e: &SqlExpr| {
        if let SqlExpr::Function(f) = e
            && let Some(name) = f.name.0.last()
            && let Some(ident) = name.as_ident()
            && TIME_FNS.contains(&ident.value.to_lowercase().as_str())
        {
            found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    found
}

/// Replace every string literal in a SELECT with `$N` (walk order), returning
/// the parameterized statement + the extracted values. `None` when there's
/// nothing to extract. Numbers/booleans stay inline — they steer plan shape
/// (LIMIT, bucket sizes) and vary little; strings carry the high-churn values
/// (timestamps, project ids, search terms).
fn parameterize_statement(stmt: &Statement) -> Option<(Statement, Vec<ScalarValue>)> {
    use std::ops::ControlFlow;

    use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Value, visit_expressions_mut};
    let mut stmt = stmt.clone();
    let mut values: Vec<ScalarValue> = Vec::new();
    let _: ControlFlow<()> = visit_expressions_mut(&mut stmt, |e: &mut SqlExpr| {
        if let SqlExpr::Value(vs) = e
            && let Value::SingleQuotedString(s) = &vs.value
        {
            values.push(ScalarValue::Utf8(Some(s.clone())));
            vs.value = Value::Placeholder(format!("${}", values.len()));
        }
        ControlFlow::Continue(())
    });
    (!values.is_empty()).then_some((stmt, values))
}

impl Default for PlanCacheHook {
    fn default() -> Self {
        Self::new(DEFAULT_PLAN_CACHE_CAPACITY)
    }
}

impl PlanCacheHook {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: dashmap::DashMap::new(),
            capacity: capacity.max(1),
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
            shapes: dashmap::DashMap::new(),
            served: dashmap::DashMap::new(),
            shape_hits: std::sync::atomic::AtomicU64::new(0),
            shape_skips: std::sync::atomic::AtomicU64::new(0),
            fast_path_hits: std::sync::atomic::AtomicU64::new(0),
            fast_path_fallthrough: std::sync::atomic::AtomicU64::new(0),
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

    /// Returns (fast_path_hits, fast_path_fallthrough) for stats observability.
    pub fn fast_path_counters(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (self.fast_path_hits.load(Relaxed), self.fast_path_fallthrough.load(Relaxed))
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
        let (param_stmt, values) = parameterize_statement(statement)?;
        let shape_key = param_stmt.to_string();

        let entry: Option<ShapeEntry> = match self.shapes.get(&shape_key) {
            Some(e) => e.value().clone(), // Some(entry) hit / None negative
            None => {
                // Build the placeholder plan once for this shape.
                let state = session_context.state();
                let built = match state.statement_to_plan(DfStatement::Statement(Box::new(param_stmt))).await {
                    Ok(p) => state.optimize(&p).ok(),
                    Err(_) => None,
                };
                let built = built.and_then(|plan| {
                    let types = plan.get_parameter_types().ok()?;
                    let param_types = (1..=values.len()).map(|i| types.get(&format!("${i}")).cloned().flatten()).collect();
                    Some(ShapeEntry { plan, param_types })
                });
                if built.is_none() {
                    self.shape_skips.fetch_add(1, Relaxed);
                    debug!(target: "plan_cache", "shape negative-cached: {shape_key}");
                }
                // Soft cap shares the template cache's philosophy: clear half
                // on overflow (shape variety should be tiny in steady state).
                if self.shapes.len() >= self.capacity {
                    let cap = self.capacity;
                    self.shapes.retain(|_, _| fastrand::usize(..cap) >= cap / 2);
                }
                self.shapes.insert(shape_key.clone(), built.clone());
                built
            }
        };
        let entry = entry?; // negative entry → normal path

        // Substitute this query's literals, cast to the inferred types.
        let cast_values: Vec<ScalarValue> = values
            .into_iter()
            .zip(entry.param_types.iter())
            .map(|(v, ty)| match ty {
                Some(t) => v.cast_to(t).unwrap_or(v),
                None => v,
            })
            .collect();
        let plan = entry
            .plan
            .clone()
            .replace_params_with_values(&ParamValues::List(cast_values.into_iter().map(Into::into).collect()))
            .ok()?;
        let plan = fold_literal_casts(plan).ok()?;
        self.shape_hits.fetch_add(1, Relaxed);
        // Mark so `was_pre_optimized` lets the handler skip state.optimize().
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
        matches!(
            stmt,
            Statement::Insert(_) | Statement::Query(_) | Statement::Update { .. } | Statement::Delete(_)
        )
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

    async fn handle_extended_parse_query(
        &self, statement: &Statement, session_context: &SessionContext, _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        // Cheap AST-variant check first; only then pay for to_string() and
        // the placeholder scan.
        if !Self::kind_is_cacheable(statement) {
            return None;
        }
        // now()/current_date/... are const-folded by the optimizer using the
        // query start time — a cached optimized plan would freeze them. Plan
        // fresh every time for such statements (template AND shape paths).
        if contains_plan_time_folded_fn(statement) {
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
        &self, statement: &Statement, logical_plan: &LogicalPlan, params: &ParamValues, session_context: &SessionContext, _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        use std::sync::atomic::Ordering::Relaxed;
        // Only intercept DML — for SELECTs the vendored path is fine.
        // The win here is post-substitution constant folding of `CAST(Literal, T)`
        // exprs that `insert_coerce` puts around every placeholder; folding them
        // before `ValuesExec` evaluates the plan saves ~9–10 ms per inserted
        // row on the 88-col schema (measured).
        if !matches!(logical_plan, LogicalPlan::Dml(_)) {
            return None;
        }
        fn api_err(e: datafusion::error::DataFusionError) -> PgWireError {
            PgWireError::ApiError(Box::new(e))
        }
        Some(
            async {
                let substituted = logical_plan.clone().replace_params_with_values(params).map_err(api_err)?;
                let folded = fold_literal_casts(substituted).map_err(api_err)?;
                // Fast-path: `Dml(Insert) → [Projection →] Values(literals)` skips the
                // executor entirely and writes the batch straight into the buffered
                // layer. Saves the ~5-6 ms/row that `ValuesExec` + `DataSinkExec`
                // were costing at the 88-col schema.
                if let Some(rows) = try_fast_path_insert(&folded, session_context).await.map_err(api_err)? {
                    self.fast_path_hits.fetch_add(1, Relaxed);
                    return Ok(Response::Execution(Tag::new("INSERT").with_oid(0).with_rows(rows as usize)));
                }
                self.fast_path_fallthrough.fetch_add(1, Relaxed);
                // Build the physical plan from a single `state` snapshot rather than via
                // `execute_logical_plan` + `DataFrame::collect`, whose `create_physical_plan`
                // re-runs the FULL logical optimizer on every execute. The portal plan was
                // already optimized at parse time and `replace_params_with_values`/
                // `fold_literal_casts` preserve that (the substituted values only feed
                // parameter-independent rules), so re-optimizing an 89-column unnest INSERT
                // per statement is pure waste (~5% of total CPU, measured). Only skip the
                // optimize when the plan came from the pre-optimizing parse cache; ad-hoc
                // DML that never hit the cache still gets a full optimize. `create_physical_plan`
                // is `optimize()` + `query_planner().create_physical_plan()`, so this is that
                // minus the redundant optimize. Reuse `state` for planning AND `task_ctx` so
                // execution runs against the same snapshot the plan was built against.
                let state = session_context.state();
                let folded = if self.was_pre_optimized(&statement.to_string()) {
                    folded
                } else {
                    state.optimize(&folded).map_err(api_err)?
                };
                let phys = state.query_planner().create_physical_plan(&folded, &state).await.map_err(api_err)?;
                let batches = datafusion::physical_plan::collect(phys, state.task_ctx()).await.map_err(api_err)?;
                dml_response(&folded, &batches).ok_or_else(|| PgWireError::ApiError("internal error: DML plan returned non-DML completion".to_string().into()))
            }
            .await,
        )
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
    fn parameterize_extracts_strings_in_walk_order_and_skips_numbers() {
        let stmt = parse("SELECT id FROM t WHERE project_id = 'p1' AND ts > '2026-07-01' AND n = 5 LIMIT 100");
        let (param, values) = parameterize_statement(&stmt).expect("has string literals");
        let text = param.to_string();
        assert!(text.contains("$1") && text.contains("$2"), "strings become placeholders: {text}");
        assert!(text.contains("= 5") && text.contains("LIMIT 100"), "numbers stay inline: {text}");
        assert_eq!(
            values,
            vec![ScalarValue::Utf8(Some("p1".into())), ScalarValue::Utf8(Some("2026-07-01".into()))],
            "values extracted in walk order"
        );
        // Same shape with different literals → identical shape key.
        let stmt2 = parse("SELECT id FROM t WHERE project_id = 'p2' AND ts > '2026-07-04' AND n = 5 LIMIT 100");
        let (param2, _) = parameterize_statement(&stmt2).unwrap();
        assert_eq!(text, param2.to_string(), "shape key must be literal-insensitive");
    }

    #[test]
    fn parameterize_none_without_string_literals() {
        assert!(parameterize_statement(&parse("SELECT count(*) FROM t WHERE n = 5")).is_none());
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
        assert!(!contains_plan_time_folded_fn(&parse(
            "SELECT id FROM t WHERE project_id = 'p' AND ts > '2026-07-01'"
        )));
        // A column merely NAMED now must not disqualify.
        assert!(!contains_plan_time_folded_fn(&parse("SELECT now FROM t WHERE project_id = 'p'")));
    }
}
