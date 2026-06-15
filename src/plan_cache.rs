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
        let new_exprs: Vec<Expr> = node
            .expressions()
            .into_iter()
            .map(|expr| {
                expr.transform_up(|e| {
                    if let Expr::Cast(Cast { expr, data_type }) = &e
                        && let Expr::Literal(value, metadata) = expr.as_ref()
                    {
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
                })
                .map(|t| t.data)
            })
            .collect::<datafusion::error::Result<_>>()?;
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
    let Some(routing) = provider.as_any().downcast_ref::<crate::database::ProjectRoutingTable>() else {
        return Ok(None);
    };
    let rows = routing.fast_insert_batch(batch).await?;
    Ok(Some(rows))
}

/// Mirror of the vendored `datafusion_postgres::handlers::dml_completion`,
/// which is `pub(super)` and so unreachable from outside the crate.
///
/// **Re-vendor checklist.** When bumping the vendored `datafusion-postgres`,
/// diff `vendored handlers.rs::dml_completion` against this implementation —
/// upstream changes to the tag format ("INSERT 0 N" oid + count), the
/// `count` column name, or the count column's Arrow type are silent
/// divergence here (no compile error, wrong wire response). Search for the
/// `RE-VENDOR-DML-COMPLETION` marker below and confirm parity.
// RE-VENDOR-DML-COMPLETION: keep in sync with vendor/datafusion-postgres/src/handlers.rs.
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
    let rows = batches
        .first()
        .and_then(|b| b.column_by_name("count"))
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .map_or(0, |a| a.value(0) as usize);
    Ok(Some(Response::Execution(tag.with_rows(rows))))
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
    cache:    dashmap::DashMap<String, LogicalPlan>,
    capacity: usize,
    hits:     std::sync::atomic::AtomicU64,
    misses:   std::sync::atomic::AtomicU64,
}

impl Default for PlanCacheHook {
    fn default() -> Self {
        Self::new(DEFAULT_PLAN_CACHE_CAPACITY)
    }
}

impl PlanCacheHook {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache:    dashmap::DashMap::new(),
            capacity: capacity.max(1),
            hits:     std::sync::atomic::AtomicU64::new(0),
            misses:   std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Returns (hits, misses) for stats observability.
    pub fn counters(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (self.hits.load(Relaxed), self.misses.load(Relaxed))
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
    /// for canonical SQL not present in our cache — see
    /// `vendor/datafusion-postgres/PATCHES.md`.
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
        let canonical = statement.to_string();
        if !Self::has_placeholder(&canonical) {
            return None;
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
        // query 30ms cost into a one-time amortization. Vendored
        // datafusion-postgres skips its `state.optimize()` call when the
        // hook returns Some — see `vendor/datafusion-postgres/src/handlers.rs`.
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
        self.cache.contains_key(canonical_sql)
    }
}
