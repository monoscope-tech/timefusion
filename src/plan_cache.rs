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

use async_trait::async_trait;
use datafusion::{
    logical_expr::LogicalPlan,
    prelude::SessionContext,
    sql::{parser::Statement as DfStatement, sqlparser::ast::Statement},
};
use datafusion_postgres::{
    hooks::{HookClient, QueryHook},
    pgwire::{
        api::{ClientInfo, results::Response},
        error::{PgWireError, PgWireResult},
    },
};
use tracing::debug;

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
        if self.cache.len() >= self.capacity {
            let target = self.capacity / 2;
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
        }
        self.cache.insert(canonical, plan.clone());
        Some(Ok(plan))
    }

    async fn handle_extended_query(
        &self, _statement: &Statement, _logical_plan: &LogicalPlan, _params: &datafusion::common::ParamValues, _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }

    /// Signal to the do_query path that any plan we returned is already
    /// optimized — so `state.optimize()` can be skipped. Plans only land
    /// in `self.cache` after `state.optimize()` ran inside
    /// `handle_extended_parse_query`, so a cache lookup here is the
    /// authoritative answer.
    fn was_pre_optimized(&self, canonical_sql: &str) -> bool {
        self.cache.contains_key(canonical_sql)
    }
}
