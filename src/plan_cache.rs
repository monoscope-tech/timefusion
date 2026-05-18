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

use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion_postgres::hooks::{HookClient, QueryHook};
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Mutex;
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

pub struct PlanCacheHook {
    cache: Mutex<LruCache<String, LogicalPlan>>,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
}

impl Default for PlanCacheHook {
    fn default() -> Self {
        Self::new(DEFAULT_PLAN_CACHE_CAPACITY)
    }
}

impl PlanCacheHook {
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            cache: Mutex::new(LruCache::new(cap)),
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Returns (hits, misses) for stats observability.
    pub fn counters(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (self.hits.load(Relaxed), self.misses.load(Relaxed))
    }

    /// Only cache INSERTs and SELECTs that have at least one placeholder.
    /// Without a placeholder, the canonical text contains literal values
    /// (timestamps, UUIDs, etc.) which would never recur — caching that
    /// just pollutes the LRU and increases lock contention.
    fn cacheable(stmt: &Statement, sql: &str) -> bool {
        // Cheap heuristic: only consider DML statement kinds and require a
        // placeholder marker in the source text. Avoids walking the AST.
        let has_placeholder = sql.contains('$');
        matches!(stmt, Statement::Insert(_) | Statement::Query(_) | Statement::Update { .. } | Statement::Delete(_)) && has_placeholder
    }
}

#[async_trait]
impl QueryHook for PlanCacheHook {
    async fn handle_simple_query(
        &self, _statement: &Statement, _session_context: &SessionContext, _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }

    async fn handle_extended_parse_query(
        &self, statement: &Statement, session_context: &SessionContext, _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let canonical = statement.to_string();
        if !Self::cacheable(statement, &canonical) {
            return None;
        }

        if let Ok(mut guard) = self.cache.lock() {
            if let Some(plan) = guard.get(&canonical) {
                self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                debug!(target: "plan_cache", "hit: {}", canonical);
                return Some(Ok(plan.clone()));
            }
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
        if let Ok(mut guard) = self.cache.lock() {
            guard.put(canonical, plan.clone());
        }
        Some(Ok(plan))
    }

    async fn handle_extended_query(
        &self, _statement: &Statement, _logical_plan: &LogicalPlan, _params: &datafusion::common::ParamValues,
        _session_context: &SessionContext, _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }
}
