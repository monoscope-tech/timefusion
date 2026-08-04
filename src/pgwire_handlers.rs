use std::{
    borrow::Cow,
    fmt::Debug,
    sync::{Arc, LazyLock},
};

use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion_postgres::{
    DfSessionService,
    hooks::{QueryHook, set_show::SetShowHook, transactions::TransactionStatementHook},
    pgwire::{
        api::{
            ClientInfo, ClientPortalStore, ErrorHandler, PgWireServerHandlers,
            auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler, cleartext::CleartextPasswordAuthStartupHandler},
            portal::Portal,
            query::{ExtendedQueryHandler, SimpleQueryHandler},
            results::{DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag},
            stmt::StoredStatement,
            store::PortalStore,
        },
        error::{ErrorInfo, PgWireError, PgWireResult},
        messages::PgWireBackendMessage,
    },
};
use futures::{Sink, TryStreamExt, stream};
use regex::Regex;
use sha2::{Digest, Sha256};
use tracing::{Instrument, error, field::Empty, info, instrument};

use crate::{database::Database, plan_cache::PlanCacheHook};

/// Auth configuration for PgWire server
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub username: String,
    pub password: Option<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self { username: "postgres".into(), password: None }
    }
}

impl AuthConfig {
    /// Construct from `CoreConfig`, requiring an explicit password unless
    /// `TIMEFUSION_ALLOW_INSECURE_AUTH=true` is set. We hard-fail the
    /// startup path rather than silently accept an empty password — the
    /// PG wire protocol's cleartext handler treats `None` as "accept any",
    /// which is an open ingest endpoint when bound to 0.0.0.0.
    pub fn from_core(core: &crate::config::CoreConfig) -> anyhow::Result<Self> {
        let allow_insecure = crate::config::is_insecure_auth_allowed();
        match (&core.pgwire_password, allow_insecure) {
            (Some(p), _) if !p.is_empty() => Ok(Self { username: core.pgwire_user.clone(), password: Some(p.clone()) }),
            (_, true) => {
                tracing::warn!(
                    "PGWIRE_PASSWORD unset and TIMEFUSION_ALLOW_INSECURE_AUTH=true — pgwire endpoint accepts any password. Acceptable for local dev ONLY; never in production."
                );
                Ok(Self { username: core.pgwire_user.clone(), password: None })
            }
            _ => anyhow::bail!("PGWIRE_PASSWORD is required (set TIMEFUSION_ALLOW_INSECURE_AUTH=true to opt into open auth for local dev)"),
        }
    }
}

/// AuthSource that validates against configured credentials
#[derive(Debug, Clone)]
pub struct ConfigAuthSource {
    config: AuthConfig,
}

impl ConfigAuthSource {
    pub fn new(config: AuthConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl AuthSource for ConfigAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let username = login.user().unwrap_or("");
        (username == self.config.username).then(|| Password::new(None, self.config.password.clone().unwrap_or_default().into_bytes())).ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new("FATAL".into(), "28P01".into(), format!("password authentication failed for user \"{username}\""))))
        })
    }
}

/// Custom handler factory that creates handlers with logging and auth
pub struct LoggingHandlerFactory {
    session_context: Arc<SessionContext>,
    auth_config: AuthConfig,
    plan_cache: Arc<PlanCacheHook>,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>,
    db: Option<Arc<Database>>,
}

impl LoggingHandlerFactory {
    pub fn new(session_context: Arc<SessionContext>, auth_config: AuthConfig) -> Self {
        let plan_cache = Arc::new(PlanCacheHook::default());
        crate::plan_cache::set_global(plan_cache.clone());
        Self { session_context, auth_config, plan_cache, scan_metrics: None, db: None }
    }

    pub fn with_scan_metrics(mut self, m: Arc<crate::database::ScanMetrics>) -> Self {
        self.scan_metrics = Some(m);
        self
    }

    /// Enables the on-demand `OPTIMIZE <table> WHERE date = '...'` admin command
    /// (intercepted in the simple-query path). Unset in test servers, which
    /// don't need it.
    pub fn with_database(mut self, db: Arc<Database>) -> Self {
        self.db = Some(db);
        self
    }

    /// Hook list passed to every `DfSessionService` instance the factory
    /// produces. Sharing the single `plan_cache` Arc is what makes the LRU
    /// global rather than per-connection.
    fn hooks(&self) -> Vec<Arc<dyn QueryHook>> {
        vec![self.plan_cache.clone() as Arc<dyn QueryHook>, Arc::new(SetShowHook), Arc::new(TransactionStatementHook)]
    }

    pub fn plan_cache(&self) -> Arc<PlanCacheHook> {
        self.plan_cache.clone()
    }
}

impl PgWireServerHandlers for LoggingHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        let h = LoggingSimpleQueryHandler::new_with_hooks(self.session_context.clone(), self.hooks());
        let h = self.scan_metrics.iter().fold(h, |h, m| h.with_scan_metrics(m.clone()));
        Arc::new(self.db.iter().fold(h, |h, db| h.with_database(db.clone())))
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        let h = LoggingExtendedQueryHandler::new_with_hooks(self.session_context.clone(), self.hooks());
        Arc::new(self.scan_metrics.iter().fold(h, |h, m| h.with_scan_metrics(m.clone())))
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(CleartextPasswordAuthStartupHandler::new(ConfigAuthSource::new(self.auth_config.clone()), DefaultServerParameterProvider::default()))
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(LoggingErrorHandler)
    }
}

struct LoggingErrorHandler;

impl ErrorHandler for LoggingErrorHandler {
    fn on_error<C>(&self, _client: &C, error: &mut PgWireError)
    where
        C: ClientInfo,
    {
        // `ApiError` wraps an internal failure (DataFusion error, including
        // `Internal error` assertions that indicate a bug) — surface at error so
        // it isn't buried. Everything else (client `UserError`, `IoError` /
        // connection resets, protocol errors) is expected or infra noise — info.
        match error {
            PgWireError::ApiError(_) => error!("PgWire internal error: {}", error),
            _ => info!("PgWire error: {}", error),
        }
    }
}

/// Concurrent-giant-statement gate. A mega-statement (monoscope's multi-MB
/// INSERTs / unnest-array enrichment UPDATEs) materializes its literals and
/// bound parameters as ScalarValue arrays during plan + bind — tens to
/// hundreds of MB of transient heap per statement, bounded only by connection
/// concurrency. The 08:13Z 2026-08-03 OOM's pre-kill heap dumps were dominated
/// by exactly this (`ScalarValue::iter_to_array`/`make_run_array` under
/// pgwire). Two permits: one giant can always run while another queues, and
/// worst-case transient parse heap is 2x one statement instead of Nx.
const GIANT_STMT_BYTES: usize = 2 * 1024 * 1024;
static GIANT_STMT_SEM: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

async fn giant_stmt_permit(len: usize) -> Option<tokio::sync::SemaphorePermit<'static>> {
    if len < GIANT_STMT_BYTES {
        return None;
    }
    let t0 = std::time::Instant::now();
    let permit = GIANT_STMT_SEM.acquire().await.expect("giant-stmt semaphore never closed");
    let waited = t0.elapsed();
    if waited.as_millis() > 50 {
        tracing::info!("giant statement ({len} B) queued {waited:?} behind the 2-permit parse gate");
    }
    Some(permit)
}

/// Simple query handler with tracing
pub struct LoggingSimpleQueryHandler {
    inner: DfSessionService,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>,
    db: Option<Arc<Database>>,
}

impl LoggingSimpleQueryHandler {
    pub fn new_with_hooks(session_context: Arc<SessionContext>, hooks: Vec<Arc<dyn QueryHook>>) -> Self {
        Self { inner: DfSessionService::new_with_hooks(session_context, hooks), scan_metrics: None, db: None }
    }

    pub fn with_scan_metrics(mut self, m: Arc<crate::database::ScanMetrics>) -> Self {
        self.scan_metrics = Some(m);
        self
    }

    pub fn with_database(mut self, db: Arc<Database>) -> Self {
        self.db = Some(db);
        self
    }

    /// Execute an intercepted `OPTIMIZE <table> WHERE date = '...'`.
    async fn run_optimize(&self, cmd: OptimizeCmd) -> PgWireResult<Vec<Response>> {
        let db = require_available(self.db.as_ref(), "OPTIMIZE")?;
        let table_ref = db.get_or_create_unified_table(&cmd.table).await.map_err(|e| admin_err(format!("OPTIMIZE: open table '{}': {e}", cmd.table)))?;
        let (removed, added) = db.compact_date(&table_ref, &cmd.table, cmd.date, cmd.project_id.as_deref()).await.map_err(|e| admin_err(e.to_string()))?;
        info!("pgwire OPTIMIZE {} date={} project={:?}: {removed} removed, {added} added", cmd.table, cmd.date, cmd.project_id);
        Ok(vec![Response::Execution(Tag::new(&format!("OPTIMIZE {removed} {added}")))])
    }

    /// Execute an intercepted `VACUUM <table> [RETAIN <n> HOURS]`.
    async fn run_vacuum(&self, cmd: VacuumCmd) -> PgWireResult<Vec<Response>> {
        let db = require_available(self.db.as_ref(), "VACUUM")?;
        let deleted = db.vacuum_named(&cmd.table, cmd.retention_hours).await.map_err(|e| admin_err(format!("VACUUM '{}': {e}", cmd.table)))?;
        info!("pgwire VACUUM {} retention={:?}: {deleted} files deleted", cmd.table, cmd.retention_hours);
        Ok(vec![Response::Execution(Tag::new(&format!("VACUUM {deleted}")))])
    }

    /// Execute an intercepted `FLUSH` — drain the whole MemBuffer to Delta.
    /// Ops pre-restart hook: run it right before a planned restart/deploy so
    /// the stop grace never bounds the shutdown flush and the boot's WAL
    /// replay is near-empty. Errors when any bucket fails so callers can
    /// gate on it.
    async fn run_flush(&self) -> PgWireResult<Vec<Response>> {
        let layer = require_available(self.db.as_ref().and_then(|d| d.buffered_layer()), "FLUSH")?;
        // Misuse guard: FLUSH commits the open window per table (tiny parquet
        // files + tantivy builds), so a looping client mints file-count
        // explosion and contends flush_lock with routine flushes. Operator
        // cadence is "once before a deploy" — enforce a floor between runs.
        // Frozen-clock (test) harnesses are exempt: their cadence is
        // script-driven and the frozen epoch resets between environments.
        use std::sync::atomic::{AtomicI64, Ordering};
        const FLUSH_MIN_INTERVAL_SECS: i64 = 10;
        static LAST_FLUSH_MICROS: AtomicI64 = AtomicI64::new(i64::MIN);
        if !crate::clock::is_frozen() {
            let now = chrono::Utc::now().timestamp_micros();
            let since = now.saturating_sub(LAST_FLUSH_MICROS.load(Ordering::Acquire));
            if since < FLUSH_MIN_INTERVAL_SECS * 1_000_000 {
                return Err(admin_err(format!("FLUSH rate-limited: last ran {}s ago (min interval {FLUSH_MIN_INTERVAL_SECS}s)", since / 1_000_000)));
            }
            LAST_FLUSH_MICROS.store(now, Ordering::Release);
        }
        let stats = layer.flush_all_now().await.map_err(|e| admin_err(format!("FLUSH: {e}")))?;
        info!("pgwire FLUSH: {} bucket(s) flushed ({} rows), {} failed", stats.buckets_flushed, stats.total_rows, stats.buckets_failed);
        if stats.buckets_failed > 0 {
            return Err(admin_err(format!(
                "FLUSH: {} bucket(s) failed to flush ({} flushed) — data stays buffered/WAL-durable",
                stats.buckets_failed, stats.buckets_flushed
            )));
        }
        // Walrus deletes checkpoint-eligible segments on its background tick.
        // Wait for one tick while this instance is still serving, otherwise
        // the replacement pays to scan the outgoing instance's consumed WAL.
        // Frozen-clock test harnesses skip this operational handoff delay.
        if !crate::clock::is_frozen() {
            layer.reclaim_wal_for_planned_handoff().await;
        }
        layer.mark_planned_handoff();
        Ok(vec![Response::Execution(Tag::new(&format!("FLUSH {}", stats.total_rows)))])
    }

    /// Read recent Delta commit metadata without requiring direct object-store
    /// credentials on the operator's machine.
    async fn run_delta_history(&self, cmd: DeltaHistoryCmd) -> PgWireResult<Vec<Response>> {
        use datafusion_postgres::pgwire::api::Type;

        let db = require_available(self.db.as_ref(), "DELTA HISTORY")?;
        let table_ref = db.get_or_create_unified_table(&cmd.table).await.map_err(|e| admin_err(format!("DELTA HISTORY: open table '{}': {e}", cmd.table)))?;
        let table = table_ref.read().await;
        let commits: Vec<_> = table.history(Some(cmd.limit)).await.map_err(|e| admin_err(format!("DELTA HISTORY '{}': {e}", cmd.table)))?.collect();
        drop(table);

        let fields = Arc::new(
            ["version", "timestamp_utc", "operation", "read_version", "is_blind_append", "operation_parameters", "commit_info"]
                .into_iter()
                .map(|name| FieldInfo::new(name.to_string(), None, None, Type::VARCHAR, FieldFormat::Text))
                .collect::<Vec<_>>(),
        );
        let rows = commits.into_iter().map({
            let fields = fields.clone();
            move |commit| {
                let mut encoder = DataRowEncoder::new(fields.clone());
                let timestamp = commit.timestamp.and_then(chrono::DateTime::from_timestamp_millis).map(|v| v.to_rfc3339()).unwrap_or_default();
                let read_version = commit.read_version.map(|v| v.to_string()).unwrap_or_default();
                let version = commit.read_version.map(|v| (v + 1).to_string()).unwrap_or_default();
                let operation = commit.operation.clone().unwrap_or_default();
                let blind_append = commit.is_blind_append.map(|v| v.to_string()).unwrap_or_default();
                let parameters = serde_json::to_string(&commit.operation_parameters).unwrap_or_default();
                let info = serde_json::to_string(&commit).unwrap_or_default();
                for value in [&version, &timestamp, &operation, &read_version, &blind_append, &parameters, &info] {
                    encoder.encode_field(value)?;
                }
                Ok(encoder.take_row())
            }
        });
        Ok(vec![Response::Query(QueryResponse::new(fields, stream::iter(rows)))])
    }

    /// Return every raw action in one Delta commit. This is an audit primitive:
    /// it reads the transaction log only and never constructs a transaction.
    async fn run_delta_actions(&self, cmd: DeltaActionsCmd) -> PgWireResult<Vec<Response>> {
        use datafusion_postgres::pgwire::api::Type;

        let db = require_available(self.db.as_ref(), "DELTA ACTIONS")?;
        let table_ref = db.get_or_create_unified_table(&cmd.table).await.map_err(|e| admin_err(format!("DELTA ACTIONS: open table '{}': {e}", cmd.table)))?;
        let log_store = table_ref.read().await.log_store();
        let bytes = log_store
            .read_commit_entry(cmd.version)
            .await
            .map_err(|e| admin_err(format!("DELTA ACTIONS '{}' VERSION {}: {e}", cmd.table, cmd.version)))?
            .ok_or_else(|| admin_err(format!("DELTA ACTIONS '{}' VERSION {}: commit not found", cmd.table, cmd.version)))?;
        let actions = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<deltalake::kernel::Action>(line).map_err(|e| admin_err(format!("decode Delta action: {e}"))))
            .collect::<PgWireResult<Vec<_>>>()?;

        let fields = Arc::new(
            ["version", "action", "path", "size_bytes", "action_json"]
                .into_iter()
                .map(|name| FieldInfo::new(name.to_string(), None, None, Type::VARCHAR, FieldFormat::Text))
                .collect::<Vec<_>>(),
        );
        let rows = actions.into_iter().map({
            let fields = fields.clone();
            move |action| {
                let (kind, path, size) = match &action {
                    deltalake::kernel::Action::Add(add) => ("add", add.path.as_str(), add.size.to_string()),
                    deltalake::kernel::Action::Remove(remove) => ("remove", remove.path.as_str(), remove.size.map(|v| v.to_string()).unwrap_or_default()),
                    deltalake::kernel::Action::CommitInfo(_) => ("commitInfo", "", String::new()),
                    _ => ("other", "", String::new()),
                };
                let version = cmd.version.to_string();
                let json = serde_json::to_string(&action).map_err(|e| admin_err(format!("encode Delta action: {e}")))?;
                let mut encoder = DataRowEncoder::new(fields.clone());
                for value in [&version, kind, path, &size, &json] {
                    encoder.encode_field(&value)?;
                }
                Ok(encoder.take_row())
            }
        });
        Ok(vec![Response::Query(QueryResponse::new(fields, stream::iter(rows)))])
    }

    /// Reconstruct the full pre-commit Add actions for files removed by
    /// `version`. This is read-only and fails unless every removal has a source.
    async fn run_delta_recovery_audit(&self, cmd: DeltaRecoveryAuditCmd) -> PgWireResult<Vec<Response>> {
        use datafusion_postgres::pgwire::api::Type;

        let db = require_available(self.db.as_ref(), "DELTA RECOVERY AUDIT")?;
        let table_ref =
            db.get_or_create_unified_table(&cmd.table).await.map_err(|e| admin_err(format!("DELTA RECOVERY AUDIT: open table '{}': {e}", cmd.table)))?;
        let mut before = table_ref.read().await.clone();
        let bytes = before
            .log_store()
            .read_commit_entry(cmd.version)
            .await
            .map_err(|e| admin_err(format!("DELTA RECOVERY AUDIT '{}' VERSION {}: {e}", cmd.table, cmd.version)))?
            .ok_or_else(|| admin_err(format!("DELTA RECOVERY AUDIT '{}' VERSION {}: commit not found", cmd.table, cmd.version)))?;
        let removed = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<deltalake::kernel::Action>(line).map_err(|e| admin_err(format!("decode Delta action: {e}"))))
            .collect::<PgWireResult<Vec<_>>>()?
            .into_iter()
            .filter_map(|action| match action {
                deltalake::kernel::Action::Remove(remove) => Some(remove.path),
                _ => None,
            })
            .collect::<std::collections::HashSet<_>>();
        if removed.is_empty() {
            return Err(admin_err(format!("DELTA RECOVERY AUDIT '{}' VERSION {}: commit removed no files", cmd.table, cmd.version)));
        }
        let previous = cmd.version.checked_sub(1).ok_or_else(|| admin_err("DELTA RECOVERY AUDIT cannot inspect before version 0"))?;
        before.load_version(previous).await.map_err(|e| admin_err(format!("DELTA RECOVERY AUDIT '{}': load version {previous}: {e}", cmd.table)))?;
        let mut sources = before
            .get_active_add_actions_by_partitions(&[])
            .try_filter_map(|view| {
                let include = removed.contains(view.path().as_ref());
                // The replacement Arrow-table API is intended for analytics
                // and does not round-trip a complete Add action. Recovery must
                // preserve raw stats/tags byte-for-byte, which this delta-rs
                // compatibility method explicitly guarantees.
                #[allow(deprecated)]
                let source = include.then(|| view.add_action());
                futures::future::ready(Ok(source))
            })
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| admin_err(format!("DELTA RECOVERY AUDIT '{}': read source actions: {e}", cmd.table)))?;
        if sources.len() != removed.len() {
            return Err(admin_err(format!(
                "DELTA RECOVERY AUDIT '{}' VERSION {}: reconstructed {} of {} removed files",
                cmd.table,
                cmd.version,
                sources.len(),
                removed.len()
            )));
        }
        sources.sort_unstable_by(|a, b| a.path.cmp(&b.path));

        let fields = Arc::new(
            ["removed_by_version", "path", "size_bytes", "source_add_json"]
                .into_iter()
                .map(|name| FieldInfo::new(name.to_string(), None, None, Type::VARCHAR, FieldFormat::Text))
                .collect::<Vec<_>>(),
        );
        let rows = sources.into_iter().map({
            let fields = fields.clone();
            move |add| {
                let version = cmd.version.to_string();
                let size = add.size.to_string();
                let json = serde_json::to_string(&deltalake::kernel::Action::Add(add.clone())).map_err(|e| admin_err(format!("encode source Add: {e}")))?;
                let mut encoder = DataRowEncoder::new(fields.clone());
                for value in [&version, &add.path, &size, &json] {
                    encoder.encode_field(value)?;
                }
                Ok(encoder.take_row())
            }
        });
        Ok(vec![Response::Query(QueryResponse::new(fields, stream::iter(rows)))])
    }
}

/// `DELTA HISTORY <table> [LIMIT <n>]` is deliberately read-only.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DeltaHistoryCmd {
    pub table: String,
    pub limit: usize,
}

pub(crate) fn parse_delta_history(query: &str) -> Result<Option<DeltaHistoryCmd>, String> {
    let Some(rest) = strip_command(query, "delta") else { return Ok(None) };
    let Some(rest) = strip_keyword(rest, "history", char::is_whitespace) else {
        return Err("DELTA supports only: DELTA HISTORY <table> [LIMIT <n>]".to_string());
    };
    let mut parts = rest.split_whitespace();
    let table = parts.next().ok_or("DELTA HISTORY requires a table: DELTA HISTORY <table> [LIMIT <n>]")?;
    let limit = match (parts.next(), parts.next(), parts.next()) {
        (None, None, None) => 100,
        (Some(keyword), Some(value), None) if keyword.eq_ignore_ascii_case("limit") => {
            let limit = value.parse::<usize>().map_err(|_| format!("invalid DELTA HISTORY limit '{value}'"))?;
            if !(1..=10_000).contains(&limit) {
                return Err("DELTA HISTORY limit must be between 1 and 10000".to_string());
            }
            limit
        }
        _ => return Err("expected: DELTA HISTORY <table> [LIMIT <n>]".to_string()),
    };
    Ok(Some(DeltaHistoryCmd { table: table.to_string(), limit }))
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DeltaActionsCmd {
    pub table: String,
    pub version: u64,
}

pub(crate) fn parse_delta_actions(query: &str) -> Result<Option<DeltaActionsCmd>, String> {
    let Some(rest) = strip_command(query, "delta") else { return Ok(None) };
    let Some(rest) = strip_keyword(rest, "actions", char::is_whitespace) else { return Ok(None) };
    let mut parts = rest.split_whitespace();
    let table = parts.next().ok_or("DELTA ACTIONS requires: DELTA ACTIONS <table> VERSION <n>")?;
    let keyword = parts.next().ok_or("DELTA ACTIONS requires a VERSION")?;
    let value = parts.next().ok_or("DELTA ACTIONS requires a numeric VERSION")?;
    if !keyword.eq_ignore_ascii_case("version") || parts.next().is_some() {
        return Err("expected: DELTA ACTIONS <table> VERSION <n>".to_string());
    }
    let version = value.parse::<u64>().map_err(|_| format!("invalid Delta version '{value}'"))?;
    Ok(Some(DeltaActionsCmd { table: table.to_string(), version }))
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DeltaRecoveryAuditCmd {
    pub table: String,
    pub version: u64,
}

pub(crate) fn parse_delta_recovery_audit(query: &str) -> Result<Option<DeltaRecoveryAuditCmd>, String> {
    let Some(rest) = strip_command(query, "delta") else { return Ok(None) };
    let Some(rest) = strip_keyword(rest, "recovery", char::is_whitespace) else { return Ok(None) };
    let Some(rest) = strip_keyword(rest.trim(), "audit", char::is_whitespace) else {
        return Err("DELTA RECOVERY supports only: DELTA RECOVERY AUDIT <table> VERSION <n>".to_string());
    };
    let mut parts = rest.split_whitespace();
    let table = parts.next().ok_or("DELTA RECOVERY AUDIT requires a table")?;
    let keyword = parts.next().ok_or("DELTA RECOVERY AUDIT requires a VERSION")?;
    let value = parts.next().ok_or("DELTA RECOVERY AUDIT requires a numeric VERSION")?;
    if !keyword.eq_ignore_ascii_case("version") || parts.next().is_some() {
        return Err("expected: DELTA RECOVERY AUDIT <table> VERSION <n>".to_string());
    }
    let version = value.parse::<u64>().map_err(|_| format!("invalid Delta version '{value}'"))?;
    Ok(Some(DeltaRecoveryAuditCmd { table: table.to_string(), version }))
}

/// An intercepted `OPTIMIZE <table> WHERE date = 'YYYY-MM-DD'` admin command.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OptimizeCmd {
    pub table: String,
    pub date: chrono::NaiveDate,
    /// Restrict the compaction to one tenant's partition. A whole-date
    /// optimize spans every project's files for that date — tens of GB on a
    /// busy day, which doesn't fit in-process next to serving load
    /// (2026-07-27: two OOMs). One (project, date) partition is a few GB.
    pub project_id: Option<String>,
}

fn admin_err(msg: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new("ERROR".into(), "42601".into(), msg.into())))
}

/// `Ok(inner)` when the admin command's dependency (db handle, buffered layer)
/// was wired in, else the standard "not available on this server" error.
fn require_available<T>(opt: Option<T>, name: &str) -> PgWireResult<T> {
    opt.ok_or_else(|| admin_err(format!("{name} is not available on this server")))
}

/// Remainder after a leading case-insensitive `keyword` that ends at end-of-input
/// or at a `boundary` char — so identifiers merely starting with it
/// (`optimizer_stats`, `aborted`) don't match. Remainder is returned untrimmed.
fn strip_keyword<'a>(s: &'a str, keyword: &str, boundary: fn(char) -> bool) -> Option<&'a str> {
    let (head, rest) = s.split_at_checked(keyword.len())?;
    (head.eq_ignore_ascii_case(keyword) && (rest.is_empty() || rest.starts_with(boundary))).then_some(rest)
}

/// Strip a leading admin keyword plus any trailing `;`, returning the trimmed
/// remainder. `None` when `query` isn't that command.
fn strip_command<'a>(query: &'a str, keyword: &str) -> Option<&'a str> {
    strip_keyword(query.trim().trim_end_matches(';').trim(), keyword, char::is_whitespace).map(str::trim)
}

/// `= '<value>'` → `<value>`, tolerating either quote style and loose spacing.
fn filter_value(rest: &str) -> Result<&str, String> {
    Ok(rest.trim().strip_prefix('=').ok_or("expected: <col> = '<value>'")?.trim().trim_matches(['\'', '"']).trim())
}

/// Parse `OPTIMIZE <table> WHERE date = 'YYYY-MM-DD'`.
///
/// - `Ok(None)`: not an OPTIMIZE statement — fall through to DataFusion.
/// - `Ok(Some(_))`: valid, run it.
/// - `Err(msg)`: it *is* OPTIMIZE but malformed (no table, missing/non-`date`
///   filter, bad date). A bare `OPTIMIZE <table>` is rejected on purpose — an
///   unbounded in-process compaction can OOM the instance — and surfaced as a
///   clear error rather than a confusing DataFusion parser error.
pub(crate) fn parse_optimize(query: &str) -> Result<Option<OptimizeCmd>, String> {
    let Some(rest) = strip_command(query, "optimize") else { return Ok(None) };
    let (table, where_part) = rest.split_once(char::is_whitespace).map(|(t, w)| (t.trim(), w.trim())).unwrap_or((rest, ""));
    if table.is_empty() {
        return Err("OPTIMIZE requires a table and date: OPTIMIZE <table> WHERE date = 'YYYY-MM-DD'".to_string());
    }
    let Some(conds) = strip_keyword(where_part, "where", char::is_whitespace) else {
        return Err(format!(
            "OPTIMIZE {table} needs a date filter: OPTIMIZE {table} WHERE date = 'YYYY-MM-DD' (bare OPTIMIZE is disabled — it would compact all history in-process)"
        ));
    };
    // `WHERE date = '...'` optionally AND-ed (either order) with
    // `project_id = '...'`. Values are simple quoted literals, so splitting on
    // a top-level ` AND ` needs no nesting awareness.
    static AND: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\s+and\s+").unwrap());
    let (date, project_id) = AND.split(conds.trim()).try_fold((None, None), |(date, project_id), cond| {
        let cond = cond.trim();
        let column = |name| strip_keyword(cond, name, |c: char| c.is_whitespace() || c == '=');
        match (column("date"), column("project_id")) {
            (Some(rest), _) => {
                let val = filter_value(rest)?;
                Ok((Some(val.parse::<chrono::NaiveDate>().map_err(|_| format!("invalid date '{val}', expected YYYY-MM-DD"))?), project_id))
            }
            (_, Some(rest)) => Ok((date, Some(filter_value(rest)?.to_string()))),
            _ => Err("OPTIMIZE supports only `date` and `project_id` filters".to_string()),
        }
    })?;
    let date = date.ok_or("OPTIMIZE requires a date filter: WHERE date = 'YYYY-MM-DD'")?;
    Ok(Some(OptimizeCmd { table: table.to_string(), date, project_id }))
}

/// An intercepted `VACUUM <table> [RETAIN <n> HOURS]` admin command.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct VacuumCmd {
    pub table: String,
    /// `None` → use the configured default retention.
    pub retention_hours: Option<u64>,
}

/// Parse `VACUUM <table> [RETAIN <n> HOURS]`.
///
/// - `Ok(None)`: not a VACUUM statement — fall through to DataFusion.
/// - `Ok(Some(_))`: valid, run it.
/// - `Err(msg)`: it *is* VACUUM but malformed. A bare `VACUUM` (no table) is
///   rejected on purpose — name the table explicitly. Unlike OPTIMIZE, VACUUM is
///   table-wide (all partitions) and takes no date filter; the optional
///   `RETAIN <n> HOURS` overrides the configured retention.
pub(crate) fn parse_vacuum(query: &str) -> Result<Option<VacuumCmd>, String> {
    let Some(rest) = strip_command(query, "vacuum") else { return Ok(None) };
    let (table, tail) = rest.split_once(char::is_whitespace).map(|(t, w)| (t.trim(), w.trim())).unwrap_or((rest, ""));
    if table.is_empty() {
        return Err("VACUUM requires a table: VACUUM <table> [RETAIN <n> HOURS] (bare VACUUM is disabled — name the table)".to_string());
    }
    let retention_hours = (!tail.is_empty())
        .then(|| {
            let lower = tail.to_ascii_lowercase();
            let after = strip_keyword(&lower, "retain", char::is_whitespace)
                .ok_or_else(|| format!("VACUUM {table}: expected optional `RETAIN <n> HOURS`, got '{tail}'"))?
                .trim();
            let num = after.strip_suffix("hours").or_else(|| after.strip_suffix("hour")).unwrap_or(after).trim();
            num.parse::<u64>().map_err(|_| format!("VACUUM {table}: invalid retention '{after}', expected `RETAIN <n> HOURS`"))
        })
        .transpose()?;
    Ok(Some(VacuumCmd { table: table.to_string(), retention_hours }))
}

/// Parse a bare `FLUSH` admin command (not a Postgres statement, so safe to
/// intercept). No arguments on purpose: it drains the whole MemBuffer.
pub(crate) fn parse_flush(query: &str) -> bool {
    strip_command(query, "flush").is_some_and(str::is_empty)
}

/// Rewrites Postgres synonyms that DataFusion's SQL parser doesn't accept.
///
/// `ABORT [ WORK | TRANSACTION ]` is a Postgres alias for `ROLLBACK`. Hasql's
/// connection pool emits `ABORT` defensively on session acquisition to clear
/// any leftover transaction state; without this rewrite, every Hasql client
/// (e.g. monoscope) sees its first statement on each connection fail with
/// `sql parser error: Expected: an SQL statement, found: ABORT`, which then
/// poisons the whole session.
fn rewrite_pg_synonyms(query: &str) -> Cow<'_, str> {
    strip_keyword(query.trim_start(), "ABORT", |c| c.is_whitespace() || c == ';').map_or(Cow::Borrowed(query), |rest| Cow::Owned(format!("ROLLBACK{rest}")))
}

/// (keyword, space-padded keyword, `query.type`, operation). First match wins,
/// so order is significant.
const QUERY_KINDS: [(&str, &str, &str, &str); 7] = [
    ("select", " select ", "SELECT", "SELECT"),
    ("update", " update ", "DML", "UPDATE"),
    ("delete", " delete ", "DML", "DELETE"),
    ("insert", " insert ", "DML", "INSERT"),
    ("create", " create ", "DDL", "CREATE"),
    ("drop", " drop ", "DDL", "DROP"),
    ("alter", " alter ", "DDL", "ALTER"),
];

fn classify_query(query: &str) -> (&'static str, &'static str) {
    let q = query.trim().to_lowercase();
    QUERY_KINDS
        .iter()
        .find(|(kw, padded, ..)| q.starts_with(kw) || q.contains(padded))
        .map_or(("OTHER", "UNKNOWN"), |&(.., query_type, operation)| (query_type, operation))
}

/// Redact literal values and comments so the result can safely be indexed and
/// used as a stable query fingerprint.
fn normalized_query(query: &str) -> String {
    static BLOCK_COMMENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?s)/\*.*?\*/").unwrap());
    static LINE_COMMENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"--[^\r\n]*").unwrap());
    static DOLLAR_STRING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?s)\$[A-Za-z_0-9]*\$.*?\$[A-Za-z_0-9]*\$").unwrap());
    static STRING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?is)(?:e|u&)?'(?:''|[^'])*'").unwrap());
    static NUMBER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\b").unwrap());
    static WHITESPACE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s+").unwrap());

    let query = BLOCK_COMMENT.replace_all(query, " ");
    let query = LINE_COMMENT.replace_all(&query, " ");
    let query = DOLLAR_STRING.replace_all(&query, "?");
    let query = STRING.replace_all(&query, "?");
    let query = NUMBER.replace_all(&query, "?");
    WHITESPACE.replace_all(&query, " ").trim().to_ascii_lowercase()
}

fn query_template(query: &str) -> String {
    const MAX_CHARS: usize = 512;
    let query = normalized_query(query);
    if query.chars().count() <= MAX_CHARS { query } else { query.chars().take(MAX_CHARS).chain(['.'; 3]).collect() }
}

fn query_fingerprint(query: &str) -> String {
    format!("{:x}", Sha256::digest(normalized_query(query).as_bytes()))
}

/// Classify `query` and stamp the standard query/db tracing fields onto `span`.
fn record_query_span(span: &tracing::Span, query: &str) {
    let (query_type, operation) = classify_query(query);
    span.record("query.type", query_type);
    span.record("query.operation", operation);
    span.record("db.operation", operation);
    span.record("query.text", query_template(query));
}

/// Emit one bounded event for statements slow enough to affect the tail. Table
/// and project dimensions are extracted only for diagnosis; raw SQL is never
/// included in this event.
fn record_statement_latency(metrics: Option<&crate::database::ScanMetrics>, query: &str, protocol: &'static str, duration_us: u64, success: bool) {
    if let Some(metrics) = metrics {
        metrics.record_pgwire_query(duration_us);
    }
    const SLOW_QUERY_US: u64 = 1_000_000;
    if duration_us < SLOW_QUERY_US {
        return;
    }

    let (_, operation) = classify_query(query);
    let (tables, project_id) = query_dimensions(query);
    info!(
        event = "pgwire.slow_statement",
        query.class = operation,
        query.fingerprint = %query_fingerprint(query),
        query.template = %query_template(query),
        query.tables = %tables,
        project.id = %project_id,
        protocol,
        duration_us,
        success,
        "slow PostgreSQL statement"
    );
}

fn query_dimensions(query: &str) -> (String, &str) {
    static TABLES: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"(?i)\b(?:from|join|into|update|table)\s+([\w.\"]+)"#).unwrap());
    static PROJECT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"(?i)\bproject_id\s*=\s*'([^']{1,128})'"#).unwrap());
    let tables = TABLES.captures_iter(query).filter_map(|captures| Some(captures.get(1)?.as_str())).take(3).collect::<Vec<_>>().join(",");
    (tables, PROJECT.captures(query).and_then(|captures| captures.get(1)).map_or("", |m| m.as_str()))
}

#[async_trait]
impl SimpleQueryHandler for LoggingSimpleQueryHandler {
    #[instrument(
        name = "postgres.query.simple",
        skip_all,
        fields(query.text = Empty, query.type = Empty, query.operation = Empty, db.system = "postgresql", db.operation = Empty)
    )]
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let rewritten = rewrite_pg_synonyms(query);
        let query = rewritten.as_ref();

        // Admin commands, caught before DataFusion (whose parser rejects all
        // three): OPTIMIZE compaction, VACUUM file reclamation, and FLUSH
        // (drain MemBuffer to Delta — pre-deploy hook).
        if let Some(cmd) = parse_optimize(query).map_err(admin_err)? {
            return self.run_optimize(cmd).await;
        }
        if let Some(cmd) = parse_vacuum(query).map_err(admin_err)? {
            return self.run_vacuum(cmd).await;
        }
        if let Some(cmd) = parse_delta_recovery_audit(query).map_err(admin_err)? {
            return self.run_delta_recovery_audit(cmd).await;
        }
        if let Some(cmd) = parse_delta_actions(query).map_err(admin_err)? {
            return self.run_delta_actions(cmd).await;
        }
        if let Some(cmd) = parse_delta_history(query).map_err(admin_err)? {
            return self.run_delta_history(cmd).await;
        }
        if parse_flush(query) {
            return self.run_flush().await;
        }

        let span = tracing::Span::current();
        record_query_span(&span, query);

        let _giant = giant_stmt_permit(query.len()).await;
        let execute_span = tracing::trace_span!(parent: &span, "datafusion.execute");
        let t0 = std::time::Instant::now();
        let result = <DfSessionService as SimpleQueryHandler>::do_query(&self.inner, client, query).instrument(execute_span).await;
        record_statement_latency(self.scan_metrics.as_deref(), query, "simple", t0.elapsed().as_micros() as u64, result.is_ok());
        result
    }
}

/// Extended query handler with tracing
pub struct LoggingExtendedQueryHandler {
    inner: DfSessionService,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>,
}

impl LoggingExtendedQueryHandler {
    pub fn with_scan_metrics(mut self, m: Arc<crate::database::ScanMetrics>) -> Self {
        self.scan_metrics = Some(m);
        self
    }

    pub fn new_with_hooks(session_context: Arc<SessionContext>, hooks: Vec<Arc<dyn QueryHook>>) -> Self {
        Self { inner: DfSessionService::new_with_hooks(session_context, hooks), scan_metrics: None }
    }
}

#[async_trait]
impl ExtendedQueryHandler for LoggingExtendedQueryHandler {
    type Statement = <DfSessionService as ExtendedQueryHandler>::Statement;
    type QueryParser = <DfSessionService as ExtendedQueryHandler>::QueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.inner.query_parser()
    }

    async fn do_describe_statement<C>(&self, client: &mut C, statement: &StoredStatement<Self::Statement>) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.inner.do_describe_statement(client, statement).await
    }

    async fn do_describe_portal<C>(&self, client: &mut C, portal: &Portal<Self::Statement>) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.inner.do_describe_portal(client, portal).await
    }

    #[instrument(
        name = "postgres.query.extended",
        skip_all,
        fields(query.text = Empty, query.type = Empty, query.operation = Empty, query.portal = %portal.name, query.max_rows = max_rows, db.system = "postgresql", db.operation = Empty)
    )]
    async fn do_query<C>(&self, client: &mut C, portal: &Portal<Self::Statement>, max_rows: usize) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let span = tracing::Span::current();
        let query = &portal.statement.statement.0;
        record_query_span(&span, query);

        let _giant = giant_stmt_permit(query.len()).await;
        let execute_span = tracing::trace_span!(parent: &span, "datafusion.execute");
        let t0 = std::time::Instant::now();
        let result = <DfSessionService as ExtendedQueryHandler>::do_query(&self.inner, client, portal, max_rows).instrument(execute_span).await;
        record_statement_latency(self.scan_metrics.as_deref(), query, "extended", t0.elapsed().as_micros() as u64, result.is_ok());
        result
    }
}

fn handler_factory(
    session_context: Arc<SessionContext>, auth_config: AuthConfig, scan_metrics: Option<Arc<crate::database::ScanMetrics>>, db: Option<Arc<Database>>,
) -> Arc<LoggingHandlerFactory> {
    let factory = LoggingHandlerFactory::new(session_context, auth_config);
    let factory = scan_metrics.into_iter().fold(factory, LoggingHandlerFactory::with_scan_metrics);
    Arc::new(db.into_iter().fold(factory, LoggingHandlerFactory::with_database))
}

/// Start the server with custom handlers. `db` enables the admin commands
/// (OPTIMIZE / VACUUM / FLUSH); without it they error as unavailable.
pub async fn serve_with_logging(
    session_context: Arc<SessionContext>, options: &datafusion_postgres::ServerOptions, auth_config: AuthConfig,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>, db: Option<Arc<Database>>, shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    let handlers = handler_factory(session_context, auth_config, scan_metrics, db);
    datafusion_postgres::serve_with_handlers(handlers, options, shutdown).await?;
    Ok(())
}

/// Variant of `serve_with_logging` over a pre-bound listener. The listener's
/// host/port/backlog were set at bind time; `options` here contributes only
/// TLS config and connection-limit settings.
pub async fn serve_with_listener(
    listener: tokio::net::TcpListener, session_context: Arc<SessionContext>, options: &datafusion_postgres::ServerOptions, auth_config: AuthConfig,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>, db: Option<Arc<Database>>, shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    let handlers = handler_factory(session_context, auth_config, scan_metrics, db);
    datafusion_postgres::serve_with_listener(listener, handlers, options, shutdown).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        parse_delta_actions, parse_delta_history, parse_delta_recovery_audit, parse_flush, parse_optimize, parse_vacuum, query_dimensions, query_fingerprint,
        query_template, rewrite_pg_synonyms,
    };

    #[test]
    fn optimize_parses_table_and_date() {
        let cmd = parse_optimize("OPTIMIZE otel_logs_and_spans WHERE date = '2026-06-19'").unwrap().unwrap();
        assert_eq!(cmd.table, "otel_logs_and_spans");
        assert_eq!(cmd.date, "2026-06-19".parse().unwrap());
        // Case / spacing / quote / trailing-semicolon tolerance.
        assert_eq!(parse_optimize("optimize t where DATE='2026-01-02';").unwrap().unwrap().date, "2026-01-02".parse().unwrap());
        assert_eq!(parse_optimize("  OPTIMIZE  t  WHERE  date  =  \"2026-01-02\"  ").unwrap().unwrap().table, "t");
    }

    #[test]
    fn optimize_rejects_unbounded_and_malformed() {
        // Bare OPTIMIZE (no date) is rejected — would compact all history in-process.
        assert!(parse_optimize("OPTIMIZE otel_logs_and_spans").is_err());
        assert!(parse_optimize("OPTIMIZE").is_err());
        // project_id alone (no date bound), bad date, unknown column.
        assert!(parse_optimize("OPTIMIZE t WHERE project_id = 'x'").is_err());
        assert!(parse_optimize("OPTIMIZE t WHERE date = 'not-a-date'").is_err());
        assert!(parse_optimize("OPTIMIZE t WHERE date = '2026-01-02' AND name = 'x'").is_err());
    }

    /// Tenant-scoped compaction (2026-07-27: whole-date OPTIMIZE OOM'd twice
    /// in-process; one (project, date) partition is the safe unit).
    #[test]
    fn optimize_accepts_project_and_date_in_either_order() {
        let cmd = parse_optimize("OPTIMIZE t WHERE project_id = 'p-1' AND date = '2026-01-02'").unwrap().unwrap();
        assert_eq!(cmd.project_id.as_deref(), Some("p-1"));
        assert_eq!(cmd.date, "2026-01-02".parse().unwrap());
        let cmd = parse_optimize("optimize t where date='2026-01-02' and PROJECT_ID=\"p-1\"").unwrap().unwrap();
        assert_eq!(cmd.project_id.as_deref(), Some("p-1"));
        assert!(parse_optimize("OPTIMIZE t WHERE date = '2026-01-02'").unwrap().unwrap().project_id.is_none());
    }

    #[test]
    fn non_optimize_queries_fall_through() {
        assert_eq!(parse_optimize("SELECT 1"), Ok(None));
        assert_eq!(parse_optimize("INSERT INTO t VALUES (1)"), Ok(None));
        // Don't false-match an identifier that merely starts with "optimize".
        assert_eq!(parse_optimize("SELECT optimizer FROM t"), Ok(None));
        assert_eq!(parse_optimize("optimizer_stats"), Ok(None));
    }

    #[test]
    fn vacuum_parses_table_and_optional_retention() {
        let cmd = parse_vacuum("VACUUM otel_logs_and_spans").unwrap().unwrap();
        assert_eq!(cmd.table, "otel_logs_and_spans");
        assert_eq!(cmd.retention_hours, None);
        // RETAIN clause, case / plural / trailing-semicolon tolerance.
        assert_eq!(parse_vacuum("vacuum t RETAIN 48 HOURS;").unwrap().unwrap().retention_hours, Some(48));
        assert_eq!(parse_vacuum("  VACUUM  t  retain  1  hour  ").unwrap().unwrap().retention_hours, Some(1));
    }

    #[test]
    fn vacuum_rejects_bare_and_malformed() {
        // Bare VACUUM (no table) is rejected — must name the table.
        assert!(parse_vacuum("VACUUM").is_err());
        // Unknown trailing clause, non-numeric retention.
        assert!(parse_vacuum("VACUUM t WHERE date = '2026-01-01'").is_err());
        assert!(parse_vacuum("VACUUM t RETAIN abc HOURS").is_err());
    }

    #[test]
    fn non_vacuum_queries_fall_through() {
        assert_eq!(parse_vacuum("SELECT 1"), Ok(None));
        // Don't false-match an identifier that merely starts with "vacuum".
        assert_eq!(parse_vacuum("SELECT vacuumed FROM t"), Ok(None));
        assert_eq!(parse_vacuum("vacuum_log"), Ok(None));
    }

    #[test]
    fn flush_parses_bare_only() {
        assert!(parse_flush("FLUSH"));
        assert!(parse_flush("  flush ; "));
        // Anything with arguments or a mere prefix match falls through.
        assert!(!parse_flush("FLUSH t"));
        assert!(!parse_flush("SELECT flushed FROM t"));
        assert!(!parse_flush("flush_log"));
    }

    #[test]
    fn delta_history_parses_bounded_read_only_command() {
        let cmd = parse_delta_history("DELTA HISTORY otel_logs_and_spans LIMIT 250;").unwrap().unwrap();
        assert_eq!(cmd.table, "otel_logs_and_spans");
        assert_eq!(cmd.limit, 250);
        assert_eq!(parse_delta_history("delta history t").unwrap().unwrap().limit, 100);
        assert!(parse_delta_history("DELTA HISTORY t LIMIT 0").is_err());
        assert!(parse_delta_history("DELTA HISTORY t LIMIT 10001").is_err());
        assert!(parse_delta_history("DELTA RESTORE t").is_err());
        assert_eq!(parse_delta_history("SELECT delta FROM t"), Ok(None));
    }

    #[test]
    fn delta_actions_requires_one_exact_version() {
        let cmd = parse_delta_actions("DELTA ACTIONS otel_logs_and_spans VERSION 462919;").unwrap().unwrap();
        assert_eq!(cmd.table, "otel_logs_and_spans");
        assert_eq!(cmd.version, 462919);
        assert!(parse_delta_actions("DELTA ACTIONS t").is_err());
        assert!(parse_delta_actions("DELTA ACTIONS t VERSION nope").is_err());
        assert_eq!(parse_delta_actions("SELECT 1"), Ok(None));
    }

    #[test]
    fn delta_recovery_audit_is_explicit_and_version_bounded() {
        let cmd = parse_delta_recovery_audit("DELTA RECOVERY AUDIT otel_logs_and_spans VERSION 462921;").unwrap().unwrap();
        assert_eq!(cmd.table, "otel_logs_and_spans");
        assert_eq!(cmd.version, 462921);
        assert!(parse_delta_recovery_audit("DELTA RECOVERY otel_logs_and_spans VERSION 462921").is_err());
        assert!(parse_delta_recovery_audit("DELTA RECOVERY AUDIT t VERSION nope").is_err());
        assert_eq!(parse_delta_recovery_audit("SELECT 1"), Ok(None));
    }

    #[test]
    fn slow_query_dimensions_are_bounded_and_sql_free() {
        let (tables, project_id) = query_dimensions("SELECT * FROM logs JOIN traces ON true WHERE project_id = 'project-123' AND body = 'secret'");
        assert_eq!(tables, "logs,traces");
        assert_eq!(project_id, "project-123");
        assert!(!tables.contains("secret"));
    }

    #[test]
    fn query_template_redacts_literals_and_has_a_stable_fingerprint() {
        let first = "SELECT * FROM logs WHERE project_id = 'project-123' AND body = 'secret' AND n = 42 -- do not log";
        let second = "select * from logs where project_id = 'project-456' and body = 'other' and n = 7";
        let first_template = query_template(first);
        assert_eq!(first_template, "select * from logs where project_id = ? and body = ? and n = ?");
        assert_eq!(first_template, query_template(second));
        assert_eq!(query_fingerprint(first), query_fingerprint(second));
        assert!(!first_template.contains("secret"));
        assert!(!first_template.contains("project-123"));
    }

    #[test]
    fn abort_rewrites_to_rollback() {
        assert_eq!(rewrite_pg_synonyms("ABORT"), "ROLLBACK");
        assert_eq!(rewrite_pg_synonyms("ABORT;"), "ROLLBACK;");
        assert_eq!(rewrite_pg_synonyms("  abort  "), "ROLLBACK  ");
        assert_eq!(rewrite_pg_synonyms("Abort Work"), "ROLLBACK Work");
        assert_eq!(rewrite_pg_synonyms("ABORT TRANSACTION;"), "ROLLBACK TRANSACTION;");
    }

    #[test]
    fn non_abort_queries_are_borrowed_unchanged() {
        // Cow::Borrowed is the fast path; we just check the content is identical.
        assert_eq!(rewrite_pg_synonyms("SELECT 1"), "SELECT 1");
        assert_eq!(rewrite_pg_synonyms("BEGIN"), "BEGIN");
        assert_eq!(rewrite_pg_synonyms("ROLLBACK"), "ROLLBACK");
        // Don't false-match identifiers/columns that start with ABORT.
        assert_eq!(rewrite_pg_synonyms("SELECT aborted FROM t"), "SELECT aborted FROM t");
        assert_eq!(rewrite_pg_synonyms("ABORTED"), "ABORTED");
    }
}
