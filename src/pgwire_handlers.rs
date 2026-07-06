use std::{fmt::Debug, sync::Arc};

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
            results::{DescribePortalResponse, DescribeStatementResponse, Response, Tag},
            stmt::StoredStatement,
            store::PortalStore,
        },
        error::{ErrorInfo, PgWireError, PgWireResult},
        messages::PgWireBackendMessage,
    },
};
use futures::Sink;
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
        Self {
            username: "postgres".into(),
            password: None,
        }
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
            (Some(p), _) if !p.is_empty() => Ok(Self {
                username: core.pgwire_user.clone(),
                password: Some(p.clone()),
            }),
            (_, true) => {
                tracing::warn!(
                    "PGWIRE_PASSWORD unset and TIMEFUSION_ALLOW_INSECURE_AUTH=true — pgwire endpoint accepts any password. Acceptable for local dev ONLY; never in production."
                );
                Ok(Self {
                    username: core.pgwire_user.clone(),
                    password: None,
                })
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
        if username == self.config.username {
            let pw = self.config.password.clone().unwrap_or_default();
            Ok(Password::new(None, pw.into_bytes()))
        } else {
            Err(PgWireError::UserError(Box::new(datafusion_postgres::pgwire::error::ErrorInfo::new(
                "FATAL".into(),
                "28P01".into(),
                format!("password authentication failed for user \"{username}\""),
            ))))
        }
    }
}

/// Custom handler factory that creates handlers with logging and auth
pub struct LoggingHandlerFactory {
    session_context: Arc<SessionContext>,
    auth_config:     AuthConfig,
    plan_cache:      Arc<PlanCacheHook>,
    scan_metrics:    Option<Arc<crate::database::ScanMetrics>>,
    db:              Option<Arc<Database>>,
}

impl LoggingHandlerFactory {
    pub fn new(session_context: Arc<SessionContext>, auth_config: AuthConfig) -> Self {
        let plan_cache = Arc::new(PlanCacheHook::default());
        crate::plan_cache::set_global(plan_cache.clone());
        Self {
            session_context,
            auth_config,
            plan_cache,
            scan_metrics: None,
            db: None,
        }
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
        let mut h = LoggingSimpleQueryHandler::new_with_hooks(self.session_context.clone(), self.hooks());
        if let Some(m) = &self.scan_metrics {
            h = h.with_scan_metrics(m.clone());
        }
        if let Some(db) = &self.db {
            h = h.with_database(db.clone());
        }
        Arc::new(h)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        let mut h = LoggingExtendedQueryHandler::new_with_hooks(self.session_context.clone(), self.hooks());
        if let Some(m) = &self.scan_metrics {
            h = h.with_scan_metrics(m.clone());
        }
        Arc::new(h)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(CleartextPasswordAuthStartupHandler::new(
            ConfigAuthSource::new(self.auth_config.clone()),
            DefaultServerParameterProvider::default(),
        ))
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

/// Simple query handler with tracing
pub struct LoggingSimpleQueryHandler {
    inner:        DfSessionService,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>,
    db:           Option<Arc<Database>>,
}

impl LoggingSimpleQueryHandler {
    pub fn new(session_context: Arc<SessionContext>) -> Self {
        Self {
            inner:        DfSessionService::new(session_context),
            scan_metrics: None,
            db:           None,
        }
    }

    pub fn new_with_hooks(session_context: Arc<SessionContext>, hooks: Vec<Arc<dyn QueryHook>>) -> Self {
        Self {
            inner:        DfSessionService::new_with_hooks(session_context, hooks),
            scan_metrics: None,
            db:           None,
        }
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
        let db = match self.db.as_ref() {
            Some(d) => d,
            None => return Err(admin_err("OPTIMIZE is not available on this server")),
        };
        let table_ref = db
            .get_or_create_unified_table(&cmd.table)
            .await
            .map_err(|e| admin_err(&format!("OPTIMIZE: open table '{}': {e}", cmd.table)))?;
        let (removed, added) = db.compact_date(&table_ref, &cmd.table, cmd.date).await.map_err(|e| admin_err(&e.to_string()))?;
        info!("pgwire OPTIMIZE {} date={}: {removed} removed, {added} added", cmd.table, cmd.date);
        Ok(vec![Response::Execution(Tag::new(&format!("OPTIMIZE {removed} {added}")))])
    }

    /// Execute an intercepted `VACUUM <table> [RETAIN <n> HOURS]`.
    async fn run_vacuum(&self, cmd: VacuumCmd) -> PgWireResult<Vec<Response>> {
        let db = match self.db.as_ref() {
            Some(d) => d,
            None => return Err(admin_err("VACUUM is not available on this server")),
        };
        let deleted = db.vacuum_named(&cmd.table, cmd.retention_hours).await.map_err(|e| admin_err(&format!("VACUUM '{}': {e}", cmd.table)))?;
        info!("pgwire VACUUM {} retention={:?}: {deleted} files deleted", cmd.table, cmd.retention_hours);
        Ok(vec![Response::Execution(Tag::new(&format!("VACUUM {deleted}")))])
    }
}

/// An intercepted `OPTIMIZE <table> WHERE date = 'YYYY-MM-DD'` admin command.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OptimizeCmd {
    pub table: String,
    pub date:  chrono::NaiveDate,
}

fn admin_err(msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new("ERROR".to_string(), "42601".to_string(), msg.to_string())))
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
    let q = query.trim().trim_end_matches(';').trim();
    let lower = q.to_ascii_lowercase();
    let is_optimize = lower == "optimize" || lower.strip_prefix("optimize").is_some_and(|r| r.starts_with(char::is_whitespace));
    if !is_optimize {
        return Ok(None);
    }
    let rest = q[8..].trim(); // "optimize" is 8 ASCII bytes
    let (table, where_part) = rest.split_once(char::is_whitespace).map(|(t, w)| (t.trim(), w.trim())).unwrap_or((rest, ""));
    if table.is_empty() {
        return Err("OPTIMIZE requires a table and date: OPTIMIZE <table> WHERE date = 'YYYY-MM-DD'".to_string());
    }
    if !where_part.to_ascii_lowercase().starts_with("where") {
        return Err(format!(
            "OPTIMIZE {table} needs a date filter: OPTIMIZE {table} WHERE date = 'YYYY-MM-DD' (bare OPTIMIZE is disabled — it would compact all history in-process)"
        ));
    }
    let cond = where_part[5..].trim(); // after WHERE
    if !cond.to_ascii_lowercase().starts_with("date") {
        return Err("OPTIMIZE only supports a single `date` filter".to_string());
    }
    let val = cond[4..]
        .trim()
        .strip_prefix('=')
        .ok_or("expected: date = 'YYYY-MM-DD'")?
        .trim()
        .trim_matches(|c| c == '\'' || c == '"')
        .trim();
    let date = val.parse::<chrono::NaiveDate>().map_err(|_| format!("invalid date '{val}', expected YYYY-MM-DD"))?;
    Ok(Some(OptimizeCmd {
        table: table.to_string(),
        date,
    }))
}

/// An intercepted `VACUUM <table> [RETAIN <n> HOURS]` admin command.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct VacuumCmd {
    pub table:           String,
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
    let q = query.trim().trim_end_matches(';').trim();
    let lower = q.to_ascii_lowercase();
    let is_vacuum = lower == "vacuum" || lower.strip_prefix("vacuum").is_some_and(|r| r.starts_with(char::is_whitespace));
    if !is_vacuum {
        return Ok(None);
    }
    let rest = q[6..].trim(); // "vacuum" is 6 ASCII bytes
    let (table, tail) = rest.split_once(char::is_whitespace).map(|(t, w)| (t.trim(), w.trim())).unwrap_or((rest, ""));
    if table.is_empty() {
        return Err("VACUUM requires a table: VACUUM <table> [RETAIN <n> HOURS] (bare VACUUM is disabled — name the table)".to_string());
    }
    let retention_hours = if tail.is_empty() {
        None
    } else {
        let after = tail
            .to_ascii_lowercase()
            .strip_prefix("retain")
            .ok_or_else(|| format!("VACUUM {table}: expected optional `RETAIN <n> HOURS`, got '{tail}'"))?
            .trim()
            .to_string();
        let num = after.strip_suffix("hours").or_else(|| after.strip_suffix("hour")).unwrap_or(&after).trim();
        Some(num.parse::<u64>().map_err(|_| format!("VACUUM {table}: invalid retention '{after}', expected `RETAIN <n> HOURS`"))?)
    };
    Ok(Some(VacuumCmd {
        table: table.to_string(),
        retention_hours,
    }))
}

/// Rewrites Postgres synonyms that DataFusion's SQL parser doesn't accept.
///
/// `ABORT [ WORK | TRANSACTION ]` is a Postgres alias for `ROLLBACK`. Hasql's
/// connection pool emits `ABORT` defensively on session acquisition to clear
/// any leftover transaction state; without this rewrite, every Hasql client
/// (e.g. monoscope) sees its first statement on each connection fail with
/// `sql parser error: Expected: an SQL statement, found: ABORT`, which then
/// poisons the whole session.
fn rewrite_pg_synonyms(query: &str) -> std::borrow::Cow<'_, str> {
    let stripped = query.trim_start();
    if stripped.len() < 5 {
        return std::borrow::Cow::Borrowed(query);
    }
    let (head, rest) = stripped.split_at(5);
    if !head.eq_ignore_ascii_case("ABORT") {
        return std::borrow::Cow::Borrowed(query);
    }
    if !(rest.is_empty() || rest.starts_with(|c: char| c.is_whitespace() || c == ';')) {
        return std::borrow::Cow::Borrowed(query);
    }
    std::borrow::Cow::Owned(format!("ROLLBACK{}", rest))
}

fn classify_query(query: &str) -> (&'static str, &'static str) {
    let q = query.trim().to_lowercase();
    if q.starts_with("select") || q.contains(" select ") {
        ("SELECT", "SELECT")
    } else if q.starts_with("update") || q.contains(" update ") {
        ("DML", "UPDATE")
    } else if q.starts_with("delete") || q.contains(" delete ") {
        ("DML", "DELETE")
    } else if q.starts_with("insert") || q.contains(" insert ") {
        ("DML", "INSERT")
    } else if q.starts_with("create") || q.contains(" create ") {
        ("DDL", "CREATE")
    } else if q.starts_with("drop") || q.contains(" drop ") {
        ("DDL", "DROP")
    } else if q.starts_with("alter") || q.contains(" alter ") {
        ("DDL", "ALTER")
    } else {
        ("OTHER", "UNKNOWN")
    }
}

fn sanitize_query(query: &str, operation: &str) -> String {
    const MAX_LEN: usize = 120;
    let lower = query.to_lowercase();
    match operation {
        "INSERT" => {
            let table_end = lower.find('(').or_else(|| lower.find("values")).unwrap_or(lower.len());
            let table_part = query[..table_end].trim_end();
            format!("{} (...) VALUES ...", table_part)
        }
        "UPDATE" => lower.find(" set ").map(|i| format!("{} SET ...", &query[..i])).unwrap_or_else(|| query.into()),
        _ => {
            if query.len() > MAX_LEN {
                format!("{}...", &query[..MAX_LEN])
            } else {
                query.into()
            }
        }
    }
}

/// Classify `query` and stamp the standard query/db tracing fields onto `span`.
fn record_query_span(span: &tracing::Span, query: &str) {
    let (query_type, operation) = classify_query(query);
    span.record("query.type", query_type);
    span.record("query.operation", operation);
    span.record("db.operation", operation);
    span.record("query.text", sanitize_query(query, operation).as_str());
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

        // OPTIMIZE <table> WHERE date = '...' — admin compaction, caught before
        // DataFusion (whose parser rejects OPTIMIZE).
        match parse_optimize(query) {
            Ok(Some(cmd)) => return self.run_optimize(cmd).await,
            Ok(None) => {}
            Err(msg) => return Err(admin_err(&msg)),
        }

        // VACUUM <table> [RETAIN <n> HOURS] — admin file reclamation, caught
        // before DataFusion (whose parser rejects VACUUM).
        match parse_vacuum(query) {
            Ok(Some(cmd)) => return self.run_vacuum(cmd).await,
            Ok(None) => {}
            Err(msg) => return Err(admin_err(&msg)),
        }

        let span = tracing::Span::current();
        record_query_span(&span, query);

        let execute_span = tracing::trace_span!(parent: &span, "datafusion.execute");
        let t0 = std::time::Instant::now();
        let result = <DfSessionService as SimpleQueryHandler>::do_query(&self.inner, client, query).instrument(execute_span).await;
        if let Some(m) = &self.scan_metrics {
            m.record_pgwire_query(t0.elapsed().as_micros() as u64);
        }
        result
    }
}

/// Extended query handler with tracing
pub struct LoggingExtendedQueryHandler {
    inner:        DfSessionService,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>,
}

impl LoggingExtendedQueryHandler {
    pub fn with_scan_metrics(mut self, m: Arc<crate::database::ScanMetrics>) -> Self {
        self.scan_metrics = Some(m);
        self
    }
}

impl LoggingExtendedQueryHandler {
    pub fn new(session_context: Arc<SessionContext>) -> Self {
        Self {
            inner:        DfSessionService::new(session_context),
            scan_metrics: None,
        }
    }

    pub fn new_with_hooks(session_context: Arc<SessionContext>, hooks: Vec<Arc<dyn QueryHook>>) -> Self {
        Self {
            inner:        DfSessionService::new_with_hooks(session_context, hooks),
            scan_metrics: None,
        }
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

        let execute_span = tracing::trace_span!(parent: &span, "datafusion.execute");
        let t0 = std::time::Instant::now();
        let result = <DfSessionService as ExtendedQueryHandler>::do_query(&self.inner, client, portal, max_rows)
            .instrument(execute_span)
            .await;
        if let Some(m) = &self.scan_metrics {
            m.record_pgwire_query(t0.elapsed().as_micros() as u64);
        }
        result
    }
}

/// Start the server with custom handlers
pub async fn serve_with_logging(
    session_context: Arc<SessionContext>, options: &datafusion_postgres::ServerOptions, auth_config: AuthConfig,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>, shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut factory = LoggingHandlerFactory::new(session_context, auth_config);
    if let Some(m) = scan_metrics {
        factory = factory.with_scan_metrics(m);
    }
    let handlers = Arc::new(factory);
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
    let mut factory = LoggingHandlerFactory::new(session_context, auth_config);
    if let Some(m) = scan_metrics {
        factory = factory.with_scan_metrics(m);
    }
    if let Some(db) = db {
        factory = factory.with_database(db);
    }
    let handlers = Arc::new(factory);
    datafusion_postgres::serve_with_listener(listener, handlers, options, shutdown).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_optimize, parse_vacuum, rewrite_pg_synonyms};

    #[test]
    fn optimize_parses_table_and_date() {
        let cmd = parse_optimize("OPTIMIZE otel_logs_and_spans WHERE date = '2026-06-19'").unwrap().unwrap();
        assert_eq!(cmd.table, "otel_logs_and_spans");
        assert_eq!(cmd.date, "2026-06-19".parse().unwrap());
        // Case / spacing / quote / trailing-semicolon tolerance.
        assert_eq!(
            parse_optimize("optimize t where DATE='2026-01-02';").unwrap().unwrap().date,
            "2026-01-02".parse().unwrap()
        );
        assert_eq!(parse_optimize("  OPTIMIZE  t  WHERE  date  =  \"2026-01-02\"  ").unwrap().unwrap().table, "t");
    }

    #[test]
    fn optimize_rejects_unbounded_and_malformed() {
        // Bare OPTIMIZE (no date) is rejected — would compact all history in-process.
        assert!(parse_optimize("OPTIMIZE otel_logs_and_spans").is_err());
        assert!(parse_optimize("OPTIMIZE").is_err());
        // Non-date filter, bad date.
        assert!(parse_optimize("OPTIMIZE t WHERE project_id = 'x'").is_err());
        assert!(parse_optimize("OPTIMIZE t WHERE date = 'not-a-date'").is_err());
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
