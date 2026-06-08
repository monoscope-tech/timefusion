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
            results::{DescribePortalResponse, DescribeStatementResponse, Response},
            stmt::StoredStatement,
            store::PortalStore,
        },
        error::{PgWireError, PgWireResult},
        messages::PgWireBackendMessage,
    },
};
use futures::Sink;
use tracing::{Instrument, field::Empty, info, instrument};

use crate::plan_cache::PlanCacheHook;

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
        }
    }

    pub fn with_scan_metrics(mut self, m: Arc<crate::database::ScanMetrics>) -> Self {
        self.scan_metrics = Some(m);
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
        info!("PgWire error occurred: {}", error);
    }
}

/// Simple query handler with tracing
pub struct LoggingSimpleQueryHandler {
    inner:        DfSessionService,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>,
}

impl LoggingSimpleQueryHandler {
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

    pub fn with_scan_metrics(mut self, m: Arc<crate::database::ScanMetrics>) -> Self {
        self.scan_metrics = Some(m);
        self
    }
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
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let rewritten = rewrite_pg_synonyms(query);
        let query = rewritten.as_ref();
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
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>, shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut factory = LoggingHandlerFactory::new(session_context, auth_config);
    if let Some(m) = scan_metrics {
        factory = factory.with_scan_metrics(m);
    }
    let handlers = Arc::new(factory);
    datafusion_postgres::serve_with_listener(listener, handlers, options, shutdown).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::rewrite_pg_synonyms;

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
