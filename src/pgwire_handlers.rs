use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion_postgres::DfSessionService;
use datafusion_postgres::pgwire::api::auth::cleartext::CleartextPasswordAuthStartupHandler;
use datafusion_postgres::pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler};
use datafusion_postgres::pgwire::api::portal::Portal;
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use datafusion_postgres::pgwire::api::stmt::StoredStatement;
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::{ClientInfo, ClientPortalStore, ErrorHandler, PgWireServerHandlers};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use futures::Sink;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::field::Empty;
use tracing::{Instrument, info, instrument};

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
    auth_config: AuthConfig,
}

impl LoggingHandlerFactory {
    pub fn new(session_context: Arc<SessionContext>, auth_config: AuthConfig) -> Self {
        Self { session_context, auth_config }
    }
}

impl PgWireServerHandlers for LoggingHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(LoggingSimpleQueryHandler::new(self.session_context.clone()))
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(LoggingExtendedQueryHandler::new(self.session_context.clone()))
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
    inner: DfSessionService,
}

impl LoggingSimpleQueryHandler {
    pub fn new(session_context: Arc<SessionContext>) -> Self {
        Self {
            inner: DfSessionService::new(session_context),
        }
    }
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
        let span = tracing::Span::current();
        let (query_type, operation) = classify_query(query);
        span.record("query.type", query_type);
        span.record("query.operation", operation);
        span.record("db.operation", operation);
        span.record("query.text", sanitize_query(query, operation).as_str());

        let execute_span = tracing::trace_span!(parent: &span, "datafusion.execute");
        <DfSessionService as SimpleQueryHandler>::do_query(&self.inner, client, query).instrument(execute_span).await
    }
}

/// Extended query handler with tracing
pub struct LoggingExtendedQueryHandler {
    inner: DfSessionService,
}

impl LoggingExtendedQueryHandler {
    pub fn new(session_context: Arc<SessionContext>) -> Self {
        Self {
            inner: DfSessionService::new(session_context),
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
        let (query_type, operation) = classify_query(query);
        span.record("query.type", query_type);
        span.record("query.operation", operation);
        span.record("db.operation", operation);
        span.record("query.text", sanitize_query(query, operation).as_str());

        let execute_span = tracing::trace_span!(parent: &span, "datafusion.execute");
        <DfSessionService as ExtendedQueryHandler>::do_query(&self.inner, client, portal, max_rows)
            .instrument(execute_span)
            .await
    }
}

/// Start the server with custom handlers
pub async fn serve_with_logging(
    session_context: Arc<SessionContext>, options: &datafusion_postgres::ServerOptions, auth_config: AuthConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let handlers = Arc::new(LoggingHandlerFactory::new(session_context, auth_config));
    datafusion_postgres::serve_with_handlers(handlers, options).await?;
    Ok(())
}
