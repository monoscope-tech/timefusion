use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion_postgres::pgwire::api::auth::{noop::NoopStartupHandler, StartupHandler};
use datafusion_postgres::pgwire::api::portal::Portal;
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use datafusion_postgres::pgwire::api::stmt::StoredStatement;
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::ClientPortalStore;
use datafusion_postgres::pgwire::api::{ClientInfo, ErrorHandler, PgWireServerHandlers};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use datafusion_postgres::{auth::AuthManager, DfSessionService};
use futures::Sink;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::field::Empty;
use tracing::{info, instrument, Instrument};

/// Custom handler factory that creates handlers which log UPDATE queries
pub struct LoggingHandlerFactory {
    session_context: Arc<SessionContext>,
    auth_manager: Arc<AuthManager>,
}

impl LoggingHandlerFactory {
    pub fn new(session_context: Arc<SessionContext>, auth_manager: Arc<AuthManager>) -> Self {
        Self { session_context, auth_manager }
    }
}

/// Simple startup handler for authentication
pub struct SimpleStartupHandler;

#[async_trait]
impl NoopStartupHandler for SimpleStartupHandler {}

impl PgWireServerHandlers for LoggingHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(LoggingSimpleQueryHandler::new(self.session_context.clone(), self.auth_manager.clone()))
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(LoggingExtendedQueryHandler::new(self.session_context.clone(), self.auth_manager.clone()))
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(SimpleStartupHandler)
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(LoggingErrorHandler)
    }
}

/// Error handler that logs errors
struct LoggingErrorHandler;

impl ErrorHandler for LoggingErrorHandler {
    fn on_error<C>(&self, _client: &C, error: &mut PgWireError)
    where
        C: ClientInfo,
    {
        info!("PgWire error occurred: {}", error);
    }
}

/// Simple query handler that logs UPDATE queries
pub struct LoggingSimpleQueryHandler {
    inner: DfSessionService,
}

impl LoggingSimpleQueryHandler {
    pub fn new(session_context: Arc<SessionContext>, auth_manager: Arc<AuthManager>) -> Self {
        Self {
            inner: DfSessionService::new(session_context, auth_manager),
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for LoggingSimpleQueryHandler {
    #[instrument(
        name = "postgres.query.simple",
        skip_all,
        fields(
            query.text = Empty,
            query.type = Empty,
            query.operation = Empty,
            db.system = "postgresql",
            db.operation = Empty,
        )
    )]
    async fn do_query<'a, C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let span = tracing::Span::current();

        // Determine query type and operation
        let query_lower = query.trim().to_lowercase();
        let (query_type, operation) = if query_lower.starts_with("select") || query_lower.contains(" select ") {
            ("SELECT", "SELECT")
        } else if query_lower.starts_with("update") || query_lower.contains(" update ") {
            ("DML", "UPDATE")
        } else if query_lower.starts_with("delete") || query_lower.contains(" delete ") {
            ("DML", "DELETE")
        } else if query_lower.starts_with("insert") || query_lower.contains(" insert ") {
            ("DML", "INSERT")
        } else if query_lower.starts_with("create") || query_lower.contains(" create ") {
            ("DDL", "CREATE")
        } else if query_lower.starts_with("drop") || query_lower.contains(" drop ") {
            ("DDL", "DROP")
        } else if query_lower.starts_with("alter") || query_lower.contains(" alter ") {
            ("DDL", "ALTER")
        } else {
            ("OTHER", "UNKNOWN")
        };

        span.record("query.type", query_type);
        span.record("query.operation", operation);
        span.record("db.operation", operation);

        // Truncate sensitive data from DML queries
        let sanitized_query = match operation {
            "INSERT" => query_lower.find(" values").map(|i| format!("{} VALUES ...", &query[..i])).unwrap_or_else(|| query.to_string()),
            "UPDATE" => query_lower.find(" set").map(|i| format!("{} SET ...", &query[..i])).unwrap_or_else(|| query.to_string()),
            _ => query.to_string(),
        };
        span.record("query.text", &sanitized_query.as_str());

        // Delegate to inner handler with the span context
        // Use the current span as parent to ensure proper context propagation
        let execute_span = tracing::trace_span!(parent: &span, "datafusion.execute");
        <DfSessionService as SimpleQueryHandler>::do_query(&self.inner, client, query).instrument(execute_span).await
    }
}

/// Extended query handler that logs UPDATE queries
pub struct LoggingExtendedQueryHandler {
    inner: DfSessionService,
}

impl LoggingExtendedQueryHandler {
    pub fn new(session_context: Arc<SessionContext>, auth_manager: Arc<AuthManager>) -> Self {
        Self {
            inner: DfSessionService::new(session_context, auth_manager),
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
        fields(
            query.text = Empty,
            query.type = Empty,
            query.operation = Empty,
            query.portal = %portal.name,
            query.max_rows = max_rows,
            db.system = "postgresql",
            db.operation = Empty,
        )
    )]
    async fn do_query<'a, C>(&self, client: &mut C, portal: &Portal<Self::Statement>, max_rows: usize) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let span = tracing::Span::current();

        // Get query text and determine type
        let query = &portal.statement.statement.0;

        let query_lower = query.trim().to_lowercase();
        let (query_type, operation) = if query_lower.starts_with("select") || query_lower.contains(" select ") {
            ("SELECT", "SELECT")
        } else if query_lower.starts_with("update") || query_lower.contains(" update ") {
            ("DML", "UPDATE")
        } else if query_lower.starts_with("delete") || query_lower.contains(" delete ") {
            ("DML", "DELETE")
        } else if query_lower.starts_with("insert") || query_lower.contains(" insert ") {
            ("DML", "INSERT")
        } else if query_lower.starts_with("create") || query_lower.contains(" create ") {
            ("DDL", "CREATE")
        } else if query_lower.starts_with("drop") || query_lower.contains(" drop ") {
            ("DDL", "DROP")
        } else if query_lower.starts_with("alter") || query_lower.contains(" alter ") {
            ("DDL", "ALTER")
        } else {
            ("OTHER", "UNKNOWN")
        };

        span.record("query.type", query_type);
        span.record("query.operation", operation);
        span.record("db.operation", operation);

        // Truncate sensitive data from DML queries
        let sanitized_query = match operation {
            "INSERT" => query_lower.find(" values").map(|i| format!("{} VALUES ...", &query[..i])).unwrap_or_else(|| query.to_string()),
            "UPDATE" => query_lower.find(" set").map(|i| format!("{} SET ...", &query[..i])).unwrap_or_else(|| query.to_string()),
            _ => query.to_string(),
        };
        span.record("query.text", &sanitized_query.as_str());

        // Delegate to inner handler with the span context
        // Use the current span as parent to ensure proper context propagation
        let execute_span = tracing::trace_span!(parent: &span, "datafusion.execute");
        <DfSessionService as ExtendedQueryHandler>::do_query(&self.inner, client, portal, max_rows)
            .instrument(execute_span)
            .await
    }
}

/// Start the server with custom handlers that log UPDATE queries
pub async fn serve_with_logging(
    session_context: Arc<SessionContext>, options: &datafusion_postgres::ServerOptions, auth_manager: Arc<AuthManager>,
) -> Result<(), Box<dyn std::error::Error>> {
    let handlers = Arc::new(LoggingHandlerFactory::new(session_context, auth_manager));

    // Use datafusion-postgres's serve_with_handlers
    datafusion_postgres::serve_with_handlers(handlers, options).await?;

    Ok(())
}

