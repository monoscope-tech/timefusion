use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion_postgres::{DfSessionService, auth::AuthManager};
use datafusion_postgres::pgwire::api::auth::{StartupHandler, noop::NoopStartupHandler};
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::results::{Response, DescribeStatementResponse, DescribePortalResponse};
use datafusion_postgres::pgwire::api::{ClientInfo, PgWireServerHandlers, ErrorHandler};
use datafusion_postgres::pgwire::api::portal::Portal;
use datafusion_postgres::pgwire::api::stmt::StoredStatement;
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::ClientPortalStore;
use datafusion_postgres::pgwire::error::{PgWireResult, PgWireError};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use futures::Sink;
use std::sync::Arc;
use std::fmt::Debug;
use tracing::info;

/// Custom handler factory that creates handlers which log UPDATE queries
pub struct LoggingHandlerFactory {
    session_context: Arc<SessionContext>,
    auth_manager: Arc<AuthManager>,
}

impl LoggingHandlerFactory {
    pub fn new(session_context: Arc<SessionContext>, auth_manager: Arc<AuthManager>) -> Self {
        Self {
            session_context,
            auth_manager,
        }
    }
}

/// Simple startup handler for authentication
pub struct SimpleStartupHandler;

#[async_trait]
impl NoopStartupHandler for SimpleStartupHandler {}

impl PgWireServerHandlers for LoggingHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(LoggingSimpleQueryHandler::new(
            self.session_context.clone(),
            self.auth_manager.clone(),
        ))
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(LoggingExtendedQueryHandler::new(
            self.session_context.clone(),
            self.auth_manager.clone(),
        ))
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
    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        query: &str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Log UPDATE and DELETE queries
        let query_lower = query.trim().to_lowercase();
        if query_lower.starts_with("update") || query_lower.contains(" update ") {
            info!("UPDATE query executed: {}", query);
        } else if query_lower.starts_with("delete") || query_lower.contains(" delete ") {
            info!("DELETE query executed: {}", query);
        }
        
        // Delegate to inner handler
        <DfSessionService as SimpleQueryHandler>::do_query(&self.inner, client, query).await
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

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.inner.do_describe_statement(client, statement).await
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.inner.do_describe_portal(client, portal).await
    }

    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Log UPDATE and DELETE queries being executed
        // portal.statement is an Arc<StoredStatement>, not Option
        let statement = &portal.statement;
        let query = &statement.statement.0;
        let query_lower = query.trim().to_lowercase();
        if query_lower.starts_with("update") || query_lower.contains(" update ") {
            info!("UPDATE query executed (extended): {}", query);
        } else if query_lower.starts_with("delete") || query_lower.contains(" delete ") {
            info!("DELETE query executed (extended): {}", query);
        }
        
        <DfSessionService as ExtendedQueryHandler>::do_query(&self.inner, client, portal, max_rows).await
    }
}

/// Start the server with custom handlers that log UPDATE queries
pub async fn serve_with_logging(
    session_context: Arc<SessionContext>,
    options: &datafusion_postgres::ServerOptions,
    auth_manager: Arc<AuthManager>,
) -> Result<(), Box<dyn std::error::Error>> {
    let handlers = Arc::new(LoggingHandlerFactory::new(session_context, auth_manager));
    
    // Use datafusion-postgres's serve_with_handlers
    datafusion_postgres::serve_with_handlers(handlers, options).await?;
    
    Ok(())
}