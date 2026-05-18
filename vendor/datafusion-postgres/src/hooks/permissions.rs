use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use pgwire::api::results::Response;
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};

use crate::auth::AuthManager;
use crate::hooks::HookClient;
use crate::QueryHook;

use datafusion_pg_catalog::pg_catalog::context::{Permission, ResourceType};

#[derive(Debug)]
pub struct PermissionsHook {
    auth_manager: Arc<AuthManager>,
}

impl PermissionsHook {
    pub fn new(auth_manager: Arc<AuthManager>) -> Self {
        PermissionsHook { auth_manager }
    }

    /// Check if the current user has permission to execute a statement
    async fn check_statement_permission<C>(
        &self,
        client: &C,
        statement: &Statement,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + ?Sized,
    {
        // Get the username from client metadata
        let username = client
            .metadata()
            .get("user")
            .map(|s| s.as_str())
            .unwrap_or("anonymous");

        // Determine required permissions based on Statement type
        let (required_permission, resource) = match statement {
            Statement::Query(_) => (Permission::Select, ResourceType::All),
            Statement::Insert(_) => (Permission::Insert, ResourceType::All),
            Statement::Update { .. } => (Permission::Update, ResourceType::All),
            Statement::Delete(_) => (Permission::Delete, ResourceType::All),
            Statement::CreateTable { .. } | Statement::CreateView { .. } => {
                (Permission::Create, ResourceType::All)
            }
            Statement::Drop { .. } => (Permission::Drop, ResourceType::All),
            Statement::AlterTable { .. } => (Permission::Alter, ResourceType::All),
            // For other statements (SET, SHOW, EXPLAIN, transactions, etc.), allow all users
            _ => return Ok(()),
        };

        // Check permission
        let has_permission = self
            .auth_manager
            .check_permission(username, required_permission, resource)
            .await;

        if !has_permission {
            return Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42501".to_string(), // insufficient_privilege
                    format!("permission denied for user \"{username}\""),
                ),
            )));
        }

        Ok(())
    }

    /// Check if a statement should skip permission checks
    fn should_skip_permission_check(statement: &Statement) -> bool {
        matches!(
            statement,
            Statement::Set { .. }
                | Statement::ShowVariable { .. }
                | Statement::ShowStatus { .. }
                | Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
                | Statement::Savepoint { .. }
                | Statement::ReleaseSavepoint { .. }
        )
    }
}

#[async_trait]
impl QueryHook for PermissionsHook {
    /// called in simple query handler to return response directly
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if Self::should_skip_permission_check(statement) {
            return None;
        }

        // Check permissions for other statements
        if let Err(e) = self.check_statement_permission(&*client, statement).await {
            return Some(Err(e));
        }

        None
    }

    async fn handle_extended_parse_query(
        &self,
        _stmt: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        None
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if Self::should_skip_permission_check(statement) {
            return None;
        }

        // Check permissions for other statements
        if let Err(e) = self.check_statement_permission(&*client, statement).await {
            return Some(Err(e));
        }

        None
    }
}
