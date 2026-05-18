use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use pgwire::api::results::{Response, Tag};
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::response::TransactionStatus;

use crate::hooks::HookClient;
use crate::QueryHook;

/// Hook for processing transaction related statements
///
/// Note that this hook doesn't create actual transactions. It just responds
/// with reasonable return values.
#[derive(Debug)]
pub struct TransactionStatementHook;

#[async_trait]
impl QueryHook for TransactionStatementHook {
    /// called in simple query handler to return response directly
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let resp = try_respond_transaction_statements(client, statement)
            .await
            .transpose();

        if let Some(result) = resp {
            return Some(result);
        }

        // Check if we're in a failed transaction and block non-transaction
        // commands
        if client.transaction_status() == TransactionStatus::Error {
            return Some(Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "25P01".to_string(),
                    "current transaction is aborted, commands ignored until end of transaction block".to_string(),
                ),
            ))));
        }

        None
    }

    async fn handle_extended_parse_query(
        &self,
        stmt: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        // We don't generate logical plan for these statements
        if matches!(
            stmt,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
        ) {
            // Return a dummy plan for transaction commands - they'll be handled by transaction handler
            let dummy_schema = datafusion::common::DFSchema::empty();
            return Some(Ok(LogicalPlan::EmptyRelation(
                datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(dummy_schema),
                },
            )));
        }
        None
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        self.handle_simple_query(statement, session_context, client)
            .await
    }
}

async fn try_respond_transaction_statements<C>(
    client: &C,
    stmt: &Statement,
) -> PgWireResult<Option<Response>>
where
    C: ClientInfo + Send + Sync + ?Sized,
{
    match stmt {
        Statement::StartTransaction { .. } => {
            match client.transaction_status() {
                TransactionStatus::Idle => Ok(Some(Response::TransactionStart(Tag::new("BEGIN")))),
                TransactionStatus::Transaction => {
                    // PostgreSQL behavior: ignore nested BEGIN, just return SUCCESS
                    // This matches PostgreSQL's handling of nested transaction blocks
                    log::warn!("BEGIN command ignored: already in transaction block");
                    Ok(Some(Response::Execution(Tag::new("BEGIN"))))
                }
                TransactionStatus::Error => {
                    // Can't start new transaction from failed state
                    Err(PgWireError::UserError(Box::new(
                            pgwire::error::ErrorInfo::new(
                                "ERROR".to_string(),
                                "25P01".to_string(),
                                "current transaction is aborted, commands ignored until end of transaction block".to_string(),
                            ),
                        )))
                }
            }
        }
        Statement::Commit { .. } => match client.transaction_status() {
            TransactionStatus::Idle | TransactionStatus::Transaction => {
                Ok(Some(Response::TransactionEnd(Tag::new("COMMIT"))))
            }
            TransactionStatus::Error => Ok(Some(Response::TransactionEnd(Tag::new("ROLLBACK")))),
        },
        Statement::Rollback { .. } => Ok(Some(Response::TransactionEnd(Tag::new("ROLLBACK")))),
        _ => Ok(None),
    }
}
