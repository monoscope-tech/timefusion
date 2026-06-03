use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser;
use log::info;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{FieldInfo, Response, Tag};
use pgwire::api::stmt::QueryParser;
use pgwire::api::{ClientInfo, ErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::types::format::FormatOptions;

use crate::hooks::set_show::SetShowHook;
use crate::hooks::transactions::TransactionStatementHook;
use crate::hooks::QueryHook;
use crate::{client, planner};
use arrow_pg::datatypes::df;
use arrow_pg::datatypes::{arrow_schema_to_pg_fields, into_pg_type};
use datafusion_pg_catalog::sql::PostgresCompatibilityParser;

/// Rewrites Postgres command synonyms that DataFusion's SQL parser doesn't
/// recognize. Applied to every incoming SQL string before parsing — covers
/// both the simple-query and extended-query (parse) paths.
///
/// Currently handles:
/// - `ABORT [ WORK | TRANSACTION ]` → `ROLLBACK [ WORK | TRANSACTION ]`.
///   Postgres treats these as synonyms; Hasql's connection pool emits
///   `ABORT` defensively on session acquisition, which would otherwise
///   produce `sql parser error: Expected: an SQL statement, found: ABORT`
///   and poison the session.
///
/// Returns `Cow::Borrowed` on the no-rewrite fast path so the common case
/// pays only a short case-insensitive prefix check.
fn rewrite_postgres_synonyms(sql: &str) -> std::borrow::Cow<'_, str> {
    let stripped = sql.trim_start();
    if stripped.len() < 5 {
        return std::borrow::Cow::Borrowed(sql);
    }
    let (head, rest) = stripped.split_at(5);
    if !head.eq_ignore_ascii_case("ABORT") {
        return std::borrow::Cow::Borrowed(sql);
    }
    // Only treat as the command form when ABORT stands alone (not when it's
    // a prefix of an identifier like `aborted`).
    if !(rest.is_empty() || rest.starts_with(|c: char| c.is_whitespace() || c == ';')) {
        return std::borrow::Cow::Borrowed(sql);
    }
    std::borrow::Cow::Owned(format!("ROLLBACK{}", rest))
}

#[cfg(test)]
mod synonym_tests {
    use super::rewrite_postgres_synonyms as r;

    #[test]
    fn rewrites_abort_forms() {
        assert_eq!(r("ABORT"), "ROLLBACK");
        assert_eq!(r("ABORT;"), "ROLLBACK;");
        assert_eq!(r("  abort  "), "ROLLBACK  ");
        assert_eq!(r("Abort Work"), "ROLLBACK Work");
        assert_eq!(r("ABORT TRANSACTION;"), "ROLLBACK TRANSACTION;");
    }

    #[test]
    fn leaves_non_abort_alone() {
        assert_eq!(r("SELECT 1"), "SELECT 1");
        assert_eq!(r("BEGIN"), "BEGIN");
        assert_eq!(r("ROLLBACK"), "ROLLBACK");
        assert_eq!(r("SELECT aborted FROM t"), "SELECT aborted FROM t");
        assert_eq!(r("ABORTED"), "ABORTED");
    }
}

/// Simple startup handler that does no authentication
pub struct SimpleStartupHandler;

#[async_trait::async_trait]
impl NoopStartupHandler for SimpleStartupHandler {}

pub struct HandlerFactory {
    pub session_service: Arc<DfSessionService>,
}

impl HandlerFactory {
    pub fn new(session_context: Arc<SessionContext>) -> Self {
        let session_service = Arc::new(DfSessionService::new(session_context));
        HandlerFactory { session_service }
    }

    pub fn new_with_hooks(
        session_context: Arc<SessionContext>,
        query_hooks: Vec<Arc<dyn QueryHook>>,
    ) -> Self {
        let session_service = Arc::new(DfSessionService::new_with_hooks(
            session_context,
            query_hooks,
        ));
        HandlerFactory { session_service }
    }
}

impl PgWireServerHandlers for HandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.session_service.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.session_service.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(SimpleStartupHandler)
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
        info!("Sending error: {error}")
    }
}

/// The pgwire handler backed by a datafusion `SessionContext`
pub struct DfSessionService {
    session_context: Arc<SessionContext>,
    parser: Arc<Parser>,
    query_hooks: Vec<Arc<dyn QueryHook>>,
}

impl DfSessionService {
    pub fn new(session_context: Arc<SessionContext>) -> DfSessionService {
        let hooks: Vec<Arc<dyn QueryHook>> =
            vec![Arc::new(SetShowHook), Arc::new(TransactionStatementHook)];
        Self::new_with_hooks(session_context, hooks)
    }

    pub fn new_with_hooks(
        session_context: Arc<SessionContext>,
        query_hooks: Vec<Arc<dyn QueryHook>>,
    ) -> DfSessionService {
        let parser = Arc::new(Parser {
            session_context: session_context.clone(),
            sql_parser: PostgresCompatibilityParser::new(),
            query_hooks: query_hooks.clone(),
        });
        DfSessionService {
            session_context,
            parser,
            query_hooks,
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for DfSessionService {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + futures::Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<PgWireBackendMessage>>::Error>,
    {
        log::debug!("Received query: {query}");
        let rewritten = rewrite_postgres_synonyms(query);
        let query = rewritten.as_ref();
        let statements = self
            .parser
            .sql_parser
            .parse(query)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        // empty query
        if statements.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        let mut results = vec![];
        'stmt: for statement in statements {
            // Call query hooks with the parsed statement
            for hook in &self.query_hooks {
                if let Some(result) = hook
                    .handle_simple_query(&statement, &self.session_context, client)
                    .await
                {
                    results.push(result?);
                    continue 'stmt;
                }
            }

            let df_result = {
                let query = statement.to_string();

                let timeout = client::get_statement_timeout(client);
                if let Some(timeout_duration) = timeout {
                    tokio::time::timeout(timeout_duration, self.session_context.sql(&query))
                        .await
                        .map_err(|_| {
                            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                                "ERROR".to_string(),
                                "57014".to_string(), // query_canceled error code
                                "canceling statement due to statement timeout".to_string(),
                            )))
                        })?
                } else {
                    self.session_context.sql(&query).await
                }
            };

            // Handle query execution errors and transaction state
            let df = match df_result {
                Ok(df) => df,
                Err(e) => {
                    return Err(PgWireError::ApiError(Box::new(e)));
                }
            };

            if let Some(resp) = dml_completion(&df).await? {
                results.push(resp);
            } else {
                let format_options =
                    Arc::new(FormatOptions::from_client_metadata(client.metadata()));
                results.push(Response::Query(
                    df::encode_dataframe(df, &Format::UnifiedText, Some(format_options)).await?,
                ));
            }
        }
        Ok(results)
    }
}

#[async_trait]
impl ExtendedQueryHandler for DfSessionService {
    type Statement = (String, Option<(sqlparser::ast::Statement, LogicalPlan)>);
    type QueryParser = Parser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + futures::Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<PgWireBackendMessage>>::Error>,
    {
        let query = &portal.statement.statement.0;
        log::debug!("Received execute extended query: {query}");
        // Check query hooks first
        if !self.query_hooks.is_empty() {
            if let (_, Some((statement, plan))) = &portal.statement.statement {
                // TODO: in the case where query hooks all return None, we do the param handling again later.
                let param_types = planner::get_inferred_parameter_types(plan)
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

                let param_values: ParamValues =
                    df::deserialize_parameters(portal, &ordered_param_types(&param_types))?;

                for hook in &self.query_hooks {
                    if let Some(result) = hook
                        .handle_extended_query(
                            statement,
                            plan,
                            &param_values,
                            &self.session_context,
                            client,
                        )
                        .await
                    {
                        return result;
                    }
                }
            }
        }

        if let (_, Some((_statement, plan))) = &portal.statement.statement {
            let param_types = planner::get_inferred_parameter_types(plan)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let param_values =
                df::deserialize_parameters(portal, &ordered_param_types(&param_types))?;

            let plan = plan
                .clone()
                .replace_params_with_values(&param_values)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let optimised = self
                .session_context
                .state()
                .optimize(&plan)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let dataframe = {
                let timeout = client::get_statement_timeout(client);
                if let Some(timeout_duration) = timeout {
                    tokio::time::timeout(
                        timeout_duration,
                        self.session_context.execute_logical_plan(optimised),
                    )
                    .await
                    .map_err(|_| {
                        PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            "57014".to_string(), // query_canceled error code
                            "canceling statement due to statement timeout".to_string(),
                        )))
                    })?
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?
                } else {
                    self.session_context
                        .execute_logical_plan(optimised)
                        .await
                        .map_err(|e| PgWireError::ApiError(Box::new(e)))?
                }
            };

            if let Some(resp) = dml_completion(&dataframe).await? {
                Ok(resp)
            } else {
                let format_options =
                    Arc::new(FormatOptions::from_client_metadata(client.metadata()));
                Ok(Response::Query(
                    df::encode_dataframe(
                        dataframe,
                        &portal.result_column_format,
                        Some(format_options),
                    )
                    .await?,
                ))
            }
        } else {
            Ok(Response::EmptyQuery)
        }
    }
}

/// If `df` runs a DML/COPY plan, execute it and return a `CommandComplete`
/// response with the right tag; otherwise return `None` so the caller falls
/// back to the regular `Response::Query` path. Driving this off
/// `LogicalPlan` (not the parsed AST) keeps the simple- and extended-query
/// paths consistent with what DataFusion actually runs — statement-level
/// rewrites can leave the AST in a non-Insert variant for what's really a write.
async fn dml_completion(df: &DataFrame) -> PgWireResult<Option<Response>> {
    use datafusion::arrow::array::UInt64Array;
    use datafusion::logical_expr::dml::WriteOp;
    let tag = match df.logical_plan() {
        LogicalPlan::Dml(d) => match d.op {
            WriteOp::Insert(_) => Tag::new("INSERT").with_oid(0),
            WriteOp::Update => Tag::new("UPDATE"),
            WriteOp::Delete => Tag::new("DELETE"),
            WriteOp::Ctas => Tag::new("SELECT"),
            WriteOp::Truncate => Tag::new("TRUNCATE"),
        },
        LogicalPlan::Copy(_) => Tag::new("COPY"),
        _ => return Ok(None),
    };
    let batches = df
        .clone()
        .collect()
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    let rows = batches
        .first()
        .and_then(|b| b.column_by_name("count"))
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .map_or(0, |a| a.value(0) as usize);
    Ok(Some(Response::Execution(tag.with_rows(rows))))
}

pub struct Parser {
    session_context: Arc<SessionContext>,
    sql_parser: PostgresCompatibilityParser,
    query_hooks: Vec<Arc<dyn QueryHook>>,
}

#[async_trait]
impl QueryParser for Parser {
    type Statement = (String, Option<(sqlparser::ast::Statement, LogicalPlan)>);

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        log::debug!("Received parse extended query: {sql}");
        let rewritten = rewrite_postgres_synonyms(sql);
        let sql = rewritten.as_ref();
        let mut statements = self
            .sql_parser
            .parse(sql)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if statements.is_empty() {
            return Ok((sql.to_string(), None));
        }

        let statement = statements.remove(0);
        let query = statement.to_string();

        let context = &self.session_context;
        let state = context.state();

        for hook in &self.query_hooks {
            if let Some(logical_plan) = hook
                .handle_extended_parse_query(&statement, context, client)
                .await
            {
                return Ok((query, Some((statement, logical_plan?))));
            }
        }

        let logical_plan = state
            .statement_to_plan(Statement::Statement(Box::new(statement.clone())))
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        Ok((query, Some((statement, logical_plan))))
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        if let (_, Some((_, plan))) = stmt {
            let params = planner::get_inferred_parameter_types(plan)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let mut param_types = Vec::with_capacity(params.len());
            for param_type in ordered_param_types(&params).iter() {
                if let Some(datatype) = param_type {
                    let pgtype = into_pg_type(datatype)?;
                    param_types.push(pgtype);
                } else {
                    param_types.push(Type::UNKNOWN);
                }
            }

            Ok(param_types)
        } else {
            Ok(vec![])
        }
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        if let (_, Some((_, plan))) = stmt {
            let schema = plan.schema();
            let fields = arrow_schema_to_pg_fields(
                schema.as_arrow(),
                column_format.unwrap_or(&Format::UnifiedBinary),
                None,
            )?;

            Ok(fields)
        } else {
            Ok(vec![])
        }
    }
}

fn ordered_param_types(types: &HashMap<String, Option<DataType>>) -> Vec<Option<&DataType>> {
    // Datafusion stores the parameters as a map.  In our case, the keys will be
    // `$1`, `$2` etc.  The values will be the parameter types.
    //
    // PATCH (timefusion): original implementation sorted lexicographically
    // (`a.0.cmp(b.0)`), which puts `$10` before `$2` and breaks every
    // INSERT/SELECT with more than 9 placeholders — the ParameterDescription
    // returned to the client has the wrong positional order, so e.g. a uuid
    // gets typed as TIMESTAMPTZ. Sort by the numeric suffix instead.
    let mut entries: Vec<_> = types.iter().collect();
    entries.sort_by_key(|(k, _)| k.trim_start_matches('$').parse::<u32>().unwrap_or(u32::MAX));
    entries.into_iter().map(|pt| pt.1.as_ref()).collect()
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::testing::MockClient;

    use crate::hooks::HookClient;

    struct TestHook;

    #[async_trait]
    impl QueryHook for TestHook {
        async fn handle_simple_query(
            &self,
            statement: &sqlparser::ast::Statement,
            _ctx: &SessionContext,
            _client: &mut dyn HookClient,
        ) -> Option<PgWireResult<Response>> {
            if statement.to_string().contains("magic") {
                Some(Ok(Response::EmptyQuery))
            } else {
                None
            }
        }

        async fn handle_extended_parse_query(
            &self,
            _statement: &sqlparser::ast::Statement,
            _session_context: &SessionContext,
            _client: &(dyn ClientInfo + Send + Sync),
        ) -> Option<PgWireResult<LogicalPlan>> {
            None
        }

        async fn handle_extended_query(
            &self,
            _statement: &sqlparser::ast::Statement,
            _logical_plan: &LogicalPlan,
            _params: &ParamValues,
            _session_context: &SessionContext,
            _client: &mut dyn HookClient,
        ) -> Option<PgWireResult<Response>> {
            None
        }
    }

    #[tokio::test]
    async fn test_query_hooks() {
        let hook = TestHook;
        let ctx = SessionContext::new();
        let mut client = MockClient::new();

        // Parse a statement that contains "magic"
        let parser = PostgresCompatibilityParser::new();
        let statements = parser.parse("SELECT magic").unwrap();
        let stmt = &statements[0];

        // Hook should intercept
        let result = hook.handle_simple_query(stmt, &ctx, &mut client).await;
        assert!(result.is_some());

        // Parse a normal statement
        let statements = parser.parse("SELECT 1").unwrap();
        let stmt = &statements[0];

        // Hook should not intercept
        let result = hook.handle_simple_query(stmt, &ctx, &mut client).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_multiple_statements_with_hook_continue() {
        // Bug #227: when a hook returned a result, the code used `break 'stmt`
        // which would exit the entire statement loop, preventing subsequent statements
        // from being processed.
        let session_context = Arc::new(SessionContext::new());

        let hooks: Vec<Arc<dyn QueryHook>> = vec![Arc::new(TestHook)];
        let service = DfSessionService::new_with_hooks(session_context, hooks);

        let mut client = MockClient::new();

        // Mix of queries with hooks and those without
        let query = "SELECT magic; SELECT 1; SELECT magic; SELECT 1";

        let results =
            <DfSessionService as SimpleQueryHandler>::do_query(&service, &mut client, query)
                .await
                .unwrap();

        assert_eq!(results.len(), 4, "Expected 4 responses");

        assert!(matches!(results[0], Response::EmptyQuery));
        assert!(matches!(results[1], Response::Query(_)));
        assert!(matches!(results[2], Response::EmptyQuery));
        assert!(matches!(results[3], Response::Query(_)));
    }

    #[tokio::test]
    async fn test_set_sends_parameter_status_via_sink() {
        use pgwire::messages::PgWireBackendMessage;

        let service = crate::testing::setup_handlers();
        let mut client = MockClient::new();

        let test_cases = vec![
            ("SET datestyle = 'ISO, MDY'", "DateStyle", "ISO, MDY"),
            (
                "SET intervalstyle = 'postgres'",
                "IntervalStyle",
                "postgres",
            ),
            ("SET bytea_output = 'hex'", "bytea_output", "hex"),
            (
                "SET application_name = 'myapp'",
                "application_name",
                "myapp",
            ),
            ("SET search_path = 'public'", "search_path", "public"),
            ("SET extra_float_digits = '2'", "extra_float_digits", "2"),
            (
                "SET TIME ZONE 'America/New_York'",
                "TimeZone",
                "America/New_York",
            ),
        ];

        for (sql, expected_key, expected_value) in test_cases {
            client.sent_messages.clear();

            let responses =
                <DfSessionService as SimpleQueryHandler>::do_query(&service, &mut client, sql)
                    .await
                    .unwrap();

            assert!(
                matches!(responses[0], Response::Execution(_)),
                "Expected SET tag for {sql}"
            );

            let ps_msgs: Vec<_> = client
                .sent_messages()
                .iter()
                .filter_map(|m| match m {
                    PgWireBackendMessage::ParameterStatus(ps) => Some(ps),
                    _ => None,
                })
                .collect();

            assert_eq!(ps_msgs.len(), 1, "Expected 1 ParameterStatus for {sql}");
            assert_eq!(ps_msgs[0].name, expected_key, "Wrong key for {sql}");
            assert_eq!(ps_msgs[0].value, expected_value, "Wrong value for {sql}");
        }
    }

    #[tokio::test]
    async fn test_set_statement_timeout_no_parameter_status() {
        use pgwire::messages::PgWireBackendMessage;

        let service = crate::testing::setup_handlers();
        let mut client = MockClient::new();

        <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "SET statement_timeout TO '5000ms'",
        )
        .await
        .unwrap();

        let has_ps = client
            .sent_messages()
            .iter()
            .any(|m| matches!(m, PgWireBackendMessage::ParameterStatus(_)));

        assert!(!has_ps, "statement_timeout should not send ParameterStatus");
    }

    /// DML SQL exercised by both wire-path tests below. Sharing the list
    /// keeps the simple- and extended-query coverage in lockstep.
    const DML_CASES: &[&str] = &[
        "INSERT INTO t VALUES (1, 'a')",
        "UPDATE t SET name = 'x' WHERE id = 1",
        "DELETE FROM t WHERE id = 1",
    ];

    /// DML must emit `Response::Execution` (→ `CommandComplete`), not
    /// `Response::Query` (→ `TuplesOk`); clients decoding writes as
    /// row-count-only treat the latter as a hard error and drop the row.
    #[tokio::test]
    async fn dml_returns_command_complete() {
        let service = crate::testing::setup_handlers();
        let mut client = MockClient::new();

        <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "CREATE TABLE t (id INT, name TEXT)",
        )
        .await
        .unwrap();

        for sql in DML_CASES {
            let resp =
                <DfSessionService as SimpleQueryHandler>::do_query(&service, &mut client, sql)
                    .await
                    .unwrap_or_else(|e| panic!("{sql} failed: {e:?}"));
            assert!(
                matches!(resp.as_slice(), [Response::Execution(_)]),
                "{sql} must return Execution (CommandComplete), got {resp:?}"
            );
        }
    }

    /// Extended path optimises the plan before execution; confirm
    /// `LogicalPlan::Dml` survives optimisation so `dml_completion` still
    /// fires (otherwise the AST/plan desync silently returns).
    #[tokio::test]
    async fn dml_completion_survives_logical_optimisation() {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE t (id INT, name TEXT)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        for sql in DML_CASES {
            let df = ctx.sql(sql).await.unwrap();
            let optimised = ctx.state().optimize(df.logical_plan()).unwrap();
            let optimised_df = ctx.execute_logical_plan(optimised).await.unwrap();
            assert!(
                matches!(dml_completion(&optimised_df).await.unwrap(), Some(Response::Execution(_))),
                "{sql}: optimised plan should still be detected as DML"
            );
        }
    }
}
