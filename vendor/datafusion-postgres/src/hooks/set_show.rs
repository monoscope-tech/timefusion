use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{ParamValues, ToDFSchema};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Expr, Set, Statement};
use log::{info, warn};
use pgwire::api::auth::DefaultServerParameterProvider;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::startup::ParameterStatus;
use pgwire::messages::PgWireBackendMessage;
use pgwire::types::format::FormatOptions;
use postgres_types::Type;

use crate::client;
use crate::hooks::HookClient;
use crate::QueryHook;

#[derive(Debug)]
pub struct SetShowHook;

#[async_trait]
impl QueryHook for SetShowHook {
    /// called in simple query handler to return response directly
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::Set { .. } => {
                try_respond_set_statements(client, statement, session_context).await
            }
            Statement::ShowVariable { .. } | Statement::ShowStatus { .. } => {
                try_respond_show_statements(client, statement, session_context).await
            }
            _ => None,
        }
    }

    async fn handle_extended_parse_query(
        &self,
        stmt: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        match stmt {
            Statement::Set { .. } => {
                let show_schema = Arc::new(Schema::new(Vec::<Field>::new()));
                let result = show_schema
                    .to_dfschema()
                    .map(|df_schema| {
                        LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                            produce_one_row: true,
                            schema: Arc::new(df_schema),
                        })
                    })
                    .map_err(|e| PgWireError::ApiError(Box::new(e)));
                Some(result)
            }
            Statement::ShowVariable { .. } | Statement::ShowStatus { .. } => {
                let show_schema =
                    Arc::new(Schema::new(vec![Field::new("show", DataType::Utf8, false)]));
                let result = show_schema
                    .to_dfschema()
                    .map(|df_schema| {
                        LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                            produce_one_row: true,
                            schema: Arc::new(df_schema),
                        })
                    })
                    .map_err(|e| PgWireError::ApiError(Box::new(e)));
                Some(result)
            }
            _ => None,
        }
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::Set { .. } => {
                try_respond_set_statements(client, statement, session_context).await
            }
            Statement::ShowVariable { .. } | Statement::ShowStatus { .. } => {
                try_respond_show_statements(client, statement, session_context).await
            }
            _ => None,
        }
    }
}

fn mock_show_response(name: &str, value: &str) -> PgWireResult<QueryResponse> {
    let fields = vec![FieldInfo::new(
        name.to_string(),
        None,
        None,
        Type::VARCHAR,
        FieldFormat::Text,
    )];

    let row = {
        let mut encoder = DataRowEncoder::new(Arc::new(fields.clone()));
        encoder.encode_field(&Some(value))?;
        Ok(encoder.take_row())
    };

    let row_stream = futures::stream::once(async move { row });
    Ok(QueryResponse::new(Arc::new(fields), Box::pin(row_stream)))
}

async fn try_respond_set_statements(
    client: &mut dyn HookClient,
    statement: &Statement,
    session_context: &SessionContext,
) -> Option<PgWireResult<Response>> {
    let Statement::Set(set_statement) = statement else {
        return None;
    };

    match &set_statement {
        Set::SingleAssignment {
            scope: None,
            hivevar: false,
            variable,
            values,
        } => {
            let var = variable.to_string().to_lowercase();
            if var == "statement_timeout" {
                let value = values[0].to_string();
                let timeout_str = value.trim_matches('"').trim_matches('\'');

                let timeout = if timeout_str == "0" || timeout_str.is_empty() {
                    None
                } else {
                    // Parse timeout value (supports ms, s, min formats)
                    let timeout_ms = if timeout_str.ends_with("ms") {
                        timeout_str.trim_end_matches("ms").parse::<u64>()
                    } else if timeout_str.ends_with("s") {
                        timeout_str
                            .trim_end_matches("s")
                            .parse::<u64>()
                            .map(|s| s * 1000)
                    } else if timeout_str.ends_with("min") {
                        timeout_str
                            .trim_end_matches("min")
                            .parse::<u64>()
                            .map(|m| m * 60 * 1000)
                    } else {
                        // Default to milliseconds
                        timeout_str.parse::<u64>()
                    };

                    match timeout_ms {
                        Ok(ms) if ms > 0 => Some(std::time::Duration::from_millis(ms)),
                        _ => None,
                    }
                };

                client::set_statement_timeout(client, timeout);
                return Some(Ok(Response::Execution(Tag::new("SET"))));
            } else if matches!(
                var.as_str(),
                "datestyle"
                    | "bytea_output"
                    | "intervalstyle"
                    | "application_name"
                    | "extra_float_digits"
                    | "search_path"
            ) && !values.is_empty()
            {
                // postgres configuration variables
                let value = values[0].clone();
                if let Expr::Value(value) = value {
                    let val_str = value.into_string().unwrap_or_else(|| "".to_string());
                    client.metadata_mut().insert(var.clone(), val_str);
                    if let Some((name, value)) = parameter_status_for_var(&var, &*client) {
                        if let Err(e) = client
                            .send_message(PgWireBackendMessage::ParameterStatus(
                                ParameterStatus::new(name, value),
                            ))
                            .await
                        {
                            return Some(Err(e));
                        }
                    }
                    return Some(Ok(Response::Execution(Tag::new("SET"))));
                }
            }
        }
        Set::SetTimeZone {
            local: false,
            value,
        } => {
            let tz = value.to_string();
            let tz = tz.trim_matches('"').trim_matches('\'');
            client::set_timezone(client, Some(tz));
            // execution options for timezone
            session_context
                .state()
                .config_mut()
                .options_mut()
                .execution
                .time_zone = Some(tz.to_string());
            let tz_value = client::get_timezone(client).unwrap_or("UTC").to_string();
            if let Err(e) = client
                .send_message(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                    "TimeZone".to_string(),
                    tz_value,
                )))
                .await
            {
                return Some(Err(e));
            }
            return Some(Ok(Response::Execution(Tag::new("SET"))));
        }
        _ => {}
    }

    // fallback to datafusion and ignore all errors
    if let Err(e) = execute_set_statement(session_context, statement.clone()).await {
        warn!(
            "SET statement {statement} is not supported by datafusion, error {e}, statement ignored",
        );
    }

    // Always return SET success
    Some(Ok(Response::Execution(Tag::new("SET"))))
}

fn parameter_status_for_var(
    var: &str,
    client: &(impl ClientInfo + ?Sized),
) -> Option<(String, String)> {
    let display_name = match var {
        "datestyle" => "DateStyle",
        "intervalstyle" => "IntervalStyle",
        "bytea_output" => "bytea_output",
        "application_name" => "application_name",
        "extra_float_digits" => "extra_float_digits",
        "search_path" => "search_path",
        _ => return None,
    };
    let value = client.metadata().get(var)?.clone();
    Some((display_name.to_string(), value))
}

async fn execute_set_statement(
    session_context: &SessionContext,
    statement: Statement,
) -> Result<(), DataFusionError> {
    let state = session_context.state();
    let logical_plan = state
        .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(
            statement,
        )))
        .await
        .and_then(|logical_plan| state.optimize(&logical_plan))?;

    session_context
        .execute_logical_plan(logical_plan)
        .await
        .map(|_| ())
}

async fn try_respond_show_statements(
    client: &dyn HookClient,
    statement: &Statement,
    session_context: &SessionContext,
) -> Option<PgWireResult<Response>> {
    let Statement::ShowVariable { variable } = statement else {
        return None;
    };

    let variables = variable
        .iter()
        .map(|v| v.value.to_lowercase())
        .collect::<Vec<_>>();
    let variables_ref = variables.iter().map(|s| s.as_str()).collect::<Vec<_>>();

    match variables_ref.as_slice() {
        ["time", "zone"] => {
            let timezone = client::get_timezone(client).unwrap_or("UTC");
            Some(mock_show_response("TimeZone", timezone).map(Response::Query))
        }
        ["server_version"] => {
            let version = format!(
                "datafusion {} on {} {}",
                session_context.state().version(),
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION")
            );
            Some(mock_show_response("server_version", &version).map(Response::Query))
        }
        ["transaction_isolation"] => Some(
            mock_show_response("transaction_isolation", "read uncommitted").map(Response::Query),
        ),
        ["catalogs"] => {
            let catalogs = session_context.catalog_names();
            let value = catalogs.join(", ");
            Some(mock_show_response("Catalogs", &value).map(Response::Query))
        }
        ["statement_timeout"] => {
            let timeout = client::get_statement_timeout(client);
            let timeout_str = match timeout {
                Some(duration) => format!("{}ms", duration.as_millis()),
                None => "0".to_string(),
            };
            Some(mock_show_response("statement_timeout", &timeout_str).map(Response::Query))
        }
        ["transaction", "isolation", "level"] => {
            Some(mock_show_response("transaction_isolation", "read_committed").map(Response::Query))
        }
        _ => {
            let val = client
                .metadata()
                .get(&variables[0])
                .map(|v| v.to_string())
                .or_else(|| match variables[0].as_str() {
                    "bytea_output" => Some(FormatOptions::default().bytea_output),
                    "datestyle" => Some(FormatOptions::default().date_style),
                    "intervalstyle" => Some(FormatOptions::default().interval_style),
                    "extra_float_digits" => {
                        Some(FormatOptions::default().extra_float_digits.to_string())
                    }
                    "application_name" => Some(
                        DefaultServerParameterProvider::default()
                            .application_name
                            .unwrap_or("".to_owned()),
                    ),
                    "search_path" => Some(DefaultServerParameterProvider::default().search_path),
                    _ => None,
                });
            if let Some(val) = val {
                Some(mock_show_response(&variables[0], &val).map(Response::Query))
            } else {
                info!("Unsupported show statement: {statement}");
                Some(mock_show_response("unsupported_show_statement", "").map(Response::Query))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    use super::*;
    use crate::testing::MockClient;

    #[tokio::test]
    async fn test_statement_timeout_set_and_show() {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();

        // Test setting timeout to 5000ms
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set statement_timeout to '5000ms'")
            .unwrap()
            .parse_statement()
            .unwrap();
        let set_response =
            try_respond_set_statements(&mut client, &statement, &session_context).await;

        assert!(set_response.is_some());
        assert!(set_response.unwrap().is_ok());

        // Verify the timeout was set in client metadata
        let timeout = client::get_statement_timeout(&client);
        assert_eq!(timeout, Some(Duration::from_millis(5000)));

        // Test SHOW statement_timeout
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("show statement_timeout")
            .unwrap()
            .parse_statement()
            .unwrap();
        let show_response =
            try_respond_show_statements(&client, &statement, &session_context).await;

        assert!(show_response.is_some());
        assert!(show_response.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_bytea_output_set_and_show() {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();

        // Test setting bytea_output to hex
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set bytea_output = 'hex'")
            .unwrap()
            .parse_statement()
            .unwrap();
        let set_response =
            try_respond_set_statements(&mut client, &statement, &session_context).await;

        assert!(set_response.is_some());
        assert!(set_response.unwrap().is_ok());

        // Verify the value was set in client metadata
        let bytea_output = client.metadata().get("bytea_output").unwrap();
        assert_eq!(bytea_output, "hex");

        // Test SHOW bytea_output
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("show bytea_output")
            .unwrap()
            .parse_statement()
            .unwrap();
        let show_response =
            try_respond_show_statements(&client, &statement, &session_context).await;

        assert!(show_response.is_some());
        assert!(show_response.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_date_style_set_and_show() {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();

        // Test setting dateStyle
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set dateStyle = 'ISO, DMY'")
            .unwrap()
            .parse_statement()
            .unwrap();
        let set_response =
            try_respond_set_statements(&mut client, &statement, &session_context).await;

        assert!(set_response.is_some());
        assert!(set_response.unwrap().is_ok());

        // Verify the value was set in client metadata
        let bytea_output = client.metadata().get("datestyle").unwrap();
        assert_eq!(bytea_output, "ISO, DMY");

        // Test SHOW dateStyle
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("show dateStyle")
            .unwrap()
            .parse_statement()
            .unwrap();
        let show_response =
            try_respond_show_statements(&client, &statement, &session_context).await;

        assert!(show_response.is_some());
        assert!(show_response.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_statement_timeout_disable() {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();

        // Set timeout first
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set statement_timeout to '1000ms'")
            .unwrap()
            .parse_statement()
            .unwrap();
        let resp = try_respond_set_statements(&mut client, &statement, &session_context).await;
        assert!(resp.is_some());
        assert!(resp.unwrap().is_ok());

        // Disable timeout with 0
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set statement_timeout to '0'")
            .unwrap()
            .parse_statement()
            .unwrap();
        let resp = try_respond_set_statements(&mut client, &statement, &session_context).await;
        assert!(resp.is_some());
        assert!(resp.unwrap().is_ok());

        let timeout = client::get_statement_timeout(&client);
        assert_eq!(timeout, None);
    }

    #[tokio::test]
    async fn test_parameter_status_sent_for_all_set_vars() {
        use pgwire::messages::PgWireBackendMessage;

        let test_cases = vec![
            ("set bytea_output = 'escape'", "bytea_output", "escape"),
            (
                "set intervalstyle = 'postgres'",
                "IntervalStyle",
                "postgres",
            ),
            (
                "set application_name = 'myapp'",
                "application_name",
                "myapp",
            ),
            ("set search_path = 'public'", "search_path", "public"),
            ("set extra_float_digits = '2'", "extra_float_digits", "2"),
            ("set datestyle = 'ISO, MDY'", "DateStyle", "ISO, MDY"),
            (
                "set time zone 'America/New_York'",
                "TimeZone",
                "America/New_York",
            ),
        ];

        for (sql, expected_key, expected_value) in test_cases {
            let session_context = SessionContext::new();
            let mut client = MockClient::new();
            let statement = Parser::new(&PostgreSqlDialect {})
                .try_with_sql(sql)
                .unwrap()
                .parse_statement()
                .unwrap();

            let result =
                try_respond_set_statements(&mut client, &statement, &session_context).await;
            assert!(result.is_some(), "Expected Some for {sql}");
            assert!(result.unwrap().is_ok(), "Expected Ok for {sql}");

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
    async fn test_no_parameter_status_for_statement_timeout() {
        use pgwire::messages::PgWireBackendMessage;

        let session_context = SessionContext::new();
        let mut client = MockClient::new();

        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set statement_timeout to '5000ms'")
            .unwrap()
            .parse_statement()
            .unwrap();

        let result = try_respond_set_statements(&mut client, &statement, &session_context).await;
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());

        let has_ps = client
            .sent_messages()
            .iter()
            .any(|m| matches!(m, PgWireBackendMessage::ParameterStatus(_)));

        assert!(!has_ps, "statement_timeout should not send ParameterStatus");
    }

    #[tokio::test]
    async fn test_supported_show_statements_returned_columns() {
        let session_context = SessionContext::new();
        let client = MockClient::new();

        let tests = [
            ("show time zone", "TimeZone"),
            ("show server_version", "server_version"),
            ("show transaction_isolation", "transaction_isolation"),
            ("show catalogs", "Catalogs"),
            ("show search_path", "search_path"),
            ("show statement_timeout", "statement_timeout"),
            ("show transaction isolation level", "transaction_isolation"),
        ];

        for (query, expected_response_col) in tests {
            let statement = Parser::new(&PostgreSqlDialect {})
                .try_with_sql(&query)
                .unwrap()
                .parse_statement()
                .unwrap();
            let show_response =
                try_respond_show_statements(&client, &statement, &session_context).await;

            let Some(Ok(Response::Query(show_response))) = show_response else {
                panic!("unexpected show response");
            };

            assert_eq!(show_response.command_tag(), "SELECT");

            let row_schema = show_response.row_schema();
            assert_eq!(row_schema.len(), 1);
            assert_eq!(row_schema[0].name(), expected_response_col);
        }
    }
}
