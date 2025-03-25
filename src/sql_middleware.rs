// sql_middleware.rs

use sqlparser::{
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use anyhow::{Result, anyhow};
use datafusion_postgres::DfSessionService; // Add this import

/// Splits a multi-statement SQL query into individual statements.
pub fn split_sql_statements(query: &str) -> Result<Vec<String>> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, query)
        .map_err(|e| anyhow!("Failed to parse SQL query: {:?}", e))?;

    let mut result = Vec::new();
    for statement in statements {
        result.push(statement.to_string());
    }
    Ok(result)
}

/// Middleware to handle multi-statement SQL queries by splitting and executing them sequentially.
pub async fn process_multi_statement_query(
    query: &str,
    session_service: &DfSessionService,
) -> Result<Vec<datafusion::prelude::DataFrame>> {
    let statements = split_sql_statements(query)?;

    let mut results = Vec::new();
    for statement in statements {
        tracing::info!("Executing statement: {}", statement);
        let df = session_service
            .sql(&statement)
            .await
            .map_err(|e| anyhow!("Failed to execute statement '{}': {:?}", statement, e))?;
        results.push(df);
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_sql_statements() {
        let query = "SELECT 1; SELECT 2; INSERT INTO telemetry_events (id, projectId, timestamp) VALUES ('1', 'proj1', '2023-01-01T00:00:00Z')";
        let statements = split_sql_statements(query).unwrap();
        assert_eq!(statements.len(), 3);
        assert_eq!(statements[0], "SELECT 1");
        assert_eq!(statements[1], "SELECT 2");
        assert_eq!(statements[2], "INSERT INTO telemetry_events (id, projectId, timestamp) VALUES ('1', 'proj1', '2023-01-01T00:00:00Z')");
    }
}