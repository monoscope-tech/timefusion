use pgwire::api::query::SimpleQueryHandler;

use datafusion_postgres::testing::*;

const GRAFANA_QUERIES: &[&str] = &[
    r#"SELECT
        CASE WHEN
              quote_ident(table_schema) IN (
              SELECT
                CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
              FROM
                generate_series(
                  array_lower(string_to_array(current_setting('search_path'),','),1),
                  array_upper(string_to_array(current_setting('search_path'),','),1)
                ) as i,
                string_to_array(current_setting('search_path'),',') s
              )
          THEN quote_ident(table_name)
          ELSE quote_ident(table_schema) || '.' || quote_ident(table_name)
        END AS "table"
        FROM information_schema.tables
        WHERE quote_ident(table_schema) NOT IN ('information_schema',
                                 'pg_catalog',
                                 '_timescaledb_cache',
                                 '_timescaledb_catalog',
                                 '_timescaledb_internal',
                                 '_timescaledb_config',
                                 'timescaledb_information',
                                 'timescaledb_experimental')
        ORDER BY CASE WHEN
              quote_ident(table_schema) IN (
              SELECT
                CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
              FROM
                generate_series(
                  array_lower(string_to_array(current_setting('search_path'),','),1),
                  array_upper(string_to_array(current_setting('search_path'),','),1)
                ) as i,
                string_to_array(current_setting('search_path'),',') s
              ) THEN 0 ELSE 1 END, 1"#,
    r#"SELECT quote_ident(column_name) AS "column", data_type AS "type"
        FROM information_schema.columns
        WHERE
          CASE WHEN array_length(parse_ident('public.games'),1) = 2
            THEN quote_ident(table_schema) = (parse_ident('public.games'))[1]
              AND quote_ident(table_name) = (parse_ident('public.games'))[2]
            ELSE quote_ident(table_name) = 'public.games'
              AND
              quote_ident(table_schema) IN (
              SELECT
                CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
              FROM
                generate_series(
                  array_lower(string_to_array(current_setting('search_path'),','),1),
                  array_upper(string_to_array(current_setting('search_path'),','),1)
                ) as i,
                string_to_array(current_setting('search_path'),',') s
              )
          END"#,
];

#[tokio::test]
pub async fn test_grafana_sql() {
    env_logger::init();
    let service = setup_handlers();
    let mut client = MockClient::new();

    for query in GRAFANA_QUERIES {
        SimpleQueryHandler::do_query(&service, &mut client, query)
            .await
            .unwrap_or_else(|e| panic!("failed to run sql: {query}\n{e}"));
    }
}
