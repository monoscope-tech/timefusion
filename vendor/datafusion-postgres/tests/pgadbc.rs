use pgwire::api::query::SimpleQueryHandler;

use datafusion_postgres::testing::*;

const PGADBC_QUERIES: &[&str] = &[
    "SELECT attname, atttypid FROM pg_catalog.pg_class AS cls INNER JOIN pg_catalog.pg_attribute AS attr ON cls.oid = attr.attrelid INNER JOIN pg_catalog.pg_type AS typ ON attr.atttypid = typ.oid WHERE attr.attnum >= 0 AND cls.oid = 'clubs'::regclass::oid ORDER BY attr.attnum",


];

#[tokio::test]
pub async fn test_pgadbc_metadata_sql() {
    env_logger::init();
    let service = setup_handlers();
    let mut client = MockClient::new();

    for query in PGADBC_QUERIES {
        SimpleQueryHandler::do_query(&service, &mut client, query)
            .await
            .unwrap_or_else(|e| {
                panic!("failed to run sql:\n--------------\n {query}\n--------------\n{e}")
            });
    }
}
