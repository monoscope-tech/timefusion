use pgwire::api::query::SimpleQueryHandler;

use datafusion_postgres::testing::*;

// pgAdmin startup queries from issue #178
// https://github.com/datafusion-contrib/datafusion-postgres/issues/178
const PGADMIN_QUERIES: &[&str] = &[
    // Basic version query (fixed by #179)
    "SELECT version()",
    // Query to check for BDR extension and replication slots
    r#"SELECT CASE
        WHEN (SELECT count(extname) FROM pg_catalog.pg_extension WHERE extname='bdr') > 0
        THEN 'pgd'
        WHEN (SELECT COUNT(*) FROM pg_replication_slots) > 0
        THEN 'log'
        ELSE NULL
    END as type"#,
];

#[tokio::test]
pub async fn test_pgadmin_startup_sql() {
    let service = setup_handlers();
    let mut client = MockClient::new();

    for query in PGADMIN_QUERIES {
        SimpleQueryHandler::do_query(&service, &mut client, query)
            .await
            .unwrap_or_else(|e| {
                panic!("failed to run sql:\n--------------\n{query}\n--------------\n{e}")
            });
    }
}
