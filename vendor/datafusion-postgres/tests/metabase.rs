use pgwire::api::query::SimpleQueryHandler;

use datafusion_postgres::testing::*;

const METABASE_QUERIES: &[&str] = &[
    "SET extra_float_digits = 2",
    "SET application_name = 'Metabase v0.55.1 [f8f63fdf-d8f8-4573-86ea-4fe4a9548041]'",
    "SHOW TRANSACTION ISOLATION LEVEL",
    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
    r#"SELECT nspname AS "TABLE_SCHEM", current_database() AS "TABLE_CATALOG" FROM pg_catalog.pg_namespace  WHERE nspname <> 'pg_toast' AND (nspname !~ '^pg_temp_'  OR nspname = (pg_catalog.current_schemas(true))[1]) AND (nspname !~ '^pg_toast_temp_'  OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_'))  ORDER BY "TABLE_SCHEM""#,
    r#"with table_privileges as (
         select
           NULL as role,
           t.schemaname as schema,
           t.objectname as table,
           pg_catalog.has_any_column_privilege(current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'update') as update,
           pg_catalog.has_any_column_privilege(current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'select') as select,
           pg_catalog.has_any_column_privilege(current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'insert') as insert,
           pg_catalog.has_table_privilege(     current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'delete') as delete
         from (
           select schemaname, tablename as objectname from pg_catalog.pg_tables
           union
           select schemaname, viewname as objectname from pg_catalog.pg_views
           union
           select schemaname, matviewname as objectname from pg_catalog.pg_matviews
         ) t
         where t.schemaname !~ '^pg_'
           and t.schemaname <> 'information_schema'
           and pg_catalog.has_schema_privilege(current_user, t.schemaname, 'usage')
        )
        select t.*
        from table_privileges t"#,
    r#"SELECT "n"."nspname" AS "schema", "c"."relname" AS "name", CASE "c"."relkind" WHEN 'r' THEN 'TABLE' WHEN 'p' THEN 'PARTITIONED TABLE' WHEN 'v' THEN 'VIEW' WHEN 'f' THEN 'FOREIGN TABLE' WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE NULL END AS "type", "d"."description" AS "description", "stat"."n_live_tup" AS "estimated_row_count" FROM "pg_catalog"."pg_class" AS "c" INNER JOIN "pg_catalog"."pg_namespace" AS "n" ON "c"."relnamespace" = "n"."oid" LEFT JOIN "pg_catalog"."pg_description" AS "d" ON ("c"."oid" = "d"."objoid") AND ("d"."objsubid" = '0') AND ("d"."classoid" = 'pg_class'::regclass) LEFT JOIN "pg_stat_user_tables" AS "stat" ON ("n"."nspname" = "stat"."schemaname") AND ("c"."relname" = "stat"."relname") WHERE ("c"."relnamespace" = "n"."oid") AND ("n"."nspname" !~ '^pg_') AND ("n"."nspname" <> 'information_schema') AND c.relkind in ('r', 'p', 'v', 'f', 'm') AND ("n"."nspname" IN ('public')) ORDER BY "type" ASC, "schema" ASC, "name" ASC"#,
    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED",
    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
    "show timezone",
];

#[tokio::test]
pub async fn test_metabase_startup_sql() {
    env_logger::init();
    let service = setup_handlers();
    let mut client = MockClient::new();

    for query in METABASE_QUERIES {
        SimpleQueryHandler::do_query(&service, &mut client, query)
            .await
            .expect(&format!(
                "failed to run sql: \n--------------\n {query}\n--------------\n"
            ));
    }
}
