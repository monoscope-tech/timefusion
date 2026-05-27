use pgwire::api::query::SimpleQueryHandler;

use datafusion_postgres::testing::*;

const PGCLI_QUERIES: &[&str] = &[
    "SELECT 1",
    "show time zone",
    "set time zone \"Asia/Shanghai\"",
    "SELECT * FROM unnest(current_schemas(true))",
    "SELECT  nspname
                FROM    pg_catalog.pg_namespace
                ORDER BY 1",
    "SELECT  n.nspname schema_name,
                        c.relname table_name
                FROM    pg_catalog.pg_class c
                        LEFT JOIN pg_catalog.pg_namespace n
                            ON n.oid = c.relnamespace
                WHERE   c.relkind = ANY('{r,p,f}')
                ORDER BY 1,2;",
    "SELECT  nsp.nspname schema_name,
                                cls.relname table_name,
                                att.attname column_name,
                                att.atttypid::regtype::text type_name,
                                att.atthasdef AS has_default,
                                pg_catalog.pg_get_expr(def.adbin, def.adrelid, true) as default
                        FROM    pg_catalog.pg_attribute att
                                INNER JOIN pg_catalog.pg_class cls
                                    ON att.attrelid = cls.oid
                                INNER JOIN pg_catalog.pg_namespace nsp
                                    ON cls.relnamespace = nsp.oid
                                LEFT OUTER JOIN pg_attrdef def
                                    ON def.adrelid = att.attrelid
                                    AND def.adnum = att.attnum
                        WHERE   cls.relkind = ANY('{r,p,f}')
                                AND NOT att.attisdropped
                                AND att.attnum  > 0
                        ORDER BY 1, 2, att.attnum",
    "SELECT s_p.nspname AS parentschema,
                               t_p.relname AS parenttable,
                               unnest((
                                select
                                    array_agg(attname ORDER BY i)
                                from
                                    (select unnest(confkey) as attnum, generate_subscripts(confkey, 1) as i) x
                                    JOIN pg_catalog.pg_attribute c USING(attnum)
                                    WHERE c.attrelid = fk.confrelid
                                )) AS parentcolumn,
                               s_c.nspname AS childschema,
                               t_c.relname AS childtable,
                               unnest((
                                select
                                    array_agg(attname ORDER BY i)
                                from
                                    (select unnest(conkey) as attnum, generate_subscripts(conkey, 1) as i) x
                                    JOIN pg_catalog.pg_attribute c USING(attnum)
                                    WHERE c.attrelid = fk.conrelid
                                )) AS childcolumn
                        FROM pg_catalog.pg_constraint fk
                        JOIN pg_catalog.pg_class      t_p ON t_p.oid = fk.confrelid
                        JOIN pg_catalog.pg_namespace  s_p ON s_p.oid = t_p.relnamespace
                        JOIN pg_catalog.pg_class      t_c ON t_c.oid = fk.conrelid
                        JOIN pg_catalog.pg_namespace  s_c ON s_c.oid = t_c.relnamespace
                        WHERE fk.contype = 'f'",
    "SELECT  n.nspname schema_name,
                        c.relname table_name
                FROM    pg_catalog.pg_class c
                        LEFT JOIN pg_catalog.pg_namespace n
                            ON n.oid = c.relnamespace
                WHERE   c.relkind = ANY('{v,m}')
                ORDER BY 1,2;",
    "SELECT  nsp.nspname schema_name,
                                cls.relname table_name,
                                att.attname column_name,
                                att.atttypid::regtype::text type_name,
                                att.atthasdef AS has_default,
                                pg_catalog.pg_get_expr(def.adbin, def.adrelid, true) as default
                        FROM    pg_catalog.pg_attribute att
                                INNER JOIN pg_catalog.pg_class cls
                                    ON att.attrelid = cls.oid
                                INNER JOIN pg_catalog.pg_namespace nsp
                                    ON cls.relnamespace = nsp.oid
                                LEFT OUTER JOIN pg_attrdef def
                                    ON def.adrelid = att.attrelid
                                    AND def.adnum = att.attnum
                        WHERE   cls.relkind = ANY('{v,m}')
                                AND NOT att.attisdropped
                                AND att.attnum  > 0
                        ORDER BY 1, 2, att.attnum",
    "SELECT n.nspname schema_name,
                                   t.typname type_name
                            FROM   pg_catalog.pg_type t
                                   INNER JOIN pg_catalog.pg_namespace n
                                      ON n.oid = t.typnamespace
                            WHERE ( t.typrelid = 0  -- non-composite types
                                    OR (  -- composite type, but not a table
                                          SELECT c.relkind = 'c'
                                          FROM pg_catalog.pg_class c
                                          WHERE c.oid = t.typrelid
                                        )
                                  )
                                  AND NOT EXISTS( -- ignore array types
                                        SELECT  1
                                        FROM    pg_catalog.pg_type el
                                        WHERE   el.oid = t.typelem AND el.typarray = t.oid
                                      )
                                  AND n.nspname <> 'pg_catalog'
                                  AND n.nspname <> 'information_schema'
                            ORDER BY 1, 2",
    "SELECT d.datname
                FROM pg_catalog.pg_database d
                ORDER BY 1",
    "SELECT n.nspname schema_name,
                                p.proname func_name,
                                p.proargnames,
                                COALESCE(proallargtypes::regtype[], proargtypes::regtype[])::text[],
                                p.proargmodes,
                                prorettype::regtype::text return_type,
                                p.prokind = 'a' is_aggregate,
                                p.prokind = 'w' is_window,
                                p.proretset is_set_returning,
                                d.deptype = 'e' is_extension,
                                pg_get_expr(proargdefaults, 0) AS arg_defaults
                        FROM pg_catalog.pg_proc p
                                INNER JOIN pg_catalog.pg_namespace n
                                    ON n.oid = p.pronamespace
                        LEFT JOIN pg_depend d ON d.objid = p.oid and d.deptype = 'e'
                        WHERE p.prorettype::regtype != 'trigger'::regtype
                        ORDER BY 1, 2",
];

#[tokio::test]
pub async fn test_pgcli_startup_sql() {
    env_logger::init();
    let service = setup_handlers();
    let mut client = MockClient::new();

    for query in PGCLI_QUERIES {
        SimpleQueryHandler::do_query(&service, &mut client, query)
            .await
            .expect(&format!(
                "failed to run sql:\n--------------\n {query}\n--------------\n"
            ));
    }
}
