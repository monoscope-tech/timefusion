use pgwire::api::query::SimpleQueryHandler;

use datafusion_postgres::testing::*;

const PSQL_QUERIES: &[&str] = &[
    "SELECT c.oid,
          n.nspname,
          c.relname
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname OPERATOR(pg_catalog.~) '^(tt)$' COLLATE pg_catalog.default
          AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY 2, 3;",
    "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname
        FROM pg_catalog.pg_class c
         LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
        LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
        WHERE c.oid = '16384';",
    // the query contains all necessary information of columns
    "SELECT a.attname,
          pg_catalog.format_type(a.atttypid, a.atttypmod),
          (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
           FROM pg_catalog.pg_attrdef d
           WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
          a.attnotnull,
          (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
           WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
          a.attidentity,
          a.attgenerated
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = '16384' AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum;",
    // the following queries should return empty results at least for now
    "SELECT pol.polname, pol.polpermissive,
          CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END,
          pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
          pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),
          CASE pol.polcmd
            WHEN 'r' THEN 'SELECT'
            WHEN 'a' THEN 'INSERT'
            WHEN 'w' THEN 'UPDATE'
            WHEN 'd' THEN 'DELETE'
            END AS cmd
        FROM pg_catalog.pg_policy pol
        WHERE pol.polrelid = '16384' ORDER BY 1;",

    "SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS nsp, stxname,
        pg_catalog.pg_get_statisticsobjdef_columns(oid) AS columns,
          'd' = any(stxkind) AS ndist_enabled,
          'f' = any(stxkind) AS deps_enabled,
          'm' = any(stxkind) AS mcv_enabled,
        stxstattarget
        FROM pg_catalog.pg_statistic_ext
        WHERE stxrelid = '16384'
        ORDER BY nsp, stxname;",

    "SELECT pubname
             , NULL
             , NULL
        FROM pg_catalog.pg_publication p
             JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid
             JOIN pg_catalog.pg_class pc ON pc.relnamespace = pn.pnnspid
        WHERE pc.oid ='16384' and pg_catalog.pg_relation_is_publishable('16384')
        UNION
        SELECT pubname
             , pg_get_expr(pr.prqual, c.oid)
             , (CASE WHEN pr.prattrs IS NOT NULL THEN
                 (SELECT string_agg(attname, ', ')
                   FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s,
                        pg_catalog.pg_attribute
                  WHERE attrelid = pr.prrelid AND attnum = prattrs[s])
                ELSE NULL END) FROM pg_catalog.pg_publication p
             JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid
             JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
        WHERE pr.prrelid = '16384'
        UNION
        SELECT pubname
             , NULL
             , NULL
        FROM pg_catalog.pg_publication p
        WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('16384')
        ORDER BY 1;",

    "SELECT c.oid::pg_catalog.regclass
        FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
        WHERE c.oid = i.inhparent AND i.inhrelid = '16384'
          AND c.relkind != 'p' AND c.relkind != 'I'
        ORDER BY inhseqno;",

    "SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
        FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
        WHERE c.oid = i.inhrelid AND i.inhparent = '16384'
        ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text;",

    r#"SELECT
      d.datname as "Name",
      pg_catalog.pg_get_userbyid(d.datdba) as "Owner",
      pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding",
      CASE d.datlocprovider WHEN 'b' THEN 'builtin' WHEN 'c' THEN 'libc' WHEN 'i' THEN 'icu' END AS "Locale Provider",
      d.datcollate as "Collate",
      d.datctype as "Ctype",
      d.daticulocale as "Locale",
      d.daticurules as "ICU Rules",
      CASE WHEN pg_catalog.array_length(d.datacl, 1) = 0 THEN '(none)' ELSE pg_catalog.array_to_string(d.datacl, E'\n') END AS "Access privileges"
    FROM pg_catalog.pg_database d
    ORDER BY 1;"#,

    // Queries from describing a table, for example `\d customer`

    r#"SELECT c.oid,
        n.nspname,
        c.relname
      FROM pg_catalog.pg_class c
          LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname OPERATOR(pg_catalog.~) '^(customer)$' COLLATE pg_catalog.default
        AND pg_catalog.pg_table_is_visible(c.oid)
      ORDER BY 2, 3;"#,

  r#"SELECT a.attname,
        pg_catalog.format_type(a.atttypid, a.atttypmod),
        (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
        FROM pg_catalog.pg_attrdef d
        WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
        a.attnotnull,
        (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
        WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
        a.attidentity,
        a.attgenerated
      FROM pg_catalog.pg_attribute a
      WHERE a.attrelid = '16417' AND a.attnum > 0 AND NOT a.attisdropped
      ORDER BY a.attnum;"#,


  r#"SELECT true as sametable, conname,
        pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
        conrelid::pg_catalog.regclass AS ontable
      FROM pg_catalog.pg_constraint r
      WHERE r.conrelid = '16417' AND r.contype = 'f'
          AND conparentid = 0
      ORDER BY conname;"#,

  r#"SELECT conname, conrelid::pg_catalog.regclass AS ontable,
            pg_catalog.pg_get_constraintdef(oid, true) AS condef
        FROM pg_catalog.pg_constraint c
      WHERE confrelid IN (SELECT pg_catalog.pg_partition_ancestors('16417')
                          UNION ALL VALUES ('16417'::pg_catalog.regclass))
            AND contype = 'f' AND conparentid = 0
      ORDER BY conname;"#,

  r#"SELECT pol.polname, pol.polpermissive,
        CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END,
        pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
        pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),
        CASE pol.polcmd
          WHEN 'r' THEN 'SELECT'
          WHEN 'a' THEN 'INSERT'
          WHEN 'w' THEN 'UPDATE'
          WHEN 'd' THEN 'DELETE'
          END AS cmd
      FROM pg_catalog.pg_policy pol
      WHERE pol.polrelid = '16417' ORDER BY 1;"#,

  r#"SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS nsp, stxname,
      pg_catalog.pg_get_statisticsobjdef_columns(oid) AS columns,
        'd' = any(stxkind) AS ndist_enabled,
        'f' = any(stxkind) AS deps_enabled,
        'm' = any(stxkind) AS mcv_enabled,
      stxstattarget
      FROM pg_catalog.pg_statistic_ext
      WHERE stxrelid = '16417'
      ORDER BY nsp, stxname;"#,

  r#"SELECT pubname
          , NULL
          , NULL
      FROM pg_catalog.pg_publication p
          JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid
          JOIN pg_catalog.pg_class pc ON pc.relnamespace = pn.pnnspid
      WHERE pc.oid ='16417' and pg_catalog.pg_relation_is_publishable('16417')
      UNION
      SELECT pubname
          , pg_get_expr(pr.prqual, c.oid)
          , (CASE WHEN pr.prattrs IS NOT NULL THEN
              (SELECT string_agg(attname, ', ')
                FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s,
                      pg_catalog.pg_attribute
                WHERE attrelid = pr.prrelid AND attnum = prattrs[s])
              ELSE NULL END) FROM pg_catalog.pg_publication p
          JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid
          JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
      WHERE pr.prrelid = '16417'
      UNION
      SELECT pubname
          , NULL
          , NULL
      FROM pg_catalog.pg_publication p
      WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('16417')
      ORDER BY 1;"#,

  r#"SELECT c.oid::pg_catalog.regclass
      FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
      WHERE c.oid = i.inhparent AND i.inhrelid = '16417'
        AND c.relkind != 'p' AND c.relkind != 'I'
      ORDER BY inhseqno;"#,

  r#"SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
      FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
      WHERE c.oid = i.inhrelid AND i.inhparent = '16417'
      ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text;"#,

];

#[tokio::test]
pub async fn test_psql_startup_sql() {
    env_logger::init();
    let service = setup_handlers();
    let mut client = MockClient::new();

    for query in PSQL_QUERIES {
        SimpleQueryHandler::do_query(&service, &mut client, query)
            .await
            .unwrap_or_else(|e| {
                panic!("failed to run sql:\n--------------\n {query}\n--------------\n{e}")
            });
    }
}
