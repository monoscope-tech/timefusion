use pgwire::api::query::SimpleQueryHandler;

use datafusion_postgres::testing::*;

const DBEAVER_QUERIES: &[&str] = &[
    "SET extra_float_digits = 3",
    "SET application_name = 'PostgreSQL JDBC Driver'",
    "SET application_name = 'DBeaver 25.1.5 - Main <postgres>'",
    "SELECT current_schema(),session_user",
    "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname",
    "SELECT n.nspname = ANY(current_schemas(true)), n.nspname, t.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = 1034",
    "SELECT typinput='pg_catalog.array_in'::regproc as is_array, typtype, typname, pg_type.oid   FROM pg_catalog.pg_type   LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select s.r, (current_schemas(false))[s.r] as nspname                    from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  WHERE pg_type.oid = 1034  ORDER BY sp.r, pg_type.oid DESC",
    "SHOW search_path",
    "SELECT db.oid,db.* FROM pg_catalog.pg_database db WHERE datname='postgres'",
    "SELECT * FROM pg_catalog.pg_settings where name='standard_conforming_strings'",
    "SELECT string_agg(word, ',' ) from pg_catalog.pg_get_keywords() where word <> ALL ('{a,abs,absolute,action,ada,add,admin,after,all,allocate,alter,aIways,and,any,are,array,as,asc,asenstitive,assertion,assignment,asymmetric,at,atomic,attribute,attributes,authorization,avg,before,begin,bernoulli,between,bigint,binary,blob,boolean,both,breaadth,by,c,call,called,cardinaliity,cascade,cascaded,case,cast,catalog,catalog_name,ceil,ceiling,chain,char,char_length,character,character_length,character_set_catalog,character_set_name,character_set_schema,characteristics,characters,check,checkeed,class_origin,clob,close,coalesce,coboI,code_units,collate,collation,collaition_catalog,collaition_name,collaition_schema,collect,colum,column_name,command_function,command_function_code,commit,committed,condiition,condiition_number,connect,connection_name,constraint,constraint_catalog,constraint_name,constraint_schema,constraints,constructors,contains,continue,convert,corr,correspondiing,count,covar_pop,covar_samp,create,cross,cube,cume_dist,current,current_collation,current_date,current_default_transfom_group,current_path,current_role,current_time,current_timestamp,current_transfom_group_for_type,current_user,cursor,cursor_name,cycle,data,date,datetime_interval_code,datetime_interval_precision,day,deallocate,dec,decimaI,declare,default,defaults,not,null,nullable,nullif,nulls,number,numeric,object,octeet_length,octets,of,old,on,only,open,option,options,or,order,ordering,ordinaliity,others,out,outer,output,over,overlaps,overlay,overriding,pad,parameter,parameter_mode,parameter_name,parameter_ordinal_position,parameter_speciific_catalog,parameter_speciific_name,parameter_speciific_schema,partiaI,partitioon,pascal,path,percent_rank,percentile_cont,percentile_disc,placing,pli,position,power,preceding,precision,prepare,preseerv,primary,prior,privileges,procedure,public,range,rank,read,reads,real,recursivve,ref,references,referencing,regr_avgx,regr_avgy,regr_count,regr_intercept,regr_r2,regr_slope,regr_sxx,regr_sxy,regr_sy y,relative,release,repeatable,restart,result,retun,returned_cardinality,returned_length,returned_octeet_length,returned_sqlstate,returns,revoe,right,role,rollback,rollup,routine,routine_catalog,routine_name,routine_schema,row,row_count,row_number,rows,savepoint,scale,schema,schema_name,scope_catalog,scope_name,scope_schema,scroll,search,second,section,security,select,self,sensitive,sequence,seriializeable,server_name,session,session_user,set,sets,similar,simple,size,smalIint,some,source,space,specifiic,speciific_name,speciifictype,sql,sqlexception,sqlstate,sqlwarning,sqrt,start,state,statement,static,stddev_pop,stddev_samp,structure,style,subclass_origin,submultiset,substring,sum,symmetric,system,system_user,table,table_name,tablesample,temporary,then,ties,time,timesamp,timezone_hour,timezone_minute,to,top_level_count,trailing,transaction,transaction_active,transactions_committed,transactions_rolled_back,transfor,transforms,translate,translation,treat,trigger,trigger_catalog,trigger_name,trigger_schema,trim,true,type,unbounde,undefined,uncommitted,under,union,unique,unknown,unnaamed,unnest,update,upper,usage,user,user_defined_type_catalog,user_defined_type_code,user_defined_type_name,user_defined_type_schema,using,value,values,var_pop,var_samp,varchar,varying,view,when,whenever,where,width_bucket,window,with,within,without,work,write,year,zone}'::text[])",
    "SELECT version()",
    "SELECT * FROM pg_catalog.pg_enum WHERE 1<>1 LIMIT 1",
    "SELECT reltype FROM pg_catalog.pg_class WHERE 1<>1 LIMIT 1",
    "SELECT t.oid,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name, d.description FROM pg_catalog.pg_type t LEFT OUTER JOIN pg_catalog.pg_type et ON et.oid=t.typelem LEFT OUTER JOIN pg_catalog.pg_class c ON c.oid=t.typrelid LEFT OUTER JOIN pg_catalog.pg_description d ON t.oid=d.objoid WHERE t.typname IS NOT NULL AND (c.relkind IS NULL OR c.relkind = 'c') AND (et.typcategory IS NULL OR et.typcategory <> 'C')",
    "SELECT c.oid,c.*,d.description,pg_catalog.pg_get_expr(c.relpartbound, c.oid) as partition_expr,  pg_catalog.pg_get_partkeydef(c.oid) as partition_key
    FROM pg_catalog.pg_class c
    LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_class'::regclass
    WHERE c.relnamespace=11 AND c.relkind not in ('i','I','c')",
    "select c.oid,pg_catalog.pg_total_relation_size(c.oid) as total_rel_size,pg_catalog.pg_relation_size(c.oid) as rel_size
     FROM pg_class c
     WHERE c.relnamespace='public'",

    "SELECT i.*,i.indkey as keys,c.relname,c.relnamespace,c.relam,c.reltablespace,tc.relname as tabrelname,dsc.description,pg_catalog.pg_get_expr(i.indpred, i.indrelid) as pred_expr,pg_catalog.pg_get_expr(i.indexprs, i.indrelid, true) as expr,pg_catalog.pg_relation_size(i.indexrelid) as index_rel_size,pg_catalog.pg_stat_get_numscans(i.indexrelid) as index_num_scans FROM pg_catalog.pg_index i
    INNER JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid
    INNER JOIN pg_catalog.pg_class tc ON tc.oid=i.indrelid
    LEFT OUTER JOIN pg_catalog.pg_description dsc ON i.indexrelid=dsc.objoid
    WHERE  i.indrelid=1 ORDER BY tabrelname, c.relname",

    "SELECT c.oid,c.*,t.relname as tabrelname,rt.relnamespace as refnamespace,d.description, case when c.contype='c' then \"substring\"(pg_get_constraintdef(c.oid), 7) else null end consrc_copy
    FROM pg_catalog.pg_constraint c
    INNER JOIN pg_catalog.pg_class t ON t.oid=c.conrelid
    LEFT OUTER JOIN pg_catalog.pg_class rt ON rt.oid=c.confrelid
    LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_constraint'::regclass
    WHERE c.conrelid=1
    ORDER BY c.oid",

];

#[tokio::test]
pub async fn test_dbeaver_startup_sql() {
    env_logger::init();
    let service = setup_handlers();
    let mut client = MockClient::new();

    for query in DBEAVER_QUERIES {
        SimpleQueryHandler::do_query(&service, &mut client, query)
            .await
            .unwrap_or_else(|e| panic!("failed to run sql: {query}\n{e}"));
    }
}
