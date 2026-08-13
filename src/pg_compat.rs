use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    },
    catalog::{MemTable, SchemaProvider, TableFunctionImpl, TableProvider},
    common::ToDFSchema,
    datasource::empty::EmptyTable,
    error::{DataFusionError, Result as DFResult},
    execution::context::SessionContext,
    logical_expr::expr::Placeholder,
    logical_expr::{
        ColumnarValue, Expr, LogicalPlan, LogicalPlanBuilder, ScalarFunctionArgs, ScalarFunctionImplementation, ScalarUDF, ScalarUDFImpl, Signature,
        TypeSignature, Volatility, create_udf, lit,
    },
    scalar::ScalarValue,
    sql::sqlparser::ast::{SelectItem, SetExpr, Statement, TableFactor},
};
use datafusion_postgres::{
    datafusion_pg_catalog::{
        pg_catalog::context::{PgCatalogContextProvider, Role},
        setup_pg_catalog,
    },
    hooks::{HookClient, QueryHook},
    pgwire::{
        api::{
            ClientInfo, Type,
            auth::{DefaultServerParameterProvider, ServerParameterProvider},
            results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response},
        },
        error::{PgWireError, PgWireResult},
    },
};
use futures::stream;

pub const PG_COMPAT_VERSION: &str = "16.6";
pub const PG_COMPAT_VERSION_NUM: &str = "160006";
pub const PG_COMPAT_DATABASE: &str = "postgres";
pub const PG_COMPAT_SCHEMA: &str = "public";
pub const DEFAULT_MAX_STATEMENT_SECS: u64 = 60;

#[derive(Debug, Clone)]
pub struct PgCatalogContext {
    role: String,
}

impl PgCatalogContext {
    pub fn new(role: impl Into<String>) -> Self {
        Self { role: role.into() }
    }
}

#[async_trait]
impl PgCatalogContextProvider for PgCatalogContext {
    async fn roles(&self) -> Vec<String> {
        vec![self.role.clone()]
    }

    async fn role(&self, name: &str) -> Option<Role> {
        (name == self.role).then(|| Role {
            name: self.role.clone(),
            is_superuser: true,
            can_login: true,
            can_create_db: false,
            can_create_role: false,
            can_create_user: false,
            can_replication: false,
            grants: vec![],
            inherited_roles: vec![],
        })
    }
}

pub fn setup_catalog(ctx: &SessionContext, role: &str, max_statement_secs: u64) -> DFResult<()> {
    setup_pg_catalog(ctx, "datafusion", PgCatalogContext::new(role)).map_err(|err| *err)?;
    register_identity_udfs(ctx, role, max_statement_secs);
    overlay_runtime_stat_views(ctx)
}

/// The runtime-statistics views pgAdmin's dashboard polls. TF keeps no session,
/// lock or replication registry, so these are honestly empty — but they must
/// *exist*, or every dashboard refresh raises a planning error.
///
/// Column shapes follow PostgreSQL 16. `oid`/`xid` map to UInt32, `name`/`inet`/
/// `pg_lsn`/`interval` to Utf8, matching how the pg_catalog crate types its own.
const RUNTIME_STAT_VIEWS: [(&str, &str); 6] = [
    (
        "pg_stat_activity",
        "datid:oid,datname:text,pid:i4,leader_pid:i4,usesysid:oid,usename:text,application_name:text,client_addr:text,client_hostname:text,\
         client_port:i4,backend_start:ts,xact_start:ts,query_start:ts,state_change:ts,wait_event_type:text,wait_event:text,state:text,\
         backend_xid:oid,backend_xmin:oid,query_id:i8,query:text,backend_type:text",
    ),
    (
        "pg_stat_database",
        "datid:oid,datname:text,numbackends:i4,xact_commit:i8,xact_rollback:i8,blks_read:i8,blks_hit:i8,tup_returned:i8,tup_fetched:i8,\
         tup_inserted:i8,tup_updated:i8,tup_deleted:i8,conflicts:i8,temp_files:i8,temp_bytes:i8,deadlocks:i8,checksum_failures:i8,\
         checksum_last_failure:ts,blk_read_time:f8,blk_write_time:f8,session_time:f8,active_time:f8,idle_in_transaction_time:f8,sessions:i8,\
         sessions_abandoned:i8,sessions_fatal:i8,sessions_killed:i8,stats_reset:ts",
    ),
    (
        "pg_locks",
        "locktype:text,database:oid,relation:oid,page:i4,tuple:i2,virtualxid:text,transactionid:oid,classid:oid,objid:oid,objsubid:i2,\
         virtualtransaction:text,pid:i4,mode:text,granted:bool,fastpath:bool,waitstart:ts",
    ),
    ("pg_prepared_xacts", "transaction:oid,gid:text,prepared:ts,owner:text,database:text"),
    (
        "pg_stat_replication",
        "pid:i4,usesysid:oid,usename:text,application_name:text,client_addr:text,client_hostname:text,client_port:i4,backend_start:ts,\
         backend_xmin:oid,state:text,sent_lsn:text,write_lsn:text,flush_lsn:text,replay_lsn:text,write_lag:text,flush_lag:text,replay_lag:text,\
         sync_priority:i4,sync_state:text,reply_time:ts",
    ),
    ("pg_available_extensions", "name:text,default_version:text,installed_version:text,comment:text"),
];

fn overlay_runtime_stat_views(ctx: &SessionContext) -> DFResult<()> {
    let catalog = ctx.catalog("datafusion").ok_or_else(|| DataFusionError::Internal("catalog 'datafusion' missing after pg_catalog setup".to_string()))?;
    let inner = catalog.schema("pg_catalog").ok_or_else(|| DataFusionError::Internal("schema 'pg_catalog' missing after setup".to_string()))?;
    let extra = RUNTIME_STAT_VIEWS.iter().map(|(name, spec)| Ok((*name, empty_pg_table(spec)?))).collect::<DFResult<HashMap<_, _>>>()?;
    catalog.register_schema("pg_catalog", Arc::new(PgCatalogOverlay { inner, extra }))?;
    Ok(())
}

fn empty_pg_table(spec: &str) -> DFResult<Arc<dyn TableProvider>> {
    let fields = spec
        .split(',')
        .map(|column| {
            let (name, code) = column.trim().split_once(':').ok_or_else(|| DataFusionError::Internal(format!("malformed column spec {column:?}")))?;
            let data_type = match code {
                "oid" => DataType::UInt32,
                "i2" => DataType::Int16,
                "i4" => DataType::Int32,
                "i8" => DataType::Int64,
                "f8" => DataType::Float64,
                "bool" => DataType::Boolean,
                "ts" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                "text" => DataType::Utf8,
                other => return Err(DataFusionError::Internal(format!("unknown column type code {other:?}"))),
            };
            Ok(Field::new(name, data_type, true))
        })
        .collect::<DFResult<Vec<_>>>()?;
    Ok(Arc::new(EmptyTable::new(Arc::new(Schema::new(fields)))))
}

/// Delegates to the pg_catalog crate's schema provider, adding tables it does
/// not ship. It hardcodes its table list and does not implement
/// `register_table`, so wrapping is the only way to extend it from here.
#[derive(Debug)]
struct PgCatalogOverlay {
    inner: Arc<dyn SchemaProvider>,
    extra: HashMap<&'static str, Arc<dyn TableProvider>>,
}

#[async_trait]
impl SchemaProvider for PgCatalogOverlay {
    fn table_names(&self) -> Vec<String> {
        let mut names = self.inner.table_names();
        names.extend(self.extra.keys().map(ToString::to_string));
        names
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match self.extra.get(name.to_ascii_lowercase().as_str()) {
            Some(table) => Ok(Some(Arc::clone(table))),
            None => self.inner.table(name).await,
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.extra.contains_key(name.to_ascii_lowercase().as_str()) || self.inner.table_exist(name)
    }
}

pub fn effective_statement_timeout(client_timeout: Option<Duration>, max_statement_secs: u64) -> Option<Duration> {
    let client_timeout = client_timeout.filter(|timeout| !timeout.is_zero());
    let server_timeout = (max_statement_secs != 0).then(|| Duration::from_secs(max_statement_secs));
    match (client_timeout, server_timeout) {
        (Some(client), Some(server)) => Some(client.min(server)),
        (Some(timeout), None) | (None, Some(timeout)) => Some(timeout),
        (None, None) => None,
    }
}

pub fn statement_timeout_error() -> PgWireError {
    PgWireError::UserError(Box::new(datafusion_postgres::pgwire::error::ErrorInfo::new(
        "ERROR".to_string(),
        "57014".to_string(),
        "canceling statement due to statement timeout".to_string(),
    )))
}

#[derive(Debug, Default)]
pub struct TimeFusionServerParameterProvider {
    inner: DefaultServerParameterProvider,
}

impl ServerParameterProvider for TimeFusionServerParameterProvider {
    fn server_parameters<C>(&self, client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        self.inner.server_parameters(client).map(|mut parameters| {
            parameters.insert("server_version".to_string(), PG_COMPAT_VERSION.to_string());
            parameters.insert("server_version_num".to_string(), PG_COMPAT_VERSION_NUM.to_string());
            parameters
        })
    }
}

#[derive(Debug)]
pub struct PgCompatibilityHook {
    role: String,
    max_statement_secs: u64,
}

/// Alias that identifies pgAdmin's role probe. No other client query uses it.
const ROLE_PROBE_ALIAS: &str = "can_signal_backend";

#[derive(Debug, Clone)]
enum RoleField {
    Oid(i32),
    Text(String),
    Bool(bool),
}

impl PgCompatibilityHook {
    pub fn new(role: impl Into<String>, max_statement_secs: u64) -> Self {
        Self { role: role.into(), max_statement_secs }
    }

    /// pgAdmin's connect-time role probe, answered here rather than planned.
    ///
    /// It computes `can_signal_backend` as
    /// `array_contains(ARRAY(WITH RECURSIVE ...), $1)`, and DataFusion supports
    /// neither the array-subquery constructor (`Invalid function 'array'`) nor
    /// that recursive CTE (`project index 0 out of bounds`). The statement
    /// cannot be planned at all, so pgAdmin cannot connect without this.
    ///
    /// An unrecognised alias bails to the planner on purpose: a pgAdmin version
    /// that adds a column then fails loudly with the same planning error,
    /// rather than being served a fabricated value for it.
    fn role_probe(&self, statement: &Statement) -> Option<Vec<(String, RoleField)>> {
        let Statement::Query(query) = statement else {
            return None;
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        if !select.from.iter().any(|from| is_pg_roles(&from.relation)) {
            return None;
        }
        let aliases = select
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.to_ascii_lowercase()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        aliases.iter().any(|alias| alias == ROLE_PROBE_ALIAS).then_some(())?;
        aliases.into_iter().map(|alias| Some((alias.clone(), self.role_field(&alias)?))).collect()
    }

    fn role_field(&self, alias: &str) -> Option<RoleField> {
        Some(match alias {
            // The pg_catalog crate reports this role with oid 0.
            "id" => RoleField::Oid(0),
            "name" => RoleField::Text(self.role.clone()),
            // TF authenticates a single superuser, and each of these is
            // `CASE WHEN rolsuper THEN true ELSE ... END` in pgAdmin's own SQL,
            // so a superuser makes every one of them true.
            "is_superuser" | "can_create_role" | "can_create_db" | ROLE_PROBE_ALIAS => RoleField::Bool(true),
            _ => return None,
        })
    }

    fn show(&self, statement: &Statement, client: &(impl ClientInfo + ?Sized)) -> Option<(String, String)> {
        let Statement::ShowVariable { variable } = statement else {
            return None;
        };
        let name = variable.iter().map(|ident| ident.value.to_ascii_lowercase()).collect::<Vec<_>>().join(".");
        let value = match name.as_str() {
            "server_version" => PG_COMPAT_VERSION.to_string(),
            "server_version_num" => PG_COMPAT_VERSION_NUM.to_string(),
            "is_superuser" => "on".to_string(),
            // `search_path` is deliberately NOT answered here: `SetShowHook`
            // runs behind this hook and reads the value `SET search_path` wrote
            // to the client session. A constant here would report `public` back
            // to every client that had switched schema.
            "statement_timeout" => effective_statement_timeout(client_statement_timeout(client), self.max_statement_secs)
                .map_or_else(|| "0".to_string(), |timeout| format!("{}ms", timeout.as_millis())),
            _ => return None,
        };
        Some((name, value))
    }
}

#[async_trait]
impl QueryHook for PgCompatibilityHook {
    async fn handle_simple_query(
        &self, statement: &Statement, _session_context: &SessionContext, client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if let Some(fields) = self.role_probe(statement) {
            return Some(role_probe_response(&fields).map(Response::Query));
        }
        self.show(statement, client).map(|(name, value)| show_response(&name, &value).map(Response::Query))
    }

    async fn handle_extended_parse_query(
        &self, statement: &Statement, _session_context: &SessionContext, _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        if let Some(fields) = self.role_probe(statement) {
            return Some(role_probe_plan(statement, &fields));
        }
        self.show(statement, _client).map(|_| show_plan())
    }

    async fn handle_extended_query(
        &self, statement: Option<&Statement>, _logical_plan: &LogicalPlan, _params: &datafusion::common::ParamValues, _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        // Deliberately NOT intercepted for the role probe: `handle_extended_parse_query`
        // already returned a plan that produces the row, and letting the normal
        // executor run it is what encodes the columns in the result format the
        // client asked for. A hand-built Response here is always text, which a
        // client requesting binary cannot decode for int4/bool.
        statement.and_then(|statement| self.show(statement, client)).map(|(name, value)| show_response(&name, &value).map(Response::Query))
    }
}

fn is_pg_roles(relation: &TableFactor) -> bool {
    matches!(relation, TableFactor::Table { name, .. } if name.to_string().to_ascii_lowercase().ends_with("pg_roles"))
}

fn role_probe_field_type(value: &RoleField) -> (Type, DataType) {
    match value {
        RoleField::Oid(_) => (Type::INT4, DataType::Int32),
        RoleField::Text(_) => (Type::VARCHAR, DataType::Utf8),
        RoleField::Bool(_) => (Type::BOOL, DataType::Boolean),
    }
}

fn role_probe_response(fields: &[(String, RoleField)]) -> PgWireResult<QueryResponse> {
    let infos = Arc::new(
        fields.iter().map(|(name, value)| FieldInfo::new(name.clone(), None, None, role_probe_field_type(value).0, FieldFormat::Text)).collect::<Vec<_>>(),
    );
    let row = {
        let mut encoder = DataRowEncoder::new(Arc::clone(&infos));
        for (_, value) in fields {
            match value {
                RoleField::Oid(oid) => encoder.encode_field(&Some(*oid))?,
                RoleField::Text(text) => encoder.encode_field(&Some(text.as_str()))?,
                RoleField::Bool(flag) => encoder.encode_field(&Some(*flag))?,
            }
        }
        encoder.take_row()
    };
    Ok(QueryResponse::new(infos, stream::once(async move { Ok(row) })))
}

/// A tautology over every `$n` the original statement bound, so the substitute
/// plan declares the same parameters. Without it the client's Bind is rejected
/// with "expected 0 parameters but got 1" — pgAdmin binds the role name it is
/// testing for. The type is Utf8 because that is what pgAdmin sends; the
/// predicate is always true, so the value is never actually consulted.
fn bound_parameter_tautology(statement: &Statement) -> Option<Expr> {
    let text = statement.to_string();
    (1..)
        .take_while(|index| text.contains(&format!("${index}")))
        .map(|index| {
            Expr::Placeholder(Placeholder::new_with_field(format!("${index}"), Some(Arc::new(Field::new(format!("${index}"), DataType::Utf8, true)))))
                .is_null()
                .or(lit(true))
        })
        .reduce(Expr::and)
}

/// A plan that *produces* the row, rather than an empty relation shaped like it:
/// the extended protocol executes this plan, and only the real executor encodes
/// columns in the result format the client requested.
fn role_probe_plan(statement: &Statement, fields: &[(String, RoleField)]) -> PgWireResult<LogicalPlan> {
    let projection = fields
        .iter()
        .map(|(name, value)| {
            match value {
                RoleField::Oid(oid) => lit(*oid),
                RoleField::Text(text) => lit(text.as_str()),
                RoleField::Bool(flag) => lit(*flag),
            }
            .alias(name)
        })
        .collect::<Vec<_>>();
    let builder = LogicalPlanBuilder::empty(true);
    match bound_parameter_tautology(statement) {
        Some(predicate) => builder.filter(predicate).and_then(|builder| builder.project(projection)),
        None => builder.project(projection),
    }
    .and_then(LogicalPlanBuilder::build)
    .map_err(|err| PgWireError::ApiError(Box::new(err)))
}

fn client_statement_timeout(client: &(impl ClientInfo + ?Sized)) -> Option<Duration> {
    client.metadata().get("statement_timeout_ms").and_then(|value| value.parse::<u64>().ok()).map(Duration::from_millis)
}

fn show_plan() -> PgWireResult<LogicalPlan> {
    Arc::new(Schema::new(vec![Field::new("show", DataType::Utf8, false)]))
        .to_dfschema()
        .map(|schema| LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation { produce_one_row: true, schema: Arc::new(schema) }))
        .map_err(|err| PgWireError::ApiError(Box::new(err)))
}

fn show_response(name: &str, value: &str) -> PgWireResult<QueryResponse> {
    let fields = Arc::new(vec![FieldInfo::new(name.to_string(), None, None, Type::VARCHAR, FieldFormat::Text)]);
    let row = {
        let mut encoder = DataRowEncoder::new(Arc::clone(&fields));
        encoder.encode_field(&Some(value))?;
        encoder.take_row()
    };
    Ok(QueryResponse::new(fields, stream::once(async move { Ok(row) })))
}

fn register_identity_udfs(ctx: &SessionContext, role: &str, max_statement_secs: u64) {
    ctx.register_udf(constant_string_udf("current_database", PG_COMPAT_DATABASE.to_string(), Volatility::Stable));
    ctx.register_udf(constant_string_udf("session_user", role.to_string(), Volatility::Stable));
    ctx.register_udf(constant_string_udf(
        "version",
        format!("PostgreSQL {PG_COMPAT_VERSION} (TimeFusion {}) on {}-{}", env!("CARGO_PKG_VERSION"), std::env::consts::ARCH, std::env::consts::OS),
        Volatility::Stable,
    ));
    ctx.register_udf(ScalarUDF::from(CurrentSettingUdf::new(max_statement_secs)));
    // pgAdmin checks replica status on connect; TF is never a standby.
    ctx.register_udf(create_udf(
        "pg_is_in_recovery",
        vec![],
        DataType::Boolean,
        Volatility::Stable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))))),
    ));
    ctx.register_udtf("pg_show_all_settings", Arc::new(PgShowAllSettingsFunction { max_statement_secs }));
}

fn constant_string_udf(name: &str, value: String, volatility: Volatility) -> ScalarUDF {
    let function: ScalarFunctionImplementation = Arc::new(move |_| Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.clone())))));
    create_udf(name, vec![], DataType::Utf8, volatility, function)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CurrentSettingUdf {
    max_statement_secs: u64,
    signature: Signature,
}

impl CurrentSettingUdf {
    fn new(max_statement_secs: u64) -> Self {
        Self {
            max_statement_secs,
            signature: Signature::one_of(
                [DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8]
                    .into_iter()
                    .flat_map(|text| [TypeSignature::Exact(vec![text.clone()]), TypeSignature::Exact(vec![text, DataType::Boolean])])
                    .collect(),
                Volatility::Stable,
            ),
        }
    }
}

impl ScalarUDFImpl for CurrentSettingUdf {
    fn name(&self) -> &str {
        "current_setting"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let names = args.args[0].to_array(args.number_rows)?;
        let missing_ok_array = args.args.get(1).map(|value| value.to_array(args.number_rows)).transpose()?;
        let missing_ok = missing_ok_array
            .as_ref()
            .map(|value| {
                value
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| DataFusionError::Execution("current_setting missing_ok must be Boolean".to_string()))
            })
            .transpose()?;
        let mut result = StringBuilder::new();
        for index in 0..names.len() {
            let name = (!names.is_null(index))
                .then(|| ScalarValue::try_from_array(&names, index))
                .transpose()?
                .and_then(|value| crate::optimizers::extract_utf8_string(&value));
            match name.and_then(|name| compatibility_setting(&name, self.max_statement_secs)) {
                Some(value) => result.append_value(value),
                None if missing_ok.is_some_and(|values| values.value(index)) => result.append_null(),
                None => return Err(DataFusionError::Execution("unrecognized configuration parameter".to_string())),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }
}

/// Every setting `current_setting()` answers — also the row set of
/// `pg_show_all_settings()`, so the two can never disagree.
const COMPATIBILITY_SETTING_NAMES: [&str; 15] = [
    "server_version",
    "server_version_num",
    "search_path",
    "is_superuser",
    "standard_conforming_strings",
    "client_encoding",
    "timezone",
    "datestyle",
    "intervalstyle",
    "statement_timeout",
    "bytea_output",
    "client_min_messages",
    "integer_datetimes",
    "default_transaction_read_only",
    "in_hot_standby",
];

fn compatibility_setting(name: &str, max_statement_secs: u64) -> Option<String> {
    Some(match name.to_ascii_lowercase().as_str() {
        "server_version" => PG_COMPAT_VERSION.to_string(),
        "server_version_num" => PG_COMPAT_VERSION_NUM.to_string(),
        "search_path" => PG_COMPAT_SCHEMA.to_string(),
        "is_superuser" | "standard_conforming_strings" | "integer_datetimes" => "on".to_string(),
        "default_transaction_read_only" | "in_hot_standby" => "off".to_string(),
        "client_encoding" => "UTF8".to_string(),
        "timezone" => "UTC".to_string(),
        "datestyle" => "ISO, MDY".to_string(),
        "intervalstyle" => "postgres".to_string(),
        "bytea_output" => "hex".to_string(),
        "client_min_messages" => "notice".to_string(),
        "statement_timeout" => (max_statement_secs * 1_000).to_string(),
        _ => return None,
    })
}

/// `pg_settings` is a view over this set-returning function; pgAdmin calls the
/// function directly on connect, and the upstream pg_catalog crate only ships
/// the view — without this, every pgAdmin connection fails at planning.
#[derive(Debug)]
struct PgShowAllSettingsFunction {
    max_statement_secs: u64,
}

impl PgShowAllSettingsFunction {
    fn schema() -> SchemaRef {
        let text = |name: &str| Field::new(name, DataType::Utf8, true);
        Arc::new(Schema::new(
            [
                "name",
                "setting",
                "unit",
                "category",
                "short_desc",
                "extra_desc",
                "context",
                "vartype",
                "source",
                "min_val",
                "max_val",
                "enumvals",
                "boot_val",
                "reset_val",
                "sourcefile",
            ]
            .map(text)
            .into_iter()
            .chain([Field::new("sourceline", DataType::Int32, true), Field::new("pending_restart", DataType::Boolean, true)])
            .collect::<Vec<_>>(),
        ))
    }

    fn batch(&self) -> DFResult<RecordBatch> {
        let rows: Vec<(&str, String)> =
            COMPATIBILITY_SETTING_NAMES.iter().filter_map(|name| compatibility_setting(name, self.max_statement_secs).map(|value| (*name, value))).collect();
        let strings = |values: Vec<Option<String>>| Arc::new(StringArray::from(values)) as ArrayRef;
        let settings: Vec<Option<String>> = rows.iter().map(|(_, value)| Some(value.clone())).collect();
        let nulls = || vec![None; rows.len()];
        let columns = vec![
            strings(rows.iter().map(|(name, _)| Some((*name).to_string())).collect()),
            strings(settings.clone()),
            strings(nulls()),
            strings(nulls()),
            strings(nulls()),
            strings(nulls()),
            strings(vec![Some("user".to_string()); rows.len()]),
            strings(rows.iter().map(|(_, value)| Some(if matches!(value.as_str(), "on" | "off") { "bool" } else { "string" }.to_string())).collect()),
            strings(vec![Some("default".to_string()); rows.len()]),
            strings(nulls()),
            strings(nulls()),
            strings(nulls()),
            strings(settings.clone()),
            strings(settings),
            strings(nulls()),
            Arc::new(Int32Array::from(vec![None::<i32>; rows.len()])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![Some(false); rows.len()])) as ArrayRef,
        ];
        RecordBatch::try_new(Self::schema(), columns).map_err(Into::into)
    }
}

impl TableFunctionImpl for PgShowAllSettingsFunction {
    fn call(&self, _args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        Ok(Arc::new(MemTable::try_new(Self::schema(), vec![vec![self.batch()?]])?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::AsArray;

    #[test]
    fn effective_timeout_uses_the_smaller_nonzero_value() {
        assert_eq!(effective_statement_timeout(None, 60), Some(Duration::from_secs(60)));
        assert_eq!(effective_statement_timeout(Some(Duration::ZERO), 60), Some(Duration::from_secs(60)));
        assert_eq!(effective_statement_timeout(Some(Duration::from_secs(5)), 60), Some(Duration::from_secs(5)));
        assert_eq!(effective_statement_timeout(Some(Duration::from_secs(120)), 60), Some(Duration::from_secs(60)));
        assert_eq!(effective_statement_timeout(None, 0), None);
    }

    #[test]
    fn compatibility_settings_are_case_insensitive() {
        assert_eq!(compatibility_setting("TimeZone", 60).as_deref(), Some("UTC"));
        assert_eq!(compatibility_setting("unknown", 60), None);
    }

    #[test]
    fn startup_parameters_report_postgres_16_6() {
        let client = datafusion_postgres::testing::MockClient::new();
        let parameters = TimeFusionServerParameterProvider::default().server_parameters(&client).unwrap();
        assert_eq!(parameters.get("server_version").map(String::as_str), Some(PG_COMPAT_VERSION));
        assert_eq!(parameters.get("server_version_num").map(String::as_str), Some(PG_COMPAT_VERSION_NUM));
    }

    /// pgAdmin fails to connect with "table function 'pg_show_all_settings' not
    /// found" if this regresses; the upstream pg_catalog only ships the view.
    #[tokio::test]
    async fn pg_show_all_settings_is_callable_as_a_table_function() {
        let ctx = SessionContext::new();
        register_identity_udfs(&ctx, "operator", 60);
        let batches = ctx.sql("SELECT setting FROM pg_show_all_settings() WHERE name = 'server_version'").await.unwrap().collect().await.unwrap();
        assert_eq!(batches[0].column(0).as_string::<i32>().value(0), PG_COMPAT_VERSION);

        let all = ctx.sql("SELECT * FROM pg_show_all_settings()").await.unwrap().collect().await.unwrap();
        assert_eq!(all[0].num_rows(), COMPATIBILITY_SETTING_NAMES.len());
        assert_eq!(all[0].num_columns(), 17);
    }

    /// pgAdmin's dashboard polls these on a timer; a missing one is a planning
    /// error on every refresh. Overlaying is load-bearing — the pg_catalog crate
    /// hardcodes its table list, so a silent overlay regression looks like this.
    #[tokio::test]
    async fn runtime_stat_views_are_queryable_and_empty() {
        let ctx = SessionContext::new();
        setup_catalog(&ctx, "operator", 60).unwrap();
        for (name, spec) in RUNTIME_STAT_VIEWS {
            let plan = ctx.sql(&format!("SELECT * FROM pg_catalog.{name}")).await.unwrap();
            assert_eq!(plan.schema().fields().len(), spec.split(',').count(), "{name} column count drifted from its spec");
            let batches = plan.collect().await.unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0, "{name} should be empty");
        }
        // The overlay must not shadow what the crate already provides.
        ctx.sql("SELECT oid FROM pg_catalog.pg_database LIMIT 1").await.unwrap().collect().await.unwrap();
    }

    /// Every name in the row set must actually resolve, or the table function
    /// silently drops it and `current_setting()` still errors on it.
    #[tokio::test]
    async fn pgadmin_connect_probes_all_resolve() {
        let ctx = SessionContext::new();
        register_identity_udfs(&ctx, "operator", 60);
        for name in COMPATIBILITY_SETTING_NAMES {
            assert!(compatibility_setting(name, 60).is_some(), "{name} is listed but unresolvable");
        }
        let batches = ctx.sql("SELECT current_setting('bytea_output'), pg_is_in_recovery()").await.unwrap().collect().await.unwrap();
        assert_eq!(batches[0].column(0).as_string::<i32>().value(0), "hex");
        assert!(!batches[0].column(1).as_boolean().value(0));
    }

    #[tokio::test]
    async fn identity_and_setting_udfs_match_the_compatibility_contract() {
        let ctx = SessionContext::new();
        register_identity_udfs(&ctx, "operator", 60);
        let batches = ctx
            .sql("SELECT current_database(), session_user, current_setting('server_version'), current_setting('unknown', true)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = &batches[0];
        assert_eq!(batch.column(0).as_string::<i32>().value(0), PG_COMPAT_DATABASE);
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "operator");
        assert_eq!(batch.column(2).as_string::<i32>().value(0), PG_COMPAT_VERSION);
        assert!(batch.column(3).is_null(0));
    }
}
