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

/// Empty PostgreSQL 16 runtime views needed by pgAdmin clients.
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
                .and_then(|value| crate::read::optimizers::extract_utf8_string(&value));
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

// ===== stats_table =====
// `timefusion.stats` — operator-visible introspection table.
//
// Exposes a flat (component, key, value) view of `BufferedWriteLayer` /
// `MemBuffer` / `WalManager` internals so monitoring and bench harnesses
// don't have to scrape `ps -o rss=` and guess what walrus is up to.
//
// Usage:
//     SELECT * FROM timefusion_stats;
//     SELECT key, value FROM timefusion_stats WHERE component='mem_buffer';

use std::sync::atomic::Ordering::Relaxed;

use datafusion::{catalog::Session, datasource::TableType, physical_plan::ExecutionPlan};

use crate::{database::ScanMetrics, observability::arrow_err, storage::FoyerRuntimeStats, write::BufferedWriteLayer};

/// Snapshot of the size of the resolve/provider caches at scan time.
/// Reported as `scan.fast_resolve_cache_entries` and
/// `scan.provider_cache_entries` so operators can spot the unbounded
/// growth (documented on each cache's field) before it shows up as
/// memory pressure in long-running processes.
pub type CacheSizeSnapshot = Arc<dyn Fn() -> (usize, usize) + Send + Sync>;
pub type FoyerStatsSnapshot = Arc<dyn Fn() -> FoyerRuntimeStats + Send + Sync>;
/// (used_bytes, pool_size) of the shared query memory pool, live.
pub type PoolSnapshot = Arc<dyn Fn() -> (usize, usize) + Send + Sync>;
/// (resident partitions, estimated bytes, byte limit, active builders).
pub type LogicalCountSnapshot = Arc<dyn Fn() -> (usize, usize, usize, usize) + Send + Sync>;

type Row = (&'static str, String, String);

/// `rows![component; "key" => value, …]` — one component's rows. Values only
/// need `ToString`, so the mixed usize/u64/bool/`&str` metric types need no
/// conversion at the call site. `rows![@atomic component; …]` takes the
/// counters themselves and loads each one `Relaxed`.
macro_rules! rows {
    (@atomic $component:expr; $($key:literal => $counter:expr),* $(,)?) => {
        rows![$component; $($key => $counter.load(Relaxed)),*]
    };
    ($component:expr; $($key:literal => $val:expr),* $(,)?) => {
        vec![$(($component, $key.to_string(), $val.to_string())),*]
    };
}

fn mb(bytes: f64) -> String {
    format!("{:.1}", bytes / (1024.0 * 1024.0))
}

fn mib(bytes: usize) -> usize {
    bytes / 1024 / 1024
}

fn gib(bytes: usize) -> usize {
    mib(bytes) / 1024
}

fn pct(n: u64, d: u64) -> String {
    format!("{:.1}", if d > 0 { n as f64 * 100.0 / d as f64 } else { 0.0 })
}

fn avg(total: u64, samples: u64) -> u64 {
    total.checked_div(samples).unwrap_or(0)
}

/// `atomic_stats!`-declared counters, rendered exactly as declared.
fn atomic_rows(rows: Vec<(&'static str, &'static str, u64)>) -> Vec<Row> {
    rows.into_iter().map(|(component, key, value)| (component, key.to_string(), value.to_string())).collect()
}

fn or_null<T: ToString>(v: Option<T>) -> String {
    v.map_or_else(|| "null".to_string(), |v| v.to_string())
}

#[derive(derive_more::Debug)]
#[debug("StatsTableProvider {{ layer: {layer:?}, scan_metrics: {scan_metrics:?}, .. }}")]
pub struct StatsTableProvider {
    layer: Option<Arc<BufferedWriteLayer>>,
    scan_metrics: Option<Arc<ScanMetrics>>,
    cache_sizes: Option<CacheSizeSnapshot>,
    foyer_stats: Option<FoyerStatsSnapshot>,
    query_pool: Option<PoolSnapshot>,
    logical_count: Option<LogicalCountSnapshot>,
    tantivy_search: Option<Arc<crate::tantivy::search::TantivySearchService>>,
    bloom_prune: Option<Arc<crate::read::bloom_prune::BloomPruneRegistry>>,
    schema: SchemaRef,
}

impl StatsTableProvider {
    pub fn new(layer: Option<Arc<BufferedWriteLayer>>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("component", DataType::Utf8, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        Self {
            layer,
            scan_metrics: None,
            cache_sizes: None,
            foyer_stats: None,
            query_pool: None,
            logical_count: None,
            tantivy_search: None,
            bloom_prune: None,
            schema,
        }
    }

    pub fn with_scan_metrics(self, m: Arc<ScanMetrics>) -> Self {
        Self { scan_metrics: Some(m), ..self }
    }

    pub fn with_cache_sizes(self, f: CacheSizeSnapshot) -> Self {
        Self { cache_sizes: Some(f), ..self }
    }

    pub fn with_foyer_stats(self, f: FoyerStatsSnapshot) -> Self {
        Self { foyer_stats: Some(f), ..self }
    }

    pub fn with_query_pool(self, f: PoolSnapshot) -> Self {
        Self { query_pool: Some(f), ..self }
    }

    pub fn with_logical_count(self, f: LogicalCountSnapshot) -> Self {
        Self { logical_count: Some(f), ..self }
    }

    /// `Option` because the sidecar is absent whenever no schema declares an
    /// indexed field or object storage is unconfigured — the stats table then
    /// simply omits the `tantivy` component rather than reporting zeros that
    /// look like an idle index.
    pub fn with_tantivy_search_opt(self, s: Option<Arc<crate::tantivy::search::TantivySearchService>>) -> Self {
        Self { tantivy_search: s, ..self }
    }

    /// Same posture as tantivy: absent registry ⇒ no `bloom_prune` component.
    pub fn with_bloom_prune_opt(self, r: Option<Arc<crate::read::bloom_prune::BloomPruneRegistry>>) -> Self {
        Self { bloom_prune: r, ..self }
    }

    fn snapshot_batch(&self) -> DFResult<RecordBatch> {
        // Boot memory budget. `slack_mb` is the figure that matters: it is what
        // absorbs allocation no budget tracks (parquet decode, walrus mmaps,
        // tantivy, allocator overhead), and a small slack is how the box gets
        // OOM-killed while every individual budget reads healthy.
        let budget = crate::config::boot_budget_audit().map_or_else(Vec::new, |a| {
            rows!["budget";
                "committed_mb" => a.committed_mb,
                "warn_at_mb" => a.warn_at_mb,
                "slack_mb" => a.slack_mb(),
                "query_pool_mb" => a.query_pool_mb,
                "mem_buffer_hard_mb" => a.mem_buffer_hard_mb,
                "maintenance_pool_mb" => a.maintenance_pool_mb,
                "foyer_mb" => a.foyer_mb,
                "tantivy_peak_mb" => a.tantivy_peak_mb,
                "df_metadata_cache_mb" => a.df_metadata_cache_mb,
                "oversubscribed" => a.oversubscribed(),
            ]
        });

        let layer = self.layer.as_ref().map_or_else(
            || rows!["buffered_layer"; "status" => "disabled"],
            |layer| {
                let s = layer.snapshot_stats();
                [
                    rows!["mem_buffer";
                        "project_count" => s.mem_project_count,
                        "total_buckets" => s.mem_total_buckets,
                        "total_rows" => s.mem_total_rows,
                        "total_batches" => s.mem_total_batches,
                        // Replay DML consumed without applying (table already flushed) —
                        // the quarantine dir no longer captures this loss class; monitor
                        // this like a quarantine count (growth ⇒ check logs + re-drive).
                        "replay_dml_noops_total" => s.mem_replay_dml_noops,
                        // Suffix `_approx` because the in-bucket coalesce path overwrites
                        // `memory_bytes` to the post-concat size, but the MemBuffer-level
                        // running total only adds the pre-concat new_size at insert time
                        // (no subtraction on coalesce). Drift is at most a few percent
                        // during coalesce-heavy bursts; the value is for capacity
                        // alerting, not for billing.
                        "estimated_bytes_approx" => s.mem_estimated_bytes,
                        "estimated_mb_approx" => mb(s.mem_estimated_bytes as f64),
                        "bucket_duration_micros" => s.bucket_duration_micros,
                        "oldest_bucket_age_secs" => or_null(s.oldest_bucket_age_secs),
                    ],
                    rows!["buffered_layer";
                        "reserved_bytes" => s.reserved_bytes,
                        "max_memory_bytes" => s.max_memory_bytes,
                        "max_memory_mb" => mb(s.max_memory_bytes as f64),
                        "pressure_pct" => s.pressure_pct,
                        "backpressure_engaged_total" => s.backpressure_engaged_total,
                        "backpressure_rejected_total" => s.backpressure_rejected_total,
                        "backpressure_force_flush_total" => s.backpressure_force_flush_total,
                        "flush_completed_total" => s.flush_completed_total,
                        "flush_failed_total" => s.flush_failed_total,
                        // Ingest-vs-drain: both climb in steady state. If ingested pulls
                        // ahead of flushed while pressure_pct=100 and flush_failed_total is
                        // flat, ingest is outpacing a working drain (throughput wedge) —
                        // not a stuck flush. `rows_in_buffer_lag` ≈ rows currently buffered
                        // (ingested includes WAL-recovered rows, so the pair stays
                        // comparable after a restart).
                        "rows_ingested_total" => s.rows_ingested_total,
                        "rows_flushed_total" => s.rows_flushed_total,
                        "rows_in_buffer_lag" => s.rows_ingested_total.saturating_sub(s.rows_flushed_total),
                        // Drain effectiveness: flat while pressure_pct=100 and flushes
                        // commit ⇒ drained buckets are empty (memory is in buckets the
                        // flush path isn't reaching, e.g. an open window needing force-flush).
                        "flush_freed_bytes_total" => s.flush_freed_bytes_total,
                        // Real RSS vs the estimate_batch_size charge. RSS far below
                        // estimated_bytes_approx ⇒ per-bucket estimate is over-counting and
                        // backpressure is tripping on phantom bytes, not real memory.
                        "process_rss_bytes" => or_null(s.process_rss_bytes),
                        "process_rss_mb" => or_null(s.process_rss_bytes.map(|v| mb(v as f64))),
                        // Orphaned topics = failed-commit rows living ONLY in the WAL,
                        // each pinning the WAL GC floor. PAGE on >0; remedy = restart.
                        "orphaned_topics" => s.orphaned_topics,
                        "orphan_pin_age_secs" => or_null(s.orphan_pin_age_secs),
                        "drained" => s.drained,
                        "boot_micros" => s.boot_micros,
                    ],
                    rows!["wal";
                        "recovery_complete" => s.wal_recovery_complete,
                        "recovery_duration_ms" => s.wal_recovery_duration_ms,
                        // Rows this boot re-inserted. Replay is not idempotent by
                        // design, so every restart re-adds rows already in Delta and
                        // dedup pays to remove them: 58% of the duplicate groups in a
                        // sampled prod file were byte-identical replay copies, not
                        // merge-on-read versions. Compare against the dedup drop rate
                        // to price a restart.
                        "replay_rows" => s.wal_replay_rows,
                    ],
                    rows!["tantivy"; "recovery_pending_files" => s.tantivy_recovery_pending_files],
                    rows!["wal";
                        "files" => s.wal_files,
                        "disk_bytes" => s.wal_disk_bytes,
                        "disk_mb" => mb(s.wal_disk_bytes as f64),
                        // Parked payloads: invisible to wal_disk_bytes (flat walk), which is
                        // how gc_wal_files deleted them unnoticed. ALERT if files > 0.
                        "quarantine_files" => s.quarantine_files,
                        "quarantine_mb" => mb(s.quarantine_bytes as f64),
                    ],
                    rows!["wal"; "shards_per_topic" => s.wal_shards_per_topic, "known_topics" => s.wal_known_topics],
                ]
                .into_iter()
                .flatten()
                .collect()
            },
        );

        let d = crate::observability::dml_stats();
        let dml = atomic_rows(d.stats_rows());

        // NONZERO = a scan advertised an ordering its data does not honour, i.e.
        // a parquet footer's `sorting_columns` is lying. Drives the hot-tail
        // footer repair; see read_dedup::Bound::advance.
        let read_dedup = rows![@atomic "read_dedup";
            "ordering_violations_total" => crate::read::ORDERING_VIOLATIONS,
            // Per-leg attribution, populated only while
            // TIMEFUSION_ORDERING_PROBE=true. All zero with a nonzero total
            // just means the probe is off — not that no leg is at fault.
            "ordering_violations_mem" => crate::read::ORDERING_VIOLATIONS_MEM,
            "ordering_violations_delta" => crate::read::ORDERING_VIOLATIONS_DELTA,
        ];

        let m = crate::observability::maintenance_stats();
        let mut maintenance = atomic_rows(m.stats_rows());
        maintenance.push(("maintenance", "retry_reason".to_owned(), crate::observability::maintenance_retry_reason()));
        // DERIVED, not hand-listed. `rollup_witnessless_slices` above is one
        // number for a population that sat byte-identical across four hourly
        // passes and a restart; these say why, in two dimensions that each sum to
        // it. Iterating the bucket lists is the point: a reason added without a
        // row is the bug class that cost 53% of prefilter skips their attribution
        // on 2026-08-24, and it cannot happen here.
        maintenance.extend(crate::database::rollup_unverifiable::gauge_rows().map(|(key, value)| ("maintenance", key, value.to_string())));
        // Same reason, same shape: one row per (operation, retry reason) the
        // process has actually seen, rather than a hand-kept list that a new
        // reason silently misses.
        maintenance.extend(crate::observability::maintenance_retry_rows().into_iter().map(|(key, value)| ("maintenance", key, value.to_string())));
        maintenance.extend(crate::observability::maintenance_work_rows().into_iter().map(|(key, value)| ("maintenance", key, value.to_string())));

        let plan_cache = crate::read::plan_cache::global().map_or_else(Vec::new, |pc| {
            let (hits, misses) = pc.counters();
            // Shape path = literal-bearing and now()-bearing SELECTs (the dashboard
            // hot path). Separate from the placeholder-`$N` counters above, and
            // previously unexposed — so the now()-shape caching (the bulk of the
            // dashboard work) was entirely invisible in stats. shape_hits = a plan
            // was served via the shape path (built or reused); shape_skips = a shape
            // couldn't be parameterized and fell back to a fresh plan.
            let (shape_hits, shape_skips) = pc.shape_counters();
            rows!["plan_cache";
                "hits" => hits,
                "misses" => misses,
                "hit_pct" => pct(hits, hits + misses),
                "shape_hits" => shape_hits,
                "shape_skips" => shape_skips,
            ]
        });

        let scan = self.scan_metrics.as_ref().map_or_else(Vec::new, |m| {
            use crate::database::scan_metric_names::*;
            let cv = crate::observability::counter_value;
            let q = |name: &str, p: f64| crate::observability::histogram_quantile(name, p).unwrap_or(0.0) as u64;
            let (total, skipped) = (cv(SCANS_TOTAL), cv(SCANS_SKIPPED_DELTA));
            let (fr_hits, fr_misses) = (cv(FAST_RESOLVE_HITS), cv(FAST_RESOLVE_MISSES));
            let (dedup_elig, dedup_skipped) = (cv(DEDUP_ELIGIBLE_SCANS), cv(DEDUP_SKIPPED));
            let (cert_never, cert_moved) = (cv(DEDUP_DENIED_NEVER_CERTIFIED), cv(DEDUP_DENIED_FP_MOVED));
            let cert_dwells = cv(CERT_DWELL_TOTAL);
            let (pc_hits, pc_misses) = (cv(PROVIDER_CACHE_HITS), cv(PROVIDER_CACHE_MISSES));
            let provider_builds = cv(PROVIDER_BUILD_TOTAL);
            let provider_scans = cv(PROVIDER_SCAN_TOTAL);
            let mem_plans = cv(MEM_PLAN_TOTAL);
            // Parquet decode heap — the largest consumer outside every budget.
            // `peak_batch_bytes x polls_inflight_peak` bounds the worst-case
            // concurrent decode heap, which is what a Transient budget must cover.
            // High-water marks stay hand-rolled atomics (`m.decode`) — `metrics::Gauge`
            // has no `fetch_max` equivalent, see `DecodeGauges`.
            let (dpeak, dinflight_peak) = (m.decode.decode_peak_batch_bytes.load(Relaxed), m.decode.decode_polls_inflight_peak.load(Relaxed));
            // The number the OOM killer acts on, live: without this the only
            // record of a memory climb is the kernel's post-mortem kill line.
            let (used, limit) =
                (crate::database::process_memory_bytes().unwrap_or(0) as u64, crate::config::try_config().map_or(0, |c| c.derived.memory_limit_bytes as u64));
            let (pool_used, pool_size) = self.query_pool.as_ref().map_or((0, 0), |f| f());
            [
                rows!["memory";
                    "charged_bytes" => used,
                    "limit_bytes" => limit,
                    "charged_pct" => if limit > 0 { used * 100 / limit } else { 0 },
                    // Saturation here surfaces as "Resources exhausted" query
                    // errors (2026-08-03 07:06: enrichment UPDATEs failing at
                    // 30.0/30.0 GB), invisible before this row.
                    "query_pool_used_bytes" => pool_used,
                    "query_pool_pct" => if pool_size > 0 { pool_used * 100 / pool_size } else { 0 },
                ],
                rows!["scan_decode";
                    "peak_batch_bytes" => dpeak,
                    "polls_inflight" => m.decode.decode_polls_inflight.load(Relaxed),
                    "polls_inflight_peak" => dinflight_peak,
                    "worst_case_heap_mb" => mb(dpeak.saturating_mul(dinflight_peak) as f64),
                ],
                rows!["scan";
                    "skipped_delta_pct" => pct(skipped, total),
                    "dedup_skipped_pct" => pct(dedup_skipped, dedup_elig),
                    "rollup_stale_moved" => cv(ROLLUP_STALE_SHRANK) + cv(ROLLUP_STALE_GREW),
                    "dedup_denied_never_certified_pct" => pct(cert_never, cert_never + cert_moved),
                    "cert_dwell_secs_avg" => avg(cv(CERT_DWELL_SECS_TOTAL), cert_dwells),
                    "cert_dwell_p50_secs" => m.cert_dwell_percentile_secs(0.50),
                    "cert_dwell_p90_secs" => m.cert_dwell_percentile_secs(0.90),
                    "fast_resolve_hit_pct" => pct(fr_hits, fr_hits + fr_misses),
                    "provider_cache_hit_pct" => pct(pc_hits, pc_hits + pc_misses),
                    "provider_build_us_avg" => avg(cv(PROVIDER_BUILD_US_TOTAL), provider_builds),
                    "provider_scan_us_avg" => avg(cv(PROVIDER_SCAN_US_TOTAL), provider_scans),
                    "dedup_full_set_pct" => pct(cv(DEDUP_FULL_SET_TOTAL), cv(DEDUP_BOUNDED_TOTAL) + cv(DEDUP_FULL_SET_TOTAL)),
                    // Read these BEFORE setting TIMEFUSION_WIDE_SCAN_REFUSE_MB — the
                    // threshold has to sit above p99 or it rejects working dashboards.
                    "wide_scan_selected_mb_p50" => q(WIDE_SCAN_SELECTED_MB, 0.50),
                    "wide_scan_selected_mb_p90" => q(WIDE_SCAN_SELECTED_MB, 0.90),
                    "wide_scan_selected_mb_p99" => q(WIDE_SCAN_SELECTED_MB, 0.99),
                    "mem_plan_us_avg" => avg(cv(MEM_PLAN_US_TOTAL), mem_plans),
                    "lat_p50_us_approx" => m.latency_percentile_us(0.50),
                    "lat_p95_us_approx" => m.latency_percentile_us(0.95),
                    "lat_p99_us_approx" => m.latency_percentile_us(0.99),
                    "lat_p999_us_approx" => m.latency_percentile_us(0.999),
                ],
                rows!["pgwire";
                    "lat_p50_us_approx" => m.pgwire_percentile_us(0.50),
                    "lat_p95_us_approx" => m.pgwire_percentile_us(0.95),
                    "lat_p99_us_approx" => m.pgwire_percentile_us(0.99),
                    "lat_p999_us_approx" => m.pgwire_percentile_us(0.999),
                ],
            ]
            .into_iter()
            .flatten()
            .chain(SCAN_ROWS.iter().map(|(component, key, metric)| (*component, (*key).to_string(), cv(metric).to_string())))
            .collect()
        });

        let foyer = self.foyer_stats.as_ref().map_or_else(Vec::new, |snap| {
            let s = snap();
            [("foyer", s.stats.main), ("foyer_metadata", s.stats.metadata)]
                .into_iter()
                .flat_map(|(component, st)| {
                    rows![component;
                        "hits" => st.hits,
                        "misses" => st.misses,
                        "range_hits" => st.range_hits,
                        "range_misses" => st.range_misses,
                        "bytes_served" => st.bytes_served,
                        "inner_bytes_read" => st.inner_bytes_read,
                        "range_bytes_read" => st.range_bytes_read,
                        "ttl_expirations" => st.ttl_expirations,
                        "inner_gets" => st.inner_gets,
                    ]
                })
                .chain(rows!["foyer";
                    "memory_mb" => mib(s.memory_size_bytes),
                    "disk_gb" => gib(s.disk_size_bytes),
                    "ttl_seconds" => s.ttl_seconds,
                    "l1_max_entry_mb" => mib(s.l1_max_entry_bytes),
                    "block_size_mb" => mib(s.block_size_bytes),
                    "cache_recent_days" => s.cache_recent_days,
                    "cache_dir" => s.cache_dir.display(),
                    "metadata_memory_mb" => mib(s.metadata_memory_size_bytes),
                    "metadata_disk_gb" => gib(s.metadata_disk_size_bytes),
                    "l1_used_bytes" => s.l1_used_bytes,
                    "l2_used_bytes" => s.l2_used_bytes,
                    "entry_count" => s.entry_count,
                    "evictions" => s.evictions,
                ])
                .collect()
        });

        let p = deltalake::delta_datafusion::parquet_metrics::snapshot();
        let parquet = rows!["parquet";
            "metadata_cache_hits" => p.metadata_cache_hits,
            "metadata_cache_misses" => p.metadata_cache_misses,
            "bytes_read" => p.bytes_read,
            "read_time_us" => p.read_time_us,
            "scans" => p.scans,
            "files_planned" => p.files_planned,
            "bytes_planned" => p.bytes_planned,
            "selected_row_groups" => p.selected_row_groups,
        ];

        let cache_sizes = self.cache_sizes.as_ref().map_or_else(Vec::new, |snap| {
            // Mirror the field-level doc: these caches don't evict; size
            // tracks unique (project, table) pairs since process start.
            let (fast_resolve, provider) = snap();
            rows!["scan"; "fast_resolve_cache_entries" => fast_resolve, "provider_cache_entries" => provider]
        });

        let logical_count = self.logical_count.as_ref().map_or_else(Vec::new, |snap| {
            let (entries, resident, limit, building) = snap();
            rows!["logical_count";
                "resident_partitions" => entries,
                "resident_bytes_estimated" => resident,
                "resident_mb_estimated" => mib(resident),
                "resident_limit_bytes" => limit,
                "resident_limit_mb" => mib(limit),
                "active_builds" => building,
            ]
        });

        // Tantivy read path. `*_us_avg` are the per-phase means the prefilter
        // had no way to report before: `indexes_per_query` is the fan-out that
        // dominates it, `blob_fetches` vs `cache_seeded` says whether the local
        // cache is actually absorbing reads, and `manifest_hit_pct` says whether
        // the 745 KB manifest is being re-fetched on the planning path.
        let tantivy = self.tantivy_search.as_ref().map_or_else(Vec::new, |svc| {
            let s = &svc.stats;
            let mean = |us: &std::sync::atomic::AtomicU64, n: &std::sync::atomic::AtomicU64| avg(us.load(Relaxed), n.load(Relaxed));
            let (mh, ml) = (s.manifest_hits.load(Relaxed), s.manifest_loads.load(Relaxed));
            let (rh, io) = (s.reader_hits.load(Relaxed), s.index_opens.load(Relaxed));
            let (idx, q) = (s.indexes_searched.load(Relaxed), s.queries.load(Relaxed));
            rows!["tantivy";
                "queries" => q,
                "indexes_searched_total" => idx,
                "indexes_per_query" => avg(idx, q),
                "searches" => s.searches.load(Relaxed),
                "search_us_avg" => mean(&s.search_us, &s.searches),
                "hits_materialized" => s.hits_materialized.load(Relaxed),
                "manifest_loads" => ml,
                "manifest_hits" => mh,
                "manifest_hit_pct" => pct(mh, mh + ml),
                "manifest_load_us_avg" => mean(&s.manifest_load_us, &s.manifest_loads),
                // Every blob fetch is an S3 round trip on the planning path that
                // a resident local cache would have served. Should trend to ~0
                // for the hot window once seeding + re-warm are working.
                "blob_fetches" => s.blob_fetches.load(Relaxed),
                "blob_fetch_us_avg" => mean(&s.blob_fetch_us, &s.blob_fetches),
                "index_opens" => io,
                "index_open_us_avg" => mean(&s.index_open_us, &s.index_opens),
                "reader_hits" => rh,
                "reader_hit_pct" => pct(rh, rh + io),
                "reader_cache_capacity" => svc.config.reader_cache_entries().get(),
                "search_concurrency" => svc.config.search_concurrency(),
                "cache_seeded" => s.cache_seeded.load(Relaxed),
                "cache_seed_failures" => s.cache_seed_failures.load(Relaxed),
                // Raw cumulative microseconds, alongside the means above,
                // because a MEAN CANNOT BE DIFFERENCED: each `*_us_avg` divides
                // by its own denominator (`search_us_avg` by per-index
                // `searches`, not by `queries`), so reconstructing a total as
                // avg*count silently mixes denominators — an attribution probe
                // built that way reported NEGATIVE per-query search time. These
                // are monotonic, so a before/after delta around a single query
                // is that query's exact spend, per phase.
                //
                // Read `search_us_total` as occupancy, not wall clock: per-index
                // searches run `search_concurrency`-way, so the sum exceeds the
                // wall time it cost, by up to that factor.
                "manifest_load_us_total" => s.manifest_load_us.load(Relaxed),
                "blob_fetch_us_total" => s.blob_fetch_us.load(Relaxed),
                "index_open_us_total" => s.index_open_us.load(Relaxed),
                "search_us_total" => s.search_us.load(Relaxed),
                // Closes the attribution gap the four above left open: a routed
                // 7d equality cost ~420ms more than its unrouted twin while
                // search_us accounted for only ~45ms of it, with zero IO. The
                // time is somewhere between "task starts" and "search timer
                // starts", or in the merge — which is exactly what these three
                // separate. fanout_us minus prepare_us minus search_us is the
                // result-merge bookkeeping.
                "plan_us_total" => s.plan_us.load(Relaxed),
                "prepare_us_total" => s.prepare_us.load(Relaxed),
                "prepares" => s.prepares.load(Relaxed),
                "fanout_us_total" => s.fanout_us.load(Relaxed),
            ]
        });

        // File-level needle pruning (bloom sidecars). `files_rejected` /
        // `files_probed` is the pruning rate; `registry_misses` trending to 0
        // says the resident set covers the queried window.
        let bloom_prune = self.bloom_prune.as_ref().map_or_else(Vec::new, |reg| {
            let s = &reg.stats;
            rows!["bloom_prune";
                "queries_pruned" => s.queries_pruned.load(Relaxed),
                "files_probed" => s.files_probed.load(Relaxed),
                "files_rejected" => s.files_rejected.load(Relaxed),
                "registry_hits" => s.registry_hits.load(Relaxed),
                "registry_misses" => s.registry_misses.load(Relaxed),
                "loads" => s.loads.load(Relaxed),
                "load_errors" => s.load_errors.load(Relaxed),
                "build_files" => s.build_files.load(Relaxed),
                "build_errors" => s.build_errors.load(Relaxed),
                "resident_bytes" => reg.resident_bytes(),
            ]
        });

        // Process age and worker starvation, in the same read as everything
        // else. Every other counter here is process-scoped, so `uptime_seconds`
        // is what makes them quotable at all: 0 accrual on a four-minute
        // process and 0 accrual on a five-hour one are opposite findings.
        // `scheduling_lag_ms` nonzero while the host has idle cores means
        // workers are BLOCKED, not busy — read it with the `block` rows below,
        // which name the section.
        let (lag_last, lag_max) = crate::observability::runtime_lag_ms();
        let runtime = rows!["runtime";
            "uptime_seconds" => crate::observability::process_uptime_secs(),
            "scheduling_lag_ms" => lag_last,
            "scheduling_lag_max_ms" => lag_max,
            "worker_threads" => std::thread::available_parallelism().map_or(0, std::num::NonZeroUsize::get),
        ];
        // Fragmentation, if jemalloc is the allocator (prod: yes). `frag_pct`
        // is the share of resident memory backing no live allocation — the last
        // candidate for the ~1.5h cost onset, since every other instrument here
        // either froze or fell across it.
        let jemalloc = crate::observability::jemalloc_bytes().map_or_else(Vec::new, |(allocated, active, resident, mapped, retained)| {
            rows!["jemalloc";
                "allocated_mb" => mib(allocated as usize),
                "active_mb" => mib(active as usize),
                "resident_mb" => mib(resident as usize),
                "mapped_mb" => mib(mapped as usize),
                "retained_mb" => mib(retained as usize),
                "frag_pct" => pct(resident.saturating_sub(allocated), resident),
            ]
        });

        // `block` = a worker was occupied that long; `section` = wall time only,
        // awaits included. Same shape, different claim — see `SECTION_STATS`.
        let mut sections = crate::observability::section_stats();
        sections.sort_unstable_by_key(|s| s.0);
        let block: Vec<Row> = sections
            .into_iter()
            .flat_map(|((component, name), count, total_us, max_us)| {
                [("count", count), ("total_ms", total_us / 1000), ("max_ms", max_us / 1000), ("avg_us", total_us.checked_div(count).unwrap_or(0))]
                    .map(|(k, v)| (component, format!("{name}.{k}"), v.to_string()))
            })
            .collect();

        let rows: Vec<Row> = [
            budget,
            layer,
            dml,
            read_dedup,
            maintenance,
            plan_cache,
            scan,
            foyer,
            logical_count,
            tantivy,
            bloom_prune,
            parquet,
            cache_sizes,
            runtime,
            block,
            jemalloc,
        ]
        .into_iter()
        .flatten()
        .collect();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(rows.iter().map(|r| Some(r.0)).collect::<StringArray>()),
            Arc::new(rows.iter().map(|r| Some(r.1.as_str())).collect::<StringArray>()),
            Arc::new(rows.iter().map(|r| Some(r.2.as_str())).collect::<StringArray>()),
        ];
        RecordBatch::try_new(Arc::clone(&self.schema), cols).map_err(arrow_err)
    }
}

#[async_trait]
impl TableProvider for StatsTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Build a fresh batch on every scan — counters move, we want point-in-time.
        let batch = self.snapshot_batch()?;
        let mem = MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}

#[cfg(test)]
mod stats_table_tests {
    use super::*;

    type OwnedRow = (String, String, String);

    fn snapshot_rows(p: &StatsTableProvider) -> Vec<OwnedRow> {
        let batch = p.snapshot_batch().unwrap();
        let col = |i: usize| batch.column(i).as_any().downcast_ref::<StringArray>().cloned().unwrap();
        let (components, keys, values) = (col(0), col(1), col(2));
        (0..batch.num_rows()).map(|i| (components.value(i).to_string(), keys.value(i).to_string(), values.value(i).to_string())).collect()
    }

    fn assert_has(rows: &[OwnedRow], component: &str, key: &str) {
        assert!(rows.iter().any(|(c, k, _)| c == component && k == key), "missing {component}.{key}");
    }

    #[test]
    fn exposes_foyer_runtime_configuration_and_occupancy() {
        let snapshot = FoyerRuntimeStats {
            memory_size_bytes: 4 * 1024 * 1024,
            disk_size_bytes: 128 * 1024 * 1024 * 1024,
            ttl_seconds: 3600,
            l1_max_entry_bytes: 64 * 1024 * 1024,
            block_size_bytes: 256 * 1024 * 1024,
            cache_recent_days: 1,
            cache_dir: "/cache".into(),
            metadata_memory_size_bytes: 512 * 1024 * 1024,
            metadata_disk_size_bytes: 5 * 1024 * 1024 * 1024,
            l1_used_bytes: 123,
            l2_used_bytes: 456,
            entry_count: 7,
            evictions: 8,
            ..Default::default()
        };
        let rows = snapshot_rows(&StatsTableProvider::new(None).with_foyer_stats(Arc::new(move || snapshot.clone())));

        for key in [
            "memory_mb",
            "disk_gb",
            "ttl_seconds",
            "l1_max_entry_mb",
            "block_size_mb",
            "cache_recent_days",
            "cache_dir",
            "metadata_memory_mb",
            "metadata_disk_gb",
            "l1_used_bytes",
            "l2_used_bytes",
            "entry_count",
            "evictions",
        ] {
            assert_has(&rows, "foyer", key);
        }
        assert!(rows.contains(&("foyer".into(), "cache_dir".into(), "/cache".into())));
    }

    #[test]
    fn exposes_logical_count_residency_and_build_activity() {
        let rows = snapshot_rows(&StatsTableProvider::new(None).with_logical_count(Arc::new(|| (3, 42, 100, 1))));
        for key in ["resident_partitions", "resident_bytes_estimated", "resident_mb_estimated", "resident_limit_bytes", "resident_limit_mb", "active_builds"] {
            assert_has(&rows, "logical_count", key);
        }
        assert!(rows.contains(&("logical_count".into(), "active_builds".into(), "1".into())));
    }

    /// Process age and worker starvation must be readable in the same query as
    /// the counters they qualify — a counter quoted without uptime is not a
    /// measurement (2026-08-23, twice).
    #[test]
    fn exposes_process_uptime_scheduling_lag_and_blocking_sections() {
        crate::observability::mark_process_start();
        drop(crate::observability::BlockWatch::new("test_section"));
        drop(crate::observability::TimedSection::new("test_section"));

        let rows = snapshot_rows(&StatsTableProvider::new(None));
        for key in ["uptime_seconds", "scheduling_lag_ms", "scheduling_lag_max_ms", "worker_threads"] {
            assert_has(&rows, "runtime", key);
        }
        // Same section name under both kinds must stay two distinct rows: one
        // claims worker occupancy, the other only wall time.
        for component in ["block", "section"] {
            for key in ["test_section.count", "test_section.total_ms", "test_section.max_ms", "test_section.avg_us"] {
                assert_has(&rows, component, key);
            }
        }
    }

    /// Every counter `scan_metrics!` declares must reach `timefusion_stats`.
    ///
    /// `rows`/`reasons` entries are rendered FROM the declaration, so for those
    /// this only re-proves what the macro already makes structural. What it
    /// actually pins is `derived`: counters that reach stats through a row
    /// pg_compat computes by hand (an average, a percentile), the one remaining
    /// way a declared counter can go unexposed.
    ///
    /// The bug it retires: the 2026-08-23 prefilter split renamed four reasons
    /// and the readout kept asking for the retired string, so `map_or(0, …)`
    /// rendered a confident 0 — 53% of prod skips unattributable on 2026-08-24.
    #[test]
    fn every_declared_scan_metric_has_a_row() {
        let rows = snapshot_rows(&StatsTableProvider::new(None).with_scan_metrics(Arc::new(ScanMetrics::default())));
        let exposed = rows.iter().filter(|(component, key, _)| component == "scan" && key.starts_with("prefilter_skipped_")).count();
        assert_eq!(
            exposed,
            crate::database::scan_metric_names::PREFILTER_SKIP_REASONS.len(),
            "{} skip reasons are registered but {exposed} rows are exposed — a reason that has no row cannot be attributed",
            crate::database::scan_metric_names::PREFILTER_SKIP_REASONS.len()
        );
    }

    /// Every unverifiable-slice bucket must reach `timefusion_stats`, and no row
    /// may exist that no bucket produces.
    ///
    /// Set equality, not a count: a bucket renamed without the readout following
    /// leaves a row that reads a confident 0 forever, which is exactly how
    /// `no_index_or_cap` hid 81% of prefilter skips. The rows are derived from
    /// `ALL`, so this passes by construction today — it is here to fail the day
    /// someone re-introduces a hand-maintained key list.
    #[test]
    fn every_unverifiable_bucket_is_exposed() {
        use crate::database::rollup_unverifiable::{UnverifiableFate, UnverifiableReason};
        let expected: std::collections::BTreeSet<String> = ["total".to_owned(), "identity_tag_incomplete_total".to_owned()]
            .into_iter()
            .chain(UnverifiableReason::ALL.iter().map(|reason| reason.as_str().to_owned()))
            .chain(UnverifiableFate::ALL.iter().map(|fate| fate.as_str().to_owned()))
            .map(|suffix| format!("rollup_unverifiable_{suffix}"))
            .collect();
        let rows = snapshot_rows(&StatsTableProvider::new(None));
        let exposed: std::collections::BTreeSet<String> = rows
            .iter()
            .filter(|(component, key, _)| component == "maintenance" && key.starts_with("rollup_unverifiable_"))
            .map(|(_, key, _)| key.clone())
            .collect();
        assert_eq!(exposed, expected, "the exposed rows and the bucket lists must be the same set — an unexposed bucket cannot be attributed");
    }

    #[test]
    fn exposes_dml_retry_outcomes() {
        use crate::database::scan_metric_names::{SCAN_DERIVED_ROWS, SCAN_ROWS};
        let rows = snapshot_rows(&StatsTableProvider::new(None).with_scan_metrics(Arc::new(ScanMetrics::default())));

        for (component, key) in SCAN_ROWS.iter().map(|(c, k, _)| (*c, *k)).chain(SCAN_DERIVED_ROWS.iter().copied()) {
            assert_has(&rows, component, key);
        }
    }

    /// `parquet` rows come from deltalake's global snapshot struct, not from a
    /// declaration this crate owns — the only counters here still exposed by hand.
    #[test]
    fn exposes_parquet_io_counters() {
        let rows = snapshot_rows(&StatsTableProvider::new(None).with_scan_metrics(Arc::new(ScanMetrics::default())));
        for key in ["metadata_cache_hits", "bytes_read"] {
            assert_has(&rows, "parquet", key);
        }
    }
}
