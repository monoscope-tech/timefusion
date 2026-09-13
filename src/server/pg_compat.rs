use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray},
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
            results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag},
        },
        error::{PgWireError, PgWireResult},
    },
};
use futures::stream;
use itertools::Itertools;

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
    catalog.register_schema("pg_catalog", Arc::new(PgCatalogOverlay { inner, extra })).map(|_| ())
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
/// not ship. It hardcodes its table list and has no `register_table`, so
/// wrapping is the only way to extend it.
#[derive(Debug)]
struct PgCatalogOverlay {
    inner: Arc<dyn SchemaProvider>,
    extra: HashMap<&'static str, Arc<dyn TableProvider>>,
}

#[async_trait]
impl SchemaProvider for PgCatalogOverlay {
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names().into_iter().chain(self.extra.keys().map(ToString::to_string)).collect()
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

/// The cap this statement runs under: `min(client, ceiling)`. The ceiling rises
/// from `max_statement_secs` to `batch_statement_secs` only for a session that
/// explicitly asks for a longer timeout; silence keeps the interactive cap.
pub fn effective_statement_timeout(client_timeout: Option<Duration>, max_statement_secs: u64, batch_statement_secs: u64) -> Option<Duration> {
    let client_timeout = client_timeout.filter(|timeout| !timeout.is_zero());
    let ceiling = max_statement_secs.max(if client_timeout.is_some() { batch_statement_secs } else { 0 });
    let server_timeout = (ceiling != 0).then(|| Duration::from_secs(ceiling));
    [client_timeout, server_timeout].into_iter().flatten().min()
}

/// The configured batch ceiling, or 0 (no raise) before the config singleton is
/// initialised.
pub fn batch_statement_secs() -> u64 {
    crate::config::try_config().map_or(0, |config| config.core.timefusion_pgwire_batch_statement_secs)
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

impl RoleField {
    fn pg_type(&self) -> Type {
        match self {
            Self::Oid(_) => Type::INT4,
            Self::Text(_) => Type::VARCHAR,
            Self::Bool(_) => Type::BOOL,
        }
    }

    fn literal(&self) -> Expr {
        match self {
            Self::Oid(oid) => lit(*oid),
            Self::Text(text) => lit(text.as_str()),
            Self::Bool(flag) => lit(*flag),
        }
    }

    fn encode(&self, encoder: &mut DataRowEncoder) -> PgWireResult<()> {
        match self {
            Self::Oid(oid) => encoder.encode_field(&Some(*oid)),
            Self::Text(text) => encoder.encode_field(&Some(text.as_str())),
            Self::Bool(flag) => encoder.encode_field(&Some(*flag)),
        }
    }
}

impl PgCompatibilityHook {
    pub fn new(role: impl Into<String>, max_statement_secs: u64) -> Self {
        Self { role: role.into(), max_statement_secs }
    }

    /// pgAdmin's connect-time role probe, answered here because DataFusion
    /// cannot plan its array-subquery/recursive-CTE form at all.
    ///
    /// An unrecognised alias bails to the planner on purpose, so a new pgAdmin
    /// column fails loudly instead of being served a fabricated value.
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
        aliases.into_iter().map(|alias| self.role_field(&alias).map(|field| (alias, field))).collect()
    }

    fn role_field(&self, alias: &str) -> Option<RoleField> {
        Some(match alias {
            // The pg_catalog crate reports this role with oid 0.
            "id" => RoleField::Oid(0),
            "name" => RoleField::Text(self.role.clone()),
            // TF authenticates a single superuser, so all of these are true.
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
            // behind this hook returns the session's actual value; a constant
            // here would report `public` to clients that switched schema.
            "statement_timeout" => effective_statement_timeout(client_statement_timeout(client), self.max_statement_secs, batch_statement_secs())
                .map_or_else(|| "0".to_string(), |timeout| format!("{}ms", timeout.as_millis())),
            _ => return None,
        };
        Some((name, value))
    }
}

#[async_trait]
impl QueryHook for PgCompatibilityHook {
    async fn handle_simple_query(
        &self, statement: &Statement, session_context: &SessionContext, client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if let Some(result) = deallocate_all(statement, session_context, client).await {
            return Some(result);
        }
        if let Some(fields) = self.role_probe(statement) {
            return Some(role_probe_response(&fields).map(Response::Query));
        }
        self.show(statement, client).map(|(name, value)| show_response(&name, &value).map(Response::Query))
    }

    async fn handle_extended_parse_query(
        &self, statement: &Statement, _session_context: &SessionContext, client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        if let Some(fields) = self.role_probe(statement) {
            return Some(role_probe_plan(statement, &fields));
        }
        self.show(statement, client).map(|_| show_plan())
    }

    async fn handle_extended_query(
        &self, statement: Option<&Statement>, _logical_plan: &LogicalPlan, _params: &datafusion::common::ParamValues, session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if let Some(statement) = statement
            && let Some(result) = deallocate_all(statement, session_context, client).await
        {
            return Some(result);
        }
        // Deliberately NOT intercepted for the role probe: only the normal
        // executor encodes the row in the result format the client asked for;
        // a hand-built Response is always text, undecodable for binary int4/bool.
        statement.and_then(|statement| self.show(statement, client)).map(|(name, value)| show_response(&name, &value).map(Response::Query))
    }
}

/// SQL and wire-protocol prepares have separate stores. Clear both on this
/// connection, preserving session settings and already-bound portals.
async fn deallocate_all(statement: &Statement, session_context: &SessionContext, client: &mut dyn HookClient) -> Option<PgWireResult<Response>> {
    let Statement::Deallocate { name, .. } = statement else {
        return None;
    };
    if name.quote_style.is_some() || !name.value.eq_ignore_ascii_case("all") {
        return None;
    }
    Some(match session_context.sql(&statement.to_string()).await {
        Ok(_) => {
            client.portal_store().clear_statements();
            Ok(Response::Execution(Tag::new("DEALLOCATE")))
        }
        Err(error) => Err(PgWireError::ApiError(Box::new(error))),
    })
}

fn is_pg_roles(relation: &TableFactor) -> bool {
    matches!(relation, TableFactor::Table { name, .. } if name.to_string().to_ascii_lowercase().ends_with("pg_roles"))
}

fn role_probe_response(fields: &[(String, RoleField)]) -> PgWireResult<QueryResponse> {
    let infos = Arc::new(fields.iter().map(|(name, value)| FieldInfo::new(name.clone(), None, None, value.pg_type(), FieldFormat::Text)).collect::<Vec<_>>());
    let row = {
        let mut encoder = DataRowEncoder::new(Arc::clone(&infos));
        fields.iter().try_for_each(|(_, value)| value.encode(&mut encoder))?;
        encoder.take_row()
    };
    Ok(QueryResponse::new(infos, stream::once(async move { Ok(row) })))
}

/// A tautology over every `$n` the original statement bound, so the substitute
/// plan declares the same parameters and Bind is not rejected. Utf8 because that
/// is what pgAdmin sends; the predicate is always true, so values are unused.
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

/// A plan that *produces* the row, not an empty relation shaped like it: only
/// the real executor encodes columns in the client's requested result format.
fn role_probe_plan(statement: &Statement, fields: &[(String, RoleField)]) -> PgWireResult<LogicalPlan> {
    let projection = fields.iter().map(|(name, value)| value.literal().alias(name)).collect::<Vec<_>>();
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
    role_probe_response(&[(name.to_string(), RoleField::Text(value.to_string()))])
}

fn register_identity_udfs(ctx: &SessionContext, role: &str, max_statement_secs: u64) {
    let version = format!("PostgreSQL {PG_COMPAT_VERSION} (TimeFusion {}) on {}-{}", env!("CARGO_PKG_VERSION"), std::env::consts::ARCH, std::env::consts::OS);
    for udf in [
        constant_udf("current_database", PG_COMPAT_DATABASE.into()),
        constant_udf("session_user", role.into()),
        constant_udf("version", version.into()),
        // pgAdmin checks replica status on connect; TF is never a standby.
        constant_udf("pg_is_in_recovery", false.into()),
        ScalarUDF::from(CurrentSettingUdf::new(max_statement_secs)),
    ] {
        ctx.register_udf(udf);
    }
    ctx.register_udtf("pg_show_all_settings", Arc::new(PgShowAllSettingsFunction { max_statement_secs }));
}

/// Zero-argument UDF answering a fixed value; the return type is the value's own.
fn constant_udf(name: &str, value: ScalarValue) -> ScalarUDF {
    let data_type = value.data_type();
    let function: ScalarFunctionImplementation = Arc::new(move |_| Ok(ColumnarValue::Scalar(value.clone())));
    create_udf(name, vec![], data_type, Volatility::Stable, function)
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
        let values = (0..names.len())
            .map(|index| {
                let name = (!names.is_null(index))
                    .then(|| ScalarValue::try_from_array(&names, index))
                    .transpose()?
                    .and_then(|value| crate::read::optimizers::extract_utf8_string(&value));
                match name.and_then(|name| compatibility_setting(&name, self.max_statement_secs)) {
                    Some(value) => Ok(Some(value)),
                    None if missing_ok.is_some_and(|values| values.value(index)) => Ok(None),
                    None => Err(DataFusionError::Execution("unrecognized configuration parameter".to_string())),
                }
            })
            .collect::<DFResult<Vec<_>>>()?;
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
    }
}

/// Every setting `current_setting()` answers — also the row set of
/// `pg_show_all_settings()`, so the two can never disagree.
const COMPATIBILITY_SETTING_NAMES: &str = "server_version,server_version_num,search_path,is_superuser,standard_conforming_strings,client_encoding,\
                                           timezone,datestyle,intervalstyle,statement_timeout,bytea_output,client_min_messages,integer_datetimes,\
                                           default_transaction_read_only,in_hot_standby";

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

/// The set-returning function `pg_settings` is a view over. pgAdmin calls it
/// directly on connect; the upstream pg_catalog crate only ships the view.
#[derive(Debug)]
struct PgShowAllSettingsFunction {
    max_statement_secs: u64,
}

impl PgShowAllSettingsFunction {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(
            "name,setting,unit,category,short_desc,extra_desc,context,vartype,source,min_val,max_val,enumvals,boot_val,reset_val,sourcefile"
                .split(',')
                .map(|name| Field::new(name, DataType::Utf8, true))
                .chain([Field::new("sourceline", DataType::Int32, true), Field::new("pending_restart", DataType::Boolean, true)])
                .collect::<Vec<_>>(),
        ))
    }

    fn batch(&self) -> DFResult<RecordBatch> {
        let rows: Vec<(&str, String)> =
            COMPATIBILITY_SETTING_NAMES.split(',').filter_map(|name| compatibility_setting(name, self.max_statement_secs).map(|value| (name, value))).collect();
        let n = rows.len();
        let strings = |values: Vec<Option<String>>| Arc::new(StringArray::from(values)) as ArrayRef;
        let repeat = |value: &str| strings(vec![Some(value.to_string()); n]);
        let nulls = strings(vec![None; n]);
        let settings = strings(rows.iter().map(|(_, value)| Some(value.clone())).collect());
        let columns = vec![
            strings(rows.iter().map(|(name, _)| Some((*name).to_string())).collect()),
            Arc::clone(&settings),
            Arc::clone(&nulls),
            Arc::clone(&nulls),
            Arc::clone(&nulls),
            Arc::clone(&nulls),
            repeat("user"),
            strings(rows.iter().map(|(_, value)| Some(if matches!(value.as_str(), "on" | "off") { "bool" } else { "string" }.to_string())).collect()),
            repeat("default"),
            Arc::clone(&nulls),
            Arc::clone(&nulls),
            Arc::clone(&nulls),
            Arc::clone(&settings),
            settings,
            nulls,
            Arc::new(Int32Array::from(vec![None::<i32>; n])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![Some(false); n])) as ArrayRef,
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

    /// Runs `sql` on a session wired the way pgwire wires one, as user `operator`.
    async fn query(sql: &str) -> RecordBatch {
        let ctx = SessionContext::new();
        register_identity_udfs(&ctx, "operator", 60);
        ctx.sql(sql).await.unwrap().collect().await.unwrap().remove(0)
    }

    fn text(batch: &RecordBatch, col: usize) -> &str {
        batch.column(col).as_string::<i32>().value(0)
    }

    /// A session raises the cap only by asking, never past the batch ceiling; a
    /// silent session keeps the interactive cap, and a batch ceiling below the
    /// interactive cap cannot lower it.
    #[test_case::test_case(None, 60, 0 => Some(60) ; "server cap applies when the client is silent")]
    #[test_case::test_case(Some(0), 60, 0 => Some(60) ; "a zero client timeout means unlimited, so the server caps")]
    #[test_case::test_case(Some(5), 60, 0 => Some(5) ; "the smaller client value wins")]
    #[test_case::test_case(Some(120), 60, 0 => Some(60) ; "the server caps a larger ask")]
    #[test_case::test_case(None, 0, 0 => None ; "no cap configured anywhere")]
    #[test_case::test_case(None, 60, 600 => Some(60) ; "silence must not raise anything")]
    #[test_case::test_case(Some(300), 60, 600 => Some(300) ; "asking within the batch ceiling is granted")]
    #[test_case::test_case(Some(900), 60, 600 => Some(600) ; "the batch ceiling still bounds the ask")]
    #[test_case::test_case(Some(5), 60, 600 => Some(5) ; "asking for less still gets less")]
    #[test_case::test_case(Some(120), 60, 30 => Some(60) ; "a batch ceiling under the interactive cap cannot lower it")]
    fn effective_timeout_is_the_smaller_nonzero_cap(client_secs: Option<u64>, max_statement_secs: u64, batch_statement_secs: u64) -> Option<u64> {
        effective_statement_timeout(client_secs.map(Duration::from_secs), max_statement_secs, batch_statement_secs).map(|timeout| timeout.as_secs())
    }

    #[test_case::test_case("TimeZone", 60 => Some("UTC".to_string()) ; "a mixed-case name still resolves")]
    #[test_case::test_case("timezone", 60 => Some("UTC".to_string()) ; "the canonical lowercase spelling resolves")]
    #[test_case::test_case("statement_timeout", 60 => Some("60000".to_string()) ; "statement_timeout is reported in millis")]
    #[test_case::test_case("unknown", 60 => None ; "an unlisted name resolves to nothing")]
    fn compatibility_settings_are_case_insensitive(name: &str, max_statement_secs: u64) -> Option<String> {
        compatibility_setting(name, max_statement_secs)
    }

    #[test]
    fn startup_parameters_report_postgres_16_6() {
        let client = datafusion_postgres::testing::MockClient::new();
        let parameters = TimeFusionServerParameterProvider::default().server_parameters(&client).unwrap();
        assert_eq!(parameters.get("server_version").map(String::as_str), Some(PG_COMPAT_VERSION));
        assert_eq!(parameters.get("server_version_num").map(String::as_str), Some(PG_COMPAT_VERSION_NUM));
    }

    /// pgAdmin cannot connect if this regresses: upstream only ships the view.
    #[tokio::test]
    async fn pg_show_all_settings_is_callable_as_a_table_function() {
        let one = query("SELECT setting FROM pg_show_all_settings() WHERE name = 'server_version'").await;
        assert_eq!(text(&one, 0), PG_COMPAT_VERSION);

        let all = query("SELECT * FROM pg_show_all_settings()").await;
        assert_eq!(all.num_rows(), COMPATIBILITY_SETTING_NAMES.split(',').count());
        assert_eq!(all.num_columns(), 17);
    }

    /// pgAdmin polls these on a timer; a missing one is a planning error on
    /// every refresh.
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

    /// Every name in the row set must resolve, or the table function silently
    /// drops it while `current_setting()` still errors on it.
    #[tokio::test]
    async fn identity_and_setting_udfs_match_the_compatibility_contract() {
        for name in COMPATIBILITY_SETTING_NAMES.split(',') {
            assert!(compatibility_setting(name, 60).is_some(), "{name} is listed but unresolvable");
        }
        let batch = query(
            "SELECT current_database(), session_user, current_setting('server_version'), current_setting('unknown', true), current_setting('bytea_output'), \
             pg_is_in_recovery()",
        )
        .await;
        assert_eq!(text(&batch, 0), PG_COMPAT_DATABASE);
        assert_eq!(text(&batch, 1), "operator");
        assert_eq!(text(&batch, 2), PG_COMPAT_VERSION);
        assert!(batch.column(3).is_null(0));
        assert_eq!(text(&batch, 4), "hex");
        assert!(!batch.column(5).as_boolean().value(0));
    }
}

// `timefusion_stats` — a flat (component, key, value) introspection table over
// write-layer, buffer, WAL, scan, cache and runtime internals.

use std::sync::atomic::Ordering::Relaxed;

use datafusion::{catalog::Session, datasource::TableType, physical_plan::ExecutionPlan};

use crate::{database::ScanMetrics, observability::arrow_err, storage::FoyerRuntimeStats, write::BufferedWriteLayer};

/// (fast-resolve cache entries, provider cache entries) at scan time.
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

/// Integer percent, 0 for an unwired pool — the `(0, 0)` snapshot convention.
fn share_pct(used: usize, size: usize) -> usize {
    (used * 100).checked_div(size).unwrap_or(0)
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
    /// Live maintenance + coordinator pool usage.
    maintenance_pool: Option<PoolSnapshot>,
    coordinator_pool: Option<PoolSnapshot>,
    logical_count: Option<LogicalCountSnapshot>,
    tantivy_search: Option<Arc<crate::tantivy::search::TantivySearchService>>,
    bloom_prune: Option<Arc<crate::read::bloom_prune::BloomPruneRegistry>>,
    schema: SchemaRef,
}

impl StatsTableProvider {
    pub fn new(layer: Option<Arc<BufferedWriteLayer>>) -> Self {
        let schema = Arc::new(Schema::new(["component", "key", "value"].map(|name| Field::new(name, DataType::Utf8, false)).to_vec()));
        Self {
            layer,
            scan_metrics: None,
            cache_sizes: None,
            foyer_stats: None,
            query_pool: None,
            maintenance_pool: None,
            coordinator_pool: None,
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

    pub fn with_maintenance_pools(self, maintenance: PoolSnapshot, coordinator: PoolSnapshot) -> Self {
        Self { maintenance_pool: Some(maintenance), coordinator_pool: Some(coordinator), ..self }
    }

    pub fn with_logical_count(self, f: LogicalCountSnapshot) -> Self {
        Self { logical_count: Some(f), ..self }
    }

    /// Absent service ⇒ the `tantivy` component is omitted entirely, rather
    /// than reporting zeros that look like an idle index.
    pub fn with_tantivy_search_opt(self, s: Option<Arc<crate::tantivy::search::TantivySearchService>>) -> Self {
        Self { tantivy_search: s, ..self }
    }

    /// Same posture as tantivy: absent registry ⇒ no `bloom_prune` component.
    pub fn with_bloom_prune_opt(self, r: Option<Arc<crate::read::bloom_prune::BloomPruneRegistry>>) -> Self {
        Self { bloom_prune: r, ..self }
    }

    fn snapshot_batch(&self) -> DFResult<RecordBatch> {
        // Boot memory budget. `slack_mb` absorbs allocation no budget tracks
        // (parquet decode, walrus mmaps, tantivy, allocator overhead).
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
                        // Replay DML consumed without applying (table already flushed).
                        "replay_dml_noops_total" => s.mem_replay_dml_noops,
                        // `_approx`: the running total is not adjusted when buckets
                        // coalesce, so it drifts high. Capacity alerting only.
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
                        // Ingest-vs-drain; `rows_in_buffer_lag` ≈ rows currently buffered.
                        "rows_ingested_total" => s.rows_ingested_total,
                        "rows_flushed_total" => s.rows_flushed_total,
                        "rows_in_buffer_lag" => s.rows_ingested_total.saturating_sub(s.rows_flushed_total),
                        // Drain effectiveness: flat under pressure ⇒ the flush path is
                        // draining empty buckets.
                        "flush_freed_bytes_total" => s.flush_freed_bytes_total,
                        // Real RSS, to check `estimated_bytes_approx` against.
                        "process_rss_bytes" => or_null(s.process_rss_bytes),
                        "process_rss_mb" => or_null(s.process_rss_bytes.map(|v| mb(v as f64))),
                        // Failed-commit rows living ONLY in the WAL, each pinning the
                        // WAL GC floor. Alert on >0; remedy = restart.
                        "orphaned_topics" => s.orphaned_topics,
                        "orphan_pin_age_secs" => or_null(s.orphan_pin_age_secs),
                        "drained" => s.drained,
                        "boot_micros" => s.boot_micros,
                    ],
                    rows!["wal";
                        "recovery_complete" => s.wal_recovery_complete,
                        "recovery_duration_ms" => s.wal_recovery_duration_ms,
                        // Rows this boot re-inserted. Replay is not idempotent, so a
                        // restart re-adds rows already in Delta and dedup removes them.
                        "replay_rows" => s.wal_replay_rows,
                        // Flushes declined because the rows were already in Delta.
                        "landed_skips" => s.landed_skips_total,
                        "landed_skipped_rows" => s.landed_skipped_rows_total,
                    ],
                    rows!["tantivy"; "recovery_pending_files" => s.tantivy_recovery_pending_files],
                    rows!["wal";
                        "files" => s.wal_files,
                        "disk_bytes" => s.wal_disk_bytes,
                        "disk_mb" => mb(s.wal_disk_bytes as f64),
                        // Parked payloads, invisible to wal_disk_bytes. Alert if > 0.
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

        let dml = atomic_rows(crate::observability::dml_stats().stats_rows());

        // Nonzero = a scan advertised an ordering its data does not honour (a
        // parquet footer's `sorting_columns` is lying). Drives hot-tail repair.
        let read_dedup = rows![@atomic "read_dedup";
            "ordering_violations_total" => crate::read::ORDERING_VIOLATIONS,
            // Per-leg attribution, populated only under TIMEFUSION_ORDERING_PROBE.
            "ordering_violations_mem" => crate::read::ORDERING_VIOLATIONS_MEM,
            "ordering_violations_delta" => crate::read::ORDERING_VIOLATIONS_DELTA,
        ];

        let maintenance: Vec<Row> = atomic_rows(crate::observability::maintenance_stats().stats_rows())
            .into_iter()
            .chain([("maintenance", "retry_reason".to_owned(), crate::observability::maintenance_retry_reason())])
            // Derived from the bucket lists, never hand-listed: a reason added
            // without a row would be silently unattributable.
            .chain(crate::database::rollup_unverifiable::gauge_rows().map(|(key, value)| ("maintenance", key, value.to_string())))
            // Same shape: one row per (operation, retry reason) actually seen.
            .chain(
                crate::observability::maintenance_retry_rows()
                    .into_iter()
                    .chain(crate::observability::maintenance_work_rows())
                    .map(|(key, value)| ("maintenance", key, value.to_string())),
            )
            .collect();

        let plan_cache = crate::read::plan_cache::global().map_or_else(Vec::new, |pc| {
            let (hits, misses) = pc.counters();
            // Shape path = literal- and now()-bearing SELECTs, distinct from the
            // placeholder-`$N` counters above. shape_skips = not parameterizable.
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
            let (fr_hits, fr_misses) = (cv(FAST_RESOLVE_HITS), cv(FAST_RESOLVE_MISSES));
            let (cert_never, cert_moved) = (cv(DEDUP_DENIED_NEVER_CERTIFIED), cv(DEDUP_DENIED_FP_MOVED));
            let (pc_hits, pc_misses) = (cv(PROVIDER_CACHE_HITS), cv(PROVIDER_CACHE_MISSES));
            // `peak_batch_bytes x polls_inflight_peak` bounds worst-case concurrent
            // parquet decode heap. High-water marks are hand-rolled atomics because
            // `metrics::Gauge` has no `fetch_max`.
            let (dpeak, dinflight_peak) = (m.decode.decode_peak_batch_bytes.load(Relaxed), m.decode.decode_polls_inflight_peak.load(Relaxed));
            // The number the OOM killer acts on, live.
            let (used, limit) = (crate::database::process_memory_bytes().unwrap_or(0), crate::config::try_config().map_or(0, |c| c.derived.memory_limit_bytes));
            let (pool_used, pool_size) = self.query_pool.as_ref().map_or((0, 0), |f| f());
            // Unwired pools use the same (0, 0) convention as the query pool.
            let (mpool_used, mpool_size) = self.maintenance_pool.as_ref().map_or((0, 0), |f| f());
            let (cpool_used, cpool_size) = self.coordinator_pool.as_ref().map_or((0, 0), |f| f());
            [
                rows!["memory";
                    "charged_bytes" => used,
                    "limit_bytes" => limit,
                    "charged_pct" => share_pct(used, limit),
                    // Saturation here surfaces as "Resources exhausted" query errors.
                    "query_pool_used_bytes" => pool_used,
                    "query_pool_pct" => share_pct(pool_used, pool_size),
                    "maintenance_pool_used_bytes" => mpool_used,
                    "maintenance_pool_pct" => share_pct(mpool_used, mpool_size),
                    "coordinator_pool_used_bytes" => cpool_used,
                    "coordinator_pool_pct" => share_pct(cpool_used, cpool_size),
                ],
                rows!["scan_decode";
                    "peak_batch_bytes" => dpeak,
                    "polls_inflight" => m.decode.decode_polls_inflight.load(Relaxed),
                    "polls_inflight_peak" => dinflight_peak,
                    "worst_case_heap_mb" => mb(dpeak.saturating_mul(dinflight_peak) as f64),
                ],
                rows!["scan";
                    "skipped_delta_pct" => pct(cv(SCANS_SKIPPED_DELTA), cv(SCANS_TOTAL)),
                    "dedup_skipped_pct" => pct(cv(DEDUP_SKIPPED), cv(DEDUP_ELIGIBLE_SCANS)),
                    "rollup_stale_moved" => cv(ROLLUP_STALE_SHRANK) + cv(ROLLUP_STALE_GREW),
                    "dedup_denied_never_certified_pct" => pct(cert_never, cert_never + cert_moved),
                    "cert_dwell_secs_avg" => avg(cv(CERT_DWELL_SECS_TOTAL), cv(CERT_DWELL_TOTAL)),
                    "cert_dwell_p50_secs" => m.cert_dwell_percentile_secs(0.50),
                    "cert_dwell_p90_secs" => m.cert_dwell_percentile_secs(0.90),
                    "fast_resolve_hit_pct" => pct(fr_hits, fr_hits + fr_misses),
                    "provider_cache_hit_pct" => pct(pc_hits, pc_hits + pc_misses),
                    "provider_build_us_avg" => avg(cv(PROVIDER_BUILD_US_TOTAL), cv(PROVIDER_BUILD_TOTAL)),
                    "provider_scan_us_avg" => avg(cv(PROVIDER_SCAN_US_TOTAL), cv(PROVIDER_SCAN_TOTAL)),
                    "dedup_full_set_pct" => pct(cv(DEDUP_FULL_SET_TOTAL), cv(DEDUP_BOUNDED_TOTAL) + cv(DEDUP_FULL_SET_TOTAL)),
                    // TIMEFUSION_WIDE_SCAN_REFUSE_MB must sit above p99 or it rejects
                    // working dashboards.
                    "wide_scan_selected_mb_p50" => q(WIDE_SCAN_SELECTED_MB, 0.50),
                    "wide_scan_selected_mb_p90" => q(WIDE_SCAN_SELECTED_MB, 0.90),
                    "wide_scan_selected_mb_p99" => q(WIDE_SCAN_SELECTED_MB, 0.99),
                    "mem_plan_us_avg" => avg(cv(MEM_PLAN_US_TOTAL), cv(MEM_PLAN_TOTAL)),
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
                    // Admission accounting; `write_capture` is invisible to the
                    // read-side counters above.
                    "admit_write_capture_bytes" => s.admit_write_capture_bytes,
                    "admit_read_miss_bytes" => s.admit_read_miss_bytes,
                    "admit_refresh_bytes" => s.admit_refresh_bytes,
                    "write_capture_admitted" => s.write_capture_admitted,
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
            // These caches don't evict; size tracks unique (project, table) pairs
            // since process start.
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

        // Tantivy read path: per-phase means plus the fan-out (`indexes_per_query`)
        // that dominates them.
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
                "histogram_snapshots" => s.histogram_snapshots.load(Relaxed),
                "histogram_delta_cache_hits" => s.histogram_delta_cache_hits.load(Relaxed),
                "histogram_parquet_prepares" => s.histogram_parquet_prepares.load(Relaxed),
                "histogram_unique_partitions" => s.histogram_unique_partitions.load(Relaxed),
                "manifest_hit_pct" => pct(mh, mh + ml),
                "manifest_load_us_avg" => mean(&s.manifest_load_us, &s.manifest_loads),
                // Every blob fetch is an S3 round trip on the planning path; should
                // trend to ~0 for the hot window once seeding is working.
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
                // Raw monotonic microseconds: each `*_us_avg` above divides by its
                // own denominator, so avg*count mixes denominators and cannot be
                // differenced. `search_us_total` is occupancy, not wall clock —
                // per-index searches run `search_concurrency`-way.
                "manifest_load_us_total" => s.manifest_load_us.load(Relaxed),
                "blob_fetch_us_total" => s.blob_fetch_us.load(Relaxed),
                "index_open_us_total" => s.index_open_us.load(Relaxed),
                "search_us_total" => s.search_us.load(Relaxed),
                // Time the four above cannot see: fanout_us minus prepare_us minus
                // search_us is the result-merge bookkeeping.
                "plan_us_total" => s.plan_us.load(Relaxed),
                "prepare_us_total" => s.prepare_us.load(Relaxed),
                "prepares" => s.prepares.load(Relaxed),
                "fanout_us_total" => s.fanout_us.load(Relaxed),
            ]
        });

        // File-level needle pruning (bloom sidecars). `files_rejected` /
        // `files_probed` is the pruning rate.
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

        // Every other counter here is process-scoped, so `uptime_seconds` is what
        // makes them quotable. `scheduling_lag_ms` nonzero with idle cores means
        // workers are blocked, not busy — read it with the `block` rows below.
        let (lag_last, lag_max) = crate::observability::runtime_lag_ms();
        let runtime = rows!["runtime";
            "uptime_seconds" => crate::observability::process_uptime_secs(),
            "scheduling_lag_ms" => lag_last,
            "scheduling_lag_max_ms" => lag_max,
            "worker_threads" => std::thread::available_parallelism().map_or(0, std::num::NonZeroUsize::get),
        ];
        // Empty unless jemalloc is the allocator. `frag_pct` is the share of
        // resident memory backing no live allocation.
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
        let block: Vec<Row> = crate::observability::section_stats()
            .into_iter()
            .sorted_unstable_by_key(|section| section.0)
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
        let col = |field: fn(&Row) -> &str| Arc::new(rows.iter().map(|row| Some(field(row))).collect::<StringArray>()) as ArrayRef;
        RecordBatch::try_new(Arc::clone(&self.schema), vec![col(|r| r.0), col(|r| r.1.as_str()), col(|r| r.2.as_str())]).map_err(arrow_err)
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

    /// The `component` rows whose key carries `prefix` — the shape both registry-completeness tests compare against their registry.
    fn exposed_keys(rows: &[OwnedRow], component: &str, prefix: &str) -> std::collections::BTreeSet<String> {
        rows.iter().filter(|(c, k, _)| c == component && k.starts_with(prefix)).map(|(_, k, _)| k.clone()).collect()
    }

    /// Every optional wiring at once. `with_scan_metrics` is required: the `memory`
    /// component is emitted inside that branch, so an unwired scan hides every pool row too.
    fn fully_wired_rows() -> Vec<OwnedRow> {
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
        snapshot_rows(
            &StatsTableProvider::new(None)
                .with_scan_metrics(Arc::new(ScanMetrics::default()))
                .with_foyer_stats(Arc::new(move || snapshot.clone()))
                .with_maintenance_pools(Arc::new(|| (25, 100)), Arc::new(|| (3, 4)))
                .with_logical_count(Arc::new(|| (3, 42, 100, 1))),
        )
    }

    /// One snapshot, every wired component: the rows a `timefusion_stats` query must
    /// carry, plus the handful whose VALUE is the point — a pool pct divides used by the size it was handed.
    #[test]
    fn exposes_every_wired_component_foyer_pools_logical_count_runtime_scan_and_parquet() {
        use crate::database::scan_metric_names::{SCAN_DERIVED_ROWS, SCAN_ROWS};
        crate::observability::mark_process_start();
        drop(crate::observability::BlockWatch::new("test_section"));
        drop(crate::observability::TimedSection::new("test_section"));

        let rows = fully_wired_rows();
        // `keys` is a space-separated list; each one must own a row.
        let expect = |component: &str, keys: &str| {
            for key in keys.split_whitespace() {
                assert!(rows.iter().any(|(c, k, _)| c == component && k == key), "missing {component}.{key}");
            }
        };

        expect(
            "foyer",
            "memory_mb disk_gb ttl_seconds l1_max_entry_mb block_size_mb cache_recent_days cache_dir metadata_memory_mb metadata_disk_gb l1_used_bytes \
             l2_used_bytes entry_count evictions",
        );
        expect("memory", "maintenance_pool_used_bytes maintenance_pool_pct coordinator_pool_used_bytes coordinator_pool_pct");
        expect("logical_count", "resident_partitions resident_bytes_estimated resident_mb_estimated resident_limit_bytes resident_limit_mb active_builds");
        expect("runtime", "uptime_seconds scheduling_lag_ms scheduling_lag_max_ms worker_threads");
        // Same section name under both kinds must stay two distinct rows: one
        // claims worker occupancy, the other only wall time.
        for component in ["block", "section"] {
            expect(component, "test_section.count test_section.total_ms test_section.max_ms test_section.avg_us");
        }
        expect("parquet", "metadata_cache_hits bytes_read");
        for (component, key) in SCAN_ROWS.iter().map(|(c, k, _)| (*c, *k)).chain(SCAN_DERIVED_ROWS.iter().copied()) {
            expect(component, key);
        }

        // Rows whose VALUE, not mere presence, is the assertion.
        for (component, key, value) in [
            ("foyer", "cache_dir", "/cache"),
            ("memory", "maintenance_pool_pct", "25"),
            ("memory", "coordinator_pool_pct", "75"),
            ("logical_count", "active_builds", "1"),
        ] {
            assert!(rows.contains(&(component.into(), key.into(), value.into())), "{component}.{key} must read {value}");
        }
    }

    /// Scan metrics wired and nothing else: an unwired pool reports 0 rather than dividing by zero, and every counter
    /// `scan_metrics!` declares reaches `timefusion_stats` — including the hand-computed `derived` rows, the one way a declared counter goes silently unexposed.
    #[test]
    fn unwired_pools_read_zero_and_every_declared_scan_metric_has_a_row() {
        let rows = snapshot_rows(&StatsTableProvider::new(None).with_scan_metrics(Arc::new(ScanMetrics::default())));
        assert!(rows.contains(&("memory".into(), "maintenance_pool_pct".into(), "0".into())), "an unwired pool reports 0, not a panic");

        let exposed = exposed_keys(&rows, "scan", "prefilter_skipped_").len();
        assert_eq!(
            exposed,
            crate::database::scan_metric_names::PREFILTER_SKIP_REASONS.len(),
            "{} skip reasons are registered but {exposed} rows are exposed — a reason that has no row cannot be attributed",
            crate::database::scan_metric_names::PREFILTER_SKIP_REASONS.len()
        );
    }

    /// Set equality (not a count) between the unverifiable-slice buckets and the exposed rows: it fails the day
    /// someone re-introduces a hand-maintained key list, which would leave rows reading a confident 0.
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
        let exposed = exposed_keys(&rows, "maintenance", "rollup_unverifiable_");
        assert_eq!(exposed, expected, "the exposed rows and the bucket lists must be the same set — an unexposed bucket cannot be attributed");
    }
}
