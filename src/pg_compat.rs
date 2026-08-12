use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    catalog::{MemTable, TableFunctionImpl, TableProvider},
    common::ToDFSchema,
    error::{DataFusionError, Result as DFResult},
    execution::context::SessionContext,
    logical_expr::{
        ColumnarValue, Expr, LogicalPlan, ScalarFunctionArgs, ScalarFunctionImplementation, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
        create_udf,
    },
    scalar::ScalarValue,
    sql::sqlparser::ast::Statement,
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
    Ok(())
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
    max_statement_secs: u64,
}

impl PgCompatibilityHook {
    pub fn new(max_statement_secs: u64) -> Self {
        Self { max_statement_secs }
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
        self.show(statement, client).map(|(name, value)| show_response(&name, &value).map(Response::Query))
    }

    async fn handle_extended_parse_query(
        &self, statement: &Statement, _session_context: &SessionContext, _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        self.show(statement, _client).map(|_| show_plan())
    }

    async fn handle_extended_query(
        &self, statement: Option<&Statement>, _logical_plan: &LogicalPlan, _params: &datafusion::common::ParamValues, _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        statement.and_then(|statement| self.show(statement, client)).map(|(name, value)| show_response(&name, &value).map(Response::Query))
    }
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
