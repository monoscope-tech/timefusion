use std::str::FromStr;

use arrow::datatypes::FieldRef;
use arrow_schema::SchemaRef;
use delta_kernel::parquet::format::SortingColumn;
use deltalake::kernel::StructField;
use log::debug;
use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize};
use serde_with::serde_as;
use std::sync::OnceLock;

use crate::schema_loader::TableSchema;
use crate::load_schema;

static OTEL_SCHEMA: OnceLock<TableSchema> = OnceLock::new();

fn get_otel_schema() -> &'static TableSchema {
    OTEL_SCHEMA.get_or_init(|| load_schema!("../schemas/otel_logs_and_spans.yaml"))
}

#[allow(non_snake_case)]
#[serde_as]
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct OtelLogsAndSpans {
    #[serde(with = "chrono::serde::ts_microseconds")]
    pub timestamp: chrono::DateTime<chrono::Utc>,

    #[serde(with = "chrono::serde::ts_microseconds_option")]
    pub observed_timestamp: Option<chrono::DateTime<chrono::Utc>>,

    pub id: String,
    pub parent_id: Option<String>,
    pub hashes: Vec<String>, // all relevant hashes can be stored here for item identification
    pub name: Option<String>,
    pub kind: Option<String>, // logs, span, request
    pub status_code: Option<String>,
    pub status_message: Option<String>,

    // Logs specific
    pub level: Option<String>, // same as severity text

    // Severity
    pub severity: Option<String>, // severity as json

    pub severity___severity_text: Option<String>,
    pub severity___severity_number: Option<String>,

    pub body: Option<String>, // body as json json

    pub duration: Option<i64>, // nanoseconds

    #[serde(with = "chrono::serde::ts_microseconds_option")]
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(with = "chrono::serde::ts_microseconds_option")]
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,

    // Context
    pub context: Option<String>, // context as json
    //
    pub context___trace_id: Option<String>,
    pub context___span_id: Option<String>,
    pub context___trace_state: Option<String>,
    pub context___trace_flags: Option<String>,
    pub context___is_remote: Option<String>,

    // Events
    pub events: Option<String>, // events json

    // Links
    pub links: Option<String>, // links json

    // Attributes
    pub attributes: Option<String>, // attirbutes object as json
    // Server and client
    pub attributes___client___address: Option<String>,
    pub attributes___client___port: Option<i32>,
    pub attributes___server___address: Option<String>,
    pub attributes___server___port: Option<i32>,

    // network https://opentelemetry.io/docs/specs/semconv/attributes-registry/network/
    pub attributes___network___local__address: Option<String>,
    pub attributes___network___local__port: Option<i32>,
    pub attributes___network___peer___address: Option<String>,
    pub attributes___network___peer__port: Option<i32>,
    pub attributes___network___protocol___name: Option<String>,
    pub attributes___network___protocol___version: Option<String>,
    pub attributes___network___transport: Option<String>,
    pub attributes___network___type: Option<String>,

    // Source Code Attributes
    pub attributes___code___number: Option<i32>,
    pub attributes___code___file___path: Option<i32>,
    pub attributes___code___function___name: Option<i32>,
    pub attributes___code___line___number: Option<i32>,
    pub attributes___code___stacktrace: Option<i32>,

    // Log records. https://opentelemetry.io/docs/specs/semconv/general/logs/
    pub attributes___log__record___original: Option<String>,
    pub attributes___log__record___uid: Option<String>,

    // Exception https://opentelemetry.io/docs/specs/semconv/exceptions/exceptions-logs/
    pub attributes___error___type: Option<String>,
    pub attributes___exception___type: Option<String>,
    pub attributes___exception___message: Option<String>,
    pub attributes___exception___stacktrace: Option<String>,

    // URL https://opentelemetry.io/docs/specs/semconv/attributes-registry/url/
    pub attributes___url___fragment: Option<String>,
    pub attributes___url___full: Option<String>,
    pub attributes___url___path: Option<String>,
    pub attributes___url___query: Option<String>,
    pub attributes___url___scheme: Option<String>,

    // Useragent https://opentelemetry.io/docs/specs/semconv/attributes-registry/user-agent/
    pub attributes___user_agent___original: Option<String>,

    // HTTP https://opentelemetry.io/docs/specs/semconv/http/http-spans/
    pub attributes___http___request___method: Option<String>,
    pub attributes___http___request___method_original: Option<String>,
    pub attributes___http___response___status_code: Option<i32>,
    pub attributes___http___request___resend_count: Option<i32>,
    pub attributes___http___request___body___size: Option<i32>,

    // Session https://opentelemetry.io/docs/specs/semconv/general/session/
    pub attributes___session___id: Option<String>,
    pub attributes___session___previous___id: Option<String>,

    // Database https://opentelemetry.io/docs/specs/semconv/database/database-spans/
    pub attributes___db___system___name: Option<String>,
    pub attributes___db___collection___name: Option<String>,
    pub attributes___db___namespace: Option<String>,
    pub attributes___db___operation___name: Option<String>,
    pub attributes___db___response___status_code: Option<String>,
    pub attributes___db___operation___batch___size: Option<i32>,
    pub attributes___db___query___summary: Option<String>,
    pub attributes___db___query___text: Option<String>,

    // https://opentelemetry.io/docs/specs/semconv/attributes-registry/user/
    pub attributes___user___id: Option<String>,
    pub attributes___user___email: Option<String>,
    pub attributes___user___full_name: Option<String>,
    pub attributes___user___name: Option<String>,
    pub attributes___user___hash: Option<String>,

    // Resource
    pub resource: Option<String>, // resource as json

    // Resource Attributes (subset) https://opentelemetry.io/docs/specs/semconv/resource/
    pub resource___service___name: Option<String>,
    pub resource___service___version: Option<String>,
    pub resource___service___instance___id: Option<String>,
    pub resource___service___namespace: Option<String>,

    pub resource___telemetry___sdk___language: Option<String>,
    pub resource___telemetry___sdk___name: Option<String>,
    pub resource___telemetry___sdk___version: Option<String>,

    pub resource___user_agent___original: Option<String>,
    // Kept at the bottom to make delta-rs happy, so its schema matches datafusion.
    // Seems delta removes the partition ids from the normal schema and moves them to the end.
    // Top-level fields
    pub project_id: String,

    #[serde(default)]
    #[serde(deserialize_with = "default_on_empty_string")]
    pub date: chrono::NaiveDate,
}

impl OtelLogsAndSpans {
    pub fn table_name() -> String {
        get_otel_schema().table_name.clone()
    }

    #[allow(dead_code)]
    pub fn fields() -> anyhow::Result<Vec<FieldRef>> {
        get_otel_schema().fields()
    }

    pub fn columns() -> anyhow::Result<Vec<StructField>> {
        let columns = get_otel_schema().columns()?;
        debug!("schema_field columns {:?}", columns);
        Ok(columns)
    }

    pub fn schema_ref() -> SchemaRef {
        get_otel_schema().schema_ref()
    }

    pub fn partitions() -> Vec<String> {
        get_otel_schema().partitions.clone()
    }

    pub fn sorting_columns() -> Vec<SortingColumn> {
        get_otel_schema().sorting_columns()
    }

    pub fn z_order_columns() -> Vec<String> {
        get_otel_schema().z_order_columns.clone()
    }
}

pub fn default_on_empty_string<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default + FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    let opt = Option::<String>::deserialize(deserializer)?;

    match opt {
        None => Ok(T::default()),
        Some(s) if s.is_empty() => Ok(T::default()),
        Some(s) => T::from_str(&s).map_err(DeError::custom),
    }
}