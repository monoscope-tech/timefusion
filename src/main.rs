// main.rs

// --- DataFusion (and Arrow) imports ---
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::{
    array::{Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use chrono::{DateTime, Utc, TimeZone, LocalResult};
use datafusion::common::{DFSchema, DataFusionError, ParamValues};
use datafusion::prelude::*;
use datafusion::logical_expr::LogicalPlan;

// --- Delta Lake imports ---
use deltalake::{kernel::StructField, DeltaOps, DeltaTable};
use deltalake::{
    DeltaTableBuilder,
    delta_datafusion::{DeltaScanConfig, DeltaTableProvider},
};

// --- Standard imports ---
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::env;

// --- PGWire and Futures imports ---
use async_trait::async_trait;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, QueryResponse, Response, FieldInfo, FieldFormat,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use futures::stream;
use tokio::net::TcpListener;
use bytes::BytesMut;
use regex::Regex; // For parsing INSERT queries

// --- Shared Database Code ---
type ProjectConfigs = Arc<RwLock<HashMap<String, (String, Arc<RwLock<DeltaTable>>)>>>;
 
fn build_timestamp_array(values: Vec<i64>, tz: Option<Arc<str>>) -> TimestampMicrosecondArray {
    use datafusion::arrow::array::ArrayData;
    use datafusion::arrow::buffer::Buffer;

    let data_type = DataType::Timestamp(TimeUnit::Microsecond, tz);
    let buffer = Buffer::from_slice_ref(&values);
    let array_data = ArrayData::builder(data_type.clone())
        .len(values.len())
        .add_buffer(buffer)
        .build()
        .unwrap();
    TimestampMicrosecondArray::from(array_data)
}
 
struct Database {
    project_configs: ProjectConfigs,
    ctx: SessionContext,
}
 
impl Database {
    async fn new() -> Result<Self, DataFusionError> {
        Ok(Self {
            project_configs: Arc::new(RwLock::new(HashMap::new())),
            ctx: SessionContext::new(),
        })
    }
 
    async fn add_project(
        &self,
        project_id: &str,
        connection_string: &str,
    ) -> Result<(), DataFusionError> {
        let table_path = if connection_string.starts_with("s3://")
            || connection_string.starts_with("s3a://")
            || connection_string.starts_with("s3n://")
        {
            connection_string.to_string()
        } else {
            format!("{}/{}", connection_string.trim_end_matches('/'), project_id)
        };
 
        let table = match DeltaTableBuilder::from_uri(&table_path).load().await {
            Ok(table) => table,
            Err(_) => {
                let struct_fields = vec![
                    StructField::new(
                        "project_id".to_string(),
                        deltalake::kernel::DataType::Primitive(
                            deltalake::kernel::PrimitiveType::String,
                        ),
                        false,
                    ),
                    StructField::new(
                        "timestamp".to_string(),
                        deltalake::kernel::DataType::Primitive(
                            deltalake::kernel::PrimitiveType::Timestamp,
                        ),
                        false,
                    ),
                    StructField::new(
                        "start_time".to_string(),
                        deltalake::kernel::DataType::Primitive(
                            deltalake::kernel::PrimitiveType::Timestamp,
                        ),
                        true,
                    ),
                    StructField::new(
                        "end_time".to_string(),
                        deltalake::kernel::DataType::Primitive(
                            deltalake::kernel::PrimitiveType::Timestamp,
                        ),
                        true,
                    ),
                    StructField::new(
                        "payload".to_string(),
                        deltalake::kernel::DataType::Primitive(
                            deltalake::kernel::PrimitiveType::String,
                        ),
                        true,
                    ),
                    StructField::new(
                        "event_date".to_string(),
                        deltalake::kernel::DataType::Primitive(
                            deltalake::kernel::PrimitiveType::String,
                        ),
                        false,
                    ),
                ];
                DeltaOps::try_from_uri(&table_path)
                    .await?
                    .create()
                    .with_columns(struct_fields)
                    .with_partition_columns(vec!["event_date".to_string()])
                    .await
                    .map_err(|e| DataFusionError::External(e.into()))?
            }
        };
 
        self.project_configs.write().unwrap().insert(
            project_id.to_string(),
            (connection_string.to_string(), Arc::new(RwLock::new(table))),
        );
 
        let configs = self.project_configs.read().unwrap();
        let (_conn_str, delta_table) = configs.get(project_id).unwrap().clone();
        let table_ref = delta_table.read().unwrap();
        let snapshot = table_ref
            .snapshot()
            .map_err(|e| DataFusionError::External(e.into()))?;
        let provider = DeltaTableProvider::try_new(
            snapshot.clone(),
            table_ref.log_store().clone(),
            DeltaScanConfig::default(),
        )
        .map_err(|e| DataFusionError::External(e.into()))?;
        let unique_table_name = format!("table_{}", project_id);
        self.ctx.register_table(&unique_table_name, Arc::new(provider))
            .map_err(|e| DataFusionError::External(e.into()))?;
 
        Ok(())
    }
 
    // When running queries via PGWire, we use the common prepare_sql function.
    async fn query(&self, sql: &str) -> Result<DataFrame, DataFusionError> {
        let new_sql = prepare_sql(sql)?;
        self.ctx.sql(&new_sql).await
    }
 
    async fn write(
        &self,
        project_id: &str,
        timestamp: DateTime<Utc>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        payload: Option<&str>,
    ) -> Result<(), DataFusionError> {
        let configs = self.project_configs.read().unwrap();
        let (conn_str, _) = configs
            .get(project_id)
            .expect("no project_id in sql query")
            .clone();
 
        let schema = Schema::new(vec![
            Field::new("project_id", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                false,
            ),
            Field::new(
                "start_time",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                true,
            ),
            Field::new(
                "end_time",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                true,
            ),
            Field::new("payload", DataType::Utf8, true),
            Field::new("event_date", DataType::Utf8, false),
        ]);
 
        let event_date = timestamp.format("%Y-%m-%d").to_string();
        let ts_micro = timestamp.timestamp_micros();
        let start_ts_micro = start_time.map_or(ts_micro, |t| t.timestamp_micros());
        let end_ts_micro = end_time.map_or(ts_micro, |t| t.timestamp_micros());
 
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![project_id])),
                Arc::new(build_timestamp_array(vec![ts_micro], Some(Arc::from("UTC")))),
                Arc::new(build_timestamp_array(vec![start_ts_micro], Some(Arc::from("UTC")))),
                Arc::new(build_timestamp_array(vec![end_ts_micro], Some(Arc::from("UTC")))),
                Arc::new(StringArray::from(vec![payload.unwrap_or("")])),
                Arc::new(StringArray::from(vec![event_date])),
            ],
        )
        .map_err(|e| DataFusionError::External(e.into()))?;
 
        let batches: Vec<RecordBatch> = vec![batch];
        DeltaOps::try_from_uri(&conn_str)
            .await?
            .write(batches.into_iter())
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;
 
        Ok(())
    }
 
    async fn compact(&self, project_id: &str) -> Result<(), DataFusionError> {
        let configs = self.project_configs.read().unwrap();
        let (_, delta_table_arc) = configs.get(project_id)
            .ok_or_else(|| DataFusionError::External(format!("Project ID '{}' not found", project_id).into()))?
            .clone();
        let _table = delta_table_arc.read().unwrap();
        println!("Compaction for project '{}' would run here.", project_id);
        Ok(())
    }
}
 
// This helper extracts the project_id from SELECT queries via the WHERE clause.
fn extract_project_id_from_sql(sql: &str) -> Result<String, DataFusionError> {
    sql.to_lowercase()
        .find("where project_id = '")
        .map(|start| {
            let idx = start + "where project_id = '".len();
            let end = sql[idx..].find('\'').unwrap();
            sql[idx..idx + end].to_string()
        })
        .ok_or_else(|| DataFusionError::External("Project ID not found in SQL".to_string().into()))
}
 
/// prepare_sql rewrites queries so that the literal table "table" becomes the project-specific table name.
/// For INSERT queries it uses a regex to extract the project_id from the VALUES clause.
fn prepare_sql(query: &str) -> Result<String, DataFusionError> {
    let query_lower = query.trim().to_lowercase();
    if query_lower.starts_with("insert") {
        // Regex to capture columns and values from INSERT.
        let re = Regex::new(r#"(?i)insert\s+into\s+"table"\s*\(([^)]+)\)\s+values\s*\(([^)]+)\)"#)
            .map_err(|e| DataFusionError::External(e.to_string().into()))?;
        if let Some(caps) = re.captures(query) {
            let columns_str = caps.get(1).unwrap().as_str();
            let values_str = caps.get(2).unwrap().as_str();
            let columns: Vec<&str> = columns_str.split(',')
                .map(|s| s.trim().trim_matches('"'))
                .collect();
            let values: Vec<&str> = values_str.split(',')
                .map(|s| s.trim())
                .collect();
            if let Some(idx) = columns.iter().position(|&col| col.eq_ignore_ascii_case("project_id")) {
                let project_value = values.get(idx)
                    .ok_or_else(|| DataFusionError::External("Missing value for project_id in INSERT".into()))?;
                let project_id = project_value.trim_matches('\'');
                let unique_table_name = format!("table_{}", project_id);
                return Ok(query.replace("\"table\"", &format!("\"{}\"", unique_table_name)));
            }
        }
        Err(DataFusionError::External("Could not extract project_id from INSERT query".into()))
    } else if query_lower.starts_with("select") {
        if let Ok(project_id) = extract_project_id_from_sql(query) {
            let unique_table_name = format!("table_{}", project_id);
            Ok(query.replace("\"table\"", &format!("\"{}\"", unique_table_name)))
        } else {
            Ok(query.to_string())
        }
    } else {
        Ok(query.to_string())
    }
}
 
// --- PGWire Integration ---
 
pub struct DfSessionService {
    session_context: Arc<SessionContext>,
    parser: Arc<PgQueryParser>,
}
 
impl DfSessionService {
    pub fn new(session_context: SessionContext) -> Self {
        let session_context = Arc::new(session_context);
        let parser = Arc::new(PgQueryParser {
            session_context: session_context.clone(),
        });
        Self {
            session_context,
            parser,
        }
    }
}
 
pub struct PgQueryParser {
    session_context: Arc<SessionContext>,
}
 
#[async_trait]
impl QueryParser for PgQueryParser {
    type Statement = LogicalPlan;
 
    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        let new_sql = prepare_sql(sql)
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let state = self.session_context.state();
        let logical_plan = state
            .create_logical_plan(&new_sql)
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let optimised = state
            .optimize(&logical_plan)
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        Ok(optimised)
    }
}
 
#[async_trait]
impl SimpleQueryHandler for DfSessionService {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let new_sql = prepare_sql(query)
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let df = self
            .session_context
            .sql(&new_sql)
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let resp = encode_dataframe(df, &Format::UnifiedText)
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        Ok(vec![Response::Query(resp)])
    }
}
 
#[async_trait]
impl ExtendedQueryHandler for DfSessionService {
    type Statement = LogicalPlan;
    type QueryParser = PgQueryParser;
 
    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
    }
 
    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let plan = &target.statement;
        let schema = plan.schema();
        let fields = pgwire_schema_from_arrow(schema)?;
        let params = plan
            .get_parameter_types()
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let mut param_types = Vec::with_capacity(params.len());
        for param in ordered_param_types(&params).iter() {
            if let Some(dt) = param {
                let pgtype = into_pg_type(dt)
                    .map_err(|e| PgWireError::ApiError(e.into()))?;
                param_types.push(pgtype);
            } else {
                param_types.push(Type::UNKNOWN);
            }
        }
        Ok(DescribeStatementResponse::new(param_types, fields))
    }
 
    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let plan = &target.statement.statement;
        let fields = pgwire_schema_from_arrow(plan.schema())?;
        Ok(DescribePortalResponse::new(fields))
    }
 
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let plan = &portal.statement.statement;
        let params = plan
            .get_parameter_types()
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let param_values = deserialize_parameters(portal, &ordered_param_types(&params))
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let plan_with_values = plan
            .clone()
            .replace_params_with_values(&param_values)
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let df = self
            .session_context
            .execute_logical_plan(plan_with_values)
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let resp = encode_dataframe(df, &portal.result_column_format)
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        Ok(Response::Query(resp))
    }
}
 
 
/// serialize_row constructs a BytesMut buffer in the PGWire "D" message format:
/// It writes the field count (i16 in big-endian), and for each field its length (i32 big-endian)
/// and the field’s bytes (or -1 for NULL).
fn serialize_row(row_values: Vec<String>) -> BytesMut {
    let mut buf = BytesMut::new();
    // Write field count as i16 in big-endian.
    buf.extend_from_slice(&(row_values.len() as i16).to_be_bytes());
    for value in row_values {
        if value == "NULL" {
            buf.extend_from_slice(&(-1i32).to_be_bytes());
        } else {
            let bytes = value.as_bytes();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
    }
    buf
}
 
/// encode_dataframe converts a DataFusion DataFrame into a PGWire QueryResponse by
/// serializing each row in the proper "D" message format.
async fn encode_dataframe(
    df: DataFrame,
    _format: &Format,
) -> Result<QueryResponse<'static>, Box<dyn std::error::Error + Send + Sync>> {
    let schema = (*df.schema()).clone();
    let batches = df.collect().await?;
    let fields = pgwire_schema_from_arrow(&schema)?;
    
    let mut all_rows: Vec<PgWireResult<DataRow>> = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut row_values = Vec::new();
            for col in 0..batch.num_columns() {
                let array = batch.column(col);
                let value = value_to_string(array.as_ref(), row);
                row_values.push(value);
            }
            let serialized = serialize_row(row_values);
            all_rows.push(Ok(DataRow::new(serialized, batch.num_columns() as i16)));
        }
    }
    let row_stream = stream::iter(all_rows);
    Ok(QueryResponse::new(fields.into(), row_stream))
}
 
// Helper: convert an Arrow cell into a string.
fn value_to_string(array: &dyn Array, index: usize) -> String {
    if array.is_null(index) {
        return "NULL".to_string();
    }
    if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
        return string_array.value(index).to_string();
    }
    if let Some(ts_array) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let micros = ts_array.value(index);
        match Utc.timestamp_micros(micros) {
            LocalResult::Single(dt) => dt.to_rfc3339(),
            _ => "Invalid timestamp".to_string(),
        }
    } else {
        format!("{:?}", array.to_data().buffers()[0].as_slice())
    }
}
 
// Convert a DFSchema into a Vec<FieldInfo> for PGWire.
fn pgwire_schema_from_arrow(schema: &DFSchema) -> Result<Vec<FieldInfo>, Box<dyn std::error::Error + Send + Sync>> {
    let mut fields = Vec::new();
    for field in schema.fields() {
        let pg_type = into_pg_type(field.data_type())?;
        fields.push(FieldInfo::new(field.name().to_string(), None, None, pg_type, FieldFormat::Text));
    }
    Ok(fields)
}
 
// Map an Arrow DataType to a PGWire Type.
fn into_pg_type(dt: &DataType) -> Result<Type, Box<dyn std::error::Error + Send + Sync>> {
    match dt {
        DataType::Utf8 => Ok(Type::TEXT),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(Type::TIMESTAMP),
        DataType::Int64 => Ok(Type::INT8),
        DataType::Int32 => Ok(Type::INT4),
        _ => Ok(Type::TEXT),
    }
}
 
// For parameter handling: return empty parameters.
fn deserialize_parameters<T>(
    _portal: &Portal<T>,
    _ordered: &Vec<Option<&DataType>>,
) -> Result<ParamValues, Box<dyn std::error::Error + Send + Sync>> {
    Ok(ParamValues::List(vec![]))
}
 
// For parameter ordering.
fn ordered_param_types(
    _types: &HashMap<String, Option<DataType>>,
) -> Vec<Option<&DataType>> {
    Vec::new()
}
 
// --- HandlerFactory for PGWire ---
#[derive(Clone)]
pub struct HandlerFactory(pub Arc<DfSessionService>);
 
impl NoopStartupHandler for DfSessionService {}
 
impl PgWireServerHandlers for HandlerFactory {
    type StartupHandler = DfSessionService;
    type SimpleQueryHandler = DfSessionService;
    type ExtendedQueryHandler = DfSessionService;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;
 
    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.0.clone()
    }
    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.0.clone()
    }
    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.0.clone()
    }
    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}
 
// --- Custom PGWire Server Runner ---
async fn run_pgwire_server<H>(
    handler: H,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    H: PgWireServerHandlers + Clone + Send + Sync + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    println!("PGWire server listening on {}", addr);

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        println!("Accepted connection from {:?}", peer_addr);

        let handler_clone = handler.clone();
        tokio::spawn(async move {
            if let Err(e) = pgwire::tokio::process_socket(socket, None, handler_clone).await {
                eprintln!("Connection error: {:?}", e);
            }
        });
    }
}
 
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // For S3 integration.
    deltalake::aws::register_handlers(None);
 
    let bucket = env::var("S3_BUCKET_NAME")
        .expect("S3_BUCKET_NAME environment variable not set");
    let s3_uri = format!("s3://{}/delta_table", bucket);
    
    let db = Database::new().await?;
    db.add_project("events", &s3_uri).await?;
    
    // Write a record using the Database API.
    let now = Utc::now();
    db.write("events", now, Some(now), None, Some("data1")).await?;
    
    // Run a SELECT query (which will be rewritten to use "table_events").
    db.query("SELECT * FROM \"table\" WHERE project_id = 'events'")
        .await?
        .show()
        .await?;
    
    db.compact("events").await?;
    
    // Start the PGWire server.
    let pg_service = DfSessionService::new(db.ctx.clone());
    let handler_factory = HandlerFactory(Arc::new(pg_service));
    let pg_addr = "127.0.0.1:5432";
    println!("Spawning PGWire server task on {}", pg_addr);

    tokio::spawn(async move {
        if let Err(e) = run_pgwire_server(handler_factory, pg_addr).await {
            eprintln!("PGWire server error: {:?}", e);
        }
    });
 
    // Wait for Ctrl+C.
    tokio::signal::ctrl_c().await?;
    println!("Shutting down PGWire server...");
    Ok(())
}
