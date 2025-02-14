use async_trait::async_trait;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, QueryResponse, Response, FieldInfo};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, Type, NoopErrorHandler, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use futures::stream;
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::logical_expr::LogicalPlan;
use std::collections::HashMap;
use datafusion::common::ParamValues;
use bytes::BytesMut;
use crate::utils::{prepare_sql, value_to_string};
use crate::pgserver_message::PGServerMessage;

pub struct DfSessionService {
    pub session_context: Arc<SessionContext>,
    pub parser: Arc<PgQueryParser>,
    pub db: Arc<crate::database::Database>,
}

impl DfSessionService {
    pub fn new(session_context: SessionContext, db: Arc<crate::database::Database>) -> Self {
        let session_context = Arc::new(session_context);
        let parser = Arc::new(PgQueryParser {
            session_context: session_context.clone(),
        });
        Self {
            session_context,
            parser,
            db,
        }
    }
}

pub struct PgQueryParser {
    pub session_context: Arc<SessionContext>,
}

#[async_trait]
impl QueryParser for PgQueryParser {
    type Statement = LogicalPlan;
 
    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        let new_sql = prepare_sql(sql)
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let state = self.session_context.state();
        let logical_plan = state.create_logical_plan(&new_sql)
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let optimised = state.optimize(&logical_plan)
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        Ok(optimised)
    }
}

#[async_trait]
impl pgwire::api::query::SimpleQueryHandler for DfSessionService {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query_lower = query.trim_start().to_lowercase();
        if query_lower.starts_with("insert") {
            let msg = (&*self.db).insert_record(query)
                .await
                .map_err(|e| PgWireError::ApiError(e.into()))?;
            return Ok(vec![command_complete_response(&msg)]);
        } else if query_lower.starts_with("update") {
            let msg = (&*self.db).update_record(query)
                .await
                .map_err(|e| PgWireError::ApiError(e.into()))?;
            return Ok(vec![command_complete_response(&msg)]);
        } else if query_lower.starts_with("delete") {
            let msg = (&*self.db).delete_record(query)
                .await
                .map_err(|e| PgWireError::ApiError(e.into()))?;
            return Ok(vec![command_complete_response(&msg)]);
        }
 
        let new_sql = prepare_sql(query)
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let df = self.session_context.sql(&new_sql)
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let resp = encode_dataframe(df, &pgwire::api::portal::Format::UnifiedText)
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        Ok(vec![Response::Query(resp)])
    }
}

#[async_trait]
impl pgwire::api::query::ExtendedQueryHandler for DfSessionService {
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
        let params = plan.get_parameter_types()
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
        target: &pgwire::api::portal::Portal<Self::Statement>,
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
        portal: &'a pgwire::api::portal::Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let plan = &portal.statement.statement;
        let params = plan.get_parameter_types()
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let param_values = deserialize_parameters(portal, &ordered_param_types(&params))
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let plan_with_values = plan.clone().replace_params_with_values(&param_values)
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let df = self.session_context.execute_logical_plan(plan_with_values)
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let resp = encode_dataframe(df, &portal.result_column_format)
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        Ok(Response::Query(resp))
    }
}
 
fn command_complete_response(msg: &str) -> Response<'static> {
    let bytes = PGServerMessage::encode(PGServerMessage::CommandComplete(msg.to_string()));
    let row_stream = stream::iter(vec![Ok(DataRow::new(bytes, 0))]);
    let qr = QueryResponse::new(Vec::new().into(), row_stream);
    Response::Query(qr)
}
 
async fn encode_dataframe(
    df: DataFrame,
    _format: &pgwire::api::portal::Format,
) -> Result<QueryResponse<'static>, Box<dyn std::error::Error + Send + Sync>> {
    let schema = (*df.schema()).clone();
    let batches = df.collect().await?;
    let fields = pgwire_schema_from_arrow(&schema)?;
    
    let mut all_rows: Vec<pgwire::error::PgWireResult<DataRow>> = Vec::new();
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
 
fn serialize_row(row_values: Vec<String>) -> BytesMut {
    // Produce only the payload: field count and per-field data.
    let payload_len: usize = 2 + row_values.iter().map(|v| 4 + if v == "NULL" { 0 } else { v.len() }).sum::<usize>();
    let mut buf = BytesMut::with_capacity(payload_len);
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
 
fn pgwire_schema_from_arrow(schema: &datafusion::common::DFSchema) -> Result<Vec<FieldInfo>, Box<dyn std::error::Error + Send + Sync>> {
    let mut fields = Vec::new();
    for field in schema.fields() {
        let pg_type = into_pg_type(field.data_type())?;
        fields.push(FieldInfo::new(field.name().to_string(), None, None, pg_type, pgwire::api::results::FieldFormat::Text));
    }
    Ok(fields)
}
 
fn into_pg_type(dt: &datafusion::arrow::datatypes::DataType) -> Result<Type, Box<dyn std::error::Error + Send + Sync>> {
    match dt {
        datafusion::arrow::datatypes::DataType::Utf8 => Ok(Type::TEXT),
        datafusion::arrow::datatypes::DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, _) => Ok(Type::TIMESTAMP),
        datafusion::arrow::datatypes::DataType::Int64 => Ok(Type::INT8),
        datafusion::arrow::datatypes::DataType::Int32 => Ok(Type::INT4),
        _ => Ok(Type::TEXT),
    }
}
 
fn deserialize_parameters<T>(
    _portal: &pgwire::api::portal::Portal<T>,
    _ordered: &Vec<Option<&datafusion::arrow::datatypes::DataType>>,
) -> Result<ParamValues, Box<dyn std::error::Error + Send + Sync>> {
    Ok(ParamValues::List(vec![]))
}
 
fn ordered_param_types(
    _types: &HashMap<String, Option<datafusion::arrow::datatypes::DataType>>,
) -> Vec<Option<&datafusion::arrow::datatypes::DataType>> {
    Vec::new()
}
 
#[derive(Clone)]
pub struct HandlerFactory(pub Arc<DfSessionService>);
 
impl pgwire::api::auth::noop::NoopStartupHandler for DfSessionService {}
 
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
 
pub async fn run_pgwire_server<H>(
    handler: H,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    H: PgWireServerHandlers + Clone + Send + Sync + 'static,
{
    use tokio::net::TcpListener;
    let listener = TcpListener::bind(addr).await?;
    println!("PGWire server listening on {}", addr);
 
    loop {
        let (socket, peer_addr) = listener.accept().await?;
        println!("Accepted connection from {:?}", peer_addr);
 
        let handler_clone = handler.clone();
        tokio::spawn(async move {
            if let Err(e) = pgwire::tokio::process_socket(socket, None, handler_clone).await {
                eprintln!("PGWire server error: {:?}", e);
            }
        });
    }
}
