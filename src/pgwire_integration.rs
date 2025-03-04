use async_trait::async_trait;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, QueryResponse, Response, FieldInfo};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, Type, PgWireServerHandlers, NoopErrorHandler};
use pgwire::api::auth::StartupHandler;
use pgwire::messages::PgWireFrontendMessage;
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::startup::{Authentication, PasswordMessageFamily};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use futures::stream;
use futures::SinkExt;
use tokio::sync::Mutex;
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::logical_expr::LogicalPlan;
use std::collections::HashMap;
use datafusion::common::ParamValues;
use bytes::BytesMut;
use crate::utils::{prepare_sql, value_to_string};
use tokio_util::sync::CancellationToken;
use tracing::{info, error, debug};

use std::fs;
use std::io::{Error as IoError, ErrorKind};
use serde::{Serialize, Deserialize};
use bcrypt::{hash, verify, DEFAULT_COST};

/// Represents a user with a username, hashed password, and admin status.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct User {
    pub username: String,
    pub hashed_password: String,
    pub is_admin: bool,
}

/// Represents a user database storing a collection of users.
#[derive(Serialize, Deserialize, Debug)]
pub struct UserDB {
    pub users: Vec<User>,
}

impl UserDB {
    /// Loads the user database from a file or creates a default one if it doesn't exist.
    pub fn load_from_file(path: &str) -> Result<Self, IoError> {
        if let Ok(contents) = fs::read_to_string(path) {
            let db: UserDB = serde_json::from_str(&contents)
                .map_err(|e| IoError::new(ErrorKind::InvalidData, e))?;
            info!("Loaded user database from {} with {} users", path, db.users.len());
            Ok(db)
        } else {
            info!("No user database found at {}. Creating default admin user.", path);
            let default_user = User {
                username: "admin".to_string(),
                hashed_password: hash("admin123", DEFAULT_COST)
                    .map_err(|e| IoError::new(ErrorKind::Other, e))?,
                is_admin: true,
            };
            let db = UserDB { users: vec![default_user] };
            db.save_to_file(path)?;
            Ok(db)
        }
    }

    /// Saves the user database to a file.
    pub fn save_to_file(&self, path: &str) -> Result<(), IoError> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| IoError::new(ErrorKind::Other, e))?;
        fs::write(path, json)?;
        info!("Saved user database to {}", path);
        Ok(())
    }

    /// Verifies a user's credentials.
    pub fn verify_user(&self, username: &str, password: &str) -> bool {
        if let Some(user) = self.users.iter().find(|u| u.username == username) {
            let result = verify(password, &user.hashed_password).unwrap_or(false);
            debug!("Verifying user '{}': password match = {}", username, result);
            result
        } else {
            debug!("User '{}' not found in database", username);
            false
        }
    }

    /// Creates a new user and saves it to the database.
    pub fn create_user(&mut self, username: &str, password: &str, is_admin: bool) -> Result<(), IoError> {
        if self.users.iter().any(|u| u.username == username) {
            return Err(IoError::new(ErrorKind::AlreadyExists, "User already exists"));
        }
        let hashed = hash(password, DEFAULT_COST)
            .map_err(|e| IoError::new(ErrorKind::Other, e))?;
        let user = User {
            username: username.to_string(),
            hashed_password: hashed,
            is_admin,
        };
        self.users.push(user);
        self.save_to_file("users.json")?;
        info!("Created new user: {}", username);
        Ok(())
    }

    /// Logs the list of registered users.
    pub fn log_users(&self) {
        info!("Registered users: {:?}", self.users);
    }
}

/// Service for handling PostgreSQL wire protocol sessions with DataFusion.
pub struct DfSessionService {
    pub session_context: Arc<SessionContext>,
    pub parser: Arc<PgQueryParser>,
    pub db: Arc<crate::database::Database>,
    pub user_db: Arc<Mutex<UserDB>>,
}

impl DfSessionService {
    /// Creates a new session service instance.
    pub fn new(session_context: SessionContext, db: Arc<crate::database::Database>) -> Self {
        let session_context = Arc::new(session_context);
        let parser = Arc::new(PgQueryParser {
            session_context: session_context.clone(),
        });
        let user_db = UserDB::load_from_file("users.json").unwrap_or_else(|e| {
            error!("Failed to load user DB: {}. Starting with empty DB.", e);
            UserDB { users: vec![] }
        });
        let service = Self {
            session_context,
            parser,
            db,
            user_db: Arc::new(Mutex::new(user_db)),
        };
        tokio::spawn({
            let user_db = service.user_db.clone();
            async move {
                user_db.lock().await.log_users();
            }
        });
        service
    }
}

/// Parser for PostgreSQL queries into DataFusion logical plans.
pub struct PgQueryParser {
    pub session_context: Arc<SessionContext>,
}

#[async_trait]
impl QueryParser for PgQueryParser {
    type Statement = LogicalPlan;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        let new_sql = prepare_sql(sql).map_err(|e| PgWireError::ApiError(e.into()))?;
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

        let new_sql = prepare_sql(query).map_err(|e| PgWireError::ApiError(e.into()))?;
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

/// Creates a command complete response for non-SELECT queries.
fn command_complete_response(msg: &str) -> Response<'static> {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&(1_i16).to_be_bytes()); // Number of fields
    let bytes = msg.as_bytes();
    buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    buf.extend_from_slice(bytes);

    let row_stream = stream::iter(vec![Ok(DataRow::new(buf, 1))]);
    let fields = vec![FieldInfo::new(
        "CommandComplete".to_string(),
        None,
        None,
        Type::TEXT,
        pgwire::api::results::FieldFormat::Text,
    )];
    let qr = QueryResponse::new(fields.into(), row_stream);
    Response::Query(qr)
}

/// Encodes a DataFusion DataFrame into a PostgreSQL wire protocol QueryResponse.
async fn encode_dataframe(
    df: DataFrame,
    _format: &pgwire::api::portal::Format,
) -> Result<QueryResponse<'static>, Box<dyn std::error::Error + Send + Sync>> {
    let schema = (*df.schema()).clone();
    let batches = df.collect().await?;
    let fields = pgwire_schema_from_arrow(&schema)?;

    let mut all_rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut row_values = Vec::new();
            for col in 0..batch.num_columns() {
                let array = batch.column(col);
                let value = if array.is_null(row) {
                    None
                } else {
                    Some(value_to_string(array.as_ref(), row))
                };
                row_values.push(value);
            }
            all_rows.push(Ok(DataRow::new(serialize_row(row_values), batch.num_columns() as i16)));
        }
    }
    let row_stream = stream::iter(all_rows);
    Ok(QueryResponse::new(fields.into(), row_stream))
}

/// Serializes a row of values into a byte buffer.
fn serialize_row(row_values: Vec<Option<String>>) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&(row_values.len() as i16).to_be_bytes());
    for value in row_values {
        match value {
            Some(v) => {
                let bytes = v.as_bytes();
                buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                buf.extend_from_slice(bytes);
            }
            None => {
                buf.extend_from_slice(&(-1i32).to_be_bytes());
            }
        }
    }
    buf
}

/// Converts a DataFusion schema to PostgreSQL wire protocol field info.
fn pgwire_schema_from_arrow(schema: &datafusion::common::DFSchema) -> Result<Vec<FieldInfo>, Box<dyn std::error::Error + Send + Sync>> {
    let mut fields = Vec::new();
    for field in schema.fields() {
        let pg_type = into_pg_type(field.data_type())?;
        fields.push(FieldInfo::new(field.name().to_string(), None, None, pg_type, pgwire::api::results::FieldFormat::Text));
    }
    Ok(fields)
}

/// Maps DataFusion data types to PostgreSQL types.
fn into_pg_type(dt: &datafusion::arrow::datatypes::DataType) -> Result<Type, Box<dyn std::error::Error + Send + Sync>> {
    match dt {
        datafusion::arrow::datatypes::DataType::Utf8 => Ok(Type::TEXT),
        datafusion::arrow::datatypes::DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, _) => Ok(Type::TIMESTAMP),
        datafusion::arrow::datatypes::DataType::Int64 => Ok(Type::INT8),
        datafusion::arrow::datatypes::DataType::Int32 => Ok(Type::INT4),
        _ => Ok(Type::TEXT),
    }
}

/// Deserializes query parameters (placeholder implementation).
fn deserialize_parameters<T>(
    _portal: &pgwire::api::portal::Portal<T>,
    _ordered: &Vec<Option<&datafusion::arrow::datatypes::DataType>>,
) -> Result<ParamValues, Box<dyn std::error::Error + Send + Sync>> {
    Ok(ParamValues::List(vec![]))
}

/// Orders parameter types from a HashMap.
fn ordered_param_types(
    types: &HashMap<String, Option<datafusion::arrow::datatypes::DataType>>,
) -> Vec<Option<&datafusion::arrow::datatypes::DataType>> {
    types.values().map(|opt| opt.as_ref()).collect()
}

#[async_trait]
impl StartupHandler for DfSessionService {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + futures::Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: std::fmt::Debug,
    {
        match message {
            PgWireFrontendMessage::Startup(startup) => {
                if let Some(username) = startup.parameters.get("user") {
                    client.metadata_mut().insert("username".to_string(), username.to_string()); // Use metadata_mut()
                    debug!("Startup - Username: '{}'", username);
                    client.send(PgWireBackendMessage::Authentication(Authentication::CleartextPassword))
                        .await
                        .map_err(|e| PgWireError::IoError(std::io::Error::new(std::io::ErrorKind::Other, format!("Send error: {:?}", e))))?;
                    Ok(())
                } else {
                    Err(PgWireError::ApiError("No username provided in startup".into()))
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(msg) => {
                if let PasswordMessageFamily::Password(ref pwd) = msg {
                    let username = client.metadata().get("username").cloned().unwrap_or_default();
                    debug!("Password message - Username: '{}', Password: '{:?}'", username, pwd.password);
                    let user_db = self.user_db.lock().await;
                    if user_db.verify_user(&username, &pwd.password) {
                        info!("User '{}' authenticated successfully", username);
                        client.send(PgWireBackendMessage::Authentication(Authentication::Ok))
                            .await
                            .map_err(|e| PgWireError::IoError(std::io::Error::new(std::io::ErrorKind::Other, format!("Send error: {:?}", e))))?;
                        Ok(())
                    } else {
                        Err(PgWireError::ApiError("Invalid username or password".into()))
                    }
                } else {
                    Err(PgWireError::ApiError("Unexpected password message variant".into()))
                }
            }
            _ => Err(PgWireError::ApiError("Expected startup or password message".into())),
        }
    }
}

/// Factory for creating PostgreSQL wire protocol handlers.
#[derive(Clone)]
pub struct HandlerFactory(pub Arc<DfSessionService>);

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

/// Runs the PostgreSQL wire protocol server.
pub async fn run_pgwire_server<H>(
    handler: H,
    addr: &str,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    H: PgWireServerHandlers + Clone + Send + Sync + 'static,
{
    use tokio::net::TcpListener;
    let listener = TcpListener::bind(addr).await?;
    info!("PGWire server listening on {}", addr);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("PGWire server shutting down");
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok((socket, peer_addr)) => {
                        info!("Accepted connection from {:?}", peer_addr);
                        let handler_clone = handler.clone();
                        tokio::spawn(async move {
                            if let Err(e) = pgwire::tokio::process_socket(socket, None, handler_clone).await {
                                error!("PGWire connection error: {:?}", e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {:?}", e);
                    }
                }
            }
        }
    }
    Ok(())
}