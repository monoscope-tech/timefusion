// database.rs

use std::{
    collections::HashMap,
    sync::{Arc as StdArc, Arc, RwLock},
};

use anyhow::Result;
use chrono::DateTime;
use datafusion::{
    arrow::{
        array::{BooleanArray, Int32Array, Int64Array, ListBuilder, StringArray, StringBuilder, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    },
    execution::context::SessionContext,
    prelude::*,
};
use deltalake::{DeltaOps, DeltaTableBuilder};
use dotenv::dotenv;
use sqlparser::{
    ast::{Expr, SetExpr, Statement, Value as SqlValue},
    dialect::GenericDialect,
};

use crate::{metrics::COMPACTION_COUNTER, utils::prepare_sql};

pub type ProjectConfigs = Arc<RwLock<HashMap<String, (String, Arc<RwLock<deltalake::DeltaTable>>)>>>;

pub struct Database {
    pub ctx:         SessionContext,
    project_configs: ProjectConfigs,
}

impl Database {
    pub async fn new() -> Result<Self> {
        dotenv().ok();
        let ctx = SessionContext::new();
        Ok(Self {
            ctx,
            project_configs: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn get_session_context(&self) -> SessionContext {
        self.ctx.clone()
    }

    /// Add a table using the fixed key "telemetry_events"
    pub async fn add_project(&self, table_name: &str, connection_string: &str) -> Result<()> {
        let table = match DeltaTableBuilder::from_uri(connection_string).load().await {
            Ok(table) => table,
            Err(e) => {
                tracing::warn!("Failed to load table '{}': {}. Creating new table.", connection_string, e);
                DeltaOps::try_from_uri(connection_string)
                    .await?
                    .create()
                    .with_columns(Self::event_schema_fields())
                    .with_partition_columns(vec!["timestamp".to_string()])
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to create table: {:?}", e))?
            }
        };
        self.project_configs
            .write()
            .map_err(|e| anyhow::anyhow!("Failed to acquire write lock: {:?}", e))?
            .insert(table_name.to_string(), (connection_string.to_string(), Arc::new(RwLock::new(table))));
        Ok(())
    }

    /// Schema matching your new DB table definition.
    fn event_schema() -> Schema {
        Schema::new(vec![
            Field::new("projectId", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some(StdArc::from("UTC"))), false),
            Field::new("traceId", DataType::Utf8, false),
            Field::new("spanId", DataType::Utf8, false),
            Field::new("eventType", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, true),
            Field::new("endTime", DataType::Timestamp(TimeUnit::Microsecond, Some(StdArc::from("UTC"))), true),
            Field::new("durationNs", DataType::Int64, false),
            Field::new("spanName", DataType::Utf8, false),
            Field::new("spanKind", DataType::Utf8, true),
            Field::new("parentSpanId", DataType::Utf8, true),
            Field::new("traceState", DataType::Utf8, true),
            Field::new("hasError", DataType::Boolean, false),
            Field::new("severityText", DataType::Utf8, true),
            Field::new("severityNumber", DataType::Int32, false),
            Field::new("body", DataType::Utf8, true),
            Field::new("httpMethod", DataType::Utf8, true),
            Field::new("httpUrl", DataType::Utf8, true),
            Field::new("httpRoute", DataType::Utf8, true),
            Field::new("httpHost", DataType::Utf8, true),
            Field::new("httpStatusCode", DataType::Int32, true),
            Field::new("pathParams", DataType::Utf8, false),
            Field::new("queryParams", DataType::Utf8, false),
            Field::new("requestBody", DataType::Utf8, false),
            Field::new("responseBody", DataType::Utf8, false),
            Field::new("sdkType", DataType::Utf8, false),
            Field::new("serviceVersion", DataType::Utf8, true),
            Field::new("errors", DataType::Utf8, false),
            Field::new("tags", DataType::List(StdArc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("parentId", DataType::Utf8, true),
            Field::new("dbSystem", DataType::Utf8, true),
            Field::new("dbName", DataType::Utf8, true),
            Field::new("dbStatement", DataType::Utf8, true),
            Field::new("dbOperation", DataType::Utf8, true),
            Field::new("rpcSystem", DataType::Utf8, true),
            Field::new("rpcService", DataType::Utf8, true),
            Field::new("rpcMethod", DataType::Utf8, true),
            Field::new("endpointHash", DataType::Utf8, false),
            Field::new("shapeHash", DataType::Utf8, false),
            Field::new("formatHashes", DataType::List(StdArc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("fieldHashes", DataType::List(StdArc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("attributes", DataType::Utf8, false),
            Field::new("events", DataType::Utf8, false),
            Field::new("links", DataType::Utf8, false),
            Field::new("resource", DataType::Utf8, false),
            Field::new("instrumentationScope", DataType::Utf8, false),
        ])
    }

    fn event_schema_fields() -> Vec<deltalake::kernel::StructField> {
        let schema = Self::event_schema();
        schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Utf8 => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String),
                    f.is_nullable(),
                ),
                DataType::Timestamp(_, _) => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Timestamp),
                    f.is_nullable(),
                ),
                DataType::Int64 => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Long),
                    f.is_nullable(),
                ),
                DataType::Int32 => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Integer),
                    f.is_nullable(),
                ),
                DataType::Boolean => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Boolean),
                    f.is_nullable(),
                ),
                DataType::List(_) => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String),
                    f.is_nullable(),
                ),
                _ => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String),
                    f.is_nullable(),
                ),
            })
            .collect()
    }

    pub async fn create_events_table(&self, table_name: &str, table_uri: &str) -> Result<()> {
        let table = match DeltaTableBuilder::from_uri(table_uri).load().await {
            Ok(table) => table,
            Err(e) => {
                tracing::warn!("Table '{}' not found: {}. Creating new table.", table_uri, e);
                DeltaOps::try_from_uri(table_uri)
                    .await?
                    .create()
                    .with_columns(Self::event_schema_fields())
                    .with_partition_columns(vec!["timestamp".to_string()])
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to create events table: {:?}", e))?
            }
        };

        let table_ref = Arc::new(RwLock::new(table));
        let provider = {
            let table_guard = table_ref.read().map_err(|_| anyhow::anyhow!("Lock error"))?;
            let snapshot = table_guard.snapshot().map_err(|e| anyhow::anyhow!("Failed to get snapshot: {:?}", e))?;
            let log_store = table_guard.log_store().clone();
            deltalake::delta_datafusion::DeltaTableProvider::try_new(snapshot.clone(), log_store, deltalake::delta_datafusion::DeltaScanConfig::default())
                .map_err(|e| anyhow::anyhow!("Failed to create provider: {:?}", e))?
        };

        // Register the table under the fixed name "telemetry_events"
        self.ctx
            .register_table("telemetry_events", Arc::new(provider))
            .map_err(|e| anyhow::anyhow!("Failed to register table: {:?}", e))?;

        self.project_configs
            .write()
            .map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))?
            .insert(table_name.to_string(), (table_uri.to_string(), table_ref));

        Ok(())
    }

    pub async fn query(&self, sql: &str) -> Result<DataFrame> {
        let new_sql = prepare_sql(sql)?;
        tracing::info!("Executing SQL: {}", new_sql);
        let df = self.ctx.sql(&new_sql).await.map_err(|e| anyhow::anyhow!("SQL execution failed: {:?}", e))?;
        Ok(df)
    }

    /// Write an IngestRecord into the fixed "telemetry_events" table.
    pub async fn write(&self, record: &crate::persistent_queue::IngestRecord) -> Result<()> {
        let (conn_str, _table_ref) = {
            let configs = self.project_configs.read().map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))?;
            configs.get("telemetry_events").ok_or_else(|| anyhow::anyhow!("Table not found"))?.clone()
        };

        let ts = DateTime::parse_from_rfc3339(&record.timestamp).map_err(|e| anyhow::anyhow!("Invalid timestamp: {:?}", e))?;
        let ts_micro = ts.timestamp_micros();
        let end_ts_micro = record.endTime.as_ref().map(|et| DateTime::parse_from_rfc3339(et).map(|dt| dt.timestamp_micros())).transpose()?;

        let project_id_array = StringArray::from(vec![record.projectId.clone()]);
        let id_array = StringArray::from(vec![record.id.clone()]);
        let timestamp_array = Arc::new(build_timestamp_array(vec![ts_micro], Some(StdArc::from("UTC"))));
        let trace_id_array = StringArray::from(vec![record.traceId.clone()]);
        let span_id_array = StringArray::from(vec![record.spanId.clone()]);
        let event_type_array = StringArray::from(vec![record.eventType.clone()]);
        let status_array = StringArray::from(vec![record.status.clone().unwrap_or_default()]);
        let end_time_array = Arc::new(build_timestamp_array(vec![end_ts_micro.unwrap_or(0)], Some(StdArc::from("UTC"))));
        let duration_ns_array = Int64Array::from(vec![record.durationNs]);
        let span_name_array = StringArray::from(vec![record.spanName.clone()]);
        let span_kind_array = StringArray::from(vec![record.spanKind.clone().unwrap_or_default()]);
        let parent_span_id_array = StringArray::from(vec![record.parentSpanId.as_deref().unwrap_or("")]);
        let trace_state_array = StringArray::from(vec![record.traceState.clone().unwrap_or_default()]);
        let has_error_array = BooleanArray::from(vec![record.hasError]);
        let severity_text_array = StringArray::from(vec![record.severityText.clone().unwrap_or_default()]);
        let severity_number_array = Int32Array::from(vec![record.severityNumber]);
        let body_array = StringArray::from(vec![record.body.clone().unwrap_or_else(|| "{}".to_string())]);
        let http_method_array = StringArray::from(vec![record.httpMethod.clone().unwrap_or_default()]);
        let http_url_array = StringArray::from(vec![record.httpUrl.clone().unwrap_or_default()]);
        let http_route_array = StringArray::from(vec![record.httpRoute.clone().unwrap_or_default()]);
        let http_host_array = StringArray::from(vec![record.httpHost.clone().unwrap_or_default()]);
        let http_status_code_array = Int32Array::from(vec![record.httpStatusCode.unwrap_or(0)]);
        let path_params_array = StringArray::from(vec![record.pathParams.clone().unwrap_or_else(|| "{}".to_string())]);
        let query_params_array = StringArray::from(vec![record.queryParams.clone().unwrap_or_else(|| "{}".to_string())]);
        let request_body_array = StringArray::from(vec![record.requestBody.clone().unwrap_or_else(|| "{}".to_string())]);
        let response_body_array = StringArray::from(vec![record.responseBody.clone().unwrap_or_else(|| "{}".to_string())]);
        let sdk_type_array = StringArray::from(vec![record.sdkType.clone()]);
        let service_version_array = StringArray::from(vec![record.serviceVersion.clone().unwrap_or_default()]);
        let errors_array = StringArray::from(vec![record.errors.clone().unwrap_or_else(|| "{}".to_string())]);

        let mut tags_builder = ListBuilder::new(StringBuilder::new());
        for tag in &record.tags {
            tags_builder.values().append_value(tag);
        }
        tags_builder.append(true);
        let tags_array = tags_builder.finish();

        let parent_id_array = StringArray::from(vec![record.parentId.clone().unwrap_or_default()]);
        let db_system_array = StringArray::from(vec![record.dbSystem.clone().unwrap_or_default()]);
        let db_name_array = StringArray::from(vec![record.dbName.clone().unwrap_or_default()]);
        let db_statement_array = StringArray::from(vec![record.dbStatement.clone().unwrap_or_default()]);
        let db_operation_array = StringArray::from(vec![record.dbOperation.clone().unwrap_or_default()]);
        let rpc_system_array = StringArray::from(vec![record.rpcSystem.clone().unwrap_or_default()]);
        let rpc_service_array = StringArray::from(vec![record.rpcService.clone().unwrap_or_default()]);
        let rpc_method_array = StringArray::from(vec![record.rpcMethod.clone().unwrap_or_default()]);
        let endpoint_hash_array = StringArray::from(vec![record.endpointHash.clone()]);
        let shape_hash_array = StringArray::from(vec![record.shapeHash.clone()]);

        let mut format_hashes_builder = ListBuilder::new(StringBuilder::new());
        for hash in &record.formatHashes {
            format_hashes_builder.values().append_value(hash);
        }
        format_hashes_builder.append(true);
        let format_hashes_array = format_hashes_builder.finish();

        let mut field_hashes_builder = ListBuilder::new(StringBuilder::new());
        for hash in &record.fieldHashes {
            field_hashes_builder.values().append_value(hash);
        }
        field_hashes_builder.append(true);
        let field_hashes_array = field_hashes_builder.finish();

        let attributes_array = StringArray::from(vec![record.attributes.clone().unwrap_or_else(|| "{}".to_string())]);
        let events_array = StringArray::from(vec![record.events.clone().unwrap_or_else(|| "{}".to_string())]);
        let links_array = StringArray::from(vec![record.links.clone().unwrap_or_else(|| "{}".to_string())]);
        let resource_array = StringArray::from(vec![record.resource.clone().unwrap_or_else(|| "{}".to_string())]);
        let instrumentation_scope_array = StringArray::from(vec![record.instrumentationScope.clone().unwrap_or_else(|| "{}".to_string())]);

        let schema = Self::event_schema();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(project_id_array),
                Arc::new(id_array),
                timestamp_array,
                Arc::new(trace_id_array),
                Arc::new(span_id_array),
                Arc::new(event_type_array),
                Arc::new(status_array),
                end_time_array,
                Arc::new(duration_ns_array),
                Arc::new(span_name_array),
                Arc::new(span_kind_array),
                Arc::new(parent_span_id_array),
                Arc::new(trace_state_array),
                Arc::new(has_error_array),
                Arc::new(severity_text_array),
                Arc::new(severity_number_array),
                Arc::new(body_array),
                Arc::new(http_method_array),
                Arc::new(http_url_array),
                Arc::new(http_route_array),
                Arc::new(http_host_array),
                Arc::new(http_status_code_array),
                Arc::new(path_params_array),
                Arc::new(query_params_array),
                Arc::new(request_body_array),
                Arc::new(response_body_array),
                Arc::new(sdk_type_array),
                Arc::new(service_version_array),
                Arc::new(errors_array),
                Arc::new(tags_array),
                Arc::new(parent_id_array),
                Arc::new(db_system_array),
                Arc::new(db_name_array),
                Arc::new(db_statement_array),
                Arc::new(db_operation_array),
                Arc::new(rpc_system_array),
                Arc::new(rpc_service_array),
                Arc::new(rpc_method_array),
                Arc::new(endpoint_hash_array),
                Arc::new(shape_hash_array),
                Arc::new(format_hashes_array),
                Arc::new(field_hashes_array),
                Arc::new(attributes_array),
                Arc::new(events_array),
                Arc::new(links_array),
                Arc::new(resource_array),
                Arc::new(instrumentation_scope_array),
            ],
        )?;
        DeltaOps::try_from_uri(&conn_str).await?.write(vec![batch]).await?;
        Ok(())
    }

    pub async fn insert_record(&self, query: &str) -> Result<String> {
        let dialect = GenericDialect {};
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, query).map_err(|e| anyhow::anyhow!("SQL parse error: {:?}", e))?;

        match &ast[0] {
            Statement::Insert(insert) => {
                let table_name_str = insert.table.to_string();
                if table_name_str != "telemetry_events" {
                    return Err(anyhow::anyhow!("Only inserts into 'telemetry_events' are supported"));
                }
                let source = insert.source.as_ref().ok_or_else(|| anyhow::anyhow!("Missing source in INSERT"))?;
                if let SetExpr::Values(values) = &*source.body {
                    let row = &values.rows[0];
                    let mut insert_values = HashMap::new();
                    for (col, val) in insert.columns.iter().zip(row.iter()) {
                        match val {
                            Expr::Value(sqlparser::ast::ValueWithSpan {
                                span: _,
                                value: SqlValue::SingleQuotedString(s),
                            }) => {
                                insert_values.insert(col.to_string(), s.clone());
                            }
                            Expr::Value(sqlparser::ast::ValueWithSpan {
                                span: _,
                                value: SqlValue::Number(n, _),
                            }) => {
                                insert_values.insert(col.to_string(), n.clone());
                            }
                            _ => return Err(anyhow::anyhow!("Unsupported value type: {:?}", val)),
                        }
                    }
                    let project_id = insert_values.get("projectId").ok_or_else(|| anyhow::anyhow!("Missing projectId"))?;
                    let timestamp = insert_values.get("timestamp").ok_or_else(|| anyhow::anyhow!("Missing timestamp"))?;
                    let record = crate::persistent_queue::IngestRecord {
                        projectId:            project_id.clone(),
                        id:                   insert_values.get("id").cloned().unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                        timestamp:            timestamp.clone(),
                        traceId:              insert_values.get("traceId").cloned().unwrap_or_default(),
                        spanId:               insert_values.get("spanId").cloned().unwrap_or_default(),
                        eventType:            insert_values.get("eventType").cloned().unwrap_or_else(|| "span".to_string()),
                        status:               insert_values.get("status").cloned(),
                        endTime:              insert_values.get("endTime").cloned(),
                        durationNs:           insert_values.get("durationNs").and_then(|s| s.parse().ok()).unwrap_or(0),
                        spanName:             insert_values.get("spanName").cloned().unwrap_or_default(),
                        spanKind:             insert_values.get("spanKind").cloned(),
                        parentSpanId:         insert_values.get("parentSpanId").cloned(),
                        traceState:           insert_values.get("traceState").cloned(),
                        hasError:             false,
                        severityText:         insert_values.get("severityText").cloned(),
                        severityNumber:       insert_values.get("severityNumber").and_then(|s| s.parse().ok()).unwrap_or(0),
                        body:                 insert_values.get("body").cloned(),
                        httpMethod:           insert_values.get("httpMethod").cloned(),
                        httpUrl:              insert_values.get("httpUrl").cloned(),
                        httpRoute:            insert_values.get("httpRoute").cloned(),
                        httpHost:             insert_values.get("httpHost").cloned(),
                        httpStatusCode:       insert_values.get("httpStatusCode").and_then(|s| s.parse().ok()),
                        pathParams:           insert_values.get("pathParams").cloned(),
                        queryParams:          insert_values.get("queryParams").cloned(),
                        requestBody:          insert_values.get("requestBody").cloned(),
                        responseBody:         insert_values.get("responseBody").cloned(),
                        sdkType:              insert_values.get("sdkType").cloned().unwrap_or_default(),
                        serviceVersion:       insert_values.get("serviceVersion").cloned(),
                        errors:               insert_values.get("errors").cloned(),
                        tags:                 insert_values.get("tags").map(|s| vec![s.clone()]).unwrap_or_default(),
                        parentId:             insert_values.get("parentId").cloned(),
                        dbSystem:             insert_values.get("dbSystem").cloned(),
                        dbName:               insert_values.get("dbName").cloned(),
                        dbStatement:          insert_values.get("dbStatement").cloned(),
                        dbOperation:          insert_values.get("dbOperation").cloned(),
                        rpcSystem:            insert_values.get("rpcSystem").cloned(),
                        rpcService:           insert_values.get("rpcService").cloned(),
                        rpcMethod:            insert_values.get("rpcMethod").cloned(),
                        endpointHash:         insert_values.get("endpointHash").cloned().unwrap_or_default(),
                        shapeHash:            insert_values.get("shapeHash").cloned().unwrap_or_default(),
                        formatHashes:         insert_values.get("formatHashes").map(|s| vec![s.clone()]).unwrap_or_default(),
                        fieldHashes:          insert_values.get("fieldHashes").map(|s| vec![s.clone()]).unwrap_or_default(),
                        attributes:           insert_values.get("attributes").cloned(),
                        events:               insert_values.get("events").cloned(),
                        links:                insert_values.get("links").cloned(),
                        resource:             insert_values.get("resource").cloned(),
                        instrumentationScope: insert_values.get("instrumentationScope").cloned(),
                    };
                    self.write(&record).await?;
                    self.refresh_table("telemetry_events").await?;
                    Ok("INSERT 1".to_string())
                } else {
                    Err(anyhow::anyhow!("Unsupported INSERT source"))
                }
            }
            _ => Err(anyhow::anyhow!("Not an INSERT statement")),
        }
    }

    pub async fn refresh_table(&self, table_name: &str) -> Result<()> {
        let (conn_str, table_ref) = {
            let configs = self.project_configs.read().map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))?;
            configs.get(table_name).ok_or_else(|| anyhow::anyhow!("Table not found"))?.clone()
        };

        let new_table = DeltaTableBuilder::from_uri(&conn_str).load().await?;
        *table_ref.write().map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))? = new_table;

        let provider = {
            let table_guard = table_ref.read().map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))?;
            let snapshot = table_guard.snapshot().map_err(|e| anyhow::anyhow!("Snapshot error: {:?}", e))?;
            let log_store = table_guard.log_store().clone();
            deltalake::delta_datafusion::DeltaTableProvider::try_new(snapshot.clone(), log_store, deltalake::delta_datafusion::DeltaScanConfig::default())?
        };

        self.ctx.register_table("telemetry_events", Arc::new(provider))?;
        Ok(())
    }

    pub async fn update_record(&self, query: &str) -> Result<String> {
        tracing::info!("Simulated update: {}", query);
        Ok("UPDATE successful (simulated)".to_string())
    }

    pub async fn delete_record(&self, query: &str) -> Result<String> {
        tracing::info!("Simulated delete: {}", query);
        Ok("DELETE successful (simulated)".to_string())
    }

    pub async fn compact_project(&self, table_name: &str) -> Result<()> {
        let (conn_str, table_ref) = {
            let configs = self.project_configs.read().map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))?;
            configs.get(table_name).ok_or_else(|| anyhow::anyhow!("Table not found"))?.clone()
        };

        let (table, _metrics) = DeltaOps::try_from_uri(&conn_str)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to load table for optimization: {:?}", e))?
            .optimize()
            .await
            .map_err(|e| anyhow::anyhow!("Optimization failed: {:?}", e))?;

        *table_ref.write().map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))? = table;
        COMPACTION_COUNTER.inc();
        tracing::info!("Compaction for table '{}' completed.", table_name);
        Ok(())
    }

    pub async fn compact_all_projects(&self) -> Result<()> {
        let table_names: Vec<String> = {
            let configs = self.project_configs.read().map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))?;
            configs.keys().cloned().collect()
        };
        for table_name in table_names {
            if let Err(e) = self.compact_project(&table_name).await {
                tracing::error!("Error compacting table {}: {:?}", table_name, e);
            }
        }
        Ok(())
    }

    pub fn has_table(&self, table_name: &str) -> bool {
        self.project_configs.read().map(|configs| configs.contains_key(table_name)).unwrap_or(false)
    }
}

/// Helper function to build a TimestampMicrosecondArray.
fn build_timestamp_array(values: Vec<i64>, tz: Option<StdArc<str>>) -> TimestampMicrosecondArray {
    use datafusion::arrow::{array::ArrayData, buffer::Buffer};
    let data_type = DataType::Timestamp(TimeUnit::Microsecond, tz);
    let buffer = Buffer::from_slice_ref(&values);
    let array_data = ArrayData::builder(data_type.clone()).len(values.len()).add_buffer(buffer).build().unwrap();
    TimestampMicrosecondArray::from(array_data)
}
