use datafusion::prelude::*;
use datafusion::execution::context::SessionContext;
use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use deltalake::arrow::array::{self as delta_arrow, TimestampMicrosecondArray};
use deltalake::arrow::buffer::{ScalarBuffer, NullBuffer};
use deltalake::{DeltaTableBuilder, DeltaOps, DeltaTable};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::utils::prepare_sql;
use anyhow::Result;
use sqlparser::dialect::GenericDialect;
use sqlparser::ast::{Statement, Expr, Value as SqlValue, SetExpr};
use chrono::DateTime;
use crate::metrics::COMPACTION_COUNTER;
use dotenv::dotenv;
use std::sync::Arc as StdArc;

pub type ProjectConfigs = Arc<RwLock<HashMap<String, (String, Arc<RwLock<DeltaTable>>)>>>;

pub struct Database {
    pub ctx: SessionContext,
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
        self.project_configs.write()
            .map_err(|e| anyhow::anyhow!("Failed to acquire write lock: {:?}", e))?
            .insert(table_name.to_string(), (connection_string.to_string(), Arc::new(RwLock::new(table))));
        Ok(())
    }

    pub fn event_schema() -> Schema {
        Schema::new(vec![
            Field::new("project_id", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some(StdArc::from("UTC"))), false),
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, true),
            Field::new("end_time", DataType::Timestamp(TimeUnit::Microsecond, Some(StdArc::from("UTC"))), true),
            Field::new("duration_ns", DataType::Int64, false),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("span_kind", DataType::Utf8, true),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("trace_state", DataType::Utf8, true),
            Field::new("has_error", DataType::Boolean, false),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("body", DataType::Utf8, true),
            Field::new("http_method", DataType::Utf8, true),
            Field::new("http_url", DataType::Utf8, true),
            Field::new("http_route", DataType::Utf8, true),
            Field::new("http_host", DataType::Utf8, true),
            Field::new("http_status_code", DataType::Int32, true),
            Field::new("path_params", DataType::Utf8, false),
            Field::new("query_params", DataType::Utf8, false),
            Field::new("request_body", DataType::Utf8, false),
            Field::new("response_body", DataType::Utf8, false),
            Field::new("sdk_type", DataType::Utf8, false),
            Field::new("service_version", DataType::Utf8, true),
            Field::new("errors", DataType::Utf8, false),
            Field::new("tags", DataType::List(StdArc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("parent_id", DataType::Utf8, true),
            Field::new("db_system", DataType::Utf8, true),
            Field::new("db_name", DataType::Utf8, true),
            Field::new("db_statement", DataType::Utf8, true),
            Field::new("db_operation", DataType::Utf8, true),
            Field::new("rpc_system", DataType::Utf8, true),
            Field::new("rpc_service", DataType::Utf8, true),
            Field::new("rpc_method", DataType::Utf8, true),
            Field::new("endpoint_hash", DataType::Utf8, false),
            Field::new("shape_hash", DataType::Utf8, false),
            Field::new("format_hashes", DataType::List(StdArc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("field_hashes", DataType::List(StdArc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("attributes", DataType::Utf8, false),
            Field::new("events", DataType::Utf8, false),
            Field::new("links", DataType::Utf8, false),
            Field::new("resource", DataType::Utf8, false),
            Field::new("instrumentation_scope", DataType::Utf8, false),
        ])
    }

    fn event_schema_fields() -> Vec<deltalake::kernel::StructField> {
        let schema = Self::event_schema();
        schema.fields().iter().map(|f| {
            match f.data_type() {
                DataType::Utf8 => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String),
                    f.is_nullable()
                ),
                DataType::Timestamp(_, _) => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Timestamp),
                    f.is_nullable()
                ),
                DataType::Int64 => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Long),
                    f.is_nullable()
                ),
                DataType::Int32 => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Integer),
                    f.is_nullable()
                ),
                DataType::Boolean => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Boolean),
                    f.is_nullable()
                ),
                DataType::List(_) => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String),
                    f.is_nullable()
                ),
                _ => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String),
                    f.is_nullable()
                ),
            }
        }).collect()
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

        // Create the DeltaTableProvider and cast it into the expected trait object.
        let provider_raw = deltalake::delta_datafusion::DeltaTableProvider::try_new(
            table.state.clone().expect("Table state should be loaded"),
            table.log_store().clone(),
            deltalake::delta_datafusion::DeltaScanConfig::default(),
        )?;
        let provider: Arc<dyn datafusion::datasource::TableProvider> = Arc::new(provider_raw);
        self.ctx.register_table("telemetry_events", provider)?;

        let table_ref = Arc::new(RwLock::new(table));
        self.project_configs.write()
            .map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))?
            .insert(table_name.to_string(), (table_uri.to_string(), table_ref));

        Ok(())
    }

    pub async fn query(&self, sql: &str) -> Result<DataFrame> {
        let new_sql = prepare_sql(sql)?;
        tracing::info!("Executing SQL: {}", new_sql);
        let df = self.ctx.sql(&new_sql)
            .await
            .map_err(|e| anyhow::anyhow!("SQL execution failed: {:?}", e))?;
        Ok(df)
    }

    pub async fn write(&self, record: &crate::persistent_queue::IngestRecord) -> Result<()> {
        let (conn_str, _table_ref) = {
            let configs = self.project_configs.read().map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))?;
            configs.get("telemetry_events").ok_or_else(|| anyhow::anyhow!("Table not found"))?.clone()
        };

        let ts = DateTime::parse_from_rfc3339(&record.timestamp)
            .map_err(|e| anyhow::anyhow!("Invalid timestamp: {:?}", e))?;
        let ts_micro = ts.timestamp_micros();
        let end_ts_micro = record.end_time.as_ref()
            .map(|et| DateTime::parse_from_rfc3339(et).map(|dt| dt.timestamp_micros()))
            .transpose()?;

        let schema = Arc::new(Self::event_schema());
        let batch = delta_arrow::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(delta_arrow::StringArray::from(vec![record.project_id.clone()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.id.clone()])),
                Arc::new(delta_arrow::TimestampMicrosecondArray::new(
                    ScalarBuffer::from(vec![ts_micro]),
                    None,
                )),
                Arc::new(delta_arrow::StringArray::from(vec![record.trace_id.clone()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.span_id.clone()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.event_type.clone()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.status.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::TimestampMicrosecondArray::new(
                    ScalarBuffer::from(vec![end_ts_micro.unwrap_or(0)]),
                    end_ts_micro.is_none().then(|| NullBuffer::new_null(1)),
                )),
                Arc::new(delta_arrow::Int64Array::from(vec![record.duration_ns])),
                Arc::new(delta_arrow::StringArray::from(vec![record.span_name.clone()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.span_kind.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.parent_span_id.as_deref().unwrap_or("")])),
                Arc::new(delta_arrow::StringArray::from(vec![record.trace_state.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::BooleanArray::from(vec![record.has_error])),
                Arc::new(delta_arrow::StringArray::from(vec![record.severity_text.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::Int32Array::from(vec![record.severity_number])),
                Arc::new(delta_arrow::StringArray::from(vec![record.body.clone().unwrap_or_else(|| "{}".to_string())])),
                Arc::new(delta_arrow::StringArray::from(vec![record.http_method.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.http_url.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.http_route.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.http_host.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::Int32Array::from(vec![record.http_status_code.unwrap_or(0)])),
                Arc::new(delta_arrow::StringArray::from(vec![record.path_params.clone().unwrap_or_else(|| "{}".to_string())])),
                Arc::new(delta_arrow::StringArray::from(vec![record.query_params.clone().unwrap_or_else(|| "{}".to_string())])),
                Arc::new(delta_arrow::StringArray::from(vec![record.request_body.clone().unwrap_or_else(|| "{}".to_string())])),
                Arc::new(delta_arrow::StringArray::from(vec![record.response_body.clone().unwrap_or_else(|| "{}".to_string())])),
                Arc::new(delta_arrow::StringArray::from(vec![record.sdk_type.clone()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.service_version.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.errors.clone().unwrap_or_else(|| "{}".to_string())])),
                {
                    let mut tags_builder = delta_arrow::ListBuilder::new(delta_arrow::StringBuilder::new());
                    for tag in &record.tags {
                        tags_builder.values().append_value(tag);
                    }
                    tags_builder.append(true);
                    Arc::new(tags_builder.finish())
                },
                Arc::new(delta_arrow::StringArray::from(vec![record.parent_id.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.db_system.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.db_name.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.db_statement.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.db_operation.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.rpc_system.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.rpc_service.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.rpc_method.clone().unwrap_or_default()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.endpoint_hash.clone()])),
                Arc::new(delta_arrow::StringArray::from(vec![record.shape_hash.clone()])),
                {
                    let mut format_hashes_builder = delta_arrow::ListBuilder::new(delta_arrow::StringBuilder::new());
                    for hash in &record.format_hashes {
                        format_hashes_builder.values().append_value(hash);
                    }
                    format_hashes_builder.append(true);
                    Arc::new(format_hashes_builder.finish())
                },
                {
                    let mut field_hashes_builder = delta_arrow::ListBuilder::new(delta_arrow::StringBuilder::new());
                    for hash in &record.field_hashes {
                        field_hashes_builder.values().append_value(hash);
                    }
                    field_hashes_builder.append(true);
                    Arc::new(field_hashes_builder.finish())
                },
                Arc::new(delta_arrow::StringArray::from(vec![record.attributes.clone().unwrap_or_else(|| "{}".to_string())])),
                Arc::new(delta_arrow::StringArray::from(vec![record.events.clone().unwrap_or_else(|| "{}".to_string())])),
                Arc::new(delta_arrow::StringArray::from(vec![record.links.clone().unwrap_or_else(|| "{}".to_string())])),
                Arc::new(delta_arrow::StringArray::from(vec![record.resource.clone().unwrap_or_else(|| "{}".to_string())])),
                Arc::new(delta_arrow::StringArray::from(vec![record.instrumentation_scope.clone().unwrap_or_else(|| "{}".to_string())])),
            ],
        )?;

        DeltaOps::try_from_uri(&conn_str)
            .await?
            .write(vec![batch])
            .await?;
        Ok(())
    }

    pub async fn insert_record(&self, query: &str) -> Result<String> {
        let dialect = GenericDialect {};
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, query)
            .map_err(|e| anyhow::anyhow!("SQL parse error: {:?}", e))?;

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
                            Expr::Value(SqlValue::SingleQuotedString(s)) => {
                                insert_values.insert(col.to_string(), s.clone());
                            }
                            Expr::Value(SqlValue::Number(n, _)) => {
                                insert_values.insert(col.to_string(), n.clone());
                            }
                            _ => return Err(anyhow::anyhow!("Unsupported value type: {:?}", val)),
                        }
                    }
                    let project_id = insert_values.get("project_id")
                        .ok_or_else(|| anyhow::anyhow!("Missing project_id"))?;
                    let timestamp = insert_values.get("timestamp")
                        .ok_or_else(|| anyhow::anyhow!("Missing timestamp"))?;
                    let record = crate::persistent_queue::IngestRecord {
                        project_id: project_id.clone(),
                        id: insert_values.get("id").cloned().unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                        timestamp: timestamp.clone(),
                        trace_id: insert_values.get("trace_id").cloned().unwrap_or_default(),
                        span_id: insert_values.get("span_id").cloned().unwrap_or_default(),
                        event_type: insert_values.get("event_type").cloned().unwrap_or_else(|| "span".to_string()),
                        status: insert_values.get("status").cloned(),
                        end_time: insert_values.get("end_time").cloned(),
                        duration_ns: insert_values.get("duration_ns").and_then(|s| s.parse().ok()).unwrap_or(0),
                        span_name: insert_values.get("span_name").cloned().unwrap_or_default(),
                        span_kind: insert_values.get("span_kind").cloned(),
                        parent_span_id: insert_values.get("parent_span_id").cloned(),
                        trace_state: insert_values.get("trace_state").cloned(),
                        has_error: false,
                        severity_text: insert_values.get("severity_text").cloned(),
                        severity_number: insert_values.get("severity_number").and_then(|s| s.parse().ok()).unwrap_or(0),
                        body: insert_values.get("body").cloned(),
                        http_method: insert_values.get("http_method").cloned(),
                        http_url: insert_values.get("http_url").cloned(),
                        http_route: insert_values.get("http_route").cloned(),
                        http_host: insert_values.get("http_host").cloned(),
                        http_status_code: insert_values.get("http_status_code").and_then(|s| s.parse().ok()),
                        path_params: insert_values.get("path_params").cloned(),
                        query_params: insert_values.get("query_params").cloned(),
                        request_body: insert_values.get("request_body").cloned(),
                        response_body: insert_values.get("response_body").cloned(),
                        sdk_type: insert_values.get("sdk_type").cloned().unwrap_or_default(),
                        service_version: insert_values.get("service_version").cloned(),
                        errors: insert_values.get("errors").cloned(),
                        tags: insert_values.get("tags").map(|s| vec![s.clone()]).unwrap_or_default(),
                        parent_id: insert_values.get("parent_id").cloned(),
                        db_system: insert_values.get("db_system").cloned(),
                        db_name: insert_values.get("db_name").cloned(),
                        db_statement: insert_values.get("db_statement").cloned(),
                        db_operation: insert_values.get("db_operation").cloned(),
                        rpc_system: insert_values.get("rpc_system").cloned(),
                        rpc_service: insert_values.get("rpc_service").cloned(),
                        rpc_method: insert_values.get("rpc_method").cloned(),
                        endpoint_hash: insert_values.get("endpoint_hash").cloned().unwrap_or_default(),
                        shape_hash: insert_values.get("shape_hash").cloned().unwrap_or_default(),
                        format_hashes: insert_values.get("format_hashes").map(|s| vec![s.clone()]).unwrap_or_default(),
                        field_hashes: insert_values.get("field_hashes").map(|s| vec![s.clone()]).unwrap_or_default(),
                        attributes: insert_values.get("attributes").cloned(),
                        events: insert_values.get("events").cloned(),
                        links: insert_values.get("links").cloned(),
                        resource: insert_values.get("resource").cloned(),
                        instrumentation_scope: insert_values.get("instrumentation_scope").cloned(),
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
        *table_ref.write().map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))? = new_table.clone();

        let provider_raw = deltalake::delta_datafusion::DeltaTableProvider::try_new(
            new_table.state.clone().expect("Table state should be loaded"),
            new_table.log_store().clone(),
            deltalake::delta_datafusion::DeltaScanConfig::default(),
        )?;
        let provider: Arc<dyn datafusion::datasource::TableProvider> = Arc::new(provider_raw);
        self.ctx.register_table("telemetry_events", provider)?;

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
        self.project_configs.read()
            .map(|configs| configs.contains_key(table_name))
            .unwrap_or(false)
    }
}

#[allow(dead_code)]
fn build_timestamp_array(values: Vec<i64>, _tz: Option<StdArc<str>>) -> TimestampMicrosecondArray {
    let buffer = ScalarBuffer::from(values);
    TimestampMicrosecondArray::new(buffer, None)
}
