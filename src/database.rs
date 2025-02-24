use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::array::{StringArray, TimestampMicrosecondArray, Int32Array, Int64Array, ListBuilder, StringBuilder};
use datafusion::common::DataFusionError;
use deltalake::{DeltaTableBuilder, DeltaOps};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::utils::prepare_sql;
use anyhow::Context;
use dotenv::dotenv;
use regex::Regex;
use sqlparser::parser::Parser;
use sqlparser::dialect::GenericDialect;
use sqlparser::ast::{Statement, Expr, Value as SqlValue};
use crate::persistent_queue::IngestRecord;
use chrono::prelude::*;

// Removed unused import: use chrono::prelude::* (we already use chrono::DateTime directly)

// Import COMPACTION_COUNTER from metrics.
use crate::metrics::COMPACTION_COUNTER;

/// Shared mapping of project IDs to (connection string, DeltaTable)
pub type ProjectConfigs = Arc<RwLock<HashMap<String, (String, Arc<RwLock<deltalake::DeltaTable>>)>>>;
 
pub struct Database {
    pub ctx: SessionContext,
    project_configs: ProjectConfigs,
}

impl Database {
    pub async fn new() -> anyhow::Result<Self> {
        dotenv().ok();
        let ctx = SessionContext::new();
        Ok(Self {
            project_configs: Arc::new(RwLock::new(HashMap::new())),
            ctx,
        })
    }

    pub fn get_session_context(&self) -> SessionContext {
        self.ctx.clone()
    }

    pub async fn add_project(&self, project_id: &str, connection_string: &str) -> anyhow::Result<()> {
        let table = match DeltaTableBuilder::from_uri(connection_string).load().await {
            Ok(table) => table,
            Err(_) => {
                DeltaOps::try_from_uri(connection_string)
                    .await?
                    .create()
                    .with_columns(Vec::<deltalake::kernel::StructField>::new())
                    .with_partition_columns(vec!["event_date".to_string()])
                    .await
                    .map_err(|e| DataFusionError::External(e.into()))
                    .context("Creating dummy Delta table failed")?
            }
        };
        self.project_configs.write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock on project configs"))
            .context("Lock error in add_project")?
            .insert(
                project_id.to_string(),
                (connection_string.to_string(), Arc::new(RwLock::new(table))),
            );
        Ok(())
    }

    /// Creates the events table with an extra "event_date" column.
    /// The table is partitioned by "event_date" (formatted as YYYY-MM-DD).
    pub async fn create_events_table(&self, project_id: &str, table_uri: &str) -> anyhow::Result<()> {
        let schema = Schema::new(vec![
            Field::new("project_id", DataType::Utf8, false),
            Field::new("event_date", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), false),
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("trace_state", DataType::Utf8, true),
            Field::new("start_time", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), false),
            Field::new("end_time", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), true),
            Field::new("duration_ns", DataType::Int64, false),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("span_kind", DataType::Utf8, false),
            Field::new("span_type", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, true),
            Field::new("status_code", DataType::Int32, false),
            Field::new("status_message", DataType::Utf8, false),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("host", DataType::Utf8, false),
            Field::new("url_path", DataType::Utf8, false),
            Field::new("raw_url", DataType::Utf8, false),
            Field::new("method", DataType::Utf8, false),
            Field::new("referer", DataType::Utf8, false),
            Field::new("path_params", DataType::Utf8, false),
            Field::new("query_params", DataType::Utf8, false),
            Field::new("request_headers", DataType::Utf8, false),
            Field::new("response_headers", DataType::Utf8, false),
            Field::new("request_body", DataType::Utf8, false),
            Field::new("response_body", DataType::Utf8, false),
            Field::new("endpoint_hash", DataType::Utf8, false),
            Field::new("shape_hash", DataType::Utf8, false),
            Field::new("format_hashes", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("field_hashes", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("sdk_type", DataType::Utf8, false),
            Field::new("service_version", DataType::Utf8, true),
            Field::new("attributes", DataType::Utf8, false),
            Field::new("events", DataType::Utf8, false),
            Field::new("links", DataType::Utf8, false),
            Field::new("resource", DataType::Utf8, false),
            Field::new("instrumentation_scope", DataType::Utf8, false),
            Field::new("errors", DataType::Utf8, false),
            Field::new("tags", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
        ]);
        let columns: Vec<deltalake::kernel::StructField> = schema.fields().iter().map(|f| {
            deltalake::kernel::StructField::new(
                f.name().to_string(),
                if let DataType::Utf8 = f.data_type() {
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String)
                } else if let DataType::Timestamp(_, _) = f.data_type() {
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Timestamp)
                } else if let DataType::Int64 = f.data_type() {
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Long)
                } else if let DataType::Int32 = f.data_type() {
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Integer)
                } else if let DataType::List(_) = f.data_type() {
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String)
                } else {
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String)
                },
                f.is_nullable(),
            )
        }).collect();
        let table = match DeltaTableBuilder::from_uri(table_uri).load().await {
            Ok(table) => table,
            Err(_) => {
                DeltaOps::try_from_uri(table_uri)
                    .await?
                    .create()
                    .with_columns(columns)
                    // Partition by "event_date".
                    .with_partition_columns(vec!["event_date".to_string()])
                    .await
                    .map_err(|e| DataFusionError::External(e.into()))
                    .context("Failed to create events table")?
            }
        };
 
        use deltalake::delta_datafusion::{DeltaTableProvider, DeltaScanConfig};
        let table_ref = Arc::new(RwLock::new(table));
        let provider = {
            let table_guard = table_ref.read().map_err(|_| anyhow::anyhow!("Lock error on delta table"))?;
            let snapshot = table_guard.snapshot().map_err(|e| anyhow::anyhow!("Failed to get table snapshot: {:?}", e))?;
            let log_store = table_guard.log_store().clone();
            DeltaTableProvider::try_new(snapshot.clone(), log_store, DeltaScanConfig::default())
                .map_err(|e| anyhow::anyhow!("Failed to create DeltaTableProvider: {:?}", e))?
        };
        let table_name = format!("table_{}", project_id);
        self.ctx.register_table(&table_name, Arc::new(provider))
            .map_err(|e| anyhow::anyhow!("Failed to register table: {:?}", e))
            .map(|_| ())
    }

    /// Logs the rewritten SQL, prints the EXPLAIN plan, and returns the DataFrame.
    pub async fn query(&self, sql: &str) -> anyhow::Result<DataFrame> {
        let new_sql = prepare_sql(sql).context("Failed to prepare SQL")?;
        println!("Executing SQL: {}", new_sql);
        let df = self.ctx.sql(&new_sql).await.context("Failed to execute SQL")?;
        let df_explain = df.clone().explain(true, false)?;
        let explain_batches = df_explain.collect().await?;
        println!("EXPLAIN plan:");
        for batch in explain_batches {
            println!("{:?}", batch);
        }
        Ok(df)
    }

    /// Writes a new record to the Delta table.
    /// Computes "event_date" from the timestamp.
    pub async fn write(&self, record: &IngestRecord) -> anyhow::Result<()> {
        let conn_str = {
            let configs = self.project_configs.read().map_err(|_| anyhow::anyhow!("Lock error in write"))?;
            configs.get(&record.project_id)
                .ok_or_else(|| anyhow::anyhow!("Project ID '{}' not found", &record.project_id))?
                .0
                .clone()
        };

        // Compute event_date (YYYY-MM-DD)
        let ts = chrono::DateTime::parse_from_rfc3339(&record.timestamp)
            .map_err(|e| anyhow::anyhow!("Invalid timestamp: {:?}", e))?;
        let event_date = ts.format("%Y-%m-%d").to_string();
        let ts_micro = ts.timestamp_micros();
        let start_ts = chrono::DateTime::parse_from_rfc3339(&record.start_time)
            .map_err(|e| anyhow::anyhow!("Invalid start_time: {:?}", e))?;
        let start_ts_micro = start_ts.timestamp_micros();
        let end_ts_micro = record.end_time.as_ref()
            .map(|et| chrono::DateTime::parse_from_rfc3339(et)
                .map_err(|e| anyhow::anyhow!("Invalid end_time: {:?}", e))
                .map(|dt| dt.timestamp_micros()))
            .transpose()?;

        let mut format_hashes_builder = ListBuilder::new(StringBuilder::new());
        for hash in &record.format_hashes {
            format_hashes_builder.values().append_value(hash);
        }
        format_hashes_builder.append(true);
        let format_hashes = format_hashes_builder.finish();

        let mut field_hashes_builder = ListBuilder::new(StringBuilder::new());
        for hash in &record.field_hashes {
            field_hashes_builder.values().append_value(hash);
        }
        field_hashes_builder.append(true);
        let field_hashes = field_hashes_builder.finish();

        let mut tags_builder = ListBuilder::new(StringBuilder::new());
        for tag in &record.tags {
            tags_builder.values().append_value(tag);
        }
        tags_builder.append(true);
        let tags = tags_builder.finish();

        // Schema includes "event_date" as the second field.
        let schema = Schema::new(vec![
            Field::new("project_id", DataType::Utf8, false),
            Field::new("event_date", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), false),
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("trace_state", DataType::Utf8, true),
            Field::new("start_time", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), false),
            Field::new("end_time", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), true),
            Field::new("duration_ns", DataType::Int64, false),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("span_kind", DataType::Utf8, false),
            Field::new("span_type", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, true),
            Field::new("status_code", DataType::Int32, false),
            Field::new("status_message", DataType::Utf8, false),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("host", DataType::Utf8, false),
            Field::new("url_path", DataType::Utf8, false),
            Field::new("raw_url", DataType::Utf8, false),
            Field::new("method", DataType::Utf8, false),
            Field::new("referer", DataType::Utf8, false),
            Field::new("path_params", DataType::Utf8, false),
            Field::new("query_params", DataType::Utf8, false),
            Field::new("request_headers", DataType::Utf8, false),
            Field::new("response_headers", DataType::Utf8, false),
            Field::new("request_body", DataType::Utf8, false),
            Field::new("response_body", DataType::Utf8, false),
            Field::new("endpoint_hash", DataType::Utf8, false),
            Field::new("shape_hash", DataType::Utf8, false),
            Field::new("format_hashes", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("field_hashes", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("sdk_type", DataType::Utf8, false),
            Field::new("service_version", DataType::Utf8, true),
            Field::new("attributes", DataType::Utf8, false),
            Field::new("events", DataType::Utf8, false),
            Field::new("links", DataType::Utf8, false),
            Field::new("resource", DataType::Utf8, false),
            Field::new("instrumentation_scope", DataType::Utf8, false),
            Field::new("errors", DataType::Utf8, false),
            Field::new("tags", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![record.project_id.clone()])),
                Arc::new(StringArray::from(vec![event_date])),
                Arc::new(StringArray::from(vec![record.id.clone()])),
                Arc::new(build_timestamp_array(vec![ts_micro], Some(Arc::from("UTC")))),
                Arc::new(StringArray::from(vec![record.trace_id.clone()])),
                Arc::new(StringArray::from(vec![record.span_id.clone()])),
                Arc::new(StringArray::from_iter(vec![record.parent_span_id.as_ref()])),
                Arc::new(StringArray::from_iter(vec![record.trace_state.as_ref()])),
                Arc::new(build_timestamp_array(vec![start_ts_micro], Some(Arc::from("UTC")))),
                Arc::new(build_timestamp_array(vec![end_ts_micro.unwrap_or(0)], Some(Arc::from("UTC")))),
                Arc::new(Int64Array::from(vec![record.duration_ns])),
                Arc::new(StringArray::from(vec![record.span_name.clone()])),
                Arc::new(StringArray::from(vec![record.span_kind.clone()])),
                Arc::new(StringArray::from(vec![record.span_type.clone()])),
                Arc::new(StringArray::from_iter(vec![record.status.as_ref()])),
                Arc::new(Int32Array::from(vec![record.status_code])),
                Arc::new(StringArray::from(vec![record.status_message.clone()])),
                Arc::new(StringArray::from_iter(vec![record.severity_text.as_ref()])),
                Arc::new(Int32Array::from(vec![record.severity_number])),
                Arc::new(StringArray::from(vec![record.host.clone()])),
                Arc::new(StringArray::from(vec![record.url_path.clone()])),
                Arc::new(StringArray::from(vec![record.raw_url.clone()])),
                Arc::new(StringArray::from(vec![record.method.clone()])),
                Arc::new(StringArray::from(vec![record.referer.clone()])),
                Arc::new(StringArray::from_iter(vec![record.path_params.as_ref()])),
                Arc::new(StringArray::from_iter(vec![record.query_params.as_ref()])),
                Arc::new(StringArray::from_iter(vec![record.request_headers.as_ref()])),
                Arc::new(StringArray::from_iter(vec![record.response_headers.as_ref()])),
                Arc::new(StringArray::from_iter(vec![record.request_body.as_ref()])),
                Arc::new(StringArray::from_iter(vec![record.response_body.as_ref()])),
                Arc::new(StringArray::from(vec![record.endpoint_hash.clone()])),
                Arc::new(StringArray::from(vec![record.shape_hash.clone()])),
                Arc::new(format_hashes),
                Arc::new(field_hashes),
                Arc::new(StringArray::from(vec![record.sdk_type.clone()])),
                Arc::new(StringArray::from_iter(vec![record.service_version.as_ref()])),
                Arc::new(StringArray::from_iter(vec![record.attributes.as_ref()])),
                Arc::new(StringArray::from_iter(vec![record.events.as_ref()])),
                Arc::new(StringArray::from_iter(vec![record.links.as_ref()])),
                Arc::new(StringArray::from_iter(vec![record.resource.as_ref()])),
                Arc::new(StringArray::from_iter(vec![record.instrumentation_scope.as_ref()])),
                Arc::new(StringArray::from_iter(vec![record.errors.as_ref()])),
                Arc::new(tags),
            ],
        ).map_err(|e| anyhow::anyhow!("Failed to create record batch: {:?}", e))?;
        let batches: Vec<RecordBatch> = vec![batch];
        DeltaOps::try_from_uri(&conn_str)
            .await?
            .write(batches.into_iter())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write batch: {:?}", e))?;
 
        Ok(())
    }
    
    /// Parses an INSERT statement and writes a new record.
    pub async fn insert_record(&self, query: &str) -> anyhow::Result<String> {
        let dialect = GenericDialect {};
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, query)
            .map_err(|e| anyhow::anyhow!("SQL parse error: {:?}", e))?;
        if let Statement::Insert(insert) = &ast[0] {
            let table_name_str = insert.table.to_string();
            if table_name_str != "\"table\"" {
                return Err(anyhow::anyhow!("Only inserts into \"table\" are supported"));
            }
            let source_query = insert.source.as_ref().ok_or_else(|| anyhow::anyhow!("Missing source in INSERT"))?;
            if let sqlparser::ast::SetExpr::Values(values) = &*source_query.body {
                let rows = &values.rows;
                if rows.len() != 1 {
                    return Err(anyhow::anyhow!("Only single row inserts are supported"));
                }
                let row = &rows[0];
                let mut insert_values = HashMap::new();
                for (col, val) in insert.columns.iter().zip(row.iter()) {
                    if let Expr::Value(SqlValue::SingleQuotedString(s)) = val {
                        insert_values.insert(col.value.clone(), s.clone());
                    } else {
                        return Err(anyhow::anyhow!("Unsupported value type in INSERT"));
                    }
                }
                self.write(&IngestRecord {
                    project_id: insert_values.get("project_id")
                        .ok_or_else(|| anyhow::anyhow!("Missing project_id"))?
                        .clone(),
                    id: insert_values.get("id")
                        .cloned()
                        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                    timestamp: insert_values.get("timestamp")
                        .ok_or_else(|| anyhow::anyhow!("Missing timestamp"))?
                        .clone(),
                    trace_id: insert_values.get("trace_id").cloned().unwrap_or_default(),
                    span_id: insert_values.get("span_id").cloned().unwrap_or_default(),
                    parent_span_id: insert_values.get("parent_span_id").cloned(),
                    trace_state: insert_values.get("trace_state").cloned(),
                    start_time: insert_values.get("start_time")
                        .cloned()
                        .unwrap_or_default(),
                    end_time: insert_values.get("end_time").cloned(),
                    duration_ns: insert_values.get("duration_ns")
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0),
                    span_name: insert_values.get("span_name").cloned().unwrap_or_default(),
                    span_kind: insert_values.get("span_kind").cloned().unwrap_or_default(),
                    span_type: insert_values.get("span_type").cloned().unwrap_or_default(),
                    status: insert_values.get("status").cloned(),
                    status_code: insert_values.get("status_code")
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0),
                    status_message: insert_values.get("status_message").cloned().unwrap_or_default(),
                    severity_text: insert_values.get("severity_text").cloned(),
                    severity_number: insert_values.get("severity_number")
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0),
                    host: insert_values.get("host").cloned().unwrap_or_default(),
                    url_path: insert_values.get("url_path").cloned().unwrap_or_default(),
                    raw_url: insert_values.get("raw_url").cloned().unwrap_or_default(),
                    method: insert_values.get("method").cloned().unwrap_or_default(),
                    referer: insert_values.get("referer").cloned().unwrap_or_default(),
                    path_params: insert_values.get("path_params").cloned(),
                    query_params: insert_values.get("query_params").cloned(),
                    request_headers: insert_values.get("request_headers").cloned(),
                    response_headers: insert_values.get("response_headers").cloned(),
                    request_body: insert_values.get("request_body").cloned(),
                    response_body: insert_values.get("response_body").cloned(),
                    endpoint_hash: insert_values.get("endpoint_hash").cloned().unwrap_or_default(),
                    shape_hash: insert_values.get("shape_hash").cloned().unwrap_or_default(),
                    format_hashes: insert_values.get("format_hashes")
                        .map(|s| vec![s.clone()])
                        .unwrap_or_default(),
                    field_hashes: insert_values.get("field_hashes")
                        .map(|s| vec![s.clone()])
                        .unwrap_or_default(),
                    sdk_type: insert_values.get("sdk_type").cloned().unwrap_or_default(),
                    service_version: insert_values.get("service_version").cloned(),
                    attributes: insert_values.get("attributes").cloned(),
                    events: insert_values.get("events").cloned(),
                    links: insert_values.get("links").cloned(),
                    resource: insert_values.get("resource").cloned(),
                    instrumentation_scope: insert_values.get("instrumentation_scope").cloned(),
                    errors: insert_values.get("errors").cloned(),
                    tags: insert_values.get("tags")
                        .map(|s| vec![s.clone()])
                        .unwrap_or_default(),
                }).await?;
                Ok("INSERT successful".to_string())
            } else {
                Err(anyhow::anyhow!("Unsupported INSERT source"))
            }
        } else {
            Err(anyhow::anyhow!("Not an INSERT statement"))
        }
    }
    
    /// Simulated update operation.
    pub async fn update_record(&self, query: &str) -> anyhow::Result<String> {
        println!("Simulated update: {}", query);
        Ok("UPDATE successful (simulated)".to_string())
    }
    
    /// Simulated delete operation.
    pub async fn delete_record(&self, query: &str) -> anyhow::Result<String> {
        println!("Simulated delete: {}", query);
        Ok("DELETE successful (simulated)".to_string())
    }

    /// Runs a production-ready compaction by issuing an OPTIMIZE command via SQL and reloading the table.
    pub async fn compact_project(&self, project_id: &str) -> anyhow::Result<()> {
        let (conn_str, table_arc) = {
            let configs = self.project_configs.read().map_err(|_| anyhow::anyhow!("Lock error in compact_project"))?;
            configs.get(project_id)
                .ok_or_else(|| anyhow::anyhow!("Project ID '{}' not found", project_id))?
                .clone()
        };
        let table_name = format!("table_{}", project_id);
        // Issue the OPTIMIZE command.
        let optimize_sql = format!("OPTIMIZE \"{}\"", table_name);
        self.ctx.sql(&optimize_sql).await.context("Failed to run OPTIMIZE command")?;
        // Reload the table.
        let new_table = DeltaTableBuilder::from_uri(&conn_str).load().await?;
        *table_arc.write().map_err(|_| anyhow::anyhow!("Lock error updating delta table"))? = new_table;
        // Increment compaction counter.
        COMPACTION_COUNTER.inc();
        println!("Compaction for project '{}' completed.", project_id);
        Ok(())
    }

    /// Iterates over all projects and compacts each table.
    pub async fn compact_all_projects(&self) -> anyhow::Result<()> {
        let project_ids: Vec<String> = {
            let configs = self.project_configs.read().map_err(|_| anyhow::anyhow!("Lock error in compact_all_projects"))?;
            configs.keys().cloned().collect()
        };
        for project_id in project_ids {
            if let Err(e) = self.compact_project(&project_id).await {
                eprintln!("Error compacting project {}: {:?}", project_id, e);
            }
        }
        Ok(())
    }

    pub fn has_project(&self, project_id: &str) -> bool {
        self.project_configs.read().unwrap().contains_key(project_id)
    }
}

/// Helper to build a TimestampMicrosecondArray.
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
