// database.rs

use std::{
    collections::HashMap,
    sync::{Arc as StdArc, Arc, RwLock},
};

use anyhow::Result;
use datafusion::{
    arrow::{
        array::{
            BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
        },
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

    pub async fn add_project(&self, table_name: &str, connection_string: &str) -> Result<()> {
        let table = match DeltaTableBuilder::from_uri(connection_string).load().await {
            Ok(table) => table,
            Err(e) => {
                tracing::warn!("Failed to load table '{}': {}. Creating new table.", connection_string, e);
                DeltaOps::try_from_uri(connection_string)
                    .await?
                    .create()
                    .with_columns(Self::event_schema_fields())
                    .with_partition_columns(vec!["start_time_unix_nano".to_string()])
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

    fn event_schema() -> Schema {
        Schema::new(vec![
            // Core span fields
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("trace_state", DataType::Utf8, true),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, true),
            Field::new("start_time_unix_nano", DataType::Timestamp(TimeUnit::Nanosecond, Some(StdArc::from("UTC"))), false),
            Field::new("end_time_unix_nano", DataType::Timestamp(TimeUnit::Nanosecond, Some(StdArc::from("UTC"))), true),

            // Span attributes
            Field::new("http_method", DataType::Utf8, true),
            Field::new("http_url", DataType::Utf8, true),
            Field::new("http_status_code", DataType::Int32, true),
            Field::new("http_request_content_length", DataType::Int64, true),
            Field::new("http_response_content_length", DataType::Int64, true),
            Field::new("http_route", DataType::Utf8, true),
            Field::new("http_scheme", DataType::Utf8, true),
            Field::new("http_client_ip", DataType::Utf8, true),
            Field::new("http_user_agent", DataType::Utf8, true),
            Field::new("http_flavor", DataType::Utf8, true),
            Field::new("http_target", DataType::Utf8, true),
            Field::new("http_host", DataType::Utf8, true),
            Field::new("rpc_system", DataType::Utf8, true),
            Field::new("rpc_service", DataType::Utf8, true),
            Field::new("rpc_method", DataType::Utf8, true),
            Field::new("rpc_grpc_status_code", DataType::Int32, true),
            Field::new("db_system", DataType::Utf8, true),
            Field::new("db_connection_string", DataType::Utf8, true),
            Field::new("db_user", DataType::Utf8, true),
            Field::new("db_name", DataType::Utf8, true),
            Field::new("db_statement", DataType::Utf8, true),
            Field::new("db_operation", DataType::Utf8, true),
            Field::new("db_sql_table", DataType::Utf8, true),
            Field::new("messaging_system", DataType::Utf8, true),
            Field::new("messaging_destination", DataType::Utf8, true),
            Field::new("messaging_destination_kind", DataType::Utf8, true),
            Field::new("messaging_message_id", DataType::Utf8, true),
            Field::new("messaging_operation", DataType::Utf8, true),
            Field::new("messaging_url", DataType::Utf8, true),
            Field::new("messaging_client_id", DataType::Utf8, true),
            Field::new("messaging_kafka_partition", DataType::Int32, true),
            Field::new("messaging_kafka_offset", DataType::Int64, true),
            Field::new("messaging_kafka_consumer_group", DataType::Utf8, true),
            Field::new("messaging_message_payload_size_bytes", DataType::Int64, true),
            Field::new("messaging_protocol", DataType::Utf8, true),
            Field::new("messaging_protocol_version", DataType::Utf8, true),
            Field::new("cache_system", DataType::Utf8, true),
            Field::new("cache_operation", DataType::Utf8, true),
            Field::new("cache_key", DataType::Utf8, true),
            Field::new("cache_hit", DataType::Boolean, true),
            Field::new("net_peer_ip", DataType::Utf8, true),
            Field::new("net_peer_port", DataType::Int32, true),
            Field::new("net_host_ip", DataType::Utf8, true),
            Field::new("net_host_port", DataType::Int32, true),
            Field::new("net_transport", DataType::Utf8, true),
            Field::new("enduser_id", DataType::Utf8, true),
            Field::new("enduser_role", DataType::Utf8, true),
            Field::new("enduser_scope", DataType::Utf8, true),
            Field::new("exception_type", DataType::Utf8, true),
            Field::new("exception_message", DataType::Utf8, true),
            Field::new("exception_stacktrace", DataType::Utf8, true),
            Field::new("exception_escaped", DataType::Boolean, true),
            Field::new("thread_id", DataType::Int64, true),
            Field::new("thread_name", DataType::Utf8, true),
            Field::new("code_function", DataType::Utf8, true),
            Field::new("code_filepath", DataType::Utf8, true),
            Field::new("code_namespace", DataType::Utf8, true),
            Field::new("code_lineno", DataType::Int32, true),
            Field::new("deployment_environment", DataType::Utf8, true),
            Field::new("deployment_version", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("service_version", DataType::Utf8, true),
            Field::new("service_instance_id", DataType::Utf8, true),
            Field::new("otel_library_name", DataType::Utf8, true),
            Field::new("otel_library_version", DataType::Utf8, true),
            Field::new("k8s_pod_name", DataType::Utf8, true),
            Field::new("k8s_namespace_name", DataType::Utf8, true),
            Field::new("k8s_deployment_name", DataType::Utf8, true),
            Field::new("container_id", DataType::Utf8, true),
            Field::new("host_name", DataType::Utf8, true),
            Field::new("os_type", DataType::Utf8, true),
            Field::new("os_version", DataType::Utf8, true),
            Field::new("process_pid", DataType::Int64, true),
            Field::new("process_command_line", DataType::Utf8, true),
            Field::new("process_runtime_name", DataType::Utf8, true),
            Field::new("process_runtime_version", DataType::Utf8, true),
            Field::new("aws_region", DataType::Utf8, true),
            Field::new("aws_account_id", DataType::Utf8, true),
            Field::new("aws_dynamodb_table_name", DataType::Utf8, true),
            Field::new("aws_dynamodb_operation", DataType::Utf8, true),
            Field::new("aws_dynamodb_consumed_capacity_total", DataType::Float64, true),
            Field::new("aws_sqs_queue_url", DataType::Utf8, true),
            Field::new("aws_sqs_message_id", DataType::Utf8, true),
            Field::new("azure_resource_id", DataType::Utf8, true),
            Field::new("azure_storage_container_name", DataType::Utf8, true),
            Field::new("azure_storage_blob_name", DataType::Utf8, true),
            Field::new("gcp_project_id", DataType::Utf8, true),
            Field::new("gcp_cloudsql_instance_id", DataType::Utf8, true),
            Field::new("gcp_pubsub_message_id", DataType::Utf8, true),
            Field::new("http_request_method", DataType::Utf8, true),
            Field::new("db_instance_identifier", DataType::Utf8, true),
            Field::new("db_rows_affected", DataType::Int64, true),
            Field::new("net_sock_peer_addr", DataType::Utf8, true),
            Field::new("net_sock_peer_port", DataType::Int32, true),
            Field::new("net_sock_host_addr", DataType::Utf8, true),
            Field::new("net_sock_host_port", DataType::Int32, true),
            Field::new("messaging_consumer_id", DataType::Utf8, true),
            Field::new("messaging_message_payload_compressed_size_bytes", DataType::Int64, true),
            Field::new("faas_invocation_id", DataType::Utf8, true),
            Field::new("faas_trigger", DataType::Utf8, true),
            Field::new("cloud_zone", DataType::Utf8, true),

            // Resource attributes
            Field::new("resource_attributes_service_name", DataType::Utf8, true),
            Field::new("resource_attributes_service_version", DataType::Utf8, true),
            Field::new("resource_attributes_service_instance_id", DataType::Utf8, true),
            Field::new("resource_attributes_service_namespace", DataType::Utf8, true),
            Field::new("resource_attributes_host_name", DataType::Utf8, true),
            Field::new("resource_attributes_host_id", DataType::Utf8, true),
            Field::new("resource_attributes_host_type", DataType::Utf8, true),
            Field::new("resource_attributes_host_arch", DataType::Utf8, true),
            Field::new("resource_attributes_os_type", DataType::Utf8, true),
            Field::new("resource_attributes_os_version", DataType::Utf8, true),
            Field::new("resource_attributes_process_pid", DataType::Int64, true),
            Field::new("resource_attributes_process_executable_name", DataType::Utf8, true),
            Field::new("resource_attributes_process_command_line", DataType::Utf8, true),
            Field::new("resource_attributes_process_runtime_name", DataType::Utf8, true),
            Field::new("resource_attributes_process_runtime_version", DataType::Utf8, true),
            Field::new("resource_attributes_process_runtime_description", DataType::Utf8, true),
            Field::new("resource_attributes_process_executable_path", DataType::Utf8, true),
            Field::new("resource_attributes_k8s_cluster_name", DataType::Utf8, true),
            Field::new("resource_attributes_k8s_namespace_name", DataType::Utf8, true),
            Field::new("resource_attributes_k8s_deployment_name", DataType::Utf8, true),
            Field::new("resource_attributes_k8s_pod_name", DataType::Utf8, true),
            Field::new("resource_attributes_k8s_pod_uid", DataType::Utf8, true),
            Field::new("resource_attributes_k8s_replicaset_name", DataType::Utf8, true),
            Field::new("resource_attributes_k8s_deployment_strategy", DataType::Utf8, true),
            Field::new("resource_attributes_k8s_container_name", DataType::Utf8, true),
            Field::new("resource_attributes_k8s_node_name", DataType::Utf8, true),
            Field::new("resource_attributes_container_id", DataType::Utf8, true),
            Field::new("resource_attributes_container_image_name", DataType::Utf8, true),
            Field::new("resource_attributes_container_image_tag", DataType::Utf8, true),
            Field::new("resource_attributes_deployment_environment", DataType::Utf8, true),
            Field::new("resource_attributes_deployment_version", DataType::Utf8, true),
            Field::new("resource_attributes_cloud_provider", DataType::Utf8, true),
            Field::new("resource_attributes_cloud_platform", DataType::Utf8, true),
            Field::new("resource_attributes_cloud_region", DataType::Utf8, true),
            Field::new("resource_attributes_cloud_availability_zone", DataType::Utf8, true),
            Field::new("resource_attributes_cloud_account_id", DataType::Utf8, true),
            Field::new("resource_attributes_cloud_resource_id", DataType::Utf8, true),
            Field::new("resource_attributes_cloud_instance_type", DataType::Utf8, true),
            Field::new("resource_attributes_telemetry_sdk_name", DataType::Utf8, true),
            Field::new("resource_attributes_telemetry_sdk_language", DataType::Utf8, true),
            Field::new("resource_attributes_telemetry_sdk_version", DataType::Utf8, true),
            Field::new("resource_attributes_application_name", DataType::Utf8, true),
            Field::new("resource_attributes_application_version", DataType::Utf8, true),
            Field::new("resource_attributes_application_tier", DataType::Utf8, true),
            Field::new("resource_attributes_application_owner", DataType::Utf8, true),
            Field::new("resource_attributes_customer_id", DataType::Utf8, true),
            Field::new("resource_attributes_tenant_id", DataType::Utf8, true),
            Field::new("resource_attributes_feature_flag_enabled", DataType::Boolean, true),
            Field::new("resource_attributes_payment_gateway", DataType::Utf8, true),
            Field::new("resource_attributes_database_type", DataType::Utf8, true),
            Field::new("resource_attributes_database_instance", DataType::Utf8, true),
            Field::new("resource_attributes_cache_provider", DataType::Utf8, true),
            Field::new("resource_attributes_message_queue_type", DataType::Utf8, true),
            Field::new("resource_attributes_http_route", DataType::Utf8, true),
            Field::new("resource_attributes_aws_ecs_cluster_arn", DataType::Utf8, true),
            Field::new("resource_attributes_aws_ecs_container_arn", DataType::Utf8, true),
            Field::new("resource_attributes_aws_ecs_task_arn", DataType::Utf8, true),
            Field::new("resource_attributes_aws_ecs_task_family", DataType::Utf8, true),
            Field::new("resource_attributes_aws_ec2_instance_id", DataType::Utf8, true),
            Field::new("resource_attributes_gcp_project_id", DataType::Utf8, true),
            Field::new("resource_attributes_gcp_zone", DataType::Utf8, true),
            Field::new("resource_attributes_azure_resource_id", DataType::Utf8, true),
            Field::new("resource_attributes_dynatrace_entity_process_id", DataType::Utf8, true),
            Field::new("resource_attributes_elastic_node_name", DataType::Utf8, true),
            Field::new("resource_attributes_istio_mesh_id", DataType::Utf8, true),
            Field::new("resource_attributes_cloudfoundry_application_id", DataType::Utf8, true),
            Field::new("resource_attributes_cloudfoundry_space_id", DataType::Utf8, true),
            Field::new("resource_attributes_opentelemetry_collector_name", DataType::Utf8, true),
            Field::new("resource_attributes_instrumentation_name", DataType::Utf8, true),
            Field::new("resource_attributes_instrumentation_version", DataType::Utf8, true),
            Field::new("resource_attributes_log_source", DataType::Utf8, true),

            // Nested structures
            Field::new("events", DataType::Utf8, true),
            Field::new("links", DataType::Utf8, true),
            Field::new("status_code", DataType::Utf8, true),
            Field::new("status_message", DataType::Utf8, true),
            Field::new("instrumentation_library_name", DataType::Utf8, true),
            Field::new("instrumentation_library_version", DataType::Utf8, true),
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
                DataType::Float64 => deltalake::kernel::StructField::new(
                    f.name().to_string(),
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Double),
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
                    .with_partition_columns(vec!["start_time_unix_nano".to_string()])
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to create events table: {:?}", e))?
            }
        };

        let table_ref = Arc::new(RwLock::new(table));
        let provider = {
            let table_guard = table_ref.read().map_err(|_| anyhow::anyhow!("Lock error"))?;
            let snapshot = table_guard.snapshot().map_err(|e| anyhow::anyhow!("Failed to get snapshot: {:?}", e))?;
            let log_store = table_guard.log_store().clone();
            deltalake::delta_datafusion::DeltaTableProvider::try_new(
                snapshot.clone(),
                log_store,
                deltalake::delta_datafusion::DeltaScanConfig::default(),
            )
            .map_err(|e| anyhow::anyhow!("Failed to create provider: {:?}", e))?
        };

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

    pub async fn write(&self, record: &crate::persistent_queue::IngestRecord) -> Result<()> {
        let (conn_str, _table_ref) = {
            let configs = self.project_configs.read().map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))?;
            configs.get("telemetry_events").ok_or_else(|| anyhow::anyhow!("Table not found"))?.clone()
        };

        let ts_nano = record.start_time_unix_nano;
        let end_ts_nano = record.end_time_unix_nano;

        let trace_id_array = StringArray::from(vec![record.trace_id.clone()]);
        let span_id_array = StringArray::from(vec![record.span_id.clone()]);
        let trace_state_array = StringArray::from(vec![record.trace_state.clone().unwrap_or_default()]);
        let parent_span_id_array = StringArray::from(vec![record.parent_span_id.clone().unwrap_or_default()]);
        let name_array = StringArray::from(vec![record.name.clone()]);
        let kind_array = StringArray::from(vec![record.kind.clone().unwrap_or_default()]);
        let start_time_array = Arc::new(build_timestamp_array(vec![ts_nano], Some(StdArc::from("UTC"))));
        let end_time_array = Arc::new(build_timestamp_array(vec![end_ts_nano.unwrap_or(0)], Some(StdArc::from("UTC"))));

        let http_method_array = StringArray::from(vec![record.http_method.clone().unwrap_or_default()]);
        let http_url_array = StringArray::from(vec![record.http_url.clone().unwrap_or_default()]);
        let http_status_code_array = Int32Array::from(vec![record.http_status_code]);
        let http_request_content_length_array = Int64Array::from(vec![record.http_request_content_length]);
        let http_response_content_length_array = Int64Array::from(vec![record.http_response_content_length]);
        let http_route_array = StringArray::from(vec![record.http_route.clone().unwrap_or_default()]);
        let http_scheme_array = StringArray::from(vec![record.http_scheme.clone().unwrap_or_default()]);
        let http_client_ip_array = StringArray::from(vec![record.http_client_ip.clone().unwrap_or_default()]);
        let http_user_agent_array = StringArray::from(vec![record.http_user_agent.clone().unwrap_or_default()]);
        let http_flavor_array = StringArray::from(vec![record.http_flavor.clone().unwrap_or_default()]);
        let http_target_array = StringArray::from(vec![record.http_target.clone().unwrap_or_default()]);
        let http_host_array = StringArray::from(vec![record.http_host.clone().unwrap_or_default()]);
        let rpc_system_array = StringArray::from(vec![record.rpc_system.clone().unwrap_or_default()]);
        let rpc_service_array = StringArray::from(vec![record.rpc_service.clone().unwrap_or_default()]);
        let rpc_method_array = StringArray::from(vec![record.rpc_method.clone().unwrap_or_default()]);
        let rpc_grpc_status_code_array = Int32Array::from(vec![record.rpc_grpc_status_code]);
        let db_system_array = StringArray::from(vec![record.db_system.clone().unwrap_or_default()]);
        let db_connection_string_array = StringArray::from(vec![record.db_connection_string.clone().unwrap_or_default()]);
        let db_user_array = StringArray::from(vec![record.db_user.clone().unwrap_or_default()]);
        let db_name_array = StringArray::from(vec![record.db_name.clone().unwrap_or_default()]);
        let db_statement_array = StringArray::from(vec![record.db_statement.clone().unwrap_or_default()]);
        let db_operation_array = StringArray::from(vec![record.db_operation.clone().unwrap_or_default()]);
        let db_sql_table_array = StringArray::from(vec![record.db_sql_table.clone().unwrap_or_default()]);
        let messaging_system_array = StringArray::from(vec![record.messaging_system.clone().unwrap_or_default()]);
        let messaging_destination_array = StringArray::from(vec![record.messaging_destination.clone().unwrap_or_default()]);
        let messaging_destination_kind_array = StringArray::from(vec![record.messaging_destination_kind.clone().unwrap_or_default()]);
        let messaging_message_id_array = StringArray::from(vec![record.messaging_message_id.clone().unwrap_or_default()]);
        let messaging_operation_array = StringArray::from(vec![record.messaging_operation.clone().unwrap_or_default()]);
        let messaging_url_array = StringArray::from(vec![record.messaging_url.clone().unwrap_or_default()]);
        let messaging_client_id_array = StringArray::from(vec![record.messaging_client_id.clone().unwrap_or_default()]);
        let messaging_kafka_partition_array = Int32Array::from(vec![record.messaging_kafka_partition]);
        let messaging_kafka_offset_array = Int64Array::from(vec![record.messaging_kafka_offset]);
        let messaging_kafka_consumer_group_array = StringArray::from(vec![record.messaging_kafka_consumer_group.clone().unwrap_or_default()]);
        let messaging_message_payload_size_bytes_array = Int64Array::from(vec![record.messaging_message_payload_size_bytes]);
        let messaging_protocol_array = StringArray::from(vec![record.messaging_protocol.clone().unwrap_or_default()]);
        let messaging_protocol_version_array = StringArray::from(vec![record.messaging_protocol_version.clone().unwrap_or_default()]);
        let cache_system_array = StringArray::from(vec![record.cache_system.clone().unwrap_or_default()]);
        let cache_operation_array = StringArray::from(vec![record.cache_operation.clone().unwrap_or_default()]);
        let cache_key_array = StringArray::from(vec![record.cache_key.clone().unwrap_or_default()]);
        let cache_hit_array = BooleanArray::from(vec![record.cache_hit]);
        let net_peer_ip_array = StringArray::from(vec![record.net_peer_ip.clone().unwrap_or_default()]);
        let net_peer_port_array = Int32Array::from(vec![record.net_peer_port]);
        let net_host_ip_array = StringArray::from(vec![record.net_host_ip.clone().unwrap_or_default()]);
        let net_host_port_array = Int32Array::from(vec![record.net_host_port]);
        let net_transport_array = StringArray::from(vec![record.net_transport.clone().unwrap_or_default()]);
        let enduser_id_array = StringArray::from(vec![record.enduser_id.clone().unwrap_or_default()]);
        let enduser_role_array = StringArray::from(vec![record.enduser_role.clone().unwrap_or_default()]);
        let enduser_scope_array = StringArray::from(vec![record.enduser_scope.clone().unwrap_or_default()]);
        let exception_type_array = StringArray::from(vec![record.exception_type.clone().unwrap_or_default()]);
        let exception_message_array = StringArray::from(vec![record.exception_message.clone().unwrap_or_default()]);
        let exception_stacktrace_array = StringArray::from(vec![record.exception_stacktrace.clone().unwrap_or_default()]);
        let exception_escaped_array = BooleanArray::from(vec![record.exception_escaped]);
        let thread_id_array = Int64Array::from(vec![record.thread_id]);
        let thread_name_array = StringArray::from(vec![record.thread_name.clone().unwrap_or_default()]);
        let code_function_array = StringArray::from(vec![record.code_function.clone().unwrap_or_default()]);
        let code_filepath_array = StringArray::from(vec![record.code_filepath.clone().unwrap_or_default()]);
        let code_namespace_array = StringArray::from(vec![record.code_namespace.clone().unwrap_or_default()]);
        let code_lineno_array = Int32Array::from(vec![record.code_lineno]);
        let deployment_environment_array = StringArray::from(vec![record.deployment_environment.clone().unwrap_or_default()]);
        let deployment_version_array = StringArray::from(vec![record.deployment_version.clone().unwrap_or_default()]);
        let service_name_array = StringArray::from(vec![record.service_name.clone().unwrap_or_default()]);
        let service_version_array = StringArray::from(vec![record.service_version.clone().unwrap_or_default()]);
        let service_instance_id_array = StringArray::from(vec![record.service_instance_id.clone().unwrap_or_default()]);
        let otel_library_name_array = StringArray::from(vec![record.otel_library_name.clone().unwrap_or_default()]);
        let otel_library_version_array = StringArray::from(vec![record.otel_library_version.clone().unwrap_or_default()]);
        let k8s_pod_name_array = StringArray::from(vec![record.k8s_pod_name.clone().unwrap_or_default()]);
        let k8s_namespace_name_array = StringArray::from(vec![record.k8s_namespace_name.clone().unwrap_or_default()]);
        let k8s_deployment_name_array = StringArray::from(vec![record.k8s_deployment_name.clone().unwrap_or_default()]);
        let container_id_array = StringArray::from(vec![record.container_id.clone().unwrap_or_default()]);
        let host_name_array = StringArray::from(vec![record.host_name.clone().unwrap_or_default()]);
        let os_type_array = StringArray::from(vec![record.os_type.clone().unwrap_or_default()]);
        let os_version_array = StringArray::from(vec![record.os_version.clone().unwrap_or_default()]);
        let process_pid_array = Int64Array::from(vec![record.process_pid]);
        let process_command_line_array = StringArray::from(vec![record.process_command_line.clone().unwrap_or_default()]);
        let process_runtime_name_array = StringArray::from(vec![record.process_runtime_name.clone().unwrap_or_default()]);
        let process_runtime_version_array = StringArray::from(vec![record.process_runtime_version.clone().unwrap_or_default()]);
        let aws_region_array = StringArray::from(vec![record.aws_region.clone().unwrap_or_default()]);
        let aws_account_id_array = StringArray::from(vec![record.aws_account_id.clone().unwrap_or_default()]);
        let aws_dynamodb_table_name_array = StringArray::from(vec![record.aws_dynamodb_table_name.clone().unwrap_or_default()]);
        let aws_dynamodb_operation_array = StringArray::from(vec![record.aws_dynamodb_operation.clone().unwrap_or_default()]);
        let aws_dynamodb_consumed_capacity_total_array = Float64Array::from(vec![record.aws_dynamodb_consumed_capacity_total]);
        let aws_sqs_queue_url_array = StringArray::from(vec![record.aws_sqs_queue_url.clone().unwrap_or_default()]);
        let aws_sqs_message_id_array = StringArray::from(vec![record.aws_sqs_message_id.clone().unwrap_or_default()]);
        let azure_resource_id_array = StringArray::from(vec![record.azure_resource_id.clone().unwrap_or_default()]);
        let azure_storage_container_name_array = StringArray::from(vec![record.azure_storage_container_name.clone().unwrap_or_default()]);
        let azure_storage_blob_name_array = StringArray::from(vec![record.azure_storage_blob_name.clone().unwrap_or_default()]);
        let gcp_project_id_array = StringArray::from(vec![record.gcp_project_id.clone().unwrap_or_default()]);
        let gcp_cloudsql_instance_id_array = StringArray::from(vec![record.gcp_cloudsql_instance_id.clone().unwrap_or_default()]);
        let gcp_pubsub_message_id_array = StringArray::from(vec![record.gcp_pubsub_message_id.clone().unwrap_or_default()]);
        let http_request_method_array = StringArray::from(vec![record.http_request_method.clone().unwrap_or_default()]);
        let db_instance_identifier_array = StringArray::from(vec![record.db_instance_identifier.clone().unwrap_or_default()]);
        let db_rows_affected_array = Int64Array::from(vec![record.db_rows_affected]);
        let net_sock_peer_addr_array = StringArray::from(vec![record.net_sock_peer_addr.clone().unwrap_or_default()]);
        let net_sock_peer_port_array = Int32Array::from(vec![record.net_sock_peer_port]);
        let net_sock_host_addr_array = StringArray::from(vec![record.net_sock_host_addr.clone().unwrap_or_default()]);
        let net_sock_host_port_array = Int32Array::from(vec![record.net_sock_host_port]);
        let messaging_consumer_id_array = StringArray::from(vec![record.messaging_consumer_id.clone().unwrap_or_default()]);
        let messaging_message_payload_compressed_size_bytes_array = Int64Array::from(vec![record.messaging_message_payload_compressed_size_bytes]);
        let faas_invocation_id_array = StringArray::from(vec![record.faas_invocation_id.clone().unwrap_or_default()]);
        let faas_trigger_array = StringArray::from(vec![record.faas_trigger.clone().unwrap_or_default()]);
        let cloud_zone_array = StringArray::from(vec![record.cloud_zone.clone().unwrap_or_default()]);

        let resource_attributes_service_name_array = StringArray::from(vec![record.resource_attributes_service_name.clone().unwrap_or_default()]);
        let resource_attributes_service_version_array = StringArray::from(vec![record.resource_attributes_service_version.clone().unwrap_or_default()]);
        let resource_attributes_service_instance_id_array = StringArray::from(vec![record.resource_attributes_service_instance_id.clone().unwrap_or_default()]);
        let resource_attributes_service_namespace_array = StringArray::from(vec![record.resource_attributes_service_namespace.clone().unwrap_or_default()]);
        let resource_attributes_host_name_array = StringArray::from(vec![record.resource_attributes_host_name.clone().unwrap_or_default()]);
        let resource_attributes_host_id_array = StringArray::from(vec![record.resource_attributes_host_id.clone().unwrap_or_default()]);
        let resource_attributes_host_type_array = StringArray::from(vec![record.resource_attributes_host_type.clone().unwrap_or_default()]);
        let resource_attributes_host_arch_array = StringArray::from(vec![record.resource_attributes_host_arch.clone().unwrap_or_default()]);
        let resource_attributes_os_type_array = StringArray::from(vec![record.resource_attributes_os_type.clone().unwrap_or_default()]);
        let resource_attributes_os_version_array = StringArray::from(vec![record.resource_attributes_os_version.clone().unwrap_or_default()]);
        let resource_attributes_process_pid_array = Int64Array::from(vec![record.resource_attributes_process_pid]);
        let resource_attributes_process_executable_name_array = StringArray::from(vec![record.resource_attributes_process_executable_name.clone().unwrap_or_default()]);
        let resource_attributes_process_command_line_array = StringArray::from(vec![record.resource_attributes_process_command_line.clone().unwrap_or_default()]);
        let resource_attributes_process_runtime_name_array = StringArray::from(vec![record.resource_attributes_process_runtime_name.clone().unwrap_or_default()]);
        let resource_attributes_process_runtime_version_array = StringArray::from(vec![record.resource_attributes_process_runtime_version.clone().unwrap_or_default()]);
        let resource_attributes_process_runtime_description_array = StringArray::from(vec![record.resource_attributes_process_runtime_description.clone().unwrap_or_default()]);
        let resource_attributes_process_executable_path_array = StringArray::from(vec![record.resource_attributes_process_executable_path.clone().unwrap_or_default()]);
        let resource_attributes_k8s_cluster_name_array = StringArray::from(vec![record.resource_attributes_k8s_cluster_name.clone().unwrap_or_default()]);
        let resource_attributes_k8s_namespace_name_array = StringArray::from(vec![record.resource_attributes_k8s_namespace_name.clone().unwrap_or_default()]);
        let resource_attributes_k8s_deployment_name_array = StringArray::from(vec![record.resource_attributes_k8s_deployment_name.clone().unwrap_or_default()]);
        let resource_attributes_k8s_pod_name_array = StringArray::from(vec![record.resource_attributes_k8s_pod_name.clone().unwrap_or_default()]);
        let resource_attributes_k8s_pod_uid_array = StringArray::from(vec![record.resource_attributes_k8s_pod_uid.clone().unwrap_or_default()]);
        let resource_attributes_k8s_replicaset_name_array = StringArray::from(vec![record.resource_attributes_k8s_replicaset_name.clone().unwrap_or_default()]);
        let resource_attributes_k8s_deployment_strategy_array = StringArray::from(vec![record.resource_attributes_k8s_deployment_strategy.clone().unwrap_or_default()]);
        let resource_attributes_k8s_container_name_array = StringArray::from(vec![record.resource_attributes_k8s_container_name.clone().unwrap_or_default()]);
        let resource_attributes_k8s_node_name_array = StringArray::from(vec![record.resource_attributes_k8s_node_name.clone().unwrap_or_default()]);
        let resource_attributes_container_id_array = StringArray::from(vec![record.resource_attributes_container_id.clone().unwrap_or_default()]);
        let resource_attributes_container_image_name_array = StringArray::from(vec![record.resource_attributes_container_image_name.clone().unwrap_or_default()]);
        let resource_attributes_container_image_tag_array = StringArray::from(vec![record.resource_attributes_container_image_tag.clone().unwrap_or_default()]);
        let resource_attributes_deployment_environment_array = StringArray::from(vec![record.resource_attributes_deployment_environment.clone().unwrap_or_default()]);
        let resource_attributes_deployment_version_array = StringArray::from(vec![record.resource_attributes_deployment_version.clone().unwrap_or_default()]);
        let resource_attributes_cloud_provider_array = StringArray::from(vec![record.resource_attributes_cloud_provider.clone().unwrap_or_default()]);
        let resource_attributes_cloud_platform_array = StringArray::from(vec![record.resource_attributes_cloud_platform.clone().unwrap_or_default()]);
        let resource_attributes_cloud_region_array = StringArray::from(vec![record.resource_attributes_cloud_region.clone().unwrap_or_default()]);
        let resource_attributes_cloud_availability_zone_array = StringArray::from(vec![record.resource_attributes_cloud_availability_zone.clone().unwrap_or_default()]);
        let resource_attributes_cloud_account_id_array = StringArray::from(vec![record.resource_attributes_cloud_account_id.clone().unwrap_or_default()]);
        let resource_attributes_cloud_resource_id_array = StringArray::from(vec![record.resource_attributes_cloud_resource_id.clone().unwrap_or_default()]);
        let resource_attributes_cloud_instance_type_array = StringArray::from(vec![record.resource_attributes_cloud_instance_type.clone().unwrap_or_default()]);
        let resource_attributes_telemetry_sdk_name_array = StringArray::from(vec![record.resource_attributes_telemetry_sdk_name.clone().unwrap_or_default()]);
        let resource_attributes_telemetry_sdk_language_array = StringArray::from(vec![record.resource_attributes_telemetry_sdk_language.clone().unwrap_or_default()]);
        let resource_attributes_telemetry_sdk_version_array = StringArray::from(vec![record.resource_attributes_telemetry_sdk_version.clone().unwrap_or_default()]);
        let resource_attributes_application_name_array = StringArray::from(vec![record.resource_attributes_application_name.clone().unwrap_or_default()]);
        let resource_attributes_application_version_array = StringArray::from(vec![record.resource_attributes_application_version.clone().unwrap_or_default()]);
        let resource_attributes_application_tier_array = StringArray::from(vec![record.resource_attributes_application_tier.clone().unwrap_or_default()]);
        let resource_attributes_application_owner_array = StringArray::from(vec![record.resource_attributes_application_owner.clone().unwrap_or_default()]);
        let resource_attributes_customer_id_array = StringArray::from(vec![record.resource_attributes_customer_id.clone().unwrap_or_default()]);
        let resource_attributes_tenant_id_array = StringArray::from(vec![record.resource_attributes_tenant_id.clone().unwrap_or_default()]);
        let resource_attributes_feature_flag_enabled_array = BooleanArray::from(vec![record.resource_attributes_feature_flag_enabled]);
        let resource_attributes_payment_gateway_array = StringArray::from(vec![record.resource_attributes_payment_gateway.clone().unwrap_or_default()]);
        let resource_attributes_database_type_array = StringArray::from(vec![record.resource_attributes_database_type.clone().unwrap_or_default()]);
        let resource_attributes_database_instance_array = StringArray::from(vec![record.resource_attributes_database_instance.clone().unwrap_or_default()]);
        let resource_attributes_cache_provider_array = StringArray::from(vec![record.resource_attributes_cache_provider.clone().unwrap_or_default()]);
        let resource_attributes_message_queue_type_array = StringArray::from(vec![record.resource_attributes_message_queue_type.clone().unwrap_or_default()]);
        let resource_attributes_http_route_array = StringArray::from(vec![record.resource_attributes_http_route.clone().unwrap_or_default()]);
        let resource_attributes_aws_ecs_cluster_arn_array = StringArray::from(vec![record.resource_attributes_aws_ecs_cluster_arn.clone().unwrap_or_default()]);
        let resource_attributes_aws_ecs_container_arn_array = StringArray::from(vec![record.resource_attributes_aws_ecs_container_arn.clone().unwrap_or_default()]);
        let resource_attributes_aws_ecs_task_arn_array = StringArray::from(vec![record.resource_attributes_aws_ecs_task_arn.clone().unwrap_or_default()]);
        let resource_attributes_aws_ecs_task_family_array = StringArray::from(vec![record.resource_attributes_aws_ecs_task_family.clone().unwrap_or_default()]);
        let resource_attributes_aws_ec2_instance_id_array = StringArray::from(vec![record.resource_attributes_aws_ec2_instance_id.clone().unwrap_or_default()]);
        let resource_attributes_gcp_project_id_array = StringArray::from(vec![record.resource_attributes_gcp_project_id.clone().unwrap_or_default()]);
        let resource_attributes_gcp_zone_array = StringArray::from(vec![record.resource_attributes_gcp_zone.clone().unwrap_or_default()]);
        let resource_attributes_azure_resource_id_array = StringArray::from(vec![record.resource_attributes_azure_resource_id.clone().unwrap_or_default()]);
        let resource_attributes_dynatrace_entity_process_id_array = StringArray::from(vec![record.resource_attributes_dynatrace_entity_process_id.clone().unwrap_or_default()]);
        let resource_attributes_elastic_node_name_array = StringArray::from(vec![record.resource_attributes_elastic_node_name.clone().unwrap_or_default()]);
        let resource_attributes_istio_mesh_id_array = StringArray::from(vec![record.resource_attributes_istio_mesh_id.clone().unwrap_or_default()]);
        let resource_attributes_cloudfoundry_application_id_array = StringArray::from(vec![record.resource_attributes_cloudfoundry_application_id.clone().unwrap_or_default()]);
        let resource_attributes_cloudfoundry_space_id_array = StringArray::from(vec![record.resource_attributes_cloudfoundry_space_id.clone().unwrap_or_default()]);
        let resource_attributes_opentelemetry_collector_name_array = StringArray::from(vec![record.resource_attributes_opentelemetry_collector_name.clone().unwrap_or_default()]);
        let resource_attributes_instrumentation_name_array = StringArray::from(vec![record.resource_attributes_instrumentation_name.clone().unwrap_or_default()]);
        let resource_attributes_instrumentation_version_array = StringArray::from(vec![record.resource_attributes_instrumentation_version.clone().unwrap_or_default()]);
        let resource_attributes_log_source_array = StringArray::from(vec![record.resource_attributes_log_source.clone().unwrap_or_default()]);

        let events_array = StringArray::from(vec![record.events.clone().unwrap_or_default()]);
        let links_array = StringArray::from(vec![record.links.clone().unwrap_or_default()]);
        let status_code_array = StringArray::from(vec![record.status_code.clone().unwrap_or_default()]);
        let status_message_array = StringArray::from(vec![record.status_message.clone().unwrap_or_default()]);
        let instrumentation_library_name_array = StringArray::from(vec![record.instrumentation_library_name.clone().unwrap_or_default()]);
        let instrumentation_library_version_array = StringArray::from(vec![record.instrumentation_library_version.clone().unwrap_or_default()]);

        let schema = Self::event_schema();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(trace_id_array),
                Arc::new(span_id_array),
                Arc::new(trace_state_array),
                Arc::new(parent_span_id_array),
                Arc::new(name_array),
                Arc::new(kind_array),
                start_time_array,
                end_time_array,
                Arc::new(http_method_array),
                Arc::new(http_url_array),
                Arc::new(http_status_code_array),
                Arc::new(http_request_content_length_array),
                Arc::new(http_response_content_length_array),
                Arc::new(http_route_array),
                Arc::new(http_scheme_array),
                Arc::new(http_client_ip_array),
                Arc::new(http_user_agent_array),
                Arc::new(http_flavor_array),
                Arc::new(http_target_array),
                Arc::new(http_host_array),
                Arc::new(rpc_system_array),
                Arc::new(rpc_service_array),
                Arc::new(rpc_method_array),
                Arc::new(rpc_grpc_status_code_array),
                Arc::new(db_system_array),
                Arc::new(db_connection_string_array),
                Arc::new(db_user_array),
                Arc::new(db_name_array),
                Arc::new(db_statement_array),
                Arc::new(db_operation_array),
                Arc::new(db_sql_table_array),
                Arc::new(messaging_system_array),
                Arc::new(messaging_destination_array),
                Arc::new(messaging_destination_kind_array),
                Arc::new(messaging_message_id_array),
                Arc::new(messaging_operation_array),
                Arc::new(messaging_url_array),
                Arc::new(messaging_client_id_array),
                Arc::new(messaging_kafka_partition_array),
                Arc::new(messaging_kafka_offset_array),
                Arc::new(messaging_kafka_consumer_group_array),
                Arc::new(messaging_message_payload_size_bytes_array),
                Arc::new(messaging_protocol_array),
                Arc::new(messaging_protocol_version_array),
                Arc::new(cache_system_array),
                Arc::new(cache_operation_array),
                Arc::new(cache_key_array),
                Arc::new(cache_hit_array),
                Arc::new(net_peer_ip_array),
                Arc::new(net_peer_port_array),
                Arc::new(net_host_ip_array),
                Arc::new(net_host_port_array),
                Arc::new(net_transport_array),
                Arc::new(enduser_id_array),
                Arc::new(enduser_role_array),
                Arc::new(enduser_scope_array),
                Arc::new(exception_type_array),
                Arc::new(exception_message_array),
                Arc::new(exception_stacktrace_array),
                Arc::new(exception_escaped_array),
                Arc::new(thread_id_array),
                Arc::new(thread_name_array),
                Arc::new(code_function_array),
                Arc::new(code_filepath_array),
                Arc::new(code_namespace_array),
                Arc::new(code_lineno_array),
                Arc::new(deployment_environment_array),
                Arc::new(deployment_version_array),
                Arc::new(service_name_array),
                Arc::new(service_version_array),
                Arc::new(service_instance_id_array),
                Arc::new(otel_library_name_array),
                Arc::new(otel_library_version_array),
                Arc::new(k8s_pod_name_array),
                Arc::new(k8s_namespace_name_array),
                Arc::new(k8s_deployment_name_array),
                Arc::new(container_id_array),
                Arc::new(host_name_array),
                Arc::new(os_type_array),
                Arc::new(os_version_array),
                Arc::new(process_pid_array),
                Arc::new(process_command_line_array),
                Arc::new(process_runtime_name_array),
                Arc::new(process_runtime_version_array),
                Arc::new(aws_region_array),
                Arc::new(aws_account_id_array),
                Arc::new(aws_dynamodb_table_name_array),
                Arc::new(aws_dynamodb_operation_array),
                Arc::new(aws_dynamodb_consumed_capacity_total_array),
                Arc::new(aws_sqs_queue_url_array),
                Arc::new(aws_sqs_message_id_array),
                Arc::new(azure_resource_id_array),
                Arc::new(azure_storage_container_name_array),
                Arc::new(azure_storage_blob_name_array),
                Arc::new(gcp_project_id_array),
                Arc::new(gcp_cloudsql_instance_id_array),
                Arc::new(gcp_pubsub_message_id_array),
                Arc::new(http_request_method_array),
                Arc::new(db_instance_identifier_array),
                Arc::new(db_rows_affected_array),
                Arc::new(net_sock_peer_addr_array),
                Arc::new(net_sock_peer_port_array),
                Arc::new(net_sock_host_addr_array),
                Arc::new(net_sock_host_port_array),
                Arc::new(messaging_consumer_id_array),
                Arc::new(messaging_message_payload_compressed_size_bytes_array),
                Arc::new(faas_invocation_id_array),
                Arc::new(faas_trigger_array),
                Arc::new(cloud_zone_array),
                Arc::new(resource_attributes_service_name_array),
                Arc::new(resource_attributes_service_version_array),
                Arc::new(resource_attributes_service_instance_id_array),
                Arc::new(resource_attributes_service_namespace_array),
                Arc::new(resource_attributes_host_name_array),
                Arc::new(resource_attributes_host_id_array),
                Arc::new(resource_attributes_host_type_array),
                Arc::new(resource_attributes_host_arch_array),
                Arc::new(resource_attributes_os_type_array),
                Arc::new(resource_attributes_os_version_array),
                Arc::new(resource_attributes_process_pid_array),
                Arc::new(resource_attributes_process_executable_name_array),
                Arc::new(resource_attributes_process_command_line_array),
                Arc::new(resource_attributes_process_runtime_name_array),
                Arc::new(resource_attributes_process_runtime_version_array),
                Arc::new(resource_attributes_process_runtime_description_array),
                Arc::new(resource_attributes_process_executable_path_array),
                Arc::new(resource_attributes_k8s_cluster_name_array),
                Arc::new(resource_attributes_k8s_namespace_name_array),
                Arc::new(resource_attributes_k8s_deployment_name_array),
                Arc::new(resource_attributes_k8s_pod_name_array),
                Arc::new(resource_attributes_k8s_pod_uid_array),
                Arc::new(resource_attributes_k8s_replicaset_name_array),
                Arc::new(resource_attributes_k8s_deployment_strategy_array),
                Arc::new(resource_attributes_k8s_container_name_array),
                Arc::new(resource_attributes_k8s_node_name_array),
                Arc::new(resource_attributes_container_id_array),
                Arc::new(resource_attributes_container_image_name_array),
                Arc::new(resource_attributes_container_image_tag_array),
                Arc::new(resource_attributes_deployment_environment_array),
                Arc::new(resource_attributes_deployment_version_array),
                Arc::new(resource_attributes_cloud_provider_array),
                Arc::new(resource_attributes_cloud_platform_array),
                Arc::new(resource_attributes_cloud_region_array),
                Arc::new(resource_attributes_cloud_availability_zone_array),
                Arc::new(resource_attributes_cloud_account_id_array),
                Arc::new(resource_attributes_cloud_resource_id_array),
                Arc::new(resource_attributes_cloud_instance_type_array),
                Arc::new(resource_attributes_telemetry_sdk_name_array),
                Arc::new(resource_attributes_telemetry_sdk_language_array),
                Arc::new(resource_attributes_telemetry_sdk_version_array),
                Arc::new(resource_attributes_application_name_array),
                Arc::new(resource_attributes_application_version_array),
                Arc::new(resource_attributes_application_tier_array),
                Arc::new(resource_attributes_application_owner_array),
                Arc::new(resource_attributes_customer_id_array),
                Arc::new(resource_attributes_tenant_id_array),
                Arc::new(resource_attributes_feature_flag_enabled_array),
                Arc::new(resource_attributes_payment_gateway_array),
                Arc::new(resource_attributes_database_type_array),
                Arc::new(resource_attributes_database_instance_array),
                Arc::new(resource_attributes_cache_provider_array),
                Arc::new(resource_attributes_message_queue_type_array),
                Arc::new(resource_attributes_http_route_array),
                Arc::new(resource_attributes_aws_ecs_cluster_arn_array),
                Arc::new(resource_attributes_aws_ecs_container_arn_array),
                Arc::new(resource_attributes_aws_ecs_task_arn_array),
                Arc::new(resource_attributes_aws_ecs_task_family_array),
                Arc::new(resource_attributes_aws_ec2_instance_id_array),
                Arc::new(resource_attributes_gcp_project_id_array),
                Arc::new(resource_attributes_gcp_zone_array),
                Arc::new(resource_attributes_azure_resource_id_array),
                Arc::new(resource_attributes_dynatrace_entity_process_id_array),
                Arc::new(resource_attributes_elastic_node_name_array),
                Arc::new(resource_attributes_istio_mesh_id_array),
                Arc::new(resource_attributes_cloudfoundry_application_id_array),
                Arc::new(resource_attributes_cloudfoundry_space_id_array),
                Arc::new(resource_attributes_opentelemetry_collector_name_array),
                Arc::new(resource_attributes_instrumentation_name_array),
                Arc::new(resource_attributes_instrumentation_version_array),
                Arc::new(resource_attributes_log_source_array),
                Arc::new(events_array),
                Arc::new(links_array),
                Arc::new(status_code_array),
                Arc::new(status_message_array),
                Arc::new(instrumentation_library_name_array),
                Arc::new(instrumentation_library_version_array),
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
                            Expr::Value(sqlparser::ast::ValueWithSpan { span: _, value: SqlValue::SingleQuotedString(s) }) => {
                                insert_values.insert(col.to_string(), s.clone());
                            }
                            Expr::Value(sqlparser::ast::ValueWithSpan { span: _, value: SqlValue::Number(n, _) }) => {
                                insert_values.insert(col.to_string(), n.clone());
                            }
                            _ => return Err(anyhow::anyhow!("Unsupported value type: {:?}", val)),
                        }
                    }
                    let trace_id = insert_values.get("trace_id").ok_or_else(|| anyhow::anyhow!("Missing trace_id"))?;
                    let span_id = insert_values.get("span_id").ok_or_else(|| anyhow::anyhow!("Missing span_id"))?;
                    let start_time_unix_nano = insert_values
                        .get("start_time_unix_nano")
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| anyhow::anyhow!("Missing or invalid start_time_unix_nano"))?;
                    let record = crate::persistent_queue::IngestRecord {
                        trace_id: trace_id.clone(),
                        span_id: span_id.clone(),
                        trace_state: insert_values.get("trace_state").cloned(),
                        parent_span_id: insert_values.get("parent_span_id").cloned(),
                        name: insert_values.get("name").cloned().unwrap_or_default(),
                        kind: insert_values.get("kind").cloned(),
                        start_time_unix_nano,
                        end_time_unix_nano: insert_values.get("end_time_unix_nano").and_then(|s| s.parse().ok()),
                        http_method: insert_values.get("http_method").cloned(),
                        http_url: insert_values.get("http_url").cloned(),
                        http_status_code: insert_values.get("http_status_code").and_then(|s| s.parse().ok()),
                        http_request_content_length: insert_values.get("http_request_content_length").and_then(|s| s.parse().ok()),
                        http_response_content_length: insert_values.get("http_response_content_length").and_then(|s| s.parse().ok()),
                        http_route: insert_values.get("http_route").cloned(),
                        http_scheme: insert_values.get("http_scheme").cloned(),
                        http_client_ip: insert_values.get("http_client_ip").cloned(),
                        http_user_agent: insert_values.get("http_user_agent").cloned(),
                        http_flavor: insert_values.get("http_flavor").cloned(),
                        http_target: insert_values.get("http_target").cloned(),
                        http_host: insert_values.get("http_host").cloned(),
                        rpc_system: insert_values.get("rpc_system").cloned(),
                        rpc_service: insert_values.get("rpc_service").cloned(),
                        rpc_method: insert_values.get("rpc_method").cloned(),
                        rpc_grpc_status_code: insert_values.get("rpc_grpc_status_code").and_then(|s| s.parse().ok()),
                        db_system: insert_values.get("db_system").cloned(),
                        db_connection_string: insert_values.get("db_connection_string").cloned(),
                        db_user: insert_values.get("db_user").cloned(),
                        db_name: insert_values.get("db_name").cloned(),
                        db_statement: insert_values.get("db_statement").cloned(),
                        db_operation: insert_values.get("db_operation").cloned(),
                        db_sql_table: insert_values.get("db_sql_table").cloned(),
                        messaging_system: insert_values.get("messaging_system").cloned(),
                        messaging_destination: insert_values.get("messaging_destination").cloned(),
                        messaging_destination_kind: insert_values.get("messaging_destination_kind").cloned(),
                        messaging_message_id: insert_values.get("messaging_message_id").cloned(),
                        messaging_operation: insert_values.get("messaging_operation").cloned(),
                        messaging_url: insert_values.get("messaging_url").cloned(),
                        messaging_client_id: insert_values.get("messaging_client_id").cloned(),
                        messaging_kafka_partition: insert_values.get("messaging_kafka_partition").and_then(|s| s.parse().ok()),
                        messaging_kafka_offset: insert_values.get("messaging_kafka_offset").and_then(|s| s.parse().ok()),
                        messaging_kafka_consumer_group: insert_values.get("messaging_kafka_consumer_group").cloned(),
                        messaging_message_payload_size_bytes: insert_values.get("messaging_message_payload_size_bytes").and_then(|s| s.parse().ok()),
                        messaging_protocol: insert_values.get("messaging_protocol").cloned(),
                        messaging_protocol_version: insert_values.get("messaging_protocol_version").cloned(),
                        cache_system: insert_values.get("cache_system").cloned(),
                        cache_operation: insert_values.get("cache_operation").cloned(),
                        cache_key: insert_values.get("cache_key").cloned(),
                        cache_hit: insert_values.get("cache_hit").and_then(|s| s.parse::<i32>().ok().map(|v| v != 0)),
                        net_peer_ip: insert_values.get("net_peer_ip").cloned(),
                        net_peer_port: insert_values.get("net_peer_port").and_then(|s| s.parse().ok()),
                        net_host_ip: insert_values.get("net_host_ip").cloned(),
                        net_host_port: insert_values.get("net_host_port").and_then(|s| s.parse().ok()),
                        net_transport: insert_values.get("net_transport").cloned(),
                        enduser_id: insert_values.get("enduser_id").cloned(),
                        enduser_role: insert_values.get("enduser_role").cloned(),
                        enduser_scope: insert_values.get("enduser_scope").cloned(),
                        exception_type: insert_values.get("exception_type").cloned(),
                        exception_message: insert_values.get("exception_message").cloned(),
                        exception_stacktrace: insert_values.get("exception_stacktrace").cloned(),
                        exception_escaped: insert_values.get("exception_escaped").and_then(|s| s.parse::<i32>().ok().map(|v| v != 0)),
                        thread_id: insert_values.get("thread_id").and_then(|s| s.parse().ok()),
                        thread_name: insert_values.get("thread_name").cloned(),
                        code_function: insert_values.get("code_function").cloned(),
                        code_filepath: insert_values.get("code_filepath").cloned(),
                        code_namespace: insert_values.get("code_namespace").cloned(),
                        code_lineno: insert_values.get("code_lineno").and_then(|s| s.parse().ok()),
                        deployment_environment: insert_values.get("deployment_environment").cloned(),
                        deployment_version: insert_values.get("deployment_version").cloned(),
                        service_name: insert_values.get("service_name").cloned(),
                        service_version: insert_values.get("service_version").cloned(),
                        service_instance_id: insert_values.get("service_instance_id").cloned(),
                        otel_library_name: insert_values.get("otel_library_name").cloned(),
                        otel_library_version: insert_values.get("otel_library_version").cloned(),
                        k8s_pod_name: insert_values.get("k8s_pod_name").cloned(),
                        k8s_namespace_name: insert_values.get("k8s_namespace_name").cloned(),
                        k8s_deployment_name: insert_values.get("k8s_deployment_name").cloned(),
                        container_id: insert_values.get("container_id").cloned(),
                        host_name: insert_values.get("host_name").cloned(),
                        os_type: insert_values.get("os_type").cloned(),
                        os_version: insert_values.get("os_version").cloned(),
                        process_pid: insert_values.get("process_pid").and_then(|s| s.parse().ok()),
                        process_command_line: insert_values.get("process_command_line").cloned(),
                        process_runtime_name: insert_values.get("process_runtime_name").cloned(),
                        process_runtime_version: insert_values.get("process_runtime_version").cloned(),
                        aws_region: insert_values.get("aws_region").cloned(),
                        aws_account_id: insert_values.get("aws_account_id").cloned(),
                        aws_dynamodb_table_name: insert_values.get("aws_dynamodb_table_name").cloned(),
                        aws_dynamodb_operation: insert_values.get("aws_dynamodb_operation").cloned(),
                        aws_dynamodb_consumed_capacity_total: insert_values.get("aws_dynamodb_consumed_capacity_total").and_then(|s| s.parse().ok()),
                        aws_sqs_queue_url: insert_values.get("aws_sqs_queue_url").cloned(),
                        aws_sqs_message_id: insert_values.get("aws_sqs_message_id").cloned(),
                        azure_resource_id: insert_values.get("azure_resource_id").cloned(),
                        azure_storage_container_name: insert_values.get("azure_storage_container_name").cloned(),
                        azure_storage_blob_name: insert_values.get("azure_storage_blob_name").cloned(),
                        gcp_project_id: insert_values.get("gcp_project_id").cloned(),
                        gcp_cloudsql_instance_id: insert_values.get("gcp_cloudsql_instance_id").cloned(),
                        gcp_pubsub_message_id: insert_values.get("gcp_pubsub_message_id").cloned(),
                        http_request_method: insert_values.get("http_request_method").cloned(),
                        db_instance_identifier: insert_values.get("db_instance_identifier").cloned(),
                        db_rows_affected: insert_values.get("db_rows_affected").and_then(|s| s.parse().ok()),
                        net_sock_peer_addr: insert_values.get("net_sock_peer_addr").cloned(),
                        net_sock_peer_port: insert_values.get("net_sock_peer_port").and_then(|s| s.parse().ok()),
                        net_sock_host_addr: insert_values.get("net_sock_host_addr").cloned(),
                        net_sock_host_port: insert_values.get("net_sock_host_port").and_then(|s| s.parse().ok()),
                        messaging_consumer_id: insert_values.get("messaging_consumer_id").cloned(),
                        messaging_message_payload_compressed_size_bytes: insert_values.get("messaging_message_payload_compressed_size_bytes").and_then(|s| s.parse().ok()),
                        faas_invocation_id: insert_values.get("faas_invocation_id").cloned(),
                        faas_trigger: insert_values.get("faas_trigger").cloned(),
                        cloud_zone: insert_values.get("cloud_zone").cloned(),
                        resource_attributes_service_name: insert_values.get("resource_attributes_service_name").cloned(),
                        resource_attributes_service_version: insert_values.get("resource_attributes_service_version").cloned(),
                        resource_attributes_service_instance_id: insert_values.get("resource_attributes_service_instance_id").cloned(),
                        resource_attributes_service_namespace: insert_values.get("resource_attributes_service_namespace").cloned(),
                        resource_attributes_host_name: insert_values.get("resource_attributes_host_name").cloned(),
                        resource_attributes_host_id: insert_values.get("resource_attributes_host_id").cloned(),
                        resource_attributes_host_type: insert_values.get("resource_attributes_host_type").cloned(),
                        resource_attributes_host_arch: insert_values.get("resource_attributes_host_arch").cloned(),
                        resource_attributes_os_type: insert_values.get("resource_attributes_os_type").cloned(),
                        resource_attributes_os_version: insert_values.get("resource_attributes_os_version").cloned(),
                        resource_attributes_process_pid: insert_values.get("resource_attributes_process_pid").and_then(|s| s.parse().ok()),
                        resource_attributes_process_executable_name: insert_values.get("resource_attributes_process_executable_name").cloned(),
                        resource_attributes_process_command_line: insert_values.get("resource_attributes_process_command_line").cloned(),
                        resource_attributes_process_runtime_name: insert_values.get("resource_attributes_process_runtime_name").cloned(),
                        resource_attributes_process_runtime_version: insert_values.get("resource_attributes_process_runtime_version").cloned(),
                        resource_attributes_process_runtime_description: insert_values.get("resource_attributes_process_runtime_description").cloned(),
                        resource_attributes_process_executable_path: insert_values.get("resource_attributes_process_executable_path").cloned(),
                        resource_attributes_k8s_cluster_name: insert_values.get("resource_attributes_k8s_cluster_name").cloned(),
                        resource_attributes_k8s_namespace_name: insert_values.get("resource_attributes_k8s_namespace_name").cloned(),
                        resource_attributes_k8s_deployment_name: insert_values.get("resource_attributes_k8s_deployment_name").cloned(),
                        resource_attributes_k8s_pod_name: insert_values.get("resource_attributes_k8s_pod_name").cloned(),
                        resource_attributes_k8s_pod_uid: insert_values.get("resource_attributes_k8s_pod_uid").cloned(),
                        resource_attributes_k8s_replicaset_name: insert_values.get("resource_attributes_k8s_replicaset_name").cloned(),
                        resource_attributes_k8s_deployment_strategy: insert_values.get("resource_attributes_k8s_deployment_strategy").cloned(),
                        resource_attributes_k8s_container_name: insert_values.get("resource_attributes_k8s_container_name").cloned(),
                        resource_attributes_k8s_node_name: insert_values.get("resource_attributes_k8s_node_name").cloned(),
                        resource_attributes_container_id: insert_values.get("resource_attributes_container_id").cloned(),
                        resource_attributes_container_image_name: insert_values.get("resource_attributes_container_image_name").cloned(),
                        resource_attributes_container_image_tag: insert_values.get("resource_attributes_container_image_tag").cloned(),
                        resource_attributes_deployment_environment: insert_values.get("resource_attributes_deployment_environment").cloned(),
                        resource_attributes_deployment_version: insert_values.get("resource_attributes_deployment_version").cloned(),
                        resource_attributes_cloud_provider: insert_values.get("resource_attributes_cloud_provider").cloned(),
                        resource_attributes_cloud_platform: insert_values.get("resource_attributes_cloud_platform").cloned(),
                        resource_attributes_cloud_region: insert_values.get("resource_attributes_cloud_region").cloned(),
                        resource_attributes_cloud_availability_zone: insert_values.get("resource_attributes_cloud_availability_zone").cloned(),
                        resource_attributes_cloud_account_id: insert_values.get("resource_attributes_cloud_account_id").cloned(),
                        resource_attributes_cloud_resource_id: insert_values.get("resource_attributes_cloud_resource_id").cloned(),
                        resource_attributes_cloud_instance_type: insert_values.get("resource_attributes_cloud_instance_type").cloned(),
                        resource_attributes_telemetry_sdk_name: insert_values.get("resource_attributes_telemetry_sdk_name").cloned(),
                        resource_attributes_telemetry_sdk_language: insert_values.get("resource_attributes_telemetry_sdk_language").cloned(),
                        resource_attributes_telemetry_sdk_version: insert_values.get("resource_attributes_telemetry_sdk_version").cloned(),
                        resource_attributes_application_name: insert_values.get("resource_attributes_application_name").cloned(),
                        resource_attributes_application_version: insert_values.get("resource_attributes_application_version").cloned(),
                        resource_attributes_application_tier: insert_values.get("resource_attributes_application_tier").cloned(),
                        resource_attributes_application_owner: insert_values.get("resource_attributes_application_owner").cloned(),
                        resource_attributes_customer_id: insert_values.get("resource_attributes_customer_id").cloned(),
                        resource_attributes_tenant_id: insert_values.get("resource_attributes_tenant_id").cloned(),
                        resource_attributes_feature_flag_enabled: insert_values.get("resource_attributes_feature_flag_enabled").and_then(|s| s.parse::<i32>().ok().map(|v| v != 0)),
                        resource_attributes_payment_gateway: insert_values.get("resource_attributes_payment_gateway").cloned(),
                        resource_attributes_database_type: insert_values.get("resource_attributes_database_type").cloned(),
                        resource_attributes_database_instance: insert_values.get("resource_attributes_database_instance").cloned(),
                        resource_attributes_cache_provider: insert_values.get("resource_attributes_cache_provider").cloned(),
                        resource_attributes_message_queue_type: insert_values.get("resource_attributes_message_queue_type").cloned(),
                        resource_attributes_http_route: insert_values.get("resource_attributes_http_route").cloned(),
                        resource_attributes_aws_ecs_cluster_arn: insert_values.get("resource_attributes_aws_ecs_cluster_arn").cloned(),
                        resource_attributes_aws_ecs_container_arn: insert_values.get("resource_attributes_aws_ecs_container_arn").cloned(),
                        resource_attributes_aws_ecs_task_arn: insert_values.get("resource_attributes_aws_ecs_task_arn").cloned(),
                        resource_attributes_aws_ecs_task_family: insert_values.get("resource_attributes_aws_ecs_task_family").cloned(),
                        resource_attributes_aws_ec2_instance_id: insert_values.get("resource_attributes_aws_ec2_instance_id").cloned(),
                        resource_attributes_gcp_project_id: insert_values.get("resource_attributes_gcp_project_id").cloned(),
                        resource_attributes_gcp_zone: insert_values.get("resource_attributes_gcp_zone").cloned(),
                        resource_attributes_azure_resource_id: insert_values.get("resource_attributes_azure_resource_id").cloned(),
                        resource_attributes_dynatrace_entity_process_id: insert_values.get("resource_attributes_dynatrace_entity_process_id").cloned(),
                        resource_attributes_elastic_node_name: insert_values.get("resource_attributes_elastic_node_name").cloned(),
                        resource_attributes_istio_mesh_id: insert_values.get("resource_attributes_istio_mesh_id").cloned(),
                        resource_attributes_cloudfoundry_application_id: insert_values.get("resource_attributes_cloudfoundry_application_id").cloned(),
                        resource_attributes_cloudfoundry_space_id: insert_values.get("resource_attributes_cloudfoundry_space_id").cloned(),
                        resource_attributes_opentelemetry_collector_name: insert_values.get("resource_attributes_opentelemetry_collector_name").cloned(),
                        resource_attributes_instrumentation_name: insert_values.get("resource_attributes_instrumentation_name").cloned(),
                        resource_attributes_instrumentation_version: insert_values.get("resource_attributes_instrumentation_version").cloned(),
                        resource_attributes_log_source: insert_values.get("resource_attributes_log_source").cloned(),
                        events: insert_values.get("events").cloned(),
                        links: insert_values.get("links").cloned(),
                        status_code: insert_values.get("status_code").cloned(),
                        status_message: insert_values.get("status_message").cloned(),
                        instrumentation_library_name: insert_values.get("instrumentation_library_name").cloned(),
                        instrumentation_library_version: insert_values.get("instrumentation_library_version").cloned(),
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
            deltalake::delta_datafusion::DeltaTableProvider::try_new(
                snapshot.clone(),
                log_store,
                deltalake::delta_datafusion::DeltaScanConfig::default(),
            )?
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

fn build_timestamp_array(values: Vec<i64>, tz: Option<StdArc<str>>) -> TimestampNanosecondArray {
    use datafusion::arrow::{array::ArrayData, buffer::Buffer};
    let data_type = DataType::Timestamp(TimeUnit::Nanosecond, tz);
    let buffer = Buffer::from_slice_ref(&values);
    let array_data = ArrayData::builder(data_type.clone()).len(values.len()).add_buffer(buffer).build().unwrap();
    TimestampNanosecondArray::from(array_data)
}