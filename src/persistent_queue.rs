use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use sled::Db;
use tokio::sync::mpsc::{self, Sender};
use tracing::error;
use uuid::Uuid;

use crate::database::Database;

#[derive(Serialize, Deserialize, Clone)]
pub struct IngestRecord {
    pub trace_id: String,
    pub span_id: String,
    pub trace_state: Option<String>,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub kind: Option<String>,
    pub start_time_unix_nano: i64,
    pub end_time_unix_nano: Option<i64>,

    // Span attributes
    pub http_method: Option<String>,
    pub http_url: Option<String>,
    pub http_status_code: Option<i32>,
    pub http_request_content_length: Option<i64>,
    pub http_response_content_length: Option<i64>,
    pub http_route: Option<String>,
    pub http_scheme: Option<String>,
    pub http_client_ip: Option<String>,
    pub http_user_agent: Option<String>,
    pub http_flavor: Option<String>,
    pub http_target: Option<String>,
    pub http_host: Option<String>,
    pub rpc_system: Option<String>,
    pub rpc_service: Option<String>,
    pub rpc_method: Option<String>,
    pub rpc_grpc_status_code: Option<i32>,
    pub db_system: Option<String>,
    pub db_connection_string: Option<String>,
    pub db_user: Option<String>,
    pub db_name: Option<String>,
    pub db_statement: Option<String>,
    pub db_operation: Option<String>,
    pub db_sql_table: Option<String>,
    pub messaging_system: Option<String>,
    pub messaging_destination: Option<String>,
    pub messaging_destination_kind: Option<String>,
    pub messaging_message_id: Option<String>,
    pub messaging_operation: Option<String>,
    pub messaging_url: Option<String>,
    pub messaging_client_id: Option<String>,
    pub messaging_kafka_partition: Option<i32>,
    pub messaging_kafka_offset: Option<i64>,
    pub messaging_kafka_consumer_group: Option<String>,
    pub messaging_message_payload_size_bytes: Option<i64>,
    pub messaging_protocol: Option<String>,
    pub messaging_protocol_version: Option<String>,
    pub cache_system: Option<String>,
    pub cache_operation: Option<String>,
    pub cache_key: Option<String>,
    pub cache_hit: Option<bool>,
    pub net_peer_ip: Option<String>,
    pub net_peer_port: Option<i32>,
    pub net_host_ip: Option<String>,
    pub net_host_port: Option<i32>,
    pub net_transport: Option<String>,
    pub enduser_id: Option<String>,
    pub enduser_role: Option<String>,
    pub enduser_scope: Option<String>,
    pub exception_type: Option<String>,
    pub exception_message: Option<String>,
    pub exception_stacktrace: Option<String>,
    pub exception_escaped: Option<bool>,
    pub thread_id: Option<i64>,
    pub thread_name: Option<String>,
    pub code_function: Option<String>,
    pub code_filepath: Option<String>,
    pub code_namespace: Option<String>,
    pub code_lineno: Option<i32>,
    pub deployment_environment: Option<String>,
    pub deployment_version: Option<String>,
    pub service_name: Option<String>,
    pub service_version: Option<String>,
    pub service_instance_id: Option<String>,
    pub otel_library_name: Option<String>,
    pub otel_library_version: Option<String>,
    pub k8s_pod_name: Option<String>,
    pub k8s_namespace_name: Option<String>,
    pub k8s_deployment_name: Option<String>,
    pub container_id: Option<String>,
    pub host_name: Option<String>,
    pub os_type: Option<String>,
    pub os_version: Option<String>,
    pub process_pid: Option<i64>,
    pub process_command_line: Option<String>,
    pub process_runtime_name: Option<String>,
    pub process_runtime_version: Option<String>,
    pub aws_region: Option<String>,
    pub aws_account_id: Option<String>,
    pub aws_dynamodb_table_name: Option<String>,
    pub aws_dynamodb_operation: Option<String>,
    pub aws_dynamodb_consumed_capacity_total: Option<f64>,
    pub aws_sqs_queue_url: Option<String>,
    pub aws_sqs_message_id: Option<String>,
    pub azure_resource_id: Option<String>,
    pub azure_storage_container_name: Option<String>,
    pub azure_storage_blob_name: Option<String>,
    pub gcp_project_id: Option<String>,
    pub gcp_cloudsql_instance_id: Option<String>,
    pub gcp_pubsub_message_id: Option<String>,
    pub http_request_method: Option<String>,
    pub db_instance_identifier: Option<String>,
    pub db_rows_affected: Option<i64>,
    pub net_sock_peer_addr: Option<String>,
    pub net_sock_peer_port: Option<i32>,
    pub net_sock_host_addr: Option<String>,
    pub net_sock_host_port: Option<i32>,
    pub messaging_consumer_id: Option<String>,
    pub messaging_message_payload_compressed_size_bytes: Option<i64>,
    pub faas_invocation_id: Option<String>,
    pub faas_trigger: Option<String>,
    pub cloud_zone: Option<String>,

    // Resource attributes
    pub resource_attributes_service_name: Option<String>,
    pub resource_attributes_service_version: Option<String>,
    pub resource_attributes_service_instance_id: Option<String>,
    pub resource_attributes_service_namespace: Option<String>,
    pub resource_attributes_host_name: Option<String>,
    pub resource_attributes_host_id: Option<String>,
    pub resource_attributes_host_type: Option<String>,
    pub resource_attributes_host_arch: Option<String>,
    pub resource_attributes_os_type: Option<String>,
    pub resource_attributes_os_version: Option<String>,
    pub resource_attributes_process_pid: Option<i64>,
    pub resource_attributes_process_executable_name: Option<String>,
    pub resource_attributes_process_command_line: Option<String>,
    pub resource_attributes_process_runtime_name: Option<String>,
    pub resource_attributes_process_runtime_version: Option<String>,
    pub resource_attributes_process_runtime_description: Option<String>,
    pub resource_attributes_process_executable_path: Option<String>,
    pub resource_attributes_k8s_cluster_name: Option<String>,
    pub resource_attributes_k8s_namespace_name: Option<String>,
    pub resource_attributes_k8s_deployment_name: Option<String>,
    pub resource_attributes_k8s_pod_name: Option<String>,
    pub resource_attributes_k8s_pod_uid: Option<String>,
    pub resource_attributes_k8s_replicaset_name: Option<String>,
    pub resource_attributes_k8s_deployment_strategy: Option<String>,
    pub resource_attributes_k8s_container_name: Option<String>,
    pub resource_attributes_k8s_node_name: Option<String>,
    pub resource_attributes_container_id: Option<String>,
    pub resource_attributes_container_image_name: Option<String>,
    pub resource_attributes_container_image_tag: Option<String>,
    pub resource_attributes_deployment_environment: Option<String>,
    pub resource_attributes_deployment_version: Option<String>,
    pub resource_attributes_cloud_provider: Option<String>,
    pub resource_attributes_cloud_platform: Option<String>,
    pub resource_attributes_cloud_region: Option<String>,
    pub resource_attributes_cloud_availability_zone: Option<String>,
    pub resource_attributes_cloud_account_id: Option<String>,
    pub resource_attributes_cloud_resource_id: Option<String>,
    pub resource_attributes_cloud_instance_type: Option<String>,
    pub resource_attributes_telemetry_sdk_name: Option<String>,
    pub resource_attributes_telemetry_sdk_language: Option<String>,
    pub resource_attributes_telemetry_sdk_version: Option<String>,
    pub resource_attributes_application_name: Option<String>,
    pub resource_attributes_application_version: Option<String>,
    pub resource_attributes_application_tier: Option<String>,
    pub resource_attributes_application_owner: Option<String>,
    pub resource_attributes_customer_id: Option<String>,
    pub resource_attributes_tenant_id: Option<String>,
    pub resource_attributes_feature_flag_enabled: Option<bool>,
    pub resource_attributes_payment_gateway: Option<String>,
    pub resource_attributes_database_type: Option<String>,
    pub resource_attributes_database_instance: Option<String>,
    pub resource_attributes_cache_provider: Option<String>,
    pub resource_attributes_message_queue_type: Option<String>,
    pub resource_attributes_http_route: Option<String>,
    pub resource_attributes_aws_ecs_cluster_arn: Option<String>,
    pub resource_attributes_aws_ecs_container_arn: Option<String>,
    pub resource_attributes_aws_ecs_task_arn: Option<String>,
    pub resource_attributes_aws_ecs_task_family: Option<String>,
    pub resource_attributes_aws_ec2_instance_id: Option<String>,
    pub resource_attributes_gcp_project_id: Option<String>,
    pub resource_attributes_gcp_zone: Option<String>,
    pub resource_attributes_azure_resource_id: Option<String>,
    pub resource_attributes_dynatrace_entity_process_id: Option<String>,
    pub resource_attributes_elastic_node_name: Option<String>,
    pub resource_attributes_istio_mesh_id: Option<String>,
    pub resource_attributes_cloudfoundry_application_id: Option<String>,
    pub resource_attributes_cloudfoundry_space_id: Option<String>,
    pub resource_attributes_opentelemetry_collector_name: Option<String>,
    pub resource_attributes_instrumentation_name: Option<String>,
    pub resource_attributes_instrumentation_version: Option<String>,
    pub resource_attributes_log_source: Option<String>,

    // Nested structures
    pub events: Option<String>,
    pub links: Option<String>,
    pub status_code: Option<String>,
    pub status_message: Option<String>,
    pub instrumentation_library_name: Option<String>,
    pub instrumentation_library_version: Option<String>,
}

pub struct PersistentQueue {
    pub db: Db,
    sender: Sender<(String, IngestRecord)>,
    database: Arc<Database>,
}

impl PersistentQueue {
    pub fn new<P: AsRef<Path>>(path: P, database: Arc<Database>) -> Result<Self> {
        let db = sled::open(path)?;
        let (sender, mut receiver) = mpsc::channel::<(String, IngestRecord)>(1000);

        let queue = Self {
            db,
            sender,
            database,
        };

        // Clone necessary components for the spawned task
        let db_clone = queue.db.clone();
        let database_clone = queue.database.clone();

        tokio::spawn(async move {
            while let Some((receipt, record)) = receiver.recv().await {
                if let Err(e) = Self::process_record_static(&db_clone, &database_clone, receipt.clone(), &record).await {
                    error!("Failed to process record with receipt {}: {:?}", receipt, e);
                }
            }
        });

        Ok(queue)
    }

    async fn process_record_static(db: &Db, database: &Arc<Database>, receipt: String, record: &IngestRecord) -> Result<()> {
        tracing::info!("Processing record with receipt: {}", receipt);
        database.write(record).await?;
        db.remove(receipt.as_bytes())?;
        tracing::info!("Record processed and removed from queue: {}", record.trace_id);
        Ok(())
    }

    pub async fn enqueue(&self, record: &IngestRecord) -> Result<String> {
        let receipt = Uuid::new_v4().to_string();
        let serialized = bincode::serialize(record)?;
        self.db.insert(receipt.as_bytes(), serialized)?;
        self.sender.send((receipt.clone(), record.clone())).await?;
        Ok(receipt)
    }

    pub async fn process_record(&self, receipt: String, record: &IngestRecord) -> Result<()> {
        Self::process_record_static(&self.db, &self.database, receipt, record).await
    }

    pub async fn dequeue(&self) -> Result<Option<(String, IngestRecord)>> {
        if let Some((key, value)) = self.db.pop_min()? {
            let receipt = String::from_utf8(key.to_vec())?;
            let record: IngestRecord = bincode::deserialize(&value)?;
            Ok(Some((receipt, record)))
        } else {
            Ok(None)
        }
    }

    pub async fn dequeue_all(&self) -> Result<Vec<(String, IngestRecord)>> {
        let mut records = Vec::new();
        while let Some((key, value)) = self.db.pop_min()? {
            let receipt = String::from_utf8(key.to_vec())?;
            let record: IngestRecord = bincode::deserialize(&value)?;
            records.push((receipt, record));
        }
        Ok(records)
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.db.len())
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.db.is_empty())
    }
}