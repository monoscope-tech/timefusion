use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, SeekFrom};
use serde::{Deserialize, Serialize};
use anyhow::Result;

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
    pub span___http_method: Option<String>,
    pub span___http_url: Option<String>,
    pub span___http_status_code: Option<i32>,
    pub span___http_request_content_length: Option<i64>,
    pub span___http_response_content_length: Option<i64>,
    pub span___http_route: Option<String>,
    pub span___http_scheme: Option<String>,
    pub span___http_client_ip: Option<String>,
    pub span___http_user_agent: Option<String>,
    pub span___http_flavor: Option<String>,
    pub span___http_target: Option<String>,
    pub span___http_host: Option<String>,
    pub span___rpc_system: Option<String>,
    pub span___rpc_service: Option<String>,
    pub span___rpc_method: Option<String>,
    pub span___rpc_grpc_status_code: Option<i32>,
    pub span___db_system: Option<String>,
    pub span___db_connection_string: Option<String>,
    pub span___db_user: Option<String>,
    pub span___db_name: Option<String>,
    pub span___db_statement: Option<String>,
    pub span___db_operation: Option<String>,
    pub span___db_sql_table: Option<String>,
    pub span___messaging_system: Option<String>,
    pub span___messaging_destination: Option<String>,
    pub span___messaging_destination_kind: Option<String>,
    pub span___messaging_message_id: Option<String>,
    pub span___messaging_operation: Option<String>,
    pub span___messaging_url: Option<String>,
    pub span___messaging_client_id: Option<String>,
    pub span___messaging_kafka_partition: Option<i32>,
    pub span___messaging_kafka_offset: Option<i64>,
    pub span___messaging_kafka_consumer_group: Option<String>,
    pub span___messaging_message_payload_size_bytes: Option<i64>,
    pub span___messaging_protocol: Option<String>,
    pub span___messaging_protocol_version: Option<String>,
    pub span___cache_system: Option<String>,
    pub span___cache_operation: Option<String>,
    pub span___cache_key: Option<String>,
    pub span___cache_hit: Option<bool>,
    pub span___net_peer_ip: Option<String>,
    pub span___net_peer_port: Option<i32>,
    pub span___net_host_ip: Option<String>,
    pub span___net_host_port: Option<i32>,
    pub span___net_transport: Option<String>,
    pub span___enduser_id: Option<String>,
    pub span___enduser_role: Option<String>,
    pub span___enduser_scope: Option<String>,
    pub span___exception_type: Option<String>,
    pub span___exception_message: Option<String>,
    pub span___exception_stacktrace: Option<String>,
    pub span___exception_escaped: Option<bool>,
    pub span___thread_id: Option<i64>,
    pub span___thread_name: Option<String>,
    pub span___code_function: Option<String>,
    pub span___code_filepath: Option<String>,
    pub span___code_namespace: Option<String>,
    pub span___code_lineno: Option<i32>,
    pub span___deployment_environment: Option<String>,
    pub span___deployment_version: Option<String>,
    pub span___service_name: Option<String>,
    pub span___service_version: Option<String>,
    pub span___service_instance_id: Option<String>,
    pub span___otel_library_name: Option<String>,
    pub span___otel_library_version: Option<String>,
    pub span___k8s_pod_name: Option<String>,
    pub span___k8s_namespace_name: Option<String>,
    pub span___k8s_deployment_name: Option<String>,
    pub span___container_id: Option<String>,
    pub span___host_name: Option<String>,
    pub span___os_type: Option<String>,
    pub span___os_version: Option<String>,
    pub span___process_pid: Option<i64>,
    pub span___process_command_line: Option<String>,
    pub span___process_runtime_name: Option<String>,
    pub span___process_runtime_version: Option<String>,
    pub span___aws_region: Option<String>,
    pub span___aws_account_id: Option<String>,
    pub span___aws_dynamodb_table_name: Option<String>,
    pub span___aws_dynamodb_operation: Option<String>,
    pub span___aws_dynamodb_consumed_capacity_total: Option<f64>,
    pub span___aws_sqs_queue_url: Option<String>,
    pub span___aws_sqs_message_id: Option<String>,
    pub span___azure_resource_id: Option<String>,
    pub span___azure_storage_container_name: Option<String>,
    pub span___azure_storage_blob_name: Option<String>,
    pub span___gcp_project_id: Option<String>,
    pub span___gcp_cloudsql_instance_id: Option<String>,
    pub span___gcp_pubsub_message_id: Option<String>,
    pub span___http_request_method: Option<String>,
    pub span___db_instance_identifier: Option<String>,
    pub span___db_rows_affected: Option<i64>,
    pub span___net_sock_peer_addr: Option<String>,
    pub span___net_sock_peer_port: Option<i32>,
    pub span___net_sock_host_addr: Option<String>,
    pub span___net_sock_host_port: Option<i32>,
    pub span___messaging_consumer_id: Option<String>,
    pub span___messaging_message_payload_compressed_size_bytes: Option<i64>,
    pub span___faas_invocation_id: Option<String>,
    pub span___faas_trigger: Option<String>,
    pub span___cloud_zone: Option<String>,
    pub attributes____service_name: Option<String>,
    pub attributes____service_version: Option<String>,
    pub attributes____service_instance_id: Option<String>,
    pub attributes____service_namespace: Option<String>,
    pub attributes____host_name: Option<String>,
    pub attributes____host_id: Option<String>,
    pub attributes____host_type: Option<String>,
    pub attributes____host_arch: Option<String>,
    pub attributes____os_type: Option<String>,
    pub attributes____os_version: Option<String>,
    pub attributes____process_pid: Option<i64>,
    pub attributes____process_executable_name: Option<String>,
    pub attributes____process_command_line: Option<String>,
    pub attributes____process_runtime_name: Option<String>,
    pub attributes____process_runtime_version: Option<String>,
    pub attributes____process_runtime_description: Option<String>,
    pub attributes____process_executable_path: Option<String>,
    pub attributes____k8s_cluster_name: Option<String>,
    pub attributes____k8s_namespace_name: Option<String>,
    pub attributes____k8s_deployment_name: Option<String>,
    pub attributes____k8s_pod_name: Option<String>,
    pub attributes____k8s_pod_uid: Option<String>,
    pub attributes____k8s_replicaset_name: Option<String>,
    pub attributes____k8s_deployment_strategy: Option<String>,
    pub attributes____k8s_container_name: Option<String>,
    pub attributes____k8s_node_name: Option<String>,
    pub attributes____container_id: Option<String>,
    pub attributes____container_image_name: Option<String>,
    pub attributes____container_image_tag: Option<String>,
    pub attributes____deployment_environment: Option<String>,
    pub attributes____deployment_version: Option<String>,
    pub attributes____cloud_provider: Option<String>,
    pub attributes____cloud_platform: Option<String>,
    pub attributes____cloud_region: Option<String>,
    pub attributes____cloud_availability_zone: Option<String>,
    pub attributes____cloud_account_id: Option<String>,
    pub attributes____cloud_resource_id: Option<String>,
    pub attributes____cloud_instance_type: Option<String>,
    pub attributes____telemetry_sdk_name: Option<String>,
    pub attributes____telemetry_sdk_language: Option<String>,
    pub attributes____telemetry_sdk_version: Option<String>,
    pub attributes____application_name: Option<String>,
    pub attributes____application_version: Option<String>,
    pub attributes____application_tier: Option<String>,
    pub attributes____application_owner: Option<String>,
    pub attributes____customer_id: Option<String>,
    pub attributes____tenant_id: Option<String>,
    pub attributes____feature_flag_enabled: Option<bool>,
    pub attributes____payment_gateway: Option<String>,
    pub attributes____database_type: Option<String>,
    pub attributes____database_instance: Option<String>,
    pub attributes____cache_provider: Option<String>,
    pub attributes____message_queue_type: Option<String>,
    pub attributes____http_route: Option<String>,
    pub attributes____aws_ecs_cluster_arn: Option<String>,
    pub attributes____aws_ecs_container_arn: Option<String>,
    pub attributes____aws_ecs_task_arn: Option<String>,
    pub attributes____aws_ecs_task_family: Option<String>,
    pub attributes____aws_ec2_instance_id: Option<String>,
    pub attributes____gcp_project_id: Option<String>,
    pub attributes____gcp_zone: Option<String>,
    pub attributes____azure_resource_id: Option<String>,
    pub attributes____dynatrace_entity_process_id: Option<String>,
    pub attributes____elastic_node_name: Option<String>,
    pub attributes____istio_mesh_id: Option<String>,
    pub attributes____cloudfoundry_application_id: Option<String>,
    pub attributes____cloudfoundry_space_id: Option<String>,
    pub attributes____opentelemetry_collector_name: Option<String>,
    pub attributes____instrumentation_name: Option<String>,
    pub attributes____instrumentation_version: Option<String>,
    pub attributes____log_source: Option<String>,
    pub events: Option<String>,
    pub links: Option<String>,
    pub status_code: Option<String>,
    pub status_message: Option<String>,
    pub instrumentation_library_name: Option<String>,
    pub instrumentation_library_version: Option<String>,
}

#[derive(Clone)]
pub struct PersistentQueue {
    path: PathBuf,
    file: Arc<Mutex<File>>,
    position: Arc<Mutex<u64>>,
}

impl PersistentQueue {
    pub async fn new(path: &str) -> Result<Self> {
        let path = PathBuf::from(path);
        if !path.parent().unwrap().exists() {
            tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await?;
        Ok(Self {
            path,
            file: Arc::new(Mutex::new(file)),
            position: Arc::new(Mutex::new(0)),
        })
    }

    pub async fn enqueue(&self, record: IngestRecord) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        let serialized = serde_json::to_string(&record)?;
        let len = serialized.len() as u64;
        file.write_all(serialized.as_bytes()).await?;
        file.write_all(b"\n").await?;
        file.flush().await?;

        let mut pos = self.position.lock().unwrap();
        *pos += len + 1; // +1 for newline
        Ok(())
    }

    pub async fn dequeue(&self) -> Result<Option<IngestRecord>> {
        let mut file = self.file.lock().unwrap();
        let mut pos = self.position.lock().unwrap();

        if *pos == 0 {
            return Ok(None); // Queue is empty
        }

        // Read the first line
        file.seek(SeekFrom::Start(0)).await?;
        let mut reader = BufReader::new(&mut *file);
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            return Ok(None); // No data to read
        }

        let record: IngestRecord = serde_json::from_str(&line.trim_end())?;
        let consumed = bytes_read as u64;

        // Drop the reader to release the mutable borrow
        drop(reader);

        // Now we can mutate the file again
        *pos -= consumed;
        file.set_len(*pos).await?;
        file.seek(SeekFrom::Start(0)).await?;

        if *pos > 0 {
            // Read remaining content with a new reader
            let mut remaining = Vec::new();
            let mut new_reader = BufReader::new(&mut *file);
            new_reader.seek(SeekFrom::Start(consumed)).await?;
            new_reader.read_to_end(&mut remaining).await?;
            drop(new_reader);

            // Write remaining content back
            file.seek(SeekFrom::Start(0)).await?;
            file.write_all(&remaining).await?;
            file.flush().await?;
        }

        Ok(Some(record))
    }
}