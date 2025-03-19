use crate::database::Database;
use crate::persistent_queue::IngestRecord;
use anyhow::Result;
use deltalake::datafusion::execution::context::SessionContext;
use deltalake::datafusion::prelude::DataFrame;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::arrow::array::Array; // Use deltalake's Array trait
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, error};

pub struct Ingestor {
    db: Arc<Database>,
}

impl Ingestor {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub async fn ingest_record(&self, record: IngestRecord) -> Result<()> {
        self.db.write(&record).await?;
        info!("Ingested record with ID: {}", record.id);
        Ok(())
    }

    pub async fn batch_ingest(&self, records: Vec<IngestRecord>) -> Result<Vec<serde_json::Value>> {
        if records.is_empty() {
            info!("No records to ingest");
            return Ok(vec![]);
        }

        let ctx = self.db.get_session_context();
        let schema = Arc::new(Database::event_schema());

        let mut project_ids = Vec::new();
        let mut ids = Vec::new();
        let mut timestamps = Vec::new();

        for record in &records {
            project_ids.push(record.project_id.clone());
            ids.push(record.id.clone());
            let ts = chrono::DateTime::parse_from_rfc3339(&record.timestamp)?.timestamp_micros();
            timestamps.push(ts);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(deltalake::arrow::array::StringArray::from(project_ids)),
                Arc::new(deltalake::arrow::array::StringArray::from(ids)),
                Arc::new(deltalake::arrow::array::TimestampMicrosecondArray::new(
                    deltalake::arrow::buffer::ScalarBuffer::from(timestamps),
                    None,
                )),
            ],
        )?;

        ctx.register_batch("temp_batch", batch)?;
        let df = ctx.sql("INSERT INTO telemetry_events SELECT * FROM temp_batch").await?;

        let batches = df.collect().await?;
        let json_rows = record_batches_to_json_rows(&batches)?;

        self.db.refresh_table("telemetry_events").await?;

        let df_compact = ctx.sql("OPTIMIZE TABLE telemetry_events").await?;
        df_compact.collect().await?;

        info!("Batch ingested {} records", records.len());
        Ok(json_rows)
    }

    pub async fn process_queue(&self, queue: Arc<RwLock<Vec<IngestRecord>>>) -> Result<()> {
        let records: Vec<IngestRecord> = {
            let mut queue_lock = queue.write().await;
            std::mem::take(&mut *queue_lock)
        };
        self.batch_ingest(records).await?;
        Ok(())
    }
}

pub async fn start_ingestion_service(db: Arc<Database>, queue: Arc<RwLock<Vec<IngestRecord>>>) -> Result<()> {
    let ingestor = Ingestor::new(db);
    tokio::spawn(async move {
        loop {
            if let Err(e) = ingestor.process_queue(queue.clone()).await {
                error!("Ingestion loop failed: {:?}", e);
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    });
    info!("Ingestion service started");
    Ok(())
}

pub fn record_batches_to_json_rows(batches: &Vec<RecordBatch>) -> Result<Vec<serde_json::Value>, anyhow::Error> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = serde_json::Map::new();
            for (col_idx, column) in batch.columns().iter().enumerate() {
                let field_name = batch.schema().field(col_idx).name().to_string(); // Owned String
                let value = match column.data_type() {
                    deltalake::arrow::datatypes::DataType::Utf8 => {
                        let array = column.as_any().downcast_ref::<deltalake::arrow::array::StringArray>().unwrap();
                        if array.is_null(row_idx) { serde_json::Value::Null } else { serde_json::Value::String(array.value(row_idx).to_string()) }
                    }
                    deltalake::arrow::datatypes::DataType::Int64 => {
                        let array = column.as_any().downcast_ref::<deltalake::arrow::array::Int64Array>().unwrap();
                        if array.is_null(row_idx) { serde_json::Value::Null } else { serde_json::Value::Number(array.value(row_idx).into()) }
                    }
                    deltalake::arrow::datatypes::DataType::Float64 => {
                        let array = column.as_any().downcast_ref::<deltalake::arrow::array::Float64Array>().unwrap();
                        if array.is_null(row_idx) { serde_json::Value::Null } else { serde_json::Value::Number(serde_json::Number::from_f64(array.value(row_idx)).unwrap()) }
                    }
                    _ => serde_json::Value::Null,
                };
                row.insert(field_name, value);
            }
            rows.push(serde_json::Value::Object(row));
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;

    #[tokio::test]
    async fn test_ingest_record() -> Result<()> {
        let db = Arc::new(Database::new().await?);
        db.create_events_table("telemetry_events", "file:///tmp/telemetry_events").await?;
        let ingestor = Ingestor::new(db.clone());

        let record = IngestRecord {
            project_id: "test_proj".to_string(),
            id: "test_id".to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            trace_id: "trace_1".to_string(),
            span_id: "span_1".to_string(),
            event_type: "span".to_string(),
            status: Some("ok".to_string()),
            end_time: None,
            duration_ns: 1000,
            span_name: "test_span".to_string(),
            span_kind: None,
            parent_span_id: None,
            trace_state: None,
            has_error: false,
            severity_text: None,
            severity_number: 0,
            body: None,
            http_method: None,
            http_url: None,
            http_route: None,
            http_host: None,
            http_status_code: None,
            path_params: None,
            query_params: None,
            request_body: None,
            response_body: None,
            sdk_type: "rust".to_string(),
            service_version: None,
            errors: None,
            tags: vec![],
            parent_id: None,
            db_system: None,
            db_name: None,
            db_statement: None,
            db_operation: None,
            rpc_system: None,
            rpc_service: None,
            rpc_method: None,
            endpoint_hash: "hash1".to_string(),
            shape_hash: "hash2".to_string(),
            format_hashes: vec![],
            field_hashes: vec![],
            attributes: None,
            events: None,
            links: None,
            resource: None,
            instrumentation_scope: None,
        };

        ingestor.ingest_record(record).await?;
        Ok(())
    }
}