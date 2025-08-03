use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossbeam::queue::SegQueue;
use datafusion::arrow::array::{Array, AsArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{error, info};

// Helper to extract project_id from a batch
fn extract_project_id_from_batch(batch: &RecordBatch) -> Option<String> {
    batch.schema().fields().iter().position(|f| f.name() == "project_id")
        .and_then(|idx| {
            let column = batch.column(idx);
            let string_array = column.as_string::<i32>();
            if string_array.len() > 0 && !string_array.is_null(0) {
                Some(string_array.value(0).to_string())
            } else {
                None
            }
        })
}

/// BatchQueue collects RecordBatches and processes them at intervals
#[derive(Debug)]
pub struct BatchQueue {
    queue: Arc<SegQueue<RecordBatch>>,
    is_shutting_down: Arc<RwLock<bool>>,
}

impl BatchQueue {
    pub fn new(db: Arc<crate::database::Database>, interval_ms: u64, max_rows: usize) -> Self {
        let queue = Arc::new(SegQueue::new());
        let is_shutting_down = Arc::new(RwLock::new(false));

        let queue_clone = Arc::clone(&queue);
        let shutdown_flag = Arc::clone(&is_shutting_down);

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(interval_ms));

            loop {
                ticker.tick().await;

                if *shutdown_flag.read().await {
                    process_batches(&db, &queue_clone, max_rows).await;
                    break;
                }

                process_batches(&db, &queue_clone, max_rows).await;
            }
        });

        Self { queue, is_shutting_down }
    }

    /// Add a batch to the queue
    pub fn queue(&self, batch: RecordBatch) -> Result<()> {
        if let Ok(flag) = self.is_shutting_down.try_read() {
            if *flag {
                return Err(anyhow::anyhow!("BatchQueue is shutting down"));
            }
        }

        self.queue.push(batch);
        Ok(())
    }

    /// Signal shutdown and wait for queue to drain
    pub async fn shutdown(&self) {
        let mut guard = self.is_shutting_down.write().await;
        *guard = true;
    }
}

/// Process batches from the queue
async fn process_batches(db: &Arc<crate::database::Database>, queue: &Arc<SegQueue<RecordBatch>>, max_rows: usize) {
    if queue.is_empty() {
        return;
    }

    let mut project_batches: std::collections::HashMap<String, Vec<RecordBatch>> = std::collections::HashMap::new();
    let mut total_rows = 0;

    // Take batches up to max_rows and group by project_id
    while !queue.is_empty() && total_rows < max_rows {
        if let Some(batch) = queue.pop() {
            total_rows += batch.num_rows();
            // Extract project_id from batch, default to "default"
            let project_id = extract_project_id_from_batch(&batch).unwrap_or_else(|| "default".to_string());
            project_batches.entry(project_id).or_default().push(batch);
        } else {
            break;
        }
    }

    if project_batches.is_empty() {
        return;
    }

    let start = Instant::now();

    // Process batches for each project
    for (project_id, batches) in project_batches {
        let batch_count = batches.len();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        
        // For batch queue, default to otel_logs_and_spans table
        // TODO: Consider adding table_name extraction from batch metadata
        match db.insert_records_batch(&project_id, "otel_logs_and_spans", batches, true).await {
            Ok(_) => {
                let elapsed = start.elapsed();
                info!(
                    project_id = project_id,
                    batches_count = batch_count,
                    rows_count = row_count,
                    duration_ms = elapsed.as_millis(),
                    "Batch insert completed for project"
                );
            }
            Err(e) => {
                error!("Failed to insert batches for project {}: {}", project_id, e);
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::database::Database;
    use crate::schema_loader::get_default_schema;
    use chrono::Utc;
    use std::sync::Arc;
    use tokio::time::sleep;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use arrow_json::ReaderBuilder;
    use datafusion::arrow::record_batch::RecordBatch;
    use serial_test::serial;

    pub fn json_to_batch(records: Vec<Value>) -> anyhow::Result<RecordBatch> {
        if records.is_empty() {
            return Err(anyhow::anyhow!("Cannot create batch from empty records"));
        }
        
        let schema = get_default_schema().schema_ref();
        let json_data = records.into_iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        
        let mut reader = ReaderBuilder::new(schema.clone())
            .build(std::io::Cursor::new(json_data.as_bytes()))?;
        
        reader.next()
            .ok_or_else(|| anyhow::anyhow!("Failed to read batch"))?
            .map_err(Into::into)
    }

    pub fn create_default_record() -> HashMap<String, Value> {
        get_default_schema().fields
            .iter()
            .map(|field| {
                let value = if field.data_type == "List(Utf8)" {
                    json!([])
                } else {
                    Value::Null
                };
                (field.name.clone(), value)
            })
            .collect()
    }

    #[serial]
    #[tokio::test]
    async fn test_batch_queue() -> Result<()> {
        dotenv::dotenv().ok();
        let test_prefix = format!("test-batch-{}-{}", uuid::Uuid::new_v4(), chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
        unsafe {
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", &test_prefix);
        }

        // Initialize DB
        let db = Arc::new(Database::new().await?);

        // Create batch queue with short interval for testing
        let batch_queue = BatchQueue::new(Arc::clone(&db), 100, 10);

        // Create test records using JSON
        let now = Utc::now();
        let records: Vec<serde_json::Value> = (0..5)
            .map(|i| {
                // Start with a default record and set only needed fields
                let mut record = create_default_record();
                record.insert("timestamp".to_string(), json!(now.timestamp_micros()));
                record.insert("id".to_string(), json!(format!("test-{}", i)));
                record.insert("project_id".to_string(), json!("default"));
                record.insert("date".to_string(), json!(now.date_naive().to_string()));
                record.insert("hashes".to_string(), json!([]));
                serde_json::Value::Object(record.into_iter().collect())
            })
            .collect();

        let batch = json_to_batch(records)?;

        // Queue and process the batch
        batch_queue.queue(batch)?;
        sleep(Duration::from_millis(200)).await;

        // Shutdown queue
        batch_queue.shutdown().await;
        sleep(Duration::from_millis(200)).await;

        Ok(())
    }
}
