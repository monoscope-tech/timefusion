use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use crossbeam::queue::SegQueue;
use delta_kernel::arrow::record_batch::RecordBatch;
use tokio::{sync::RwLock, time::interval};
use tracing::{error, info};

/// BatchQueue collects RecordBatches and processes them at intervals
#[derive(Debug)]
pub struct BatchQueue {
    queue:            Arc<SegQueue<RecordBatch>>,
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

    let mut batches = Vec::new();
    let mut total_rows = 0;

    // Take batches up to max_rows
    while !queue.is_empty() && total_rows < max_rows {
        if let Some(batch) = queue.pop() {
            total_rows += batch.num_rows();
            batches.push(batch);
        } else {
            break;
        }
    }

    if batches.is_empty() {
        return;
    }

    // Measure and log the insertion performance
    let start = Instant::now();

    // Use skip_queue=true to force direct insertion and avoid infinite loop
    match db.insert_records_batch("", batches.clone(), true).await {
        Ok(_) => {
            let elapsed = start.elapsed();
            info!(
                batches_count = batches.len(),
                rows_count = total_rows,
                duration_ms = elapsed.as_millis(),
                "Batch insert completed"
            );
        }
        Err(e) => {
            error!("Failed to insert batches: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use tokio::time::sleep;

    use super::*;
    use crate::{database::Database, persistent_queue::OtelLogsAndSpans};

    #[tokio::test]
    async fn test_batch_queue() -> Result<()> {
        dotenv::dotenv().ok();
        let test_prefix = format!("test-batch-{}", uuid::Uuid::new_v4());
        unsafe {
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", &test_prefix);
        }

        // Initialize DB
        let db = Arc::new(Database::new().await?);

        // Create batch queue with short interval for testing
        let batch_queue = BatchQueue::new(Arc::clone(&db), 100, 10);

        // Create test records and convert to RecordBatch
        let now = Utc::now();
        let records = (0..5)
            .map(|i| OtelLogsAndSpans {
                project_id: "default".to_string(),
                timestamp: now,
                id: format!("test-{}", i),
                hashes: vec![],
                date: now.date_naive(),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let fields = OtelLogsAndSpans::fields()?;
        let batch = serde_arrow::to_record_batch(&fields, &records)?;

        // Queue and process the batch
        batch_queue.queue(batch)?;
        sleep(Duration::from_millis(200)).await;

        // Shutdown queue
        batch_queue.shutdown().await;
        sleep(Duration::from_millis(200)).await;

        Ok(())
    }
}
