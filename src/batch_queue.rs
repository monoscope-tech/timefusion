use std::{sync::Arc, time::Duration};

use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tracing::{error, info};

#[derive(Debug)]
pub struct BatchQueue {
    tx:       mpsc::Sender<RecordBatch>,
    shutdown: tokio_util::sync::CancellationToken,
}

impl BatchQueue {
    pub fn new(db: Arc<crate::database::Database>, interval_ms: u64, max_rows: usize) -> Self {
        let channel_capacity = db.config().core.timefusion_batch_queue_capacity;
        let (tx, rx) = mpsc::channel(channel_capacity);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let shutdown_clone = shutdown.clone();

        tokio::spawn(async move {
            let stream = ReceiverStream::new(rx).chunks_timeout(max_rows, Duration::from_millis(interval_ms));
            tokio::pin!(stream);

            loop {
                tokio::select! {
                    Some(batches) = stream.next() => {
                        if !batches.is_empty() {
                            let mut grouped = std::collections::HashMap::<String, Vec<RecordBatch>>::new();
                            for batch in batches {
                                // Partition row-wise: a queued batch may carry rows for many projects.
                                match crate::database::partition_batch_by_project(batch, "default") {
                                    Ok(parts) => {
                                        for (project_id, sub) in parts {
                                            grouped.entry(project_id).or_default().push(sub);
                                        }
                                    }
                                    Err(e) => error!("Skipping batch: failed to partition by project_id: {}", e),
                                }
                            }

                            for (project_id, batches) in grouped {
                                let count = batches.len();
                                let row_counts: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();
                                if let Err(e) = db.insert_records_batch(&project_id, "otel_logs_and_spans", batches, true, None).await {
                                    error!("Failed to insert {} batches for project {}: {}", count, project_id, e);
                                } else {
                                    info!("Inserted {} batches with rows {:?} for project {}", count, row_counts, project_id);
                                }
                            }
                        }
                    }
                    _ = shutdown_clone.cancelled() => break,
                }
            }
        });

        Self { tx, shutdown }
    }

    pub fn queue(&self, batch: RecordBatch) -> Result<()> {
        self.tx.try_send(batch).map_err(|_| anyhow::anyhow!("Queue full"))
    }

    pub async fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use serial_test::serial;
    use tokio::time::sleep;

    use super::*;
    use crate::{database::Database, test_utils::test_helpers::*};

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_queue_processing() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(30), async {
            dotenv::dotenv().ok();
            unsafe {
                std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
                std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-bq-{}", uuid::Uuid::new_v4()));
            }

            let db = Arc::new(Database::new().await?);
            let batch_queue = BatchQueue::new(Arc::clone(&db), 100, 10);

            // Create test records
            let now = Utc::now();
            let records: Vec<serde_json::Value> = (0..5)
                .map(|i| {
                    let mut record = create_default_record();
                    record.insert("timestamp".to_string(), json!(now.timestamp_micros()));
                    record.insert("id".to_string(), json!(format!("test-{}", i)));
                    record.insert("project_id".to_string(), json!("test-project-uuid"));
                    record.insert("date".to_string(), json!(now.date_naive().to_string()));
                    record.insert("hashes".to_string(), json!([]));
                    record.insert("summary".to_string(), json!(vec![format!("Batch queue test record {}", i)]));
                    serde_json::Value::Object(record.into_iter().collect())
                })
                .collect();

            let batch = json_to_batch(records)?;
            batch_queue.queue(batch)?;

            // Wait for processing
            sleep(Duration::from_millis(200)).await;
            batch_queue.shutdown().await;
            sleep(Duration::from_millis(100)).await;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_queue_grouping() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(30), async {
            dotenv::dotenv().ok();
            unsafe {
                std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
                std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-bq-{}", uuid::Uuid::new_v4()));
            }

            let db = Arc::new(Database::new().await?);
            let batch_queue = BatchQueue::new(Arc::clone(&db), 100, 100);

            // Queue batches for different projects
            for project in ["project_a", "project_b", "project_c"] {
                let batch = json_to_batch(vec![test_span(&format!("id_{}", project), &format!("span_{}", project), project)])?;
                batch_queue.queue(batch)?;
            }

            // Wait for processing
            sleep(Duration::from_millis(200)).await;
            batch_queue.shutdown().await;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out"))?
    }
}
