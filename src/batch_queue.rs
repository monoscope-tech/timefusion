use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use datafusion::arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[derive(Debug)]
pub struct BatchQueue {
    tx: mpsc::Sender<RecordBatch>,
    shutdown: CancellationToken,
}

/// Row-wise partition of a chunk: one queued batch may carry rows for many projects.
fn group_by_project(batches: impl IntoIterator<Item = RecordBatch>) -> HashMap<String, Vec<RecordBatch>> {
    batches
        .into_iter()
        .filter_map(|batch| {
            crate::database::partition_batch_by_project(batch, "default")
                .inspect_err(|e| error!(error = %e, "Skipping batch: failed to partition by project_id"))
                .ok()
        })
        .flatten()
        .fold(HashMap::new(), |mut grouped, (project_id, sub)| {
            grouped.entry(project_id).or_default().push(sub);
            grouped
        })
}

impl BatchQueue {
    pub fn new(db: Arc<crate::database::Database>, interval_ms: u64, max_rows: usize) -> Self {
        let (tx, rx) = mpsc::channel(db.config().core.timefusion_batch_queue_capacity);
        let shutdown = CancellationToken::new();
        let cancelled = shutdown.clone();

        tokio::spawn(async move {
            let stream = ReceiverStream::new(rx).chunks_timeout(max_rows, Duration::from_millis(interval_ms));
            tokio::pin!(stream);

            while let Some(chunk) = tokio::select! {
                chunk = stream.next() => chunk,
                _ = cancelled.cancelled() => None,
            } {
                // Effectful loop: one awaited Delta insert per project.
                for (project_id, batches) in group_by_project(chunk) {
                    let (count, rows) = (batches.len(), batches.iter().map(RecordBatch::num_rows).collect::<Vec<_>>());
                    match db.insert_records_batch(&project_id, "otel_logs_and_spans", batches, true, None).await {
                        Ok(_) => info!(%project_id, count, ?rows, "Inserted batches"),
                        Err(e) => error!(%project_id, count, error = %e, "Failed to insert batches"),
                    }
                }
            }
        });

        Self { tx, shutdown }
    }

    pub fn queue(&self, batch: RecordBatch) -> Result<()> {
        self.tx.try_send(batch).map_err(|_| anyhow!("Queue full"))
    }

    pub async fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use tokio::time::sleep;

    use super::*;
    use crate::{database::Database, test_utils::test_helpers::*};

    /// SAFETY: `set_var` races other threads' env reads; every caller is `#[serial]`.
    async fn test_queue(max_rows: usize) -> Result<BatchQueue> {
        dotenv::dotenv().ok();
        unsafe {
            std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-bq-{}", uuid::Uuid::new_v4()));
        }
        Ok(BatchQueue::new(Arc::new(Database::new().await?), 100, max_rows))
    }

    /// Queue one span per (id, project), let the flush interval fire, then shut down.
    async fn run(max_rows: usize, spans: impl IntoIterator<Item = (String, String)>) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(30), async move {
            let queue = test_queue(max_rows).await?;
            spans.into_iter().try_for_each(|(id, project)| queue.queue(json_to_batch(vec![test_span(&id, &format!("span_{project}"), &project)])?))?;
            sleep(Duration::from_millis(200)).await;
            queue.shutdown().await;
            Ok(())
        })
        .await
        .map_err(|_| anyhow!("Test timed out"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_queue_processing() -> Result<()> {
        run(10, (0..5).map(|i| (format!("test-{i}"), "test-project-uuid".to_string()))).await
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_queue_grouping() -> Result<()> {
        run(100, ["project_a", "project_b", "project_c"].map(|p| (format!("id_{p}"), p.to_string()))).await
    }
}
