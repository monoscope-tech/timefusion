use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use dashmap::DashMap;
use datafusion::logical_expr::Expr;
use std::sync::RwLock;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use tracing::{debug, info, instrument, warn};

const BUCKET_DURATION_MICROS: i64 = 10 * 60 * 1_000_000; // 10 minutes in microseconds

pub struct MemBuffer {
    projects: DashMap<String, ProjectBuffer>,
    estimated_bytes: AtomicUsize,
}

pub struct ProjectBuffer {
    table_buffers: DashMap<String, TableBuffer>,
}

pub struct TableBuffer {
    buckets: DashMap<i64, TimeBucket>,
    schema: SchemaRef,
}

pub struct TimeBucket {
    batches: RwLock<Vec<RecordBatch>>,
    row_count: AtomicUsize,
    memory_bytes: AtomicUsize,
    min_timestamp: AtomicI64,
    max_timestamp: AtomicI64,
}

#[derive(Debug, Clone)]
pub struct FlushableBucket {
    pub project_id: String,
    pub table_name: String,
    pub bucket_id: i64,
    pub batches: Vec<RecordBatch>,
    pub row_count: usize,
}

#[derive(Debug, Default)]
pub struct MemBufferStats {
    pub project_count: usize,
    pub total_buckets: usize,
    pub total_rows: usize,
    pub total_batches: usize,
    pub estimated_memory_bytes: usize,
}

fn estimate_batch_size(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
}

impl MemBuffer {
    pub fn new() -> Self {
        Self {
            projects: DashMap::new(),
            estimated_bytes: AtomicUsize::new(0),
        }
    }

    pub fn estimated_memory_bytes(&self) -> usize {
        self.estimated_bytes.load(Ordering::Relaxed)
    }

    fn compute_bucket_id(timestamp_micros: i64) -> i64 {
        timestamp_micros / BUCKET_DURATION_MICROS
    }

    pub fn current_bucket_id() -> i64 {
        let now_micros = chrono::Utc::now().timestamp_micros();
        Self::compute_bucket_id(now_micros)
    }

    #[instrument(skip(self, batch), fields(project_id, table_name, rows))]
    pub fn insert(&self, project_id: &str, table_name: &str, batch: RecordBatch, timestamp_micros: i64) -> anyhow::Result<()> {
        let bucket_id = Self::compute_bucket_id(timestamp_micros);
        let schema = batch.schema();
        let row_count = batch.num_rows();
        let batch_size = estimate_batch_size(&batch);

        let project = self.projects.entry(project_id.to_string()).or_insert_with(ProjectBuffer::new);

        // Check if table exists and validate schema
        if let Some(existing_table) = project.table_buffers.get(table_name) {
            let existing_schema = existing_table.schema();
            if existing_schema != schema {
                warn!(
                    "Schema mismatch for {}.{}: expected {} fields, got {}",
                    project_id,
                    table_name,
                    existing_schema.fields().len(),
                    schema.fields().len()
                );
                anyhow::bail!(
                    "Schema mismatch for {}.{}: incoming schema does not match existing schema",
                    project_id,
                    table_name
                );
            }
        }

        let table = project.table_buffers.entry(table_name.to_string()).or_insert_with(|| TableBuffer::new(schema.clone()));

        let bucket = table.buckets.entry(bucket_id).or_insert_with(TimeBucket::new);

        {
            let mut batches = bucket.batches.write().map_err(|e| anyhow::anyhow!("Failed to acquire write lock on bucket: {}", e))?;
            batches.push(batch);
        }

        bucket.row_count.fetch_add(row_count, Ordering::Relaxed);
        bucket.memory_bytes.fetch_add(batch_size, Ordering::Relaxed);
        bucket.update_timestamps(timestamp_micros);
        self.estimated_bytes.fetch_add(batch_size, Ordering::Relaxed);

        debug!(
            "MemBuffer insert: project={}, table={}, bucket={}, rows={}, bytes={}",
            project_id, table_name, bucket_id, row_count, batch_size
        );
        Ok(())
    }

    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub fn insert_batches(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>, timestamp_micros: i64) -> anyhow::Result<()> {
        for batch in batches {
            self.insert(project_id, table_name, batch, timestamp_micros)?;
        }
        Ok(())
    }

    #[instrument(skip(self, _filters), fields(project_id, table_name))]
    pub fn query(&self, project_id: &str, table_name: &str, _filters: &[Expr]) -> anyhow::Result<Vec<RecordBatch>> {
        let mut results = Vec::new();

        if let Some(project) = self.projects.get(project_id)
            && let Some(table) = project.table_buffers.get(table_name)
        {
            for bucket_entry in table.buckets.iter() {
                if let Ok(batches) = bucket_entry.batches.read() {
                    results.extend(batches.clone());
                }
            }
        }

        debug!("MemBuffer query: project={}, table={}, batches={}", project_id, table_name, results.len());
        Ok(results)
    }

    /// Query and return partitioned data - one partition per time bucket.
    /// This enables parallel execution across time buckets.
    #[instrument(skip(self), fields(project_id, table_name))]
    pub fn query_partitioned(&self, project_id: &str, table_name: &str) -> anyhow::Result<Vec<Vec<RecordBatch>>> {
        let mut partitions = Vec::new();

        if let Some(project) = self.projects.get(project_id)
            && let Some(table) = project.table_buffers.get(table_name)
        {
            // Sort buckets by bucket_id for consistent ordering
            let mut bucket_ids: Vec<i64> = table.buckets.iter().map(|b| *b.key()).collect();
            bucket_ids.sort();

            for bucket_id in bucket_ids {
                if let Some(bucket) = table.buckets.get(&bucket_id)
                    && let Ok(batches) = bucket.batches.read()
                    && !batches.is_empty()
                {
                    partitions.push(batches.clone());
                }
            }
        }

        debug!(
            "MemBuffer query_partitioned: project={}, table={}, partitions={}",
            project_id,
            table_name,
            partitions.len()
        );
        Ok(partitions)
    }

    /// Get the time range (oldest, newest) for a project/table.
    /// Returns None if no data exists.
    pub fn get_time_range(&self, project_id: &str, table_name: &str) -> Option<(i64, i64)> {
        let oldest = self.get_oldest_timestamp(project_id, table_name)?;
        let newest = self.get_newest_timestamp(project_id, table_name)?;
        if oldest == i64::MAX || newest == i64::MIN { None } else { Some((oldest, newest)) }
    }

    pub fn get_oldest_timestamp(&self, project_id: &str, table_name: &str) -> Option<i64> {
        self.projects.get(project_id).and_then(|project| {
            project.table_buffers.get(table_name).map(|table| {
                table
                    .buckets
                    .iter()
                    .map(|b| b.min_timestamp.load(Ordering::Relaxed))
                    .filter(|&ts| ts != i64::MAX)
                    .min()
                    .unwrap_or(i64::MAX)
            })
        })
    }

    pub fn get_newest_timestamp(&self, project_id: &str, table_name: &str) -> Option<i64> {
        self.projects.get(project_id).and_then(|project| {
            project.table_buffers.get(table_name).map(|table| {
                table
                    .buckets
                    .iter()
                    .map(|b| b.max_timestamp.load(Ordering::Relaxed))
                    .filter(|&ts| ts != i64::MIN)
                    .max()
                    .unwrap_or(i64::MIN)
            })
        })
    }

    #[instrument(skip(self), fields(project_id, table_name, bucket_id))]
    pub fn drain_bucket(&self, project_id: &str, table_name: &str, bucket_id: i64) -> Option<Vec<RecordBatch>> {
        if let Some(project) = self.projects.get(project_id)
            && let Some(table) = project.table_buffers.get(table_name)
            && let Some((_, bucket)) = table.buckets.remove(&bucket_id)
        {
            let freed_bytes = bucket.memory_bytes.load(Ordering::Relaxed);
            self.estimated_bytes.fetch_sub(freed_bytes, Ordering::Relaxed);
            if let Ok(batches) = bucket.batches.into_inner() {
                debug!(
                    "MemBuffer drain: project={}, table={}, bucket={}, batches={}, freed_bytes={}",
                    project_id, table_name, bucket_id, batches.len(), freed_bytes
                );
                return Some(batches);
            }
        }
        None
    }

    pub fn get_flushable_buckets(&self, cutoff_bucket_id: i64) -> Vec<FlushableBucket> {
        let mut flushable = Vec::new();

        for project_entry in self.projects.iter() {
            let project_id = project_entry.key().clone();
            for table_entry in project_entry.table_buffers.iter() {
                let table_name = table_entry.key().clone();
                for bucket_entry in table_entry.buckets.iter() {
                    let bucket_id = *bucket_entry.key();
                    if bucket_id < cutoff_bucket_id
                        && let Ok(batches) = bucket_entry.batches.read()
                        && !batches.is_empty()
                    {
                        flushable.push(FlushableBucket {
                            project_id: project_id.clone(),
                            table_name: table_name.clone(),
                            bucket_id,
                            batches: batches.clone(),
                            row_count: bucket_entry.row_count.load(Ordering::Relaxed),
                        });
                    }
                }
            }
        }

        info!("MemBuffer flushable buckets: count={}, cutoff={}", flushable.len(), cutoff_bucket_id);
        flushable
    }

    pub fn get_all_buckets(&self) -> Vec<FlushableBucket> {
        let mut all_buckets = Vec::new();

        for project_entry in self.projects.iter() {
            let project_id = project_entry.key().clone();
            for table_entry in project_entry.table_buffers.iter() {
                let table_name = table_entry.key().clone();
                for bucket_entry in table_entry.buckets.iter() {
                    let bucket_id = *bucket_entry.key();
                    if let Ok(batches) = bucket_entry.batches.read()
                        && !batches.is_empty()
                    {
                        all_buckets.push(FlushableBucket {
                            project_id: project_id.clone(),
                            table_name: table_name.clone(),
                            bucket_id,
                            batches: batches.clone(),
                            row_count: bucket_entry.row_count.load(Ordering::Relaxed),
                        });
                    }
                }
            }
        }

        all_buckets
    }

    #[instrument(skip(self))]
    pub fn evict_old_data(&self, cutoff_timestamp_micros: i64) -> usize {
        let cutoff_bucket_id = Self::compute_bucket_id(cutoff_timestamp_micros);
        let mut evicted_count = 0;
        let mut freed_bytes = 0usize;

        for project_entry in self.projects.iter() {
            for table_entry in project_entry.table_buffers.iter() {
                let bucket_ids_to_remove: Vec<i64> = table_entry.buckets.iter().filter(|b| *b.key() < cutoff_bucket_id).map(|b| *b.key()).collect();

                for bucket_id in bucket_ids_to_remove {
                    if let Some((_, bucket)) = table_entry.buckets.remove(&bucket_id) {
                        freed_bytes += bucket.memory_bytes.load(Ordering::Relaxed);
                        evicted_count += 1;
                    }
                }
            }
        }

        if freed_bytes > 0 {
            self.estimated_bytes.fetch_sub(freed_bytes, Ordering::Relaxed);
        }

        if evicted_count > 0 {
            info!(
                "MemBuffer evicted {} buckets older than bucket_id={}, freed {} bytes",
                evicted_count, cutoff_bucket_id, freed_bytes
            );
        }
        evicted_count
    }

    pub fn get_stats(&self) -> MemBufferStats {
        let mut stats = MemBufferStats {
            project_count: self.projects.len(),
            estimated_memory_bytes: self.estimated_bytes.load(Ordering::Relaxed),
            ..Default::default()
        };

        for project_entry in self.projects.iter() {
            for table_entry in project_entry.table_buffers.iter() {
                stats.total_buckets += table_entry.buckets.len();
                for bucket_entry in table_entry.buckets.iter() {
                    stats.total_rows += bucket_entry.row_count.load(Ordering::Relaxed);
                    if let Ok(batches) = bucket_entry.batches.read() {
                        stats.total_batches += batches.len();
                    }
                }
            }
        }

        stats
    }

    pub fn is_empty(&self) -> bool {
        self.projects.is_empty()
    }

    pub fn clear(&self) {
        self.projects.clear();
        self.estimated_bytes.store(0, Ordering::Relaxed);
        info!("MemBuffer cleared");
    }
}

impl Default for MemBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectBuffer {
    fn new() -> Self {
        Self { table_buffers: DashMap::new() }
    }
}

impl TableBuffer {
    fn new(schema: SchemaRef) -> Self {
        Self {
            buckets: DashMap::new(),
            schema,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl TimeBucket {
    fn new() -> Self {
        Self {
            batches: RwLock::new(Vec::new()),
            row_count: AtomicUsize::new(0),
            memory_bytes: AtomicUsize::new(0),
            min_timestamp: AtomicI64::new(i64::MAX),
            max_timestamp: AtomicI64::new(i64::MIN),
        }
    }

    fn update_timestamps(&self, timestamp: i64) {
        self.min_timestamp.fetch_min(timestamp, Ordering::Relaxed);
        self.max_timestamp.fetch_max(timestamp, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn create_test_batch(timestamp_micros: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ts_array = TimestampMicrosecondArray::from(vec![timestamp_micros]).with_timezone("UTC");
        let id_array = Int64Array::from(vec![1]);
        let name_array = StringArray::from(vec!["test"]);
        RecordBatch::try_new(schema, vec![Arc::new(ts_array), Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[test]
    fn test_insert_and_query() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let batch = create_test_batch(ts);

        buffer.insert("project1", "table1", batch.clone(), ts).unwrap();

        let results = buffer.query("project1", "table1", &[]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
    }

    #[test]
    fn test_bucket_partitioning() {
        let buffer = MemBuffer::new();
        let now = chrono::Utc::now().timestamp_micros();

        let ts1 = now;
        let ts2 = now + BUCKET_DURATION_MICROS; // Next bucket

        buffer.insert("project1", "table1", create_test_batch(ts1), ts1).unwrap();
        buffer.insert("project1", "table1", create_test_batch(ts2), ts2).unwrap();

        let results = buffer.query("project1", "table1", &[]).unwrap();
        assert_eq!(results.len(), 2);

        let stats = buffer.get_stats();
        assert_eq!(stats.total_buckets, 2);
    }

    #[test]
    fn test_drain_bucket() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let bucket_id = MemBuffer::compute_bucket_id(ts);

        buffer.insert("project1", "table1", create_test_batch(ts), ts).unwrap();

        let drained = buffer.drain_bucket("project1", "table1", bucket_id);
        assert!(drained.is_some());
        assert_eq!(drained.unwrap().len(), 1);

        let results = buffer.query("project1", "table1", &[]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_evict_old_data() {
        let buffer = MemBuffer::new();
        let old_ts = chrono::Utc::now().timestamp_micros() - 2 * BUCKET_DURATION_MICROS;
        let new_ts = chrono::Utc::now().timestamp_micros();

        buffer.insert("project1", "table1", create_test_batch(old_ts), old_ts).unwrap();
        buffer.insert("project1", "table1", create_test_batch(new_ts), new_ts).unwrap();

        let evicted = buffer.evict_old_data(new_ts - BUCKET_DURATION_MICROS / 2);
        assert_eq!(evicted, 1);

        let results = buffer.query("project1", "table1", &[]).unwrap();
        assert_eq!(results.len(), 1);
    }
}
