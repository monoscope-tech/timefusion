//! `timefusion.stats` — operator-visible introspection table.
//!
//! Exposes a flat (component, key, value) view of `BufferedWriteLayer` /
//! `MemBuffer` / `WalManager` internals so monitoring and bench harnesses
//! don't have to scrape `ps -o rss=` and guess what walrus is up to.
//!
//! Usage:
//!     SELECT * FROM timefusion_stats;
//!     SELECT key, value FROM timefusion_stats WHERE component='mem_buffer';

use crate::buffered_write_layer::BufferedWriteLayer;
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result as DFResult;
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct StatsTableProvider {
    layer: Option<Arc<BufferedWriteLayer>>,
    schema: SchemaRef,
}

impl StatsTableProvider {
    pub fn new(layer: Option<Arc<BufferedWriteLayer>>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("component", DataType::Utf8, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        Self { layer, schema }
    }

    fn snapshot_batch(&self) -> DFResult<RecordBatch> {
        let mut rows: Vec<(&'static str, String, String)> = Vec::with_capacity(16);

        if let Some(layer) = &self.layer {
            let s = layer.snapshot_stats();
            rows.push(("mem_buffer", "project_count".into(), s.mem_project_count.to_string()));
            rows.push(("mem_buffer", "total_buckets".into(), s.mem_total_buckets.to_string()));
            rows.push(("mem_buffer", "total_rows".into(), s.mem_total_rows.to_string()));
            rows.push(("mem_buffer", "total_batches".into(), s.mem_total_batches.to_string()));
            rows.push(("mem_buffer", "estimated_bytes".into(), s.mem_estimated_bytes.to_string()));
            rows.push(("mem_buffer", "estimated_mb".into(), format!("{:.1}", s.mem_estimated_bytes as f64 / (1024.0 * 1024.0))));
            rows.push(("mem_buffer", "bucket_duration_micros".into(), s.bucket_duration_micros.to_string()));
            rows.push(("buffered_layer", "reserved_bytes".into(), s.reserved_bytes.to_string()));
            rows.push(("buffered_layer", "max_memory_bytes".into(), s.max_memory_bytes.to_string()));
            rows.push(("buffered_layer", "max_memory_mb".into(), format!("{:.1}", s.max_memory_bytes as f64 / (1024.0 * 1024.0))));
            rows.push(("buffered_layer", "pressure_pct".into(), s.pressure_pct.to_string()));
            rows.push(("wal", "files".into(), s.wal_files.to_string()));
            rows.push(("wal", "disk_bytes".into(), s.wal_disk_bytes.to_string()));
            rows.push(("wal", "disk_mb".into(), format!("{:.1}", s.wal_disk_bytes as f64 / (1024.0 * 1024.0))));
            rows.push(("wal", "shards_per_topic".into(), s.wal_shards_per_topic.to_string()));
            rows.push(("wal", "known_topics".into(), s.wal_known_topics.to_string()));
        } else {
            rows.push(("buffered_layer", "status".into(), "disabled".into()));
        }

        if let Some(pc) = crate::plan_cache::global() {
            let (hits, misses) = pc.counters();
            let total = hits + misses;
            let hit_pct = if total > 0 { hits as f64 * 100.0 / total as f64 } else { 0.0 };
            rows.push(("plan_cache", "hits".into(), hits.to_string()));
            rows.push(("plan_cache", "misses".into(), misses.to_string()));
            rows.push(("plan_cache", "hit_pct".into(), format!("{:.1}", hit_pct)));
        }

        let components: Vec<&str> = rows.iter().map(|r| r.0).collect();
        let keys: Vec<&str> = rows.iter().map(|r| r.1.as_str()).collect();
        let values: Vec<&str> = rows.iter().map(|r| r.2.as_str()).collect();

        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(components)),
            Arc::new(StringArray::from(keys)),
            Arc::new(StringArray::from(values)),
        ];
        RecordBatch::try_new(Arc::clone(&self.schema), cols).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

#[async_trait]
impl TableProvider for StatsTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Build a fresh batch on every scan — counters move, we want point-in-time.
        let batch = self.snapshot_batch()?;
        let mem = MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}
