use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, TimestampMicrosecondArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use dashmap::DashMap;
use datafusion::common::DFSchema;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser as SqlParser;
use std::sync::RwLock;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use tracing::{debug, info, instrument, warn};

// 10-minute buckets balance flush granularity vs overhead. Shorter = more flushes,
// longer = larger Delta files. Matches default flush interval for aligned boundaries.
// Note: Timestamps before 1970 (negative microseconds) produce negative bucket IDs,
// which is supported but may result in unexpected ordering if mixed with post-1970 data.
const BUCKET_DURATION_MICROS: i64 = 10 * 60 * 1_000_000;

/// Check if two schemas are compatible for merge.
/// Compatible means: all existing fields must be present in incoming schema with same type,
/// incoming schema may have additional nullable fields.
fn schemas_compatible(existing: &SchemaRef, incoming: &SchemaRef) -> bool {
    for existing_field in existing.fields() {
        match incoming.field_with_name(existing_field.name()) {
            Ok(incoming_field) => {
                // Types must match (ignoring nullability - can become more lenient)
                if !types_compatible(existing_field.data_type(), incoming_field.data_type()) {
                    return false;
                }
            }
            Err(_) => return false, // Existing field not found in incoming schema
        }
    }
    // New fields in incoming schema are OK if nullable (for SchemaMode::Merge compatibility)
    for incoming_field in incoming.fields() {
        if existing.field_with_name(incoming_field.name()).is_err() && !incoming_field.is_nullable() {
            return false; // New non-nullable field would break existing data
        }
    }
    true
}

fn types_compatible(existing: &DataType, incoming: &DataType) -> bool {
    match (existing, incoming) {
        // Timestamps: unit must match, timezone differences are allowed but logged
        (DataType::Timestamp(u1, tz1), DataType::Timestamp(u2, tz2)) => {
            if u1 == u2 && tz1 != tz2 {
                tracing::debug!("Timestamp timezone mismatch: {:?} vs {:?} (allowed)", tz1, tz2);
            }
            u1 == u2
        }
        // Lists: check element types recursively
        (DataType::List(f1), DataType::List(f2)) | (DataType::LargeList(f1), DataType::LargeList(f2)) => types_compatible(f1.data_type(), f2.data_type()),
        // Structs: all existing fields must be compatible
        (DataType::Struct(fields1), DataType::Struct(fields2)) => {
            for f1 in fields1.iter() {
                match fields2.iter().find(|f| f.name() == f1.name()) {
                    Some(f2) => {
                        if !types_compatible(f1.data_type(), f2.data_type()) {
                            return false;
                        }
                    }
                    None => return false, // Field missing in incoming
                }
            }
            true
        }
        // Maps: check key and value types
        (DataType::Map(f1, _), DataType::Map(f2, _)) => types_compatible(f1.data_type(), f2.data_type()),
        // Dictionary: compare value types (key types can differ)
        (DataType::Dictionary(_, v1), DataType::Dictionary(_, v2)) => types_compatible(v1, v2),
        // Decimals: precision/scale must match
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => p1 == p2 && s1 == s2,
        (DataType::Decimal256(p1, s1), DataType::Decimal256(p2, s2)) => p1 == p2 && s1 == s2,
        // Fixed size types: size must match
        (DataType::FixedSizeBinary(n1), DataType::FixedSizeBinary(n2)) => n1 == n2,
        (DataType::FixedSizeList(f1, n1), DataType::FixedSizeList(f2, n2)) => n1 == n2 && types_compatible(f1.data_type(), f2.data_type()),
        // All other types: exact match
        _ => existing == incoming,
    }
}

/// Extract the min timestamp from a batch's "timestamp" column (if present).
/// Returns None if no timestamp column exists or it's empty.
pub fn extract_min_timestamp(batch: &RecordBatch) -> Option<i64> {
    let schema = batch.schema();
    let ts_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == "timestamp" && matches!(f.data_type(), DataType::Timestamp(TimeUnit::Microsecond, _)))?;
    let ts_col = batch.column(ts_idx);
    let ts_array = ts_col.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
    arrow::compute::min(ts_array)
}

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

pub fn estimate_batch_size(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
}

/// Merge two arrays based on a boolean mask.
/// For each row: if mask[i] is true, use new_values[i], else use original[i].
fn merge_arrays(original: &ArrayRef, new_values: &ArrayRef, mask: &BooleanArray) -> DFResult<ArrayRef> {
    arrow::compute::kernels::zip::zip(mask, new_values, original).map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Parse a SQL WHERE clause fragment into a DataFusion Expr.
fn parse_sql_predicate(sql: &str) -> DFResult<Expr> {
    let dialect = GenericDialect {};
    let sql_expr = SqlParser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|e| datafusion::error::DataFusionError::SQL(e.into(), None))?
        .parse_expr()
        .map_err(|e| datafusion::error::DataFusionError::SQL(e.into(), None))?;
    let context_provider = EmptyContextProvider;
    let planner = SqlToRel::new(&context_provider);
    planner.sql_to_expr(sql_expr, &DFSchema::empty(), &mut Default::default())
}

/// Parse a SQL expression (for UPDATE SET values).
fn parse_sql_expr(sql: &str) -> DFResult<Expr> {
    // Reuse the same parsing logic
    parse_sql_predicate(sql)
}

/// Minimal context provider for SQL parsing (no tables/schemas needed for simple expressions)
struct EmptyContextProvider;

impl datafusion::sql::planner::ContextProvider for EmptyContextProvider {
    fn get_table_source(&self, _name: datafusion::sql::TableReference) -> DFResult<std::sync::Arc<dyn datafusion::logical_expr::TableSource>> {
        Err(datafusion::error::DataFusionError::Plan("No table context available".into()))
    }
    fn get_function_meta(&self, _name: &str) -> Option<std::sync::Arc<datafusion::logical_expr::ScalarUDF>> {
        None
    }
    fn get_aggregate_meta(&self, _name: &str) -> Option<std::sync::Arc<datafusion::logical_expr::AggregateUDF>> {
        None
    }
    fn get_window_meta(&self, _name: &str) -> Option<std::sync::Arc<datafusion::logical_expr::WindowUDF>> {
        None
    }
    fn get_variable_type(&self, _var: &[String]) -> Option<DataType> {
        None
    }
    fn options(&self) -> &datafusion::config::ConfigOptions {
        static OPTIONS: std::sync::LazyLock<datafusion::config::ConfigOptions> = std::sync::LazyLock::new(datafusion::config::ConfigOptions::default);
        &OPTIONS
    }
    fn udf_names(&self) -> Vec<String> {
        vec![]
    }
    fn udaf_names(&self) -> Vec<String> {
        vec![]
    }
    fn udwf_names(&self) -> Vec<String> {
        vec![]
    }
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

        // Atomic schema validation and table creation using entry API
        let table = match project.table_buffers.entry(table_name.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                let existing_schema = entry.get().schema();
                // Fast path: same Arc pointer means identical schema
                if !std::sync::Arc::ptr_eq(&existing_schema, &schema) && !schemas_compatible(&existing_schema, &schema) {
                    warn!(
                        "Schema incompatible for {}.{}: existing has {} fields, incoming has {}",
                        project_id,
                        table_name,
                        existing_schema.fields().len(),
                        schema.fields().len()
                    );
                    anyhow::bail!(
                        "Schema incompatible for {}.{}: field types don't match or new non-nullable field added",
                        project_id,
                        table_name
                    );
                }
                entry.into_ref().downgrade()
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => entry.insert(TableBuffer::new(schema.clone())).downgrade(),
        };

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
                    // RecordBatch clone is cheap: Arc<Schema> + Vec<Arc<Array>>
                    // Only clones pointers (~100 bytes/batch), NOT the underlying data
                    // A 4GB buffer query adds ~1MB overhead, not 4GB
                    results.extend(batches.iter().cloned());
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
                    // RecordBatch clone is cheap (~100 bytes/batch), data is Arc-shared
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
                    project_id,
                    table_name,
                    bucket_id,
                    batches.len(),
                    freed_bytes
                );
                return Some(batches);
            }
        }
        None
    }

    pub fn get_flushable_buckets(&self, cutoff_bucket_id: i64) -> Vec<FlushableBucket> {
        let flushable = self.collect_buckets(|bucket_id| bucket_id < cutoff_bucket_id);
        info!("MemBuffer flushable buckets: count={}, cutoff={}", flushable.len(), cutoff_bucket_id);
        flushable
    }

    pub fn get_all_buckets(&self) -> Vec<FlushableBucket> {
        self.collect_buckets(|_| true)
    }

    fn collect_buckets(&self, filter: impl Fn(i64) -> bool) -> Vec<FlushableBucket> {
        let mut result = Vec::new();
        for project in self.projects.iter() {
            let project_id = project.key().clone();
            for table in project.table_buffers.iter() {
                let table_name = table.key().clone();
                for bucket in table.buckets.iter() {
                    let bucket_id = *bucket.key();
                    if filter(bucket_id) {
                        if let Ok(batches) = bucket.batches.read() {
                            if !batches.is_empty() {
                                result.push(FlushableBucket {
                                    project_id: project_id.clone(),
                                    table_name: table_name.clone(),
                                    bucket_id,
                                    batches: batches.clone(),
                                    row_count: bucket.row_count.load(Ordering::Relaxed),
                                });
                            }
                        }
                    }
                }
            }
        }
        result
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

    /// Check if a table exists in the buffer
    pub fn has_table(&self, project_id: &str, table_name: &str) -> bool {
        self.projects.get(project_id).is_some_and(|project| project.table_buffers.contains_key(table_name))
    }

    /// Delete rows matching the predicate from the buffer.
    /// Returns the number of rows deleted.
    #[instrument(skip(self, predicate), fields(project_id, table_name, rows_deleted))]
    pub fn delete(&self, project_id: &str, table_name: &str, predicate: Option<&Expr>) -> DFResult<u64> {
        let Some(project) = self.projects.get(project_id) else {
            return Ok(0);
        };
        let Some(table) = project.table_buffers.get(table_name) else {
            return Ok(0);
        };

        let schema = table.schema();
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
        let props = ExecutionProps::new();

        let physical_predicate = predicate.map(|p| create_physical_expr(p, &df_schema, &props)).transpose()?;

        let mut total_deleted = 0u64;
        let mut memory_freed = 0usize;

        for mut bucket_entry in table.buckets.iter_mut() {
            let bucket = bucket_entry.value_mut();
            let mut batches = bucket.batches.write().map_err(|e| datafusion::error::DataFusionError::Execution(format!("Lock error: {}", e)))?;

            let mut new_batches = Vec::with_capacity(batches.len());
            for batch in batches.drain(..) {
                let original_rows = batch.num_rows();
                let original_size = estimate_batch_size(&batch);

                let filtered_batch = if let Some(ref phys_pred) = physical_predicate {
                    let result = phys_pred.evaluate(&batch)?;
                    let mask = result.into_array(batch.num_rows())?;
                    let bool_mask = mask
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| datafusion::error::DataFusionError::Execution("Predicate did not return boolean".into()))?;
                    // Invert mask: keep rows where predicate is FALSE
                    let inverted = arrow::compute::not(bool_mask)?;
                    filter_record_batch(&batch, &inverted)?
                } else {
                    // No predicate = delete all rows
                    RecordBatch::new_empty(batch.schema())
                };

                let deleted = original_rows - filtered_batch.num_rows();
                total_deleted += deleted as u64;

                if filtered_batch.num_rows() > 0 {
                    let new_size = estimate_batch_size(&filtered_batch);
                    memory_freed += original_size.saturating_sub(new_size);
                    new_batches.push(filtered_batch);
                } else {
                    memory_freed += original_size;
                }
            }

            *batches = new_batches;
            let new_row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            bucket.row_count.store(new_row_count, Ordering::Relaxed);
        }

        if memory_freed > 0 {
            self.estimated_bytes.fetch_sub(memory_freed, Ordering::Relaxed);
        }

        debug!("MemBuffer delete: project={}, table={}, rows_deleted={}", project_id, table_name, total_deleted);
        Ok(total_deleted)
    }

    /// Update rows matching the predicate with new values.
    /// Returns the number of rows updated.
    #[instrument(skip(self, predicate, assignments), fields(project_id, table_name, rows_updated))]
    pub fn update(&self, project_id: &str, table_name: &str, predicate: Option<&Expr>, assignments: &[(String, Expr)]) -> DFResult<u64> {
        if assignments.is_empty() {
            return Ok(0);
        }

        let Some(project) = self.projects.get(project_id) else {
            return Ok(0);
        };
        let Some(table) = project.table_buffers.get(table_name) else {
            return Ok(0);
        };

        let schema = table.schema();
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
        let props = ExecutionProps::new();

        let physical_predicate = predicate.map(|p| create_physical_expr(p, &df_schema, &props)).transpose()?;

        // Pre-compile assignment expressions
        let physical_assignments: Vec<_> = assignments
            .iter()
            .map(|(col, expr)| {
                let phys_expr = create_physical_expr(expr, &df_schema, &props)?;
                let col_idx = schema.index_of(col).map_err(|_| datafusion::error::DataFusionError::Execution(format!("Column '{}' not found", col)))?;
                Ok((col_idx, phys_expr))
            })
            .collect::<DFResult<Vec<_>>>()?;

        let mut total_updated = 0u64;

        for mut bucket_entry in table.buckets.iter_mut() {
            let bucket = bucket_entry.value_mut();
            let mut batches = bucket.batches.write().map_err(|e| datafusion::error::DataFusionError::Execution(format!("Lock error: {}", e)))?;

            let new_batches: Vec<RecordBatch> = batches
                .drain(..)
                .map(|batch| {
                    let num_rows = batch.num_rows();
                    if num_rows == 0 {
                        return Ok(batch);
                    }

                    // Evaluate predicate to find matching rows
                    let mask = if let Some(ref phys_pred) = physical_predicate {
                        let result = phys_pred.evaluate(&batch)?;
                        let arr = result.into_array(num_rows)?;
                        arr.as_any()
                            .downcast_ref::<BooleanArray>()
                            .cloned()
                            .ok_or_else(|| datafusion::error::DataFusionError::Execution("Predicate did not return boolean".into()))?
                    } else {
                        // No predicate = update all rows
                        BooleanArray::from(vec![true; num_rows])
                    };

                    let matching_count = mask.iter().filter(|v| v == &Some(true)).count();
                    total_updated += matching_count as u64;

                    if matching_count == 0 {
                        return Ok(batch);
                    }

                    // Build new columns with updated values
                    let new_columns: Vec<ArrayRef> = (0..batch.num_columns())
                        .map(|col_idx| {
                            // Check if this column has an assignment
                            if let Some((_, phys_expr)) = physical_assignments.iter().find(|(idx, _)| *idx == col_idx) {
                                // Evaluate the new value expression
                                let new_values = phys_expr.evaluate(&batch)?.into_array(num_rows)?;
                                // Merge: use new value where mask is true, original otherwise
                                merge_arrays(batch.column(col_idx), &new_values, &mask)
                            } else {
                                Ok(batch.column(col_idx).clone())
                            }
                        })
                        .collect::<DFResult<Vec<_>>>()?;

                    RecordBatch::try_new(batch.schema(), new_columns).map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
                })
                .collect::<DFResult<Vec<_>>>()?;

            *batches = new_batches;
        }

        debug!("MemBuffer update: project={}, table={}, rows_updated={}", project_id, table_name, total_updated);
        Ok(total_updated)
    }

    /// Delete rows using a SQL predicate string (for WAL recovery).
    /// Parses the SQL WHERE clause and delegates to delete().
    #[instrument(skip(self), fields(project_id, table_name))]
    pub fn delete_by_sql(&self, project_id: &str, table_name: &str, predicate_sql: Option<&str>) -> DFResult<u64> {
        let predicate = predicate_sql.map(parse_sql_predicate).transpose()?;
        self.delete(project_id, table_name, predicate.as_ref())
    }

    /// Update rows using SQL strings (for WAL recovery).
    /// Parses the SQL WHERE clause and assignment expressions, then delegates to update().
    #[instrument(skip(self, assignments), fields(project_id, table_name))]
    pub fn update_by_sql(&self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, assignments: &[(String, String)]) -> DFResult<u64> {
        let predicate = predicate_sql.map(parse_sql_predicate).transpose()?;
        let parsed_assignments: Vec<(String, Expr)> = assignments
            .iter()
            .map(|(col, val_sql)| parse_sql_expr(val_sql).map(|expr| (col.clone(), expr)))
            .collect::<DFResult<Vec<_>>>()?;
        self.update(project_id, table_name, predicate.as_ref(), &parsed_assignments)
    }

    pub fn get_stats(&self) -> MemBufferStats {
        let (mut total_buckets, mut total_rows, mut total_batches) = (0, 0, 0);
        for project in self.projects.iter() {
            for table in project.table_buffers.iter() {
                total_buckets += table.buckets.len();
                for bucket in table.buckets.iter() {
                    total_rows += bucket.row_count.load(Ordering::Relaxed);
                    total_batches += bucket.batches.read().map(|b| b.len()).unwrap_or(0);
                }
            }
        }
        MemBufferStats {
            project_count: self.projects.len(),
            total_buckets,
            total_rows,
            total_batches,
            estimated_memory_bytes: self.estimated_bytes.load(Ordering::Relaxed),
        }
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

    fn create_multi_row_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        let ts = chrono::Utc::now().timestamp_micros();
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ts_array = TimestampMicrosecondArray::from(vec![ts; ids.len()]).with_timezone("UTC");
        let id_array = Int64Array::from(ids);
        let name_array = StringArray::from(names);
        RecordBatch::try_new(schema, vec![Arc::new(ts_array), Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[test]
    fn test_delete_all_rows() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let batch = create_multi_row_batch(vec![1, 2, 3], vec!["a", "b", "c"]);

        buffer.insert("project1", "table1", batch, ts).unwrap();

        // Delete all rows (no predicate)
        let deleted = buffer.delete("project1", "table1", None).unwrap();
        assert_eq!(deleted, 3);

        let results = buffer.query("project1", "table1", &[]).unwrap();
        assert!(results.is_empty() || results.iter().all(|b| b.num_rows() == 0));
    }

    #[test]
    fn test_delete_with_predicate() {
        use datafusion::logical_expr::{col, lit};

        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let batch = create_multi_row_batch(vec![1, 2, 3], vec!["a", "b", "c"]);

        buffer.insert("project1", "table1", batch, ts).unwrap();

        // Delete rows where id = 2
        let predicate = col("id").eq(lit(2i64));
        let deleted = buffer.delete("project1", "table1", Some(&predicate)).unwrap();
        assert_eq!(deleted, 1);

        let results = buffer.query("project1", "table1", &[]).unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_update_with_predicate() {
        use datafusion::logical_expr::{col, lit};

        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let batch = create_multi_row_batch(vec![1, 2, 3], vec!["a", "b", "c"]);

        buffer.insert("project1", "table1", batch, ts).unwrap();

        // Update name to "updated" where id = 2
        let predicate = col("id").eq(lit(2i64));
        let assignments = vec![("name".to_string(), lit("updated"))];
        let updated = buffer.update("project1", "table1", Some(&predicate), &assignments).unwrap();
        assert_eq!(updated, 1);

        // Verify the update
        let results = buffer.query("project1", "table1", &[]).unwrap();
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 3);

        let name_col = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(name_col.value(0), "a");
        assert_eq!(name_col.value(1), "updated");
        assert_eq!(name_col.value(2), "c");
    }

    #[test]
    fn test_has_table() {
        let buffer = MemBuffer::new();
        assert!(!buffer.has_table("project1", "table1"));

        let ts = chrono::Utc::now().timestamp_micros();
        buffer.insert("project1", "table1", create_test_batch(ts), ts).unwrap();

        assert!(buffer.has_table("project1", "table1"));
        assert!(!buffer.has_table("project1", "table2"));
        assert!(!buffer.has_table("project2", "table1"));
    }

    #[test]
    fn test_bucket_boundary_exact() {
        let buffer = MemBuffer::new();

        // Test timestamps exactly at bucket boundaries
        let bucket_0_start = 0i64;
        let bucket_1_start = BUCKET_DURATION_MICROS;
        let bucket_2_start = BUCKET_DURATION_MICROS * 2;

        assert_eq!(MemBuffer::compute_bucket_id(bucket_0_start), 0);
        assert_eq!(MemBuffer::compute_bucket_id(bucket_1_start), 1);
        assert_eq!(MemBuffer::compute_bucket_id(bucket_2_start), 2);

        // Insert at exact boundary
        buffer.insert("project1", "table1", create_test_batch(bucket_1_start), bucket_1_start).unwrap();

        let stats = buffer.get_stats();
        assert_eq!(stats.total_buckets, 1);
    }

    #[test]
    fn test_bucket_boundary_one_before() {
        let buffer = MemBuffer::new();

        // Test timestamp one microsecond before bucket boundary
        let just_before_bucket_1 = BUCKET_DURATION_MICROS - 1;
        let bucket_1_start = BUCKET_DURATION_MICROS;

        assert_eq!(MemBuffer::compute_bucket_id(just_before_bucket_1), 0);
        assert_eq!(MemBuffer::compute_bucket_id(bucket_1_start), 1);

        buffer.insert("project1", "table1", create_test_batch(just_before_bucket_1), just_before_bucket_1).unwrap();
        buffer.insert("project1", "table1", create_test_batch(bucket_1_start), bucket_1_start).unwrap();

        let stats = buffer.get_stats();
        assert_eq!(stats.total_buckets, 2, "Should have 2 separate buckets");
    }

    #[test]
    fn test_schema_compatibility_race_condition() {
        use std::sync::Arc;
        use std::thread;

        let buffer = Arc::new(MemBuffer::new());
        let ts = chrono::Utc::now().timestamp_micros();

        // Create two batches with compatible schemas
        let batch1 = create_test_batch(ts);

        // Spawn multiple threads trying to insert simultaneously
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let buffer = Arc::clone(&buffer);
                let batch = batch1.clone();
                thread::spawn(move || buffer.insert("project1", "table1", batch, ts + i))
            })
            .collect();

        // All should succeed since schemas are compatible
        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        let results = buffer.query("project1", "table1", &[]).unwrap();
        assert_eq!(results.len(), 10, "All 10 inserts should succeed");
    }
}
