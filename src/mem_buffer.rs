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
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use tracing::{debug, info, instrument, warn};

// 10-minute buckets balance flush granularity vs overhead. Shorter = more flushes,
// longer = larger Delta files. Matches default flush interval for aligned boundaries.
// Note: Timestamps before 1970 (negative microseconds) produce negative bucket IDs,
// which is supported but may result in unexpected ordering if mixed with post-1970 data.
const DEFAULT_BUCKET_DURATION_MICROS: i64 = 10 * 60 * 1_000_000;
#[cfg(test)]
const BUCKET_DURATION_MICROS: i64 = DEFAULT_BUCKET_DURATION_MICROS;

static BUCKET_DURATION_MICROS_CFG: std::sync::OnceLock<i64> = std::sync::OnceLock::new();

/// Configured bucket window in microseconds. Set once at startup via
/// `set_bucket_duration_micros`; defaults to 10 minutes when unset. Smaller
/// windows free MemBuffer memory sooner (because the previous bucket becomes
/// flushable sooner) at the cost of more, smaller Delta commits.
pub fn bucket_duration_micros() -> i64 {
    *BUCKET_DURATION_MICROS_CFG.get_or_init(|| DEFAULT_BUCKET_DURATION_MICROS)
}

/// Set the bucket window. No-op after the first call (OnceLock). Must be
/// invoked before any MemBuffer activity, e.g. from `init_config`.
pub fn set_bucket_duration_micros(micros: i64) {
    let _ = BUCKET_DURATION_MICROS_CFG.set(micros.max(1_000_000));
}

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
    let mut new_fields = 0;
    for incoming_field in incoming.fields() {
        if existing.field_with_name(incoming_field.name()).is_err() {
            if !incoming_field.is_nullable() {
                return false; // New non-nullable field would break existing data
            }
            new_fields += 1;
        }
    }
    if new_fields > 0 {
        info!("Schema evolution: {} new nullable field(s) added", new_fields);
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

/// Table key type using Arc<str> for efficient cloning and comparison.
/// Composite key of (project_id, table_name) for flattened lookup.
pub type TableKey = (Arc<str>, Arc<str>);

pub struct MemBuffer {
    /// Flattened structure: (project_id, table_name) → TableBuffer
    /// Reduces 3 hash lookups to 1 for table access.
    tables: DashMap<TableKey, Arc<TableBuffer>>,
    estimated_bytes: AtomicUsize,
}

pub struct TableBuffer {
    buckets: DashMap<i64, TimeBucket>,
    schema: SchemaRef, // Immutable after creation - no lock needed
    project_id: Arc<str>,
    table_name: Arc<str>,
}

pub struct TimeBucket {
    batches: Mutex<Vec<RecordBatch>>,
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
    /// Min `min_timestamp` across all buckets in microseconds, or None if empty.
    /// Used to derive `mem_buffer_oldest_bucket_age_seconds` for the metrics
    /// exporter — a key staleness signal (alert if > 2× flush interval).
    pub oldest_bucket_micros: Option<i64>,
}

/// Per-batch fixed overhead: RecordBatch struct, schema Arc bump, ArrayData
/// metadata for each column, and DashMap/Mutex slots when held in a TimeBucket.
/// Empirically ~64 B for the batch + 96 B per column (ArrayData + Buffer headers).
const BATCH_FIXED_OVERHEAD: usize = 64;
const PER_COLUMN_OVERHEAD: usize = 96;

fn apply_signed_delta(counter: &AtomicUsize, delta: i64) {
    if delta > 0 {
        counter.fetch_add(delta as usize, Ordering::Relaxed);
    } else if delta < 0 {
        counter.fetch_sub((-delta) as usize, Ordering::Relaxed);
    }
}

pub fn estimate_batch_size(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size() + BATCH_FIXED_OVERHEAD + batch.num_columns() * PER_COLUMN_OVERHEAD
}

/// Merge two arrays based on a boolean mask.
/// For each row: if mask[i] is true, use new_values[i], else use original[i].
fn merge_arrays(original: &ArrayRef, new_values: &ArrayRef, mask: &BooleanArray) -> DFResult<ArrayRef> {
    // Cast new_values to match original's type if they differ (e.g., Utf8 -> Utf8View)
    let new_values = if original.data_type() != new_values.data_type() {
        arrow::compute::cast(new_values, original.data_type()).map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?
    } else {
        new_values.clone()
    };
    arrow::compute::kernels::zip::zip(mask, &new_values, original).map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
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
    fn get_table_source(&self, _: datafusion::sql::TableReference) -> DFResult<std::sync::Arc<dyn datafusion::logical_expr::TableSource>> {
        Err(datafusion::error::DataFusionError::Plan("No table context".into()))
    }
    fn get_function_meta(&self, _: &str) -> Option<std::sync::Arc<datafusion::logical_expr::ScalarUDF>> {
        None
    }
    fn get_aggregate_meta(&self, _: &str) -> Option<std::sync::Arc<datafusion::logical_expr::AggregateUDF>> {
        None
    }
    fn get_window_meta(&self, _: &str) -> Option<std::sync::Arc<datafusion::logical_expr::WindowUDF>> {
        None
    }
    fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
        None
    }
    fn options(&self) -> &datafusion::config::ConfigOptions {
        static O: std::sync::LazyLock<datafusion::config::ConfigOptions> = std::sync::LazyLock::new(Default::default);
        &O
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

/// Extract min/max timestamp bounds from filter expressions for bucket pruning.
fn extract_timestamp_range(filters: &[Expr]) -> (Option<i64>, Option<i64>) {
    let (mut min_ts, mut max_ts) = (None, None);
    for filter in filters {
        if let Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr { left, op, right }) = filter {
            let is_ts = matches!(left.as_ref(), Expr::Column(c) if c.name == "timestamp");
            if !is_ts {
                continue;
            }
            let ts = match right.as_ref() {
                Expr::Literal(datafusion::scalar::ScalarValue::TimestampMicrosecond(Some(ts), _), _) => Some(*ts),
                Expr::Literal(datafusion::scalar::ScalarValue::TimestampNanosecond(Some(ts), _), _) => Some(*ts / 1000),
                Expr::Literal(datafusion::scalar::ScalarValue::TimestampMillisecond(Some(ts), _), _) => Some(*ts * 1000),
                Expr::Literal(datafusion::scalar::ScalarValue::TimestampSecond(Some(ts), _), _) => Some(*ts * 1_000_000),
                _ => None,
            };
            if let Some(ts) = ts {
                match op {
                    datafusion::logical_expr::Operator::Gt | datafusion::logical_expr::Operator::GtEq => {
                        min_ts = Some(min_ts.map_or(ts, |m: i64| m.max(ts)));
                    }
                    datafusion::logical_expr::Operator::Lt | datafusion::logical_expr::Operator::LtEq => {
                        max_ts = Some(max_ts.map_or(ts, |m: i64| m.min(ts)));
                    }
                    datafusion::logical_expr::Operator::Eq => {
                        min_ts = Some(ts);
                        max_ts = Some(ts);
                    }
                    _ => {}
                }
            }
        }
    }
    (min_ts, max_ts)
}

/// Compile filters into a single conjunction physical expression evaluated against `schema`.
fn compile_filter_conjunction(filters: &[Expr], schema: &SchemaRef) -> DFResult<Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>> {
    if filters.is_empty() {
        return Ok(None);
    }
    let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
    let props = ExecutionProps::new();
    let conjunction = filters.iter().cloned().reduce(datafusion::logical_expr::and).unwrap();
    Ok(Some(create_physical_expr(&conjunction, &df_schema, &props)?))
}

/// Apply a compiled predicate, returning only matching rows. Best-effort: on
/// any evaluation error we return the original batch so DataFusion's FilterExec
/// can finish the job.
fn apply_predicate(batch: &RecordBatch, pred: &Arc<dyn datafusion::physical_expr::PhysicalExpr>) -> RecordBatch {
    let Ok(value) = pred.evaluate(batch) else { return batch.clone() };
    let Ok(arr) = value.into_array(batch.num_rows()) else { return batch.clone() };
    let Some(mask) = arr.as_any().downcast_ref::<BooleanArray>() else { return batch.clone() };
    filter_record_batch(batch, mask).unwrap_or_else(|_| batch.clone())
}

/// Check if a bucket's time range overlaps with the query range.
fn bucket_overlaps_range(bucket: &TimeBucket, range: &(Option<i64>, Option<i64>)) -> bool {
    let (min_filter, max_filter) = range;
    if let Some(max) = max_filter {
        let bucket_min = bucket.min_timestamp.load(Ordering::Relaxed);
        if bucket_min != i64::MAX && bucket_min > *max {
            return false;
        }
    }
    if let Some(min) = min_filter {
        let bucket_max = bucket.max_timestamp.load(Ordering::Relaxed);
        if bucket_max != i64::MIN && bucket_max < *min {
            return false;
        }
    }
    true
}

impl MemBuffer {
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            estimated_bytes: AtomicUsize::new(0),
        }
    }

    pub fn estimated_memory_bytes(&self) -> usize {
        self.estimated_bytes.load(Ordering::Relaxed)
    }

    pub fn compute_bucket_id(timestamp_micros: i64) -> i64 {
        timestamp_micros / bucket_duration_micros()
    }

    #[inline]
    fn make_key(project_id: &str, table_name: &str) -> TableKey {
        (Arc::from(project_id), Arc::from(table_name))
    }

    pub fn current_bucket_id() -> i64 {
        let now_micros = crate::clock::now_micros();
        Self::compute_bucket_id(now_micros)
    }

    /// Get or create a TableBuffer, returning a cached Arc reference.
    /// This is the preferred entry point for batch operations - cache the returned
    /// Arc<TableBuffer> and call insert_batch() directly to avoid repeated lookups.
    pub fn get_or_create_table(&self, project_id: &str, table_name: &str, schema: &SchemaRef) -> anyhow::Result<Arc<TableBuffer>> {
        let key = Self::make_key(project_id, table_name);

        // Fast path: table exists
        if let Some(table) = self.tables.get(&key) {
            let existing_schema = table.schema();
            if !Arc::ptr_eq(&existing_schema, schema) && !schemas_compatible(&existing_schema, schema) {
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
            return Ok(Arc::clone(&table));
        }

        // Slow path: create table using entry API
        let table = match self.tables.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                let existing_schema = entry.get().schema();
                if !Arc::ptr_eq(&existing_schema, schema) && !schemas_compatible(&existing_schema, schema) {
                    anyhow::bail!(
                        "Schema incompatible for {}.{}: field types don't match or new non-nullable field added",
                        project_id,
                        table_name
                    );
                }
                Arc::clone(entry.get())
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let new_table = Arc::new(TableBuffer::new(schema.clone(), Arc::from(project_id), Arc::from(table_name)));
                entry.insert(Arc::clone(&new_table));
                new_table
            }
        };

        Ok(table)
    }

    /// Get a TableBuffer if it exists (for read operations).
    fn get_table(&self, project_id: &str, table_name: &str) -> Option<Arc<TableBuffer>> {
        let key = Self::make_key(project_id, table_name);
        self.tables.get(&key).map(|t| Arc::clone(&t))
    }

    #[instrument(skip(self, batch), fields(project_id, table_name, rows))]
    pub fn insert(&self, project_id: &str, table_name: &str, batch: RecordBatch, timestamp_micros: i64) -> anyhow::Result<()> {
        let schema = batch.schema();
        let table = self.get_or_create_table(project_id, table_name, &schema)?;
        let batch_size = table.insert_batch(batch, timestamp_micros)?;
        self.estimated_bytes.fetch_add(batch_size, Ordering::Relaxed);
        Ok(())
    }

    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub fn insert_batches(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>, timestamp_micros: i64) -> anyhow::Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let schema = batches[0].schema();
        let table = self.get_or_create_table(project_id, table_name, &schema)?;

        let mut total_size = 0usize;
        for batch in batches {
            total_size += table.insert_batch(batch, timestamp_micros)?;
        }
        self.estimated_bytes.fetch_add(total_size, Ordering::Relaxed);
        Ok(())
    }

    #[instrument(skip(self, filters), fields(project_id, table_name))]
    pub fn query(&self, project_id: &str, table_name: &str, filters: &[Expr]) -> anyhow::Result<Vec<RecordBatch>> {
        let mut results = Vec::new();
        let ts_range = extract_timestamp_range(filters);

        if let Some(table) = self.get_table(project_id, table_name) {
            // Pre-compile filters into a single physical predicate so each batch is
            // filtered to matching rows before returning. Best-effort: anything that
            // fails to compile is left for FilterExec on top to evaluate.
            let pred = compile_filter_conjunction(filters, &table.schema).ok().flatten();
            for bucket_entry in table.buckets.iter() {
                let bucket = bucket_entry.value();
                if !bucket_overlaps_range(bucket, &ts_range) {
                    continue;
                }
                // Hold the lock only long enough to clone Arc'd batch refs; release
                // before filtering so writers / concurrent readers aren't blocked.
                let snapshot: Vec<RecordBatch> = bucket.batches.lock().iter().cloned().collect();
                match &pred {
                    Some(p) => results.extend(snapshot.iter().map(|b| apply_predicate(b, p)).filter(|b| b.num_rows() > 0)),
                    None => results.extend(snapshot),
                }
            }
        }

        debug!("MemBuffer query: project={}, table={}, batches={}", project_id, table_name, results.len());
        Ok(results)
    }

    /// Query and return partitioned data - one partition per time bucket.
    /// This enables parallel execution across time buckets.
    /// Optional filters enable timestamp-based bucket pruning.
    #[instrument(skip(self, filters), fields(project_id, table_name))]
    pub fn query_partitioned(&self, project_id: &str, table_name: &str, filters: &[Expr]) -> anyhow::Result<Vec<Vec<RecordBatch>>> {
        let mut partitions = Vec::new();
        let ts_range = extract_timestamp_range(filters);

        if let Some(table) = self.get_table(project_id, table_name) {
            let pred = compile_filter_conjunction(filters, &table.schema).ok().flatten();
            let mut bucket_ids: Vec<i64> = table.buckets.iter().map(|b| *b.key()).collect();
            bucket_ids.sort();

            for bucket_id in bucket_ids {
                if let Some(bucket) = table.buckets.get(&bucket_id)
                    && bucket_overlaps_range(&bucket, &ts_range)
                {
                    let snapshot: Vec<RecordBatch> = bucket.batches.lock().iter().cloned().collect();
                    if snapshot.is_empty() {
                        continue;
                    }
                    let out: Vec<RecordBatch> = match &pred {
                        Some(p) => snapshot.iter().map(|b| apply_predicate(b, p)).filter(|b| b.num_rows() > 0).collect(),
                        None => snapshot,
                    };
                    if !out.is_empty() {
                        partitions.push(out);
                    }
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
    /// Time ranges (start, end_exclusive) of every bucket currently held in
    /// MemBuffer for this project/table, sorted ascending by start. Used by
    /// the query path to exclude exactly those ranges from the Delta scan,
    /// so a stuck/un-flushed old bucket no longer hides Delta data above it.
    /// Returns an empty Vec if the table is absent.
    pub fn get_bucket_ranges(&self, project_id: &str, table_name: &str) -> Vec<(i64, i64)> {
        let Some(table) = self.get_table(project_id, table_name) else {
            return Vec::new();
        };
        let dur = bucket_duration_micros();
        let mut ranges: Vec<(i64, i64)> = table.buckets.iter().map(|b| {
            let id = *b.key();
            (id * dur, (id + 1) * dur)
        }).collect();
        ranges.sort_by_key(|(s, _)| *s);
        ranges
    }

    pub fn get_time_range(&self, project_id: &str, table_name: &str) -> Option<(i64, i64)> {
        let oldest = self.get_oldest_timestamp(project_id, table_name)?;
        let newest = self.get_newest_timestamp(project_id, table_name)?;
        if oldest == i64::MAX || newest == i64::MIN { None } else { Some((oldest, newest)) }
    }

    pub fn get_oldest_timestamp(&self, project_id: &str, table_name: &str) -> Option<i64> {
        self.get_table(project_id, table_name).map(|table| {
            table
                .buckets
                .iter()
                .map(|b| b.min_timestamp.load(Ordering::Relaxed))
                .filter(|&ts| ts != i64::MAX)
                .min()
                .unwrap_or(i64::MAX)
        })
    }

    pub fn get_newest_timestamp(&self, project_id: &str, table_name: &str) -> Option<i64> {
        self.get_table(project_id, table_name).map(|table| {
            table
                .buckets
                .iter()
                .map(|b| b.max_timestamp.load(Ordering::Relaxed))
                .filter(|&ts| ts != i64::MIN)
                .max()
                .unwrap_or(i64::MIN)
        })
    }

    #[instrument(skip(self), fields(project_id, table_name, bucket_id))]
    pub fn drain_bucket(&self, project_id: &str, table_name: &str, bucket_id: i64) -> Option<Vec<RecordBatch>> {
        let key = Self::make_key(project_id, table_name);
        if let Some(table) = self.tables.get(&key).map(|e| e.value().clone())
            && let Some((_, bucket)) = table.buckets.remove(&bucket_id)
        {
            let freed_bytes = bucket.memory_bytes.load(Ordering::Relaxed);
            self.estimated_bytes.fetch_sub(freed_bytes, Ordering::Relaxed);
            let batches = bucket.batches.into_inner();
            debug!(
                "MemBuffer drain: project={}, table={}, bucket={}, batches={}, freed_bytes={}",
                project_id,
                table_name,
                bucket_id,
                batches.len(),
                freed_bytes
            );
            drop(table);
            self.try_drop_empty_table(&key);
            return Some(batches);
        }
        None
    }

    /// Race-safe removal of an empty TableBuffer. `remove_if` holds the shard
    /// write lock; the strong_count check skips eviction whenever a
    /// writer/reader is mid-operation on this table.
    fn try_drop_empty_table(&self, key: &TableKey) -> bool {
        self.tables.remove_if(key, |_, v| v.buckets.is_empty() && Arc::strong_count(v) == 1).is_some()
    }

    pub fn get_flushable_buckets(&self, cutoff_bucket_id: i64) -> Vec<FlushableBucket> {
        let flushable = self.collect_buckets(|bucket_id| bucket_id < cutoff_bucket_id);
        debug!("MemBuffer flushable buckets: count={}, cutoff={}", flushable.len(), cutoff_bucket_id);
        flushable
    }

    pub fn get_all_buckets(&self) -> Vec<FlushableBucket> {
        self.collect_buckets(|_| true)
    }

    fn collect_buckets(&self, filter: impl Fn(i64) -> bool) -> Vec<FlushableBucket> {
        let mut result = Vec::new();
        for table_entry in self.tables.iter() {
            let (project_id, table_name) = table_entry.key();
            let table = table_entry.value();
            for bucket in table.buckets.iter() {
                let bucket_id = *bucket.key();
                if !filter(bucket_id) {
                    continue;
                }
                // Snapshot under the lock with Arc-bumps only — no deep copy.
                // Parquet writer downstream regroups rows into row groups
                // regardless of input batch boundaries, so pre-compaction is
                // unnecessary and would temporarily double bucket memory.
                let batches: Vec<RecordBatch> = bucket.batches.lock().iter().cloned().collect();
                if batches.is_empty() {
                    continue;
                }
                result.push(FlushableBucket {
                    project_id: project_id.to_string(),
                    table_name: table_name.to_string(),
                    bucket_id,
                    batches,
                    row_count: bucket.row_count.load(Ordering::Relaxed),
                });
            }
        }
        result
    }

    /// Count buckets whose `max_timestamp` is older than `cutoff_micros`.
    /// Used by the eviction task to surface buckets that have aged past
    /// retention without being flushed (which means flushes are stuck).
    pub fn count_buckets_with_max_ts_before(&self, cutoff_micros: i64) -> usize {
        let mut n = 0usize;
        for t in self.tables.iter() {
            for b in t.value().buckets.iter() {
                if b.value().max_timestamp.load(Ordering::Relaxed) < cutoff_micros {
                    n += 1;
                }
            }
        }
        n
    }

    #[instrument(skip(self))]
    pub fn evict_old_data(&self, cutoff_timestamp_micros: i64) -> usize {
        let cutoff_bucket_id = Self::compute_bucket_id(cutoff_timestamp_micros);
        let mut evicted_count = 0;
        let mut freed_bytes = 0usize;
        let mut empty_table_keys: Vec<TableKey> = Vec::new();

        for table_entry in self.tables.iter() {
            let table = table_entry.value();
            let bucket_ids_to_remove: Vec<i64> = table.buckets.iter().filter(|b| *b.key() < cutoff_bucket_id).map(|b| *b.key()).collect();

            for bucket_id in bucket_ids_to_remove {
                if let Some((_, bucket)) = table.buckets.remove(&bucket_id) {
                    freed_bytes += bucket.memory_bytes.load(Ordering::Relaxed);
                    evicted_count += 1;
                }
            }
            if table.buckets.is_empty() {
                empty_table_keys.push(table_entry.key().clone());
            }
        }

        // Drop empty TableBuffer entries so per-table metadata (schema Arc,
        // project/table name Arcs, DashMap shards) is reclaimed at scale.
        // `get_or_create_table` recreates a fresh entry on the next write.
        let mut tables_dropped = 0usize;
        for key in empty_table_keys {
            if self.try_drop_empty_table(&key) {
                tables_dropped += 1;
            }
        }

        if freed_bytes > 0 {
            self.estimated_bytes.fetch_sub(freed_bytes, Ordering::Relaxed);
        }

        if evicted_count > 0 || tables_dropped > 0 {
            debug!(
                "MemBuffer evicted {} buckets older than bucket_id={}, dropped {} empty tables, freed {} bytes",
                evicted_count, cutoff_bucket_id, tables_dropped, freed_bytes
            );
        }
        evicted_count
    }

    /// Check if a table exists in the buffer
    pub fn has_table(&self, project_id: &str, table_name: &str) -> bool {
        let key = Self::make_key(project_id, table_name);
        self.tables.contains_key(&key)
    }

    /// Delete rows matching the predicate from the buffer.
    /// Returns the number of rows deleted.
    #[instrument(skip(self, predicate), fields(project_id, table_name, rows_deleted))]
    pub fn delete(&self, project_id: &str, table_name: &str, predicate: Option<&Expr>) -> DFResult<u64> {
        let Some(table) = self.get_table(project_id, table_name) else {
            return Ok(0);
        };

        let schema = table.schema();
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
        let props = ExecutionProps::new();

        let physical_predicate = predicate.map(|p| create_physical_expr(p, &df_schema, &props)).transpose()?;

        let mut total_deleted = 0u64;
        let mut total_freed = 0usize;

        for mut bucket_entry in table.buckets.iter_mut() {
            let bucket = bucket_entry.value_mut();
            let mut batches = bucket.batches.lock();

            let mut new_batches = Vec::with_capacity(batches.len());
            let mut bucket_freed = 0usize;
            let mut bucket_rows_removed = 0usize;
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
                bucket_rows_removed += deleted;

                if filtered_batch.num_rows() > 0 {
                    let new_size = estimate_batch_size(&filtered_batch);
                    bucket_freed += original_size.saturating_sub(new_size);
                    new_batches.push(filtered_batch);
                } else {
                    bucket_freed += original_size;
                }
            }

            *batches = new_batches;
            if bucket_rows_removed > 0 {
                bucket.row_count.fetch_sub(bucket_rows_removed, Ordering::Relaxed);
            }
            if bucket_freed > 0 {
                bucket.memory_bytes.fetch_sub(bucket_freed, Ordering::Relaxed);
            }
            total_deleted += bucket_rows_removed as u64;
            total_freed += bucket_freed;
        }

        if total_freed > 0 {
            self.estimated_bytes.fetch_sub(total_freed, Ordering::Relaxed);
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

        let Some(table) = self.get_table(project_id, table_name) else {
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
        let mut total_delta: i64 = 0;

        for mut bucket_entry in table.buckets.iter_mut() {
            let bucket = bucket_entry.value_mut();
            let mut batches = bucket.batches.lock();

            // Track delta only for batches actually rebuilt — unchanged batches
            // contribute 0 to the delta and don't need re-estimation.
            let mut bucket_delta: i64 = 0;
            let new_batches: Vec<RecordBatch> = batches
                .drain(..)
                .map(|batch| {
                    let num_rows = batch.num_rows();
                    if num_rows == 0 {
                        return Ok(batch);
                    }

                    let mask = if let Some(ref phys_pred) = physical_predicate {
                        let result = phys_pred.evaluate(&batch)?;
                        let arr = result.into_array(num_rows)?;
                        arr.as_any()
                            .downcast_ref::<BooleanArray>()
                            .cloned()
                            .ok_or_else(|| datafusion::error::DataFusionError::Execution("Predicate did not return boolean".into()))?
                    } else {
                        BooleanArray::from(vec![true; num_rows])
                    };

                    let matching_count = mask.iter().filter(|v| v == &Some(true)).count();
                    if matching_count == 0 {
                        return Ok(batch);
                    }
                    total_updated += matching_count as u64;

                    let old_size = estimate_batch_size(&batch);
                    let new_columns: Vec<ArrayRef> = (0..batch.num_columns())
                        .map(|col_idx| {
                            if let Some((_, phys_expr)) = physical_assignments.iter().find(|(idx, _)| *idx == col_idx) {
                                let new_values = phys_expr.evaluate(&batch)?.into_array(num_rows)?;
                                merge_arrays(batch.column(col_idx), &new_values, &mask)
                            } else {
                                Ok(batch.column(col_idx).clone())
                            }
                        })
                        .collect::<DFResult<Vec<_>>>()?;

                    let new_batch = RecordBatch::try_new(batch.schema(), new_columns)
                        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
                    bucket_delta += estimate_batch_size(&new_batch) as i64 - old_size as i64;
                    Ok(new_batch)
                })
                .collect::<DFResult<Vec<_>>>()?;

            *batches = new_batches;
            apply_signed_delta(&bucket.memory_bytes, bucket_delta);
            total_delta += bucket_delta;
        }

        apply_signed_delta(&self.estimated_bytes, total_delta);

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
        let mut project_ids = std::collections::HashSet::new();
        let mut oldest: Option<i64> = None;

        for table_entry in self.tables.iter() {
            let (project_id, _) = table_entry.key();
            project_ids.insert(project_id.clone());

            let table = table_entry.value();
            total_buckets += table.buckets.len();
            for bucket in table.buckets.iter() {
                total_rows += bucket.row_count.load(Ordering::Relaxed);
                total_batches += bucket.batches.lock().len();
                let ts = bucket.min_timestamp.load(Ordering::Relaxed);
                if ts != i64::MAX {
                    oldest = Some(oldest.map_or(ts, |o| o.min(ts)));
                }
            }
        }
        MemBufferStats {
            project_count: project_ids.len(),
            total_buckets,
            total_rows,
            total_batches,
            estimated_memory_bytes: self.estimated_bytes.load(Ordering::Relaxed),
            oldest_bucket_micros: oldest,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    pub fn clear(&self) {
        self.tables.clear();
        self.estimated_bytes.store(0, Ordering::Relaxed);
        debug!("MemBuffer cleared");
    }
}

impl Default for MemBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl TableBuffer {
    fn new(schema: SchemaRef, project_id: Arc<str>, table_name: Arc<str>) -> Self {
        Self {
            buckets: DashMap::new(),
            schema,
            project_id,
            table_name,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone() // Arc clone is cheap
    }

    /// Insert a batch into this table's appropriate time bucket.
    /// Returns the batch size in bytes for memory tracking.
    pub fn insert_batch(&self, batch: RecordBatch, timestamp_micros: i64) -> anyhow::Result<usize> {
        let bucket_id = MemBuffer::compute_bucket_id(timestamp_micros);
        let row_count = batch.num_rows();
        let batch_size = estimate_batch_size(&batch);

        let bucket = self.buckets.entry(bucket_id).or_insert_with(TimeBucket::new);

        bucket.batches.lock().push(batch);

        bucket.row_count.fetch_add(row_count, Ordering::Relaxed);
        bucket.memory_bytes.fetch_add(batch_size, Ordering::Relaxed);
        bucket.update_timestamps(timestamp_micros);

        debug!(
            "TableBuffer insert: project={}, table={}, bucket={}, rows={}, bytes={}",
            self.project_id, self.table_name, bucket_id, row_count, batch_size
        );
        Ok(batch_size)
    }
}

impl TimeBucket {
    fn new() -> Self {
        Self {
            batches: Mutex::new(Vec::new()),
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
    use arrow::array::{Int64Array, StringViewArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn create_test_batch(timestamp_micros: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, false),
        ]));
        let ts_array = TimestampMicrosecondArray::from(vec![timestamp_micros]).with_timezone("UTC");
        let id_array = Int64Array::from(vec![1]);
        let name_array = StringViewArray::from(vec!["test"]);
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
            Field::new("name", DataType::Utf8View, false),
        ]));
        let ts_array = TimestampMicrosecondArray::from(vec![ts; ids.len()]).with_timezone("UTC");
        let id_array = Int64Array::from(ids);
        let name_array = StringViewArray::from(names);
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

        let name_col = batch.column(2).as_any().downcast_ref::<StringViewArray>().unwrap();
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
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10, "All 10 inserts should succeed");
    }

    #[test]
    fn test_flushable_buckets_carry_all_batches() {
        // We no longer pre-compact at flush time — the parquet writer downstream
        // regroups rows into row groups itself, and pre-compacting forces an
        // unnecessary deep copy of the entire bucket.
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();

        let total_rows = 10;
        for i in 0..total_rows {
            let batch = create_multi_row_batch(vec![i as i64], vec!["test"]);
            buffer.insert("project1", "table1", batch, ts).unwrap();
        }

        let cutoff = MemBuffer::compute_bucket_id(ts) + 1;
        let flushable = buffer.get_flushable_buckets(cutoff);
        assert_eq!(flushable.len(), 1);
        assert_eq!(flushable[0].batches.len(), total_rows);
        assert_eq!(flushable[0].row_count, total_rows);
        let summed: usize = flushable[0].batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(summed, total_rows);
    }

    #[test]
    fn test_point_lookup_fast_path_filters_inline() {
        use datafusion::logical_expr::{col, lit};

        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        // 10 rows in a single bucket — point lookup should return only the matching one.
        let batch = create_multi_row_batch(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10], vec!["a"; 10]);
        buffer.insert("project1", "table1", batch, ts).unwrap();

        // Non-point query: returns the whole bucket (downstream FilterExec narrows it).
        let no_id_filter = buffer.query("project1", "table1", &[]).unwrap();
        let total_rows: usize = no_id_filter.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10);

        // Point lookup by id: MemBuffer applies filter inline, returns 1 row.
        let id_pred = col("id").eq(lit(5i64));
        let point = buffer.query("project1", "table1", &[id_pred]).unwrap();
        let total_rows: usize = point.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "point lookup should return exactly the matching row");

        // query_partitioned must also apply the filter inline.
        let id_pred2 = col("id").eq(lit(7i64));
        let parts = buffer.query_partitioned("project1", "table1", &[id_pred2]).unwrap();
        let total_rows: usize = parts.iter().flatten().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[test]
    fn test_negative_bucket_ids_pre_1970() {
        // Integer division truncates toward zero: -1 / N = 0, -N / N = -1
        assert_eq!(MemBuffer::compute_bucket_id(-1), 0); // Just before epoch -> bucket 0
        assert_eq!(MemBuffer::compute_bucket_id(-BUCKET_DURATION_MICROS), -1);
        assert_eq!(MemBuffer::compute_bucket_id(-BUCKET_DURATION_MICROS - 1), -1);
        assert_eq!(MemBuffer::compute_bucket_id(-BUCKET_DURATION_MICROS * 2), -2);

        let buffer = MemBuffer::new();
        let pre_1970_ts = -BUCKET_DURATION_MICROS * 2; // 20 minutes before epoch

        buffer.insert("project1", "table1", create_test_batch(pre_1970_ts), pre_1970_ts).unwrap();

        let results = buffer.query("project1", "table1", &[]).unwrap();
        assert_eq!(results.len(), 1);

        let bucket_id = MemBuffer::compute_bucket_id(pre_1970_ts);
        assert_eq!(bucket_id, -2, "20 minutes before epoch should be bucket -2");
    }
}
