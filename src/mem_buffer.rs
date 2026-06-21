use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicUsize, Ordering},
};

use arrow::{
    array::{Array, ArrayRef, BooleanArray, RecordBatch, TimestampMicrosecondArray, UInt32Array},
    compute::{concat, filter_record_batch},
    datatypes::{DataType, SchemaRef, TimeUnit},
    row::{OwnedRow, RowConverter, SortField},
};
use dashmap::DashMap;
use datafusion::{
    common::{Column, DFSchema, tree_node::TreeNode},
    error::Result as DFResult,
    logical_expr::Expr,
    physical_expr::{create_physical_expr, execution_props::ExecutionProps},
    sql::{
        planner::SqlToRel,
        sqlparser::{dialect::GenericDialect, parser::Parser as SqlParser},
    },
};
use parking_lot::Mutex;
use tracing::{debug, info, instrument, warn};

use crate::{errors::arrow_err, functions::FnRegistry};

// 10-minute buckets balance flush granularity vs overhead. Shorter = more flushes,
// longer = larger Delta files. Matches default flush interval for aligned boundaries.
// Note: Timestamps before 1970 (negative microseconds) produce negative bucket IDs,
// which is supported but may result in unexpected ordering if mixed with post-1970 data.
// Fallback when `set_bucket_duration_micros` is never called (i.e. unit tests
// that build a MemBuffer directly). MUST track `d_bucket_duration_secs` in
// config.rs — prod always overrides via bootstrap, but keeping the two in sync
// avoids the test-only `BUCKET_DURATION_MICROS` const diverging from the
// process-global runtime value once any test pins the OnceLock.
const DEFAULT_BUCKET_DURATION_MICROS: i64 = 5 * 60 * 1_000_000;
#[cfg(test)]
const BUCKET_DURATION_MICROS: i64 = DEFAULT_BUCKET_DURATION_MICROS;

static BUCKET_DURATION_MICROS_CFG: std::sync::OnceLock<i64> = std::sync::OnceLock::new();

/// Hard cap on RecordBatch count per TimeBucket. Insert just pushes; when
/// the bucket crosses this threshold, one insert pays an amortized coalesce
/// (all batches → one). 8 is the sweet spot at prod scale: lower means more
/// concat work per insert (but each concat is cheap, since batches are
/// small), higher means each read scans more RecordBatches with per-batch
/// Arrow overhead. Empirically, dropping from 32→8 cut p95 at 200-project
/// load from 240ms to ~80ms.
const MAX_BATCH_COUNT_PER_BUCKET: usize = 8;
/// Skip the in-lock coalesce when the bucket's combined payload exceeds
/// this many bytes. The point of coalesce is to bound query-side
/// per-bucket batch fanout for sub-ms reads on bursty small-INSERT
/// workloads — once a bucket already holds a multi-megabyte payload the
/// per-query iteration overhead is already dwarfed by the data work, and
/// `concat_batches` on tens of MB would hold the bucket lock for
/// milliseconds, starving every concurrent reader of that bucket. 4 MB
/// matches one Arrow IPC default block; arrived at empirically — see
/// `tests/membuffer_concurrency_bench.rs`.
const MAX_BATCH_BYTES_FOR_COALESCE: usize = 4 * 1024 * 1024;

/// Configured bucket window in microseconds. Set once at startup via
/// `set_bucket_duration_micros`; defaults to 5 minutes when unset. Smaller
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
    tables:               DashMap<TableKey, Arc<TableBuffer>>,
    /// Running approximation of in-memory bytes across all live buckets.
    /// Reported via `timefusion_stats` as `mem_buffer.estimated_bytes_approx`.
    ///
    /// Accounting is intentionally cheap, not exact:
    /// - `+= new_size` on every insert (pre-coalesce batch size).
    /// - On coalesce, the bucket's `memory_bytes` field is overwritten to
    ///   the post-concat size, but this MemBuffer-level total isn't
    ///   decremented — so the running sum stays high until the bucket
    ///   drains.
    /// - `-= freed_bytes` on `drain_bucket` and eviction (sees the
    ///   post-coalesce bucket size, fully reconciling that bucket's
    ///   contribution).
    ///
    /// **Maximum drift bound**: at any instant, the over-reporting is at
    /// most the sum of `(pre_coalesce_size - post_coalesce_size)` across
    /// buckets that have coalesced since their last drain. The bucket-
    /// level field is exact; only the MemBuffer-level sum drifts. With
    /// 10-minute buckets and pressure-driven flushes, this converges
    /// every retention window (single-digit minutes).
    ///
    /// **Why not exact**: making this exact requires holding a lock that
    /// spans bucket coalesce + counter update for every insert. The hot
    /// path is currently lock-free (per-bucket atomic) and the drift is
    /// bounded, monotone, and self-correcting on drain.
    ///
    /// **Operator caution**: do NOT use this counter for back-pressure
    /// decisions where a false-high reading would cause incorrect
    /// throttling. The `pressure_pct` reported on `buffered_layer` is
    /// what the flush task and the memory-reservation CAS actually use.
    estimated_bytes:      AtomicUsize,
    /// Mirrors `WalManager::shards_per_topic` so `FlushableBucket.wal_shard_counts`
    /// is always sized correctly when snapshotted at seal time.
    shards_per_topic:     usize,
    /// LRU cache of per-bucket tantivy indexes. Lives at the MemBuffer
    /// level (not on individual TimeBuckets) so the LRU has a global view
    /// for byte-budget eviction. Entries are dropped:
    /// - when `text_index_max_bytes` is exceeded (LRU-evict tail)
    /// - when the bucket receives an insert (cache_invalidate by key)
    /// - when the bucket drains/evicts (cache_invalidate by key)
    text_index_cache:     parking_lot::Mutex<lru::LruCache<BucketCacheKey, Arc<crate::tantivy_index::mem_index::BucketTextIndex>>>,
    /// Sum of `size_bytes` across cached entries. Kept in an atomic so the
    /// hot insert path can do a single load to check "over budget?" without
    /// taking the LRU mutex.
    text_index_bytes:     AtomicUsize,
    /// Soft budget for cached text indexes (bytes). When exceeded, LRU
    /// evictions drop oldest cached buckets until under. Auto-tuned from
    /// `buffer_max_memory_mb` at MemBuffer construction.
    text_index_max_bytes: usize,
    /// (project_id, table_name) → bucket_ids whose rows were force-flushed
    /// to Delta while the bucket was open. Such a bucket's window holds rows
    /// legitimately in *both* stores (disjoint sets — force-flush removes
    /// rows from MemBuffer before committing), so it must stay exempt from
    /// the Delta-scan exclusion for its whole lifetime, not just while
    /// current (2026-06-11: the sealed exclusion + a 2h flush backlog masked
    /// force-flushed Delta rows for 2h). Kept at MemBuffer level — not on
    /// TableBuffer/TimeBucket — so the mark survives empty-bucket reclaim in
    /// `take_bucket_for_flush` and bucket/table re-creation by later inserts.
    /// Pruned on drain and eviction.
    force_flushed:        DashMap<TableKey, std::collections::HashSet<i64>>,
}

/// Cache key: (project_id, table_name, bucket_id). All three are cheap to
/// clone (Arc<str> + i64) so the key lives both in the LRU and in the
/// invalidation calls.
pub type BucketCacheKey = (Arc<str>, Arc<str>, i64);

pub struct TableBuffer {
    buckets:    DashMap<i64, TimeBucket>,
    schema:     SchemaRef, // Immutable after creation - no lock needed
    project_id: Arc<str>,
    table_name: Arc<str>,
}

pub struct TimeBucket {
    batches:         Mutex<Vec<RecordBatch>>,
    row_count:       AtomicUsize,
    memory_bytes:    AtomicUsize,
    min_timestamp:   AtomicI64,
    max_timestamp:   AtomicI64,
    /// Per-shard WAL-entry counts (drive `advance_by_counts` on flush) and
    /// post-append walrus positions (written to Delta commit metadata for
    /// crash-mid-flush recovery). One mutex so a single append updates both
    /// atomically — a snapshot can't see counts ahead of positions.
    wal_shard_state: Mutex<WalShardState>,
}

#[derive(Debug, Default, Clone)]
struct WalShardState {
    counts:    Vec<u64>,
    positions: Vec<Option<walrus_rust::WalPosition>>,
}

#[derive(Debug, Clone)]
pub struct FlushableBucket {
    pub project_id:       String,
    pub table_name:       String,
    pub bucket_id:        i64,
    pub batches:          Vec<RecordBatch>,
    pub row_count:        usize,
    /// Drives `Wal::advance_by_counts` after a successful flush.
    pub wal_shard_counts: Vec<u64>,
    /// Written into Delta commit metadata so a crash between Delta commit
    /// and `advance_by_counts` can recover the cursor from Delta on restart.
    pub wal_positions:    Vec<Option<walrus_rust::WalPosition>>,
    /// Actual min/max timestamp of the taken rows, captured before the source
    /// bucket's atomics were reset. `restore_taken_bucket` replays these so a
    /// restored bucket keeps its true time range (and stays visible to
    /// time-range pruning) rather than collapsing to the bucket's start.
    pub min_timestamp:    i64,
    pub max_timestamp:    i64,
}

#[derive(Debug, Default)]
pub struct MemBufferStats {
    pub project_count:          usize,
    pub total_buckets:          usize,
    pub total_rows:             usize,
    pub total_batches:          usize,
    pub estimated_memory_bytes: usize,
    /// Min `min_timestamp` across all buckets in microseconds, or None if empty.
    /// Used to derive `mem_buffer_oldest_bucket_age_seconds` for the metrics
    /// exporter — a key staleness signal (alert if > 2× flush interval).
    pub oldest_bucket_micros:   Option<i64>,
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

/// Compact string/binary-view arrays whose buffers dwarf the bytes the views
/// actually reference. Two prod failure modes (both 2026-06-11):
///
/// - Builder slack: `ScalarValue::to_array_of_size` (pgwire fast-insert, WAL
///   replay of those rows) allocates a ~16KB view block per Utf8View column
///   even for a <32B string — ~1MB of reported capacity per single-row batch
///   on the otel schema (1.5TB tracked inside a 66.6GiB cgroup).
/// - Inherited scan blocks: rows read back by the DML UPDATE path arrive as
///   view arrays referencing the parquet reader's full column-chunk data
///   blocks (`capacity == len`, so slack detection can't see it) — ~250-row
///   UPDATE batches charged ~135MB each (29.9GB for 54k rows).
///
/// Hence the gate compares buffer capacity against `total_buffer_bytes_used`
/// (bytes the views reference), which catches both. `gc()` rewrites the
/// views into exact-size buffers; compact arrays pass through as Arc clones.
/// Recurses into List/LargeList/FixedSizeList/Struct children.
fn compact_view_arrays(arr: &ArrayRef) -> ArrayRef {
    use arrow::array::{Array, BinaryViewArray, FixedSizeListArray, LargeListArray, ListArray, StringViewArray, StructArray};
    fn wasteful(buffers: &[arrow::buffer::Buffer], used: usize) -> bool {
        buffers.iter().map(|b| b.capacity()).sum::<usize>() > used * 2 + 1024
    }
    match arr.data_type() {
        DataType::Utf8View => {
            let v = arr.as_any().downcast_ref::<StringViewArray>().unwrap();
            if wasteful(v.data_buffers(), v.total_buffer_bytes_used()) { Arc::new(v.gc()) } else { arr.clone() }
        }
        DataType::BinaryView => {
            let v = arr.as_any().downcast_ref::<BinaryViewArray>().unwrap();
            if wasteful(v.data_buffers(), v.total_buffer_bytes_used()) { Arc::new(v.gc()) } else { arr.clone() }
        }
        DataType::List(f) => {
            let l = arr.as_any().downcast_ref::<ListArray>().unwrap();
            let vals = compact_view_arrays(l.values());
            if Arc::ptr_eq(&vals, l.values()) {
                arr.clone()
            } else {
                Arc::new(ListArray::new(f.clone(), l.offsets().clone(), vals, l.nulls().cloned()))
            }
        }
        DataType::LargeList(f) => {
            let l = arr.as_any().downcast_ref::<LargeListArray>().unwrap();
            let vals = compact_view_arrays(l.values());
            if Arc::ptr_eq(&vals, l.values()) {
                arr.clone()
            } else {
                Arc::new(LargeListArray::new(f.clone(), l.offsets().clone(), vals, l.nulls().cloned()))
            }
        }
        DataType::FixedSizeList(f, size) => {
            let l = arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let vals = compact_view_arrays(l.values());
            if Arc::ptr_eq(&vals, l.values()) {
                arr.clone()
            } else {
                Arc::new(FixedSizeListArray::new(f.clone(), *size, vals, l.nulls().cloned()))
            }
        }
        DataType::Struct(fields) => {
            let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
            let cols: Vec<ArrayRef> = s.columns().iter().map(compact_view_arrays).collect();
            if cols.iter().zip(s.columns()).all(|(a, b)| Arc::ptr_eq(a, b)) {
                arr.clone()
            } else {
                Arc::new(StructArray::new(fields.clone(), cols, s.nulls().cloned()))
            }
        }
        _ => arr.clone(),
    }
}

/// Copy an array into freshly-allocated exact-size buffers when its current
/// buffers are mostly someone else's bytes. Arrow IPC decode (WAL replay,
/// gRPC ingest) reads the whole message body into one allocation and hands
/// every column a slice of it — `Buffer::capacity()` reports the full body,
/// so a replayed batch is charged ~n_cols × message size (prod 2026-06-11
/// night: 6,546 replayed entries charged 772.5GB inside a 66.6GiB cgroup).
/// The cap-vs-len gate sees exactly that: a slice's `len` is its own region,
/// `capacity` is the underlying allocation. `MutableArrayData` rebases
/// offsets and copies only the referenced region, recursing into children.
fn privatize_sliced(arr: &ArrayRef) -> ArrayRef {
    use arrow::array::{ArrayData, MutableArrayData, make_array};
    fn waste(data: &ArrayData) -> (usize, usize) {
        let (cap, len) = data.buffers().iter().fold((0usize, 0usize), |(c, l), b| (c + b.capacity(), l + b.len()));
        data.child_data().iter().map(waste).fold((cap, len), |(c, l), (cc, cl)| (c + cc, l + cl))
    }
    let data = arr.to_data();
    let (cap, len) = waste(&data);
    if cap <= len * 2 + 1024 {
        return arr.clone();
    }
    let mut m = MutableArrayData::new(vec![&data], false, data.len());
    m.extend(0, 0, data.len());
    make_array(m.freeze())
}

/// Full charge-honesty pass: view gc first (block slack / scan-block
/// inheritance), then buffer privatization (IPC message-body slices). Runs
/// at every bucket insert and before WAL serialization so neither memory
/// accounting nor WAL entries carry other allocations' bytes.
pub(crate) fn compact_batch(batch: RecordBatch) -> RecordBatch {
    let cols: Vec<ArrayRef> = batch.columns().iter().map(|c| privatize_sliced(&compact_view_arrays(c))).collect();
    if cols.iter().zip(batch.columns()).all(|(a, b)| Arc::ptr_eq(a, b)) {
        batch
    } else {
        RecordBatch::try_new(batch.schema(), cols).unwrap_or(batch)
    }
}

/// Collapse rows in `batches` to one row per unique value of `keys`, keep-last.
/// Empty `keys` or empty input → no-op. Surviving row order is preserved.
/// Tiebreaker on identical key tuples: last occurrence in insertion order wins.
/// Only collapses dupes inside this call's input — cross-bucket dupes need
/// the read-side row_number() rewrite.
pub fn dedup_batches(batches: Vec<RecordBatch>, keys: &[String]) -> anyhow::Result<Vec<RecordBatch>> {
    if keys.is_empty() || batches.is_empty() {
        return Ok(batches);
    }
    // Dedup across all batches by concatenating ONLY the key columns — never the
    // full batches. A large coalesced otel flush (fat body/attributes/resource
    // strings across many buckets) exceeds Arrow's 2GB i32 string-offset limit,
    // so `concat_batches` on the whole payload returns "Offset overflow" and
    // fails the entire flush; the buckets then never drain, MemBuffer wedges at
    // the hard limit, and every insert is rejected (prod 2026-06-20, project
    // …88a1a8). Key columns (e.g. `id`) are tiny, so concatenating just those is
    // safe; superseded rows are then dropped by filtering each batch in place,
    // keeping every output array bounded by its source batch.
    let key_arrays: Vec<ArrayRef> = keys
        .iter()
        .map(|k| {
            let cols = batches
                .iter()
                .map(|b| b.column_by_name(k).cloned().ok_or_else(|| anyhow::anyhow!("dedup key `{k}` missing from batch schema")))
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(concat(&cols.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?)
        })
        .collect::<anyhow::Result<_>>()?;
    let converter = RowConverter::new(key_arrays.iter().map(|a| SortField::new(a.data_type().clone())).collect())?;
    let rows = converter.convert_columns(&key_arrays)?;
    // Last occurrence of each key wins (keep-last across coalesced time windows).
    let mut last: std::collections::HashMap<OwnedRow, u32> = std::collections::HashMap::with_capacity(rows.num_rows());
    for i in 0..rows.num_rows() {
        last.insert(rows.row(i).owned(), i as u32);
    }
    if last.len() == rows.num_rows() {
        // No duplicates — return the batches untouched (never fused into one array).
        return Ok(batches);
    }
    let keep: std::collections::HashSet<u32> = last.into_values().collect();
    // Filter each batch against its slice of the global row-index space.
    let mut out = Vec::with_capacity(batches.len());
    let mut base = 0u32;
    for b in &batches {
        let n = b.num_rows() as u32;
        let mask = BooleanArray::from((0..n).map(|r| keep.contains(&(base + r))).collect::<Vec<bool>>());
        base += n;
        let kept = filter_record_batch(b, &mask)?;
        if kept.num_rows() > 0 {
            out.push(kept);
        }
    }
    Ok(out)
}

/// Merge two arrays based on a boolean mask.
/// For each row: if mask[i] is true, use new_values[i], else use original[i].
fn merge_arrays(original: &ArrayRef, new_values: &ArrayRef, mask: &BooleanArray) -> DFResult<ArrayRef> {
    // Cast new_values to match original's type if they differ (e.g., Utf8 -> Utf8View)
    let new_values = if original.data_type() != new_values.data_type() {
        arrow::compute::cast(new_values, original.data_type()).map_err(arrow_err)?
    } else {
        new_values.clone()
    };
    arrow::compute::kernels::zip::zip(mask, &new_values, original).map_err(arrow_err)
}

/// Parse a SQL fragment into a DataFusion Expr. `schema` resolves column refs
/// (column refs nested inside function args need a non-empty schema).
/// `registry` resolves UDFs — required if the SQL has any function call.
fn parse_sql_predicate(sql: &str, schema: &DFSchema, registry: Option<&FnRegistry>) -> DFResult<Expr> {
    let dialect = GenericDialect {};
    let sql_expr = SqlParser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|e| datafusion::error::DataFusionError::SQL(e.into(), None))?
        .parse_expr()
        .map_err(|e| datafusion::error::DataFusionError::SQL(e.into(), None))?;
    let context_provider = RegistryContextProvider { registry };
    let planner = SqlToRel::new(&context_provider);
    let expr = planner.sql_to_expr(sql_expr, schema, &mut Default::default())?;
    // DF54's comparison kernels no longer auto-coerce mismatched string views
    // (e.g. `upper(name)` Utf8View vs a Utf8 literal), so apply the same type
    // coercion the analyzer would before this predicate is lowered to a physical expr.
    Ok(expr.rewrite(&mut datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter::new(schema))?.data)
}

struct RegistryContextProvider<'a> {
    registry: Option<&'a FnRegistry>,
}

impl<'a> datafusion::sql::planner::ContextProvider for RegistryContextProvider<'a> {
    fn get_table_source(&self, _: datafusion::sql::TableReference) -> DFResult<std::sync::Arc<dyn datafusion::logical_expr::TableSource>> {
        Err(datafusion::error::DataFusionError::Plan("No table context".into()))
    }
    fn get_function_meta(&self, name: &str) -> Option<std::sync::Arc<datafusion::logical_expr::ScalarUDF>> {
        self.registry?.udf(name).ok()
    }
    fn get_aggregate_meta(&self, name: &str) -> Option<std::sync::Arc<datafusion::logical_expr::AggregateUDF>> {
        self.registry?.udaf(name).ok()
    }
    fn get_window_meta(&self, name: &str) -> Option<std::sync::Arc<datafusion::logical_expr::WindowUDF>> {
        self.registry?.udwf(name).ok()
    }
    fn get_higher_order_meta(&self, _name: &str) -> Option<std::sync::Arc<datafusion::logical_expr::HigherOrderUDF>> {
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
        self.registry.map(|r| r.udfs().into_iter().collect()).unwrap_or_default()
    }
    fn udaf_names(&self) -> Vec<String> {
        self.registry.map(|r| r.udafs().into_iter().collect()).unwrap_or_default()
    }
    fn udwf_names(&self) -> Vec<String> {
        self.registry.map(|r| r.udwfs().into_iter().collect()).unwrap_or_default()
    }
    fn higher_order_function_names(&self) -> Vec<String> {
        Vec::new()
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

/// Filter a batch to rows whose `id` is in `ids`. Returns a fresh batch.
/// On any error (missing `id` column, unexpected type) the batch is
/// returned unfiltered — the caller's predicate-based filter will catch
/// any over-inclusion. Supports Utf8View, Utf8, and LargeUtf8 ID types.
fn filter_batch_by_id_set(batch: &RecordBatch, ids: &std::collections::HashSet<String>) -> RecordBatch {
    use arrow::array::{AsArray, BooleanArray, LargeStringArray, StringArray, StringViewArray};
    let Some(arr) = batch.column_by_name("id") else { return batch.clone() };
    let mask: BooleanArray = if let Some(a) = arr.as_any().downcast_ref::<StringViewArray>() {
        (0..a.len()).map(|i| !a.is_null(i) && ids.contains(a.value(i))).collect()
    } else if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        (0..a.len()).map(|i| !a.is_null(i) && ids.contains(a.value(i))).collect()
    } else if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        (0..a.len()).map(|i| !a.is_null(i) && ids.contains(a.value(i))).collect()
    } else {
        // Unknown id type — let the original predicate handle the filtering.
        let _ = arr.as_string_opt::<i32>(); // keep AsArray import live
        return batch.clone();
    };
    filter_record_batch(batch, &mask).unwrap_or_else(|_| batch.clone())
}

/// Apply a compiled predicate, returning only matching rows. Best-effort: on
/// any evaluation error we return the original batch so DataFusion's FilterExec
/// can finish the job.
fn apply_predicate(batch: &RecordBatch, pred: &Arc<dyn datafusion::physical_expr::PhysicalExpr>) -> RecordBatch {
    let Ok(value) = pred.evaluate(batch) else { return batch.clone() };
    let Ok(arr) = value.into_array(batch.num_rows()) else { return batch.clone() };
    let Some(mask) = arr.as_any().downcast_ref::<BooleanArray>() else {
        return batch.clone();
    };
    filter_record_batch(batch, mask).unwrap_or_else(|_| batch.clone())
}

/// Apply an optional compiled predicate to a bucket snapshot, dropping
/// non-matching rows and any batch that ends up empty. `None` returns the
/// snapshot unchanged.
fn filter_snapshot(snapshot: Vec<RecordBatch>, pred: &Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>) -> Vec<RecordBatch> {
    match pred {
        Some(p) => snapshot.iter().map(|b| apply_predicate(b, p)).filter(|b| b.num_rows() > 0).collect(),
        None => snapshot,
    }
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

/// Strip table qualifiers from Column refs (e.g. `otel_logs_and_spans.timestamp` → `timestamp`)
/// so exprs from SQL planning resolve against the bare-column DFSchema built from the
/// in-memory table.
fn strip_column_qualifiers(expr: Expr) -> DFResult<Expr> {
    expr.transform(|e| match &e {
        Expr::Column(col) => Ok(datafusion::common::tree_node::Transformed::yes(Expr::Column(Column::from_name(&col.name)))),
        _ => Ok(datafusion::common::tree_node::Transformed::no(e)),
    })
    .map(|t| t.data)
}

impl MemBuffer {
    pub fn new() -> Self {
        // Default text-index budget: 128MB. Production code path goes
        // through `new_with_max_index_bytes` from BufferedWriteLayer which
        // sizes this against the configured MemBuffer memory budget.
        Self::new_with_max_index_bytes(128 * 1024 * 1024)
    }

    pub fn new_with_max_index_bytes(text_index_max_bytes: usize) -> Self {
        Self::new_with_max_index_bytes_and_shards(text_index_max_bytes, 4)
    }

    pub fn new_with_max_index_bytes_and_shards(text_index_max_bytes: usize, shards_per_topic: usize) -> Self {
        Self {
            tables: DashMap::new(),
            estimated_bytes: AtomicUsize::new(0),
            shards_per_topic,
            text_index_cache: parking_lot::Mutex::new(lru::LruCache::unbounded()),
            text_index_bytes: AtomicUsize::new(0),
            text_index_max_bytes,
            force_flushed: DashMap::new(),
        }
    }

    /// Record that `bucket_id`'s rows were committed to Delta while the
    /// bucket was still open — see the `force_flushed` field docs. Called
    /// before the commit so no query can race into the masked window; a
    /// failed commit leaves a stale mark, which only costs that bucket the
    /// brief commit-then-drain exclusion at its eventual sealed flush.
    pub fn mark_force_flushed(&self, project_id: &str, table_name: &str, bucket_id: i64) {
        self.force_flushed.entry(Self::make_key(project_id, table_name)).or_default().insert(bucket_id);
    }

    pub fn shards_per_topic(&self) -> usize {
        self.shards_per_topic
    }

    /// Approximate bytes currently held by cached per-bucket text indexes.
    pub fn text_index_bytes(&self) -> usize {
        self.text_index_bytes.load(Ordering::Relaxed)
    }

    /// Configured byte budget for the text-index cache.
    pub fn text_index_max_bytes(&self) -> usize {
        self.text_index_max_bytes
    }

    /// Cache key for a bucket. Builds an Arc<str> per call but only on the
    /// cache-miss path, so the hot lookup is cheap (the bucket_id alone).
    fn cache_key(project_id: &str, table_name: &str, bucket_id: i64) -> BucketCacheKey {
        (Arc::from(project_id), Arc::from(table_name), bucket_id)
    }

    /// Look up a cached text index. Promotes the entry to MRU on hit.
    fn cache_get(&self, key: &BucketCacheKey) -> Option<Arc<crate::tantivy_index::mem_index::BucketTextIndex>> {
        self.text_index_cache.lock().get(key).cloned()
    }

    /// Insert a freshly-built index into the cache, evicting LRU entries
    /// to stay under `text_index_max_bytes`. Returns the inserted Arc.
    fn cache_put(
        &self, key: BucketCacheKey, idx: Arc<crate::tantivy_index::mem_index::BucketTextIndex>,
    ) -> Arc<crate::tantivy_index::mem_index::BucketTextIndex> {
        let size = idx.size_bytes;
        let mut cache = self.text_index_cache.lock();
        // Overwrite any existing entry for this key (stale index from a
        // smaller snapshot). Adjust the byte counter accordingly.
        if let Some(old) = cache.put(key, idx.clone()) {
            self.text_index_bytes.fetch_sub(old.size_bytes, Ordering::Relaxed);
        }
        self.text_index_bytes.fetch_add(size, Ordering::Relaxed);
        // Evict LRU until under budget.
        while self.text_index_bytes.load(Ordering::Relaxed) > self.text_index_max_bytes {
            match cache.pop_lru() {
                Some((_, evicted)) => {
                    self.text_index_bytes.fetch_sub(evicted.size_bytes, Ordering::Relaxed);
                }
                None => break,
            }
        }
        idx
    }

    /// Drop the cached entry for a bucket. Called by `insert_batch` and
    /// `drain_bucket` to keep the cache from going stale.
    fn cache_invalidate(&self, key: &BucketCacheKey) {
        if let Some(old) = self.text_index_cache.lock().pop(key) {
            self.text_index_bytes.fetch_sub(old.size_bytes, Ordering::Relaxed);
        }
    }

    /// Authoritative live MemBuffer size — sums every (table, bucket)'s
    /// `memory_bytes` atomic directly. The internal `estimated_bytes`
    /// AtomicUsize cache drifts because the in-bucket coalesce path
    /// (`TimeBucket::insert_batch`) shrinks `bucket.memory_bytes` via
    /// `.store(combined_size)` without telling MemBuffer about the savings.
    /// Each coalesce leaks `(sum_before − combined_size)` upward, and any
    /// `fetch_sub` underflow on this cache wraps to ~USIZE_MAX. Prod hit
    /// 948 GB reported with a real budget of 44 GB — every `try_reserve_memory`
    /// then rejected forever ("Memory limit exceeded"). Recomputing here is
    /// O(N tables × N buckets), microseconds at prod scale (~30 projects ×
    /// a handful of buckets each), and correct-by-construction.
    pub fn estimated_memory_bytes(&self) -> usize {
        self.tables
            .iter()
            .map(|t| t.value().buckets.iter().map(|b| b.value().memory_bytes.load(Ordering::Relaxed)).sum::<usize>())
            .sum()
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
        let (batch_size, bucket_id) = table.insert_batch(batch, timestamp_micros)?;
        self.estimated_bytes.fetch_add(batch_size, Ordering::Relaxed);
        // Drop any stale text-index cache entry for this bucket — the
        // `indexed_rows == snapshot_rows` check would reject it on the
        // next query anyway, but freeing the bytes now lets the LRU give
        // budget to other buckets immediately.
        self.cache_invalidate(&Self::cache_key(project_id, table_name, bucket_id));
        Ok(())
    }

    /// Record that `count` WAL entries for `(project_id, table_name)` were
    /// appended to walrus `shard`, attributing them to the MemBuffer bucket
    /// covering `timestamp_micros`. Called by the write path *after*
    /// `Wal::append*` returns the chosen shard. Not called during WAL replay
    /// (those entries are *read from* walrus, not appended to it).
    ///
    /// No-op if the bucket doesn't exist — the caller must have already
    /// inserted into the same bucket via `insert` / `insert_batches`, so
    /// missing-bucket here would mean a TOCTOU race we don't currently
    /// expose (insert + record are both synchronous, no await between them
    /// at the call site).
    pub fn record_wal_append(
        &self, project_id: &str, table_name: &str, timestamp_micros: i64, shard: usize, count: u64, position: Option<walrus_rust::WalPosition>,
    ) {
        let key = Self::make_key(project_id, table_name);
        let Some(table) = self.tables.get(&key) else {
            return;
        };
        let bucket_id = Self::compute_bucket_id(timestamp_micros);
        if let Some(bucket) = table.buckets.get(&bucket_id) {
            bucket.record_wal_append(shard, count, position);
        }
    }

    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub fn insert_batches(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>, timestamp_micros: i64) -> anyhow::Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let schema = batches[0].schema();
        let table = self.get_or_create_table(project_id, table_name, &schema)?;

        let mut total_size = 0usize;
        let mut touched_buckets: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for batch in batches {
            let (sz, bucket_id) = table.insert_batch(batch, timestamp_micros)?;
            total_size += sz;
            touched_buckets.insert(bucket_id);
        }
        self.estimated_bytes.fetch_add(total_size, Ordering::Relaxed);
        for bucket_id in touched_buckets {
            self.cache_invalidate(&Self::cache_key(project_id, table_name, bucket_id));
        }
        Ok(())
    }

    /// Search every bucket of `(project_id, table_name)` for rows matching
    /// the given `text_match` predicates. Builds per-bucket tantivy indexes
    /// JIT (cached until row_count changes; dropped on drain/evict).
    ///
    /// Semantics mirror `TantivySearchService::search`:
    /// - `Ok(None)`: table has no indexed fields → caller falls back to
    ///   running the original predicate (which is always present in the
    ///   plan thanks to the rewriter being additive).
    /// - `Ok(Some(ids))`: union of matching IDs across all buckets,
    ///   intersected across multiple predicates (AND semantics).
    pub fn search_text_match(
        &self, project_id: &str, table_name: &str, preds: &[crate::tantivy_index::udf::TextMatchPred],
    ) -> anyhow::Result<Option<std::collections::HashSet<String>>> {
        if preds.is_empty() {
            return Ok(None);
        }
        let Some(table_schema) = crate::schema_loader::get_schema(table_name) else {
            return Ok(None);
        };
        // Skip if the schema has no tantivy-indexed fields — the per-bucket
        // build would just return None per-bucket anyway, but checking once
        // here avoids the per-bucket overhead.
        if !table_schema.fields.iter().any(|f| f.tantivy.as_ref().is_some_and(|t| t.indexed)) {
            return Ok(None);
        }
        let Some(table) = self.get_table(project_id, table_name) else {
            return Ok(None);
        };

        // Walk buckets, taking each bucket's atomic snapshot+ids.
        // NOTE: this returns IDs without the matching snapshot, so the
        // caller MUST NOT use it to filter a separately-fetched snapshot —
        // a concurrent insert could add a row to the bucket between this
        // call and the snapshot, and the new row would be incorrectly
        // dropped. For SQL routing use `query_partitioned_with_text_match`
        // which keeps snapshot+ids atomic per bucket. This method is kept
        // for tests + future read-only consumers (e.g. EXPLAIN).
        let mut acc: Option<std::collections::HashSet<String>> = None;
        let mut any_usable = false;
        for bucket_entry in table.buckets.iter() {
            let bucket_id = *bucket_entry.key();
            let bucket = bucket_entry.value();
            let key = Self::cache_key(project_id, table_name, bucket_id);
            let (_snapshot, ids_opt) = self.search_with_snapshot(bucket, &key, table_schema, preds)?;
            if let Some(ids) = ids_opt {
                any_usable = true;
                acc = Some(match acc.take() {
                    None => ids,
                    Some(mut prev) => {
                        prev.extend(ids);
                        prev
                    }
                });
            }
        }
        if any_usable { Ok(acc) } else { Ok(None) }
    }

    /// Atomic MemBuffer query with text-match prefilter. For each bucket:
    ///   - Snapshot batches + run text_match search → ID set (under the
    ///     same `batches` lock).
    ///   - Apply `id IN (ids)` and the rest of `filters` to the snapshot.
    /// This guarantees the prefilter and the data come from the same point
    /// in time — closing the race where a concurrent insert would otherwise
    /// be visible in the data but absent from the prefilter ID set.
    ///
    /// When `preds` is empty or the table has no indexed fields, behaves
    /// exactly like `query_partitioned`.
    #[instrument(skip(self, filters, preds), fields(project_id, table_name))]
    pub fn query_partitioned_with_text_match(
        &self, project_id: &str, table_name: &str, filters: &[Expr], preds: &[crate::tantivy_index::udf::TextMatchPred],
    ) -> anyhow::Result<Vec<Vec<RecordBatch>>> {
        if preds.is_empty() {
            return self.query_partitioned(project_id, table_name, filters);
        }
        let table_schema = crate::schema_loader::get_schema(table_name);
        let has_indexed = table_schema.as_ref().is_some_and(|s| s.fields.iter().any(|f| f.tantivy.as_ref().is_some_and(|t| t.indexed)));
        if !has_indexed {
            return self.query_partitioned(project_id, table_name, filters);
        }
        let table_schema = table_schema.expect("has_indexed implies Some");

        let mut partitions = Vec::new();
        let ts_range = extract_timestamp_range(filters);

        let Some(table) = self.get_table(project_id, table_name) else {
            return Ok(partitions);
        };
        let pred = compile_filter_conjunction(filters, &table.schema).ok().flatten();
        let mut bucket_ids: Vec<i64> = table.buckets.iter().map(|b| *b.key()).collect();
        bucket_ids.sort();

        for bucket_id in bucket_ids {
            let Some(bucket) = table.buckets.get(&bucket_id) else { continue };
            if !bucket_overlaps_range(&bucket, &ts_range) {
                continue;
            }
            let key = Self::cache_key(project_id, table_name, bucket_id);
            let (snapshot, ids_opt) = self.search_with_snapshot(&bucket, &key, table_schema, preds)?;
            if snapshot.is_empty() {
                continue;
            }

            // Apply id IN ids (atomic with snapshot) when available; the
            // rest of `filters` (including the original `=` / `LIKE` /
            // `text_match` UDF call) runs afterwards via the compiled
            // predicate. Without an id set, fall through to predicate-only.
            let filtered: Vec<RecordBatch> = snapshot
                .into_iter()
                .filter_map(|b| {
                    let b = if let Some(ids) = ids_opt.as_ref() { filter_batch_by_id_set(&b, ids) } else { b };
                    if b.num_rows() == 0 {
                        return None;
                    }
                    match &pred {
                        Some(p) => {
                            let out = apply_predicate(&b, p);
                            (out.num_rows() > 0).then_some(out)
                        }
                        None => Some(b),
                    }
                })
                .collect();

            if !filtered.is_empty() {
                partitions.push(filtered);
            }
        }
        debug!(
            "MemBuffer query_partitioned_with_text_match: project={}, table={}, partitions={}",
            project_id,
            table_name,
            partitions.len()
        );
        Ok(partitions)
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
                results.extend(filter_snapshot(snapshot, &pred));
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
                    let out = filter_snapshot(snapshot, &pred);
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

    /// Time ranges (start, end_exclusive) the Delta scan must exclude
    /// because MemBuffer is authoritative for them, sorted ascending. The
    /// range is each bucket's *actual* row range `[min_ts, max_ts]` — not
    /// its 10-min window — so a bucket holding partial data (WAL-replay
    /// cutoff, late arrivals) can't mask unrelated Delta rows in the rest
    /// of its window. Skipped entirely:
    /// - the current (open) bucket and any force-flushed bucket: their
    ///   windows legitimately hold rows in both stores (disjoint sets, see
    ///   `force_flushed`), so excluding them hides the Delta share;
    /// - empty shells (sentinel min/max), which hold nothing to dedup.
    ///
    /// Returns an empty Vec if the table is absent.
    pub fn get_bucket_ranges(&self, project_id: &str, table_name: &str) -> Vec<(i64, i64)> {
        let Some(table) = self.get_table(project_id, table_name) else {
            return Vec::new();
        };
        let current = Self::current_bucket_id();
        let force_flushed = self.force_flushed.get(&Self::make_key(project_id, table_name));
        let mut ranges: Vec<(i64, i64)> = table
            .buckets
            .iter()
            .filter(|b| *b.key() != current && !force_flushed.as_ref().is_some_and(|s| s.contains(b.key())))
            .filter_map(|b| {
                let (min, max) = (b.min_timestamp.load(Ordering::Relaxed), b.max_timestamp.load(Ordering::Relaxed));
                (min <= max).then_some((min, max + 1))
            })
            .collect();
        ranges.sort_by_key(|(s, _)| *s);
        ranges
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
            // Bucket is gone — drop its text-index cache entry so the LRU
            // doesn't hold ~MB of dead postings until natural eviction.
            self.cache_invalidate(&Self::cache_key(project_id, table_name, bucket_id));
            if let Some(mut s) = self.force_flushed.get_mut(&key) {
                s.remove(&bucket_id);
            }
            self.force_flushed.remove_if(&key, |_, s| s.is_empty());
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
                let (wal_shard_counts, wal_positions) = bucket.snapshot_wal_shard_state(self.shards_per_topic);
                result.push(FlushableBucket {
                    project_id: project_id.to_string(),
                    table_name: table_name.to_string(),
                    bucket_id,
                    batches,
                    wal_positions,
                    row_count: bucket.row_count.load(Ordering::Relaxed),
                    wal_shard_counts,
                    min_timestamp: bucket.min_timestamp.load(Ordering::Relaxed),
                    max_timestamp: bucket.max_timestamp.load(Ordering::Relaxed),
                });
            }
        }
        result
    }

    /// (project_id, table_name, bucket_id) for every bucket at or after
    /// `min_bucket_id` — i.e. the still-open current bucket(s). Used by the
    /// insert-path backpressure escalation to force-flush the open window when
    /// flushing completed buckets alone can't relieve memory pressure.
    pub fn current_bucket_keys(&self, min_bucket_id: i64) -> Vec<(String, String, i64)> {
        let mut out = Vec::new();
        for t in self.tables.iter() {
            let (project_id, table_name) = t.key();
            for b in t.value().buckets.iter() {
                if *b.key() >= min_bucket_id {
                    out.push((project_id.to_string(), table_name.to_string(), *b.key()));
                }
            }
        }
        out
    }

    /// Cheap existence check (no batch cloning) for any bucket of *this topic*
    /// strictly older than `cutoff_bucket_id`. Gates current-bucket force-flush:
    /// advancing the WAL cursor past the open bucket is only correct once every
    /// older bucket on the shard has been flushed + advanced (the cursor consumes
    /// entries sequentially, not by logical bucket). A leftover completed bucket
    /// means a failed flush left the cursor behind it.
    ///
    /// Scoped per `(project, table)` because the WAL invariant it protects is
    /// per-topic (`advance_by_counts` is per-topic). A global check let a stuck
    /// completed bucket in one table freeze current-bucket force-flush for *every*
    /// table — one poison tenant wedged the whole instance at the hard limit,
    /// rejecting all inserts.
    pub fn has_buckets_before(&self, project_id: &str, table_name: &str, cutoff_bucket_id: i64) -> bool {
        self.get_table(project_id, table_name)
            .is_some_and(|t| t.buckets.iter().any(|b| *b.key() < cutoff_bucket_id))
    }

    /// Atomically take a bucket's rows + WAL counts for an out-of-band flush.
    /// Unlike `collect_buckets` + `drain_bucket` (safe only on sealed buckets),
    /// this is safe on the *current* still-written bucket: the take happens
    /// under the same `batches` lock inserts use, so no row can be lost between
    /// snapshot and removal. The now-empty bucket stays in the map so
    /// concurrent/subsequent inserts keep writing into it. Returns None when
    /// the bucket is absent or already empty.
    pub fn take_bucket_for_flush(&self, project_id: &str, table_name: &str, bucket_id: i64) -> Option<FlushableBucket> {
        let table = self.get_table(project_id, table_name)?;
        let bucket_ref = table.buckets.get(&bucket_id)?;
        let bucket = bucket_ref.value();
        let mut batches_g = bucket.batches.lock();
        if batches_g.is_empty() {
            return None;
        }
        // Lock wal_shard_state too so the taken counts match the taken rows
        // exactly — advance_by_counts must not over- or under-advance.
        let mut wal_g = bucket.wal_shard_state.lock();
        let batches: Vec<RecordBatch> = std::mem::take(&mut *batches_g);
        let wal_state = std::mem::take(&mut *wal_g);
        let freed = bucket.memory_bytes.swap(0, Ordering::Relaxed);
        let row_count = bucket.row_count.swap(0, Ordering::Relaxed);
        // Capture the real range as we reset the sentinels so a restore (on
        // Delta commit failure) can replay it instead of guessing bucket-start.
        let min_timestamp = bucket.min_timestamp.swap(i64::MAX, Ordering::Relaxed);
        let max_timestamp = bucket.max_timestamp.swap(i64::MIN, Ordering::Relaxed);
        drop(wal_g);
        drop(batches_g);
        drop(bucket_ref);
        self.estimated_bytes.fetch_sub(freed, Ordering::Relaxed);
        self.cache_invalidate(&Self::cache_key(project_id, table_name, bucket_id));

        // Drop the now-empty bucket so a stale empty shell can't — once time rolls
        // past its window (id < current) — make `has_buckets_before` permanently
        // gate off future current-bucket force-flushes. `remove_if` re-checks
        // emptiness under the shard write lock, so a concurrent insert that
        // repopulated the bucket between the take and here is preserved.
        table.buckets.remove_if(&bucket_id, |_, b| b.batches.lock().is_empty());

        // Pad to shards_per_topic so advance_by_counts indices line up.
        let shards = self.shards_per_topic;
        let mut counts = vec![0u64; shards];
        let mut positions = vec![None; shards];
        for (i, &c) in wal_state.counts.iter().take(shards).enumerate() {
            counts[i] = c;
        }
        for (i, p) in wal_state.positions.iter().take(shards).enumerate() {
            positions[i] = *p;
        }
        Some(FlushableBucket {
            project_id: project_id.to_string(),
            table_name: table_name.to_string(),
            bucket_id,
            batches,
            row_count,
            wal_shard_counts: counts,
            wal_positions: positions,
            min_timestamp,
            max_timestamp,
        })
    }

    /// Re-insert a bucket previously removed by `take_bucket_for_flush` whose
    /// Delta commit then failed. Restores rows (query visibility) and merges
    /// the WAL counts back so the next flush advances the cursor correctly.
    /// Durability never depended on this — the rows are still in the WAL — but
    /// it avoids a query-visibility gap until the next restart/replay.
    pub fn restore_taken_bucket(&self, b: &FlushableBucket) {
        let Some(table) = self.get_table(&b.project_id, &b.table_name) else { return };
        let bucket = table.buckets.entry(b.bucket_id).or_insert_with(TimeBucket::new);
        let mut batches_g = bucket.batches.lock();
        let mut wal_g = bucket.wal_shard_state.lock();
        let added: usize = b.batches.iter().map(estimate_batch_size).sum();
        for batch in &b.batches {
            batches_g.push(batch.clone());
        }
        if wal_g.counts.len() < b.wal_shard_counts.len() {
            wal_g.counts.resize(b.wal_shard_counts.len(), 0);
        }
        for (i, &c) in b.wal_shard_counts.iter().enumerate() {
            wal_g.counts[i] += c;
        }
        if wal_g.positions.len() < b.wal_positions.len() {
            wal_g.positions.resize(b.wal_positions.len(), None);
        }
        for (i, p) in b.wal_positions.iter().enumerate() {
            if let Some(pos) = p {
                wal_g.positions[i] = Some(wal_g.positions[i].map_or(*pos, |prev| prev.max(*pos)));
            }
        }
        bucket.memory_bytes.fetch_add(added, Ordering::Relaxed);
        bucket.row_count.fetch_add(b.row_count, Ordering::Relaxed);
        // Replay the true range (monotonic widen) so restored rows stay visible
        // to time-range pruning; concurrent inserts into the same open bucket
        // are preserved since fetch_min/max only widens.
        bucket.update_timestamps(b.min_timestamp);
        bucket.update_timestamps(b.max_timestamp);
        drop(wal_g);
        drop(batches_g);
        self.estimated_bytes.fetch_add(added, Ordering::Relaxed);
        self.cache_invalidate(&Self::cache_key(&b.project_id, &b.table_name, b.bucket_id));
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
                    // Free the bucket's text-index cache entry alongside its
                    // batches — same reasoning as in drain_bucket.
                    self.cache_invalidate(&Self::cache_key(&table.project_id, &table.table_name, bucket_id));
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

        // Evicted buckets can't mask Delta anymore — drop their marks too.
        self.force_flushed.retain(|_, s| {
            s.retain(|id| *id >= cutoff_bucket_id);
            !s.is_empty()
        });

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

        let physical_predicate = predicate.map(|p| create_physical_expr(&strip_column_qualifiers(p.clone())?, &df_schema, &props)).transpose()?;

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

        let physical_predicate = predicate.map(|p| create_physical_expr(&strip_column_qualifiers(p.clone())?, &df_schema, &props)).transpose()?;

        // Pre-compile assignment expressions
        let physical_assignments: Vec<_> = assignments
            .iter()
            .map(|(col, expr)| {
                let phys_expr = create_physical_expr(&strip_column_qualifiers(expr.clone())?, &df_schema, &props)?;
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

                    let new_batch = RecordBatch::try_new(batch.schema(), new_columns).map_err(arrow_err)?;
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

    /// Update rows matching the predicate with values joined from a
    /// pre-materialized source batch — the `UPDATE ... FROM` execution path.
    ///
    /// For each target row, the join keys are hashed against the source's
    /// matching keys (Arrow `RowConverter` produces canonical comparable
    /// rows). Matched rows then evaluate assignment exprs against a per-batch
    /// "widened" `RecordBatch` whose schema is `(target_fields..., source_fields
    /// renamed to source__<name>...)`, with source columns taken at the
    /// looked-up row index (NULL where no match).
    ///
    /// Multi-match semantics: first source row wins (PG leaves this undefined).
    #[instrument(skip(self, predicate, assignments, source), fields(project_id, table_name, rows_updated))]
    pub fn update_with_source(
        &self, project_id: &str, table_name: &str, predicate: Option<&Expr>, assignments: &[(String, Expr)], source: &crate::dml::UpdateSource,
    ) -> DFResult<u64> {
        use std::collections::HashMap;

        if assignments.is_empty() {
            return Ok(0);
        }
        let Some(table) = self.get_table(project_id, table_name) else {
            return Ok(0);
        };
        let target_schema = table.schema();

        // Build widened schema: target fields + source fields prefixed `source__`,
        // all nullable on the source side (rows without a matching source row
        // produce NULLs in source columns).
        let mut widened_fields: Vec<arrow::datatypes::Field> = target_schema.fields().iter().map(|f| (**f).clone()).collect();
        for f in source.schema.fields() {
            widened_fields.push(arrow::datatypes::Field::new(format!("source__{}", f.name()), f.data_type().clone(), true));
        }
        let widened_schema: SchemaRef = Arc::new(arrow::datatypes::Schema::new(widened_fields));
        let widened_df_schema = DFSchema::try_from(widened_schema.as_ref().clone())?;
        let props = ExecutionProps::new();

        // Rewriter: turn source-qualified column refs into bare `source__<name>`
        // refs so they resolve against the widened schema. Bare source-named
        // columns get the same treatment; everything else passes through.
        let source_col_names: std::collections::HashSet<String> = source.schema.fields().iter().map(|f| f.name().clone()).collect();
        let rewrite = |e: Expr| -> DFResult<Expr> {
            use datafusion::common::tree_node::Transformed;
            e.transform(|expr| match &expr {
                Expr::Column(c) => {
                    let is_source_qual = matches!(c.relation.as_ref(), Some(r) if r.table() == "source");
                    let is_bare_source = c.relation.is_none() && source_col_names.contains(&c.name);
                    if is_source_qual || is_bare_source {
                        Ok(Transformed::yes(Expr::Column(Column::from_name(format!("source__{}", c.name)))))
                    } else {
                        Ok(Transformed::no(expr))
                    }
                }
                _ => Ok(Transformed::no(expr)),
            })
            .map(|t| t.data)
            .map_err(|e| datafusion::error::DataFusionError::Execution(format!("update_with_source: rewrite failed: {e}")))
        };

        let physical_predicate = predicate
            .map(|p| -> DFResult<_> {
                let stripped = strip_column_qualifiers(p.clone())?;
                let rewritten = rewrite(stripped)?;
                create_physical_expr(&rewritten, &widened_df_schema, &props)
            })
            .transpose()?;

        let physical_assignments: Vec<(usize, _)> = assignments
            .iter()
            .map(|(col, expr)| -> DFResult<_> {
                let stripped = strip_column_qualifiers(expr.clone())?;
                let rewritten = rewrite(stripped)?;
                let phys_expr = create_physical_expr(&rewritten, &widened_df_schema, &props)?;
                let col_idx = target_schema
                    .index_of(col)
                    .map_err(|_| datafusion::error::DataFusionError::Execution(format!("Column '{}' not found", col)))?;
                Ok((col_idx, phys_expr))
            })
            .collect::<DFResult<Vec<_>>>()?;

        // Hash the source side once via Arrow's RowConverter so target rows can
        // probe the lookup with byte-identical key encoding.
        //
        // RowConverter requires matching data types across both sides. Target
        // and source key columns can differ (e.g. target stores `Utf8View`
        // while a VALUES-derived source produces `Utf8`), so cast source key
        // cols to the target key col types before hashing. Target columns
        // are used as-is.
        let src_key_cols: Vec<ArrayRef> = source
            .join_keys
            .iter()
            .map(|(tgt_col_name, src_col_name)| {
                let raw = source
                    .batch
                    .column_by_name(src_col_name)
                    .ok_or_else(|| datafusion::error::DataFusionError::Plan(format!("Source column '{}' not found in source batch", src_col_name)))?;
                let target_ty = target_schema
                    .field_with_name(tgt_col_name)
                    .map_err(|_| datafusion::error::DataFusionError::Plan(format!("Target column '{}' not found", tgt_col_name)))?
                    .data_type()
                    .clone();
                if raw.data_type() == &target_ty {
                    Ok(raw.clone())
                } else {
                    arrow::compute::cast(raw.as_ref(), &target_ty).map_err(arrow_err)
                }
            })
            .collect::<DFResult<Vec<_>>>()?;
        let sort_fields: Vec<SortField> = src_key_cols.iter().map(|c| SortField::new(c.data_type().clone())).collect();
        let row_converter = RowConverter::new(sort_fields).map_err(arrow_err)?;
        let src_rows = row_converter.convert_columns(&src_key_cols).map_err(arrow_err)?;
        let mut src_lookup: HashMap<OwnedRow, u32> = HashMap::with_capacity(source.batch.num_rows());
        for (i, row) in src_rows.iter().enumerate() {
            // First-wins on duplicate source keys (PG leaves multi-match
            // semantics undefined; deterministic first-row-wins is our pick).
            src_lookup.entry(row.owned()).or_insert(i as u32);
        }

        let mut total_updated = 0u64;
        let mut total_delta: i64 = 0;

        for mut bucket_entry in table.buckets.iter_mut() {
            let bucket = bucket_entry.value_mut();
            let mut batches = bucket.batches.lock();
            let mut bucket_delta: i64 = 0;
            let mut new_batches: Vec<RecordBatch> = Vec::with_capacity(batches.len());

            for batch in batches.drain(..) {
                let num_rows = batch.num_rows();
                if num_rows == 0 {
                    new_batches.push(batch);
                    continue;
                }

                // Probe source lookup per target row.
                let tgt_key_cols: Vec<ArrayRef> = source
                    .join_keys
                    .iter()
                    .map(|(tgt_col, _)| {
                        batch
                            .column_by_name(tgt_col)
                            .cloned()
                            .ok_or_else(|| datafusion::error::DataFusionError::Plan(format!("Target column '{}' not found", tgt_col)))
                    })
                    .collect::<DFResult<Vec<_>>>()?;
                let tgt_rows = row_converter.convert_columns(&tgt_key_cols).map_err(arrow_err)?;

                let src_idxs: UInt32Array = (0..num_rows).map(|i| src_lookup.get(&tgt_rows.row(i).owned()).copied()).collect();

                // Build widened batch by appending source cols taken at probed indices.
                let mut widened_cols: Vec<ArrayRef> = batch.columns().to_vec();
                for i in 0..source.schema.fields().len() {
                    let src_col = source.batch.column(i);
                    let taken = arrow::compute::take(src_col.as_ref(), &src_idxs, None).map_err(arrow_err)?;
                    widened_cols.push(taken);
                }
                let widened_batch = RecordBatch::try_new(widened_schema.clone(), widened_cols).map_err(arrow_err)?;

                // has_match: source idx was non-null (a join match existed).
                let has_match = BooleanArray::from((0..num_rows).map(|i| !src_idxs.is_null(i)).collect::<Vec<_>>());

                let pred_mask = if let Some(ref phys_pred) = physical_predicate {
                    let result = phys_pred.evaluate(&widened_batch)?;
                    let arr = result.into_array(num_rows)?;
                    arr.as_any()
                        .downcast_ref::<BooleanArray>()
                        .cloned()
                        .ok_or_else(|| datafusion::error::DataFusionError::Execution("Predicate did not return boolean".into()))?
                } else {
                    BooleanArray::from(vec![true; num_rows])
                };

                let mask = arrow::compute::and(&pred_mask, &has_match).map_err(arrow_err)?;

                let matching_count = mask.iter().filter(|v| v == &Some(true)).count();
                if matching_count == 0 {
                    new_batches.push(batch);
                    continue;
                }
                total_updated += matching_count as u64;

                let old_size = estimate_batch_size(&batch);
                let new_columns: Vec<ArrayRef> = (0..batch.num_columns())
                    .map(|col_idx| {
                        if let Some((_, phys_expr)) = physical_assignments.iter().find(|(idx, _)| *idx == col_idx) {
                            let new_values = phys_expr.evaluate(&widened_batch)?.into_array(num_rows)?;
                            merge_arrays(batch.column(col_idx), &new_values, &mask)
                        } else {
                            Ok(batch.column(col_idx).clone())
                        }
                    })
                    .collect::<DFResult<Vec<_>>>()?;

                let new_batch = RecordBatch::try_new(batch.schema(), new_columns).map_err(arrow_err)?;
                bucket_delta += estimate_batch_size(&new_batch) as i64 - old_size as i64;
                new_batches.push(new_batch);
            }

            *batches = new_batches;
            apply_signed_delta(&bucket.memory_bytes, bucket_delta);
            total_delta += bucket_delta;
        }

        apply_signed_delta(&self.estimated_bytes, total_delta);
        debug!(
            "MemBuffer update_with_source: project={}, table={}, rows_updated={}",
            project_id, table_name, total_updated
        );
        Ok(total_updated)
    }

    /// Delete rows using a SQL predicate string (for WAL recovery).
    /// Parses the SQL WHERE clause and delegates to delete().
    #[instrument(skip(self, registry), fields(project_id, table_name))]
    pub fn delete_by_sql(&self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, registry: Option<&FnRegistry>) -> DFResult<u64> {
        let df_schema = self.df_schema_for(project_id, table_name)?;
        let predicate = predicate_sql.map(|s| parse_sql_predicate(s, &df_schema, registry)).transpose()?;
        self.delete(project_id, table_name, predicate.as_ref())
    }

    /// WAL replay path for `UPDATE ... FROM`. Reconstructs an
    /// [`crate::dml::UpdateSource`] from the WAL-stored join keys + source
    /// `RecordBatch`, parses the SQL predicate/assignments against the
    /// widened schema (target + `source__`-prefixed source columns), then
    /// delegates to [`Self::update_with_source`].
    #[instrument(skip(self, assignments, source_batch, registry), fields(project_id, table_name, source_rows = source_batch.num_rows()))]
    #[allow(clippy::too_many_arguments)]
    pub fn update_with_source_by_sql(
        &self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, assignments: &[(String, String)], join_keys: &[(String, String)],
        source_batch: RecordBatch, registry: Option<&FnRegistry>,
    ) -> DFResult<u64> {
        let target_df_schema = self.df_schema_for(project_id, table_name)?;

        // Build the widened DFSchema the assignment SQL was originally parsed
        // against (target fields + source fields renamed with `source__`
        // prefix). Same shape as `update_with_source` constructs internally.
        let mut widened_fields: Vec<arrow::datatypes::Field> = target_df_schema.fields().iter().map(|f| (**f).clone()).collect();
        for f in source_batch.schema().fields() {
            widened_fields.push(arrow::datatypes::Field::new(format!("source__{}", f.name()), f.data_type().clone(), true));
        }
        let widened_schema = Arc::new(arrow::datatypes::Schema::new(widened_fields));
        let widened_df_schema = DFSchema::try_from(widened_schema.as_ref().clone())?;

        let predicate = predicate_sql.map(|s| parse_sql_predicate(s, &widened_df_schema, registry)).transpose()?;
        let parsed_assignments: Vec<(String, Expr)> = assignments
            .iter()
            .map(|(col, val_sql)| parse_sql_predicate(val_sql, &widened_df_schema, registry).map(|expr| (col.clone(), expr)))
            .collect::<DFResult<Vec<_>>>()?;

        let source = crate::dml::UpdateSource {
            schema:    source_batch.schema(),
            batch:     source_batch,
            join_keys: join_keys.to_vec(),
        };
        self.update_with_source(project_id, table_name, predicate.as_ref(), &parsed_assignments, &source)
    }

    /// Update rows using SQL strings (for WAL recovery).
    /// Parses the SQL WHERE clause and assignment expressions, then delegates to update().
    #[instrument(skip(self, assignments, registry), fields(project_id, table_name))]
    pub fn update_by_sql(
        &self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, assignments: &[(String, String)], registry: Option<&FnRegistry>,
    ) -> DFResult<u64> {
        let df_schema = self.df_schema_for(project_id, table_name)?;
        let predicate = predicate_sql.map(|s| parse_sql_predicate(s, &df_schema, registry)).transpose()?;
        let parsed_assignments: Vec<(String, Expr)> = assignments
            .iter()
            .map(|(col, val_sql)| parse_sql_predicate(val_sql, &df_schema, registry).map(|expr| (col.clone(), expr)))
            .collect::<DFResult<Vec<_>>>()?;
        self.update(project_id, table_name, predicate.as_ref(), &parsed_assignments)
    }

    /// DFSchema of the in-memory table, or `DFSchema::empty()` if it isn't
    /// tracked yet — empty schema raises "Column not found" downstream rather
    /// than silently mis-resolving.
    pub fn df_schema_for(&self, project_id: &str, table_name: &str) -> DFResult<DFSchema> {
        match self.get_table(project_id, table_name) {
            Some(table) => DFSchema::try_from(table.schema().as_ref().clone()),
            None => Ok(DFSchema::empty()),
        }
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
            // Authoritative — see `estimated_memory_bytes()` for why we don't
            // trust the `estimated_bytes` AtomicUsize cache.
            estimated_memory_bytes: self.estimated_memory_bytes(),
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
    ///
    /// Fast path: push the batch as-is. When the bucket count crosses
    /// `MAX_BATCH_COUNT_PER_BUCKET`, that insert pays an amortized coalesce
    /// (all batches → one), which both bounds bucket scan time (≤32 batches)
    /// and reclaims per-batch dictionary fragmentation. Concat happens under
    /// the bucket lock, but only once per N inserts instead of every insert,
    /// so writer→reader contention on the snapshot path drops by N×.
    ///
    /// Returns `(new_batch_size_bytes, bucket_id)`. The size returned is the
    /// incoming batch's contribution; coalesce-induced memory shrinkage is
    /// reflected in `bucket.memory_bytes` directly (authoritative) but not
    /// in the value returned (the caller's MemBuffer-level counter is
    /// approximate by design — converges on drain/evict).
    pub fn insert_batch(&self, batch: RecordBatch, timestamp_micros: i64) -> anyhow::Result<(usize, i64)> {
        let batch = compact_batch(batch);
        let bucket_id = MemBuffer::compute_bucket_id(timestamp_micros);
        let row_count = batch.num_rows();
        let new_size = estimate_batch_size(&batch);

        let bucket = self.buckets.entry(bucket_id).or_insert_with(TimeBucket::new);

        {
            let mut g = bucket.batches.lock();
            g.push(batch);
            bucket.memory_bytes.fetch_add(new_size, Ordering::Relaxed);
            // Coalesce gate: fold only the trailing run of batches that are
            // each ≤ MAX_BATCH_BYTES_FOR_COALESCE. Segments past that size
            // graduate and are never re-copied, so the under-lock memcpy is
            // bounded (~the cap) regardless of bucket size, and the tail-run
            // scan touches ≤ tail+1 batches per insert. The previous gate
            // compared *total bucket bytes* against the cap: a busy 10-min
            // bucket crossed 4MB within seconds and then accumulated
            // thousands of uncoalesced single-row batches forever (~30KB
            // allocated+charged per ~2KB row — prod 2026-06-11 tracked 117GB
            // inside a 66.6GiB cgroup).
            //
            // Best-effort: coalesce is an optimisation, not a correctness
            // requirement. If `concat_batches` fails (e.g. schema-evolution
            // mismatch between buffered batches), we leave the Vec as-is
            // and log — the just-pushed batch is durably in the bucket
            // either way. Propagating the error here used to leak the
            // pushed batch back to the caller as Err, who'd then retry
            // and insert a duplicate.
            if g.len() > MAX_BATCH_COUNT_PER_BUCKET {
                let mut tail_start = g.len();
                while tail_start > 0 && estimate_batch_size(&g[tail_start - 1]) <= MAX_BATCH_BYTES_FOR_COALESCE {
                    tail_start -= 1;
                }
                if g.len() - tail_start > MAX_BATCH_COUNT_PER_BUCKET {
                    let schema = g[tail_start].schema();
                    match arrow::compute::concat_batches(&schema, g[tail_start..].iter()) {
                        Ok(combined) => {
                            let folded_size: usize = g[tail_start..].iter().map(estimate_batch_size).sum();
                            let combined_size = estimate_batch_size(&combined);
                            g.truncate(tail_start);
                            g.push(combined);
                            let bucket_bytes = bucket.memory_bytes.load(Ordering::Relaxed);
                            bucket.memory_bytes.store(bucket_bytes.saturating_sub(folded_size) + combined_size, Ordering::Relaxed);
                        }
                        Err(e) => {
                            tracing::warn!(
                                target = "mem_buffer",
                                error = %e,
                                bucket_batch_count = g.len(),
                                "coalesce concat_batches failed; continuing without coalesce (bucket data intact)"
                            );
                        }
                    }
                }
            }
        }
        // `row_count` and the min/max timestamps update OUTSIDE the bucket
        // lock. This is intentional: a concurrent reader that snapshots the
        // batches Vec between the lock release and these atomic stores will
        // momentarily see the new batch's rows in the query result while
        // `row_count` reports the pre-insert total. Stats / observability
        // counters can briefly lag the actual data, but query correctness is
        // unaffected — readers always see the authoritative `batches` vec
        // contents under the lock. Keeping the atomic updates outside the
        // lock holds the critical section to just the Vec push (and the
        // amortised coalesce) so writer throughput isn't capped by atomic
        // ordering on uncontended buckets.
        //
        // **Not used for flush decisions.** Verified by inspection: every
        // consumer of `bucket.row_count` is observability-only —
        // FlushableBucket snapshot for logging (`mem_buffer.rs` snapshot
        // build site), `total_rows` for `timefusion_stats`, and
        // `fetch_sub` on drain (symmetric reconciliation, not a threshold
        // gate). If you ever wire row_count into a flush-trigger threshold,
        // move the update back inside the lock OR derive the value from
        // `batches.iter().map(|b| b.num_rows()).sum()` under the lock.
        bucket.row_count.fetch_add(row_count, Ordering::Relaxed);
        bucket.update_timestamps(timestamp_micros);

        Ok((new_size, bucket_id))
    }
}

impl TimeBucket {
    fn new() -> Self {
        Self {
            batches:         Mutex::new(Vec::new()),
            row_count:       AtomicUsize::new(0),
            memory_bytes:    AtomicUsize::new(0),
            min_timestamp:   AtomicI64::new(i64::MAX),
            max_timestamp:   AtomicI64::new(i64::MIN),
            wal_shard_state: Mutex::new(WalShardState::default()),
        }
    }

    fn record_wal_append(&self, shard: usize, count: u64, position: Option<walrus_rust::WalPosition>) {
        let mut s = self.wal_shard_state.lock();
        if s.counts.len() <= shard {
            s.counts.resize(shard + 1, 0);
        }
        s.counts[shard] += count;
        if let Some(pos) = position {
            if s.positions.len() <= shard {
                s.positions.resize(shard + 1, None);
            }
            s.positions[shard] = Some(s.positions[shard].map_or(pos, |prev| prev.max(pos)));
        }
    }

    fn snapshot_wal_shard_state(&self, shards_per_topic: usize) -> (Vec<u64>, Vec<Option<walrus_rust::WalPosition>>) {
        let s = self.wal_shard_state.lock();
        let mut counts = vec![0u64; shards_per_topic];
        let mut positions = vec![None; shards_per_topic];
        for (i, &c) in s.counts.iter().take(shards_per_topic).enumerate() {
            counts[i] = c;
        }
        for (i, p) in s.positions.iter().take(shards_per_topic).enumerate() {
            positions[i] = *p;
        }
        (counts, positions)
    }

    fn update_timestamps(&self, timestamp: i64) {
        self.min_timestamp.fetch_min(timestamp, Ordering::Relaxed);
        self.max_timestamp.fetch_max(timestamp, Ordering::Relaxed);
    }

    /// Atomic snapshot of this bucket's batches + row count. Both come
    /// from the same lock acquisition so they're guaranteed consistent.
    fn snapshot(&self) -> (Vec<arrow::record_batch::RecordBatch>, usize) {
        let g = self.batches.lock();
        let snap: Vec<arrow::record_batch::RecordBatch> = g.iter().cloned().collect();
        let n: usize = snap.iter().map(|b| b.num_rows()).sum();
        (snap, n)
    }
}

impl MemBuffer {
    /// Atomic snapshot + text-match search for one bucket.
    ///
    /// The bucket's `batches.lock()` provides the snapshot; the
    /// MemBuffer-level cache provides (or builds) the tantivy index. Cache
    /// hit is gated on `indexed_rows == snapshot_rows` — a concurrent
    /// insert between cache hit and use would NOT silently return stale
    /// results because the snapshot we took precedes any later insert.
    ///
    /// `Ok((snapshot, None))` means "no usable text index for this table"
    /// or "no preds passed" — caller falls back to running the original
    /// SQL predicate on the snapshot.
    fn search_with_snapshot(
        &self, bucket: &TimeBucket, cache_key: &BucketCacheKey, table_schema: &crate::schema_loader::TableSchema,
        preds: &[crate::tantivy_index::udf::TextMatchPred],
    ) -> anyhow::Result<(Vec<arrow::record_batch::RecordBatch>, Option<std::collections::HashSet<String>>)> {
        let (snapshot, snapshot_rows) = bucket.snapshot();
        if preds.is_empty() || snapshot.is_empty() {
            return Ok((snapshot, None));
        }

        // Try the cache. Reuse only if its row count matches the snapshot.
        let mut idx = self.cache_get(cache_key);
        if idx.as_ref().is_none_or(|i| i.indexed_rows != snapshot_rows) {
            let built = crate::tantivy_index::mem_index::BucketTextIndex::build(table_schema, &snapshot, snapshot_rows)?;
            let Some(built) = built else {
                return Ok((snapshot, None));
            };
            idx = Some(self.cache_put(cache_key.clone(), Arc::new(built)));
        }
        let idx = idx.expect("idx is Some on this path");

        // Run each predicate and intersect — sound only because predicates are
        // AND-ed; `collect_text_matches` skips OR subtrees so disjunctive terms
        // never reach here (else x_ids ∩ y_ids = ∅ would drop every match).
        let ids_per_pred: anyhow::Result<Vec<std::collections::HashSet<String>>> =
            preds.iter().map(|p| idx.search(p).map(|hits| hits.into_iter().map(|h| h.id).collect())).collect();
        let combined = ids_per_pred?.into_iter().reduce(|a, b| a.intersection(&b).cloned().collect()).unwrap_or_default();
        Ok((snapshot, Some(combined)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, StringViewArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
    };

    use super::*;

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

    // Prod 2026-06-11: pgwire fast-insert / WAL-replay batches materialize a
    // ~16KB view block per Utf8View column via `ScalarValue::to_array_of_size`
    // (~1MB charged per row on the 89-col otel schema). MemBuffer tracked
    // 1.5TB against a 66.6GiB cgroup and rejected every insert within minutes
    // of boot. Guard: a single-row wide-Utf8View batch must be charged near
    // its logical size, including view columns nested in List and Struct.
    /// One ~3KB-logical row across 64 Utf8View columns plus view columns
    /// nested in List and Struct — built the way the pgwire fast-insert path
    /// builds rows (`ScalarValue::to_array_of_size`).
    fn wide_view_row(ts: i64) -> RecordBatch {
        use datafusion::common::ScalarValue;
        let n_str_cols = 64;
        let mut fields: Vec<Field> = vec![Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false)];
        fields.extend((0..n_str_cols).map(|i| Field::new(format!("c{i}"), DataType::Utf8View, true)));
        let item = Arc::new(Field::new("item", DataType::Utf8View, true));
        fields.push(Field::new("l", DataType::List(item.clone()), true));
        fields.push(Field::new("s", DataType::Struct(vec![Field::new("v", DataType::Utf8View, true)].into()), true));
        let schema = Arc::new(Schema::new(fields));

        let view_col = || ScalarValue::Utf8View(Some("a string too long to inline in the view".into())).to_array_of_size(1).unwrap();
        let mut cols: Vec<ArrayRef> = vec![Arc::new(TimestampMicrosecondArray::from(vec![ts]).with_timezone("UTC"))];
        cols.extend((0..n_str_cols).map(|_| view_col()));
        cols.push(Arc::new(arrow::array::ListArray::new(
            item,
            arrow::buffer::OffsetBuffer::from_lengths([1]),
            view_col(),
            None,
        )));
        cols.push(Arc::new(arrow::array::StructArray::new(
            vec![Field::new("v", DataType::Utf8View, true)].into(),
            vec![view_col()],
            None,
        )));
        RecordBatch::try_new(schema, cols).unwrap()
    }

    #[test]
    fn single_row_utf8view_insert_charged_logical_size_not_block_capacity() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        buffer.insert("p1", "t1", wide_view_row(ts), ts).unwrap();
        let charged = buffer.estimated_memory_bytes();
        assert!(
            charged < 64 * 1024,
            "single ~3KB-logical row charged {charged} bytes — view block capacity is leaking into memory accounting"
        );
    }

    // Prod 2026-06-11 evening: 54,767 rows / 221 batches charged 29.9GB
    // (~135MB per batch). monoscope's `UPDATE otel_logs_and_spans` jobs make
    // the DML path re-insert rows read from parquet: filtered/sliced view
    // arrays inherit the reader's full column-chunk data blocks, where
    // capacity == len — invisible to slack-based waste detection. The gate
    // must compare capacity against bytes the views actually reference.
    #[test]
    fn sliced_scan_rows_charged_referenced_bytes_not_block_size() {
        let big: Vec<String> = (0..1000).map(|i| format!("{i:0>100}")).collect();
        let full = StringViewArray::from(big.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        let ts = chrono::Utc::now().timestamp_micros();
        let mut fields: Vec<Field> = vec![Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false)];
        fields.extend((0..8).map(|i| Field::new(format!("c{i}"), DataType::Utf8View, true)));
        let schema = Arc::new(Schema::new(fields));
        let mut cols: Vec<ArrayRef> = vec![Arc::new(TimestampMicrosecondArray::from(vec![ts]).with_timezone("UTC"))];
        cols.extend((0..8).map(|_| Arc::new(full.slice(0, 1)) as ArrayRef));
        let batch = RecordBatch::try_new(schema, cols).unwrap();

        let buffer = MemBuffer::new();
        buffer.insert("p1", "t1", batch, ts).unwrap();
        let charged = buffer.estimated_memory_bytes();
        assert!(
            charged < 32 * 1024,
            "1 sliced row (8 view cols x 100B referenced) charged {charged} bytes — inherited scan blocks are leaking into accounting"
        );
    }

    // Companion guard: tail-fold outputs must also stay near logical size
    // (folds concat view arrays; inherited buffers may carry slack).
    #[test]
    fn fold_outputs_charged_near_logical_size() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        for _ in 0..200 {
            buffer.insert("p1", "t1", wide_view_row(ts), ts).unwrap();
        }
        let charged = buffer.estimated_memory_bytes();
        // 200 rows × ~3KB logical ≈ 600KB; allow generous overhead but stay
        // orders of magnitude under the ~megabytes-per-fold failure mode.
        assert!(
            charged < 16 * 1024 * 1024,
            "200 ~3KB-logical rows charged {charged} bytes — fold outputs are carrying view block capacity"
        );
    }

    // Prod 2026-06-11 follow-up to the view-block fix: the coalesce gate
    // compared *total bucket bytes* against MAX_BATCH_BYTES_FOR_COALESCE, so
    // a busy 10-min bucket crossed the 4MB cap within seconds and then
    // accumulated thousands of uncoalesced single-row batches forever
    // (~30KB allocated+charged per ~2KB row; tracked memory hit 117GB in a
    // 66.6GiB cgroup). Coalescing must keep working on the small tail no
    // matter how large the bucket grows.
    #[test]
    fn bucket_keeps_coalescing_past_4mb() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let payload = "x".repeat(64 * 1024);
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("body", DataType::Utf8View, false),
        ]));
        for _ in 0..200 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMicrosecondArray::from(vec![ts]).with_timezone("UTC")),
                    Arc::new(StringViewArray::from(vec![payload.as_str()])),
                ],
            )
            .unwrap();
            buffer.insert("p1", "t1", batch, ts).unwrap();
        }
        let table = buffer.tables.get(&MemBuffer::make_key("p1", "t1")).unwrap();
        let bucket = table.buckets.get(&MemBuffer::compute_bucket_id(ts)).unwrap();
        let g = bucket.batches.lock();
        let (n_batches, total_rows) = (g.len(), g.iter().map(|b| b.num_rows()).sum::<usize>());
        assert_eq!(total_rows, 200, "coalesce must not lose rows");
        assert!(
            n_batches <= 2 * (MAX_BATCH_COUNT_PER_BUCKET + 1),
            "bucket holds {n_batches} batches — coalesce stopped once the bucket crossed MAX_BATCH_BYTES_FOR_COALESCE"
        );
    }

    #[test]
    fn dedup_batches_keep_last_on_composite_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8View, false),
        ]));
        let mk = |ts: Vec<i64>, ids: Vec<i64>, pl: Vec<&str>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringViewArray::from(pl)),
                ],
            )
            .unwrap()
        };
        let batches = vec![
            mk(vec![100, 200], vec![1, 2], vec!["v1-old", "v2-old"]),
            mk(vec![100, 300], vec![1, 3], vec!["v1-new", "v3"]),
            mk(vec![200], vec![2], vec!["v2-new"]),
        ];
        let keys = vec!["id".to_string(), "timestamp".to_string()];
        let out = dedup_batches(batches, &keys).expect("dedup ok");
        // Dedup filters each batch in place (no full-payload concat), so the
        // survivors come back as multiple batches — collect across all of them.
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "should collapse to 3 unique (id,ts)");
        let got: Vec<(i64, String)> = out
            .iter()
            .flat_map(|b| {
                let ids = b.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
                let pl = b.column_by_name("payload").unwrap().as_any().downcast_ref::<StringViewArray>().unwrap();
                (0..b.num_rows()).map(move |i| (ids.value(i), pl.value(i).to_string())).collect::<Vec<_>>()
            })
            .collect();
        // Surviving global indices [2,3,4] → batch1 keeps (1,new),(3,v3); batch2 keeps (2,new).
        assert_eq!(got, vec![(1, "v1-new".into()), (3, "v3".into()), (2, "v2-new".into())]);
    }

    #[test]
    fn dedup_batches_noop_when_keys_empty_or_input_empty() {
        let empty: Vec<RecordBatch> = vec![];
        assert!(dedup_batches(empty, &["id".to_string()]).unwrap().is_empty());

        let batch = create_test_batch(123);
        let out = dedup_batches(vec![batch.clone()], &[]).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), batch.num_rows());
    }

    /// Regression (prod 2026-06-20): a large coalesced otel flush fused every
    /// batch into one RecordBatch inside `dedup_batches`, overflowing Arrow's 2GB
    /// i32 string-offset limit ("Offset overflow error"). That failed the flush,
    /// so the buckets never drained and MemBuffer wedged at the hard limit —
    /// every insert rejected. Dedup must not build a single fused array: with no
    /// duplicate keys across N input batches it returns those N batches untouched.
    #[test]
    fn dedup_batches_does_not_concatenate_full_payload() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8View, false),
        ]));
        let mk = |id: i64, p: &str| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![id])), Arc::new(StringViewArray::from(vec![p]))]).unwrap()
        };
        let batches = vec![mk(1, "a"), mk(2, "b"), mk(3, "c")];
        let out = dedup_batches(batches, &["id".to_string()]).expect("dedup ok");
        assert_eq!(out.len(), 3, "distinct-key batches must be returned un-fused (no 2GB-prone concat)");
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    #[test]
    fn dedup_batches_errors_on_unknown_key() {
        let batch = create_test_batch(1);
        let err = dedup_batches(vec![batch], &["nonexistent".to_string()]).unwrap_err();
        assert!(err.to_string().contains("nonexistent"), "msg: {err}");
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
    fn search_text_match_returns_matching_ids_from_membuffer() {
        // Build a real otel_logs_and_spans batch and verify the per-bucket
        // tantivy index returns matching IDs before flush. This exercises:
        // (1) lazy build on first query, (2) ngram3 tokenizer integration,
        // (3) the bucket-search → MemBuffer.search_text_match plumbing.
        use crate::test_utils::test_helpers::{json_to_batch, test_span};
        let buffer = MemBuffer::new();
        let r1 = test_span("row-1", "auth-svc", "p1");
        let r2 = test_span("row-2", "billing-svc", "p1");
        let batch = json_to_batch(vec![r1, r2]).expect("json_to_batch");
        let ts = chrono::Utc::now().timestamp_micros();
        buffer.insert("p1", "otel_logs_and_spans", batch, ts).unwrap();

        let preds = vec![crate::tantivy_index::udf::TextMatchPred {
            column: "name".into(),
            query:  "auth".into(),
        }];
        let got = buffer.search_text_match("p1", "otel_logs_and_spans", &preds).expect("search");
        let ids = got.expect("indexed table produces Some");
        assert!(ids.contains("row-1"), "expected row-1 (auth-svc) in hit set: {:?}", ids);
        assert!(!ids.contains("row-2"), "expected row-2 (billing-svc) NOT in hit set: {:?}", ids);
    }

    #[test]
    fn search_text_match_returns_none_for_unindexed_table() {
        // table1 isn't in the YAML schema registry → no indexed fields →
        // search_text_match returns None so the caller falls back.
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        buffer.insert("p1", "table1", create_test_batch(ts), ts).unwrap();

        let preds = vec![crate::tantivy_index::udf::TextMatchPred {
            column: "name".into(),
            query:  "test".into(),
        }];
        let got = buffer.search_text_match("p1", "table1", &preds).expect("search");
        assert!(got.is_none(), "unindexed table should return None, got {:?}", got);
    }

    #[test]
    fn search_text_match_cache_invalidates_on_insert() {
        // Build cache via first query, insert new rows, second query must
        // see them (i.e. cache was invalidated and rebuilt).
        use crate::test_utils::test_helpers::{json_to_batch, test_span};
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let batch1 = json_to_batch(vec![test_span("a", "alpha-svc", "p1")]).unwrap();
        buffer.insert("p1", "otel_logs_and_spans", batch1, ts).unwrap();
        let preds = vec![crate::tantivy_index::udf::TextMatchPred {
            column: "name".into(),
            query:  "beta".into(),
        }];
        let initial = buffer.search_text_match("p1", "otel_logs_and_spans", &preds).unwrap().unwrap();
        assert!(initial.is_empty(), "no 'beta' row inserted yet");

        let batch2 = json_to_batch(vec![test_span("b", "beta-svc", "p1")]).unwrap();
        buffer.insert("p1", "otel_logs_and_spans", batch2, ts + 1).unwrap();
        let post = buffer.search_text_match("p1", "otel_logs_and_spans", &preds).unwrap().unwrap();
        assert!(post.contains("b"), "expected 'b' after insert+rebuild, got {:?}", post);
    }

    #[test]
    fn query_partitioned_with_text_match_returns_atomic_snapshot() {
        // Atomicity invariant: query_partitioned_with_text_match returns
        // batches filtered against an id set taken from the SAME snapshot.
        // A row that exists in the bucket at query time MUST be either in
        // both (returned) or in neither (filtered) — never in the snapshot
        // but missing from the id set.
        use crate::test_utils::test_helpers::{json_to_batch, test_span};
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        // Build a batch with two rows, one matching the search and one not.
        let batch = json_to_batch(vec![
            test_span("hit-1", "alpha-search-svc", "p1"),
            test_span("miss-1", "completely-unrelated-svc", "p1"),
        ])
        .unwrap();
        buffer.insert("p1", "otel_logs_and_spans", batch, ts).unwrap();

        let preds = vec![crate::tantivy_index::udf::TextMatchPred {
            column: "name".into(),
            query:  "alpha".into(),
        }];
        let parts = buffer.query_partitioned_with_text_match("p1", "otel_logs_and_spans", &[], &preds).unwrap();
        let total_rows: usize = parts.iter().flatten().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "expected only the matching row, got {} rows in {:?}", total_rows, parts);

        // Verify the returned row is the matching one by checking the id col.
        use arrow::array::AsArray;
        let returned = &parts[0][0];
        let id_arr = returned.column_by_name("id").unwrap().as_string_view();
        assert_eq!(id_arr.value(0), "hit-1");
    }

    #[test]
    fn query_partitioned_with_text_match_empty_preds_falls_through() {
        // No text_match preds → behave identically to query_partitioned.
        use crate::test_utils::test_helpers::{json_to_batch, test_span};
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let batch = json_to_batch(vec![test_span("a", "svc", "p1"), test_span("b", "svc", "p1")]).unwrap();
        buffer.insert("p1", "otel_logs_and_spans", batch, ts).unwrap();

        let parts = buffer.query_partitioned_with_text_match("p1", "otel_logs_and_spans", &[], &[]).unwrap();
        let total: usize = parts.iter().flatten().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "no text_match preds → all rows returned");
    }

    /// Regression: `restore_taken_bucket` (the Delta-commit-failure path of the
    /// open-bucket force-flush) used to reset the bucket's min/max to the bucket
    /// *start* (`bucket_id * duration`), hiding restored rows from time-range
    /// pruning until the next insert. It must replay the rows' real range.
    #[test]
    fn restore_taken_bucket_preserves_timestamp_range() {
        use crate::test_utils::test_helpers::{json_to_batch, test_span};
        let buffer = MemBuffer::new();
        let dur = bucket_duration_micros();
        let ts = 7 * dur + 12_345; // mid-bucket — distinct from the bucket-start sentinel
        let bucket_id = MemBuffer::compute_bucket_id(ts);
        buffer.insert("p1", "otel_logs_and_spans", json_to_batch(vec![test_span("a", "svc", "p1")]).unwrap(), ts).unwrap();

        let taken = buffer.take_bucket_for_flush("p1", "otel_logs_and_spans", bucket_id).expect("bucket taken");
        assert_eq!((taken.min_timestamp, taken.max_timestamp), (ts, ts), "take must capture the real row range");

        buffer.restore_taken_bucket(&taken); // simulate Delta commit failure
        let again = buffer.take_bucket_for_flush("p1", "otel_logs_and_spans", bucket_id).expect("restored bucket present");
        assert_eq!((again.min_timestamp, again.max_timestamp), (ts, ts), "restore must preserve the true range");
        assert_ne!(again.min_timestamp, bucket_id * dur, "must not collapse to bucket start");
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

    // Regression: predicate/assignment exprs from the SQL planner carry table
    // qualifiers (e.g. `Column { relation: Some("table1"), name: "id" }`), but
    // DFSchema is built from the bare table schema. Without qualifier stripping,
    // create_physical_expr fails with "No field named table1.id".
    #[test]
    fn test_delete_with_qualified_predicate() {
        use datafusion::{
            common::{Column, TableReference},
            logical_expr::{Expr, lit},
        };

        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let batch = create_multi_row_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        buffer.insert("project1", "table1", batch, ts).unwrap();

        let predicate = Expr::Column(Column::new(Some(TableReference::bare("table1")), "id")).eq(lit(2i64));
        let deleted = buffer.delete("project1", "table1", Some(&predicate)).unwrap();
        assert_eq!(deleted, 1);
    }

    #[test]
    fn test_update_with_qualified_predicate_and_assignment() {
        use datafusion::{
            common::{Column, TableReference},
            logical_expr::{Expr, lit},
        };

        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let batch = create_multi_row_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        buffer.insert("project1", "table1", batch, ts).unwrap();

        let predicate = Expr::Column(Column::new(Some(TableReference::bare("table1")), "id")).eq(lit(2i64));
        // Assignment value also references a qualified column — the planner
        // produces these when the SET RHS reads from the same table.
        let value_expr = Expr::Column(Column::new(Some(TableReference::bare("table1")), "name"));
        let assignments = vec![("name".to_string(), value_expr)];
        let updated = buffer.update("project1", "table1", Some(&predicate), &assignments).unwrap();
        assert_eq!(updated, 1);
    }

    fn test_table_df_schema() -> DFSchema {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let batch = create_multi_row_batch(vec![1], vec!["a"]);
        buffer.insert("p", "t", batch, ts).unwrap();
        buffer.df_schema_for("p", "t").unwrap()
    }

    // Regression: WAL replay used to fail "No functions registered" on any UDF.
    #[test]
    fn parse_sql_predicate_without_registry_rejects_udf() {
        let schema = test_table_df_schema();
        let err = super::parse_sql_predicate("coalesce(name, '') = 'x'", &schema, None).unwrap_err();
        assert!(
            err.to_string().contains("No functions registered"),
            "expected 'No functions registered' error, got: {err}"
        );
    }

    #[test]
    fn parse_sql_predicate_with_registry_handles_udf() {
        let schema = test_table_df_schema();
        let reg = crate::functions::function_registry().unwrap();
        super::parse_sql_predicate("coalesce(name, '') = 'x'", &schema, Some(reg.as_ref())).expect("coalesce should resolve");
        super::parse_sql_predicate("to_char(timestamp, 'YYYY') = '2024'", &schema, Some(reg.as_ref())).expect("to_char should resolve");
    }

    // upper() — a UDF that survives logical->physical lowering (unlike coalesce
    // which the optimizer rewrites to CASE).
    #[test]
    fn update_by_sql_with_udf_replays_when_registry_present() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let batch = create_multi_row_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        buffer.insert("project1", "table1", batch, ts).unwrap();

        let reg = crate::functions::function_registry().unwrap();
        let updated = buffer
            .update_by_sql(
                "project1",
                "table1",
                Some("upper(name) = 'B'"),
                &[("name".into(), "'updated'".into())],
                Some(reg.as_ref()),
            )
            .expect("UDF-bearing UPDATE should replay with registry");
        assert_eq!(updated, 1);

        assert!(
            buffer.update_by_sql("project1", "table1", Some("upper(name) = 'A'"), &[("name".into(), "'x'".into())], None).is_err(),
            "without registry, UDF planning should fail rather than silently no-op"
        );
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
        use std::{sync::Arc, thread};

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
        // Flush-time pre-compaction is gone (the parquet writer downstream
        // regroups rows into row groups itself, and pre-compacting forced an
        // unnecessary deep copy of the entire bucket).
        //
        // Insert-time coalesce still applies: when `batches.len()` crosses
        // `MAX_BATCH_COUNT_PER_BUCKET` we concat under the bucket lock to
        // amortise per-batch RecordBatch overhead. So after N tiny inserts
        // the flushable bucket may carry fewer than N RecordBatches —
        // the invariant the flush path actually cares about is "every row
        // makes it through", not "one RecordBatch per insert".
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();

        let total_rows = 10;
        for i in 0..total_rows {
            let batch = create_multi_row_batch(vec![i as i64], vec!["test"]);
            buffer.insert("project1", "table1", batch, ts).unwrap();
        }

        let cutoff = MemBuffer::compute_bucket_id(ts) + 1;
        let flushable = buffer.get_flushable_buckets(cutoff);
        assert_eq!(flushable.len(), 1, "all inserts share one time bucket");
        assert_eq!(flushable[0].row_count, total_rows);
        let summed: usize = flushable[0].batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(summed, total_rows, "no rows lost to insert-time coalesce");
        // Coalesce keeps the in-bucket batch count bounded by MAX_BATCH_COUNT_PER_BUCKET + 1
        // (concat fires when len > MAX_BATCH_COUNT_PER_BUCKET, then the next push reaches
        // the cap again before the next concat).
        assert!(
            flushable[0].batches.len() <= MAX_BATCH_COUNT_PER_BUCKET + 1,
            "got {} batches, expected ≤ {}",
            flushable[0].batches.len(),
            MAX_BATCH_COUNT_PER_BUCKET + 1
        );
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

    /// Repro for the prod fragmentation incident (docs/membuffer_flush_fix_plan.md):
    /// monoscope ingests OTLP traces as ~30-row INSERTs. Pre-fix, each INSERT
    /// became one RecordBatch in the bucket → 1000 inserts = 1000 batches,
    /// 30 rows/batch, scan-time bound. With amortized coalesce, the bucket
    /// is bounded at MAX_BATCH_COUNT_PER_BUCKET — when a push crosses
    /// the threshold the next insert folds the lot into one.
    #[test]
    fn insert_coalesces_small_batches_into_bucket_tail() {
        let buffer = MemBuffer::new();
        let ts = 1_000_000_000_000i64;
        let row_count_per_insert = 30;
        let inserts = 1000;
        let total_rows = row_count_per_insert * inserts;

        for i in 0..inserts {
            let batch = make_batch_with_rows(ts + i as i64, row_count_per_insert);
            buffer.insert("p1", "t1", batch, ts).unwrap();
        }

        let bucket_id = MemBuffer::compute_bucket_id(ts);
        let table = buffer.get_table("p1", "t1").unwrap();
        let bucket = table.buckets.get(&bucket_id).expect("bucket exists");

        let snapshot: Vec<RecordBatch> = bucket.batches.lock().iter().cloned().collect();
        let n_batches = snapshot.len();
        let total_in_bucket: usize = snapshot.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_in_bucket, total_rows, "row preservation");
        assert!(
            n_batches <= MAX_BATCH_COUNT_PER_BUCKET + 1,
            "bucket should hold ≤{} batches after amortized coalesce, got {n_batches}",
            MAX_BATCH_COUNT_PER_BUCKET + 1
        );
        // The bound on n_batches above is the load-bearing assertion for
        // coalesce. A separate avg-rows-per-batch check is redundant
        // (avg = total_rows / n_batches by definition) and would be
        // schema-sensitive — bigger column types ⇒ larger per-batch
        // memory for the same row count without changing the count.
    }

    fn make_batch_with_rows(start_ts: i64, n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, false),
        ]));
        let ts_array = TimestampMicrosecondArray::from(vec![start_ts; n]).with_timezone("UTC");
        let id_array = Int64Array::from((0..n as i64).collect::<Vec<_>>());
        let name_array = StringViewArray::from((0..n).map(|i| format!("row-{i}")).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(ts_array), Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    /// Single-bucket batch with a `name` Utf8View column for the OR-equality test.
    fn name_batch(ts: i64, names: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, false),
        ]));
        let n = names.len();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![ts; n]).with_timezone("UTC")),
                Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
                Arc::new(StringViewArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn membuffer_or_equality_on_utf8view_keeps_all_matches() {
        use datafusion::{
            logical_expr::{Expr, col},
            scalar::ScalarValue,
        };
        let buf = MemBuffer::new();
        let ts = 1_700_000_000_000_000;
        let mut names = Vec::new();
        names.extend(std::iter::repeat_n("client", 1258));
        names.extend(std::iter::repeat_n("internal", 13346));
        names.extend(std::iter::repeat_n("server", 200));
        buf.insert("p", "t", name_batch(ts, names), ts).unwrap();

        // Prod literal type: Utf8View (map_string_types_to_utf8view=true).
        let view = |s: &str| Expr::Literal(ScalarValue::Utf8View(Some(s.to_string())), None);
        let or = col("name").eq(view("client")).or(col("name").eq(view("internal")));

        let parts = buf.query_partitioned("p", "t", std::slice::from_ref(&or)).unwrap();
        let rows: usize = parts.iter().flatten().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1258 + 13346, "OR of two Utf8View equalities must keep all matches");
    }
}
