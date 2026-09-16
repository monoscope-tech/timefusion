use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering},
};

use arrow::{
    array::{Array, ArrayRef, BooleanArray, RecordBatch, TimestampMicrosecondArray, UInt32Array},
    compute::{concat, filter_record_batch},
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit},
    row::{RowConverter, SortField},
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
use itertools::Itertools;
use parking_lot::Mutex;
use tracing::{debug, error, info, instrument, warn};

use crate::{observability::arrow_err, read::functions::FnRegistry};

// Must track `d_bucket_duration_secs` in config.rs.
const DEFAULT_BUCKET_DURATION_MICROS: i64 = 5 * 60 * 1_000_000;
#[cfg(test)]
const BUCKET_DURATION_MICROS: i64 = DEFAULT_BUCKET_DURATION_MICROS;

static BUCKET_DURATION_MICROS_CFG: std::sync::OnceLock<i64> = std::sync::OnceLock::new();

/// Crossing it makes one insert pay an amortized coalesce (all batches → one).
const MAX_BATCH_COUNT_PER_BUCKET: usize = 8;
/// Skip the in-lock coalesce above this size — `concat_batches` on tens of MB
/// holds the bucket lock and starves readers. 4 MB = one Arrow IPC block.
const MAX_BATCH_BYTES_FOR_COALESCE: usize = 4 * 1024 * 1024;

/// Configured bucket window in microseconds; defaults to 5 minutes when unset.
pub fn bucket_duration_micros() -> i64 {
    *BUCKET_DURATION_MICROS_CFG.get_or_init(|| DEFAULT_BUCKET_DURATION_MICROS)
}

/// No-op after the first call; must precede any MemBuffer activity.
pub fn set_bucket_duration_micros(micros: i64) {
    let _ = BUCKET_DURATION_MICROS_CFG.set(micros.max(1_000_000));
}

/// Compatible = every existing field present in `incoming` with a compatible
/// type; `incoming` may add fields only if they are nullable.
fn schemas_compatible(existing: &SchemaRef, incoming: &SchemaRef) -> bool {
    if !existing.fields().iter().all(|f| incoming.field_with_name(f.name()).is_ok_and(|i| types_compatible(f.data_type(), i.data_type()))) {
        return false;
    }
    // A new NOT NULL field would break the already-buffered rows.
    let Some(added) =
        incoming.fields().iter().filter(|f| existing.field_with_name(f.name()).is_err()).try_fold(0usize, |n, f| f.is_nullable().then_some(n + 1))
    else {
        return false;
    };
    if added > 0 {
        info!("Schema evolution: {added} new nullable field(s) added");
    }
    true
}

/// Take nullability from the **declared** schema for same-name, exactly-
/// same-type fields only; tightening to NOT NULL also requires the column to
/// hold no nulls. `null_count` is `None` for schema-only alignment, where the
/// declared nullability is trusted. `None` result = nothing changed.
fn align_nullability(schema: &SchemaRef, declared: &SchemaRef, null_count: Option<&dyn Fn(usize) -> usize>) -> Option<SchemaRef> {
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let honest = |d: &Field| d.is_nullable() || null_count.is_none_or(|nc| nc(i) == 0);
            match declared.field_with_name(f.name()) {
                Ok(d) if d.data_type() == f.data_type() && d.is_nullable() != f.is_nullable() && honest(d) => {
                    Arc::new(f.as_ref().clone().with_nullable(d.is_nullable()))
                }
                _ => f.clone(),
            }
        })
        .collect();
    let changed = fields.iter().zip(schema.fields()).any(|(new, old)| !Arc::ptr_eq(new, old));
    changed.then(|| Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())) as SchemaRef)
}

fn align_batch_nullability(batch: RecordBatch, declared: Option<&SchemaRef>) -> RecordBatch {
    let Some(declared) = declared else { return batch };
    let nc = |i: usize| batch.column(i).null_count();
    let Some(aligned) = align_nullability(&batch.schema(), declared, Some(&nc)) else { return batch };
    RecordBatch::try_new(aligned, batch.columns().to_vec()).unwrap_or(batch)
}

/// Put a registered table's batch in its declared column order and type
/// representation before it can establish or join a [`TableBuffer`], so an
/// incidental INSERT/WAL-replay column order never becomes the table schema.
fn canonicalize_declared_batch(batch: RecordBatch, declared: Option<&SchemaRef>) -> anyhow::Result<RecordBatch> {
    let Some(declared) = declared else { return Ok(batch) };
    if batch.schema_ref() == declared {
        return Ok(batch);
    }
    // Widening a partial batch would have to invent values for declared
    // NOT-NULL fields, so leave projections alone.
    let incoming = batch.schema();
    if declared.fields().iter().any(|field| !field.is_nullable() && incoming.field_with_name(field.name()).is_err())
        || incoming.fields().iter().any(|field| declared.field_with_name(field.name()).is_err())
    {
        return Ok(batch);
    }
    // Casting directly to a declared NOT NULL field would relabel an array that
    // contains nulls as non-nullable, and downstream code would trust it.
    let fields: Vec<FieldRef> = declared
        .fields()
        .iter()
        .map(|field| {
            batch
                .schema()
                .index_of(field.name())
                .ok()
                .filter(|&i| !field.is_nullable() && batch.column(i).null_count() > 0)
                .map_or_else(|| field.clone(), |_| Arc::new(field.as_ref().clone().with_nullable(true)))
        })
        .collect();
    let honest_declared = Arc::new(Schema::new_with_metadata(fields, declared.metadata().clone()));
    deltalake::kernel::schema::cast_record_batch(&batch, honest_declared, true, true)
        .map_err(|e| anyhow::anyhow!("batch does not match declared table schema: {e}"))
}

fn types_compatible(existing: &DataType, incoming: &DataType) -> bool {
    match (existing, incoming) {
        (DataType::Timestamp(u1, tz1), DataType::Timestamp(u2, tz2)) => {
            if u1 == u2 && tz1 != tz2 {
                debug!("Timestamp timezone mismatch: {:?} vs {:?} (allowed)", tz1, tz2);
            }
            u1 == u2
        }
        (DataType::List(f1), DataType::List(f2)) | (DataType::LargeList(f1), DataType::LargeList(f2)) => types_compatible(f1.data_type(), f2.data_type()),
        (DataType::Struct(fields1), DataType::Struct(fields2)) => {
            fields1.iter().all(|f1| fields2.iter().find(|f| f.name() == f1.name()).is_some_and(|f2| types_compatible(f1.data_type(), f2.data_type())))
        }
        (DataType::Map(f1, _), DataType::Map(f2, _)) => types_compatible(f1.data_type(), f2.data_type()),
        (DataType::Dictionary(_, v1), DataType::Dictionary(_, v2)) => types_compatible(v1, v2),
        (DataType::FixedSizeList(f1, n1), DataType::FixedSizeList(f2, n2)) => n1 == n2 && types_compatible(f1.data_type(), f2.data_type()),
        _ => existing == incoming,
    }
}

/// Widened schema for `UPDATE ... FROM`: target fields + source fields
/// prefixed `source__`, all nullable on the source side. [`MemBuffer::update_with_source`]
/// and [`MemBuffer::update_with_source_by_sql`] must build byte-identical schemas.
fn widen_schema_with_source(target_fields: &arrow::datatypes::Fields, source_fields: &arrow::datatypes::Fields) -> SchemaRef {
    let widened_fields = target_fields
        .iter()
        .map(|f| (**f).clone())
        .chain(source_fields.iter().map(|f| Field::new(format!("source__{}", f.name()), f.data_type().clone(), true)))
        .collect::<Vec<_>>();
    Arc::new(Schema::new(widened_fields))
}

/// (min, max) of the batch's `timestamp` column, if present.
pub fn batch_timestamp_range(batch: &RecordBatch) -> Option<(i64, i64)> {
    let schema = batch.schema();
    let ts_idx = schema.fields().iter().position(|f| f.name() == "timestamp" && matches!(f.data_type(), DataType::Timestamp(TimeUnit::Microsecond, _)))?;
    let ts_array = batch.column(ts_idx).as_any().downcast_ref::<TimestampMicrosecondArray>()?;
    Some((arrow::compute::min(ts_array)?, arrow::compute::max(ts_array)?))
}

/// Composite (project_id, table_name) key for flattened table lookup.
pub type TableKey = (Arc<str>, Arc<str>);

#[inline]
pub fn table_key(project_id: &str, table_name: &str) -> TableKey {
    (Arc::from(project_id), Arc::from(table_name))
}

pub struct MemBuffer {
    tables: DashMap<TableKey, Arc<TableBuffer>>,
    /// Running total of in-memory bytes across all live buckets. Maintained by
    /// DELTA at every site that changes a bucket's `memory_bytes`, so it must
    /// stay in step with the sum of those atomics; `TableBuffer::insert_batch`
    /// folds its push and its coalesce shrinkage into one net delta — never
    /// re-derive it. Every subtraction must go through [`apply_signed_delta`]:
    /// a raw `fetch_sub` underflow wraps and rejects every insert forever.
    estimated_bytes: AtomicUsize,
    /// Mirrors `WalManager::shards_per_topic` so `FlushableBucket.wal_first_positions`
    /// is always sized correctly when snapshotted at seal time.
    shards_per_topic: usize,
    /// LRU cache of per-bucket tantivy indexes; global (not per-TimeBucket) so
    /// the byte budget has one view. Entries are dropped over budget and
    /// whenever their bucket is inserted into / drained / evicted.
    text_index_cache: parking_lot::Mutex<lru::LruCache<BucketCacheKey, Arc<crate::tantivy::BucketTextIndex>>>,
    /// Sum of `size_bytes` across cached entries. Atomic so the hot insert path
    /// can check "over budget?" without taking the LRU mutex.
    text_index_bytes: AtomicUsize,
    /// Soft budget for cached text indexes (bytes), auto-tuned from
    /// `buffer_max_memory_mb` at construction.
    text_index_max_bytes: usize,
    /// (project_id, table_name) → bucket_ids whose rows were force-flushed to
    /// Delta while the bucket was open. Such a window holds rows legitimately
    /// in *both* stores (disjoint sets), so it stays exempt from the Delta-scan
    /// exclusion for its whole lifetime. Kept here, not on the bucket, so the
    /// mark survives empty-bucket reclaim and re-creation. Pruned on
    /// drain/eviction.
    force_flushed: DashMap<TableKey, std::collections::HashSet<i64>>,
    /// Per (table, bucket): the highest row timestamp that bucket has ever
    /// handed to Delta; `get_bucket_ranges` floors its mask just above it.
    /// Lives here, not on the bucket, because both flush paths delete a bucket
    /// the moment it empties and the next insert recreates it under the same
    /// id. It cannot be folded into `min_timestamp` (which `fetch_min`s): one
    /// later row would pull the mask back over rows that left memory.
    /// Bounded by `evict_old_data`, which drops an entry with its bucket.
    flushed_max: DashMap<TableKey, std::collections::HashMap<i64, i64>>,
    /// GC-floor pins of buckets mid-take, keyed by `taking_seq`.
    /// `take_bucket_for_flush` removes the bucket before the flush path
    /// registers its inflight pin; without this pin a GC sweep in that gap
    /// could delete the airborne bucket's WAL file. Held from before the
    /// removal until [`Self::release_taking_pin`].
    taking_pins: DashMap<u64, i64>,
    taking_seq: AtomicU64,
    /// WAL-replay DML entries consumed as no-ops because their table had no
    /// buffered rows. Surfaced in `timefusion_stats`; growth is the re-drive
    /// signal.
    replay_dml_noops: AtomicU64,
}

/// Cache key: (project_id, table_name, bucket_id).
pub type BucketCacheKey = (Arc<str>, Arc<str>, i64);

pub struct TableBuffer {
    buckets: DashMap<i64, TimeBucket>,
    schema: SchemaRef, // Immutable after creation - no lock needed
    /// Declared (YAML) schema, resolved once at construction. `None` for tables
    /// with no registry entry, which keeps the incoming batch authoritative.
    declared: Option<SchemaRef>,
    project_id: Arc<str>,
    table_name: Arc<str>,
}

pub struct TimeBucket {
    batches: Mutex<Vec<RecordBatch>>,
    row_count: AtomicUsize,
    memory_bytes: AtomicUsize,
    min_timestamp: AtomicI64,
    max_timestamp: AtomicI64,
    /// Span of the row `timestamp` values actually held, used ONLY for read
    /// pruning. Deliberately separate from `min_timestamp`/`max_timestamp`,
    /// which track the ROUTING timestamp and drive the Delta merge-on-read
    /// mask: widening those to the true row span masks Delta rows sharing a
    /// timestamp with buffered ones, losing rows. Sentinels (`MAX`/`MIN`) mean
    /// "unknown" and fall back to the routing span.
    row_min_ts: AtomicI64,
    row_max_ts: AtomicI64,
    /// Wall-clock micros (via `crate::support`) of bucket creation. Drives the
    /// flush-dwell staleness signal; independent of event time so backfills
    /// can't false-trip the "oldest bucket" alarm.
    created_micros: i64,
    /// Per-shard walrus positions captured BEFORE this bucket's first WAL entry
    /// (min-merged) — the bucket's read-cursor *holds*: while the bucket is
    /// unflushed the cursor must not advance past `first_positions[shard]`, or
    /// a crash replays past acked entries.
    wal_shard_state: Mutex<WalShardState>,
    /// While a flush snapshot is airborne, the first N batches are the
    /// snapshot's prefix: insert-time coalesce must not fold across this
    /// boundary or the post-commit prefix drain would remove late (unflushed)
    /// rows merged into a combined batch. 0 = no snapshot in flight.
    flush_pinned_prefix: AtomicUsize,
    /// Bumped by every in-place DML mutation of this bucket's batches (under
    /// the batches lock). A flush snapshot captures it; if it moved by commit
    /// time the commit landed pre-DML values and prefix indices may have
    /// shifted, so `finish_flushed_snapshot` must NOT drain.
    mutation_gen: AtomicU64,
    /// Wall-clock micros of the newest WAL entry pinned here (append time, so
    /// ARRIVAL time). Drives [`MemBuffer::reap_expired_empty_buckets`]'s grace
    /// period.
    last_wal_pin_micros: AtomicI64,
    /// Real-clock micros (chrono, not `crate::support` — compared to file
    /// mtimes) of the OLDEST WAL append this bucket's un-flushed data may
    /// depend on: the WAL GC floor. Event time is deliberately NOT used, since
    /// a backfill of old events would drag the floor days back.
    first_wal_pin_micros: AtomicI64,
}

/// Decode the `i64::MAX = no pin` sentinel used by `first_wal_pin_micros`.
pub(crate) fn pin_opt(v: i64) -> Option<i64> {
    (v != i64::MAX).then_some(v)
}

#[derive(Debug, Default, Clone)]
struct WalShardState {
    first_positions: Vec<Option<walrus_rust::WalPosition>>,
}

impl WalShardState {
    /// The hold is a *floor*: the earliest position ever seen for a shard wins.
    fn merge(&mut self, shard: usize, pos: walrus_rust::WalPosition) {
        if self.first_positions.len() <= shard {
            self.first_positions.resize(shard + 1, None);
        }
        self.first_positions[shard] = Some(self.first_positions[shard].map_or(pos, |prev| prev.min(pos)));
    }
}

/// Per-shard holds re-shaped to exactly `shards` entries (truncating extras,
/// padding with `None`) so watermark indices always line up.
fn pad_positions(src: &[Option<walrus_rust::WalPosition>], shards: usize) -> Vec<Option<walrus_rust::WalPosition>> {
    (0..shards).map(|i| src.get(i).copied().flatten()).collect()
}

#[derive(Debug, Clone)]
pub struct FlushableBucket {
    pub project_id: String,
    pub table_name: String,
    pub bucket_id: i64,
    pub batches: Vec<RecordBatch>,
    pub row_count: usize,
    /// Per-shard positions BEFORE this bucket's first WAL entry — the bucket's
    /// read-cursor holds. Registered as in-flight holds while the flush is
    /// airborne; restored to the bucket if the Delta commit fails.
    pub wal_first_positions: Vec<Option<walrus_rust::WalPosition>>,
    /// `mutation_gen` at snapshot time (snapshot-flush path only). If it moved
    /// by commit time a DML mutated the bucket mid-flight, so
    /// `finish_flushed_snapshot` must keep the rows and re-flush.
    pub snapshot_gen: u64,
    /// True min/max of the taken rows, captured before the source bucket's
    /// atomics were reset, so `restore_taken_bucket` can replay them.
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    /// Source bucket's `first_wal_pin_micros` (WAL GC floor), carried so an
    /// airborne take keeps flooring GC and a failed commit can re-apply it.
    pub first_wal_pin_micros: i64,
    /// Key of this take's entry in `MemBuffer::taking_pins`; released via
    /// [`MemBuffer::release_taking_pin`] once the inflight pin is registered.
    pub taking_pin_seq: u64,
}

#[derive(Debug, Default)]
pub struct MemBufferStats {
    pub project_count: usize,
    pub total_buckets: usize,
    pub total_rows: usize,
    pub total_batches: usize,
    pub estimated_memory_bytes: usize,
    /// See `MemBuffer::replay_dml_noops` — growth means buffered DML was
    /// consumed without applying.
    pub replay_dml_noops: u64,
    /// Creation wall-clock micros of the oldest non-empty already-flushable
    /// bucket (`bucket_id < current`). A flush-DWELL staleness signal,
    /// deliberately NOT the rows' event-time min.
    pub oldest_bucket_micros: Option<i64>,
}

/// Drift (% of the recomputed truth) [`MemBuffer::reconcile_estimated_bytes`]
/// tolerates silently; above it is an accounting bug and is warned.
const MAX_TOLERATED_DRIFT_PCT: usize = 2;

/// Per-batch fixed overhead: RecordBatch struct, schema Arc bump, and ArrayData
/// metadata per column (ArrayData + Buffer headers).
const BATCH_FIXED_OVERHEAD: usize = 64;
const PER_COLUMN_OVERHEAD: usize = 96;

/// Apply a signed byte delta to a memory counter, **saturating at 0**. Never
/// `fetch_sub`: an underflow wraps the `usize` and, on
/// `MemBuffer::estimated_bytes`, permanently rejects every insert.
fn apply_signed_delta(counter: &AtomicUsize, delta: i64) {
    if delta > 0 {
        counter.fetch_add(delta as usize, Ordering::Relaxed);
    } else if delta < 0 {
        sub_saturating(counter, delta.unsigned_abs() as usize);
    }
}

/// `counter -= n`, floored at 0; returns the amount ACTUALLY subtracted.
/// Callers mirroring a bucket-level subtraction onto
/// `MemBuffer::estimated_bytes` must forward this value, not `n`.
fn sub_saturating(counter: &AtomicUsize, n: usize) -> usize {
    counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(n))).map_or(0, |prev| prev.min(n))
}

pub fn estimate_batch_size(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size() + BATCH_FIXED_OVERHEAD + batch.num_columns() * PER_COLUMN_OVERHEAD
}

/// Compact string/binary-view arrays whose buffers dwarf the bytes the views
/// reference (builder slack, or views inherited over a parquet reader's whole
/// column chunk — hence capacity vs `total_buffer_bytes_used`, which catches
/// both). Already-compact arrays pass through as Arc clones.
fn compact_view_arrays(arr: &ArrayRef) -> ArrayRef {
    use arrow::{
        array::{Array, FixedSizeListArray, GenericByteViewArray, GenericListArray, OffsetSizeTrait, StructArray},
        datatypes::{BinaryViewType, ByteViewType, StringViewType},
    };
    fn wasteful(buffers: &[arrow::buffer::Buffer], used: usize) -> bool {
        buffers.iter().map(|b| b.capacity()).sum::<usize>() > used * 2 + 1024
    }
    fn gc_view<T: ByteViewType + ?Sized>(arr: &ArrayRef) -> ArrayRef {
        let v = arr.as_any().downcast_ref::<GenericByteViewArray<T>>().unwrap();
        if wasteful(v.data_buffers(), v.total_buffer_bytes_used()) { Arc::new(v.gc()) } else { arr.clone() }
    }
    /// Rebuild through `make` only when compacting actually replaced the child values.
    fn remap(arr: &ArrayRef, vals: &ArrayRef, make: impl FnOnce(ArrayRef) -> ArrayRef) -> ArrayRef {
        let compacted = compact_view_arrays(vals);
        if Arc::ptr_eq(&compacted, vals) { arr.clone() } else { make(compacted) }
    }
    fn compact_list<O: OffsetSizeTrait>(arr: &ArrayRef, field: &FieldRef) -> ArrayRef {
        let l = arr.as_any().downcast_ref::<GenericListArray<O>>().unwrap();
        remap(arr, l.values(), |vals| Arc::new(GenericListArray::<O>::new(field.clone(), l.offsets().clone(), vals, l.nulls().cloned())))
    }
    match arr.data_type() {
        DataType::Utf8View => gc_view::<StringViewType>(arr),
        DataType::BinaryView => gc_view::<BinaryViewType>(arr),
        DataType::List(f) => compact_list::<i32>(arr, f),
        DataType::LargeList(f) => compact_list::<i64>(arr, f),
        DataType::FixedSizeList(f, size) => {
            let l = arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            remap(arr, l.values(), |vals| Arc::new(FixedSizeListArray::new(f.clone(), *size, vals, l.nulls().cloned())))
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
/// buffers are mostly someone else's bytes — Arrow IPC decode hands every
/// column a slice of one whole-message allocation, so a replayed batch would
/// otherwise be charged ~n_cols × message size.
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

/// View gc then buffer privatization. Runs at every bucket insert and before
/// WAL serialization so neither memory accounting nor WAL entries carry other
/// allocations' bytes.
pub fn compact_batch(batch: RecordBatch) -> RecordBatch {
    let cols: Vec<ArrayRef> = batch.columns().iter().map(|c| privatize_sliced(&compact_view_arrays(c))).collect();
    if cols.iter().zip(batch.columns()).all(|(a, b)| Arc::ptr_eq(a, b)) { batch } else { RecordBatch::try_new(batch.schema(), cols).unwrap_or(batch) }
}

/// Collapse rows to one per unique value of `keys`, preserving order. Empty
/// `keys` or empty input → no-op. Only collapses dupes inside THIS call's
/// input — cross-bucket dupes need the read-side dedup (`DedupExec`).
///
/// `tiebreak`: among rows sharing a key, the greatest value in this column
/// wins; ties → last occurrence. Nulls sort lowest. `None` = keep-last.
///
/// `drop_tombstones`: pass the schema's `tombstone_column` **only when the
/// caller guarantees no older version of any key survives outside this call's
/// input** — dropping early is silent data resurrection. `None` retains the
/// tombstone row; when unsure, pass `None`.
pub fn dedup_batches(batches: Vec<RecordBatch>, keys: &[String], tiebreak: Option<&str>, drop_tombstones: Option<&str>) -> anyhow::Result<Vec<RecordBatch>> {
    if keys.is_empty() || batches.is_empty() {
        return Ok(batches);
    }
    // Concatenate ONLY the key columns — never the full batches: a large flush
    // exceeds Arrow's 2GB i32 string-offset limit and `concat_batches` fails
    // with "Offset overflow". Superseded rows are dropped by filtering each
    // batch in place, keeping every output array bounded by its source batch.
    let concat_col = |name: &str| -> anyhow::Result<ArrayRef> {
        let cols = batches
            .iter()
            .map(|b| b.column_by_name(name).cloned().ok_or_else(|| anyhow::anyhow!("dedup column `{name}` missing from batch schema")))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(concat(&cols.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?)
    };
    let rows_of = |arrays: Vec<ArrayRef>| -> anyhow::Result<arrow::row::Rows> {
        let converter = RowConverter::new(arrays.iter().map(|a| SortField::new(a.data_type().clone())).collect())?;
        Ok(converter.convert_columns(&arrays)?)
    };
    let rows = rows_of(keys.iter().map(|k| concat_col(k)).collect::<anyhow::Result<_>>()?)?;

    // A MISSING tiebreak degrades to "no tiebreak" rather than failing: rows
    // buffered under an older schema lack a newly-enabled version column, and
    // erroring would make those buckets permanently unflushable. Missing dedup
    // KEYS stay fatal — that is a real schema fault.
    let has_tiebreak = |col: &str| batches.iter().all(|b| b.column_by_name(col).is_some());
    let tb_rows = tiebreak.filter(|col| has_tiebreak(col)).map(|col| rows_of(vec![concat_col(col)?])).transpose()?;

    // One surviving index per key: greatest tiebreak wins, ties → last
    // occurrence. Keys are BORROWED `Row<'_>` so there is no per-row allocation.
    let mut chosen: std::collections::HashMap<arrow::row::Row<'_>, u32, ahash::RandomState> =
        std::collections::HashMap::with_capacity_and_hasher(rows.num_rows(), ahash::RandomState::new());
    for i in 0..rows.num_rows() {
        let k = rows.row(i);
        match (&tb_rows, chosen.get(&k)) {
            (Some(tb), Some(&j)) if tb.row(i) < tb.row(j as usize) => {}
            _ => {
                chosen.insert(k, i as u32);
            }
        }
    }
    // Loaded before the no-duplicates fast path: a tombstone with no surviving
    // older version in this input is still a row to drop.
    let tombstones = match drop_tombstones {
        Some(col) => {
            let arr = concat_col(col)?;
            let flags = arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("tombstone column `{col}` must be Boolean, got {}", arr.data_type()))?
                .clone();
            (flags.true_count() > 0).then_some(flags)
        }
        None => None,
    };
    if chosen.len() == rows.num_rows() && tombstones.is_none() {
        return Ok(batches);
    }
    // NULL and false both mean live; only `true` retires the key.
    let mut keep = vec![false; rows.num_rows()];
    for i in chosen.into_values().filter(|&i| tombstones.as_ref().is_none_or(|f| !(f.is_valid(i as usize) && f.value(i as usize)))) {
        keep[i as usize] = true;
    }
    // `scan` carries the running base offset into the global row-index space.
    Ok(batches
        .iter()
        .scan(0u32, |base, b| {
            let n = b.num_rows() as u32;
            Some((b, std::mem::replace(base, *base + n), n))
        })
        .map(|(b, base, n)| filter_record_batch(b, &BooleanArray::from_iter((0..n).map(|r| Some(keep[(base + r) as usize])))))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|kept| kept.num_rows() > 0)
        .collect())
}

fn merge_arrays(original: &ArrayRef, new_values: &ArrayRef, mask: &BooleanArray) -> DFResult<ArrayRef> {
    let new_values = if original.data_type() != new_values.data_type() {
        arrow::compute::cast(new_values, original.data_type()).map_err(arrow_err)?
    } else {
        new_values.clone()
    };
    arrow::compute::kernels::zip::zip(mask, &new_values, original).map_err(arrow_err)
}

fn eval_bool_mask(pred: &Arc<dyn datafusion::physical_expr::PhysicalExpr>, batch: &RecordBatch) -> DFResult<BooleanArray> {
    pred.evaluate(batch)?
        .into_array(batch.num_rows())?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .ok_or_else(|| datafusion::error::DataFusionError::Execution("Predicate did not return boolean".into()))
}

/// `None` matches every row.
fn eval_bool_mask_or_all(pred: Option<&Arc<dyn datafusion::physical_expr::PhysicalExpr>>, batch: &RecordBatch) -> DFResult<BooleanArray> {
    pred.map_or_else(|| Ok(BooleanArray::from(vec![true; batch.num_rows()])), |p| eval_bool_mask(p, batch))
}

/// `schema` resolves column refs (must be non-empty for refs nested inside
/// function args); `registry` resolves UDFs and is required for any call.
fn parse_sql_predicate(sql: &str, schema: &DFSchema, registry: Option<&FnRegistry>) -> DFResult<Expr> {
    let sql_err = |e: datafusion::sql::sqlparser::parser::ParserError| datafusion::error::DataFusionError::SQL(e.into(), None);
    let sql_expr = SqlParser::new(&GenericDialect {}).try_with_sql(sql).map_err(sql_err)?.parse_expr().map_err(sql_err)?;
    // Same expr planners, in the same order, as the live session — otherwise
    // DML SQL using `->`/`->>`, array literals etc. fails to re-parse on replay.
    let expr_planners: Vec<Arc<dyn datafusion::logical_expr::planner::ExprPlanner>> =
        std::iter::once(Arc::new(crate::read::functions::VariantAwareExprPlanner) as Arc<dyn datafusion::logical_expr::planner::ExprPlanner>)
            .chain(datafusion::execution::SessionStateDefaults::default_expr_planners())
            .collect();
    let context_provider = RegistryContextProvider { registry, expr_planners };
    let planner = SqlToRel::new(&context_provider);
    let expr = planner.sql_to_expr(sql_expr, schema, &mut Default::default())?;
    // Comparison kernels do not auto-coerce mismatched string views, so apply
    // the analyzer's type coercion before lowering to a physical expr.
    Ok(expr.rewrite(&mut datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter::new(schema))?.data)
}

/// Parse `(column, value-SQL)` assignment pairs against `schema`.
fn parse_sql_assignments(assignments: &[(String, String)], schema: &DFSchema, registry: Option<&FnRegistry>) -> DFResult<Vec<(String, Expr)>> {
    assignments.iter().map(|(col, sql)| parse_sql_predicate(sql, schema, registry).map(|expr| (col.clone(), expr))).collect()
}

struct RegistryContextProvider<'a> {
    registry: Option<&'a FnRegistry>,
    expr_planners: Vec<Arc<dyn datafusion::logical_expr::planner::ExprPlanner>>,
}

impl datafusion::sql::planner::ContextProvider for RegistryContextProvider<'_> {
    fn get_table_source(&self, _: datafusion::sql::TableReference) -> DFResult<std::sync::Arc<dyn datafusion::logical_expr::TableSource>> {
        Err(datafusion::error::DataFusionError::Plan("No table context".into()))
    }
    fn get_function_meta(&self, name: &str) -> Option<std::sync::Arc<datafusion::logical_expr::ScalarUDF>> {
        self.registry?.udf(name).ok()
    }
    fn get_expr_planners(&self) -> &[Arc<dyn datafusion::logical_expr::planner::ExprPlanner>] {
        &self.expr_planners
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
    use datafusion::{
        logical_expr::{BinaryExpr, Operator},
        scalar::ScalarValue as SV,
    };
    filters.iter().fold((None, None), |bounds: (Option<i64>, Option<i64>), filter| {
        let (min_ts, max_ts) = bounds;
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter else { return bounds };
        if !matches!(left.as_ref(), Expr::Column(c) if c.name == "timestamp") {
            return bounds;
        }
        let Expr::Literal(lit, _) = right.as_ref() else { return bounds };
        let Some(ts) = (match lit {
            SV::TimestampMicrosecond(Some(t), _) => Some(*t),
            SV::TimestampNanosecond(Some(t), _) => Some(*t / 1000),
            SV::TimestampMillisecond(Some(t), _) => Some(*t * 1000),
            SV::TimestampSecond(Some(t), _) => Some(*t * 1_000_000),
            _ => None,
        }) else {
            return bounds;
        };
        match op {
            Operator::Gt | Operator::GtEq => (Some(min_ts.map_or(ts, |m: i64| m.max(ts))), max_ts),
            Operator::Lt | Operator::LtEq => (min_ts, Some(max_ts.map_or(ts, |m: i64| m.min(ts)))),
            Operator::Eq => (Some(ts), Some(ts)),
            _ => bounds,
        }
    })
}

/// Compile filters into a single conjunction physical expression evaluated against `schema`.
pub fn compile_filter_conjunction(filters: &[Expr], schema: &SchemaRef) -> DFResult<Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>> {
    let Some(conjunction) = filters.iter().cloned().reduce(datafusion::logical_expr::and) else { return Ok(None) };
    let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
    Ok(Some(create_physical_expr(&conjunction, &df_schema, &ExecutionProps::new())?))
}

/// Filter a batch to rows whose `id` is in `ids` (Utf8View/Utf8/LargeUtf8).
/// On any error the batch is returned UNFILTERED — the caller's
/// predicate-based filter must catch the over-inclusion.
fn filter_batch_by_id_set(batch: &RecordBatch, ids: &std::collections::HashSet<String>) -> RecordBatch {
    use arrow::array::{AsArray, BooleanArray};
    let Some(arr) = batch.column_by_name("id") else { return batch.clone() };
    let hit = |v: Option<&str>| v.is_some_and(|s| ids.contains(s));
    let mask: BooleanArray = match arr.data_type() {
        DataType::Utf8View => arr.as_string_view().iter().map(hit).collect(),
        DataType::Utf8 => arr.as_string::<i32>().iter().map(hit).collect(),
        DataType::LargeUtf8 => arr.as_string::<i64>().iter().map(hit).collect(),
        _ => return batch.clone(),
    };
    filter_record_batch(batch, &mask).unwrap_or_else(|_| batch.clone())
}

/// Drop non-matching rows and any batch that ends up empty; `None` is a no-op.
/// Best-effort: on any evaluation error the batch is kept UNFILTERED, so
/// DataFusion's FilterExec must still be in the plan.
pub fn filter_snapshot(snapshot: Vec<RecordBatch>, pred: &Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>) -> Vec<RecordBatch> {
    let Some(p) = pred else { return snapshot };
    snapshot
        .iter()
        .map(|b| eval_bool_mask(p, b).ok().and_then(|mask| filter_record_batch(b, &mask).ok()).unwrap_or_else(|| b.clone()))
        .filter(|b| b.num_rows() > 0)
        .collect()
}

/// One partition per surviving time bucket. `sorted` = EVERY partition is
/// ordered by the table's declared `sorting_columns`; it is a correctness
/// claim, not a hint, and only then may the caller declare that ordering.
#[derive(Debug, Default)]
pub struct MemLeg {
    pub partitions: Vec<Vec<RecordBatch>>,
    pub sorted: bool,
}

/// Rows and Delta exclusions captured together for merge-on-read aggregation.
/// Each bucket is captured under its own batch lock — this is NOT a transaction
/// across buckets. Capture it before the Delta snapshot, or a completed flush
/// can remove a row from both legs of the resulting read view.
#[derive(Debug, Default)]
pub struct MemSnapshot {
    pub batches: Vec<RecordBatch>,
    pub covered_ranges: Vec<(i64, i64)>,
}

/// Sort one partition by `schema`'s declared `sorting_columns`, or `None` when
/// that cannot be done truthfully. Only the leading RUN of present sorting
/// columns is used and the first is mandatory: the caller declares that first
/// column, so sorting by a later one would make the declaration a lie — a
/// wrong-results bug, not a slow plan.
pub fn sort_partition(schema: &crate::schema::TableSchema, batches: Vec<RecordBatch>) -> Option<Vec<RecordBatch>> {
    use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take_record_batch};
    if batches.is_empty() {
        return None;
    }
    let arrow_schema = batches[0].schema();
    // A schema-diverse partition is left alone; undeclared ordering is safe.
    if batches.iter().any(|b| b.schema() != arrow_schema) {
        return None;
    }
    let spec: Vec<(usize, SortOptions)> = schema
        .sorting_columns
        .iter()
        .map_while(|sc| {
            let idx = arrow_schema.index_of(&sc.name).ok()?;
            Some((idx, SortOptions { descending: sc.descending, nulls_first: sc.nulls_first }))
        })
        .collect();
    if spec.is_empty() {
        return None;
    }
    let combined = match batches.len() {
        1 => batches.into_iter().next().unwrap(),
        _ => concat_batches(&arrow_schema, &batches).ok()?,
    };
    let sort_cols: Vec<SortColumn> = spec.into_iter().map(|(i, options)| SortColumn { values: combined.column(i).clone(), options: Some(options) }).collect();
    let indices = lexsort_to_indices(&sort_cols, None).ok()?;
    let already_ordered = indices.values().iter().enumerate().all(|(i, &v)| v as usize == i);
    let sorted = if already_ordered { combined } else { take_record_batch(&combined, &indices).ok()? };
    // Hand back BATCHES, not the concatenated monolith, so nothing downstream
    // holds the whole partition as one value. Slicing is zero-copy.
    Some((0..sorted.num_rows()).step_by(SORT_CHUNK_ROWS).map(|off| sorted.slice(off, SORT_CHUNK_ROWS.min(sorted.num_rows() - off))).collect())
}

/// Rows per batch handed back by [`sort_partition`]; not load-bearing.
const SORT_CHUNK_ROWS: usize = 8192;

/// The DISTINCT bucket ids `batch`'s rows land in, keyed off `time_col`.
/// Empty when the column is absent or is not an i64-backed time type, so only
/// callers that treat "unknown" as "do not exempt" may use this.
pub fn batch_bucket_ids(batch: &RecordBatch, time_col: &str) -> Vec<i64> {
    let Some(col) = batch.column_by_name(time_col) else { return Vec::new() };
    let Some(values) = crate::read::bound_slice(col) else { return Vec::new() };
    values.iter().map(|&t| MemBuffer::compute_bucket_id(t)).sorted_unstable().dedup().collect()
}

/// Half-open `[start, end)` overlap — the ONE range convention shared by
/// bucket ranges, hot-tier file ranges, and the Delta exclusion filters.
pub fn overlaps(a: (i64, i64), b: (i64, i64)) -> bool {
    a.0 < b.1 && b.0 < a.1
}

/// Sort + coalesce touching or overlapping half-open ranges, collapsing the
/// Delta leg's per-bucket `(ts < a OR ts >= b)` exclusion conjuncts.
pub fn merge_ranges(ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    ranges.into_iter().sorted_unstable().coalesce(|a, b| if b.0 <= a.1 { Ok((a.0, a.1.max(b.1))) } else { Err((a, b)) }).collect()
}

/// Does a bucket's time range overlap the query range? Sentinels (empty
/// bucket) mean "unknown range" and never prune.
fn bucket_overlaps_range(bucket: &TimeBucket, (min_filter, max_filter): &(Option<i64>, Option<i64>)) -> bool {
    let or_routing = |row: &AtomicI64, routing: &AtomicI64, sentinel: i64| match row.load(Ordering::Relaxed) {
        v if v == sentinel => routing.load(Ordering::Relaxed),
        v => v,
    };
    let lo = or_routing(&bucket.row_min_ts, &bucket.min_timestamp, i64::MAX);
    let hi = or_routing(&bucket.row_max_ts, &bucket.max_timestamp, i64::MIN);
    let starts_after = |max: i64| lo != i64::MAX && lo > max;
    let ends_before = |min: i64| hi != i64::MIN && hi < min;
    !max_filter.is_some_and(starts_after) && !min_filter.is_some_and(ends_before)
}

/// Does the YAML schema declare any tantivy-indexed field? Gates every
/// text-match path; without one the caller falls back to the SQL predicate.
fn has_indexed_fields(schema: &crate::schema::TableSchema) -> bool {
    schema.fields.iter().any(|f| f.tantivy.as_ref().is_some_and(|t| t.indexed))
}

/// Strip table qualifiers from Column refs so exprs from SQL planning resolve
/// against the bare-column DFSchema built from the in-memory table.
pub(crate) fn strip_column_qualifiers(expr: Expr) -> DFResult<Expr> {
    expr.transform(|e| match &e {
        Expr::Column(col) => Ok(datafusion::common::tree_node::Transformed::yes(Expr::Column(Column::from_name(&col.name)))),
        _ => Ok(datafusion::common::tree_node::Transformed::no(e)),
    })
    .map(|t| t.data)
}

/// The window one bucket is authoritative over — what the Delta scan must
/// exclude: its published span, clamped to `bounds`, lifted strictly above
/// `flushed_through` (read AFTER the span, as both call sites did). Never masks
/// an instant this bucket already committed: those rows are in Delta and gone
/// from memory, so masking them would make them answer no query at all.
fn authority_range(bucket: &TimeBucket, bounds: Option<(i64, i64)>, flushed_through: impl FnOnce() -> Option<i64>) -> Option<(i64, i64)> {
    let (lo, hi) = bounds.unwrap_or((i64::MIN, i64::MAX));
    let min = bucket.min_timestamp.load(Ordering::Relaxed).max(lo);
    let max = bucket.max_timestamp.load(Ordering::Relaxed).min(hi);
    let min = flushed_through().map_or(min, |through| min.max(through.saturating_add(1)));
    (min <= max).then_some((min, max + 1))
}

impl MemBuffer {
    pub fn new() -> Self {
        Self::new_with_max_index_bytes_and_shards(128 * 1024 * 1024, 4)
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
            flushed_max: DashMap::new(),
            taking_pins: DashMap::new(),
            taking_seq: AtomicU64::new(0),
            replay_dml_noops: AtomicU64::new(0),
        }
    }

    /// Record that everything up to `ts` in this bucket is now Delta's.
    /// Monotonic on purpose: never lower the floor.
    fn note_flushed_through(&self, key: TableKey, bucket_id: i64, ts: i64) {
        self.flushed_max.entry(key).or_default().entry(bucket_id).and_modify(|v| *v = (*v).max(ts)).or_insert(ts);
    }

    /// Record that `bucket_id`'s rows were committed to Delta while the bucket
    /// was still open (see `force_flushed`). Must be called BEFORE the commit
    /// so no query can race into the masked window.
    pub fn mark_force_flushed(&self, project_id: &str, table_name: &str, bucket_id: i64) {
        self.force_flushed.entry(table_key(project_id, table_name)).or_default().insert(bucket_id);
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

    fn cache_key(project_id: &str, table_name: &str, bucket_id: i64) -> BucketCacheKey {
        (Arc::from(project_id), Arc::from(table_name), bucket_id)
    }

    fn cache_get(&self, key: &BucketCacheKey) -> Option<Arc<crate::tantivy::BucketTextIndex>> {
        self.text_index_cache.lock().get(key).cloned()
    }

    /// Insert an index, evicting LRU entries to stay under
    /// `text_index_max_bytes`. Returns the inserted Arc.
    fn cache_put(&self, key: BucketCacheKey, idx: Arc<crate::tantivy::BucketTextIndex>) -> Arc<crate::tantivy::BucketTextIndex> {
        let size = idx.size_bytes;
        let mut cache = self.text_index_cache.lock();
        if let Some(old) = cache.put(key, idx.clone()) {
            self.text_index_bytes.fetch_sub(old.size_bytes, Ordering::Relaxed);
        }
        self.text_index_bytes.fetch_add(size, Ordering::Relaxed);
        while self.text_index_bytes.load(Ordering::Relaxed) > self.text_index_max_bytes {
            let Some((_, evicted)) = cache.pop_lru() else { break };
            self.text_index_bytes.fetch_sub(evicted.size_bytes, Ordering::Relaxed);
        }
        idx
    }

    /// Drop a bucket's cached index; must follow every mutation of the bucket.
    fn cache_invalidate(&self, key: &BucketCacheKey) {
        if let Some(old) = self.text_index_cache.lock().pop(key) {
            self.text_index_bytes.fetch_sub(old.size_bytes, Ordering::Relaxed);
        }
    }

    /// Live MemBuffer size in bytes — one relaxed load of the running total.
    /// Deliberately NOT a sum over the bucket atomics: `try_reserve_memory`
    /// calls this once per CAS attempt.
    pub fn estimated_memory_bytes(&self) -> usize {
        self.estimated_bytes.load(Ordering::Relaxed)
    }

    /// Authoritative sum of every live bucket's `memory_bytes`. O(tables ×
    /// buckets) with a shard lock per table — NOT for the hot path; use
    /// [`Self::estimated_memory_bytes`] there.
    pub(crate) fn recompute_memory_bytes(&self) -> usize {
        self.tables.iter().map(|t| t.value().buckets.iter().map(|b| b.value().memory_bytes.load(Ordering::Relaxed)).sum::<usize>()).sum()
    }

    /// Periodic drift correction for [`Self::estimated_bytes`]: recompute the
    /// authoritative sum, store it, and warn past [`MAX_TOLERATED_DRIFT_PCT`].
    /// Returns `(cached_before, truth)`. The warn is a bug detector — drift
    /// means some `memory_bytes` mutation site is not reporting its delta.
    pub fn reconcile_estimated_bytes(&self) -> (usize, usize) {
        let truth = self.recompute_memory_bytes();
        let cached = self.estimated_bytes.swap(truth, Ordering::Relaxed);
        let drift = cached.abs_diff(truth);
        if drift > 0 && drift as u128 * 100 > truth.max(1) as u128 * MAX_TOLERATED_DRIFT_PCT as u128 {
            warn!(
                target = "mem_buffer",
                cached_bytes = cached,
                recomputed_bytes = truth,
                drift_bytes = drift,
                "MemBuffer estimated_bytes drifted beyond tolerance — a bucket memory_bytes mutation site is not reporting its delta (see MemBuffer::estimated_bytes)"
            );
        }
        (cached, truth)
    }

    pub fn compute_bucket_id(timestamp_micros: i64) -> i64 {
        timestamp_micros / bucket_duration_micros()
    }

    pub fn current_bucket_id() -> i64 {
        Self::compute_bucket_id(crate::support::now_micros())
    }

    /// Callers doing many inserts should cache the returned Arc.
    pub fn get_or_create_table(&self, project_id: &str, table_name: &str, schema: &SchemaRef) -> anyhow::Result<Arc<TableBuffer>> {
        let key = table_key(project_id, table_name);
        let ensure_compatible = |existing: SchemaRef| -> anyhow::Result<()> {
            if Arc::ptr_eq(&existing, schema) || schemas_compatible(&existing, schema) {
                return Ok(());
            }
            warn!(
                "Schema incompatible for {}.{}: existing has {} fields, incoming has {}",
                project_id,
                table_name,
                existing.fields().len(),
                schema.fields().len()
            );
            anyhow::bail!("Schema incompatible for {}.{}: field types don't match or new non-nullable field added", project_id, table_name)
        };

        // Bound to a local so the shard Ref is dropped before `entry` below
        // re-locks the same shard.
        let existing = self.tables.get(&key).map(|t| Arc::clone(&t));
        let table = existing.unwrap_or_else(|| {
            let make = || Arc::new(TableBuffer::new(schema.clone(), Arc::from(project_id), Arc::from(table_name)));
            Arc::clone(&self.tables.entry(key).or_insert_with(make))
        });
        // Registered tables canonicalize in `insert_batch`; checking the
        // pre-canonical column order here would reject valid WAL entries.
        if table.declared.is_none() {
            ensure_compatible(table.schema())?;
        }
        Ok(table)
    }

    fn get_table(&self, project_id: &str, table_name: &str) -> Option<Arc<TableBuffer>> {
        self.tables.get(&table_key(project_id, table_name)).map(|t| Arc::clone(&t))
    }

    #[instrument(skip(self, batch), fields(project_id, table_name, rows))]
    pub fn insert(&self, project_id: &str, table_name: &str, batch: RecordBatch, timestamp_micros: i64) -> anyhow::Result<()> {
        self.insert_with_hold(project_id, table_name, batch, timestamp_micros, None)
    }

    /// `wal_hold` = (shard, pre-append position) of the batch's WAL entry,
    /// recorded under the bucket lock so a concurrent take can never separate
    /// rows from their cursor hold. `None` only when the entry needs no pin
    /// (WAL replay pins via [`Self::record_replay_hold`] instead).
    pub fn insert_with_hold(
        &self, project_id: &str, table_name: &str, batch: RecordBatch, timestamp_micros: i64, wal_hold: Option<(usize, walrus_rust::WalPosition)>,
    ) -> anyhow::Result<()> {
        let declared = crate::schema::get_schema(table_name).map(|schema| schema.schema_ref());
        let batch = canonicalize_declared_batch(batch, declared.as_ref())?;
        let schema = batch.schema();
        let table = self.get_or_create_table(project_id, table_name, &schema)?;
        let (mem_delta, bucket_id) = table.insert_batch(batch, timestamp_micros, wal_hold)?;
        apply_signed_delta(&self.estimated_bytes, mem_delta);
        self.cache_invalidate(&Self::cache_key(project_id, table_name, bucket_id));
        Ok(())
    }

    /// Pin the bucket owning `timestamp_micros` at a replayed entry's WAL
    /// `(shard, pos)` (min-merged), so replay stays resumable as buckets drain.
    pub fn record_replay_hold(&self, project_id: &str, table_name: &str, timestamp_micros: i64, shard: usize, pos: walrus_rust::WalPosition) {
        let key = table_key(project_id, table_name);
        let Some(table) = self.tables.get(&key) else {
            return;
        };
        let bucket_id = Self::compute_bucket_id(timestamp_micros);
        if let Some(bucket) = table.buckets.get(&bucket_id) {
            bucket.record_wal_append(shard, Some(pos));
            // Must be the entry's ORIGINAL append time; a now-stamp would let
            // GC delete the backing file from under the parked cursor.
            bucket.first_wal_pin_micros.fetch_min(timestamp_micros, Ordering::Relaxed);
        }
    }

    /// Oldest WAL-append real-clock micros any un-flushed bucket may depend
    /// on — the WAL GC floor. `None` when nothing is buffered. Must include
    /// buckets mid-take (`taking_pins`), or a GC sweep racing a take can
    /// delete the bucket's backing file.
    pub fn oldest_wal_append_micros(&self) -> Option<i64> {
        self.tables
            .iter()
            .filter_map(|t| t.buckets.iter().filter_map(|b| pin_opt(b.first_wal_pin_micros.load(Ordering::Relaxed))).min())
            .chain(self.taking_pins.iter().map(|e| *e.value()))
            .min()
    }

    /// Drop a take-in-progress pin once the flush path has registered its own
    /// inflight pin. Always pairs with a successful `take_bucket_for_flush`.
    pub fn release_taking_pin(&self, seq: u64) {
        self.taking_pins.remove(&seq);
    }

    /// Per-shard min of `first_positions` across every live bucket — the
    /// earliest WAL entry still owned by unflushed in-memory data.
    pub fn wal_holds(&self, project_id: &str, table_name: &str, shards_per_topic: usize) -> Vec<Option<walrus_rust::WalPosition>> {
        let Some(table) = self.get_table(project_id, table_name) else { return vec![None; shards_per_topic] };
        table.buckets.iter().fold(vec![None; shards_per_topic], |holds, bucket| {
            holds
                .into_iter()
                .zip(bucket.snapshot_wal_shard_state(shards_per_topic))
                .map(|(held, pos)| match (held, pos) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                })
                .collect()
        })
    }

    /// Project every bucket whose id passes `filter`, across all tables,
    /// through `mk`. The per-table `collect` is load-bearing — the DashMap
    /// refs cannot outlive the closure.
    fn buckets_where<T>(&self, filter: impl Fn(i64) -> bool, mk: impl Fn(&TableKey, i64, &TimeBucket) -> T) -> Vec<T> {
        self.tables
            .iter()
            .flat_map(|t| t.value().buckets.iter().filter(|b| filter(*b.key())).map(|b| mk(t.key(), *b.key(), b.value())).collect::<Vec<_>>())
            .collect()
    }

    /// `(bucket_id, created_micros, memory_bytes)` for every bucket matching
    /// `filter`. The same id can appear once per (project, table).
    pub fn bucket_flush_meta(&self, filter: impl Fn(i64) -> bool) -> Vec<(i64, i64, usize)> {
        self.buckets_where(filter, |_, id, b| (id, b.created_micros, b.memory_bytes.load(Ordering::Relaxed)))
    }

    /// (project_id, table_name, bucket_id) for every bucket whose id passes `filter`.
    pub fn bucket_keys(&self, filter: impl Fn(i64) -> bool) -> Vec<(String, String, i64)> {
        self.buckets_where(filter, |(project_id, table_name), id, _| (project_id.to_string(), table_name.to_string(), id))
    }

    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub fn insert_batches(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>, timestamp_micros: i64) -> anyhow::Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let schema = batches[0].schema();
        let table = self.get_or_create_table(project_id, table_name, &schema)?;

        let inserted: Vec<(i64, i64)> = batches.into_iter().map(|batch| table.insert_batch(batch, timestamp_micros, None)).try_collect()?;
        apply_signed_delta(&self.estimated_bytes, inserted.iter().map(|&(sz, _)| sz).sum());
        for bucket_id in inserted.into_iter().map(|(_, id)| id).unique() {
            self.cache_invalidate(&Self::cache_key(project_id, table_name, bucket_id));
        }
        Ok(())
    }

    /// Search every bucket for rows matching `preds`, building per-bucket
    /// tantivy indexes JIT. `Ok(None)`: no indexed fields → caller MUST fall
    /// back to the original predicate. `Ok(Some(ids))`: union across buckets,
    /// intersected across predicates (AND semantics).
    pub fn search_text_match(
        &self, project_id: &str, table_name: &str, preds: &[crate::tantivy::udf::TextMatchPred],
    ) -> anyhow::Result<Option<std::collections::HashSet<String>>> {
        let Some(node) = crate::tantivy::udf::PredNode::from_preds(preds) else {
            return Ok(None);
        };
        let Some(table_schema) = crate::schema::get_schema(table_name).filter(|s| has_indexed_fields(s)) else {
            return Ok(None);
        };
        let Some(table) = self.get_table(project_id, table_name) else {
            return Ok(None);
        };

        // IDs WITHOUT the matching snapshot: callers must not use these to
        // filter a separately-fetched snapshot (a concurrent insert would be
        // dropped). Use `query_partitioned_with_text_match`, which keeps
        // snapshot + ids atomic per bucket.
        table.buckets.iter().try_fold(None::<std::collections::HashSet<String>>, |acc, entry| {
            let key = Self::cache_key(project_id, table_name, *entry.key());
            let (_snapshot, ids) = self.search_with_snapshot(entry.value(), &key, table_schema, &node)?;
            anyhow::Ok(match (acc, ids) {
                (Some(mut prev), Some(ids)) => {
                    prev.extend(ids);
                    Some(prev)
                }
                (a, None) | (None, a) => a,
            })
        })
    }

    /// Any buffered rows whose timestamps could fall in `[lo, hi]`?
    /// Bucket-granular, so it may report `true` when no row actually falls
    /// inside — the safe direction for callers gating exact-count shortcuts.
    pub fn has_rows_in_range(&self, project_id: &str, table_name: &str, lo: i64, hi: i64) -> bool {
        self.get_table(project_id, table_name).is_some_and(|t| t.buckets.iter().any(|b| Self::live_in_range(b.value(), lo, hi)))
    }

    /// A bucket still holding rows whose span can reach `[lo, hi]`.
    fn live_in_range(bucket: &TimeBucket, lo: i64, hi: i64) -> bool {
        bucket.row_count.load(Ordering::Relaxed) > 0 && bucket_overlaps_range(bucket, &(Some(lo), Some(hi)))
    }

    /// Lower bound on the timestamp of any row still buffered for
    /// `(project, table)` that could fall in `[lo, hi]`, or `None` when nothing
    /// is buffered there.
    ///
    /// Must take the min of the bucket's KEY-derived start AND its published
    /// `min_timestamp`: the latter is stored after `row_count`, so a reader can
    /// see a non-empty bucket whose min has not dropped yet, and using it alone
    /// yields a horizon that is too new. Scoped to `[lo, hi]` so one late
    /// straggler cannot collapse the horizon for every other query.
    pub fn min_buffered_micros(&self, project_id: &str, table_name: &str, lo: i64, hi: i64) -> Option<i64> {
        let table = self.get_table(project_id, table_name)?;
        table
            .buckets
            .iter()
            .filter(|b| Self::live_in_range(b.value(), lo, hi))
            .map(|b| b.key().saturating_mul(bucket_duration_micros()).min(b.value().min_timestamp.load(Ordering::Relaxed)))
            .min()
    }

    /// Query with a text-match prefilter. Per bucket the snapshot and the ID
    /// set are taken under the same `batches` lock, so a concurrent insert
    /// cannot be visible in the data but absent from the IDs. With `node`
    /// `None` or no indexed fields, behaves like `query_partitioned`.
    #[instrument(skip(self, filters, node), fields(project_id, table_name))]
    pub fn query_partitioned_with_text_match(
        &self, project_id: &str, table_name: &str, filters: &[Expr], node: Option<&crate::tantivy::udf::PredNode>,
    ) -> anyhow::Result<MemLeg> {
        self.scan_buckets(project_id, table_name, filters, crate::schema::get_schema(table_name).filter(|s| has_indexed_fields(s)).zip(node))
    }

    /// Flattened [`Self::query_partitioned`] — same rows, bucket partitioning dropped.
    #[instrument(skip(self, filters), fields(project_id, table_name))]
    pub fn query(&self, project_id: &str, table_name: &str, filters: &[Expr]) -> anyhow::Result<Vec<RecordBatch>> {
        Ok(self.query_partitioned(project_id, table_name, filters)?.partitions.into_iter().flatten().collect())
    }

    /// Query and return one partition per time bucket; `filters` also prune
    /// buckets by timestamp.
    #[instrument(skip(self, filters), fields(project_id, table_name))]
    pub fn query_partitioned(&self, project_id: &str, table_name: &str, filters: &[Expr]) -> anyhow::Result<MemLeg> {
        self.scan_buckets(project_id, table_name, filters, None)
    }

    /// Captures complete overlapping buckets and their authority over Delta.
    /// Rows are NOT predicate-filtered: version resolution must see nonmatching
    /// replacements too. Callers apply the half-open window after resolving
    /// versions, and must retain exclusions even when a DELETE emptied a bucket.
    pub fn snapshot_for_merge(&self, project_id: &str, table_name: &str, lo: i64, hi: i64) -> anyhow::Result<MemSnapshot> {
        anyhow::ensure!(lo < hi, "memory snapshot requires a nonempty time window");
        let Some(table) = self.get_table(project_id, table_name) else { return Ok(MemSnapshot::default()) };
        let key = table_key(project_id, table_name);
        let current = Self::current_bucket_id();
        let bucket_ids = table.buckets.iter().map(|bucket| *bucket.key()).sorted_unstable().collect_vec();
        let mut snapshot = MemSnapshot::default();
        for bucket_id in bucket_ids {
            let Some(bucket) = table.buckets.get(&bucket_id) else { continue };
            let batches = bucket.batches.lock();
            if !bucket_overlaps_range(&bucket, &(Some(lo), Some(hi - 1))) {
                continue;
            }
            snapshot.batches.extend(batches.iter().cloned());
            if bucket_id == current || self.force_flushed.get(&key).is_some_and(|set| set.contains(&bucket_id)) {
                continue;
            }
            // Use the authority range, not the surviving rows' range: a DELETE
            // can remove all rows while an older flush is still in flight.
            let through = || self.flushed_max.get(&key).and_then(|map| map.get(&bucket_id).copied());
            snapshot.covered_ranges.extend(authority_range(&bucket, Some((lo, hi - 1)), through));
        }
        snapshot.covered_ranges = merge_ranges(snapshot.covered_ranges);
        Ok(snapshot)
    }

    /// Bucket scan shared by both query entry points: prune by timestamp
    /// range, snapshot each surviving bucket — atomically with its text-match
    /// id set when `text` is given — then apply the compiled predicate.
    fn scan_buckets(
        &self, project_id: &str, table_name: &str, filters: &[Expr], text: Option<(&crate::schema::TableSchema, &crate::tantivy::udf::PredNode)>,
    ) -> anyhow::Result<MemLeg> {
        let ts_range = extract_timestamp_range(filters);
        let Some(table) = self.get_table(project_id, table_name) else { return Ok(MemLeg::default()) };
        // Best-effort: anything that fails to compile is left to the FilterExec above.
        let pred = compile_filter_conjunction(filters, &table.schema).ok().flatten();

        // `sorted_unstable` is eager, so the DashMap iterator is fully consumed
        // and dropped before the per-bucket `get` below re-locks the same shard.
        let partitions = table
            .buckets
            .iter()
            .map(|b| *b.key())
            .sorted_unstable()
            .filter_map(|bucket_id| table.buckets.get(&bucket_id).filter(|b| bucket_overlaps_range(b, &ts_range)).map(|b| (bucket_id, b)))
            .map(|(bucket_id, bucket)| {
                // Hold the lock only long enough to clone Arc'd batch refs (and,
                // with a prefilter, take the id set atomically with them).
                let (snapshot, ids) = match text {
                    Some((schema, node)) => self.search_with_snapshot(&bucket, &Self::cache_key(project_id, table_name, bucket_id), schema, node)?,
                    None => (bucket.batches.lock().to_vec(), None),
                };
                let by_id = match ids {
                    Some(ids) => snapshot.iter().map(|b| filter_batch_by_id_set(b, &ids)).filter(|b| b.num_rows() > 0).collect(),
                    None => snapshot,
                };
                anyhow::Ok(filter_snapshot(by_id, &pred))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .filter(|p| !p.is_empty())
            .collect::<Vec<_>>();

        // Sort each partition so the leg can DECLARE its ordering, which keeps
        // `DedupExec` on its bounded seen-set. All-or-nothing:
        // `try_with_sort_information` declares one ordering for the whole source,
        // so one unsortable partition retracts the claim for all of them.
        let schema = crate::schema::get_schema(table_name).filter(|s| !s.sorting_columns.is_empty());
        let sorted_parts = schema.and_then(|s| partitions.iter().map(|p| sort_partition(s, p.clone())).collect::<Option<Vec<_>>>());
        if schema.is_some() && sorted_parts.is_none() {
            let diverse = partitions.iter().filter(|p| p.first().is_some_and(|f| p.iter().any(|b| b.schema() != f.schema()))).count();
            metrics::counter!(crate::database::scan_metric_names::MEM_SORT_RETRACTED).increment(1);
            if diverse > 0 {
                metrics::counter!(crate::database::scan_metric_names::MEM_SORT_RETRACTED_SCHEMA_DIVERSE).increment(1);
            }
            warn!(
                table_name,
                project_id,
                partitions = partitions.len(),
                schema_diverse_partitions = diverse,
                event = "mem_sort_retracted",
                "the in-memory leg could not sort every partition, so the whole union loses its ordering claim"
            );
        }
        let sorted = sorted_parts.is_some();
        let partitions = sorted_parts.unwrap_or(partitions);

        debug!(
            "MemBuffer scan_buckets: project={}, table={}, text_match={}, partitions={}, sorted={sorted}",
            project_id,
            table_name,
            text.is_some(),
            partitions.len()
        );
        Ok(MemLeg { partitions, sorted })
    }

    /// Time ranges (start, end_exclusive) the Delta scan must exclude because
    /// MemBuffer is authoritative for them, sorted ascending. Each range is the
    /// bucket's *actual* row range, not its 10-min window, so a partially filled
    /// bucket can't mask unrelated Delta rows. The current (open) bucket,
    /// force-flushed buckets and empty shells are skipped — their windows hold
    /// rows in both stores, so excluding them would hide the Delta share.
    pub fn get_bucket_ranges(&self, project_id: &str, table_name: &str) -> Vec<(i64, i64)> {
        let Some(table) = self.get_table(project_id, table_name) else {
            return Vec::new();
        };
        let current = Self::current_bucket_id();
        let force_flushed = self.force_flushed.get(&table_key(project_id, table_name));
        let flushed_max = self.flushed_max.get(&table_key(project_id, table_name));
        table
            .buckets
            .iter()
            .filter(|b| *b.key() != current && !force_flushed.as_ref().is_some_and(|s| s.contains(b.key())))
            .filter_map(|b| authority_range(b.value(), None, || flushed_max.as_ref().and_then(|m| m.get(b.key()).copied())))
            .sorted_by_key(|(s, _)| *s)
            .collect()
    }

    /// Snapshot a sealed bucket for flush WITHOUT removing its rows, so they
    /// stay queryable while the Delta commit is airborne. WAL holds are taken
    /// (reset), so late arrivals pin themselves. After the commit lands,
    /// [`Self::finish_flushed_snapshot`] removes exactly the snapshotted
    /// batches; on failure [`Self::restore_snapshot_holds`] merges the holds
    /// back.
    pub fn snapshot_bucket_for_flush(&self, project_id: &str, table_name: &str, bucket_id: i64) -> Option<FlushableBucket> {
        let table = self.get_table(project_id, table_name)?;
        let bucket_ref = table.buckets.get(&bucket_id)?;
        let bucket = bucket_ref.value();
        let batches_g = bucket.batches.lock();
        if batches_g.is_empty() {
            return None;
        }
        let mut wal_g = bucket.wal_shard_state.lock();
        let batches: Vec<RecordBatch> = batches_g.to_vec();
        let wal_state = std::mem::take(&mut *wal_g);
        // Fence the snapshot prefix against insert-time coalesce.
        bucket.flush_pinned_prefix.store(batches.len(), Ordering::Relaxed);
        // Capture the DML generation under the same lock as the batch clones,
        // so a mutation can't slip between clone and capture.
        let snapshot_gen = bucket.mutation_gen.load(Ordering::Relaxed);
        drop(wal_g);
        drop(batches_g);
        Some(FlushableBucket {
            project_id: project_id.to_string(),
            table_name: table_name.to_string(),
            bucket_id,
            row_count: batches.iter().map(|b| b.num_rows()).sum(),
            batches,
            wal_first_positions: pad_positions(&wal_state.first_positions, self.shards_per_topic),
            snapshot_gen,
            min_timestamp: bucket.min_timestamp.load(Ordering::Relaxed),
            max_timestamp: bucket.max_timestamp.load(Ordering::Relaxed),
            first_wal_pin_micros: bucket.first_wal_pin_micros.load(Ordering::Relaxed),
            // u64::MAX is never allocated by `taking_seq`, so releasing it is a no-op.
            taking_pin_seq: u64::MAX,
        })
    }

    /// Complete a successful snapshot-flush for one source bucket.
    ///
    /// Clean case (`mutation_gen` unchanged since the snapshot): remove exactly
    /// the snapshotted prefix batches and drop the bucket when nothing remains.
    /// Dirty case (a DML mutated the bucket mid-flight): the commit landed
    /// PRE-DML values and the prefix indices may have shifted, so keep all rows,
    /// merge the snapshot's holds back and re-flush next cycle.
    ///
    /// Returns true when the prefix was drained (clean case).
    pub fn finish_flushed_snapshot(&self, b: &FlushableBucket) -> bool {
        let key = table_key(&b.project_id, &b.table_name);
        // Source evaporated while airborne (evicted/reaped): the rows are
        // durably in Delta, so count as drained.
        let Some(table) = self.get_table(&b.project_id, &b.table_name) else {
            return true;
        };
        // Greatest row timestamp this commit handed to Delta — both the flushed
        // watermark and the floor the survivor's mask must start above.
        let drained_max = b.batches.iter().filter_map(batch_timestamp_range).map(|(_, hi)| hi).max();
        let mut emptied = false;
        let mut flushed_through: Option<i64> = None;
        if let Some(bucket_ref) = table.buckets.get(&b.bucket_id) {
            let bucket = bucket_ref.value();
            let mut g = bucket.batches.lock();
            if bucket.mutation_gen.load(Ordering::Relaxed) != b.snapshot_gen {
                // Dirty: re-pin and re-flush next cycle.
                bucket.restore_holds(&b.wal_first_positions);
                info!("finish_flushed_snapshot: bucket {}.{}/{} mutated mid-flight — keeping rows for re-flush", b.project_id, b.table_name, b.bucket_id);
                return false;
            }
            let n = b.batches.len().min(g.len());
            let (freed, rows): (usize, usize) =
                g.drain(..n).map(|batch| (estimate_batch_size(&batch), batch.num_rows())).fold((0, 0), |a, x| (a.0 + x.0, a.1 + x.1));
            emptied = g.is_empty();
            // Recorded whether or not anything survived: a bucket drained clean
            // still takes later inserts, and the branch below never runs for it.
            flushed_through = drained_max;
            if !emptied {
                // Narrow the surviving bucket's range to the remaining
                // (late-arrival) rows, or the old span keeps masking the drained
                // rows' freshly committed Delta copies.
                let (min, max) = g.iter().filter_map(batch_timestamp_range).fold((i64::MAX, i64::MIN), |a, r| (a.0.min(r.0), a.1.max(r.1)));
                // The mask is a time range, so it must begin strictly AFTER
                // everything this drain committed; the cost is at worst a
                // double-count, which read-side dedup collapses.
                let min = drained_max.map_or(min, |hi| min.max(hi.saturating_add(1)));
                bucket.min_timestamp.store(min, Ordering::Relaxed);
                bucket.max_timestamp.store(max, Ordering::Relaxed);
            }
            bucket.flush_pinned_prefix.store(0, Ordering::Relaxed);
            drop(g);
            // Mirror the CLAMPED amount onto the MemBuffer total — raw `freed`
            // over-subtracts whenever the bucket held less.
            let applied = sub_saturating(&bucket.memory_bytes, freed);
            bucket.row_count.fetch_sub(rows.min(bucket.row_count.load(Ordering::Relaxed)), Ordering::Relaxed);
            sub_saturating(&self.estimated_bytes, applied);
        }
        if let Some(hi) = flushed_through {
            self.note_flushed_through(key.clone(), b.bucket_id, hi);
        }
        self.cache_invalidate(&Self::cache_key(&b.project_id, &b.table_name, b.bucket_id));
        // remove_if re-checks under the shard lock (bucket_ref dropped above, so
        // no self-deadlock), keeping a bucket a racing insert repopulated.
        // Removing it drops its `memory_bytes` atomic — discount any residual.
        let removed = emptied.then(|| table.buckets.remove_if(&b.bucket_id, |_, bk| bk.batches.lock().is_empty())).flatten();
        if let Some((_, shell)) = &removed {
            sub_saturating(&self.estimated_bytes, shell.memory_bytes.load(Ordering::Relaxed));
        }
        if removed.is_some() {
            if let Some(mut s) = self.force_flushed.get_mut(&key) {
                s.remove(&b.bucket_id);
            }
            self.force_flushed.remove_if(&key, |_, s| s.is_empty());
            drop(table);
            self.try_drop_empty_table(&key);
        }
        true
    }

    /// Merge a snapshot's WAL holds back after a failed commit (rows never
    /// left the bucket). Returns false when the bucket no longer exists
    /// (e.g. evicted) — the caller must then keep its in-flight holds so the
    /// entries stay replayable.
    #[must_use]
    pub fn restore_snapshot_holds(&self, b: &FlushableBucket) -> bool {
        let Some(table) = self.get_table(&b.project_id, &b.table_name) else {
            return false;
        };
        let Some(bucket) = table.buckets.get(&b.bucket_id) else {
            return false;
        };
        bucket.restore_holds(&b.wal_first_positions);
        true
    }

    /// Remove empty bucket shells whose pinned WAL entries have aged past
    /// `arrival_cutoff_micros`, releasing their cursor holds so the topic's WAL
    /// can advance and GC.
    ///
    /// SOUNDNESS: this is only exact because an empty shell's pinned entries are
    /// an insert set plus the DML(s) that emptied it, which net to zero rows on
    /// replay. Any future path that leaves holds on an empty bucket whose
    /// entries do NOT net to zero must not be reaped here. The age gate is a
    /// grace period so an airborne DML pair isn't split.
    pub fn reap_expired_empty_buckets(&self, arrival_cutoff_micros: i64) -> usize {
        let releasable = |b: &TimeBucket| b.batches.lock().is_empty() && b.last_wal_pin_micros.load(Ordering::Relaxed) < arrival_cutoff_micros;
        let reaped: usize = self
            .tables
            .iter()
            .map(|table| {
                let expired: Vec<i64> =
                    table.buckets.iter().filter(|b| releasable(b) && b.flush_pinned_prefix.load(Ordering::Relaxed) == 0).map(|b| *b.key()).collect();
                // Re-check emptiness under the shard lock — a concurrent
                // insert that repopulated the shell keeps it.
                expired
                    .into_iter()
                    .filter_map(|id| {
                        table.buckets.remove_if(&id, |_, b| releasable(b)).map(|(_, shell)| {
                            sub_saturating(&self.estimated_bytes, shell.memory_bytes.load(Ordering::Relaxed));
                            self.cache_invalidate(&Self::cache_key(&table.project_id, &table.table_name, id));
                        })
                    })
                    .count()
            })
            .sum();
        if reaped > 0 {
            debug!("reap_expired_empty_buckets: released {} expired empty shell(s)", reaped);
        }
        reaped
    }

    /// Race-safe removal of an empty TableBuffer. `remove_if` holds the shard
    /// write lock; the strong_count check skips eviction whenever a
    /// writer/reader is mid-operation on this table.
    fn try_drop_empty_table(&self, key: &TableKey) -> bool {
        self.tables.remove_if(key, |_, v| v.buckets.is_empty() && Arc::strong_count(v) == 1).is_some()
    }

    /// Atomically take a bucket's rows + WAL holds for a flush, under the same
    /// `batches` lock inserts use, so no row is lost between snapshot and
    /// removal — safe on sealed AND currently-written buckets. Returns None
    /// when the bucket is absent or already empty.
    pub fn take_bucket_for_flush(&self, project_id: &str, table_name: &str, bucket_id: i64) -> Option<FlushableBucket> {
        let table = self.get_table(project_id, table_name)?;
        let bucket_ref = table.buckets.get(&bucket_id)?;
        let bucket = bucket_ref.value();
        let mut batches_g = bucket.batches.lock();
        if batches_g.is_empty() {
            return None;
        }
        // Lock wal_shard_state too so the taken holds cover exactly the taken
        // rows' WAL entries.
        let mut wal_g = bucket.wal_shard_state.lock();
        let batches: Vec<RecordBatch> = std::mem::take(&mut *batches_g);
        let wal_state = std::mem::take(&mut *wal_g);
        bucket.flush_pinned_prefix.store(0, Ordering::Relaxed);
        let freed = bucket.memory_bytes.swap(0, Ordering::Relaxed);
        let row_count = bucket.row_count.swap(0, Ordering::Relaxed);
        // Capture the real range as the sentinels reset, so a restore (on Delta
        // commit failure) replays it instead of guessing bucket-start.
        let min_timestamp = bucket.min_timestamp.swap(i64::MAX, Ordering::Relaxed);
        let max_timestamp = bucket.max_timestamp.swap(i64::MIN, Ordering::Relaxed);
        // The bucket keeps taking inserts, so without this the next row at an
        // already-flushed instant rebuilds the mask over the rows just taken.
        let flushed_through = bucket.row_max_ts.load(Ordering::Relaxed).max(max_timestamp);
        // Reset with them, or a reused bucket reports a span for rows it no longer holds.
        bucket.row_min_ts.store(i64::MAX, Ordering::Relaxed);
        bucket.row_max_ts.store(i64::MIN, Ordering::Relaxed);
        // Park the GC-floor pin in `taking_pins` BEFORE clearing it from the
        // bucket, so a GC sweep racing the flush path's `register_inflight_pin`
        // still sees the floor; released via `release_taking_pin`.
        let first_wal_pin_micros = bucket.first_wal_pin_micros.load(Ordering::Relaxed);
        let taking_pin_seq = self.taking_seq.fetch_add(1, Ordering::Relaxed);
        if let Some(pin) = pin_opt(first_wal_pin_micros) {
            self.taking_pins.insert(taking_pin_seq, pin);
        }
        bucket.first_wal_pin_micros.store(i64::MAX, Ordering::Relaxed);
        drop(wal_g);
        drop(batches_g);
        drop(bucket_ref);
        sub_saturating(&self.estimated_bytes, freed);
        self.note_flushed_through(table_key(project_id, table_name), bucket_id, flushed_through);
        self.cache_invalidate(&Self::cache_key(project_id, table_name, bucket_id));

        // `remove_if` re-checks emptiness under the shard write lock, so an
        // insert racing the take is preserved.
        if let Some((_, shell)) = table.buckets.remove_if(&bucket_id, |_, b| b.batches.lock().is_empty()) {
            sub_saturating(&self.estimated_bytes, shell.memory_bytes.load(Ordering::Relaxed));
        }

        Some(FlushableBucket {
            project_id: project_id.to_string(),
            table_name: table_name.to_string(),
            bucket_id,
            batches,
            row_count,
            wal_first_positions: pad_positions(&wal_state.first_positions, self.shards_per_topic),
            snapshot_gen: 0, // take removes rows; the gen check is snapshot-path-only
            min_timestamp,
            max_timestamp,
            first_wal_pin_micros,
            taking_pin_seq,
        })
    }

    /// Re-insert a bucket previously removed by `take_bucket_for_flush` whose
    /// Delta commit then failed. Restores rows and merges the WAL holds back.
    ///
    /// Returns false when the rows could NOT be restored (e.g. the table was
    /// recreated with an incompatible schema while the bucket was airborne).
    /// The caller must then keep its in-flight cursor holds registered so the
    /// watermark can't pass the un-restored entries.
    #[must_use]
    pub fn restore_taken_bucket(&self, b: &FlushableBucket) -> bool {
        // Recreate the table if it was reaped while the bucket was airborne: a
        // silent no-op drops the rows AND their cursor holds, letting the
        // watermark pass acked entries.
        let Some(schema) = b.batches.first().map(|batch| batch.schema()) else {
            return true; // nothing to restore
        };
        let table = match self.get_or_create_table(&b.project_id, &b.table_name, &schema) {
            Ok(t) => t,
            Err(e) => {
                error!(
                    "restore_taken_bucket: cannot restore {} rows for {}.{} bucket {} ({}); rows stay WAL-only until restart replay",
                    b.row_count, b.project_id, b.table_name, b.bucket_id, e
                );
                return false;
            }
        };
        let bucket = table.buckets.entry(b.bucket_id).or_insert_with(TimeBucket::new);
        let mut batches_g = bucket.batches.lock();
        let mut wal_g = bucket.wal_shard_state.lock();
        let added: usize = b.batches.iter().map(estimate_batch_size).sum();
        batches_g.extend(b.batches.iter().cloned());
        b.wal_first_positions.iter().enumerate().filter_map(|(i, p)| p.map(|p| (i, p))).for_each(|(i, pos)| wal_g.merge(i, pos));
        bucket.memory_bytes.fetch_add(added, Ordering::Relaxed);
        bucket.row_count.fetch_add(b.row_count, Ordering::Relaxed);
        // Monotonic widen so restored rows stay visible to time-range pruning
        // without clobbering concurrent inserts into the same open bucket.
        bucket.update_timestamps(b.min_timestamp);
        bucket.update_timestamps(b.max_timestamp);
        bucket.first_wal_pin_micros.fetch_min(b.first_wal_pin_micros, Ordering::Relaxed);
        drop(wal_g);
        drop(batches_g);
        self.estimated_bytes.fetch_add(added, Ordering::Relaxed);
        self.cache_invalidate(&Self::cache_key(&b.project_id, &b.table_name, b.bucket_id));
        true
    }

    /// Count buckets that have DWELLED here since before `cutoff_micros` —
    /// persistence debt: buffered long ago and still not flushed. Must use
    /// dwell (`created_micros`), never the rows' `max_timestamp`: a
    /// merge-on-read UPDATE appends the row's ORIGINAL timestamp, so a fresh
    /// bucket can hold hours-old event time and would read as debt forever.
    pub fn count_buckets_dwelling_since(&self, cutoff_micros: i64) -> usize {
        self.tables.iter().map(|t| t.value().buckets.iter().filter(|b| b.value().created_micros < cutoff_micros).count()).sum()
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

            for (bucket_id, bucket) in bucket_ids_to_remove.into_iter().filter_map(|id| table.buckets.remove(&id)) {
                freed_bytes += bucket.memory_bytes.load(Ordering::Relaxed);
                evicted_count += 1;
                self.cache_invalidate(&Self::cache_key(&table.project_id, &table.table_name, bucket_id));
            }
            // Flushed watermarks outlive their buckets on purpose, but only
            // until the window itself falls out of retention.
            self.flushed_max.remove_if_mut(table_entry.key(), |_, w| {
                w.retain(|bucket_id, _| *bucket_id >= cutoff_bucket_id);
                w.is_empty()
            });
            if table.buckets.is_empty() {
                empty_table_keys.push(table_entry.key().clone());
            }
        }

        let tables_dropped = empty_table_keys.iter().filter(|key| self.try_drop_empty_table(key)).count();

        sub_saturating(&self.estimated_bytes, freed_bytes);

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

    pub fn has_table(&self, project_id: &str, table_name: &str) -> bool {
        self.tables.contains_key(&table_key(project_id, table_name))
    }

    /// Delete rows matching the predicate; returns the number deleted.
    /// `wal_hold` = the DELETE WAL entry's (shard, pre-append position),
    /// pinned onto every bucket this call mutates.
    #[instrument(skip(self, predicate, wal_hold), fields(project_id, table_name, rows_deleted))]
    pub fn delete(&self, project_id: &str, table_name: &str, predicate: Option<&Expr>, wal_hold: Option<(usize, walrus_rust::WalPosition)>) -> DFResult<u64> {
        let Some(table) = self.get_table(project_id, table_name) else {
            return Ok(0);
        };

        let schema = table.schema();
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
        let props = ExecutionProps::new();

        let physical_predicate = predicate.map(|p| create_physical_expr(&strip_column_qualifiers(p.clone())?, &df_schema, &props)).transpose()?;

        let (total_deleted, total_freed) = table.buckets.iter_mut().try_fold((0u64, 0usize), |(deleted, freed), mut bucket_entry| -> DFResult<_> {
            let bucket = bucket_entry.value_mut();
            let mut batches = bucket.batches.lock();

            let cap = batches.len();
            let (new_batches, bucket_freed, rows_removed) =
                batches.drain(..).try_fold((Vec::with_capacity(cap), 0usize, 0usize), |(mut kept, freed, removed), batch| -> DFResult<_> {
                    let (rows, original_size) = (batch.num_rows(), estimate_batch_size(&batch));
                    let survived = match physical_predicate.as_ref() {
                        // Keep rows where the predicate is FALSE.
                        Some(phys_pred) => filter_record_batch(&batch, &arrow::compute::not(&eval_bool_mask(phys_pred, &batch)?)?)?,
                        // No predicate = delete all rows.
                        None => RecordBatch::new_empty(batch.schema()),
                    };
                    let kept_rows = survived.num_rows();
                    let freed = freed + if kept_rows > 0 { original_size.saturating_sub(estimate_batch_size(&survived)) } else { original_size };
                    if kept_rows > 0 {
                        kept.push(survived);
                    }
                    Ok((kept, freed, removed + rows - kept_rows))
                })?;

            *batches = new_batches;
            if rows_removed > 0 {
                bucket.note_dml_mutation(wal_hold);
                bucket.row_count.fetch_sub(rows_removed, Ordering::Relaxed);
            }
            Ok((deleted + rows_removed as u64, freed + sub_saturating(&bucket.memory_bytes, bucket_freed)))
        })?;

        sub_saturating(&self.estimated_bytes, total_freed);

        debug!("MemBuffer delete: project={}, table={}, rows_deleted={}", project_id, table_name, total_deleted);
        Ok(total_deleted)
    }

    /// Eagerly drop buffered row versions that `appended` supersedes: among
    /// buffered rows sharing a dedup key with an appended row, keep only the
    /// greatest `tiebreak` per key (ties keep both) — exactly what read-side
    /// dedup keeps, applied before the flush so one copy lands in parquet
    /// instead of two. Only the appended rows' own buckets are touched (a
    /// version carries its row's ORIGINAL timestamp, so every older copy lives
    /// in the same bucket). Fail-safe by construction: a missed row flushes
    /// both copies and read-side dedup collapses them — the status quo.
    pub fn retract_superseded(&self, project_id: &str, table_name: &str, appended: &RecordBatch, keys: &[String], tiebreak: &str, time_col: &str) -> usize {
        let Some(table) = self.get_table(project_id, table_name) else { return 0 };
        let key_cols = |batch: &RecordBatch| keys.iter().map(|k| batch.column_by_name(k).cloned()).collect::<Option<Vec<ArrayRef>>>();
        let Some(probe_cols) = key_cols(appended) else { return 0 };
        let Ok(converter) = RowConverter::new(probe_cols.iter().map(|a| SortField::new(a.data_type().clone())).collect()) else { return 0 };
        let Ok(probe) = converter.convert_columns(&probe_cols) else { return 0 };
        let updated_keys: std::collections::HashSet<Vec<u8>, ahash::RandomState> = (0..probe.num_rows()).map(|i| probe.row(i).as_ref().to_vec()).collect();
        let stamp_rows = |batch: &RecordBatch| -> Option<arrow::row::Rows> {
            let col = batch.column_by_name(tiebreak)?;
            RowConverter::new(vec![SortField::new(col.data_type().clone())]).ok()?.convert_columns(std::slice::from_ref(col)).ok()
        };

        let mut total_removed = 0usize;
        let mut total_freed = 0usize;
        for bucket_id in batch_bucket_ids(appended, time_col) {
            let Some(bucket) = table.buckets.get(&bucket_id) else { continue };
            let mut batches = bucket.batches.lock();
            // Pass 1: greatest stamp per updated key across the bucket.
            let per_batch: Vec<Option<(arrow::row::Rows, arrow::row::Rows)>> =
                batches.iter().map(|b| Some((converter.convert_columns(&key_cols(b)?).ok()?, stamp_rows(b)?))).collect();
            let mut max_stamp: std::collections::HashMap<Vec<u8>, Vec<u8>, ahash::RandomState> = Default::default();
            for (krows, srows) in per_batch.iter().flatten() {
                for i in 0..krows.num_rows() {
                    let k = krows.row(i);
                    if updated_keys.contains(k.as_ref()) {
                        let s = srows.row(i);
                        match max_stamp.get(k.as_ref()) {
                            Some(m) if s.as_ref() <= m.as_slice() => {}
                            _ => {
                                max_stamp.insert(k.as_ref().to_vec(), s.as_ref().to_vec());
                            }
                        }
                    }
                }
            }
            // Pass 2: remove matching rows strictly below their key's winner.
            let (mut removed, mut freed) = (0usize, 0usize);
            let new_batches: Vec<RecordBatch> = batches
                .iter()
                .zip(&per_batch)
                .map(|(batch, rows)| {
                    let Some((krows, srows)) = rows else { return Ok(batch.clone()) };
                    let keep: BooleanArray = (0..krows.num_rows())
                        .map(|i| Some(max_stamp.get(krows.row(i).as_ref()).is_none_or(|m| srows.row(i).as_ref() >= m.as_slice())))
                        .collect();
                    if keep.false_count() == 0 {
                        return Ok(batch.clone());
                    }
                    let survived = filter_record_batch(batch, &keep)?;
                    removed += batch.num_rows() - survived.num_rows();
                    freed += estimate_batch_size(batch).saturating_sub(estimate_batch_size(&survived));
                    Ok(survived)
                })
                .collect::<anyhow::Result<_>>()
                .unwrap_or_else(|_| batches.clone());
            if removed > 0 {
                *batches = new_batches.into_iter().filter(|b| b.num_rows() > 0).collect();
                bucket.note_dml_mutation(None);
                bucket.row_count.fetch_sub(removed, Ordering::Relaxed);
                sub_saturating(&bucket.memory_bytes, freed);
                total_removed += removed;
                total_freed += freed;
            }
        }
        sub_saturating(&self.estimated_bytes, total_freed);
        total_removed
    }

    /// Compile assignment exprs to `(target column index, physical expr)`.
    /// `rewrite` runs after qualifier stripping — identity (`Ok`) for a plain
    /// UPDATE, the `source__` renamer for `UPDATE ... FROM`.
    fn compile_assignments(
        assignments: &[(String, Expr)], target: &SchemaRef, df_schema: &DFSchema, props: &ExecutionProps, rewrite: impl Fn(Expr) -> DFResult<Expr>,
    ) -> DFResult<Vec<(usize, Arc<dyn datafusion::physical_expr::PhysicalExpr>)>> {
        assignments
            .iter()
            .map(|(col, expr)| {
                let phys_expr = create_physical_expr(&rewrite(strip_column_qualifiers(expr.clone())?)?, df_schema, props)?;
                let col_idx = target.index_of(col).map_err(|_| datafusion::error::DataFusionError::Execution(format!("Column '{}' not found", col)))?;
                Ok((col_idx, phys_expr))
            })
            .collect()
    }

    /// Update rows matching the predicate; returns the number updated.
    #[instrument(skip(self, predicate, assignments), fields(project_id, table_name, rows_updated))]
    pub fn update(
        &self, project_id: &str, table_name: &str, predicate: Option<&Expr>, assignments: &[(String, Expr)],
        wal_hold: Option<(usize, walrus_rust::WalPosition)>,
    ) -> DFResult<u64> {
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

        let physical_assignments = Self::compile_assignments(assignments, &schema, &df_schema, &props, Ok)?;

        let mut total_updated = 0u64;
        let mut total_delta: i64 = 0;

        for mut bucket_entry in table.buckets.iter_mut() {
            let bucket = bucket_entry.value_mut();
            let mut batches = bucket.batches.lock();
            let updated_before = total_updated;

            let mut bucket_delta: i64 = 0;
            let new_batches: Vec<RecordBatch> = batches
                .drain(..)
                .map(|batch| {
                    let num_rows = batch.num_rows();
                    if num_rows == 0 {
                        return Ok(batch);
                    }

                    let mask = eval_bool_mask_or_all(physical_predicate.as_ref(), &batch)?;

                    let matching_count = mask.true_count();
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
            if total_updated > updated_before {
                bucket.note_dml_mutation(wal_hold);
            }
            apply_signed_delta(&bucket.memory_bytes, bucket_delta);
            total_delta += bucket_delta;
        }

        apply_signed_delta(&self.estimated_bytes, total_delta);

        debug!("MemBuffer update: project={}, table={}, rows_updated={}", project_id, table_name, total_updated);
        Ok(total_updated)
    }

    /// `UPDATE ... FROM`: target rows are hashed against the source keys via
    /// Arrow `RowConverter`; matched rows evaluate assignment exprs against a
    /// per-batch "widened" `RecordBatch` with schema
    /// `(target_fields..., source__<name>...)`. On multi-match the first source
    /// row wins (PG leaves this undefined).
    #[instrument(skip(self, predicate, assignments, source), fields(project_id, table_name, rows_updated))]
    pub fn update_with_source(
        &self, project_id: &str, table_name: &str, predicate: Option<&Expr>, assignments: &[(String, Expr)], source: &crate::dml::UpdateSource,
        wal_hold: Option<(usize, walrus_rust::WalPosition)>,
    ) -> DFResult<u64> {
        use std::collections::HashMap;

        if assignments.is_empty() {
            return Ok(0);
        }
        let Some(table) = self.get_table(project_id, table_name) else {
            return Ok(0);
        };
        let target_schema = table.schema();
        let widened_schema = widen_schema_with_source(target_schema.fields(), source.schema.fields());
        let widened_df_schema = DFSchema::try_from(widened_schema.as_ref().clone())?;
        let props = ExecutionProps::new();

        // Turn source-qualified (and bare source-named) column refs into
        // `source__<name>` so they resolve against the widened schema.
        let source_col_names: std::collections::HashSet<String> = source.schema.fields().iter().map(|f| f.name().clone()).collect();
        let rewrite = |e: Expr| -> DFResult<Expr> {
            use datafusion::common::tree_node::Transformed;
            e.transform(|expr| match &expr {
                Expr::Column(c)
                    if matches!(c.relation.as_ref(), Some(r) if r.table() == "source") || (c.relation.is_none() && source_col_names.contains(&c.name)) =>
                {
                    Ok(Transformed::yes(Expr::Column(Column::from_name(format!("source__{}", c.name)))))
                }
                _ => Ok(Transformed::no(expr)),
            })
            .map(|t| t.data)
            .map_err(|e| datafusion::error::DataFusionError::Execution(format!("update_with_source: rewrite failed: {e}")))
        };

        let physical_predicate =
            predicate.map(|p| create_physical_expr(&rewrite(strip_column_qualifiers(p.clone())?)?, &widened_df_schema, &props)).transpose()?;

        let physical_assignments = Self::compile_assignments(assignments, &target_schema, &widened_df_schema, &props, rewrite)?;

        // RowConverter requires matching data types on both sides, so cast the
        // source key columns to the target key column types before hashing.
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
                if raw.data_type() == &target_ty { Ok(raw.clone()) } else { arrow::compute::cast(raw.as_ref(), &target_ty).map_err(arrow_err) }
            })
            .collect::<DFResult<Vec<_>>>()?;
        let sort_fields: Vec<SortField> = src_key_cols.iter().map(|c| SortField::new(c.data_type().clone())).collect();
        let row_converter = RowConverter::new(sort_fields).map_err(arrow_err)?;
        let src_rows = row_converter.convert_columns(&src_key_cols).map_err(arrow_err)?;
        let mut src_lookup: HashMap<arrow::row::Row<'_>, u32, ahash::RandomState> =
            HashMap::with_capacity_and_hasher(source.batch.num_rows(), ahash::RandomState::new());
        for (i, row) in src_rows.iter().enumerate() {
            src_lookup.entry(row).or_insert(i as u32);
        }

        let mut total_updated = 0u64;
        let mut total_delta: i64 = 0;
        // The predicate arrives as one nested AND, so split it —
        // `extract_timestamp_range` only reads top-level conjuncts.
        let conjuncts: Vec<Expr> = predicate.map(|p| datafusion::logical_expr::utils::split_conjunction(p).into_iter().cloned().collect()).unwrap_or_default();
        let ts_range = extract_timestamp_range(&conjuncts);

        for mut bucket_entry in table.buckets.iter_mut() {
            let bucket = bucket_entry.value_mut();
            if !bucket_overlaps_range(bucket, &ts_range) {
                continue;
            }
            let mut batches = bucket.batches.lock();
            let updated_before = total_updated;
            let mut bucket_delta: i64 = 0;
            let mut new_batches: Vec<RecordBatch> = Vec::with_capacity(batches.len());

            for batch in batches.drain(..) {
                let num_rows = batch.num_rows();
                if num_rows == 0 {
                    new_batches.push(batch);
                    continue;
                }

                let tgt_key_cols: Vec<ArrayRef> = source
                    .join_keys
                    .iter()
                    .zip(&src_key_cols)
                    .map(|((tgt_col, _), src_col)| {
                        let raw = batch
                            .column_by_name(tgt_col)
                            .ok_or_else(|| datafusion::error::DataFusionError::Plan(format!("Target column '{}' not found", tgt_col)))?;
                        // `row_converter` was built from the SOURCE key types. A buffered batch may
                        // hold a column as `Utf8View` where the declared schema says `Utf8`; feeding
                        // that in raw makes every lookup miss and the UPDATE silently match 0 rows.
                        if raw.data_type() == src_col.data_type() {
                            Ok(raw.clone())
                        } else {
                            arrow::compute::cast(raw.as_ref(), src_col.data_type()).map_err(arrow_err)
                        }
                    })
                    .collect::<DFResult<Vec<_>>>()?;
                let tgt_rows = row_converter.convert_columns(&tgt_key_cols).map_err(arrow_err)?;

                let src_idxs: UInt32Array = (0..num_rows).map(|i| src_lookup.get(&tgt_rows.row(i)).copied()).collect();

                // The predicate can only narrow the join match, never widen it,
                // so zero joined rows skips the widened-batch materialization.
                if src_idxs.null_count() == num_rows {
                    new_batches.push(batch);
                    continue;
                }

                // Widen + evaluate ONLY the matched candidate rows and preserve
                // the rest with a single `filter`. Output is
                // [preserved-rows, updated-rows]; row order within a bucket is
                // not semantically meaningful, so splitting is safe.
                let has_match = arrow::compute::is_not_null(&src_idxs).map_err(arrow_err)?;

                let cand_batch = filter_record_batch(&batch, &has_match).map_err(arrow_err)?;
                let cand_src_idxs = arrow::compute::filter(&src_idxs, &has_match).map_err(arrow_err)?;
                let cand_src_idxs = cand_src_idxs.as_any().downcast_ref::<UInt32Array>().expect("filter preserves UInt32 type");
                let mut widened_cols: Vec<ArrayRef> = cand_batch.columns().to_vec();
                for i in 0..source.schema.fields().len() {
                    widened_cols.push(arrow::compute::take(source.batch.column(i).as_ref(), cand_src_idxs, None).map_err(arrow_err)?);
                }
                let cand_widened = RecordBatch::try_new(widened_schema.clone(), widened_cols).map_err(arrow_err)?;

                let cand_pred = eval_bool_mask_or_all(physical_predicate.as_ref(), &cand_widened)?;

                let matching_count = cand_pred.true_count();
                if matching_count == 0 {
                    new_batches.push(batch);
                    continue;
                }
                total_updated += matching_count as u64;
                let old_size = estimate_batch_size(&batch);

                // The candidate cursor must only advance on has_match rows —
                // guaranteed by `&&` short-circuiting below.
                let updated_full: Vec<bool> = (0..num_rows)
                    .scan(0usize, |c, i| {
                        Some(
                            has_match.value(i) && {
                                let hit = cand_pred.value(*c);
                                *c += 1;
                                hit
                            },
                        )
                    })
                    .collect();
                let not_updated = arrow::compute::not(&BooleanArray::from(updated_full)).map_err(arrow_err)?;
                let preserved = filter_record_batch(&batch, &not_updated).map_err(arrow_err)?;

                // Target columns are the first N of the widened batch, so
                // non-assignment columns come straight from it.
                let upd_widened = filter_record_batch(&cand_widened, &cand_pred).map_err(arrow_err)?;
                let upd_n = upd_widened.num_rows();
                let new_columns: Vec<ArrayRef> = (0..target_schema.fields().len())
                    .map(|col_idx| {
                        if let Some((_, phys_expr)) = physical_assignments.iter().find(|(idx, _)| *idx == col_idx) {
                            let evaluated = phys_expr.evaluate(&upd_widened)?.into_array(upd_n)?;
                            // The RHS may evaluate to a related-but-distinct type
                            // (e.g. List<Utf8View> for a List<Utf8> target), which
                            // RecordBatch::try_new would reject.
                            let want = target_schema.field(col_idx).data_type();
                            if evaluated.data_type() == want { Ok(evaluated) } else { arrow::compute::cast(&evaluated, want).map_err(arrow_err) }
                        } else {
                            Ok(upd_widened.column(col_idx).clone())
                        }
                    })
                    .collect::<DFResult<Vec<_>>>()?;
                let updated_batch = RecordBatch::try_new(batch.schema(), new_columns).map_err(arrow_err)?;

                bucket_delta += (estimate_batch_size(&preserved) + estimate_batch_size(&updated_batch)) as i64 - old_size as i64;
                if preserved.num_rows() > 0 {
                    new_batches.push(preserved);
                }
                new_batches.push(updated_batch);
            }

            *batches = new_batches;
            if total_updated > updated_before {
                bucket.note_dml_mutation(wal_hold);
            }
            apply_signed_delta(&bucket.memory_bytes, bucket_delta);
            total_delta += bucket_delta;
        }

        apply_signed_delta(&self.estimated_bytes, total_delta);
        debug!("MemBuffer update_with_source: project={}, table={}, rows_updated={}", project_id, table_name, total_updated);
        Ok(total_updated)
    }

    /// Delete rows using a SQL predicate string (WAL recovery path).
    #[instrument(skip(self, registry), fields(project_id, table_name))]
    pub fn delete_by_sql(
        &self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, registry: Option<&FnRegistry>,
        wal_hold: Option<(usize, walrus_rust::WalPosition)>,
    ) -> DFResult<u64> {
        if self.replay_dml_noop(project_id, table_name, "DELETE") {
            return Ok(0);
        }
        let df_schema = self.df_schema_for(project_id, table_name)?;
        let predicate = predicate_sql.map(|s| parse_sql_predicate(s, &df_schema, registry)).transpose()?;
        self.delete(project_id, table_name, predicate.as_ref(), wal_hold)
    }

    /// WAL replay path for `UPDATE ... FROM`: parses the SQL against the widened
    /// schema (target + `source__`-prefixed source columns), then delegates to
    /// [`Self::update_with_source`].
    #[instrument(skip(self, assignments, source, registry), fields(project_id, table_name, source_rows = source.batch.num_rows()))]
    // 8 after folding the join keys and the batch into `UpdateSource` (the pair that must agree);
    // the rest mirror `update_by_sql`'s arguments one for one and are genuinely independent.
    #[allow(clippy::too_many_arguments)]
    pub fn update_with_source_by_sql(
        &self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, assignments: &[(String, String)], source: crate::dml::UpdateSource,
        registry: Option<&FnRegistry>, wal_hold: Option<(usize, walrus_rust::WalPosition)>,
    ) -> DFResult<u64> {
        if self.replay_dml_noop(project_id, table_name, "UPDATE...FROM") {
            return Ok(0);
        }
        let target_df_schema = self.df_schema_for(project_id, table_name)?;

        // Must match the widened DFSchema the assignment SQL was originally
        // parsed against, as built by `update_with_source`.
        let widened_schema = widen_schema_with_source(target_df_schema.fields(), source.schema.fields());
        let widened_df_schema = DFSchema::try_from(widened_schema.as_ref().clone())?;

        let predicate = predicate_sql.map(|s| parse_sql_predicate(s, &widened_df_schema, registry)).transpose()?;
        let parsed_assignments = parse_sql_assignments(assignments, &widened_df_schema, registry)?;

        self.update_with_source(project_id, table_name, predicate.as_ref(), &parsed_assignments, &source, wal_hold)
    }

    /// Update rows using SQL strings (WAL recovery path).
    #[instrument(skip(self, assignments, registry), fields(project_id, table_name))]
    pub fn update_by_sql(
        &self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, assignments: &[(String, String)], registry: Option<&FnRegistry>,
        wal_hold: Option<(usize, walrus_rust::WalPosition)>,
    ) -> DFResult<u64> {
        if self.replay_dml_noop(project_id, table_name, "UPDATE") {
            return Ok(0);
        }
        let df_schema = self.df_schema_for(project_id, table_name)?;
        let predicate = predicate_sql.map(|s| parse_sql_predicate(s, &df_schema, registry)).transpose()?;
        let parsed_assignments = parse_sql_assignments(assignments, &df_schema, registry)?;
        self.update(project_id, table_name, predicate.as_ref(), &parsed_assignments, wal_hold)
    }

    /// Replay-path guard: DML for a table with no buffered rows is a no-op.
    /// Parsing would otherwise hit `DFSchema::empty()` and quarantine the entry
    /// with a bogus schema error.
    fn replay_dml_noop(&self, project_id: &str, table_name: &str, op: &str) -> bool {
        let untracked = self.get_table(project_id, table_name).is_none();
        if untracked {
            self.replay_dml_noops.fetch_add(1, Ordering::Relaxed);
            warn!("{op} replay: no buffered table {project_id}.{table_name} — nothing to mutate, consuming as no-op");
        }
        untracked
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
        // Only buckets the flush path should already have drained count toward
        // the dwell signal — non-empty AND past the open window.
        let current = Self::current_bucket_id();
        let mut stats = MemBufferStats {
            project_count: self.tables.iter().map(|t| t.key().0.clone()).unique().count(),
            replay_dml_noops: self.replay_dml_noops.load(Ordering::Relaxed),
            ..Default::default()
        };
        for table_entry in self.tables.iter() {
            stats.total_buckets += table_entry.value().buckets.len();
            for bucket in table_entry.value().buckets.iter() {
                stats.total_rows += bucket.row_count.load(Ordering::Relaxed);
                stats.estimated_memory_bytes += bucket.memory_bytes.load(Ordering::Relaxed);
                let batch_count = bucket.batches.lock().len();
                stats.total_batches += batch_count;
                if batch_count > 0 && *bucket.key() < current {
                    stats.oldest_bucket_micros = Some(stats.oldest_bucket_micros.map_or(bucket.created_micros, |o| o.min(bucket.created_micros)));
                }
            }
        }
        stats
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
        // Resolved once per table (`schema_ref()` builds ~100 fields, too costly
        // per batch); its nullability is pinned onto the advertised schema so
        // readers see the declared truth, not the first batch's metadata.
        let declared = crate::schema::get_schema(&table_name).map(|s| s.schema_ref());
        // A deliberately narrow internal batch cannot advertise columns it does
        // not carry, so retain its shape and align only matching fields.
        let schema = declared.as_ref().and_then(|d| align_nullability(&schema, d, None)).unwrap_or(schema);
        Self { buckets: DashMap::new(), schema, declared, project_id, table_name }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Insert a batch into this table's time bucket, paying an amortized
    /// coalesce once the bucket crosses `MAX_BATCH_COUNT_PER_BUCKET`.
    ///
    /// Returns `(net_memory_delta_bytes, bucket_id)` — the **signed** change to
    /// `bucket.memory_bytes`. The caller must apply it verbatim to
    /// `MemBuffer::estimated_bytes`, or that total drifts.
    pub fn insert_batch(&self, batch: RecordBatch, timestamp_micros: i64, wal_hold: Option<(usize, walrus_rust::WalPosition)>) -> anyhow::Result<(i64, i64)> {
        // Reconcile against the declared schema BEFORE storing, so every
        // downstream consumer sees one authoritative nullability.
        let batch = canonicalize_declared_batch(batch, self.declared.as_ref())?;
        let batch = align_batch_nullability(compact_batch(batch), self.declared.as_ref());
        let bucket_id = MemBuffer::compute_bucket_id(timestamp_micros);
        let row_count = batch.num_rows();
        let new_size = estimate_batch_size(&batch);
        // Taken before the batch is moved into the bucket; see `row_min_ts`.
        let ts_bounds = batch_timestamp_range(&batch);

        let bucket = self.buckets.entry(bucket_id).or_insert_with(TimeBucket::new);

        // The coalesce's own change to `bucket.memory_bytes`, 0 when it doesn't fire.
        let coalesce_delta: i64 = {
            let mut g = bucket.batches.lock();
            // The WAL cursor hold MUST be recorded under the same lock as the
            // batch push: `take_bucket_for_flush` snapshots batches + holds
            // under this lock too, so a concurrent take can never grab rows
            // without their hold (else acked-write loss on crash).
            match wal_hold {
                Some((shard, pos)) => bucket.record_wal_append(shard, Some(pos)),
                // WAL GC floor: `record_wal_append` stamps it for the live
                // path; only replay inserts (no wal_hold) need it here.
                None => {
                    bucket.first_wal_pin_micros.fetch_min(chrono::Utc::now().timestamp_micros(), Ordering::Relaxed);
                }
            }
            g.push(batch);
            bucket.memory_bytes.fetch_add(new_size, Ordering::Relaxed);
            // Fold only the trailing run of batches each ≤
            // MAX_BATCH_BYTES_FOR_COALESCE, so the under-lock memcpy is bounded
            // regardless of bucket size. A `concat_batches` failure must NOT
            // propagate: the pushed batch is already in the bucket, so an Err
            // here would make the caller retry and insert a duplicate. Never
            // fold across an airborne flush snapshot's prefix; clamped because a
            // DML can drop emptied batches mid-flight, leaving `pinned > len`.
            let pinned = bucket.flush_pinned_prefix.load(Ordering::Relaxed).min(g.len());
            // First index of the trailing all-small run (never below `pinned`).
            let tail_start = if g.len() > MAX_BATCH_COUNT_PER_BUCKET {
                pinned + g[pinned..].iter().rposition(|b| estimate_batch_size(b) > MAX_BATCH_BYTES_FOR_COALESCE).map_or(0, |i| i + 1)
            } else {
                g.len()
            };
            if g.len() - tail_start <= MAX_BATCH_COUNT_PER_BUCKET {
                0
            } else {
                let schema = g[tail_start].schema();
                match arrow::compute::concat_batches(&schema, g[tail_start..].iter()) {
                    Ok(combined) => {
                        let folded_size: usize = g[tail_start..].iter().map(estimate_batch_size).sum();
                        let combined_size = estimate_batch_size(&combined);
                        g.truncate(tail_start);
                        g.push(combined);
                        // Signed delta, not load+store: other paths mutate this
                        // atomic without holding `batches`, so a store would
                        // clobber their subtraction.
                        let delta = combined_size as i64 - folded_size as i64;
                        apply_signed_delta(&bucket.memory_bytes, delta);
                        delta
                    }
                    Err(e) => {
                        warn!(
                            target = "mem_buffer",
                            error = %e,
                            bucket_batch_count = g.len(),
                            "coalesce concat_batches failed; continuing without coalesce (bucket data intact)"
                        );
                        0
                    }
                }
            }
        };
        // `row_count` and the min/max timestamps update OUTSIDE the bucket lock,
        // so they can briefly lag the batches Vec. That is only safe because
        // `row_count` is observability-only: if you ever wire it into a
        // flush-trigger threshold, move the update back inside the lock.
        bucket.row_count.fetch_add(row_count, Ordering::Relaxed);
        bucket.update_timestamps(timestamp_micros);
        // Read pruning needs the span the rows actually occupy; the routing span
        // widens by only one point and would prune buckets still holding rows.
        if let Some((lo, hi)) = ts_bounds {
            bucket.row_min_ts.fetch_min(lo, Ordering::Relaxed);
            bucket.row_max_ts.fetch_max(hi, Ordering::Relaxed);
        }

        Ok((new_size as i64 + coalesce_delta, bucket_id))
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
            row_min_ts: AtomicI64::new(i64::MAX),
            row_max_ts: AtomicI64::new(i64::MIN),
            created_micros: crate::support::now_micros(),
            wal_shard_state: Mutex::new(WalShardState::default()),
            flush_pinned_prefix: AtomicUsize::new(0),
            mutation_gen: AtomicU64::new(0),
            last_wal_pin_micros: AtomicI64::new(crate::support::now_micros()),
            first_wal_pin_micros: AtomicI64::new(i64::MAX),
        }
    }

    /// Note an in-place DML mutation: bump the generation and pin the DML's
    /// WAL entry on this bucket. Call while holding the `batches` lock so
    /// snapshot/drain observe a consistent (rows, gen, holds) triple.
    fn note_dml_mutation(&self, wal_hold: Option<(usize, walrus_rust::WalPosition)>) {
        self.mutation_gen.fetch_add(1, Ordering::Relaxed);
        if let Some((shard, pos)) = wal_hold {
            self.record_wal_append(shard, Some(pos));
        }
    }

    fn record_wal_append(&self, shard: usize, pre_position: Option<walrus_rust::WalPosition>) {
        self.last_wal_pin_micros.fetch_max(crate::support::now_micros(), Ordering::Relaxed);
        self.first_wal_pin_micros.fetch_min(chrono::Utc::now().timestamp_micros(), Ordering::Relaxed);
        if let Some(pos) = pre_position {
            self.wal_shard_state.lock().merge(shard, pos);
        }
    }

    /// Re-pin a flush snapshot's WAL holds and lift the prefix fence — the
    /// snapshotted rows are the bucket's own again (failed commit, or a DML
    /// that dirtied the bucket mid-flight).
    fn restore_holds(&self, positions: &[Option<walrus_rust::WalPosition>]) {
        for (shard, pos) in positions.iter().enumerate() {
            self.record_wal_append(shard, *pos);
        }
        self.flush_pinned_prefix.store(0, Ordering::Relaxed);
    }

    fn snapshot_wal_shard_state(&self, shards_per_topic: usize) -> Vec<Option<walrus_rust::WalPosition>> {
        pad_positions(&self.wal_shard_state.lock().first_positions, shards_per_topic)
    }

    fn update_timestamps(&self, timestamp: i64) {
        self.min_timestamp.fetch_min(timestamp, Ordering::Relaxed);
        self.max_timestamp.fetch_max(timestamp, Ordering::Relaxed);
    }

    /// Atomic snapshot of this bucket's batches + row count, taken under one
    /// lock acquisition so the pair is consistent.
    fn snapshot(&self) -> (Vec<RecordBatch>, usize) {
        let snap = self.batches.lock().to_vec();
        let n = snap.iter().map(|b| b.num_rows()).sum();
        (snap, n)
    }
}

impl MemBuffer {
    /// Atomic snapshot + text-match search for one bucket. The cache hit is
    /// gated on `indexed_rows == snapshot_rows`.
    ///
    /// `Ok((snapshot, None))` means "no usable text index for this table" or
    /// "no preds passed" — the caller falls back to running the original SQL
    /// predicate on the snapshot.
    fn search_with_snapshot(
        &self, bucket: &TimeBucket, cache_key: &BucketCacheKey, table_schema: &crate::schema::TableSchema, node: &crate::tantivy::udf::PredNode,
    ) -> anyhow::Result<(Vec<RecordBatch>, Option<std::collections::HashSet<String>>)> {
        let (snapshot, snapshot_rows) = bucket.snapshot();
        if snapshot.is_empty() {
            return Ok((snapshot, None));
        }

        // Index build/search failures must DEGRADE to the unfiltered snapshot
        // (ids=None → the SQL predicate still filters), never error: the scan
        // maps an Err to "no MemBuffer data", silently dropping every
        // acked-but-unflushed row.
        let idx = match self.cache_get(cache_key).filter(|i| i.indexed_rows == snapshot_rows) {
            Some(hit) => hit,
            None => match crate::tantivy::BucketTextIndex::build(table_schema, &snapshot, snapshot_rows) {
                Ok(Some(built)) => self.cache_put(cache_key.clone(), Arc::new(built)),
                Ok(None) => return Ok((snapshot, None)),
                Err(e) => {
                    warn!("mem text index build failed (degrading to unfiltered snapshot): {e}");
                    return Ok((snapshot, None));
                }
            },
        };

        let hits = idx.search_node(node).map_err(|e| warn!("mem text index search failed (degrading to unfiltered snapshot): {e}")).ok();
        Ok((snapshot, hits.map(|hits| hits.into_iter().map(|h| h.id).collect())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Int64Array, StringArray, StringViewArray, TimestampMicrosecondArray},
        datatypes::{
            DataType::{self, Boolean, Int64, Utf8View},
            Field, Schema, TimeUnit,
        },
    };
    use test_case::test_case;

    use super::*;

    fn ts_ty() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    }

    /// `(name, type, nullable)` triples → a schema.
    fn schema_of(fields: impl IntoIterator<Item = (&'static str, DataType, bool)>) -> SchemaRef {
        Arc::new(Schema::new(fields.into_iter().map(|(n, t, nullable)| Field::new(n, t, nullable)).collect::<Vec<_>>()))
    }

    fn n_rows<'a>(batches: impl IntoIterator<Item = &'a RecordBatch>) -> usize {
        batches.into_iter().map(RecordBatch::num_rows).sum()
    }

    #[test]
    fn declared_batch_canonicalization_reorders_casts_and_fills_nullable_fields() {
        let incoming_schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true), Field::new("value", DataType::Int64, true)]));
        let incoming = RecordBatch::try_new(incoming_schema, vec![Arc::new(StringArray::from(vec!["x"])), Arc::new(Int64Array::from(vec![7]))]).unwrap();
        let declared = schema_of([("value", Int64, false), ("name", Utf8View, false), ("optional", Int64, true)]);

        let got = canonicalize_declared_batch(incoming, Some(&declared)).unwrap();
        assert_eq!(got.schema(), declared);
        assert_eq!(got.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 7);
        assert_eq!(got.column(1).as_any().downcast_ref::<StringViewArray>().unwrap().value(0), "x");
        assert!(got.column(2).is_null(0));
    }

    #[test]
    fn declared_batch_canonicalization_preserves_intentional_partial_batches() {
        let incoming = RecordBatch::try_from_iter(vec![("value", Arc::new(Int64Array::from(vec![7])) as ArrayRef)]).unwrap();
        let declared = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false), Field::new("required", DataType::Utf8, false)]));
        let got = canonicalize_declared_batch(incoming.clone(), Some(&declared)).unwrap();
        assert_eq!(got.schema(), incoming.schema());
        assert_eq!(got.num_rows(), 1);
    }

    /// Adjacent ranges (`end == next start`) collapse; disjoint ones must NOT —
    /// merging a gap would hide the Delta rows in it.
    #[test]
    fn merge_ranges_collapses_contiguous_windows_only() {
        assert_eq!(merge_ranges(vec![(20, 30), (0, 10), (10, 20)]), vec![(0, 30)]);
        assert_eq!(merge_ranges(vec![(0, 10), (11, 20)]), vec![(0, 10), (11, 20)]);
        assert_eq!(merge_ranges(vec![(0, 30), (5, 10)]), vec![(0, 30)]);
        assert!(merge_ranges(vec![]).is_empty());
        assert!(overlaps((0, 10), (9, 20)) && !overlaps((0, 10), (10, 20)));
    }

    /// Bucket-id derivation must cover every row and invent none.
    #[test]
    fn batch_bucket_ids_covers_every_row_and_dedups() {
        let d = bucket_duration_micros();
        let schema = Arc::new(Schema::new(vec![Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false)]));
        let batch = |ts: Vec<i64>| RecordBatch::try_new(schema.clone(), vec![Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC"))]).unwrap();

        assert_eq!(batch_bucket_ids(&batch(vec![0, 1, d]), "timestamp"), vec![0, 1]);
        assert_eq!(batch_bucket_ids(&batch(vec![5 * d, 0]), "timestamp"), vec![0, 5]);
        assert!(batch_bucket_ids(&batch(vec![0]), "no_such_column").is_empty());
    }

    /// Batch shaped like a client-built insert: every field marked nullable even
    /// though the columns are fully populated.
    fn nullable_otel_batch(with_null_id: bool) -> RecordBatch {
        let schema = schema_of([("timestamp", ts_ty(), true), ("id", Utf8View, true)]);
        let ts = TimestampMicrosecondArray::from(vec![1_000i64, 2_000]).with_timezone("UTC");
        let id = StringViewArray::from(vec![Some("a"), if with_null_id { None } else { Some("b") }]);
        RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(id)]).unwrap()
    }

    fn nullable_test_declared_schema() -> SchemaRef {
        schema_of([("timestamp", ts_ty(), false), ("id", Utf8View, false)])
    }

    /// The declared schema decides nullability, except that a declared-NOT-NULL
    /// column really holding nulls stays nullable. Returns
    /// `(timestamp_nullable, id_nullable)` of the canonicalized batch.
    #[test_case(false => (false, false) ; "declared NOT NULL beats the first batch's nullable=true")]
    #[test_case(true => (false, true) ; "a declared-NOT-NULL column that really holds nulls stays nullable")]
    fn canonicalization_takes_nullability_from_declared_schema_and_the_data(with_null_id: bool) -> (bool, bool) {
        let stored = canonicalize_declared_batch(nullable_otel_batch(with_null_id), Some(&nullable_test_declared_schema())).unwrap();
        let schema = stored.schema();
        (schema.field_with_name("timestamp").unwrap().is_nullable(), schema.field_with_name("id").unwrap().is_nullable())
    }

    /// Alignment is name+exact-type only, so it must leave unknown tables and
    /// mismatched types alone rather than silently re-typing a column.
    #[test]
    fn nullability_alignment_ignores_unknown_tables_and_mismatched_types() {
        let buffer = MemBuffer::new();
        buffer.insert("p1", "not_a_registered_table", nullable_otel_batch(false), 1_000).unwrap();
        assert!(buffer.query("p1", "not_a_registered_table", &[]).unwrap()[0].schema().field_with_name("timestamp").unwrap().is_nullable());

        let declared = crate::schema::get_schema("otel_logs_and_spans").unwrap().schema_ref();
        let mistyped = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        assert!(align_nullability(&mistyped, &declared, None).is_none());
    }

    /// `sort_partition` must return bounded chunks with the order surviving the
    /// chunking.
    #[test]
    fn sort_partition_orders_descending_and_chunks_the_result() {
        let schema = crate::schema::get_schema("mor_versioned").expect("fixture registered");
        assert_eq!(schema.sorting_columns[0].name, "timestamp", "this test asserts a `timestamp DESC` ordering");
        assert!(schema.sorting_columns[0].descending);

        // Scrambled event-time order: an arrival-ordered fixture would come out
        // sorted even if nothing sorted.
        const N: i64 = SORT_CHUNK_ROWS as i64 * 2 + 17;
        let batches: Vec<RecordBatch> = (0..N).map(|i| create_test_batch((i * 7919) % N)).collect();
        let sorted = sort_partition(schema, batches).expect("a partition with the sorting column must sort");

        assert!(sorted.len() > 1, "the result must be chunked, got {} batch(es)", sorted.len());
        assert!(sorted.iter().all(|b| b.num_rows() <= SORT_CHUNK_ROWS), "no chunk may exceed the bound");
        assert_eq!(sorted.iter().map(|b| b.num_rows()).sum::<usize>(), N as usize, "sorting must not lose or duplicate rows");

        let ts: Vec<i64> = sorted
            .iter()
            .flat_map(|b| {
                let c = b.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>().expect("timestamp column");
                c.values().to_vec()
            })
            .collect();
        assert!(ts.windows(2).all(|w| w[0] >= w[1]), "rows must be ordered timestamp DESC across chunk boundaries");
        assert_eq!(ts.first().copied(), Some(N - 1), "greatest timestamp first");
        assert_eq!(ts.last().copied(), Some(0), "least timestamp last");
    }

    /// The row span must stay a SEPARATE pair from the routing span: widening
    /// `min/max_timestamp` also moves the span `get_bucket_ranges` masks Delta
    /// over. Merging the two is wrong.
    #[test]
    fn row_span_tracks_rows_while_routing_span_stays_put() {
        let schema = schema_of([("timestamp", ts_ty(), false), ("id", Int64, false)]);
        let (lo, mid, hi) = (1_735_678_800_000_000i64, 1_735_682_400_000_000, 1_735_686_000_000_000);
        let cols: Vec<ArrayRef> =
            vec![Arc::new(TimestampMicrosecondArray::from(vec![lo, mid, hi]).with_timezone("UTC")), Arc::new(Int64Array::from(vec![1, 2, 3]))];
        let batch = RecordBatch::try_new(schema, cols).unwrap();

        let buffer = MemBuffer::new();
        buffer.insert_with_hold("p", "otel_logs_and_spans", batch, lo, None).expect("insert");
        let table = buffer.tables.get(&table_key("p", "otel_logs_and_spans")).expect("table");
        let bucket = table.buckets.iter().next().expect("one bucket");

        assert_eq!(bucket.row_max_ts.load(Ordering::Relaxed), hi, "row span must reach the batch's latest row");
        assert_eq!(bucket.row_min_ts.load(Ordering::Relaxed), lo, "row span must start at the batch's earliest row");
        assert_eq!(bucket.max_timestamp.load(Ordering::Relaxed), lo, "the routing span must stay on the routing timestamp");

        assert!(
            bucket_overlaps_range(bucket.value(), &(Some(lo + 1), None)),
            "a lower bound above the first row must not prune a bucket still holding later rows"
        );
        assert!(!bucket_overlaps_range(bucket.value(), &(Some(hi + 1), None)), "a bound above every row must still prune");
    }

    /// The same row plus one EXTRA nullable column, which `insert_batch`
    /// deliberately accepts.
    fn create_test_batch_with_extra_field(timestamp_micros: i64) -> RecordBatch {
        let schema = schema_of([("timestamp", ts_ty(), false), ("id", Int64, false), ("name", Utf8View, false), ("extra", Utf8View, true)]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(TimestampMicrosecondArray::from(vec![timestamp_micros]).with_timezone("UTC")),
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(StringViewArray::from(vec!["test"])),
            Arc::new(StringViewArray::from(vec![Some("new-field")])),
        ];
        RecordBatch::try_new(schema, cols).unwrap()
    }

    /// ONE schema-diverse partition retracts the ordering claim for the WHOLE
    /// leg, and `insert_batch` accepts nullable field additions by design, so
    /// one new optional field is enough to trigger it.
    #[test]
    fn one_schema_diverse_partition_retracts_the_whole_legs_ordering() {
        let schema = crate::schema::get_schema("mor_versioned").expect("fixture registered");
        let ts = 1_700_000_000_000_000i64;

        let uniform = vec![create_test_batch(ts), create_test_batch(ts + 1)];
        assert!(sort_partition(schema, uniform).is_some(), "a uniform partition must sort, or this test proves nothing");

        let diverse = vec![create_test_batch(ts), create_test_batch_with_extra_field(ts + 1)];
        assert!(sort_partition(schema, diverse).is_none(), "a partition mixing schemas is refused, which is what retracts the ordering for the entire leg");
    }

    /// One `(timestamp, id, name)` batch — the shared shape of most tests here.
    fn tin_batch(ts: Vec<i64>, ids: Vec<i64>, names: Vec<String>) -> RecordBatch {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringViewArray::from(names.iter().map(String::as_str).collect::<Vec<_>>())),
        ];
        RecordBatch::try_new(schema_of([("timestamp", ts_ty(), false), ("id", Int64, false), ("name", Utf8View, false)]), cols).unwrap()
    }

    fn create_test_batch(timestamp_micros: i64) -> RecordBatch {
        tin_batch(vec![timestamp_micros], vec![1], vec!["test".into()])
    }

    /// One ~3KB-logical row across 64 Utf8View columns plus view columns nested
    /// in List and Struct, built the way the pgwire fast-insert path builds rows.
    fn wide_view_row(ts: i64) -> RecordBatch {
        use datafusion::common::ScalarValue;
        let n_str_cols = 64;
        let mut fields: Vec<Field> = vec![Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false)];
        fields.extend((0..n_str_cols).map(|i| Field::new(format!("c{i}"), DataType::Utf8View, true)));
        let item = Arc::new(Field::new("item", DataType::Utf8View, true));
        fields.push(Field::new("l", DataType::List(item.clone()), true));
        fields.push(Field::new("s", DataType::Struct(vec![Field::new("v", DataType::Utf8View, true)].into()), true));

        let view_col = || ScalarValue::Utf8View(Some("a string too long to inline in the view".into())).to_array_of_size(1).unwrap();
        let mut cols: Vec<ArrayRef> = vec![Arc::new(TimestampMicrosecondArray::from(vec![ts]).with_timezone("UTC"))];
        cols.extend((0..n_str_cols).map(|_| view_col()));
        cols.push(Arc::new(arrow::array::ListArray::new(item, arrow::buffer::OffsetBuffer::from_lengths([1]), view_col(), None)));
        cols.push(Arc::new(arrow::array::StructArray::new(vec![Field::new("v", DataType::Utf8View, true)].into(), vec![view_col()], None)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    /// The WAL GC floor must be the APPEND time: stamping event time lets one
    /// old-event-time backfill row suspend WAL file GC for days.
    #[test]
    fn live_insert_gc_floor_is_append_time_not_event_time() {
        let buffer = MemBuffer::new();
        let ten_days_ago = chrono::Utc::now().timestamp_micros() - 10 * 24 * 3600 * 1_000_000;
        buffer.insert("p1", "t1", create_test_batch(ten_days_ago), ten_days_ago).unwrap();
        let floor = buffer.oldest_wal_append_micros().expect("un-flushed bucket must floor GC");
        let hour = 3600 * 1_000_000;
        assert!(floor > chrono::Utc::now().timestamp_micros() - hour, "backfill event time leaked into the GC floor: {floor} (≈10 days old)");
        buffer.record_replay_hold("p1", "t1", ten_days_ago, 0, walrus_rust::WalPosition::ORIGIN);
        assert_eq!(buffer.oldest_wal_append_micros(), Some(ten_days_ago), "replay pins must keep the original append time as the floor");
    }

    /// The GC-floor pin must stay visible from `take_bucket_for_flush` until
    /// `release_taking_pin`, or a GC sweep in that gap deletes the airborne
    /// bucket's backing WAL file.
    #[test]
    fn taken_bucket_pin_stays_visible_until_released() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        buffer.insert("p1", "t1", create_test_batch(ts), ts).unwrap();
        let pin_before = buffer.oldest_wal_append_micros().expect("insert must set the floor");

        let bucket_id = MemBuffer::compute_bucket_id(ts);
        let taken = buffer.take_bucket_for_flush("p1", "t1", bucket_id).expect("bucket must be takeable");
        assert_eq!(buffer.oldest_wal_append_micros(), Some(pin_before), "floor must not blink out between take and inflight-pin registration");
        buffer.release_taking_pin(taken.taking_pin_seq);
        assert_eq!(buffer.oldest_wal_append_micros(), None, "released take must drop the floor");
    }

    /// One row whose 8 Utf8View columns are SLICES of a large scan block (the
    /// DML re-insert shape: capacity == len, so only referenced bytes differ).
    fn sliced_scan_row(ts: i64) -> RecordBatch {
        let big: Vec<String> = (0..1000).map(|i| format!("{i:0>100}")).collect();
        let full = StringViewArray::from(big.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        let mut fields: Vec<Field> = vec![Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false)];
        fields.extend((0..8).map(|i| Field::new(format!("c{i}"), DataType::Utf8View, true)));
        let mut cols: Vec<ArrayRef> = vec![Arc::new(TimestampMicrosecondArray::from(vec![ts]).with_timezone("UTC"))];
        cols.extend((0..8).map(|_| Arc::new(full.slice(0, 1)) as ArrayRef));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    /// Bytes MemBuffer charges for `n` batches from `mk`, all inserted at ONE
    /// timestamp so they land in one bucket and the coalesce/fold path runs.
    fn charged_bytes(n: usize, mk: fn(i64) -> RecordBatch) -> usize {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        for _ in 0..n {
            buffer.insert("p1", "t1", mk(ts), ts).unwrap();
        }
        buffer.estimated_memory_bytes()
    }

    // Memory accounting must charge the bytes the views actually reference, not
    // the capacity of the column-chunk block they were sliced from.
    #[test_case(1, wide_view_row, 64 * 1024 ; "single ~3KB-logical wide-Utf8View row")]
    #[test_case(1, sliced_scan_row, 32 * 1024 ; "1 sliced row, 8 view cols x 100B referenced")]
    #[test_case(200, wide_view_row, 16 * 1024 * 1024 ; "200 rows: fold outputs stay near logical size")]
    fn view_rows_charged_logical_size_not_block_capacity(n: usize, mk: fn(i64) -> RecordBatch, limit: usize) {
        let charged = charged_bytes(n, mk);
        assert!(charged < limit, "{n} ~3KB-logical row(s) charged {charged} bytes (limit {limit}) — view block capacity is leaking into memory accounting");
    }

    // Coalescing must keep working on the small tail no matter how large the
    // bucket grows.
    #[test]
    fn bucket_keeps_coalescing_past_4mb() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let payload = "x".repeat(64 * 1024);
        let schema = schema_of([("timestamp", ts_ty(), false), ("body", Utf8View, false)]);
        for _ in 0..200 {
            let cols: Vec<ArrayRef> =
                vec![Arc::new(TimestampMicrosecondArray::from(vec![ts]).with_timezone("UTC")), Arc::new(StringViewArray::from(vec![payload.as_str()]))];
            buffer.insert("p1", "t1", RecordBatch::try_new(schema.clone(), cols).unwrap(), ts).unwrap();
        }
        let table = buffer.tables.get(&table_key("p1", "t1")).unwrap();
        let bucket = table.buckets.get(&MemBuffer::compute_bucket_id(ts)).unwrap();
        let g = bucket.batches.lock();
        let n_batches = g.len();
        assert_eq!(n_rows(g.iter()), 200, "coalesce must not lose rows");
        assert!(
            n_batches <= 2 * (MAX_BATCH_COUNT_PER_BUCKET + 1),
            "bucket holds {n_batches} batches — coalesce stopped once the bucket crossed MAX_BATCH_BYTES_FOR_COALESCE"
        );
    }

    /// Every row's `name` column across `batches`, in order.
    fn col_strings(batches: &[RecordBatch], name: &str) -> Vec<String> {
        batches
            .iter()
            .flat_map(|b| {
                let a = b.column_by_name(name).unwrap().as_any().downcast_ref::<StringViewArray>().unwrap();
                (0..b.num_rows()).map(|i| a.value(i).to_string()).collect::<Vec<_>>()
            })
            .collect()
    }

    fn col_i64(batches: &[RecordBatch], name: &str) -> Vec<i64> {
        batches.iter().flat_map(|b| b.column_by_name(name).unwrap().as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()).collect()
    }

    /// `[id, <stamp_col>?, payload]` rows — the shared shape of the dedup tests.
    /// `stamp_col: None` omits the stamp column (the legacy batch shape).
    fn key_batch(stamp_col: Option<&str>, rows: &[(i64, Option<i64>, &str)]) -> RecordBatch {
        let mut fields = vec![Field::new("id", Int64, false)];
        let mut cols: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>()))];
        if let Some(c) = stamp_col {
            fields.push(Field::new(c, ts_ty(), true));
            cols.push(Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>()).with_timezone("UTC")));
        }
        fields.push(Field::new("payload", DataType::Utf8View, false));
        cols.push(Arc::new(StringViewArray::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    #[test]
    fn dedup_batches_keep_last_on_composite_key() {
        let batches = vec![
            key_batch(Some("timestamp"), &[(1, Some(100), "v1-old"), (2, Some(200), "v2-old")]),
            key_batch(Some("timestamp"), &[(1, Some(100), "v1-new"), (3, Some(300), "v3")]),
            key_batch(Some("timestamp"), &[(2, Some(200), "v2-new")]),
        ];
        let keys = vec!["id".to_string(), "timestamp".to_string()];
        let out = dedup_batches(batches, &keys, None, None).expect("dedup ok");
        // Dedup filters each batch in place, so survivors come back as multiple batches.
        assert_eq!(n_rows(&out), 3, "should collapse to 3 unique (id,ts)");
        let got: Vec<(i64, String)> = col_i64(&out, "id").into_iter().zip(col_strings(&out, "payload")).collect();
        assert_eq!(got, vec![(1, "v1-new".into()), (3, "v3".into()), (2, "v2-new".into())]);
    }

    /// Keep-greatest-per-key on the tiebreak column: a NULL tiebreak sorts
    /// LOWEST in either arrival order, input position never decides, and a batch
    /// with no tiebreak column degrades to last-occurrence-wins instead of
    /// failing — failing makes such buckets permanently unflushable.
    /// Rows are sorted before comparison: the batch order out of dedup is incidental.
    #[test_case(Some("updated_at"), Some("updated_at"), &[&[(1, None, "legacy")], &[(1, Some(10), "stamped")]]
        => vec![(1, "stamped".to_string())] ; "NULL tiebreak arrives first and still loses")]
    #[test_case(Some("updated_at"), Some("updated_at"), &[&[(1, Some(10), "stamped")], &[(1, None, "legacy")]]
        => vec![(1, "stamped".to_string())] ; "NULL tiebreak arrives last and still loses")]
    #[test_case(Some("observed"), Some("observed"), &[&[(1, Some(200), "1-enriched"), (2, None, "2-base")], &[(1, Some(100), "1-base"), (2, Some(50), "2-enriched")]]
        => vec![(1, "1-enriched".to_string()), (2, "2-enriched".to_string())] ; "greatest tiebreak wins per key whatever its input position")]
    #[test_case(None, Some("updated_at"), &[&[(1, None, "old")], &[(1, None, "new")]]
        => vec![(1, "new".to_string())] ; "a legacy batch with no tiebreak column flushes as last-occurrence-wins")]
    fn dedup_batches_tiebreak_keeps_greatest(stamp_col: Option<&str>, tiebreak: Option<&str>, rows: &[&[(i64, Option<i64>, &str)]]) -> Vec<(i64, String)> {
        let batches = rows.iter().map(|r| key_batch(stamp_col, r)).collect();
        let out = dedup_batches(batches, &["id".to_string()], tiebreak, None).expect("dedup ok");
        let mut got: Vec<(i64, String)> = col_i64(&out, "id").into_iter().zip(col_strings(&out, "payload")).collect();
        got.sort();
        got
    }

    #[test]
    fn dedup_batches_noop_when_keys_empty_or_input_empty() {
        let empty: Vec<RecordBatch> = vec![];
        assert!(dedup_batches(empty, &["id".to_string()], None, None).unwrap().is_empty());

        let batch = create_test_batch(123);
        let out = dedup_batches(vec![batch.clone()], &[], None, None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), batch.num_rows());
    }

    /// Dedup must not fuse its inputs into one RecordBatch — that overflows
    /// Arrow's 2GB i32 string-offset limit on a large flush.
    #[test]
    fn dedup_batches_does_not_concatenate_full_payload() {
        let batches = vec![key_batch(None, &[(1, None, "a")]), key_batch(None, &[(2, None, "b")]), key_batch(None, &[(3, None, "c")])];
        let out = dedup_batches(batches, &["id".to_string()], None, None).expect("dedup ok");
        assert_eq!(out.len(), 3, "distinct-key batches must be returned un-fused (no 2GB-prone concat)");
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    #[test]
    fn dedup_batches_errors_on_unknown_key() {
        let err = dedup_batches(vec![create_test_batch(1)], &["nonexistent".to_string()], None, None).unwrap_err();
        assert!(err.to_string().contains("nonexistent"), "msg: {err}");
        assert!(
            dedup_batches(vec![key_batch(None, &[(1, None, "x")])], &["nope".to_string()], None, None).is_err(),
            "a missing dedup key must still fail loudly"
        );
    }

    /// Merge-on-read version collapse: the survivor is the greatest `updated_at`
    /// per id, and `deleted=true` on that survivor retires the key only when the
    /// caller declares its scope safe (`drop_tombstones = Some("deleted")`).
    mod tombstones {
        use test_case::test_case;

        use super::*;

        fn schema() -> SchemaRef {
            schema_of([("id", Int64, false), ("updated_at", ts_ty(), true), ("deleted", Boolean, true), ("payload", Utf8View, false)])
        }

        /// One row per batch, so input order is explicit at every call site.
        fn row(id: i64, updated_at: Option<i64>, deleted: Option<bool>, payload: &str) -> RecordBatch {
            let cols: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(TimestampMicrosecondArray::from(vec![updated_at]).with_timezone("UTC")),
                Arc::new(BooleanArray::from(vec![deleted])),
                Arc::new(StringViewArray::from(vec![payload])),
            ];
            RecordBatch::try_new(schema(), cols).unwrap()
        }

        fn collapse(batches: Vec<RecordBatch>, drop_tombstones: Option<&str>) -> Vec<(i64, String, Option<bool>)> {
            let out = dedup_batches(batches, &["id".to_string()], Some("updated_at"), drop_tombstones).expect("collapse ok");
            let deleted: Vec<Option<bool>> = out
                .iter()
                .flat_map(|b| {
                    let d = b.column_by_name("deleted").unwrap().as_any().downcast_ref::<BooleanArray>().unwrap();
                    (0..b.num_rows()).map(|i| d.is_valid(i).then(|| d.value(i))).collect::<Vec<_>>()
                })
                .collect();
            let mut got: Vec<(i64, String, Option<bool>)> = itertools::izip!(col_i64(&out, "id"), col_strings(&out, "payload"), deleted).collect();
            got.sort();
            got
        }

        /// A tombstone is just another version: it wins on `updated_at`, not on
        /// being a tombstone. Dropping takes the whole key, and is only correct
        /// when the caller's scope holds every version of that key — otherwise
        /// an older version elsewhere resurrects the row.
        #[test_case(&[(1, Some(10), None, "live"), (1, Some(20), Some(true), "deleted")], None
            => vec![(1, "deleted".to_string(), Some(true))] ; "tombstone beats older live version")]
        #[test_case(&[(1, Some(20), Some(true), "deleted"), (1, Some(10), None, "live")], None
            => vec![(1, "deleted".to_string(), Some(true))] ; "late older live version does not resurrect")]
        #[test_case(&[(1, Some(20), Some(true), "deleted"), (1, Some(10), None, "live")], Some("deleted")
            => Vec::<(i64, String, Option<bool>)>::new() ; "tombstone drop takes the whole key, not just the tombstone row")]
        #[test_case(&[(1, Some(10), Some(false), "live"), (1, Some(20), Some(true), "deleted"), (2, Some(5), None, "untouched")], None
            => vec![(1, "deleted".to_string(), Some(true)), (2, "untouched".to_string(), None)] ; "tombstone retained unless caller declares scope safe")]
        #[test_case(&[(1, Some(10), Some(false), "live"), (1, Some(20), Some(true), "deleted"), (2, Some(5), None, "untouched")], Some("deleted")
            => vec![(2, "untouched".to_string(), None)] ; "scope-safe caller drops the tombstoned key only")]
        #[test_case(&[(1, Some(20), Some(true), "deleted")], Some("deleted")
            => Vec::<(i64, String, Option<bool>)>::new() ; "lone tombstone drops without a duplicate")]
        #[test_case(&[(1, Some(20), Some(true), "deleted"), (1, Some(30), Some(false), "reinserted")], Some("deleted")
            => vec![(1, "reinserted".to_string(), Some(false))] ; "newer live version un-deletes the key")]
        #[test_case(&[(1, Some(10), None, "a"), (2, Some(20), None, "b")], Some("deleted")
            => vec![(1, "a".to_string(), None), (2, "b".to_string(), None)] ; "all-NULL deleted column is live under the drop policy")]
        #[test_case(&[(1, Some(10), None, "a"), (2, Some(20), None, "b")], None
            => vec![(1, "a".to_string(), None), (2, "b".to_string(), None)] ; "all-NULL deleted column is unchanged without the drop policy")]
        fn version_collapse(rows: &[(i64, Option<i64>, Option<bool>, &str)], drop_tombstones: Option<&str>) -> Vec<(i64, String, Option<bool>)> {
            collapse(rows.iter().map(|&(id, updated_at, deleted, payload)| row(id, updated_at, deleted, payload)).collect(), drop_tombstones)
        }

        /// With no tombstone present the drop policy must preserve the
        /// untouched-batches fast path, not re-filter every batch.
        #[test]
        fn all_null_tombstone_column_keeps_the_untouched_fast_path() {
            let all_null = vec![row(1, Some(10), None, "a"), row(2, Some(20), None, "b")];
            let out = dedup_batches(all_null, &["id".to_string()], Some("updated_at"), Some("deleted")).expect("collapse ok");
            assert_eq!(out.len(), 2, "no tombstones present → batches returned untouched, not re-filtered");
        }

        /// The tombstone argument is the only thing that can change the row set,
        /// even when a Boolean column happens to be named `deleted` and set.
        #[test]
        fn no_tombstone_column_means_unchanged_behaviour() {
            let batches = vec![row(1, Some(10), Some(true), "a"), row(2, Some(20), Some(true), "b")];
            let out = dedup_batches(batches, &["id".to_string()], Some("updated_at"), None).expect("collapse ok");
            assert_eq!(out.len(), 2, "distinct keys, no drop → inputs returned un-fused");
            assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        }

        /// A non-Boolean tombstone column is a schema error, not a silent mis-drop.
        #[test]
        fn non_boolean_tombstone_column_errors() {
            let err = dedup_batches(vec![row(1, Some(10), None, "a")], &["id".to_string()], Some("updated_at"), Some("payload")).unwrap_err();
            assert!(err.to_string().contains("must be Boolean"), "msg: {err}");
        }
    }

    /// One insert is queryable at any event time — a pre-epoch bucket included.
    #[test_case(1_700_000_000_000_000 ; "a recent event time")]
    #[test_case(-BUCKET_DURATION_MICROS * 2 ; "20 minutes before epoch is still queryable")]
    fn test_insert_and_query(ts: i64) {
        let buffer = MemBuffer::new();
        buffer.insert("project1", "table1", create_test_batch(ts), ts).unwrap();

        let results = buffer.query("project1", "table1", &[]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
    }

    /// A real `otel_logs_and_spans` batch of `(id, service_name)` rows.
    fn spans(rows: &[(&str, &str)]) -> RecordBatch {
        use crate::support::test_helpers::{json_to_batch, test_span};
        json_to_batch(rows.iter().map(|&(id, svc)| test_span(id, svc, "p1")).collect()).expect("json_to_batch")
    }

    fn name_preds(query: &str) -> Vec<crate::tantivy::udf::TextMatchPred> {
        vec![crate::tantivy::udf::TextMatchPred { column: "name".into(), query: query.into() }]
    }

    /// `table1` is not in the YAML schema registry, so it has no indexed fields
    /// and must return None for the caller to fall back on. Returns
    /// `(row-1 in the hit set, row-2 in the hit set)`.
    #[test_case("otel_logs_and_spans", "auth" => Some((true, false)) ; "row-1 (auth-svc) hits, row-2 (billing-svc) does not")]
    #[test_case("table1", "test" => None ; "unindexed table returns None so the caller falls back")]
    fn search_text_match_returns_matching_ids_from_membuffer(table: &str, query: &str) -> Option<(bool, bool)> {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let batch = if table == "table1" { create_test_batch(ts) } else { spans(&[("row-1", "auth-svc"), ("row-2", "billing-svc")]) };
        buffer.insert("p1", table, batch, ts).unwrap();

        let ids = buffer.search_text_match("p1", table, &name_preds(query)).expect("search")?;
        Some((ids.contains("row-1"), ids.contains("row-2")))
    }

    #[test]
    fn search_text_match_cache_invalidates_on_insert() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        buffer.insert("p1", "otel_logs_and_spans", spans(&[("a", "alpha-svc")]), ts).unwrap();
        let preds = name_preds("beta");
        let initial = buffer.search_text_match("p1", "otel_logs_and_spans", &preds).unwrap().unwrap();
        assert!(initial.is_empty(), "no 'beta' row inserted yet");

        buffer.insert("p1", "otel_logs_and_spans", spans(&[("b", "beta-svc")]), ts + 1).unwrap();
        let post = buffer.search_text_match("p1", "otel_logs_and_spans", &preds).unwrap().unwrap();
        assert!(post.contains("b"), "expected 'b' after insert+rebuild, got {:?}", post);
    }

    /// Atomicity invariant: `query_partitioned_with_text_match` returns batches
    /// filtered against an id set taken from the SAME snapshot — a row must be
    /// in both or in neither, never in the snapshot but missing from the id set.
    /// Returns the returned ids, sorted (the scan promises no insertion order).
    #[test_case(Some("alpha") => vec!["hit-1".to_string()] ; "only the matching row survives the snapshot's id set")]
    #[test_case(None => vec!["hit-1".to_string(), "miss-1".to_string()] ; "no text_match preds falls through to query_partitioned")]
    fn query_partitioned_with_text_match_returns_atomic_snapshot(query: Option<&str>) -> Vec<String> {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        buffer.insert("p1", "otel_logs_and_spans", spans(&[("hit-1", "alpha-search-svc"), ("miss-1", "completely-unrelated-svc")]), ts).unwrap();

        let preds = query.map(name_preds);
        let node = preds.as_deref().and_then(crate::tantivy::udf::PredNode::from_preds);
        let parts = buffer.query_partitioned_with_text_match("p1", "otel_logs_and_spans", &[], node.as_ref()).unwrap();
        col_strings(&parts.partitions.concat(), "id").into_iter().sorted().collect()
    }

    /// `restore_taken_bucket` must replay the rows' real timestamp range, not
    /// the bucket start, or restored rows are hidden from time-range pruning.
    #[test]
    fn restore_taken_bucket_preserves_timestamp_range() {
        let buffer = MemBuffer::new();
        let dur = bucket_duration_micros();
        let ts = 7 * dur + 12_345; // mid-bucket — distinct from the bucket-start sentinel
        let bucket_id = MemBuffer::compute_bucket_id(ts);
        buffer.insert("p1", "otel_logs_and_spans", spans(&[("a", "svc")]), ts).unwrap();

        let taken = buffer.take_bucket_for_flush("p1", "otel_logs_and_spans", bucket_id).expect("bucket taken");
        assert_eq!((taken.min_timestamp, taken.max_timestamp), (ts, ts), "take must capture the real row range");

        assert!(buffer.restore_taken_bucket(&taken), "restore into a live table must succeed"); // simulate Delta commit failure
        let again = buffer.take_bucket_for_flush("p1", "otel_logs_and_spans", bucket_id).expect("restored bucket present");
        assert_eq!((again.min_timestamp, again.max_timestamp), (ts, ts), "restore must preserve the true range");
        assert_ne!(again.min_timestamp, bucket_id * dur, "must not collapse to bucket start");
    }

    /// Bucket membership is half-open, so adjacent windows and the microsecond
    /// before a boundary land in separate buckets. Returns
    /// `(query batches, total buckets)`.
    #[test_case(&[5 * BUCKET_DURATION_MICROS, 6 * BUCKET_DURATION_MICROS] => (2, 2) ; "adjacent buckets")]
    #[test_case(&[BUCKET_DURATION_MICROS] => (1, 1) ; "the boundary instant alone opens exactly one bucket")]
    #[test_case(&[BUCKET_DURATION_MICROS, BUCKET_DURATION_MICROS - 1] => (2, 2) ; "one microsecond earlier belongs to the previous bucket")]
    fn inserts_land_in_half_open_buckets(timestamps: &[i64]) -> (usize, usize) {
        let buffer = MemBuffer::new();
        for &ts in timestamps {
            buffer.insert("project1", "table1", create_test_batch(ts), ts).unwrap();
        }
        (buffer.query("project1", "table1", &[]).unwrap().len(), buffer.get_stats().total_buckets)
    }

    /// Start of a sealed (no longer current) bucket window, aligned to the window
    /// so `ts + 60s` stays in the same bucket; `get_bucket_ranges` only reports
    /// non-current buckets. Wall clock, not the virtual clock, on purpose.
    fn sealed_bucket_start() -> i64 {
        (chrono::Utc::now().timestamp_micros() - 2 * BUCKET_DURATION_MICROS) / BUCKET_DURATION_MICROS * BUCKET_DURATION_MICROS
    }

    #[test]
    fn merge_snapshot_preserves_rows_and_delete_exclusions_across_flush() {
        let buffer = MemBuffer::new();
        let ts = sealed_bucket_start();
        let bucket_id = MemBuffer::compute_bucket_id(ts);
        let batch = create_test_batch(ts);
        let rows = batch.num_rows();
        buffer.insert("p", "table1", batch, ts).unwrap();
        let before = buffer.snapshot_for_merge("p", "table1", ts, ts + 1).unwrap();
        assert_eq!(before.covered_ranges, vec![(ts, ts + 1)]);
        let flush = buffer.snapshot_bucket_for_flush("p", "table1", bucket_id).unwrap();
        assert_eq!(buffer.delete("p", "table1", None, None).unwrap(), rows as u64);
        let deleted = buffer.snapshot_for_merge("p", "table1", ts, ts + 1).unwrap();
        assert!(deleted.batches.is_empty());
        assert_eq!(deleted.covered_ranges, before.covered_ranges, "deleted rows must still suppress the in-flight Delta copy");
        assert!(!buffer.finish_flushed_snapshot(&flush), "a deleted bucket must invalidate its older flush snapshot");
        assert_eq!(before.batches.iter().map(RecordBatch::num_rows).sum::<usize>(), rows, "later deletes must not alter captured rows");
        buffer.mark_force_flushed("p", "table1", bucket_id);
        assert!(buffer.snapshot_for_merge("p", "table1", ts, ts + 1).unwrap().covered_ranges.is_empty());
        assert_eq!(deleted.covered_ranges, vec![(ts, ts + 1)], "later flush markers must not alter captured exclusions");
        assert!(buffer.snapshot_for_merge("p", "table1", ts + 1, ts + 2).unwrap().covered_ranges.is_empty());
        assert!(buffer.snapshot_for_merge("p", "table1", ts, ts).is_err());
    }

    /// Snapshot-flush lifecycle: rows stay queryable after the snapshot, a
    /// late insert survives the prefix drain, and its post-snapshot hold is
    /// preserved.
    #[test]
    fn snapshot_then_prefix_drain_preserves_late_rows() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let bucket_id = MemBuffer::compute_bucket_id(ts);

        buffer.insert("project1", "table1", create_test_batch(ts), ts).unwrap();
        let snap = buffer.snapshot_bucket_for_flush("project1", "table1", bucket_id).unwrap();
        assert_eq!(snap.batches.len(), 1);
        assert_eq!(buffer.query("project1", "table1", &[]).unwrap().len(), 1, "rows must stay visible while the snapshot is airborne");

        buffer.insert_with_hold("project1", "table1", create_test_batch(ts), ts, Some((0, walrus_rust::WalPosition { block_id: 9, offset: 9 }))).unwrap();

        assert!(buffer.finish_flushed_snapshot(&snap), "clean (non-dirty) snapshot must report drained");
        let remaining = n_rows(&buffer.query("project1", "table1", &[]).unwrap());
        assert_eq!(remaining, create_test_batch(ts).num_rows(), "late rows must survive the prefix drain");
        let holds = buffer.wal_holds("project1", "table1", 4);
        assert!(holds[0].is_some(), "late arrival's hold must survive the prefix drain");
    }

    /// The prefix drain must narrow a surviving bucket's range to the surviving
    /// rows — otherwise `get_bucket_ranges` masks the drained rows' freshly
    /// committed Delta copies. It must not blanket-exempt the bucket either: the
    /// exclusion stays armed for a later DML + airborne-commit race.
    #[test]
    fn prefix_drain_narrows_survivor_range_to_late_rows() {
        let buffer = MemBuffer::new();
        let ts = sealed_bucket_start();
        let bucket_id = MemBuffer::compute_bucket_id(ts);

        buffer.insert("project1", "table1", create_test_batch(ts), ts).unwrap();
        let snap = buffer.snapshot_bucket_for_flush("project1", "table1", bucket_id).unwrap();
        assert!(!buffer.get_bucket_ranges("project1", "table1").is_empty(), "sealed bucket masks Delta pre-flush");

        let late_ts = ts + 60_000_000;
        assert_eq!(MemBuffer::compute_bucket_id(late_ts), bucket_id, "late row must land in the same bucket");
        buffer.insert("project1", "table1", create_test_batch(late_ts), late_ts).unwrap();
        assert!(buffer.finish_flushed_snapshot(&snap));

        let ranges = buffer.get_bucket_ranges("project1", "table1");
        assert_eq!(ranges, vec![(late_ts, late_ts + 1)], "survivor's mask must cover only the late rows so the drained rows' Delta copies stay visible");
    }

    /// A flushed instant must stay visible however the bucket emptied.
    ///
    /// The mask is a time RANGE, so range narrowing alone cannot save a drained
    /// row that shares its timestamp with a survivor — and identical timestamps
    /// are the common case here (a batch of OTel spans arrives stamped to one
    /// instant). Each mode below defeats the narrowing, because the narrowing
    /// `store`s into `min_timestamp` and the INSERT path `fetch_min`s the same
    /// atomic, so any later row at the flushed instant pulls the mask back down
    /// over rows that already left memory. That is why the floor belongs at READ
    /// time in `get_bucket_ranges`, outside the value inserts maintain.
    #[test_case("survivor_at_same_instant"; "a drained row sharing a timestamp with a survivor is masked")]
    #[test_case("take"; "full take, then a row at the same instant")]
    #[test_case("drain_to_empty"; "snapshot drained clean, then a row at the same instant")]
    #[test_case("narrow_then_insert"; "partial-drain narrowing undone by a later row")]
    fn a_flushed_instant_stays_visible_however_the_bucket_emptied(mode: &str) {
        let buffer = MemBuffer::new();
        let ts = sealed_bucket_start();
        let bucket_id = MemBuffer::compute_bucket_id(ts);

        // The row that reaches Delta and leaves memory, in every mode.
        buffer.insert("project1", "table1", create_test_batch(ts), ts).unwrap();
        match mode {
            "survivor_at_same_instant" => {
                let snap = buffer.snapshot_bucket_for_flush("project1", "table1", bucket_id).unwrap();
                buffer.insert("project1", "table1", create_test_batch(ts), ts).unwrap();
                assert!(buffer.finish_flushed_snapshot(&snap));
            }
            "take" => {
                buffer.take_bucket_for_flush("project1", "table1", bucket_id).expect("bucket has rows");
            }
            "drain_to_empty" => {
                let snap = buffer.snapshot_bucket_for_flush("project1", "table1", bucket_id).unwrap();
                assert!(buffer.finish_flushed_snapshot(&snap), "nothing arrived mid-flight, so the bucket drains clean");
            }
            "narrow_then_insert" => {
                let snap = buffer.snapshot_bucket_for_flush("project1", "table1", bucket_id).unwrap();
                // A late arrival keeps the bucket alive so the narrowing runs.
                buffer.insert("project1", "table1", create_test_batch(ts + 60_000_000), ts + 60_000_000).unwrap();
                assert!(buffer.finish_flushed_snapshot(&snap));
            }
            other => unreachable!("unknown mode {other}"),
        }
        // One more row at the flushed instant; the survivor mode already has one.
        if mode != "survivor_at_same_instant" {
            buffer.insert("project1", "table1", create_test_batch(ts), ts).unwrap();
        }

        // Any mask covering `ts` hides a committed row that memory no longer holds.
        let ranges = buffer.get_bucket_ranges("project1", "table1");
        assert!(
            !ranges.iter().any(|&(start, end)| (start..end).contains(&ts)),
            "[{mode}] the mask covers an instant whose rows are committed to Delta and gone from \
             memory, so those rows answer no query at all: ranges={ranges:?}, flushed ts={ts}"
        );
    }

    /// A DML that empties a sealed bucket leaves a shell that can never flush,
    /// so only the reap sweep releases its WAL holds — and only once its pinned
    /// entries' ARRIVAL time passes the replay cutoff.
    #[test]
    fn reap_releases_dml_emptied_bucket_holds_after_replay_cutoff() {
        let buffer = MemBuffer::new();
        // Event-time old (sealed) but ARRIVING NOW — the reap must key on
        // arrival, not event time, or backfilled deletes release too early.
        let ts = chrono::Utc::now().timestamp_micros() - 2 * BUCKET_DURATION_MICROS;

        buffer.insert_with_hold("project1", "table1", create_test_batch(ts), ts, Some((0, walrus_rust::WalPosition { block_id: 3, offset: 3 }))).unwrap();
        let deleted = buffer.delete("project1", "table1", None, Some((1, walrus_rust::WalPosition { block_id: 4, offset: 4 }))).unwrap();
        assert!(deleted > 0);

        // Entries arrived just now: a replay-cutoff in the past must NOT release
        // the pins, or a partial release could resurrect the deleted rows.
        buffer.reap_expired_empty_buckets(crate::support::now_micros() - 1_000_000);
        let holds = buffer.wal_holds("project1", "table1", 4);
        assert!(holds[0].is_some() && holds[1].is_some(), "emptied bucket must keep pinning while its entries are inside the replay window, got {holds:?}");

        buffer.reap_expired_empty_buckets(crate::support::now_micros() + 1_000_000);
        let holds = buffer.wal_holds("project1", "table1", 4);
        assert!(holds.iter().all(Option::is_none), "reap must release the expired shell's holds, got {holds:?}");
    }

    #[test]
    fn test_evict_old_data() {
        let buffer = MemBuffer::new();
        let new_ts = chrono::Utc::now().timestamp_micros();
        let old_ts = new_ts - 2 * BUCKET_DURATION_MICROS;

        buffer.insert("project1", "table1", create_test_batch(old_ts), old_ts).unwrap();
        buffer.insert("project1", "table1", create_test_batch(new_ts), new_ts).unwrap();

        assert_eq!(buffer.evict_old_data(new_ts - BUCKET_DURATION_MICROS / 2), 1);
        assert_eq!(buffer.query("project1", "table1", &[]).unwrap().len(), 1);
    }

    fn create_multi_row_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        let ts = chrono::Utc::now().timestamp_micros();
        tin_batch(vec![ts; ids.len()], ids, names.into_iter().map(Into::into).collect())
    }

    /// Keep-greatest applied eagerly: only strictly-older versions of the
    /// appended keys leave the buffer; other keys and the winner survive.
    #[test]
    fn retract_superseded_drops_only_strictly_older_versions_of_updated_keys() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let mk = |ids: Vec<i64>, stamps: Vec<i64>, names: Vec<&str>| -> RecordBatch {
            RecordBatch::try_new(
                schema_of([("timestamp", ts_ty(), false), ("id", Int64, false), ("name", Utf8View, false), ("updated_at", ts_ty(), false)]),
                vec![
                    Arc::new(TimestampMicrosecondArray::from(vec![ts; ids.len()]).with_timezone("UTC")),
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringViewArray::from(names)),
                    Arc::new(TimestampMicrosecondArray::from(stamps).with_timezone("UTC")),
                ],
            )
            .unwrap()
        };
        buffer.insert("p", "t", mk(vec![1, 2], vec![10, 10], vec!["k1-old", "k2"]), ts).unwrap();
        let appended = mk(vec![1], vec![20], vec!["k1-new"]);
        buffer.insert("p", "t", appended.clone(), ts).unwrap();
        let keys = vec!["id".to_string(), "timestamp".to_string()];
        assert_eq!(buffer.retract_superseded("p", "t", &appended, &keys, "updated_at", "timestamp"), 1, "exactly the superseded k1 version leaves");
        let rows: usize = buffer.query("p", "t", &[]).unwrap().iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "k2 and the k1 winner survive");
        assert_eq!(buffer.retract_superseded("p", "t", &appended, &keys, "updated_at", "timestamp"), 0, "idempotent once nothing older remains");
    }

    /// The (id, name) table every DML case below mutates.
    fn dml_buffer(project: &str, table: &str, ids: Vec<i64>, names: Vec<&str>) -> MemBuffer {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        buffer.insert(project, table, create_multi_row_batch(ids, names), ts).unwrap();
        buffer
    }

    /// A column reference in the two forms the planner produces: SQL-planner
    /// exprs carry table qualifiers, but DFSchema is built from the bare table
    /// schema, so the qualifier must be stripped before physical planning.
    fn maybe_qualified(name: &str, qualified: bool) -> Expr {
        use datafusion::{
            common::{Column, TableReference},
            logical_expr::col,
        };
        if qualified { Expr::Column(Column::new(Some(TableReference::bare("table1")), name)) } else { col(name) }
    }

    fn rows(pairs: &[(i64, &str)]) -> Vec<(i64, String)> {
        pairs.iter().map(|&(id, name)| (id, name.to_string())).collect()
    }

    #[test_case(None => (3, vec![]) ; "no predicate deletes every row")]
    #[test_case(Some(false) => (1, rows(&[(1, "a"), (3, "c")])) ; "bare predicate deletes only id=2")]
    #[test_case(Some(true) => (1, rows(&[(1, "a"), (3, "c")])) ; "SQL-planner qualified predicate resolves against the bare schema")]
    fn delete_applies_predicate(qualified: Option<bool>) -> (u64, Vec<(i64, String)>) {
        use datafusion::logical_expr::lit;
        let buffer = dml_buffer("project1", "table1", vec![1, 2, 3], vec!["a", "b", "c"]);
        let predicate = qualified.map(|q| maybe_qualified("id", q).eq(lit(2i64)));
        let deleted = buffer.delete("project1", "table1", predicate.as_ref(), None).unwrap();
        (deleted, collect_id_name(&buffer, "project1", "table1"))
    }

    // The qualified case's SET RHS is `table1.name`, so its rows come back unchanged.
    #[test_case(false, None => (1, 1, rows(&[(1, "a"), (2, "updated"), (3, "c")])) ; "bare predicate, literal assignment")]
    #[test_case(true, Some("name") => (1, 1, rows(&[(1, "a"), (2, "b"), (3, "c")])) ; "qualified predicate and qualified assignment value")]
    fn update_applies_predicate(qualified: bool, value_column: Option<&str>) -> (u64, usize, Vec<(i64, String)>) {
        use datafusion::logical_expr::lit;
        let buffer = dml_buffer("project1", "table1", vec![1, 2, 3], vec!["a", "b", "c"]);
        let predicate = maybe_qualified("id", qualified).eq(lit(2i64));
        let value = value_column.map_or_else(|| lit("updated"), |c| maybe_qualified(c, qualified));
        let updated = buffer.update("project1", "table1", Some(&predicate), &[("name".to_string(), value)], None).unwrap();
        let batches = buffer.query("project1", "table1", &[]).unwrap().len();
        (updated, batches, collect_id_name(&buffer, "project1", "table1"))
    }

    /// Sorted (id, name) pairs from a buffer query, for asserting the full row set.
    fn collect_id_name(buffer: &MemBuffer, project: &str, table: &str) -> Vec<(i64, String)> {
        use arrow::array::AsArray;
        let mut out = Vec::new();
        for b in buffer.query(project, table, &[]).unwrap() {
            let ids = b.column(b.schema().index_of("id").unwrap()).as_primitive::<arrow::datatypes::Int64Type>();
            let names = arrow::compute::cast(b.column(b.schema().index_of("name").unwrap()), &DataType::Utf8).unwrap();
            let names = names.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            out.extend((0..b.num_rows()).map(|i| (ids.value(i), names.value(i).to_string())));
        }
        out.sort();
        out
    }

    fn id_source() -> crate::dml::UpdateSource {
        // Int64 join key (no Utf8/Utf8View RowConverter mismatch); Utf8View
        // new_name to match the target `name` type on assignment.
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("new_name", DataType::Utf8View, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![2i64, 4])), Arc::new(arrow::array::StringViewArray::from(vec!["B", "D"]))],
        )
        .unwrap();
        crate::dml::UpdateSource { schema, batch, join_keys: vec![("id".to_string(), "id".to_string())] }
    }

    /// `update_with_source` must update exactly the source-matched rows and keep
    /// every other row intact. The source matches ids 2 and 4; a source-matched
    /// row that FAILS the predicate must be preserved UNCHANGED (not updated,
    /// not dropped). Neither case may lose a row: the set is always all 5.
    #[test_case(false => (2, rows(&[(1, "a"), (2, "B"), (3, "c"), (4, "D"), (5, "e")])) ; "no predicate: exactly the 2 source-matched rows update")]
    #[test_case(true => (1, rows(&[(1, "a"), (2, "B"), (3, "c"), (4, "d"), (5, "e")])) ; "predicate id=2: matched-but-predicate-false id=4 preserved unchanged")]
    fn update_with_source_updates_only_matched_and_preserves_rest(with_predicate: bool) -> (u64, Vec<(i64, String)>) {
        use datafusion::logical_expr::{col, lit};
        let buffer = dml_buffer("p", "t", vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
        let pred = with_predicate.then(|| col("id").eq(lit(2i64)));
        let n = buffer.update_with_source("p", "t", pred.as_ref(), &[("name".to_string(), col("new_name"))], &id_source(), None).unwrap();
        (n, collect_id_name(&buffer, "p", "t"))
    }

    /// The replay path's `UpdateSource`: schema is always the batch's own.
    fn replay_source(batch: RecordBatch, join_keys: &[(String, String)]) -> crate::dml::UpdateSource {
        crate::dml::UpdateSource { schema: batch.schema(), batch, join_keys: join_keys.to_vec() }
    }

    /// WAL replay parses DML against the bare buffer schema (plus `source__`
    /// source cols), so the serializer must store normalized, unqualified column
    /// refs: the qualified form fails to parse, the normalized form applies.
    #[test]
    fn update_with_source_by_sql_replay_requires_normalized_columns() {
        let buffer = dml_buffer("p", "t", vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
        let src = id_source(); // id (Int64) join key, new_name (Utf8View); matches 2 & 4
        let assigns = [("name".to_string(), "source__new_name".to_string())];
        let keys = [("id".to_string(), "id".to_string())];

        let bad = buffer.update_with_source_by_sql("p", "t", Some("t.id > 0"), &assigns, replay_source(src.batch.clone(), &keys), None, None);
        assert!(bad.is_err(), "table-qualified predicate must fail against the bare replay schema");

        let n = buffer.update_with_source_by_sql("p", "t", Some("id > 0"), &assigns, replay_source(src.batch, &keys), None, None).unwrap();
        assert_eq!(n, 2);
        assert_eq!(collect_id_name(&buffer, "p", "t"), rows(&[(1, "a"), (2, "B"), (3, "c"), (4, "D"), (5, "e")]));
    }

    /// A `hashes` List<Utf8> column updated via a normalized
    /// `array_concat(CASE WHEN hashes ..., [source__tag])` assignment must both
    /// parse and physically evaluate (List<Utf8> concat with a Utf8View literal).
    #[test]
    fn update_with_source_by_sql_applies_normalized_hashes_shape() {
        use arrow::array::{ListBuilder, StringBuilder};

        let tgt_schema = schema_of([("id", Int64, false), ("hashes", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true)]);
        let mut hb = ListBuilder::new(StringBuilder::new());
        hb.values().append_value("h1");
        hb.append(true); // row id=1: ["h1"]
        hb.values().append_value("h2");
        hb.append(true); // row id=2: ["h2"]
        let tgt = RecordBatch::try_new(tgt_schema, vec![Arc::new(Int64Array::from(vec![1i64, 2])), Arc::new(hb.finish())]).unwrap();

        let buffer = MemBuffer::new();
        buffer.insert("p", "t", tgt, chrono::Utc::now().timestamp_micros()).unwrap();

        let src_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("tag", DataType::Utf8View, false)]));
        let src_batch =
            RecordBatch::try_new(src_schema, vec![Arc::new(Int64Array::from(vec![1i64])), Arc::new(StringViewArray::from(vec!["newtag"]))]).unwrap();

        let assigns = [("hashes".to_string(), "array_concat(CASE WHEN hashes IS NOT NULL THEN hashes ELSE [] END, [source__tag])".to_string())];
        let keys = [("id".to_string(), "id".to_string())];
        let reg = crate::read::functions::function_registry().unwrap();
        let n = buffer
            .update_with_source_by_sql("p", "t", None, &assigns, replay_source(src_batch, &keys), Some(reg.as_ref()), None)
            .expect("normalized hashes UPDATE...FROM must parse AND apply");
        assert_eq!(n, 1, "only id=1 matches the source");

        use arrow::array::AsArray;
        let mut got: std::collections::HashMap<i64, Vec<String>> = Default::default();
        for b in buffer.query("p", "t", &[]).unwrap() {
            let ids = b.column(b.schema().index_of("id").unwrap()).as_primitive::<arrow::datatypes::Int64Type>();
            let lists = b.column(b.schema().index_of("hashes").unwrap()).as_list::<i32>();
            for i in 0..b.num_rows() {
                let s = arrow::compute::cast(&lists.value(i), &DataType::Utf8).unwrap();
                let s = s.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                got.insert(ids.value(i), (0..s.len()).map(|j| s.value(j).to_string()).collect());
            }
        }
        assert_eq!(got.get(&1), Some(&vec!["h1".to_string(), "newtag".to_string()]), "matched row appends the tag");
        assert_eq!(got.get(&2), Some(&vec!["h2".to_string()]), "unmatched row unchanged");
    }

    /// DML replayed for a table with NO buffered rows must no-op rather than
    /// schema-error: nothing buffered ⇒ nothing to mutate.
    #[test]
    fn dml_by_sql_on_untracked_table_noops_instead_of_schema_error() {
        let buffer = MemBuffer::new();
        let src = id_source();
        let assigns = [("hashes".to_string(), "array_concat(CASE WHEN hashes IS NOT NULL THEN hashes ELSE [] END, [source__new_name])".to_string())];
        let keys = [("id".to_string(), "id".to_string())];
        let pred = "context___span_id IS NOT NULL AND context___trace_id IS NOT NULL";
        let n = buffer
            .update_with_source_by_sql("p", "t", Some(pred), &assigns, replay_source(src.batch, &keys), None, None)
            .expect("UPDATE...FROM replay on an untracked table must no-op, not schema-error");
        assert_eq!(n, 0);
        assert_eq!(
            buffer.update_by_sql("p", "t", Some("id > 0"), &[("name".to_string(), "'x'".to_string())], None, None).unwrap(),
            0,
            "plain UPDATE replay must no-op on an untracked table"
        );
        assert_eq!(buffer.delete_by_sql("p", "t", Some("id > 0"), None, None).unwrap(), 0, "DELETE replay must no-op on an untracked table");
    }

    /// A replayed DELETE/UPDATE must migrate its WAL `(shard, pos)` onto the
    /// buckets it mutates, so the rewind marker cannot advance past the DML while
    /// a mutated bucket is un-flushed — otherwise a mid-replay crash re-replays
    /// the INSERTs but skips the DELETE, resurrecting deleted rows.
    #[test]
    fn replay_dml_migrates_wal_hold_onto_mutated_bucket() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let ins = walrus_rust::WalPosition { block_id: 3, offset: 30 };
        let del = walrus_rust::WalPosition { block_id: 5, offset: 50 };
        buffer.insert_with_hold("p", "t", create_multi_row_batch(vec![1, 2], vec!["a", "b"]), ts, Some((0, ins))).unwrap();
        let deleted = buffer.delete_by_sql("p", "t", Some("id = 1"), None, Some((1, del))).unwrap();
        assert_eq!(deleted, 1, "DELETE must match the row it targets");
        let holds = buffer.wal_holds("p", "t", 4);
        assert_eq!(holds[0], Some(ins), "insert hold must remain on the bucket");
        assert_eq!(holds[1], Some(del), "DELETE's WAL hold must be migrated onto the mutated bucket (else the marker skips it on crash-resume)");
    }

    fn test_table_df_schema() -> DFSchema {
        dml_buffer("p", "t", vec![1], vec!["a"]).df_schema_for("p", "t").unwrap()
    }

    /// The widened replay schema the normalized `hashes` enrichment
    /// UPDATE...FROM re-parses against (source col → `source__tag`).
    fn widened_hashes_df_schema() -> DFSchema {
        let hashes = ("hashes", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true);
        let cols = [("context___span_id", Utf8View, true), ("context___trace_id", Utf8View, true), ("timestamp", ts_ty(), true)];
        DFSchema::try_from(schema_of(cols.into_iter().chain([hashes, ("source__tag", Utf8View, true)])).as_ref().clone()).unwrap()
    }

    /// Returns whether the predicate parsed. UDFs resolve only with a registry,
    /// and the prod `hashes` shapes (array_concat + CASE + array literals) must
    /// re-parse against the widened replay schema — qualifier stripping alone is
    /// not enough.
    #[test_case("coalesce(name, '') = 'x'", false, false => false ; "without a registry a UDF is rejected with 'No functions registered'")]
    #[test_case("coalesce(name, '') = 'x'", true, false => true ; "coalesce resolves with a registry")]
    #[test_case("to_char(timestamp, 'YYYY') = '2024'", true, false => true ; "to_char resolves with a registry")]
    #[test_case("((context___span_id IS NOT NULL AND context___trace_id IS NOT NULL) \
                 AND (\"timestamp\" >= CAST('2026-07-05T14:37:49.995+00:00' AS TIMESTAMP)) \
                 AND (\"timestamp\" < CAST('2026-07-05T14:38:22.731+00:00' AS TIMESTAMP)))", true, true
        => true ; "normalized prod hashes-update predicate parses after normalization")]
    #[test_case("array_concat(CASE WHEN hashes IS NOT NULL THEN hashes ELSE [] END, [source__tag])", true, true
        => true ; "normalized prod hashes-update assignment parses after normalization")]
    fn parse_sql_predicate_resolves_udfs_with_a_registry_and_prod_hashes_shapes(sql: &str, with_registry: bool, widened: bool) -> bool {
        let schema = if widened { widened_hashes_df_schema() } else { test_table_df_schema() };
        let owned = crate::read::functions::function_registry().unwrap();
        let registry = with_registry.then(|| owned.as_ref());
        match super::parse_sql_predicate(sql, &schema, registry) {
            Ok(_) => true,
            Err(err) => {
                assert!(err.to_string().contains("No functions registered"), "expected 'No functions registered' error, got: {err}");
                false
            }
        }
    }

    // upper() survives logical->physical lowering; coalesce is rewritten to CASE.
    #[test]
    fn update_by_sql_with_udf_replays_when_registry_present() {
        let buffer = dml_buffer("project1", "table1", vec![1, 2, 3], vec!["a", "b", "c"]);
        let reg = crate::read::functions::function_registry().unwrap();
        let updated = buffer
            .update_by_sql("project1", "table1", Some("upper(name) = 'B'"), &[("name".into(), "'updated'".into())], Some(reg.as_ref()), None)
            .expect("UDF-bearing UPDATE should replay with registry");
        assert_eq!(updated, 1);

        assert!(
            buffer.update_by_sql("project1", "table1", Some("upper(name) = 'A'"), &[("name".into(), "'x'".into())], None, None).is_err(),
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

    /// A window is half-open: the last microsecond before a boundary belongs to
    /// the previous bucket, and the boundary itself opens the next one. Negative
    /// (pre-1970) timestamps ride the same rule even though integer division
    /// truncates toward zero: -1 / N = 0, -N / N = -1.
    #[test_case(0 => 0)]
    #[test_case(BUCKET_DURATION_MICROS - 1 => 0 ; "last microsecond of the first window")]
    #[test_case(BUCKET_DURATION_MICROS => 1 ; "the boundary instant opens the next window")]
    #[test_case(BUCKET_DURATION_MICROS * 2 => 2)]
    #[test_case(-1 => 0 ; "just before epoch -> bucket 0")]
    #[test_case(-BUCKET_DURATION_MICROS => -1)]
    #[test_case(-BUCKET_DURATION_MICROS - 1 => -1)]
    #[test_case(-BUCKET_DURATION_MICROS * 2 => -2 ; "20 minutes before epoch should be bucket -2")]
    fn compute_bucket_id_is_half_open(ts: i64) -> i64 {
        MemBuffer::compute_bucket_id(ts)
    }

    /// The rollup read path splits a window at the oldest buffered row:
    /// everything below is served from Delta, everything above is read raw. A
    /// horizon that is too NEW silently drops rows, so the bound must never sit
    /// above a buffered row's own timestamp.
    #[test]
    fn min_buffered_micros_never_exceeds_the_oldest_buffered_row() {
        let buffer = MemBuffer::new();
        let (early, late) = (BUCKET_DURATION_MICROS + 7, BUCKET_DURATION_MICROS * 4 + 11);
        buffer.insert("p", "t", create_test_batch(late), late).unwrap();
        buffer.insert("p", "t", create_test_batch(early), early).unwrap();

        let horizon = buffer.min_buffered_micros("p", "t", 0, late).expect("rows are buffered in range");
        assert!(horizon <= early, "horizon {horizon} must not exceed the oldest buffered row {early}");
        assert_eq!(horizon, BUCKET_DURATION_MICROS, "the bound is the bucket start, which is what a rollup grain aligns against");

        // Scoped to the range: a window above `early` must not be dragged down
        // by it, or one straggler collapses the horizon for every query.
        let above = buffer.min_buffered_micros("p", "t", late, late).expect("the late bucket overlaps");
        assert!(above > early, "a window that excludes the early row must not report it: {above}");

        assert_eq!(buffer.min_buffered_micros("p", "t", late * 10, late * 20), None, "no overlapping bucket");
        assert_eq!(buffer.min_buffered_micros("p", "absent", 0, late), None, "unknown table");
    }

    #[test]
    fn test_schema_compatibility_race_condition() {
        use std::{sync::Arc, thread};

        let buffer = Arc::new(MemBuffer::new());
        let ts = chrono::Utc::now().timestamp_micros();
        let batch1 = create_test_batch(ts);

        // Concurrent inserts of compatible schemas must all succeed.
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let (buffer, batch) = (Arc::clone(&buffer), batch1.clone());
                thread::spawn(move || buffer.insert("project1", "table1", batch, ts + i))
            })
            .collect();
        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        assert_eq!(n_rows(&buffer.query("project1", "table1", &[]).unwrap()), 10, "All 10 inserts should succeed");
    }

    #[test]
    fn test_point_lookup_fast_path_filters_inline() {
        use datafusion::logical_expr::{col, lit};

        let buffer = dml_buffer("project1", "table1", (1..=10).collect(), vec!["a"; 10]);

        // Non-point query returns the whole bucket; downstream FilterExec narrows it.
        assert_eq!(n_rows(&buffer.query("project1", "table1", &[]).unwrap()), 10);

        let point = buffer.query("project1", "table1", &[col("id").eq(lit(5i64))]).unwrap();
        assert_eq!(n_rows(&point), 1, "point lookup should return exactly the matching row");

        let parts = buffer.query_partitioned("project1", "table1", &[col("id").eq(lit(7i64))]).unwrap();
        assert_eq!(n_rows(parts.partitions.iter().flatten()), 1);
    }

    /// Many tiny INSERTs must stay bounded at `MAX_BATCH_COUNT_PER_BUCKET`
    /// batches per bucket, on the insert path and through the flush path. (Concat
    /// fires when len > the cap, and the next push reaches the cap again before
    /// the next concat, hence the `+ 1`.)
    #[test_case(10, 1 ; "ten one-row inserts share a single bucket")]
    #[test_case(1000, 30 ; "prod fragmentation incident: 1000 OTLP INSERTs of ~30 rows")]
    fn insert_coalesces_small_batches_into_bucket_tail(inserts: usize, row_count_per_insert: usize) {
        let buffer = MemBuffer::new();
        let ts = 1_000_000_000_000i64;
        let total_rows = row_count_per_insert * inserts;

        for i in 0..inserts {
            let batch = make_batch_with_rows(ts + i as i64, row_count_per_insert);
            buffer.insert("p1", "t1", batch, ts).unwrap();
        }

        let bucket_id = MemBuffer::compute_bucket_id(ts);
        let table = buffer.get_table("p1", "t1").unwrap();
        let snapshot: Vec<RecordBatch> = table.buckets.get(&bucket_id).expect("bucket exists").batches.lock().to_vec();
        let n_batches = snapshot.len();

        assert_eq!(snapshot.iter().map(|b| b.num_rows()).sum::<usize>(), total_rows, "row preservation");
        assert!(
            n_batches <= MAX_BATCH_COUNT_PER_BUCKET + 1,
            "bucket should hold ≤{} batches after amortized coalesce, got {n_batches}",
            MAX_BATCH_COUNT_PER_BUCKET + 1
        );

        let flushable: Vec<_> =
            buffer.bucket_keys(|id| id < bucket_id + 1).into_iter().filter_map(|(p, t, id)| buffer.take_bucket_for_flush(&p, &t, id)).collect();
        assert_eq!(flushable.len(), 1, "all inserts share one time bucket");
        assert_eq!(flushable[0].row_count, total_rows);
        assert_eq!(flushable[0].batches.iter().map(|b| b.num_rows()).sum::<usize>(), total_rows, "no rows lost to insert-time coalesce");
        assert!(
            flushable[0].batches.len() <= MAX_BATCH_COUNT_PER_BUCKET + 1,
            "got {} batches, expected ≤ {}",
            flushable[0].batches.len(),
            MAX_BATCH_COUNT_PER_BUCKET + 1
        );
    }

    fn make_batch_with_rows(start_ts: i64, n: usize) -> RecordBatch {
        tin_batch(vec![start_ts; n], (0..n as i64).collect(), (0..n).map(|i| format!("row-{i}")).collect())
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
        let batch = tin_batch(vec![ts; names.len()], (0..names.len() as i64).collect(), names.into_iter().map(Into::into).collect());
        buf.insert("p", "t", batch, ts).unwrap();

        // Prod literal type: Utf8View (map_string_types_to_utf8view=true).
        let view = |s: &str| Expr::Literal(ScalarValue::Utf8View(Some(s.to_string())), None);
        let or = col("name").eq(view("client")).or(col("name").eq(view("internal")));

        let parts = buf.query_partitioned("p", "t", std::slice::from_ref(&or)).unwrap();
        assert_eq!(n_rows(parts.partitions.iter().flatten()), 1258 + 13346, "OR of two Utf8View equalities must keep all matches");
    }

    /// The staleness signal measures how long a bucket has waited to flush
    /// (dwell), not its rows' event-time age, so backfill must not false-alarm.
    #[test]
    fn oldest_bucket_age_reflects_dwell_not_event_time() {
        let buffer = MemBuffer::new();
        let old_event_ts = crate::support::now_micros() - 6 * 3600 * 1_000_000; // 6h-old event time
        buffer.insert("p", "t", make_batch_with_rows(old_event_ts, 10), old_event_ts).unwrap();

        let oldest = buffer.get_stats().oldest_bucket_micros.expect("flushable bucket present");
        let dwell_secs = (crate::support::now_micros() - oldest) / 1_000_000;
        assert!(dwell_secs < 60, "dwell should be ~0 for a freshly-buffered backfill bucket, got {dwell_secs}s");
    }

    /// The cached total must equal the authoritative sum of the bucket atomics
    /// EXACTLY after any mixed sequence of inserts, coalesces, DML, drains,
    /// evictions and reaps; a missed mutation site shows up as an inequality.
    #[test]
    fn cached_total_matches_recomputed_total_under_random_mutations() {
        // Deterministic xorshift — no rand dep, and a failure is reproducible.
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move |n: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % n
        };
        let buffer = MemBuffer::new();
        let base = crate::support::now_micros();
        let bucket_span = bucket_duration_micros();

        let check = |step: usize, op: &str| {
            let cached = buffer.estimated_memory_bytes();
            let truth = buffer.recompute_memory_bytes();
            assert_eq!(cached, truth, "step {step} ({op}): cached total {cached} != recomputed {truth}");
        };

        for step in 0..400 {
            let project = format!("p{}", next(3));
            let bucket_offset = next(3) as i64;
            let ts = base + bucket_offset * bucket_span;
            match next(10) {
                0..=5 => {
                    buffer.insert(&project, "t1", create_test_batch(ts), ts).unwrap();
                    check(step, "insert");
                }
                6 => {
                    buffer.insert_batches(&project, "t1", vec![create_test_batch(ts), wide_view_row(ts)], ts).unwrap();
                    check(step, "insert_batches");
                }
                7 => {
                    let bucket_id = MemBuffer::compute_bucket_id(ts);
                    buffer.take_bucket_for_flush(&project, "t1", bucket_id);
                    check(step, "take_bucket_for_flush");
                }
                8 => {
                    buffer.delete(&project, "t1", None, None).unwrap();
                    check(step, "delete");
                }
                _ => {
                    buffer.evict_old_data(base + bucket_offset * bucket_span);
                    buffer.reap_expired_empty_buckets(crate::support::now_micros() + bucket_span);
                    check(step, "evict + reap");
                }
            }
        }
        buffer.evict_old_data(base + 100 * bucket_span);
        buffer.reap_expired_empty_buckets(crate::support::now_micros() + 100 * bucket_span);
        assert_eq!(buffer.estimated_memory_bytes(), buffer.recompute_memory_bytes(), "post-teardown cached total diverged");
        assert_eq!(buffer.recompute_memory_bytes(), 0, "everything evicted but buckets still charge bytes");
    }

    /// A too-large subtraction must saturate at 0 rather than wrap (a wrapped
    /// counter rejects every `try_reserve_memory` forever); the reconciler then
    /// restores truth on the next flush tick.
    #[test]
    fn oversized_subtraction_saturates_and_reconciler_restores_truth() {
        let buffer = MemBuffer::new();
        let ts = crate::support::now_micros();
        for _ in 0..8 {
            buffer.insert("p1", "t1", create_test_batch(ts), ts).unwrap();
        }
        let truth = buffer.recompute_memory_bytes();
        assert!(truth > 0);

        sub_saturating(&buffer.estimated_bytes, truth + 1_000_000_000);
        assert_eq!(buffer.estimated_memory_bytes(), 0, "underflow must saturate at 0, never wrap to ~usize::MAX");

        let (cached_before, reconciled) = buffer.reconcile_estimated_bytes();
        assert_eq!(cached_before, 0);
        assert_eq!(reconciled, truth);
        assert_eq!(buffer.estimated_memory_bytes(), truth, "reconciler must store the authoritative sum");
    }

    /// Coalesce shrinks a bucket's `memory_bytes`, and that shrinkage must ride
    /// back to the MemBuffer total via `insert_batch`'s returned delta.
    #[test]
    fn coalesce_shrinkage_is_reported_to_the_membuffer_total() {
        let buffer = MemBuffer::new();
        let ts = crate::support::now_micros();
        for _ in 0..(MAX_BATCH_COUNT_PER_BUCKET * 4) {
            buffer.insert("p1", "t1", create_test_batch(ts), ts).unwrap();
        }
        assert_eq!(buffer.estimated_memory_bytes(), buffer.recompute_memory_bytes(), "coalesce leaked (sum_before - combined_size) into the cached total");
    }
}
