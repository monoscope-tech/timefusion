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
use parking_lot::Mutex;
use tracing::{debug, error, info, instrument, warn};

use crate::{observability::arrow_err, read::functions::FnRegistry};

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
    // Every existing field must survive with a compatible type (nullability may loosen).
    if !existing.fields().iter().all(|f| incoming.field_with_name(f.name()).is_ok_and(|i| types_compatible(f.data_type(), i.data_type()))) {
        return false;
    }
    // New fields are OK only if nullable (SchemaMode::Merge) — a new NOT NULL
    // field would break the already-buffered rows.
    let new_fields = incoming.fields().iter().filter(|f| existing.field_with_name(f.name()).is_err()).collect::<Vec<_>>();
    if new_fields.iter().any(|f| !f.is_nullable()) {
        return false;
    }
    if !new_fields.is_empty() {
        info!("Schema evolution: {} new nullable field(s) added", new_fields.len());
    }
    true
}

/// Take field nullability from the **declared** table schema wherever the data
/// honestly supports it, so one schema is authoritative end-to-end.
///
/// Arrow records nullability from how an array was *built*, not from whether it
/// holds nulls, so client and decode paths routinely hand us
/// `timestamp: nullable=true` for a column that is never null. That metadata
/// used to be pinned on the TableBuffer for the process lifetime (whatever the
/// first batch happened to say), ride into demoted hot-tier IPC files and the
/// flush path, and only surface far downstream as a mismatch against the
/// declared NOT NULL schema — as a hot-tier read miss, or as DataFusion's
/// `physical true vs logical false` aggregate error (prod 2026-07-31).
///
/// Only same-name, **exactly**-same-type fields are substituted, which leaves
/// variant columns (Utf8View SQL view vs Struct storage view) and any timezone
/// mismatch untouched. Tightening to NOT NULL additionally requires the column
/// to actually hold no nulls: a declared-NOT-NULL column that does contain
/// nulls stays nullable rather than asserting an invariant the data violates.
/// `null_count` is `None` when no data is available to check (schema-only
/// alignment), in which case the declared nullability is taken as given.
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

/// [`align_nullability`] applied to a batch. Returns the batch untouched when
/// nothing differs (the steady-state path) or when the rebuild would fail.
fn align_batch_nullability(batch: RecordBatch, declared: Option<&SchemaRef>) -> RecordBatch {
    let Some(declared) = declared else { return batch };
    let nc = |i: usize| batch.column(i).null_count();
    let Some(aligned) = align_nullability(&batch.schema(), declared, Some(&nc)) else { return batch };
    RecordBatch::try_new(aligned, batch.columns().to_vec()).unwrap_or(batch)
}

/// Put a registered table's batch in its one declared column order and type
/// representation before it can establish or join a [`TableBuffer`].
///
/// SQL INSERTs may name columns in any order, and older WAL/hot-tier entries
/// can predate newly-added nullable columns. Treating the first replayed
/// batch's incidental schema as the table schema made replay depend on the
/// first producer's field order and Arrow representation. Production on
/// 2026-08-04 accumulated 5.1 GiB / 5,767 INSERT payloads from this class of
/// mismatch. Delta's schema caster reorders by name, fills missing nullable
/// fields, and performs supported representation casts without copying
/// already-matching arrays.
fn canonicalize_declared_batch(batch: RecordBatch, declared: Option<&SchemaRef>) -> anyhow::Result<RecordBatch> {
    let Some(declared) = declared else { return Ok(batch) };
    if batch.schema_ref() == declared {
        return Ok(batch);
    }
    // A direct MemBuffer caller may intentionally provide only a projection
    // of the registered schema (unit tests and internal narrow paths do this).
    // Widening that projection would have to invent values for declared
    // NOT-NULL fields such as `summary`. Preserve the established partial-
    // batch path; full WAL/client rows still canonicalize, including filling
    // nullable columns added after an older payload was written.
    let incoming = batch.schema();
    if declared.fields().iter().any(|field| !field.is_nullable() && incoming.field_with_name(field.name()).is_err())
        || incoming.fields().iter().any(|field| declared.field_with_name(field.name()).is_err())
    {
        return Ok(batch);
    }
    // Preserve honest nullability for malformed legacy data. Casting directly
    // to a declared NOT NULL field relabels an array containing nulls as
    // non-nullable; downstream code then trusts a false invariant. Missing
    // required fields still stay NOT NULL in the target and are rejected by
    // the caster.
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
        // Timestamps: unit must match, timezone differences are allowed but logged
        (DataType::Timestamp(u1, tz1), DataType::Timestamp(u2, tz2)) => {
            if u1 == u2 && tz1 != tz2 {
                debug!("Timestamp timezone mismatch: {:?} vs {:?} (allowed)", tz1, tz2);
            }
            u1 == u2
        }
        // Lists: check element types recursively
        (DataType::List(f1), DataType::List(f2)) | (DataType::LargeList(f1), DataType::LargeList(f2)) => types_compatible(f1.data_type(), f2.data_type()),
        // Structs: all existing fields must be present and compatible
        (DataType::Struct(fields1), DataType::Struct(fields2)) => {
            fields1.iter().all(|f1| fields2.iter().find(|f| f.name() == f1.name()).is_some_and(|f2| types_compatible(f1.data_type(), f2.data_type())))
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

/// Widened schema for `UPDATE ... FROM`: target fields + source fields
/// prefixed `source__`, all nullable on the source side (rows without a
/// matching source row produce NULLs in source columns). Shared by
/// [`MemBuffer::update_with_source`] and [`MemBuffer::update_with_source_by_sql`],
/// which must build byte-identical schemas since the latter's SQL is parsed
/// against this shape before being handed to the former.
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

/// Table key type using Arc<str> for efficient cloning and comparison.
/// Composite key of (project_id, table_name) for flattened lookup.
pub type TableKey = (Arc<str>, Arc<str>);

#[inline]
pub fn table_key(project_id: &str, table_name: &str) -> TableKey {
    (Arc::from(project_id), Arc::from(table_name))
}

pub struct MemBuffer {
    /// Flattened structure: (project_id, table_name) → TableBuffer
    /// Reduces 3 hash lookups to 1 for table access.
    tables: DashMap<TableKey, Arc<TableBuffer>>,
    /// Running total of in-memory bytes across all live buckets — the value
    /// [`MemBuffer::estimated_memory_bytes`] returns, and therefore what the
    /// memory-reservation CAS, `pressure_pct`, and `unflushed_backlog_bytes`
    /// all read. Maintained by DELTA at every site that changes any bucket's
    /// `memory_bytes`, so it must stay in step with the sum of those atomics.
    ///
    /// **Exhaustive list of mutation sites** (the historical bug was a missed
    /// site — grep `memory_bytes` before adding one and update this list):
    /// 1. `MemBuffer::insert_with_hold` — adds the net delta returned by
    ///    `TableBuffer::insert_batch` (insert size *plus* the coalesce
    ///    shrinkage, see 2).
    /// 2. `TableBuffer::insert_batch` — `+= new_size` on push, then on
    ///    coalesce `+= combined_size - folded_size` (negative). Both are
    ///    folded into its returned net delta; the caller applies it. NOT
    ///    re-derived — this is exactly what the old cache got wrong.
    /// 3. `MemBuffer::insert_batches` — sums the net deltas of 2.
    /// 4. `finish_flushed_snapshot` prefix drain — `-= applied`, the value
    ///    actually subtracted from the bucket (clamped identically), and
    ///    `-= residual` of the bucket if the drain removes it.
    /// 5. `take_bucket_for_flush` — `-= swap(0)`; plus the residual of the
    ///    bucket if the empty shell is removed.
    /// 6. `restore_bucket` (failed-commit restore) — `+= added`.
    /// 7. `evict_old_data` — `-= sum(memory_bytes)` of removed buckets.
    /// 8. `reap_expired_empty_buckets` — `-= residual` of removed shells.
    /// 9. `delete` (DML mem leg) — `-= total_freed`.
    /// 10. `update` / `update_with_source` (DML mem legs) — signed delta.
    /// 11. `clear` — `store(0)` alongside `tables.clear()`.
    ///
    /// **Drift is bounded, never permanent.** Every subtraction goes through
    /// [`apply_signed_delta`], which saturates at 0: an earlier version used
    /// raw `fetch_sub` and a single mismatched subtraction wrapped the counter
    /// to ~948 GB against a 44 GB budget, after which *every*
    /// `try_reserve_memory` failed with "Memory limit exceeded" until restart.
    /// Saturation makes that failure mode under-report instead of wedging
    /// ingest, and [`MemBuffer::reconcile_estimated_bytes`] — called once per
    /// flush-task timer tick — recomputes the authoritative sum, stores it,
    /// and warns if the drift exceeded a few percent. So a future missed
    /// mutation site shows up as a warn log within a minute rather than as a
    /// permanent outage.
    estimated_bytes: AtomicUsize,
    /// Mirrors `WalManager::shards_per_topic` so `FlushableBucket.wal_first_positions`
    /// is always sized correctly when snapshotted at seal time.
    shards_per_topic: usize,
    /// LRU cache of per-bucket tantivy indexes. Lives at the MemBuffer
    /// level (not on individual TimeBuckets) so the LRU has a global view
    /// for byte-budget eviction. Entries are dropped:
    /// - when `text_index_max_bytes` is exceeded (LRU-evict tail)
    /// - when the bucket receives an insert (cache_invalidate by key)
    /// - when the bucket drains/evicts (cache_invalidate by key)
    text_index_cache: parking_lot::Mutex<lru::LruCache<BucketCacheKey, Arc<crate::tantivy::BucketTextIndex>>>,
    /// Sum of `size_bytes` across cached entries. Kept in an atomic so the
    /// hot insert path can do a single load to check "over budget?" without
    /// taking the LRU mutex.
    text_index_bytes: AtomicUsize,
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
    force_flushed: DashMap<TableKey, std::collections::HashSet<i64>>,
    /// Per (table, bucket): the highest row timestamp that bucket has ever
    /// handed to Delta. `get_bucket_ranges` floors its mask just above it.
    ///
    /// It lives HERE, not on the bucket, because both flush paths delete the
    /// bucket the moment it empties (`remove_if` in `take_bucket_for_flush` and
    /// `finish_flushed_snapshot`) — a per-bucket atomic is gone before the next
    /// insert recreates the bucket under the same id, which is precisely the
    /// case that loses rows.
    ///
    /// Nor can it be folded into `min_timestamp`: the prefix drain tried that
    /// (`store(drained_max + 1)`) and the INSERT path `fetch_min`s the same
    /// atomic, so one later row at the drained instant pulls the mask back over
    /// rows that have left memory. The floor has to live outside the value
    /// inserts maintain, and outside the bucket's lifetime.
    ///
    /// Bounded by `evict_old_data`, which drops an entry with its bucket.
    flushed_max: DashMap<TableKey, std::collections::HashMap<i64, i64>>,
    /// GC-floor pins of buckets mid-take: `take_bucket_for_flush` removes a
    /// bucket (and its `first_wal_pin_micros`) from the tables map before the
    /// flush path registers its inflight pin; a GC sweep sampling the floor
    /// in that gap would miss the airborne bucket and could delete its
    /// backing WAL file. The pin parks here from before the removal until
    /// [`Self::release_taking_pin`]. Keyed by `taking_seq`.
    taking_pins: DashMap<u64, i64>,
    taking_seq: AtomicU64,
    /// WAL-replay DML entries consumed as no-ops because their table had no
    /// buffered rows (already flushed / drained mid-replay). Surfaced in
    /// `timefusion_stats` — this replaced the quarantine file that used to be
    /// this loss-class's on-disk canary, so growth here is the re-drive signal.
    replay_dml_noops: AtomicU64,
}

/// Cache key: (project_id, table_name, bucket_id). All three are cheap to
/// clone (Arc<str> + i64) so the key lives both in the LRU and in the
/// invalidation calls.
pub type BucketCacheKey = (Arc<str>, Arc<str>, i64);

pub struct TableBuffer {
    buckets: DashMap<i64, TimeBucket>,
    schema: SchemaRef, // Immutable after creation - no lock needed
    /// Declared (YAML) schema for `table_name`, resolved once at construction.
    /// Every inbound batch is reconciled against it by [`align_nullability`];
    /// `None` for tables with no registry entry (tests, ad-hoc tables), which
    /// keeps the incoming batch's own metadata authoritative.
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
    /// Span of the row `timestamp` values this bucket actually holds, used
    /// ONLY to decide whether a query window can skip the bucket
    /// (`bucket_overlaps_range`).
    ///
    /// Deliberately separate from `min_timestamp`/`max_timestamp`, which are
    /// widened by the batch's single ROUTING timestamp and carry two other
    /// meanings: the span `get_bucket_ranges` masks Delta over (merge-on-read),
    /// and the flushed watermark. Widening those to the true row span masks
    /// Delta rows that share a timestamp with buffered ones — it dropped 15 of
    /// 20 rows across a dirty restart in `cold_start_under_five_seconds`.
    /// Read pruning is the only consumer that wants the row span, so it gets
    /// its own pair. Sentinels (`MAX`/`MIN`) mean "unknown" and fall back to
    /// the routing span, which is what pre-existing/restored buckets report.
    row_min_ts: AtomicI64,
    row_max_ts: AtomicI64,
    /// Wall-clock micros (via `crate::support`) when this bucket was created.
    /// Drives the flush-dwell staleness signal — how long the bucket has
    /// waited to flush — independent of its rows' event-time range, so
    /// backfilled/late data can't false-trip the "oldest bucket" alarm.
    created_micros: i64,
    /// Per-shard walrus positions captured BEFORE this bucket's first WAL
    /// entry on each shard (min-merged). These are the bucket's read-cursor
    /// *holds*: while the bucket is unflushed, the cursor must not advance
    /// past `first_positions[shard]`, or a crash would replay past this
    /// bucket's acked entries and lose them (prod 2026-07-03).
    wal_shard_state: Mutex<WalShardState>,
    /// While a flush snapshot is airborne, the first N batches are the
    /// snapshot's prefix: insert-time coalesce must not fold across this
    /// boundary or the post-commit prefix drain would remove late
    /// (unflushed) rows merged into a combined batch. 0 = no snapshot in
    /// flight.
    flush_pinned_prefix: AtomicUsize,
    /// Bumped by every in-place DML mutation of this bucket's batches (under
    /// the batches lock). A flush snapshot captures it; if it changed by
    /// commit time, the commit landed pre-DML row values and the prefix
    /// indices may have shifted (DELETE drops emptied batches), so
    /// `finish_flushed_snapshot` must NOT drain — the bucket re-flushes
    /// whole next cycle with the post-DML values.
    mutation_gen: AtomicU64,
    /// Wall-clock micros of the newest WAL entry pinned on this bucket
    /// (WalEntry timestamps are append-time, so this is ARRIVAL time).
    /// Drives [`MemBuffer::reap_expired_empty_buckets`]'s grace period —
    /// see its doc for the (pair-netting) soundness argument; replay itself
    /// no longer filters by age.
    last_wal_pin_micros: AtomicI64,
    /// Real-clock micros of the OLDEST WAL append this bucket's un-flushed
    /// data may depend on — the WAL GC floor: no WAL file whose mtime is at
    /// or after `min(first_wal_pin)` across live buckets may be deleted.
    /// Stamped `Utc::now()` on live inserts and DML pins (the append just
    /// happened); replay buckets are additionally floored at the entry's
    /// original append time via [`MemBuffer::record_replay_hold`]. Event
    /// time is deliberately NOT used: a backfill of old events would drag
    /// the floor days back and suspend WAL GC for the backfill's duration.
    /// Deliberately real-clock (chrono, not `crate::support`) — it is compared
    /// against file mtimes.
    first_wal_pin_micros: AtomicI64,
}

/// Decode the `i64::MAX = no pin` sentinel used by `first_wal_pin_micros`
/// and everything that carries it (taking/inflight/orphan pins).
pub(crate) fn pin_opt(v: i64) -> Option<i64> {
    (v != i64::MAX).then_some(v)
}

#[derive(Debug, Default, Clone)]
struct WalShardState {
    first_positions: Vec<Option<walrus_rust::WalPosition>>,
}

impl WalShardState {
    /// Min-merge a hold for `shard`, growing the vec as needed. The hold is a
    /// *floor*, so the earliest position ever seen for a shard wins.
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
    /// `mutation_gen` at snapshot time (snapshot-flush path only). If the
    /// bucket's gen moved by commit time, a DML mutated it mid-flight: the
    /// commit landed pre-DML values, so `finish_flushed_snapshot` must keep
    /// the rows and re-flush instead of draining.
    pub snapshot_gen: u64,
    /// Actual min/max timestamp of the taken rows, captured before the source
    /// bucket's atomics were reset. `restore_taken_bucket` replays these so a
    /// restored bucket keeps its true time range (and stays visible to
    /// time-range pruning) rather than collapsing to the bucket's start.
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    /// Source bucket's `first_wal_pin_micros` (WAL GC floor) — carried so an
    /// airborne take/commit keeps flooring the GC, and a failed commit's
    /// restore re-applies it.
    pub first_wal_pin_micros: i64,
    /// Key of this take's entry in `MemBuffer::taking_pins`; the flush path
    /// releases it via [`MemBuffer::release_taking_pin`] once its own
    /// inflight pin is registered.
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
    /// consumed without applying; check the warn logs + consider a re-drive.
    pub replay_dml_noops: u64,
    /// Creation wall-clock micros of the oldest non-empty bucket that is
    /// already flushable (`bucket_id < current`), or None. Used to derive
    /// `mem_buffer_oldest_bucket_age_seconds` (`now - this`) — a flush-DWELL
    /// staleness signal (alert if > 2× flush interval). Deliberately NOT the
    /// rows' event-time min: backfilled/late data has a fresh creation time, so
    /// it can't false-trip the alarm while flush is healthy.
    pub oldest_bucket_micros: Option<i64>,
}

/// Drift (as a % of the recomputed truth) that [`MemBuffer::reconcile_estimated_bytes`]
/// tolerates silently. Anything above it is logged at warn as an accounting bug.
const MAX_TOLERATED_DRIFT_PCT: usize = 2;

/// Per-batch fixed overhead: RecordBatch struct, schema Arc bump, ArrayData
/// metadata for each column, and DashMap/Mutex slots when held in a TimeBucket.
/// Empirically ~64 B for the batch + 96 B per column (ArrayData + Buffer headers).
const BATCH_FIXED_OVERHEAD: usize = 64;
const PER_COLUMN_OVERHEAD: usize = 96;

/// Apply a signed byte delta to a memory counter, **saturating at 0** on the
/// way down. Never `fetch_sub`: an underflow wraps a `usize` counter to ~2^64
/// and, on `MemBuffer::estimated_bytes`, permanently rejects every insert
/// (prod: 948 GB reported against a 44 GB budget). Under-reporting is
/// recoverable — the reconciler restores truth within a flush tick.
fn apply_signed_delta(counter: &AtomicUsize, delta: i64) {
    if delta > 0 {
        counter.fetch_add(delta as usize, Ordering::Relaxed);
    } else if delta < 0 {
        sub_saturating(counter, delta.unsigned_abs() as usize);
    }
}

/// `counter -= n`, floored at 0; returns the amount ACTUALLY subtracted (`< n`
/// only when the counter was already below `n`). Callers that mirror a
/// bucket-level subtraction onto `MemBuffer::estimated_bytes` must forward this
/// return value, not `n` — subtracting the unclamped amount from the total is
/// itself a drift source. See [`apply_signed_delta`] for the underflow history.
fn sub_saturating(counter: &AtomicUsize, n: usize) -> usize {
    counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(n))).map_or(0, |prev| prev.min(n))
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
            if Arc::ptr_eq(&vals, l.values()) { arr.clone() } else { Arc::new(ListArray::new(f.clone(), l.offsets().clone(), vals, l.nulls().cloned())) }
        }
        DataType::LargeList(f) => {
            let l = arr.as_any().downcast_ref::<LargeListArray>().unwrap();
            let vals = compact_view_arrays(l.values());
            if Arc::ptr_eq(&vals, l.values()) { arr.clone() } else { Arc::new(LargeListArray::new(f.clone(), l.offsets().clone(), vals, l.nulls().cloned())) }
        }
        DataType::FixedSizeList(f, size) => {
            let l = arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let vals = compact_view_arrays(l.values());
            if Arc::ptr_eq(&vals, l.values()) { arr.clone() } else { Arc::new(FixedSizeListArray::new(f.clone(), *size, vals, l.nulls().cloned())) }
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
/// buffers are mostly someone else's bytes. Arrow IPC decode during WAL replay
/// reads the whole message body into one allocation and hands
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
    if cols.iter().zip(batch.columns()).all(|(a, b)| Arc::ptr_eq(a, b)) { batch } else { RecordBatch::try_new(batch.schema(), cols).unwrap_or(batch) }
}

/// Collapse rows in `batches` to one row per unique value of `keys`.
/// Empty `keys` or empty input → no-op. Surviving row order is preserved.
///
/// `tiebreak`: when rows share a key tuple, keep the one with the greatest value
/// in this column (e.g. `observed_timestamp` so an enriched re-emit wins over the
/// base row — parity plan Defect 3); ties fall back to last-occurrence. `None`
/// = pure keep-last by position (the back-compat default; identical retries
/// behave identically either way). Nulls sort lowest, so a non-null tiebreak
/// always beats a null one.
///
/// `drop_tombstones`: name of the schema's `tombstone_column` **only when the
/// caller's scope guarantees that no older version of any key in `batches` can
/// survive outside this call's input** — then a key whose winning version is a
/// tombstone (`true`) disappears entirely instead of leaving the tombstone
/// behind. `None` = retain the tombstone as the surviving version. Every
/// current caller passes `None`; see the safety derivation at the dedup sweep's
/// call site in `database.rs`. Dropping early is silent data resurrection,
/// retaining is a bounded storage cost — when unsure, pass `None`.
///
/// Only collapses dupes inside this call's input — cross-bucket dupes need
/// the read-side dedup (`DedupExec`).
pub fn dedup_batches(batches: Vec<RecordBatch>, keys: &[String], tiebreak: Option<&str>, drop_tombstones: Option<&str>) -> anyhow::Result<Vec<RecordBatch>> {
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
    // Concatenate one named column across all batches (never the full payload).
    let concat_col = |name: &str| -> anyhow::Result<ArrayRef> {
        let cols = batches
            .iter()
            .map(|b| b.column_by_name(name).cloned().ok_or_else(|| anyhow::anyhow!("dedup column `{name}` missing from batch schema")))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(concat(&cols.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?)
    };
    let key_arrays: Vec<ArrayRef> = keys.iter().map(|k| concat_col(k)).collect::<anyhow::Result<_>>()?;
    let converter = RowConverter::new(key_arrays.iter().map(|a| SortField::new(a.data_type().clone())).collect())?;
    let rows = converter.convert_columns(&key_arrays)?;

    // Optional tiebreak column, encoded order-preservingly (same concat-just-this-
    // -column trick as the keys, so we never fuse the fat payload).
    // A MISSING tiebreak degrades to "no tiebreak" instead of failing the flush.
    // Enabling `version_append` on an existing table adds the tiebreak column,
    // but rows already buffered (or replayed from the WAL) were written under
    // the OLD schema and don't carry it. Erroring here made those buckets
    // permanently unflushable: they pinned MemBuffer at the hard limit forever
    // while newer buckets drained fine (prod 2026-08-02, ~8k failures/2min of
    // "dedup column `updated_at` missing from batch schema"). Unstamped rows
    // carry no version information, so last-occurrence-wins is the correct
    // reading of them, and cross-version resolution belongs to `DedupExec`,
    // which already ranks a NULL stamp below any stamped version.
    // Missing dedup KEYS stay fatal — that is a real schema fault, not legacy data.
    let has_tiebreak = |col: &str| batches.iter().all(|b| b.column_by_name(col).is_some());
    let tb_rows = match tiebreak.filter(|col| has_tiebreak(col)) {
        Some(col) => {
            let arr = concat_col(col)?;
            let conv = RowConverter::new(vec![SortField::new(arr.data_type().clone())])?;
            Some(conv.convert_columns(&[arr])?)
        }
        None => None,
    };

    // Choose one surviving index per key. With no tiebreak: last occurrence wins
    // (unconditional insert). With a tiebreak: the greatest value wins, ties →
    // last (we only keep the existing pick when its tiebreak is strictly greater).
    // BORROWED keys, ahash, no per-row allocation. This runs once per row of
    // every flush and every dedup rewrite, and it used to `.owned()` each key —
    // one heap allocation per row — into a SipHash map. `Row<'_>` already hashes
    // and compares by the same encoded bytes, and it borrows `rows`, which
    // outlives this map, so the copy bought nothing.
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
    // Tombstone winners. Loaded before the no-duplicates fast path because a
    // tombstone with no surviving older version in this input is still a row to
    // drop. `true_count() == 0` (the all-NULL/all-false steady state of a table
    // that has never been deleted from) keeps the fast path intact.
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
        // No duplicates — return the batches untouched (never fused into one array).
        return Ok(batches);
    }
    // NULL and false both mean live; only `true` retires the key.
    //
    // A dense bitmap, not a `HashSet<u32>`: the survivors are indices into
    // `0..num_rows`, so the set below is a direct-addressed lookup with no
    // hashing at all — and the filter loop under it probes it once per row.
    let mut keep = vec![false; rows.num_rows()];
    for i in chosen.into_values().filter(|&i| tombstones.as_ref().is_none_or(|f| !(f.is_valid(i as usize) && f.value(i as usize)))) {
        keep[i as usize] = true;
    }
    // Filter each batch against its slice of the global row-index space
    // (`scan` carries the running base offset).
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
    let sql_expr = SqlParser::new(&GenericDialect {})
        .try_with_sql(sql)
        .map_err(|e| datafusion::error::DataFusionError::SQL(e.into(), None))?
        .parse_expr()
        .map_err(|e| datafusion::error::DataFusionError::SQL(e.into(), None))?;
    // Register the same expr planners the live session has, so DML SQL that
    // uses array literals (`[...]`), struct/field access, etc. re-parses on WAL
    // replay. Without the nested-expression planner the monoscope `hashes`
    // UPDATE...FROM (`array_concat(CASE ..., [tag])`) fails with "Could not plan
    // array literal" and quarantines — even after qualifier normalization.
    // VariantAwareExprPlanner first (matches live ordering for `->`/`->>`).
    let expr_planners: Vec<Arc<dyn datafusion::logical_expr::planner::ExprPlanner>> =
        std::iter::once(Arc::new(crate::read::functions::VariantAwareExprPlanner) as Arc<dyn datafusion::logical_expr::planner::ExprPlanner>)
            .chain(datafusion::execution::SessionStateDefaults::default_expr_planners())
            .collect();
    let context_provider = RegistryContextProvider { registry, expr_planners };
    let planner = SqlToRel::new(&context_provider);
    let expr = planner.sql_to_expr(sql_expr, schema, &mut Default::default())?;
    // DF54's comparison kernels no longer auto-coerce mismatched string views
    // (e.g. `upper(name)` Utf8View vs a Utf8 literal), so apply the same type
    // coercion the analyzer would before this predicate is lowered to a physical expr.
    Ok(expr.rewrite(&mut datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter::new(schema))?.data)
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
        // Normalize every timestamp literal precision to micros.
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

/// Filter a batch to rows whose `id` is in `ids`. Returns a fresh batch.
/// On any error (missing `id` column, unexpected type) the batch is
/// returned unfiltered — the caller's predicate-based filter will catch
/// any over-inclusion. Supports Utf8View, Utf8, and LargeUtf8 ID types.
fn filter_batch_by_id_set(batch: &RecordBatch, ids: &std::collections::HashSet<String>) -> RecordBatch {
    use arrow::array::{AsArray, BooleanArray};
    let Some(arr) = batch.column_by_name("id") else { return batch.clone() };
    let hit = |v: Option<&str>| v.is_some_and(|s| ids.contains(s));
    let mask: BooleanArray = match arr.data_type() {
        DataType::Utf8View => arr.as_string_view().iter().map(hit).collect(),
        DataType::Utf8 => arr.as_string::<i32>().iter().map(hit).collect(),
        DataType::LargeUtf8 => arr.as_string::<i64>().iter().map(hit).collect(),
        // Unknown id type — let the original predicate handle the filtering.
        _ => return batch.clone(),
    };
    filter_record_batch(batch, &mask).unwrap_or_else(|_| batch.clone())
}

/// Apply a compiled predicate, returning only matching rows. Best-effort: on
/// any evaluation error we return the original batch so DataFusion's FilterExec
/// can finish the job.
fn apply_predicate(batch: &RecordBatch, pred: &Arc<dyn datafusion::physical_expr::PhysicalExpr>) -> RecordBatch {
    pred.evaluate(batch)
        .ok()
        .and_then(|v| v.into_array(batch.num_rows()).ok())
        .and_then(|arr| filter_record_batch(batch, arr.as_any().downcast_ref::<BooleanArray>()?).ok())
        .unwrap_or_else(|| batch.clone())
}

/// Apply an optional compiled predicate to a bucket snapshot, dropping
/// non-matching rows and any batch that ends up empty. `None` returns the
/// snapshot unchanged.
pub fn filter_snapshot(snapshot: Vec<RecordBatch>, pred: &Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>) -> Vec<RecordBatch> {
    match pred {
        Some(p) => snapshot.iter().map(|b| apply_predicate(b, p)).filter(|b| b.num_rows() > 0).collect(),
        None => snapshot,
    }
}

/// MemBuffer's contribution to a scan: one partition per surviving time
/// bucket, plus whether EVERY partition is ordered by the table's declared
/// `sorting_columns`. Only then may the caller declare that ordering on the
/// `MemorySourceConfig` — the flag is a correctness claim, not a hint.
#[derive(Debug, Default)]
pub struct MemLeg {
    pub partitions: Vec<Vec<RecordBatch>>,
    pub sorted: bool,
}

/// Sort one partition's batches into a single batch ordered by `schema`'s
/// declared `sorting_columns`, or `None` when that cannot be done truthfully.
///
/// This is what lets the in-memory legs DECLARE an ordering
/// (`MemorySourceConfig::try_with_sort_information`). Without it, a
/// merge-on-read scan's `keep_greatest` requirement is unsatisfied by the mem
/// and hot legs, so `ordered_children` injects a fetch-less blocking `SortExec`
/// over each of them and `DedupExec` falls back to its unbounded `full-set`
/// seen-set.
///
/// Only the leading RUN of sorting columns that are actually present is used,
/// and the first one is mandatory: the caller declares the first column, so
/// silently sorting by a later column instead would make that declaration a
/// lie — and a false ordering claim is a wrong-results bug, not a slow plan.
pub fn sort_partition(schema: &crate::schema::TableSchema, batches: Vec<RecordBatch>) -> Option<Vec<RecordBatch>> {
    use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take_record_batch};
    if batches.is_empty() {
        return None;
    }
    let arrow_schema = batches[0].schema();
    // A schema-diverse partition is left alone rather than merged: the merge
    // is the expensive, failure-prone half of `sort_batches_by_schema` and this
    // path runs per query. Undeclared ordering is always safe.
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
    let sorted = match indices.values().iter().enumerate().all(|(i, &v)| v as usize == i) {
        true => combined,
        false => take_record_batch(&combined, &indices).ok()?,
    };
    // Hand back BATCHES, not the one concatenated monolith: a global lexsort has
    // to materialize the partition once, but nothing downstream should have to
    // hold it as a single value. Slicing is zero-copy and order-preserving, so
    // this restores the batch shape both callers had before — a demoted IPC file
    // stays multi-batch, and a scan partition stays pollable in steps rather
    // than as one giant batch.
    Some((0..sorted.num_rows()).step_by(SORT_CHUNK_ROWS).map(|off| sorted.slice(off, SORT_CHUNK_ROWS.min(sorted.num_rows() - off))).collect())
}

/// Rows per batch handed back by [`sort_partition`]. Matches the order of
/// magnitude of a normal flush batch; the exact value is not load-bearing.
const SORT_CHUNK_ROWS: usize = 8192;

/// The DISTINCT bucket ids `batch`'s rows land in, keyed off `time_col`.
/// Empty when the column is absent or is not an i64-backed time type — the
/// caller then simply marks nothing, which is the conservative direction only
/// for callers that treat "unknown" as "do not exempt"; today's single caller
/// (`BufferedWriteLayer::insert_versions`) is on a table that always declares
/// the column.
pub fn batch_bucket_ids(batch: &RecordBatch, time_col: &str) -> Vec<i64> {
    let Some(col) = batch.column_by_name(time_col) else { return Vec::new() };
    let Some(values) = crate::read::bound_slice(col) else { return Vec::new() };
    let mut ids: Vec<i64> = values.iter().map(|&t| MemBuffer::compute_bucket_id(t)).collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Half-open `[start, end)` interval overlap — the ONE range convention shared
/// by MemBuffer bucket ranges, hot-tier file ranges, and the Delta exclusion
/// filters built from them.
pub fn overlaps(a: (i64, i64), b: (i64, i64)) -> bool {
    a.0 < b.1 && b.0 < a.1
}

/// Sort + coalesce touching or overlapping half-open ranges. The Delta leg
/// excludes the UNION of the other tiers' ranges, and consecutive sealed
/// buckets are contiguous, so this collapses what would be one
/// `(ts < a OR ts >= b)` conjunct per bucket into (typically) one.
pub fn merge_ranges(mut ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    ranges.sort_unstable();
    ranges.into_iter().fold(Vec::new(), |mut out, (start, end)| {
        match out.last_mut() {
            Some((_, prev_end)) if start <= *prev_end => *prev_end = (*prev_end).max(end),
            _ => out.push((start, end)),
        }
        out
    })
}

/// Check if a bucket's time range overlaps with the query range.
fn bucket_overlaps_range(bucket: &TimeBucket, (min_filter, max_filter): &(Option<i64>, Option<i64>)) -> bool {
    // `&&` short-circuits, so an unbounded side never pays its atomic load.
    // The sentinels (empty bucket) mean "unknown range" — never prune.
    let lo = match bucket.row_min_ts.load(Ordering::Relaxed) {
        i64::MAX => bucket.min_timestamp.load(Ordering::Relaxed),
        v => v,
    };
    let hi = match bucket.row_max_ts.load(Ordering::Relaxed) {
        i64::MIN => bucket.max_timestamp.load(Ordering::Relaxed),
        v => v,
    };
    let starts_after = |max: i64| lo != i64::MAX && lo > max;
    let ends_before = |min: i64| hi != i64::MIN && hi < min;
    !max_filter.is_some_and(starts_after) && !min_filter.is_some_and(ends_before)
}

/// Does the YAML schema declare any tantivy-indexed field? Gates every
/// text-match path: without one the per-bucket index build can only ever
/// return None, so the caller falls straight back to the SQL predicate.
fn has_indexed_fields(schema: &crate::schema::TableSchema) -> bool {
    schema.fields.iter().any(|f| f.tantivy.as_ref().is_some_and(|t| t.indexed))
}

/// Strip table qualifiers from Column refs (e.g. `otel_logs_and_spans.timestamp` → `timestamp`)
/// so exprs from SQL planning resolve against the bare-column DFSchema built from the
/// in-memory table.
pub(crate) fn strip_column_qualifiers(expr: Expr) -> DFResult<Expr> {
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
            flushed_max: DashMap::new(),
            taking_pins: DashMap::new(),
            taking_seq: AtomicU64::new(0),
            replay_dml_noops: AtomicU64::new(0),
        }
    }

    /// Record that `bucket_id`'s rows were committed to Delta while the
    /// bucket was still open — see the `force_flushed` field docs. Called
    /// before the commit so no query can race into the masked window; a
    /// failed commit leaves a stale mark, which only costs that bucket the
    /// brief commit-then-drain exclusion at its eventual sealed flush.
    /// Record that everything up to `ts` in this bucket is now Delta's.
    /// Monotonic: a failed commit restores the rows, and an unfloored Delta leg
    /// merely contributes nothing there — the safe direction.
    fn note_flushed_through(&self, key: TableKey, bucket_id: i64, ts: i64) {
        self.flushed_max.entry(key).or_default().entry(bucket_id).and_modify(|v| *v = (*v).max(ts)).or_insert(ts);
    }

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

    /// Cache key for a bucket. Builds an Arc<str> per call but only on the
    /// cache-miss path, so the hot lookup is cheap (the bucket_id alone).
    fn cache_key(project_id: &str, table_name: &str, bucket_id: i64) -> BucketCacheKey {
        (Arc::from(project_id), Arc::from(table_name), bucket_id)
    }

    /// Look up a cached text index. Promotes the entry to MRU on hit.
    fn cache_get(&self, key: &BucketCacheKey) -> Option<Arc<crate::tantivy::BucketTextIndex>> {
        self.text_index_cache.lock().get(key).cloned()
    }

    /// Insert a freshly-built index into the cache, evicting LRU entries
    /// to stay under `text_index_max_bytes`. Returns the inserted Arc.
    fn cache_put(&self, key: BucketCacheKey, idx: Arc<crate::tantivy::BucketTextIndex>) -> Arc<crate::tantivy::BucketTextIndex> {
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
            let Some((_, evicted)) = cache.pop_lru() else { break };
            self.text_index_bytes.fetch_sub(evicted.size_bytes, Ordering::Relaxed);
        }
        idx
    }

    /// Drop the cached entry for a bucket. Called by `insert_batch` and
    /// drain to keep the cache from going stale.
    fn cache_invalidate(&self, key: &BucketCacheKey) {
        if let Some(old) = self.text_index_cache.lock().pop(key) {
            self.text_index_bytes.fetch_sub(old.size_bytes, Ordering::Relaxed);
        }
    }

    /// Live MemBuffer size in bytes — a single relaxed atomic load of the
    /// running total maintained by every mutation site (enumerated on
    /// [`Self::estimated_bytes`]).
    ///
    /// History, because this method has been both things:
    /// - It started as this cached counter, but the in-bucket coalesce path
    ///   shrank `bucket.memory_bytes` without reporting the savings upward,
    ///   leaking `(sum_before − combined_size)` per coalesce, and a mismatched
    ///   `fetch_sub` wrapped the counter to ~948 GB against a 44 GB budget —
    ///   after which every `try_reserve_memory` rejected forever ("Memory
    ///   limit exceeded"). Fixed by summing the bucket atomics on every call.
    /// - That sum is O(tables × buckets) with a DashMap shard lock per table,
    ///   and `try_reserve_memory` calls it once per CAS *attempt*. At prod
    ///   scale it became 10.3% of total process CPU — ~96% of the insert
    ///   path's cost (profile 2026-07-30).
    ///
    /// So we are back to the counter, with both original failure modes closed:
    /// coalesce now folds its shrinkage into the delta the insert site applies,
    /// every subtraction saturates at 0 (see [`apply_signed_delta`]), and
    /// [`Self::reconcile_estimated_bytes`] re-derives the authoritative sum
    /// once per flush tick, storing it and warning on non-trivial drift. Any
    /// residual error is therefore bounded by one tick, not permanent.
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
    /// authoritative sum, store it, and warn if the cached value was off by
    /// more than [`MAX_TOLERATED_DRIFT_PCT`]. Called once per flush-task timer
    /// tick (~`flush_interval_secs`) so one O(N) sweep per minute replaces one
    /// per insert. Returns `(cached_before, truth)`.
    ///
    /// The warn is a bug detector: a non-trivial drift means some site that
    /// mutates a bucket's `memory_bytes` is not reporting its delta (that is
    /// exactly the class of bug that produced the 948 GB wedge), so treat it
    /// as an action item, not noise.
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

    /// Get or create a TableBuffer, returning a cached Arc reference.
    /// This is the preferred entry point for batch operations - cache the returned
    /// Arc<TableBuffer> and call insert_batch() directly to avoid repeated lookups.
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

        // Fast path: table exists
        if let Some(table) = self.tables.get(&key) {
            // Registered tables canonicalize the actual batch against their
            // declared schema in `insert_batch`; comparing the caller's
            // pre-canonical column order here would reject valid WAL entries.
            if table.declared.is_none() {
                ensure_compatible(table.schema())?;
            }
            return Ok(Arc::clone(&table));
        }

        // Slow path: create table using entry API
        match self.tables.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                if entry.get().declared.is_none() {
                    ensure_compatible(entry.get().schema())?;
                }
                Ok(Arc::clone(entry.get()))
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let new_table = Arc::new(TableBuffer::new(schema.clone(), Arc::from(project_id), Arc::from(table_name)));
                entry.insert(Arc::clone(&new_table));
                Ok(new_table)
            }
        }
    }

    /// Get a TableBuffer if it exists (for read operations).
    fn get_table(&self, project_id: &str, table_name: &str) -> Option<Arc<TableBuffer>> {
        self.tables.get(&table_key(project_id, table_name)).map(|t| Arc::clone(&t))
    }

    #[instrument(skip(self, batch), fields(project_id, table_name, rows))]
    pub fn insert(&self, project_id: &str, table_name: &str, batch: RecordBatch, timestamp_micros: i64) -> anyhow::Result<()> {
        self.insert_with_hold(project_id, table_name, batch, timestamp_micros, None)
    }

    /// `wal_hold` = (shard, pre-append position) of the batch's WAL entry;
    /// recorded atomically with the batch (under the bucket lock) so a
    /// concurrent take can never separate rows from their cursor hold. Pass
    /// None only when the entry needs no pin (WAL replay pins via
    /// [`Self::record_replay_hold`] instead).
    pub fn insert_with_hold(
        &self, project_id: &str, table_name: &str, batch: RecordBatch, timestamp_micros: i64, wal_hold: Option<(usize, walrus_rust::WalPosition)>,
    ) -> anyhow::Result<()> {
        let declared = crate::schema::get_schema(table_name).map(|schema| schema.schema_ref());
        let batch = canonicalize_declared_batch(batch, declared.as_ref())?;
        let schema = batch.schema();
        let table = self.get_or_create_table(project_id, table_name, &schema)?;
        let (mem_delta, bucket_id) = table.insert_batch(batch, timestamp_micros, wal_hold)?;
        apply_signed_delta(&self.estimated_bytes, mem_delta);
        // Drop any stale text-index cache entry for this bucket — the
        // `indexed_rows == snapshot_rows` check would reject it on the
        // next query anyway, but freeing the bytes now lets the LRU give
        // budget to other buckets immediately.
        self.cache_invalidate(&Self::cache_key(project_id, table_name, bucket_id));
        Ok(())
    }

    /// Pin the bucket owning `timestamp_micros` at a replayed entry's REAL WAL
    /// `(shard, pos)` (min-merge per shard). Used by WAL recovery so the rewind
    /// marker can advance bucket-by-bucket as buckets drain (resumable replay)
    /// instead of freezing at the pre-recovery cursor.
    pub fn record_replay_hold(&self, project_id: &str, table_name: &str, timestamp_micros: i64, shard: usize, pos: walrus_rust::WalPosition) {
        let key = table_key(project_id, table_name);
        let Some(table) = self.tables.get(&key) else {
            return;
        };
        let bucket_id = Self::compute_bucket_id(timestamp_micros);
        if let Some(bucket) = table.buckets.get(&bucket_id) {
            bucket.record_wal_append(shard, Some(pos));
            // GC floor at the entry's ORIGINAL append time (replay entry
            // timestamps are append-time): the backing file's mtime is that
            // old, so the insert path's now-stamp alone would let GC delete
            // it from under the parked cursor.
            bucket.first_wal_pin_micros.fetch_min(timestamp_micros, Ordering::Relaxed);
        }
    }

    /// Oldest WAL-append real-clock micros any un-flushed bucket may depend
    /// on — the WAL GC floor. `None` when nothing is buffered. Includes
    /// buckets mid-take (see `taking_pins`): between `take_bucket_for_flush`
    /// removing a bucket and the flush path registering its inflight pin, the
    /// pin must stay visible or a concurrent GC sweep can delete the bucket's
    /// backing file. See `TimeBucket::first_wal_pin_micros`.
    pub fn oldest_wal_append_micros(&self) -> Option<i64> {
        self.tables
            .iter()
            .filter_map(|t| t.buckets.iter().filter_map(|b| pin_opt(b.first_wal_pin_micros.load(Ordering::Relaxed))).min())
            .chain(self.taking_pins.iter().map(|e| *e.value()))
            .min()
    }

    /// Drop a take-in-progress pin once the flush path has registered its own
    /// inflight pin for the taken bucket (always paired with a successful
    /// `take_bucket_for_flush`).
    pub fn release_taking_pin(&self, seq: u64) {
        self.taking_pins.remove(&seq);
    }

    /// Per-shard min of `first_positions` across every live bucket of the
    /// table — the earliest WAL entry still owned by unflushed in-memory
    /// data. The flush path advances the read cursor to exactly this (or the
    /// write tail when no bucket holds a position).
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

    /// (project_id, table_name, bucket_id) for every bucket whose id passes
    /// `filter`. Drives the take-based flush paths.
    /// `(bucket_id, created_micros, memory_bytes)` for every bucket matching
    /// `filter`, across all tables — the flush dwell gate's input. Same id can
    /// appear once per (project, table).
    pub fn bucket_flush_meta(&self, filter: impl Fn(i64) -> bool) -> Vec<(i64, i64, usize)> {
        self.tables
            .iter()
            .flat_map(|t| {
                t.value()
                    .buckets
                    .iter()
                    .filter(|b| filter(*b.key()))
                    .map(|b| (*b.key(), b.value().created_micros, b.value().memory_bytes.load(std::sync::atomic::Ordering::Relaxed)))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn bucket_keys(&self, filter: impl Fn(i64) -> bool) -> Vec<(String, String, i64)> {
        self.tables
            .iter()
            .flat_map(|t| {
                let (project_id, table_name) = t.key();
                // Collect per table: the DashMap ref can't outlive this closure.
                t.value()
                    .buckets
                    .iter()
                    .map(|b| *b.key())
                    .filter(|id| filter(*id))
                    .map(|id| (project_id.to_string(), table_name.to_string(), id))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub fn insert_batches(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>, timestamp_micros: i64) -> anyhow::Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let schema = batches[0].schema();
        let table = self.get_or_create_table(project_id, table_name, &schema)?;

        let (total_delta, touched_buckets) = batches.into_iter().try_fold((0i64, std::collections::HashSet::new()), |(delta, mut touched), batch| {
            let (sz, bucket_id) = table.insert_batch(batch, timestamp_micros, None)?;
            touched.insert(bucket_id);
            anyhow::Ok((delta + sz, touched))
        })?;
        apply_signed_delta(&self.estimated_bytes, total_delta);
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
        &self, project_id: &str, table_name: &str, preds: &[crate::tantivy::udf::TextMatchPred],
    ) -> anyhow::Result<Option<std::collections::HashSet<String>>> {
        let Some(node) = crate::tantivy::udf::PredNode::from_preds(preds) else {
            return Ok(None);
        };
        // Checking once here avoids the (always-None) per-bucket build.
        let Some(table_schema) = crate::schema::get_schema(table_name).filter(|s| has_indexed_fields(s)) else {
            return Ok(None);
        };
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
        // `None` = no bucket produced a usable index (caller falls back to the
        // SQL predicate); `Some` = union of every usable bucket's hits.
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

    /// Any buffered rows for (project, table) whose timestamps could fall in
    /// `[lo, hi]`? Bucket-granular (min/max check) — may report `true` for a
    /// bucket whose actual rows all fall outside, which is the safe direction
    /// for callers gating exact-count shortcuts.
    pub fn has_rows_in_range(&self, project_id: &str, table_name: &str, lo: i64, hi: i64) -> bool {
        let Some(table) = self.get_table(project_id, table_name) else {
            return false;
        };
        let range = (Some(lo), Some(hi));
        table.buckets.iter().any(|b| b.value().row_count.load(Ordering::Relaxed) > 0 && bucket_overlaps_range(b.value(), &range))
    }

    /// Lower bound on the timestamp of any row still buffered for
    /// `(project, table)` that could fall in `[lo, hi]`, or `None` when nothing
    /// is buffered there.
    ///
    /// The bound is the minimum of the bucket's KEY-derived start and its
    /// published `min_timestamp`. The key is fixed when the bucket is created
    /// and is a lower bound on every row the bucket can ever hold, whereas
    /// `min_timestamp` is stored *after* `row_count` — so a reader can observe a
    /// non-empty bucket whose min has not dropped yet. Taking only the latter
    /// would hand back a horizon that is too NEW, and every row below it would
    /// be served from a rollup that does not contain it. Taking both is free and
    /// also absorbs `compute_bucket_id`'s truncating division on negative
    /// timestamps, where `key * duration` sits above the rows it holds.
    ///
    /// Scoped to `[lo, hi]` on purpose: one late straggler outside the query's
    /// window must not collapse the certified horizon for every other query.
    pub fn min_buffered_micros(&self, project_id: &str, table_name: &str, lo: i64, hi: i64) -> Option<i64> {
        let table = self.get_table(project_id, table_name)?;
        let range = (Some(lo), Some(hi));
        table
            .buckets
            .iter()
            .filter(|b| b.value().row_count.load(Ordering::Relaxed) > 0 && bucket_overlaps_range(b.value(), &range))
            .map(|b| {
                let start = b.key().saturating_mul(bucket_duration_micros());
                match b.value().min_timestamp.load(Ordering::Relaxed) {
                    i64::MAX => start,
                    min => start.min(min),
                }
            })
            .min()
    }

    /// Atomic MemBuffer query with text-match prefilter. For each bucket:
    ///   - Snapshot batches + run text_match search → ID set (under the
    ///     same `batches` lock).
    ///   - Apply `id IN (ids)` and the rest of `filters` to the snapshot.
    ///
    /// This guarantees the prefilter and the data come from the same point
    /// in time — closing the race where a concurrent insert would otherwise
    /// be visible in the data but absent from the prefilter ID set.
    ///
    /// When `node` is None or the table has no indexed fields, behaves
    /// exactly like `query_partitioned`.
    #[instrument(skip(self, filters, node), fields(project_id, table_name))]
    pub fn query_partitioned_with_text_match(
        &self, project_id: &str, table_name: &str, filters: &[Expr], node: Option<&crate::tantivy::udf::PredNode>,
    ) -> anyhow::Result<MemLeg> {
        // No preds or no indexed field ⇒ no prefilter ⇒ plain bucket scan.
        self.scan_buckets(project_id, table_name, filters, crate::schema::get_schema(table_name).filter(|s| has_indexed_fields(s)).zip(node))
    }

    /// Flattened [`Self::query_partitioned`] — same rows, bucket partitioning dropped.
    #[instrument(skip(self, filters), fields(project_id, table_name))]
    pub fn query(&self, project_id: &str, table_name: &str, filters: &[Expr]) -> anyhow::Result<Vec<RecordBatch>> {
        Ok(self.query_partitioned(project_id, table_name, filters)?.partitions.into_iter().flatten().collect())
    }

    /// Query and return partitioned data - one partition per time bucket.
    /// This enables parallel execution across time buckets.
    /// Optional filters enable timestamp-based bucket pruning.
    #[instrument(skip(self, filters), fields(project_id, table_name))]
    pub fn query_partitioned(&self, project_id: &str, table_name: &str, filters: &[Expr]) -> anyhow::Result<MemLeg> {
        self.scan_buckets(project_id, table_name, filters, None)
    }

    /// Bucket scan shared by both query entry points: prune by timestamp
    /// range, snapshot each surviving bucket — atomically with its text-match
    /// id set when `text` is given — then apply the compiled predicate.
    /// Empty partitions are dropped.
    fn scan_buckets(
        &self, project_id: &str, table_name: &str, filters: &[Expr], text: Option<(&crate::schema::TableSchema, &crate::tantivy::udf::PredNode)>,
    ) -> anyhow::Result<MemLeg> {
        let ts_range = extract_timestamp_range(filters);
        let Some(table) = self.get_table(project_id, table_name) else { return Ok(MemLeg::default()) };
        // Pre-compile filters into a single physical predicate so each batch is
        // filtered to matching rows before returning. Best-effort: anything that
        // fails to compile is left for FilterExec on top to evaluate.
        let pred = compile_filter_conjunction(filters, &table.schema).ok().flatten();
        let mut bucket_ids: Vec<i64> = table.buckets.iter().map(|b| *b.key()).collect();
        bucket_ids.sort_unstable();

        let partitions = bucket_ids
            .into_iter()
            .filter_map(|bucket_id| table.buckets.get(&bucket_id).filter(|b| bucket_overlaps_range(b, &ts_range)).map(|b| (bucket_id, b)))
            .map(|(bucket_id, bucket)| {
                // Hold the lock only long enough to clone Arc'd batch refs (and,
                // with a prefilter, to take the id set atomically with them);
                // release before filtering so writers aren't blocked.
                let (snapshot, ids) = match text {
                    Some((schema, node)) => self.search_with_snapshot(&bucket, &Self::cache_key(project_id, table_name, bucket_id), schema, node)?,
                    None => (bucket.batches.lock().to_vec(), None),
                };
                // Apply id IN ids (atomic with snapshot) when available; the
                // rest of `filters` (including the original `=` / `LIKE` /
                // `text_match` UDF call) runs afterwards via the compiled
                // predicate. Without an id set, fall through to predicate-only.
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

        // Sort each partition so the leg can DECLARE its ordering (see
        // `sort_partition`). Done here, on the POST-FILTER rows, rather than on
        // the insert path: ingest throughput is untouched, and the rows sorted
        // are only those the query actually returns. This is not extra work
        // overall — an undeclared leg makes `ordered_children` inject a
        // `SortExec` over the very same rows — but it is work the plan can now
        // see, which is what keeps `DedupExec` on its bounded seen-set.
        //
        // All-or-nothing: `try_with_sort_information` declares one ordering for
        // the whole source, so one unsortable partition (schema-diverse bucket,
        // sorting column projected away) must retract the claim for all of them.
        let schema = crate::schema::get_schema(table_name).filter(|s| !s.sorting_columns.is_empty());
        let (partitions, sorted) = match schema {
            Some(s) => match partitions.iter().map(|p| sort_partition(s, p.clone())).collect::<Option<Vec<_>>>() {
                Some(sorted) => (sorted, true),
                None => {
                    // WHY the whole leg lost its claim, because the consequence is
                    // out of all proportion to the cause: ONE unsortable partition
                    // retracts the ordering for every partition, the union then
                    // advertises none, `DedupExec` falls to unbounded `full-set`,
                    // and `ORDER BY ts DESC LIMIT n` materialises the window.
                    //
                    // Schema diversity is the expected reason and the alarming one:
                    // `insert_batch` deliberately ACCEPTS nullable field additions,
                    // so a single new optional field appearing in the ingest stream
                    // is enough to make one bucket diverse and degrade every listing
                    // query on that project. Counted apart from the other refusals
                    // (sorting column absent, concat/lexsort failure) because it is
                    // a different fix.
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
                    (partitions, false)
                }
            },
            None => (partitions, false),
        };

        debug!(
            "MemBuffer scan_buckets: project={}, table={}, text_match={}, partitions={}, sorted={sorted}",
            project_id,
            table_name,
            text.is_some(),
            partitions.len()
        );
        Ok(MemLeg { partitions, sorted })
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
        let force_flushed = self.force_flushed.get(&table_key(project_id, table_name));
        let flushed_max = self.flushed_max.get(&table_key(project_id, table_name));
        let mut ranges: Vec<(i64, i64)> = table
            .buckets
            .iter()
            .filter(|b| *b.key() != current && !force_flushed.as_ref().is_some_and(|s| s.contains(b.key())))
            .filter_map(|b| {
                let (min, max) = (b.min_timestamp.load(Ordering::Relaxed), b.max_timestamp.load(Ordering::Relaxed));
                // Never mask an instant this bucket has already committed. See
                // `flushed_max`: the mask's premise is "memory is authoritative
                // here", and for anything at or below the flushed watermark that
                // is simply false — those rows are in Delta and gone from memory,
                // so masking them makes them answer no query at all.
                //
                // The cost is stated where the prefix drain first made this trade:
                // an unmasked survivor can be double-counted, and a duplicate is
                // collapsed by read-side dedup while a masked row is just wrong.
                let min = flushed_max.as_ref().and_then(|m| m.get(b.key())).map_or(min, |hi| min.max(hi.saturating_add(1)));
                (min <= max).then_some((min, max + 1))
            })
            .collect();
        ranges.sort_by_key(|(s, _)| *s);
        ranges
    }

    /// Snapshot a sealed bucket for flush WITHOUT removing its rows — they
    /// stay queryable while the Delta commit is airborne (a take here made
    /// every periodic flush a multi-minute read blackout for that window).
    /// WAL holds are taken (reset) exactly like `take_bucket_for_flush`, so
    /// late arrivals pin themselves and the snapshot's holds ride the
    /// caller's in-flight registry. After the commit lands,
    /// [`Self::finish_flushed_snapshot`] removes exactly the snapshotted
    /// batches (or keeps them when a DML dirtied the bucket); on failure
    /// [`Self::restore_snapshot_holds`] merges the holds
    /// back (the rows never left).
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
        // Fence the snapshot prefix against insert-time coalesce (see field docs).
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
            // Snapshot leaves the bucket (and its pin) in the map — no
            // taking-pin gap to cover. u64::MAX is never allocated by
            // `taking_seq`, so releasing it is a no-op.
            taking_pin_seq: u64::MAX,
        })
    }

    /// Complete a successful snapshot-flush for one source bucket.
    ///
    /// Clean case (`mutation_gen` unchanged since the snapshot): remove
    /// exactly the snapshotted prefix batches, preserving any appended since
    /// (their holds were recorded post-snapshot and still pin them), and drop
    /// the bucket entirely when nothing remains.
    ///
    /// Dirty case (a DML mutated the bucket while the commit was airborne):
    /// the commit landed PRE-DML values and the prefix indices may have
    /// shifted (DELETE drops emptied batches), so draining would both lose
    /// the post-DML rows and remove the wrong batches. Keep all rows and
    /// merge the snapshot's holds back. The stale Delta copies are corrected
    /// by the DML's own Delta leg, which `DmlContext` orders AFTER in-flight
    /// commits (`BufferedWriteLayer::await_inflight_flushes`) — updates get
    /// merged, deletes removed — while the living bucket's exclusion masks
    /// them in the interim. The next cycle re-flushes the post-DML state.
    ///
    /// Returns true when the prefix was drained (clean case) — callers count
    /// flush metrics only for drained buckets, since a dirty-kept bucket's
    /// rows are neither freed nor authoritative in Delta yet.
    pub fn finish_flushed_snapshot(&self, b: &FlushableBucket) -> bool {
        let key = table_key(&b.project_id, &b.table_name);
        // Source evaporated while airborne (evicted/reaped): the commit's
        // rows are durably in Delta and nothing remains to drain — count as
        // drained, same as the bucket-gone case below.
        let Some(table) = self.get_table(&b.project_id, &b.table_name) else {
            return true;
        };
        let mut emptied = false;
        let mut flushed_through: Option<i64> = None;
        if let Some(bucket_ref) = table.buckets.get(&b.bucket_id) {
            let bucket = bucket_ref.value();
            let mut g = bucket.batches.lock();
            if bucket.mutation_gen.load(Ordering::Relaxed) != b.snapshot_gen {
                // Dirty: re-pin and re-flush next cycle.
                for (shard, pos) in b.wal_first_positions.iter().enumerate() {
                    bucket.record_wal_append(shard, *pos);
                }
                bucket.flush_pinned_prefix.store(0, Ordering::Relaxed);
                // No further drain-progress escalation needed for sustained
                // DML churn: backpressure relief force-flushes via take-based
                // snapshots, which a racing DML cannot dirty.
                info!("finish_flushed_snapshot: bucket {}.{}/{} mutated mid-flight — keeping rows for re-flush", b.project_id, b.table_name, b.bucket_id);
                return false;
            }
            let n = b.batches.len().min(g.len());
            let (freed, rows): (usize, usize) =
                g.drain(..n).map(|batch| (estimate_batch_size(&batch), batch.num_rows())).fold((0, 0), |a, x| (a.0 + x.0, a.1 + x.1));
            emptied = g.is_empty();
            // Recorded whether or not anything survived: a bucket drained CLEAN
            // still takes later inserts, and the branch below never runs for it.
            if let Some(hi) = b.batches.iter().filter_map(batch_timestamp_range).map(|(_, hi)| hi).max() {
                flushed_through = Some(hi);
            }
            if !emptied {
                // Narrow the surviving bucket's time range to the remaining
                // (late-arrival) rows: the old span still covered the DRAINED
                // rows, and the whole-range exclusion would mask their
                // freshly committed Delta copies for up to a flush cycle.
                // Narrowing (instead of a force_flushed exemption) keeps the
                // exclusion armed for a later DML + airborne-commit race on
                // this bucket, which relies on the mask.
                let (min, max) = g.iter().filter_map(batch_timestamp_range).fold((i64::MAX, i64::MIN), |a, r| (a.0.min(r.0), a.1.max(r.1)));
                // ...but narrowing ALONE cannot save a drained row that shares an
                // instant with a survivor, because the mask is a time RANGE while
                // the rows differ only by key. A survivor at exactly the drained
                // row's timestamp still covers it, so the committed Delta copy
                // stays hidden while memory no longer holds it — the row is
                // invisible until the survivor itself flushes.
                //
                // This is the same class the `row_min_ts` field doc records
                // ("masks Delta rows that share a timestamp with buffered ones —
                // it dropped 15 of 20 rows across a dirty restart"), reached
                // through the narrowing path instead. So the mask must begin
                // strictly AFTER everything this drain committed.
                //
                // What that gives up, stated plainly: survivors at or below the
                // drained max stop being masked. They are in memory and NOT in
                // Delta — the snapshot did not include them — so the only exposure
                // is the later DML + airborne-commit race the mask also defends,
                // and on a keyed table read-side dedup collapses that anyway.
                // Trading a rare double-count for a certain disappearance is the
                // right side of this: a masked row is WRONG, a duplicated one is
                // deduped.
                let drained_max = b.batches.iter().filter_map(batch_timestamp_range).map(|(_, hi)| hi).max();
                let min = drained_max.map_or(min, |hi| min.max(hi.saturating_add(1)));
                bucket.min_timestamp.store(min, Ordering::Relaxed);
                bucket.max_timestamp.store(max, Ordering::Relaxed);
            }
            bucket.flush_pinned_prefix.store(0, Ordering::Relaxed);
            drop(g);
            // Mirror the CLAMPED amount onto the MemBuffer total — passing the
            // raw `freed` would over-subtract whenever the bucket held less.
            let applied = sub_saturating(&bucket.memory_bytes, freed);
            bucket.row_count.fetch_sub(rows.min(bucket.row_count.load(Ordering::Relaxed)), Ordering::Relaxed);
            sub_saturating(&self.estimated_bytes, applied);
        }
        if let Some(hi) = flushed_through {
            self.note_flushed_through(key.clone(), b.bucket_id, hi);
        }
        self.cache_invalidate(&Self::cache_key(&b.project_id, &b.table_name, b.bucket_id));
        // remove_if re-checks under the shard lock (bucket_ref dropped above,
        // so no self-deadlock); a racing insert that repopulated the bucket
        // keeps it AND its force_flushed marker — clearing the marker on a
        // survived bucket would re-mask its force-flushed Delta rows.
        // Removing the bucket drops its `memory_bytes` atomic, so any residual
        // it still carried leaves the authoritative sum — discount the total by
        // the same amount or the cache drifts high (see `estimated_bytes`).
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
        for (shard, pos) in b.wal_first_positions.iter().enumerate() {
            bucket.record_wal_append(shard, *pos);
        }
        bucket.flush_pinned_prefix.store(0, Ordering::Relaxed);
        true
    }

    /// Remove empty bucket shells whose pinned WAL entries have aged past
    /// `arrival_cutoff_micros`, releasing their cursor holds so the topic's
    /// WAL can advance and GC. A DML that empties a sealed bucket leaves
    /// such a shell: it can never flush (snapshot/take return None on
    /// empty), so without this sweep its holds pin the watermark for the
    /// process lifetime — unbounded WAL growth for DML-heavy topics.
    ///
    /// SOUNDNESS (load-bearing since replay lost its age cutoff, 2026-07-08):
    /// releasing a shell's holds lets the watermark consume its entries, so
    /// this is only exact because an empty shell's pinned entries are always
    /// an insert set plus the DML(s) that emptied it — skipping ALL of them
    /// on a later replay nets to the same zero rows, and the DML's Delta leg
    /// completed before its ack. Any future path that leaves holds on an
    /// empty bucket whose entries do NOT net to zero must not be reaped
    /// here. The age gate (default: retention) is only a grace period so an
    /// airborne DML pair isn't split; shells with an airborne snapshot are
    /// skipped.
    pub fn reap_expired_empty_buckets(&self, arrival_cutoff_micros: i64) -> usize {
        let releasable = |b: &TimeBucket| b.batches.lock().is_empty() && b.last_wal_pin_micros.load(Ordering::Relaxed) < arrival_cutoff_micros;
        let mut reaped = 0usize;
        for table in self.tables.iter() {
            let expired: Vec<i64> =
                table.buckets.iter().filter(|b| releasable(b) && b.flush_pinned_prefix.load(Ordering::Relaxed) == 0).map(|b| *b.key()).collect();
            for id in expired {
                // Re-check emptiness under the shard lock — a concurrent
                // insert that repopulated the shell keeps it.
                if let Some((_, shell)) = table.buckets.remove_if(&id, |_, b| releasable(b)) {
                    // Residual bytes on a dropped shell must leave the total too.
                    sub_saturating(&self.estimated_bytes, shell.memory_bytes.load(Ordering::Relaxed));
                    self.cache_invalidate(&Self::cache_key(&table.project_id, &table.table_name, id));
                    reaped += 1;
                }
            }
        }
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

    /// Atomically take a bucket's rows + WAL holds for a flush. The take
    /// happens under the same `batches` lock inserts use, so no row can be
    /// lost between snapshot and removal — safe on sealed AND current
    /// still-written buckets. The now-empty bucket stays in the map so
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
        // Lock wal_shard_state too so the taken holds match the taken rows
        // exactly: appends after the take record fresh (later) holds, so the
        // taken holds cover exactly the taken rows' WAL entries.
        let mut wal_g = bucket.wal_shard_state.lock();
        let batches: Vec<RecordBatch> = std::mem::take(&mut *batches_g);
        let wal_state = std::mem::take(&mut *wal_g);
        bucket.flush_pinned_prefix.store(0, Ordering::Relaxed);
        let freed = bucket.memory_bytes.swap(0, Ordering::Relaxed);
        let row_count = bucket.row_count.swap(0, Ordering::Relaxed);
        // Capture the real range as we reset the sentinels so a restore (on
        // Delta commit failure) can replay it instead of guessing bucket-start.
        let min_timestamp = bucket.min_timestamp.swap(i64::MAX, Ordering::Relaxed);
        let max_timestamp = bucket.max_timestamp.swap(i64::MIN, Ordering::Relaxed);
        // The bucket stays in the map and keeps taking inserts, so without this
        // the next row at an already-flushed instant rebuilds the mask over the
        // rows just taken. Set at TAKE, not at commit: if the commit fails the
        // rows are restored and served by the mem leg, and an unfloored Delta
        // leg simply contributes nothing there — the safe direction.
        let flushed_through = bucket.row_max_ts.load(Ordering::Relaxed).max(max_timestamp);
        // Reset with them: a reused bucket that kept a drained tenant's row span would
        // survive pruning it should skip (wasteful, not wrong) and, worse, report a span
        // for rows it no longer holds.
        bucket.row_min_ts.store(i64::MAX, Ordering::Relaxed);
        bucket.row_max_ts.store(i64::MIN, Ordering::Relaxed);
        // Park the GC-floor pin in `taking_pins` BEFORE clearing it from the
        // bucket, so a concurrent GC sweep between this take and the flush
        // path's `register_inflight_pin` still sees the floor (see
        // `taking_pins`). Released via `release_taking_pin`.
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

        // Drop the now-empty bucket so stale empty shells don't accumulate.
        // `remove_if` re-checks emptiness under the shard write lock, so a concurrent insert that
        // repopulated the bucket between the take and here is preserved.
        // Discount any residual the dropped shell still carried, same as the
        // prefix-drain removal — the atomic goes away with the bucket.
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
    /// Delta commit then failed. Restores rows (query visibility) and merges
    /// the WAL holds back so the cursor stays pinned behind them.
    /// Durability never depended on this — the rows are still in the WAL — but
    /// it avoids a query-visibility gap until the next restart/replay.
    ///
    /// Returns false when the rows could NOT be restored (e.g. the table was
    /// recreated with an incompatible schema while the bucket was airborne).
    /// The caller must then keep its in-flight cursor holds registered so the
    /// watermark can't pass the un-restored entries — they stay replayable.
    #[must_use]
    pub fn restore_taken_bucket(&self, b: &FlushableBucket) -> bool {
        // Recreate the table if it was reaped while the bucket was airborne —
        // a silent no-op here would drop the rows from memory AND their
        // cursor holds, letting the watermark pass acked entries.
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
        // Replay the true range (monotonic widen) so restored rows stay visible
        // to time-range pruning; concurrent inserts into the same open bucket
        // are preserved since fetch_min/max only widens.
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
    /// i.e. persistence debt: buffered long ago and still not flushed.
    ///
    /// Dwell (`created_micros`), never the rows' `max_timestamp`. Event time
    /// says nothing about whether flush is keeping up: a merge-on-read UPDATE
    /// appends the row's ORIGINAL timestamp, so a bucket created seconds ago
    /// can hold hours-old event time. Under monoscope's continuous enrichment
    /// there is essentially always such a bucket, and reading it as debt made
    /// `light_optimize_brake` hard-STOP forever — prod 2026-08-07 ran 25
    /// minutes with `light_optimize_bins_committed_total = 0`, 12 flush-debt
    /// yields and `stale_buckets=1`, while flush was healthy (0 failures,
    /// oldest dwell 12 min). Same bug class the 2026-06-29 dwell fix caught in
    /// `oldest_bucket_age`; this caller was missed.
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

            for bucket_id in bucket_ids_to_remove {
                if let Some((_, bucket)) = table.buckets.remove(&bucket_id) {
                    freed_bytes += bucket.memory_bytes.load(Ordering::Relaxed);
                    evicted_count += 1;
                    // Free the bucket's text-index cache entry alongside its
                    // batches — same reasoning as in the flush drain.
                    self.cache_invalidate(&Self::cache_key(&table.project_id, &table.table_name, bucket_id));
                }
            }
            // The flushed watermarks outlive their buckets on purpose, but only
            // until the window itself falls out of retention — past the cutoff
            // no query can ask about that instant, so the floor has no reader.
            if let Some(mut w) = self.flushed_max.get_mut(table_entry.key()) {
                w.retain(|bucket_id, _| *bucket_id >= cutoff_bucket_id);
            }
            self.flushed_max.remove_if(table_entry.key(), |_, w| w.is_empty());
            if table.buckets.is_empty() {
                empty_table_keys.push(table_entry.key().clone());
            }
        }

        // Drop empty TableBuffer entries so per-table metadata (schema Arc,
        // project/table name Arcs, DashMap shards) is reclaimed at scale.
        // `get_or_create_table` recreates a fresh entry on the next write.
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

    /// Check if a table exists in the buffer
    pub fn has_table(&self, project_id: &str, table_name: &str) -> bool {
        self.tables.contains_key(&table_key(project_id, table_name))
    }

    /// Delete rows matching the predicate from the buffer.
    /// Returns the number of rows deleted.
    /// `wal_hold` = the DELETE WAL entry's (shard, pre-append position),
    /// pinned onto every bucket this call mutates (see `note_dml_mutation`).
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
                        Some(phys_pred) => {
                            let mask = phys_pred.evaluate(&batch)?.into_array(rows)?;
                            let mask = mask
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .ok_or_else(|| datafusion::error::DataFusionError::Execution("Predicate did not return boolean".into()))?;
                            filter_record_batch(&batch, &arrow::compute::not(mask)?)?
                        }
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
            // Mirror the CLAMPED bucket subtraction onto the MemBuffer total.
            Ok((deleted + rows_removed as u64, freed + sub_saturating(&bucket.memory_bytes, bucket_freed)))
        })?;

        sub_saturating(&self.estimated_bytes, total_freed);

        debug!("MemBuffer delete: project={}, table={}, rows_deleted={}", project_id, table_name, total_deleted);
        Ok(total_deleted)
    }

    /// Update rows matching the predicate with new values.
    /// Returns the number of rows updated.
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
            let updated_before = total_updated;

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
                let col_idx = target_schema.index_of(col).map_err(|_| datafusion::error::DataFusionError::Execution(format!("Column '{}' not found", col)))?;
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
                if raw.data_type() == &target_ty { Ok(raw.clone()) } else { arrow::compute::cast(raw.as_ref(), &target_ty).map_err(arrow_err) }
            })
            .collect::<DFResult<Vec<_>>>()?;
        let sort_fields: Vec<SortField> = src_key_cols.iter().map(|c| SortField::new(c.data_type().clone())).collect();
        let row_converter = RowConverter::new(sort_fields).map_err(arrow_err)?;
        let src_rows = row_converter.convert_columns(&src_key_cols).map_err(arrow_err)?;
        // Borrowed keys + ahash, as in `dedup_batches`: `src_rows` outlives this
        // map, so owning each key only bought a heap allocation per source row
        // on the enrichment UPDATE path.
        let mut src_lookup: HashMap<arrow::row::Row<'_>, u32, ahash::RandomState> =
            HashMap::with_capacity_and_hasher(source.batch.num_rows(), ahash::RandomState::new());
        for (i, row) in src_rows.iter().enumerate() {
            // First-wins on duplicate source keys (PG leaves multi-match
            // semantics undefined; deterministic first-row-wins is our pick).
            src_lookup.entry(row).or_insert(i as u32);
        }

        let mut total_updated = 0u64;
        let mut total_delta: i64 = 0;
        // Bucket-level time pruning, same as the query path: at prod scale the
        // buffer holds GBs across many buckets, and probing every row of every
        // batch per UPDATE (~2k/h) is enough transient allocation to balloon
        // RSS via allocator retention. The predicate arrives as one nested AND
        // — split it, since extract_timestamp_range only reads top-level
        // conjuncts.
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

                let src_idxs: UInt32Array = (0..num_rows).map(|i| src_lookup.get(&tgt_rows.row(i)).copied()).collect();

                // The common case is zero joined rows in this batch (source is a
                // handful of ids vs the whole buffer): skip the widened-batch
                // materialization entirely — the predicate can only narrow the
                // join match, never widen it.
                if src_idxs.null_count() == num_rows {
                    new_batches.push(batch);
                    continue;
                }

                // Matched-rows-only path. The old code widened the WHOLE batch,
                // evaluated the predicate + every assignment over all `num_rows`,
                // and `merge_arrays`/`zip`ped each assignment column over the full
                // column — and `zip` on Utf8View/BinaryView (Variant/string)
                // columns calls `to_data`, materializing the entire column.
                // Matches are sparse (a few source ids vs a whole bucket), so
                // that was ~all wasted work and the source of the 89GB OOM
                // (confirmed by heap+CPU profile 2026-07-04: update_with_source →
                // arrow zip / make_array / GenericByteViewArray::to_data).
                //
                // Instead: widen + evaluate ONLY the matched candidate rows, and
                // preserve the rest with a single `filter` (which shares
                // byte-view data buffers — no `to_data` materialization). Output
                // is [preserved-rows, updated-rows]; row order within the bucket
                // is not semantically meaningful (queries sort; dedup keys on
                // values), so splitting is safe.
                let has_match = BooleanArray::from((0..num_rows).map(|i| !src_idxs.is_null(i)).collect::<Vec<_>>());

                // Candidate rows (source matched) — widen only these.
                let cand_batch = filter_record_batch(&batch, &has_match).map_err(arrow_err)?;
                let cand_src_idxs = arrow::compute::filter(&src_idxs, &has_match).map_err(arrow_err)?;
                let cand_src_idxs = cand_src_idxs.as_any().downcast_ref::<UInt32Array>().expect("filter preserves UInt32 type");
                let cand_n = cand_batch.num_rows();

                let mut widened_cols: Vec<ArrayRef> = cand_batch.columns().to_vec();
                for i in 0..source.schema.fields().len() {
                    let taken = arrow::compute::take(source.batch.column(i).as_ref(), cand_src_idxs, None).map_err(arrow_err)?;
                    widened_cols.push(taken);
                }
                let cand_widened = RecordBatch::try_new(widened_schema.clone(), widened_cols).map_err(arrow_err)?;

                // Predicate over candidates (all already have a source match).
                let cand_pred = if let Some(ref phys_pred) = physical_predicate {
                    let arr = phys_pred.evaluate(&cand_widened)?.into_array(cand_n)?;
                    arr.as_any()
                        .downcast_ref::<BooleanArray>()
                        .cloned()
                        .ok_or_else(|| datafusion::error::DataFusionError::Execution("Predicate did not return boolean".into()))?
                } else {
                    BooleanArray::from(vec![true; cand_n])
                };

                let matching_count = cand_pred.iter().filter(|v| v == &Some(true)).count();
                if matching_count == 0 {
                    new_batches.push(batch);
                    continue;
                }
                total_updated += matching_count as u64;
                let old_size = estimate_batch_size(&batch);

                // Preserved = rows NOT updated. Map the candidate-space predicate
                // back to full-batch positions (candidates are the has_match rows
                // in order), then one filter over the original batch.
                // The candidate cursor only advances on has_match rows —
                // candidates are exactly those, in order (`&&` short-circuits).
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

                // Updated rows: matched candidates that passed the predicate.
                // Target columns are the first N of the widened batch, so
                // non-assignment columns come straight from it (matched subset).
                let upd_widened = filter_record_batch(&cand_widened, &cand_pred).map_err(arrow_err)?;
                let upd_n = upd_widened.num_rows();
                let new_columns: Vec<ArrayRef> = (0..target_schema.fields().len())
                    .map(|col_idx| {
                        if let Some((_, phys_expr)) = physical_assignments.iter().find(|(idx, _)| *idx == col_idx) {
                            let evaluated = phys_expr.evaluate(&upd_widened)?.into_array(upd_n)?;
                            // The RHS may evaluate to a related-but-distinct type
                            // (e.g. `array_concat(..., [Utf8View])` → List<Utf8View>
                            // while target `hashes` is List<Utf8>; or Utf8 literal →
                            // Utf8View column). Cast to the target column type so the
                            // rebuilt batch matches the buffer schema — otherwise
                            // RecordBatch::try_new rejects it and the DML quarantines.
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

    /// Delete rows using a SQL predicate string (for WAL recovery).
    /// Parses the SQL WHERE clause and delegates to delete().
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

    /// WAL replay path for `UPDATE ... FROM`. Reconstructs an
    /// [`crate::dml::UpdateSource`] from the WAL-stored join keys + source
    /// `RecordBatch`, parses the SQL predicate/assignments against the
    /// widened schema (target + `source__`-prefixed source columns), then
    /// delegates to [`Self::update_with_source`].
    #[instrument(skip(self, assignments, source_batch, registry), fields(project_id, table_name, source_rows = source_batch.num_rows()))]
    #[allow(clippy::too_many_arguments)]
    pub fn update_with_source_by_sql(
        &self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, assignments: &[(String, String)], join_keys: &[(String, String)],
        source_batch: RecordBatch, registry: Option<&FnRegistry>, wal_hold: Option<(usize, walrus_rust::WalPosition)>,
    ) -> DFResult<u64> {
        if self.replay_dml_noop(project_id, table_name, "UPDATE...FROM") {
            return Ok(0);
        }
        let target_df_schema = self.df_schema_for(project_id, table_name)?;

        // Build the widened DFSchema the assignment SQL was originally parsed
        // against (target fields + source fields renamed with `source__`
        // prefix). Same shape as `update_with_source` constructs internally.
        let widened_schema = widen_schema_with_source(target_df_schema.fields(), source_batch.schema().fields());
        let widened_df_schema = DFSchema::try_from(widened_schema.as_ref().clone())?;

        let predicate = predicate_sql.map(|s| parse_sql_predicate(s, &widened_df_schema, registry)).transpose()?;
        let parsed_assignments: Vec<(String, Expr)> = assignments
            .iter()
            .map(|(col, val_sql)| parse_sql_predicate(val_sql, &widened_df_schema, registry).map(|expr| (col.clone(), expr)))
            .collect::<DFResult<Vec<_>>>()?;

        let source = crate::dml::UpdateSource { schema: source_batch.schema(), batch: source_batch, join_keys: join_keys.to_vec() };
        self.update_with_source(project_id, table_name, predicate.as_ref(), &parsed_assignments, &source, wal_hold)
    }

    /// Update rows using SQL strings (for WAL recovery).
    /// Parses the SQL WHERE clause and assignment expressions, then delegates to update().
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
        let parsed_assignments: Vec<(String, Expr)> = assignments
            .iter()
            .map(|(col, val_sql)| parse_sql_predicate(val_sql, &df_schema, registry).map(|expr| (col.clone(), expr)))
            .collect::<DFResult<Vec<_>>>()?;
        self.update(project_id, table_name, predicate.as_ref(), &parsed_assignments, wal_hold)
    }

    /// Replay-path guard: DML for a table with no buffered rows is a no-op —
    /// its rows were already flushed (or drained mid-replay by the budget
    /// relief), so the buffer has nothing to mutate. Parsing would otherwise
    /// hit `DFSchema::empty()` and quarantine the entry with a bogus schema
    /// error (2026-07-08: "No field named context___span_id"). The warn is the
    /// operator signal that un-flushed enrichment for this window, if any,
    /// needs a monoscope re-drive — the Delta leg of the original DML covered
    /// only rows below the flush watermark at the time it ran.
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
        let (mut total_buckets, mut total_rows, mut total_batches) = (0, 0, 0);
        let mut estimated_bytes = 0usize;
        let mut project_ids = std::collections::HashSet::new();
        let mut oldest: Option<i64> = None;
        // Only buckets the flush path should already have drained count toward
        // the dwell signal — non-empty AND past the open window.
        let current = Self::current_bucket_id();

        for table_entry in self.tables.iter() {
            let (project_id, _) = table_entry.key();
            project_ids.insert(project_id.clone());

            let table = table_entry.value();
            total_buckets += table.buckets.len();
            for bucket in table.buckets.iter() {
                total_rows += bucket.row_count.load(Ordering::Relaxed);
                estimated_bytes += bucket.memory_bytes.load(Ordering::Relaxed);
                let batch_count = bucket.batches.lock().len();
                total_batches += batch_count;
                if batch_count > 0 && *bucket.key() < current {
                    let created = bucket.created_micros;
                    oldest = Some(oldest.map_or(created, |o| o.min(created)));
                }
            }
        }
        MemBufferStats {
            project_count: project_ids.len(),
            total_buckets,
            total_rows,
            total_batches,
            replay_dml_noops: self.replay_dml_noops.load(Ordering::Relaxed),
            // Summed from the bucket atomics in the sweep above (free — the
            // loop already visits every bucket), so `timefusion_stats` and the
            // `mem_buffer.estimated_bytes` gauge report the authoritative value
            // even if the hot-path cache has drifted since the last reconcile.
            estimated_memory_bytes: estimated_bytes,
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
        // Resolve the declared schema once per table (`schema_ref()` builds
        // ~100 fields, far too costly per batch) and pin its nullability onto
        // the advertised schema, so readers see the declared truth rather than
        // whatever metadata the first batch to arrive happened to carry.
        let declared = crate::schema::get_schema(&table_name).map(|s| s.schema_ref());
        // The caller canonicalizes full registered batches before construction.
        // A deliberately narrow internal batch cannot advertise columns it
        // does not carry, so retain its shape and align only matching fields.
        let schema = declared.as_ref().and_then(|d| align_nullability(&schema, d, None)).unwrap_or(schema);
        Self { buckets: DashMap::new(), schema, declared, project_id, table_name }
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
    /// Returns `(net_memory_delta_bytes, bucket_id)` — the **signed** change
    /// this call made to `bucket.memory_bytes`: `+new_size` for the pushed
    /// batch, plus `(combined_size - folded_size)` if a coalesce fired. The
    /// caller applies it verbatim to `MemBuffer::estimated_bytes`; returning
    /// only the insert size is what leaked the coalesce savings and let the
    /// MemBuffer-level total drift (see `MemBuffer::estimated_bytes`).
    pub fn insert_batch(&self, batch: RecordBatch, timestamp_micros: i64, wal_hold: Option<(usize, walrus_rust::WalPosition)>) -> anyhow::Result<(i64, i64)> {
        // Reconcile against the declared schema BEFORE the batch is stored:
        // every downstream consumer (coalesce, flush, hot-tier demotion, query)
        // then sees one authoritative nullability. See `align_nullability`.
        let batch = canonicalize_declared_batch(batch, self.declared.as_ref())?;
        let batch = align_batch_nullability(compact_batch(batch), self.declared.as_ref());
        let bucket_id = MemBuffer::compute_bucket_id(timestamp_micros);
        let row_count = batch.num_rows();
        let new_size = estimate_batch_size(&batch);
        // Taken before the batch is moved into the bucket; see `row_min_ts`.
        let ts_bounds = batch_timestamp_range(&batch);

        let bucket = self.buckets.entry(bucket_id).or_insert_with(TimeBucket::new);

        // Yields the coalesce's own change to `bucket.memory_bytes` (0 when it
        // doesn't fire), which rides out in the returned net delta so the
        // caller can mirror this call exactly onto the MemBuffer total.
        let coalesce_delta: i64 = {
            let mut g = bucket.batches.lock();
            // Record the WAL cursor hold under the SAME lock as the batch
            // push: `take_bucket_for_flush` snapshots batches + holds under
            // this lock too, so a concurrent take can never grab the rows
            // without their hold (a two-step insert-then-record left a window
            // where the take got hold-less rows and a failed commit's restore
            // left the entry unpinned — acked-write loss on crash).
            if let Some((shard, pos)) = wal_hold {
                bucket.record_wal_append(shard, Some(pos));
            }
            // WAL GC floor (see `first_wal_pin_micros`): the append just
            // happened, so `now` IS the append time. The live path (hold
            // present) was already stamped by record_wal_append above; only
            // replay inserts (no wal_hold) need it here, and they are
            // additionally floored at the entry's original append time in
            // `record_replay_hold`.
            if wal_hold.is_none() {
                bucket.first_wal_pin_micros.fetch_min(chrono::Utc::now().timestamp_micros(), Ordering::Relaxed);
            }
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
            //
            // The `g.len()` gate also keeps the tail-run scan (one
            // `estimate_batch_size` per trailing batch) off every insert.
            //
            // Never fold across an airborne flush snapshot's prefix — see
            // `flush_pinned_prefix` docs. Clamped: a DML can drop emptied
            // batches while a snapshot is airborne, leaving `pinned > len`.
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
                        // Read-modify-write via a signed delta, not load+store:
                        // other paths (prefix drain) mutate this atomic without
                        // holding `batches`, so a store would clobber their
                        // subtraction. The same delta rides out to the caller so
                        // the MemBuffer total tracks the shrinkage.
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
        // Read pruning needs the span the rows actually occupy: routing widens by one
        // point, so a batch spanning 21:00-23:00 advertised 21:00 and `>= 21:30` skipped
        // the bucket entirely while the rows sat in memory.
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
        // GC-floor stamp for DML pins. Replay pins go through here with "now"
        // too, but their buckets were already floored at the original append
        // time by `insert_batch`, so fetch_min can only keep the older value.
        self.first_wal_pin_micros.fetch_min(chrono::Utc::now().timestamp_micros(), Ordering::Relaxed);
        if let Some(pos) = pre_position {
            self.wal_shard_state.lock().merge(shard, pos);
        }
    }

    fn snapshot_wal_shard_state(&self, shards_per_topic: usize) -> Vec<Option<walrus_rust::WalPosition>> {
        pad_positions(&self.wal_shard_state.lock().first_positions, shards_per_topic)
    }

    fn update_timestamps(&self, timestamp: i64) {
        self.min_timestamp.fetch_min(timestamp, Ordering::Relaxed);
        self.max_timestamp.fetch_max(timestamp, Ordering::Relaxed);
    }

    /// Atomic snapshot of this bucket's batches + row count. Both come
    /// from the same lock acquisition so they're guaranteed consistent.
    fn snapshot(&self) -> (Vec<RecordBatch>, usize) {
        let snap = self.batches.lock().to_vec();
        let n = snap.iter().map(|b| b.num_rows()).sum();
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
        &self, bucket: &TimeBucket, cache_key: &BucketCacheKey, table_schema: &crate::schema::TableSchema, node: &crate::tantivy::udf::PredNode,
    ) -> anyhow::Result<(Vec<RecordBatch>, Option<std::collections::HashSet<String>>)> {
        let (snapshot, snapshot_rows) = bucket.snapshot();
        if snapshot.is_empty() {
            return Ok((snapshot, None));
        }

        // Try the cache. Reuse only if its row count matches the snapshot.
        // Index build/search failures must DEGRADE to the unfiltered snapshot
        // (ids=None → the SQL predicate still filters), never error: the scan
        // maps an Err to "no MemBuffer data", silently dropping every acked-
        // but-unflushed row (2026-07-05 review hardening). This also covers
        // hand-written `text_match(unindexed_col, ..)` reaching a bucket
        // index that has no such field.
        let idx = match self.cache_get(cache_key).filter(|i| i.indexed_rows == snapshot_rows) {
            Some(hit) => hit,
            None => {
                let built = match crate::tantivy::BucketTextIndex::build(table_schema, &snapshot, snapshot_rows) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!("mem text index build failed (degrading to unfiltered snapshot): {e}");
                        return Ok((snapshot, None));
                    }
                };
                let Some(built) = built else {
                    return Ok((snapshot, None));
                };
                self.cache_put(cache_key.clone(), Arc::new(built))
            }
        };

        // One combined boolean query. `collect_text_match_tree` only emits
        // OR nodes whose every branch is completely covered, so evaluating
        // the tree in-engine (And→Must, Or→Should) yields a sound superset.
        match idx.search_node(node) {
            Ok(hits) => Ok((snapshot, Some(hits.into_iter().map(|h| h.id).collect()))),
            Err(e) => {
                warn!("mem text index search failed (degrading to unfiltered snapshot): {e}");
                Ok((snapshot, None))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Int64Array, StringArray, StringViewArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
    };

    use super::*;

    #[test]
    fn declared_batch_canonicalization_reorders_casts_and_fills_nullable_fields() {
        // IPC decoders frequently advertise nullable=true even when the array
        // has no nulls; the declared target may truthfully be NOT NULL.
        let incoming_schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true), Field::new("value", DataType::Int64, true)]));
        let incoming = RecordBatch::try_new(incoming_schema, vec![Arc::new(StringArray::from(vec!["x"])), Arc::new(Int64Array::from(vec![7]))]).unwrap();
        let declared = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, false),
            Field::new("optional", DataType::Int64, true),
        ]));

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

    /// The Delta leg excludes the UNION of the tiers' ranges, so merging must
    /// preserve it exactly: adjacent buckets (`end == next start`) collapse,
    /// disjoint ones must NOT — merging a gap would hide the Delta rows in it.
    #[test]
    fn merge_ranges_collapses_contiguous_windows_only() {
        assert_eq!(merge_ranges(vec![(20, 30), (0, 10), (10, 20)]), vec![(0, 30)]);
        assert_eq!(merge_ranges(vec![(0, 10), (11, 20)]), vec![(0, 10), (11, 20)]);
        assert_eq!(merge_ranges(vec![(0, 30), (5, 10)]), vec![(0, 30)]);
        assert!(merge_ranges(vec![]).is_empty());
        assert!(overlaps((0, 10), (9, 20)) && !overlaps((0, 10), (10, 20)));
    }

    /// `insert_versions` exempts exactly the buckets a merge-on-read append
    /// lands in, so the id derivation must cover every row (a missed bucket is
    /// a window whose untouched Delta rows go dark) and must not invent any.
    #[test]
    fn batch_bucket_ids_covers_every_row_and_dedups() {
        let d = bucket_duration_micros();
        let schema = Arc::new(Schema::new(vec![Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false)]));
        let batch = |ts: Vec<i64>| RecordBatch::try_new(schema.clone(), vec![Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC"))]).unwrap();

        // Two rows in one bucket and one in the next → two ids, sorted, once each.
        assert_eq!(batch_bucket_ids(&batch(vec![0, 1, d]), "timestamp"), vec![0, 1]);
        // Rows spanning a gap keep the gap: only buckets that actually receive
        // a row are exempted.
        assert_eq!(batch_bucket_ids(&batch(vec![5 * d, 0]), "timestamp"), vec![0, 5]);
        // An absent time column yields nothing rather than a wrong bucket.
        assert!(batch_bucket_ids(&batch(vec![0]), "no_such_column").is_empty());
    }

    /// Batch shaped like a client-built insert: every field marked nullable
    /// (Arrow takes nullability from how the array was *built*, not from the
    /// data) even though the columns are fully populated.
    fn nullable_otel_batch(with_null_id: bool) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
            Field::new("id", DataType::Utf8View, true),
        ]));
        let ts = TimestampMicrosecondArray::from(vec![1_000i64, 2_000]).with_timezone("UTC");
        let id = StringViewArray::from(vec![Some("a"), if with_null_id { None } else { Some("b") }]);
        RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(id)]).unwrap()
    }

    fn nullable_test_declared_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Utf8View, false),
        ]))
    }

    /// Regression (prod 2026-07-31): MemBuffer pinned whatever nullability the
    /// FIRST batch happened to carry, so a fully-populated `timestamp` arriving
    /// as `nullable=true` made the buffer advertise a schema that disagreed
    /// with the declared NOT NULL one — surfacing downstream as hot-tier read
    /// misses and DataFusion's `physical true vs logical false` aggregate error.
    #[test]
    fn membuffer_takes_nullability_from_declared_schema_not_first_batch() {
        let declared = nullable_test_declared_schema();
        let stored = canonicalize_declared_batch(nullable_otel_batch(false), Some(&declared)).unwrap();

        for col in ["timestamp", "id"] {
            assert!(
                !stored.schema().field_with_name(col).unwrap().is_nullable(),
                "`{col}` is declared NOT NULL and holds no nulls, so it must not stay nullable"
            );
        }
    }

    /// The alignment must never assert an invariant the data violates: a
    /// declared-NOT-NULL column that genuinely contains nulls stays nullable
    /// (surfacing the bad data) rather than being relabelled NOT NULL.
    #[test]
    fn declared_not_null_column_holding_nulls_stays_nullable() {
        let declared = nullable_test_declared_schema();
        let stored = canonicalize_declared_batch(nullable_otel_batch(true), Some(&declared)).unwrap();
        let schema = stored.schema();
        assert!(schema.field_with_name("id").unwrap().is_nullable(), "`id` actually contains a null; relabelling it NOT NULL would lie about the data");
        assert!(!schema.field_with_name("timestamp").unwrap().is_nullable(), "`timestamp` is null-free and must still be tightened");
    }

    /// Alignment is name+exact-type only, so it must leave unknown tables and
    /// mismatched types alone rather than silently re-typing a column.
    #[test]
    fn nullability_alignment_ignores_unknown_tables_and_mismatched_types() {
        let buffer = MemBuffer::new();
        buffer.insert("p1", "not_a_registered_table", nullable_otel_batch(false), 1_000).unwrap();
        assert!(buffer.query("p1", "not_a_registered_table", &[]).unwrap()[0].schema().field_with_name("timestamp").unwrap().is_nullable());

        // `id` is declared Utf8; an Int64 `id` must keep its own metadata.
        let declared = crate::schema::get_schema("otel_logs_and_spans").unwrap().schema_ref();
        let mistyped = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        assert!(align_nullability(&mistyped, &declared, None).is_none());
    }

    /// `sort_partition` is what earns the in-memory legs their ordering claim,
    /// so it must actually order the rows — and hand them back as BATCHES.
    /// A global lexsort has to concatenate the partition once; returning that
    /// monolith would make every demoted IPC file and every scan partition a
    /// single giant value, so the sorted result is sliced back (zero-copy)
    /// into bounded chunks. Order must survive the slicing.
    #[test]
    fn sort_partition_orders_descending_and_chunks_the_result() {
        let schema = crate::schema::get_schema("mor_versioned").expect("fixture registered");
        assert_eq!(schema.sorting_columns[0].name, "timestamp", "this test asserts a `timestamp DESC` ordering");
        assert!(schema.sorting_columns[0].descending);

        // More than one chunk's worth, in scrambled event-time order: an
        // arrival-ordered fixture would come out sorted even if nothing sorted.
        const N: i64 = SORT_CHUNK_ROWS as i64 * 2 + 17;
        let batches: Vec<RecordBatch> = (0..N).map(|i| create_test_batch((i * 7919) % N)).collect();
        let sorted = sort_partition(schema, batches).expect("a partition with the sorting column must sort");

        assert!(sorted.len() > 1, "the result must be chunked, got {} batch(es)", sorted.len());
        assert!(sorted.iter().all(|b| b.num_rows() <= SORT_CHUNK_ROWS), "no chunk may exceed the bound");
        assert_eq!(sorted.iter().map(|b| b.num_rows()).sum::<usize>(), N as usize, "sorting must not lose or duplicate rows");

        // Descending across the WHOLE partition, chunk boundaries included.
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

    /// Read pruning must consult the span the rows actually occupy. Routing widens
    /// `min/max_timestamp` by ONE point, so a batch spanning 21:00-23:00 advertised
    /// 21:00 and any `timestamp >= 21:30` skipped the bucket while its rows sat in
    /// memory, invisible until flush.
    ///
    /// The row span is deliberately a SEPARATE pair: widening `min/max_timestamp`
    /// itself also moves the span `get_bucket_ranges` masks Delta over, which dropped
    /// 15 of 20 rows across a dirty restart (`cold_start_under_five_seconds`). This
    /// asserts both halves — the row span tracks the rows, the routing span does not
    /// move — so a future simplification that merges them fails here.
    #[test]
    fn row_span_tracks_rows_while_routing_span_stays_put() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Int64, false),
        ]));
        let (lo, mid, hi) = (1_735_678_800_000_000i64, 1_735_682_400_000_000, 1_735_686_000_000_000);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMicrosecondArray::from(vec![lo, mid, hi]).with_timezone("UTC")), Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

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
        cols.push(Arc::new(arrow::array::ListArray::new(item, arrow::buffer::OffsetBuffer::from_lengths([1]), view_col(), None)));
        cols.push(Arc::new(arrow::array::StructArray::new(vec![Field::new("v", DataType::Utf8View, true)].into(), vec![view_col()], None)));
        RecordBatch::try_new(schema, cols).unwrap()
    }

    /// 2026-07-08 review finding: the WAL GC floor must be the APPEND time.
    /// Stamping event time meant one old-event-time backfill row dragged the
    /// floor days back and suspended WAL file GC for the backfill's duration.
    #[test]
    fn live_insert_gc_floor_is_append_time_not_event_time() {
        let buffer = MemBuffer::new();
        let ten_days_ago = chrono::Utc::now().timestamp_micros() - 10 * 24 * 3600 * 1_000_000;
        buffer.insert("p1", "t1", create_test_batch(ten_days_ago), ten_days_ago).unwrap();
        let floor = buffer.oldest_wal_append_micros().expect("un-flushed bucket must floor GC");
        let hour = 3600 * 1_000_000;
        assert!(floor > chrono::Utc::now().timestamp_micros() - hour, "backfill event time leaked into the GC floor: {floor} (≈10 days old)");
        // Replay counterpart: record_replay_hold floors at the entry's
        // ORIGINAL append time so the old backing file stays protected.
        buffer.record_replay_hold("p1", "t1", ten_days_ago, 0, walrus_rust::WalPosition::ORIGIN);
        assert_eq!(buffer.oldest_wal_append_micros(), Some(ten_days_ago), "replay pins must keep the original append time as the floor");
    }

    /// 2026-07-08 review finding: `take_bucket_for_flush` removed the bucket
    /// (and its GC-floor pin) before the flush path registered its inflight
    /// pin — a GC sweep sampling the floor in that gap could delete the
    /// airborne bucket's backing WAL file. The pin must stay visible from
    /// take until `release_taking_pin`.
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

    #[test]
    fn single_row_utf8view_insert_charged_logical_size_not_block_capacity() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        buffer.insert("p1", "t1", wide_view_row(ts), ts).unwrap();
        let charged = buffer.estimated_memory_bytes();
        assert!(charged < 64 * 1024, "single ~3KB-logical row charged {charged} bytes — view block capacity is leaking into memory accounting");
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
        assert!(charged < 16 * 1024 * 1024, "200 ~3KB-logical rows charged {charged} bytes — fold outputs are carrying view block capacity");
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
                vec![Arc::new(TimestampMicrosecondArray::from(vec![ts]).with_timezone("UTC")), Arc::new(StringViewArray::from(vec![payload.as_str()]))],
            )
            .unwrap();
            buffer.insert("p1", "t1", batch, ts).unwrap();
        }
        let table = buffer.tables.get(&table_key("p1", "t1")).unwrap();
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
                vec![Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")), Arc::new(Int64Array::from(ids)), Arc::new(StringViewArray::from(pl))],
            )
            .unwrap()
        };
        let batches = vec![
            mk(vec![100, 200], vec![1, 2], vec!["v1-old", "v2-old"]),
            mk(vec![100, 300], vec![1, 3], vec!["v1-new", "v3"]),
            mk(vec![200], vec![2], vec!["v2-new"]),
        ];
        let keys = vec!["id".to_string(), "timestamp".to_string()];
        let out = dedup_batches(batches, &keys, None, None).expect("dedup ok");
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

    /// A NULL tiebreak must sort LOWEST, in either arrival order. This is the
    /// whole migration story for the `updated_at` version stamp: rows written
    /// before the column existed carry NULL and must always lose to any stamped
    /// version, no matter which one the flush happens to see first. (It holds
    /// because `SortField::new` takes `SortOptions::default()` — ascending,
    /// nulls first — so a NULL row encodes below every value.)
    #[test]
    fn dedup_batches_null_tiebreak_always_loses() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
            Field::new("payload", DataType::Utf8View, false),
        ]));
        let mk = |stamp: Option<i64>, payload: &str| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1i64])),
                    Arc::new(TimestampMicrosecondArray::from(vec![stamp]).with_timezone("UTC")),
                    Arc::new(StringViewArray::from(vec![payload])),
                ],
            )
            .unwrap()
        };
        let keys = vec!["id".to_string()];
        for (batches, want) in [(vec![mk(None, "legacy"), mk(Some(10), "stamped")], "stamped"), (vec![mk(Some(10), "stamped"), mk(None, "legacy")], "stamped")]
        {
            let out = dedup_batches(batches, &keys, Some("updated_at"), None).expect("dedup ok");
            let survivors: Vec<String> = out
                .iter()
                .flat_map(|b| {
                    let pl = b.column_by_name("payload").unwrap().as_any().downcast_ref::<StringViewArray>().unwrap();
                    (0..b.num_rows()).map(|i| pl.value(i).to_string()).collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(survivors, vec![want.to_string()], "NULL tiebreak must lose regardless of arrival order");
        }
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

    /// Regression (prod 2026-06-20): a large coalesced otel flush fused every
    /// batch into one RecordBatch inside `dedup_batches`, overflowing Arrow's 2GB
    /// i32 string-offset limit ("Offset overflow error"). That failed the flush,
    /// so the buckets never drained and MemBuffer wedged at the hard limit —
    /// every insert rejected. Dedup must not build a single fused array: with no
    /// duplicate keys across N input batches it returns those N batches untouched.
    #[test]
    fn dedup_batches_does_not_concatenate_full_payload() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("payload", DataType::Utf8View, false)]));
        let mk = |id: i64, p: &str| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![id])), Arc::new(StringViewArray::from(vec![p]))]).unwrap()
        };
        let batches = vec![mk(1, "a"), mk(2, "b"), mk(3, "c")];
        let out = dedup_batches(batches, &["id".to_string()], None, None).expect("dedup ok");
        assert_eq!(out.len(), 3, "distinct-key batches must be returned un-fused (no 2GB-prone concat)");
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    #[test]
    fn dedup_batches_errors_on_unknown_key() {
        let batch = create_test_batch(1);
        let err = dedup_batches(vec![batch], &["nonexistent".to_string()], None, None).unwrap_err();
        assert!(err.to_string().contains("nonexistent"), "msg: {err}");
    }

    /// Rows buffered under the PRE-`version_append` schema carry no tiebreak
    /// column. Failing them made those buckets permanently unflushable — they
    /// pinned MemBuffer at the hard limit while newer buckets drained fine
    /// (prod 2026-08-02). Degrade to last-occurrence-wins instead; unstamped
    /// rows carry no version information, and `DedupExec` ranks a NULL stamp
    /// below any stamped version on the read side.
    #[test]
    fn dedup_batches_tolerates_a_legacy_batch_with_no_tiebreak_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("payload", DataType::Utf8View, false)]));
        let mk = |ids: Vec<i64>, pl: Vec<&str>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids)), Arc::new(StringViewArray::from(pl))]).unwrap()
        };
        let batches = vec![mk(vec![1], vec!["old"]), mk(vec![1], vec!["new"])];
        let out = dedup_batches(batches, &["id".to_string()], Some("updated_at"), None).expect("a legacy batch must flush, not fail the whole commit");
        let got: Vec<String> = out
            .iter()
            .flat_map(|b| {
                let pl = b.column_by_name("payload").unwrap().as_any().downcast_ref::<StringViewArray>().unwrap();
                (0..b.num_rows()).map(|i| pl.value(i).to_string()).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(got, vec!["new".to_string()], "with no tiebreak available the last occurrence must win");

        // A missing dedup KEY stays fatal — that is a schema fault, not legacy data.
        assert!(dedup_batches(vec![mk(vec![1], vec!["x"])], &["nope".to_string()], None, None).is_err(), "a missing dedup key must still fail loudly");
    }

    /// Defect 3: with a tiebreak column, the row with the greatest tiebreak value
    /// wins per key (an enriched re-emit carries a later `observed_timestamp`),
    /// regardless of input position — and a non-null tiebreak beats a null one.
    #[test]
    fn dedup_batches_tiebreak_keeps_greatest() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("observed", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
            Field::new("payload", DataType::Utf8View, false),
        ]));
        let mk = |ids: Vec<i64>, obs: Vec<Option<i64>>, pl: Vec<&str>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(ids)), Arc::new(TimestampMicrosecondArray::from(obs).with_timezone("UTC")), Arc::new(StringViewArray::from(pl))],
            )
            .unwrap()
        };
        // id=1: base (observed=100) appears AFTER enriched (observed=200) in input —
        //   keep-last would wrongly pick base; tiebreak must pick the observed=200 row.
        // id=2: base has a NULL observed, enriched has 50 — non-null must win.
        let batches =
            vec![mk(vec![1, 2], vec![Some(200), None], vec!["1-enriched", "2-base"]), mk(vec![1, 2], vec![Some(100), Some(50)], vec!["1-base", "2-enriched"])];
        let out = dedup_batches(batches, &["id".to_string()], Some("observed"), None).expect("dedup ok");
        let mut got: Vec<(i64, String)> = out
            .iter()
            .flat_map(|b| {
                let ids = b.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
                let pl = b.column_by_name("payload").unwrap().as_any().downcast_ref::<StringViewArray>().unwrap();
                (0..b.num_rows()).map(move |i| (ids.value(i), pl.value(i).to_string())).collect::<Vec<_>>()
            })
            .collect();
        got.sort();
        assert_eq!(got, vec![(1, "1-enriched".into()), (2, "2-enriched".into())]);
    }

    /// Merge-on-read version collapse. Builds `[id, updated_at, deleted, payload]`
    /// rows; the survivor is the greatest `updated_at` per id, and `deleted=true`
    /// on that survivor retires the key when — and only when — the caller declares
    /// its scope safe (`drop_tombstones = Some("deleted")`).
    mod tombstones {
        use super::*;

        fn schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
                Field::new("deleted", DataType::Boolean, true),
                Field::new("payload", DataType::Utf8View, false),
            ]))
        }

        /// One row per batch, so input order is explicit at every call site.
        fn row(id: i64, updated_at: Option<i64>, deleted: Option<bool>, payload: &str) -> RecordBatch {
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from(vec![id])),
                    Arc::new(TimestampMicrosecondArray::from(vec![updated_at]).with_timezone("UTC")),
                    Arc::new(BooleanArray::from(vec![deleted])),
                    Arc::new(StringViewArray::from(vec![payload])),
                ],
            )
            .unwrap()
        }

        fn collapse(batches: Vec<RecordBatch>, drop_tombstones: Option<&str>) -> Vec<(i64, String, Option<bool>)> {
            let out = dedup_batches(batches, &["id".to_string()], Some("updated_at"), drop_tombstones).expect("collapse ok");
            let mut got: Vec<(i64, String, Option<bool>)> = out
                .iter()
                .flat_map(|b| {
                    let ids = b.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
                    let del = b.column_by_name("deleted").unwrap().as_any().downcast_ref::<BooleanArray>().unwrap();
                    let pl = b.column_by_name("payload").unwrap().as_any().downcast_ref::<StringViewArray>().unwrap();
                    (0..b.num_rows()).map(move |i| (ids.value(i), pl.value(i).to_string(), del.is_valid(i).then(|| del.value(i)))).collect::<Vec<_>>()
                })
                .collect();
            got.sort();
            got
        }

        /// A tombstone is just another version: it wins on `updated_at`, not on
        /// being a tombstone. The live row is gone either way — what differs is
        /// whether the tombstone itself survives to keep it gone.
        #[test]
        fn tombstone_beats_older_live_version() {
            let batches = vec![row(1, Some(10), None, "live"), row(1, Some(20), Some(true), "deleted")];
            assert_eq!(collapse(batches, None), vec![(1, "deleted".into(), Some(true))]);
        }

        /// Out-of-order flush: the older live version arrives AFTER the tombstone.
        /// `updated_at`, not position, decides — so the row is not resurrected.
        #[test]
        fn late_older_live_version_does_not_resurrect() {
            let batches = vec![row(1, Some(20), Some(true), "deleted"), row(1, Some(10), None, "live")];
            assert_eq!(collapse(batches.clone(), None), vec![(1, "deleted".into(), Some(true))]);
            assert_eq!(collapse(batches, Some("deleted")), vec![], "tombstone drop must take the whole key, not just the tombstone row");
        }

        /// The two policies, same input: retained (the sweep's choice — an older
        /// version may live in a file/partition/buffer it never saw) vs dropped
        /// (only when the caller's scope holds every version of the key).
        #[test]
        fn tombstone_retained_unless_caller_declares_scope_safe() {
            let batches = vec![row(1, Some(10), Some(false), "live"), row(1, Some(20), Some(true), "deleted"), row(2, Some(5), None, "untouched")];
            assert_eq!(collapse(batches.clone(), None), vec![(1, "deleted".into(), Some(true)), (2, "untouched".into(), None)]);
            assert_eq!(collapse(batches, Some("deleted")), vec![(2, "untouched".into(), None)]);
        }

        /// A tombstone with no older version in the input is still a row to drop —
        /// it must not slip through the no-duplicates fast path.
        #[test]
        fn lone_tombstone_drops_without_a_duplicate() {
            assert_eq!(collapse(vec![row(1, Some(20), Some(true), "deleted")], Some("deleted")), vec![]);
        }

        /// A newer LIVE version un-deletes the key (re-insert after delete), and
        /// NULL/false are both live. The all-NULL steady state of a table that
        /// declares `tombstone_column` but has never been deleted from is a no-op:
        /// same rows, and the untouched-batches fast path is preserved.
        #[test]
        fn null_and_false_are_live() {
            let undelete = vec![row(1, Some(20), Some(true), "deleted"), row(1, Some(30), Some(false), "reinserted")];
            assert_eq!(collapse(undelete, Some("deleted")), vec![(1, "reinserted".into(), Some(false))]);

            let all_null = vec![row(1, Some(10), None, "a"), row(2, Some(20), None, "b")];
            let out = dedup_batches(all_null.clone(), &["id".to_string()], Some("updated_at"), Some("deleted")).expect("collapse ok");
            assert_eq!(out.len(), 2, "no tombstones present → batches returned untouched, not re-filtered");
            assert_eq!(collapse(all_null.clone(), Some("deleted")), collapse(all_null, None));
        }

        /// A table that declares no tombstone column is byte-identical to before:
        /// the tombstone argument is the only thing that can change the row set,
        /// even when a Boolean column happens to be named `deleted` and set.
        #[test]
        fn no_tombstone_column_means_unchanged_behaviour() {
            let batches = vec![row(1, Some(10), Some(true), "a"), row(2, Some(20), Some(true), "b")];
            let out = dedup_batches(batches, &["id".to_string()], Some("updated_at"), None).expect("collapse ok");
            assert_eq!(out.len(), 2, "distinct keys, no drop → inputs returned un-fused");
            assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        }

        /// A non-Boolean tombstone column is a schema error, not a silent
        /// mis-drop. (`validate()` rejects it at load; this pins the runtime.)
        #[test]
        fn non_boolean_tombstone_column_errors() {
            let err = dedup_batches(vec![row(1, Some(10), None, "a")], &["id".to_string()], Some("updated_at"), Some("payload")).unwrap_err();
            assert!(err.to_string().contains("must be Boolean"), "msg: {err}");
        }
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
        use crate::support::test_helpers::{json_to_batch, test_span};
        let buffer = MemBuffer::new();
        let r1 = test_span("row-1", "auth-svc", "p1");
        let r2 = test_span("row-2", "billing-svc", "p1");
        let batch = json_to_batch(vec![r1, r2]).expect("json_to_batch");
        let ts = chrono::Utc::now().timestamp_micros();
        buffer.insert("p1", "otel_logs_and_spans", batch, ts).unwrap();

        let preds = vec![crate::tantivy::udf::TextMatchPred { column: "name".into(), query: "auth".into() }];
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

        let preds = vec![crate::tantivy::udf::TextMatchPred { column: "name".into(), query: "test".into() }];
        let got = buffer.search_text_match("p1", "table1", &preds).expect("search");
        assert!(got.is_none(), "unindexed table should return None, got {:?}", got);
    }

    #[test]
    fn search_text_match_cache_invalidates_on_insert() {
        // Build cache via first query, insert new rows, second query must
        // see them (i.e. cache was invalidated and rebuilt).
        use crate::support::test_helpers::{json_to_batch, test_span};
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let batch1 = json_to_batch(vec![test_span("a", "alpha-svc", "p1")]).unwrap();
        buffer.insert("p1", "otel_logs_and_spans", batch1, ts).unwrap();
        let preds = vec![crate::tantivy::udf::TextMatchPred { column: "name".into(), query: "beta".into() }];
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
        use crate::support::test_helpers::{json_to_batch, test_span};
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        // Build a batch with two rows, one matching the search and one not.
        let batch = json_to_batch(vec![test_span("hit-1", "alpha-search-svc", "p1"), test_span("miss-1", "completely-unrelated-svc", "p1")]).unwrap();
        buffer.insert("p1", "otel_logs_and_spans", batch, ts).unwrap();

        let preds = vec![crate::tantivy::udf::TextMatchPred { column: "name".into(), query: "alpha".into() }];
        let node = crate::tantivy::udf::PredNode::from_preds(&preds);
        let parts = buffer.query_partitioned_with_text_match("p1", "otel_logs_and_spans", &[], node.as_ref()).unwrap();
        let total_rows: usize = parts.partitions.iter().flatten().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "expected only the matching row, got {} rows in {:?}", total_rows, parts);

        // Verify the returned row is the matching one by checking the id col.
        use arrow::array::AsArray;
        let returned = &parts.partitions[0][0];
        let id_arr = returned.column_by_name("id").unwrap().as_string_view();
        assert_eq!(id_arr.value(0), "hit-1");
    }

    #[test]
    fn query_partitioned_with_text_match_empty_preds_falls_through() {
        // No text_match preds → behave identically to query_partitioned.
        use crate::support::test_helpers::{json_to_batch, test_span};
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        let batch = json_to_batch(vec![test_span("a", "svc", "p1"), test_span("b", "svc", "p1")]).unwrap();
        buffer.insert("p1", "otel_logs_and_spans", batch, ts).unwrap();

        let parts = buffer.query_partitioned_with_text_match("p1", "otel_logs_and_spans", &[], None).unwrap();
        let total: usize = parts.partitions.iter().flatten().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "no text_match preds → all rows returned");
    }

    /// Regression: `restore_taken_bucket` (the Delta-commit-failure path of the
    /// open-bucket force-flush) used to reset the bucket's min/max to the bucket
    /// *start* (`bucket_id * duration`), hiding restored rows from time-range
    /// pruning until the next insert. It must replay the rows' real range.
    #[test]
    fn restore_taken_bucket_preserves_timestamp_range() {
        use crate::support::test_helpers::{json_to_batch, test_span};
        let buffer = MemBuffer::new();
        let dur = bucket_duration_micros();
        let ts = 7 * dur + 12_345; // mid-bucket — distinct from the bucket-start sentinel
        let bucket_id = MemBuffer::compute_bucket_id(ts);
        buffer.insert("p1", "otel_logs_and_spans", json_to_batch(vec![test_span("a", "svc", "p1")]).unwrap(), ts).unwrap();

        let taken = buffer.take_bucket_for_flush("p1", "otel_logs_and_spans", bucket_id).expect("bucket taken");
        assert_eq!((taken.min_timestamp, taken.max_timestamp), (ts, ts), "take must capture the real row range");

        assert!(buffer.restore_taken_bucket(&taken), "restore into a live table must succeed"); // simulate Delta commit failure
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

        // Late arrival after the snapshot, with its own cursor hold.
        buffer.insert_with_hold("project1", "table1", create_test_batch(ts), ts, Some((0, walrus_rust::WalPosition { block_id: 9, offset: 9 }))).unwrap();

        assert!(buffer.finish_flushed_snapshot(&snap), "clean (non-dirty) snapshot must report drained");
        let remaining: usize = buffer.query("project1", "table1", &[]).unwrap().iter().map(|b| b.num_rows()).sum();
        assert_eq!(remaining, create_test_batch(ts).num_rows(), "late rows must survive the prefix drain");
        let holds = buffer.wal_holds("project1", "table1", 4);
        assert!(holds[0].is_some(), "late arrival's hold must survive the prefix drain");
    }

    /// Regression: a bucket surviving the prefix drain (late arrivals) kept
    /// a min/max span covering the DRAINED rows, so `get_bucket_ranges`
    /// masked the drained rows' freshly committed Delta copies for up to a
    /// full flush cycle. The drain must narrow the range to the surviving
    /// rows (not blanket-exempt the bucket — the exclusion stays armed for a
    /// later DML + airborne-commit race, which relies on the mask).
    #[test]
    fn prefix_drain_narrows_survivor_range_to_late_rows() {
        let buffer = MemBuffer::new();
        // Sealed (old) bucket, aligned to its window start so ts+60s stays
        // inside the same bucket. get_bucket_ranges only reports non-current
        // buckets.
        let ts = (chrono::Utc::now().timestamp_micros() - 2 * BUCKET_DURATION_MICROS) / BUCKET_DURATION_MICROS * BUCKET_DURATION_MICROS;
        let bucket_id = MemBuffer::compute_bucket_id(ts);

        buffer.insert("project1", "table1", create_test_batch(ts), ts).unwrap();
        let snap = buffer.snapshot_bucket_for_flush("project1", "table1", bucket_id).unwrap();
        assert!(!buffer.get_bucket_ranges("project1", "table1").is_empty(), "sealed bucket masks Delta pre-flush");

        // Late arrival, deeper into the same bucket's window.
        let late_ts = ts + 60_000_000;
        assert_eq!(MemBuffer::compute_bucket_id(late_ts), bucket_id, "late row must land in the same bucket");
        buffer.insert("project1", "table1", create_test_batch(late_ts), late_ts).unwrap();
        assert!(buffer.finish_flushed_snapshot(&snap));

        let ranges = buffer.get_bucket_ranges("project1", "table1");
        assert_eq!(ranges, vec![(late_ts, late_ts + 1)], "survivor's mask must cover only the late rows so the drained rows' Delta copies stay visible");
    }

    /// The narrowing above CANNOT save a drained row that shares its timestamp with a survivor,
    /// and that is a live correctness bug, not a corner case.
    ///
    /// The mask is a time RANGE; the rows differ only by `id`. So when a flushed row and a
    /// retained row sit at the same instant, narrowing the survivor's range to `(ts, ts+1)` still
    /// covers the drained row's freshly committed Delta copy — it is masked out of the Delta leg
    /// while no longer being in the memory leg, and the row is invisible to queries until the
    /// survivors themselves flush.
    ///
    /// Reproduced end to end 2026-08-28 by `e2e smoke::count_star_returns_correct_value` under
    /// machine load: seven INSERTs at an identical timestamp, `COUNT(*)` = 6, the missing row is
    /// the FIRST one, MemBuffer holds exactly the other 6, and an immediate re-query still says 6
    /// (so it is masked, not in flight). Filed as a flake for three days.
    ///
    /// Identical timestamps are the common case for this workload, not an edge: a batch of OTel
    /// spans arrives stamped to the same instant.
    #[test]
    fn a_drained_row_sharing_a_timestamp_with_a_survivor_is_not_masked() {
        let buffer = MemBuffer::new();
        let ts = (chrono::Utc::now().timestamp_micros() - 2 * BUCKET_DURATION_MICROS) / BUCKET_DURATION_MICROS * BUCKET_DURATION_MICROS;
        let bucket_id = MemBuffer::compute_bucket_id(ts);

        // The row that will be flushed to Delta and drained.
        buffer.insert("project1", "table1", create_test_batch(ts), ts).unwrap();
        let snap = buffer.snapshot_bucket_for_flush("project1", "table1", bucket_id).unwrap();

        // A survivor at the SAME instant — the only difference from
        // `prefix_drain_narrows_survivor_range_to_late_rows`, which uses ts+60s.
        buffer.insert("project1", "table1", create_test_batch(ts), ts).unwrap();
        assert!(buffer.finish_flushed_snapshot(&snap));

        // The drained row's Delta copy lives at exactly `ts`. Any mask covering
        // `ts` hides a committed row that memory no longer holds.
        let ranges = buffer.get_bucket_ranges("project1", "table1");
        assert!(
            !ranges.iter().any(|&(start, end)| (start..end).contains(&ts)),
            "the survivor's mask covers the drained row's committed Delta copy, so that row is \
             invisible to every query until the survivor flushes: ranges={ranges:?}, drained ts={ts}"
        );
    }

    /// The same disappearance, reached three more ways — and all three defeat
    /// the narrowing above, because the narrowing `store`s into `min_timestamp`
    /// and the INSERT path `fetch_min`s the very same atomic (`2857`). Any later
    /// row at the flushed instant pulls the mask back down over rows that have
    /// already left memory.
    ///
    /// - `Take`: `take_bucket_for_flush` empties the bucket and resets min/max to
    ///   sentinels; the bucket stays in the map, so the next insert at the same
    ///   instant rebuilds the mask from scratch over the flushed rows.
    /// - `DrainToEmpty`: `finish_flushed_snapshot` narrows only under `!emptied`,
    ///   so a bucket drained clean and then re-populated never narrows at all.
    /// - `NarrowThenInsert`: the partial-drain narrowing runs and is then undone
    ///   by one more row at the drained instant.
    ///
    /// This is why the floor belongs at READ time in `get_bucket_ranges` rather
    /// than in a stored minimum: it has to live outside the value inserts maintain.
    #[test_case::test_case("take"; "full take, then a row at the same instant")]
    #[test_case::test_case("drain_to_empty"; "snapshot drained clean, then a row at the same instant")]
    #[test_case::test_case("narrow_then_insert"; "partial-drain narrowing undone by a later row")]
    fn a_flushed_instant_stays_visible_however_the_bucket_emptied(mode: &str) {
        let buffer = MemBuffer::new();
        let ts = (chrono::Utc::now().timestamp_micros() - 2 * BUCKET_DURATION_MICROS) / BUCKET_DURATION_MICROS * BUCKET_DURATION_MICROS;
        let bucket_id = MemBuffer::compute_bucket_id(ts);

        // The row that reaches Delta and leaves memory, in every mode.
        buffer.insert("project1", "table1", create_test_batch(ts), ts).unwrap();
        match mode {
            "take" => {
                buffer.take_bucket_for_flush("project1", "table1", bucket_id).expect("bucket has rows");
            }
            "drain_to_empty" => {
                let snap = buffer.snapshot_bucket_for_flush("project1", "table1", bucket_id).unwrap();
                assert!(buffer.finish_flushed_snapshot(&snap), "nothing arrived mid-flight, so the bucket drains clean");
            }
            "narrow_then_insert" => {
                let snap = buffer.snapshot_bucket_for_flush("project1", "table1", bucket_id).unwrap();
                // A late arrival keeps the bucket alive so the narrowing runs...
                buffer.insert("project1", "table1", create_test_batch(ts + 60_000_000), ts + 60_000_000).unwrap();
                assert!(buffer.finish_flushed_snapshot(&snap));
            }
            other => unreachable!("unknown mode {other}"),
        }
        // ...and now one more row at the flushed instant, which is what the
        // workload actually does: a batch of spans all stamped the same.
        buffer.insert("project1", "table1", create_test_batch(ts), ts).unwrap();

        let ranges = buffer.get_bucket_ranges("project1", "table1");
        assert!(
            !ranges.iter().any(|&(start, end)| (start..end).contains(&ts)),
            "[{mode}] the mask covers an instant whose rows are committed to Delta and gone from \
             memory, so those rows answer no query at all: ranges={ranges:?}, flushed ts={ts}"
        );
    }

    /// Regression: a DML that empties a sealed bucket leaves an empty shell
    /// whose WAL holds would pin the topic's cursor forever — it can never
    /// flush (snapshot/take return None on empty). The reap sweep releases
    /// it, but only once its pinned entries' ARRIVAL time passes the replay
    /// cutoff.
    #[test]
    fn reap_releases_dml_emptied_bucket_holds_after_replay_cutoff() {
        let buffer = MemBuffer::new();
        // Event-time old (sealed) but ARRIVING NOW — the reap must key on
        // arrival, not event time, or backfilled deletes release too early.
        let ts = chrono::Utc::now().timestamp_micros() - 2 * BUCKET_DURATION_MICROS;

        buffer.insert_with_hold("project1", "table1", create_test_batch(ts), ts, Some((0, walrus_rust::WalPosition { block_id: 3, offset: 3 }))).unwrap();
        let deleted = buffer.delete("project1", "table1", None, Some((1, walrus_rust::WalPosition { block_id: 4, offset: 4 }))).unwrap();
        assert!(deleted > 0);

        // Entries arrived just now: a replay-cutoff in the past must NOT
        // release the pins (their WAL entries would still replay — a partial
        // cross-shard release could resurrect the deleted rows).
        buffer.reap_expired_empty_buckets(crate::support::now_micros() - 1_000_000);
        let holds = buffer.wal_holds("project1", "table1", 4);
        assert!(holds[0].is_some() && holds[1].is_some(), "emptied bucket must keep pinning while its entries are inside the replay window, got {holds:?}");

        // Once the cutoff passes the entries' arrival time, the shell is
        // reaped and the pins release.
        buffer.reap_expired_empty_buckets(crate::support::now_micros() + 1_000_000);
        let holds = buffer.wal_holds("project1", "table1", 4);
        assert!(holds.iter().all(Option::is_none), "reap must release the expired shell's holds, got {holds:?}");
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
        let deleted = buffer.delete("project1", "table1", None, None).unwrap();
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
        let deleted = buffer.delete("project1", "table1", Some(&predicate), None).unwrap();
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
        let updated = buffer.update("project1", "table1", Some(&predicate), &assignments, None).unwrap();
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
        let deleted = buffer.delete("project1", "table1", Some(&predicate), None).unwrap();
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
        let updated = buffer.update("project1", "table1", Some(&predicate), &assignments, None).unwrap();
        assert_eq!(updated, 1);
    }

    /// id -> name from a buffer query, for asserting the full row set.
    fn collect_id_name(buffer: &MemBuffer) -> std::collections::HashMap<i64, String> {
        use arrow::array::AsArray;
        let mut out = std::collections::HashMap::new();
        for b in buffer.query("p", "t", &[]).unwrap() {
            let ids = b.column(b.schema().index_of("id").unwrap()).as_primitive::<arrow::datatypes::Int64Type>();
            let names = arrow::compute::cast(b.column(b.schema().index_of("name").unwrap()), &DataType::Utf8).unwrap();
            let names = names.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            for i in 0..b.num_rows() {
                out.insert(ids.value(i), names.value(i).to_string());
            }
        }
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

    /// update_with_source must update exactly the source-matched rows and keep
    /// every other row intact — proving the matched-rows-only split rewrite
    /// (the 89GB-OOM fix) doesn't lose/duplicate rows or corrupt unmatched ones.
    #[test]
    fn update_with_source_updates_only_matched_and_preserves_rest() {
        use datafusion::logical_expr::col;
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        buffer.insert("p", "t", create_multi_row_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]), ts).unwrap();

        let n = buffer.update_with_source("p", "t", None, &[("name".to_string(), col("new_name"))], &id_source(), None).unwrap();
        assert_eq!(n, 2, "exactly the 2 source-matched rows update");

        let rows = collect_id_name(&buffer);
        assert_eq!(rows.len(), 5, "no rows lost or duplicated by the split rebuild");
        assert_eq!(rows.get(&1).map(String::as_str), Some("a"));
        assert_eq!(rows.get(&2).map(String::as_str), Some("B"), "matched row updated");
        assert_eq!(rows.get(&3).map(String::as_str), Some("c"));
        assert_eq!(rows.get(&4).map(String::as_str), Some("D"), "matched row updated");
        assert_eq!(rows.get(&5).map(String::as_str), Some("e"));
    }

    /// A source-matched row that FAILS the predicate must be preserved
    /// UNCHANGED (not updated, not dropped) — the critical path of the split
    /// rewrite (`updated_full` scatter + not-updated filter).
    #[test]
    fn update_with_source_respects_predicate_and_preserves_unmatched_predicate_rows() {
        use datafusion::logical_expr::{col, lit};
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        buffer.insert("p", "t", create_multi_row_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]), ts).unwrap();

        // Source matches 2 and 4; predicate keeps only id=2.
        let pred = col("id").eq(lit(2i64));
        let n = buffer.update_with_source("p", "t", Some(&pred), &[("name".to_string(), col("new_name"))], &id_source(), None).unwrap();
        assert_eq!(n, 1, "only the source-matched row passing the predicate updates");

        let rows = collect_id_name(&buffer);
        assert_eq!(rows.len(), 5, "no rows lost");
        assert_eq!(rows.get(&2).map(String::as_str), Some("B"), "matched + predicate-true → updated");
        assert_eq!(rows.get(&4).map(String::as_str), Some("d"), "matched but predicate-false → preserved unchanged");
        assert_eq!(rows.get(&1).map(String::as_str), Some("a"));
    }

    /// WAL-replay regression (2026-07-05 quarantine flood): the DML unparser
    /// emitted table/alias-qualified column refs (`otel_logs_and_spans.x`,
    /// `u.y`), but replay parses against the bare buffer schema (+`source__`
    /// source cols), so every `UPDATE ... FROM` quarantined with "No field named
    /// otel_logs_and_spans.…". Guard: the qualified form fails to parse, the
    /// normalized bare/`source__` form (what the fixed serializer stores) applies.
    #[test]
    fn update_with_source_by_sql_replay_requires_normalized_columns() {
        let buffer = MemBuffer::new();
        let ts = chrono::Utc::now().timestamp_micros();
        buffer.insert("p", "t", create_multi_row_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]), ts).unwrap();
        let src = id_source(); // id (Int64) join key, new_name (Utf8View); matches 2 & 4
        let assigns = [("name".to_string(), "source__new_name".to_string())];
        let keys = [("id".to_string(), "id".to_string())];

        // Table-qualified predicate (the pre-fix stored form) can't resolve.
        let bad = buffer.update_with_source_by_sql("p", "t", Some("t.id > 0"), &assigns, &keys, src.batch.clone(), None, None);
        assert!(bad.is_err(), "table-qualified predicate must fail against the bare replay schema (bug repro)");

        // Bare predicate (the normalized stored form) parses and applies.
        let n = buffer.update_with_source_by_sql("p", "t", Some("id > 0"), &assigns, &keys, src.batch, None, None).unwrap();
        assert_eq!(n, 2);
        let rows = collect_id_name(&buffer);
        assert_eq!(rows.get(&2).map(String::as_str), Some("B"));
        assert_eq!(rows.get(&4).map(String::as_str), Some("D"));
    }

    /// End-to-end reprocessing of the EXACT prod quarantine shape: a buffer row
    /// with a `hashes` List<Utf8> column, updated via the normalized
    /// `array_concat(CASE WHEN hashes ... , [source__tag])` assignment. Proves
    /// parse AND physical eval (array_concat of List<Utf8> with a Utf8View array
    /// literal) succeed — the full path a reprocessed entry takes.
    #[test]
    fn update_with_source_by_sql_applies_normalized_hashes_shape() {
        use arrow::{
            array::{Int64Array, ListBuilder, StringBuilder, StringViewArray},
            datatypes::{DataType, Field},
        };

        let tgt_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("hashes", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
        ]));
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

        let n = buffer
            .update_with_source_by_sql(
                "p",
                "t",
                None,
                &[("hashes".to_string(), "array_concat(CASE WHEN hashes IS NOT NULL THEN hashes ELSE [] END, [source__tag])".to_string())],
                &[("id".to_string(), "id".to_string())],
                src_batch,
                Some(crate::read::functions::function_registry().unwrap().as_ref()),
                None,
            )
            .expect("normalized hashes UPDATE...FROM must parse AND apply");
        assert_eq!(n, 1, "only id=1 matches the source");

        // id=1 hashes must now be ["h1","newtag"]; id=2 untouched.
        use arrow::array::AsArray;
        let mut got: std::collections::HashMap<i64, Vec<String>> = Default::default();
        for b in buffer.query("p", "t", &[]).unwrap() {
            let ids = b.column(b.schema().index_of("id").unwrap()).as_primitive::<arrow::datatypes::Int64Type>();
            let lists = b.column(b.schema().index_of("hashes").unwrap()).as_list::<i32>();
            for i in 0..b.num_rows() {
                let v = lists.value(i);
                let s = arrow::compute::cast(&v, &DataType::Utf8).unwrap();
                let s = s.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                got.insert(ids.value(i), (0..s.len()).map(|j| s.value(j).to_string()).collect());
            }
        }
        assert_eq!(got.get(&1), Some(&vec!["h1".to_string(), "newtag".to_string()]), "matched row appends the tag");
        assert_eq!(got.get(&2), Some(&vec!["h2".to_string()]), "unmatched row unchanged");
    }

    /// WAL-replay regression (2026-07-08 prod quarantine): DML replayed for a
    /// table with NO buffered rows — drained mid-replay by the budget relief
    /// flush, or never re-inserted because its rows were already in Delta —
    /// parsed the predicate against `DFSchema::empty()` and quarantined with
    /// "Schema error: No field named context___span_id. Did you mean
    /// 'source__span_id'?". Nothing buffered ⇒ nothing to mutate ⇒ no-op,
    /// exactly like the live-path `update_with_source` on an untracked table.
    #[test]
    fn dml_by_sql_on_untracked_table_noops_instead_of_schema_error() {
        let buffer = MemBuffer::new();
        let src = id_source();
        let assigns = [("hashes".to_string(), "array_concat(CASE WHEN hashes IS NOT NULL THEN hashes ELSE [] END, [source__new_name])".to_string())];
        let keys = [("id".to_string(), "id".to_string())];
        let n = buffer
            .update_with_source_by_sql(
                "p",
                "t",
                Some("context___span_id IS NOT NULL AND context___trace_id IS NOT NULL"),
                &assigns,
                &keys,
                src.batch,
                None,
                None,
            )
            .expect("UPDATE...FROM replay on an untracked table must no-op, not schema-error");
        assert_eq!(n, 0);
        assert_eq!(
            buffer.update_by_sql("p", "t", Some("id > 0"), &[("name".to_string(), "'x'".to_string())], None, None).unwrap(),
            0,
            "plain UPDATE replay must no-op on an untracked table"
        );
        assert_eq!(buffer.delete_by_sql("p", "t", Some("id > 0"), None, None).unwrap(), 0, "DELETE replay must no-op on an untracked table");
    }

    /// Regression: resumable-replay DML hold (2026-07-09 review, "DML
    /// resurrection"). A replayed DELETE/UPDATE must migrate its WAL `(shard,
    /// pos)` onto the buckets it mutates (via `note_dml_mutation`), so the
    /// resumable rewind marker can't advance past the DML while a mutated bucket
    /// is still un-flushed — otherwise a mid-replay crash re-replays the INSERTs
    /// but skips the DELETE, resurrecting deleted rows. Pre-fix, `delete_by_sql`
    /// passed `None` and the mutated bucket carried no delete hold.
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
        assert!(err.to_string().contains("No functions registered"), "expected 'No functions registered' error, got: {err}");
    }

    /// The 4273 prod quarantines are ONE shape: the monoscope `hashes`
    /// enrichment UPDATE...FROM. Verify the NORMALIZED form of that exact
    /// predicate + assignment (array_concat + CASE + array literals `[]`/`[x]`,
    /// source col → `source__tag`) actually parses against the widened replay
    /// schema — qualifier-stripping alone is not enough if the array/UDF syntax
    /// also fails to re-parse.
    #[test]
    fn parse_prod_hashes_update_from_shape_after_normalization() {
        use arrow::datatypes::{DataType, Field, TimeUnit};
        let widened = DFSchema::try_from(Schema::new(vec![
            Field::new("context___span_id", DataType::Utf8View, true),
            Field::new("context___trace_id", DataType::Utf8View, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
            Field::new("hashes", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
            Field::new("source__tag", DataType::Utf8View, true),
        ]))
        .unwrap();
        let reg = crate::read::functions::function_registry().unwrap();

        // Normalized predicate (table qualifier stripped).
        super::parse_sql_predicate(
            "((context___span_id IS NOT NULL AND context___trace_id IS NOT NULL) \
             AND (\"timestamp\" >= CAST('2026-07-05T14:37:49.995+00:00' AS TIMESTAMP)) \
             AND (\"timestamp\" < CAST('2026-07-05T14:38:22.731+00:00' AS TIMESTAMP)))",
            &widened,
            Some(reg.as_ref()),
        )
        .expect("normalized hashes-update predicate must parse");

        // Normalized assignment RHS (`o.`→bare, `u."tag"`→source__tag).
        super::parse_sql_predicate("array_concat(CASE WHEN hashes IS NOT NULL THEN hashes ELSE [] END, [source__tag])", &widened, Some(reg.as_ref()))
            .expect("normalized hashes-update assignment must parse");
    }

    #[test]
    fn parse_sql_predicate_with_registry_handles_udf() {
        let schema = test_table_df_schema();
        let reg = crate::read::functions::function_registry().unwrap();
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
        let flushable: Vec<_> = buffer.bucket_keys(|id| id < cutoff).into_iter().filter_map(|(p, t, id)| buffer.take_bucket_for_flush(&p, &t, id)).collect();
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
        let total_rows: usize = parts.partitions.iter().flatten().map(|b| b.num_rows()).sum();
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
        let rows: usize = parts.partitions.iter().flatten().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1258 + 13346, "OR of two Utf8View equalities must keep all matches");
    }

    /// Regression: a freshly-buffered batch carrying an OLD event timestamp
    /// (backfill / late-arriving data, or a client pinning a fixed timestamp)
    /// must NOT report a huge `oldest_bucket_age`. The staleness signal measures
    /// how long a bucket has waited to flush (dwell), not its rows' event-time
    /// age — otherwise it false-alarms at "6h stale" while flush is healthy
    /// (prod 2026-06-29: age climbed in real time with flush_failed=0).
    #[test]
    fn oldest_bucket_age_reflects_dwell_not_event_time() {
        let buffer = MemBuffer::new();
        let old_event_ts = crate::support::now_micros() - 6 * 3600 * 1_000_000; // 6h-old event time
        buffer.insert("p", "t", make_batch_with_rows(old_event_ts, 10), old_event_ts).unwrap();

        let oldest = buffer.get_stats().oldest_bucket_micros.expect("flushable bucket present");
        let dwell_secs = (crate::support::now_micros() - oldest) / 1_000_000;
        assert!(dwell_secs < 60, "dwell should be ~0 for a freshly-buffered backfill bucket, got {dwell_secs}s");
    }

    /// Regression guard for the 948 GB-wedge bug class (see
    /// `MemBuffer::estimated_bytes`): the cached total must equal the
    /// authoritative sum of the bucket atomics EXACTLY after any mixed
    /// sequence of inserts, coalesces, DML, drains, evictions and reaps. A
    /// missed or mis-computed mutation site shows up here as an inequality.
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
            // Spread across 3 projects × 3 buckets so coalesce, per-bucket
            // drains and whole-table drops all get exercised.
            let project = format!("p{}", next(3));
            let bucket_offset = next(3) as i64;
            let ts = base + bucket_offset * bucket_span;
            match next(10) {
                0..=5 => {
                    // Insert — drives the coalesce path once a bucket passes
                    // MAX_BATCH_COUNT_PER_BUCKET batches.
                    buffer.insert(&project, "t1", create_test_batch(ts), ts).unwrap();
                    check(step, "insert");
                }
                6 => {
                    buffer.insert_batches(&project, "t1", vec![create_test_batch(ts), wide_view_row(ts)], ts).unwrap();
                    check(step, "insert_batches");
                }
                7 => {
                    // Take + drop the snapshot: rows leave the buffer for good.
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
        // Full teardown must land on exactly zero, not merely "close".
        buffer.evict_old_data(base + 100 * bucket_span);
        buffer.reap_expired_empty_buckets(crate::support::now_micros() + 100 * bucket_span);
        assert_eq!(buffer.estimated_memory_bytes(), buffer.recompute_memory_bytes(), "post-teardown cached total diverged");
        assert_eq!(buffer.recompute_memory_bytes(), 0, "everything evicted but buckets still charge bytes");
    }

    /// The old counter used raw `fetch_sub`; one mismatched subtraction wrapped
    /// it to ~948 GB and every `try_reserve_memory` rejected forever. Now a
    /// too-large subtraction saturates at 0 (under-report, ingest keeps
    /// flowing) and the reconciler restores truth on the next flush tick.
    #[test]
    fn oversized_subtraction_saturates_and_reconciler_restores_truth() {
        let buffer = MemBuffer::new();
        let ts = crate::support::now_micros();
        for _ in 0..8 {
            buffer.insert("p1", "t1", create_test_batch(ts), ts).unwrap();
        }
        let truth = buffer.recompute_memory_bytes();
        assert!(truth > 0);

        // Simulate a mutation site subtracting far more than is charged.
        sub_saturating(&buffer.estimated_bytes, truth + 1_000_000_000);
        assert_eq!(buffer.estimated_memory_bytes(), 0, "underflow must saturate at 0, never wrap to ~usize::MAX");

        let (cached_before, reconciled) = buffer.reconcile_estimated_bytes();
        assert_eq!(cached_before, 0);
        assert_eq!(reconciled, truth);
        assert_eq!(buffer.estimated_memory_bytes(), truth, "reconciler must store the authoritative sum");
    }

    /// The specific leak the recompute-per-call version was introduced to fix:
    /// coalesce shrinks a bucket's `memory_bytes`, and that shrinkage must ride
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
