use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result, ensure};
use arrow::{array::Array, record_batch::RecordBatch};
use datafusion::execution::TaskContext;

use crate::tantivy::{
    histogram::{HistogramWindow, Membership},
    search::{HistogramFile, HistogramSnapshot, HistogramSnapshotResult, TantivySearchService},
    visibility::{SnapshotFile, SourceRows, read_file_rows, resolve_with_memory, winner_masks},
};

/// Exact metadata equality also preserves source order for equal-version ties.
#[derive(Clone, Debug, PartialEq)]
struct DeltaCacheKey {
    root: url::Url,
    files: Vec<SnapshotFile>,
    schema: arrow::datatypes::SchemaRef,
    keys: Vec<String>,
    tiebreak: Option<String>,
}

#[derive(Debug)]
struct CachedDelta {
    key: DeltaCacheKey,
    /// Live masks identify Delta winners, including tombstones. Memory is never cached.
    sources: Vec<SourceRows>,
    decoded_bytes: usize,
    reservation: datafusion::execution::memory_pool::MemoryReservation,
}

#[derive(Debug, Clone, PartialEq)]
struct VisibilityCacheKey {
    delta: DeltaCacheKey,
    bounds: std::ops::Range<i64>,
    tombstone: Option<String>,
}

impl VisibilityCacheKey {
    /// Account retained identity as well as bitmaps. Hash-table buckets use a
    /// conservative allowance for spare capacity and control bytes.
    fn retained_bytes(&self) -> Result<usize> {
        let mut bytes = [
            std::mem::size_of::<Self>(),
            self.delta.root.as_str().len(),
            std::mem::size_of_val(self.delta.files.as_slice()),
            std::mem::size_of_val(self.delta.keys.as_slice()),
            self.delta.tiebreak.as_ref().map_or(0, String::capacity),
            self.tombstone.as_ref().map_or(0, String::capacity),
        ]
        .into_iter()
        .chain(self.delta.keys.iter().map(String::capacity))
        .chain(self.delta.schema.fields().iter().map(|field| field.size()))
        .chain(self.delta.schema.metadata().iter().flat_map(|(key, value)| [key.capacity(), value.capacity()]))
        .try_fold(0_usize, usize::checked_add)
        .context("visibility identity size overflow")?;
        for file in &self.delta.files {
            let buckets = file
                .partition_values
                .capacity()
                .checked_mul(2 * (std::mem::size_of::<(String, Option<String>)>() + 1))
                .context("visibility partition capacity overflow")?;
            bytes = [file.path.capacity(), buckets, file.deletion_vector.as_ref().map_or(0, |dv| dv.path_or_inline_dv.capacity())]
                .into_iter()
                .chain(file.partition_values.iter().flat_map(|(key, value)| [key.capacity(), value.as_ref().map_or(0, String::capacity)]))
                .try_fold(bytes, usize::checked_add)
                .context("visibility file identity size overflow")?;
        }
        Ok(bytes)
    }
}

#[derive(Debug)]
struct CapturedDeltaMasks {
    masks: Vec<arrow::buffer::BooleanBuffer>,
    reservation: datafusion::execution::memory_pool::MemoryReservation,
}

#[derive(Debug)]
enum HistogramCacheEntry {
    Rows(Arc<CachedDelta>),
    Visibility { key: Box<VisibilityCacheKey>, masks: Arc<CapturedDeltaMasks> },
}

impl HistogramCacheEntry {
    fn bytes(&self) -> usize {
        match self {
            Self::Rows(rows) => rows.reservation.size(),
            Self::Visibility { masks, .. } => masks.reservation.size(),
        }
    }

    fn same_snapshot(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Rows(a), Self::Rows(b)) => a.key == b.key,
            (Self::Visibility { key: a, .. }, Self::Visibility { key: b, .. }) => a == b,
            (Self::Rows(_), Self::Visibility { .. }) | (Self::Visibility { .. }, Self::Rows(_)) => false,
        }
    }
}

/// Small resident LRU. Eviction releases its ownership, while captured queries
/// retain the entry and its memory-pool reservation until execution finishes.
#[derive(Debug, Default)]
pub(super) struct HistogramDeltaCache {
    entries: parking_lot::Mutex<std::collections::VecDeque<HistogramCacheEntry>>,
}

impl HistogramDeltaCache {
    const MAX_RESIDENT_BYTES: usize = 64 * 1024 * 1024;

    fn get(&self, key: &DeltaCacheKey) -> Option<Arc<CachedDelta>> {
        self.lookup(|entry| match entry {
            HistogramCacheEntry::Rows(rows) => (rows.key == *key).then(|| rows.clone()),
            HistogramCacheEntry::Visibility { .. } => None,
        })
    }

    fn visibility(&self, key: &VisibilityCacheKey) -> Option<Arc<CapturedDeltaMasks>> {
        self.lookup(|entry| match entry {
            HistogramCacheEntry::Visibility { key: candidate, masks } => (candidate.as_ref() == key).then(|| masks.clone()),
            HistogramCacheEntry::Rows(_) => None,
        })
    }

    fn lookup<T>(&self, find: impl Fn(&HistogramCacheEntry) -> Option<Arc<T>>) -> Option<Arc<T>> {
        let mut entries = self.entries.lock();
        let (position, value) = entries.iter().enumerate().find_map(|(position, entry)| find(entry).map(|value| (position, value)))?;
        let entry = entries.remove(position)?;
        entries.push_back(entry);
        Some(value)
    }

    fn reserve(
        &self, bytes: usize, pool: &Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    ) -> Result<datafusion::execution::memory_pool::MemoryReservation> {
        let reservation = datafusion::execution::memory_pool::MemoryConsumer::new("TantivyHistogramDelta").register(pool);
        loop {
            match reservation.try_grow(bytes) {
                Ok(()) => return Ok(reservation),
                Err(error) => {
                    // Cached data must not prevent a new snapshot from being admitted.
                    // Pinned queries retain their own ownership and remain charged.
                    if self.entries.lock().pop_front().is_none() {
                        return Err(error.into());
                    }
                }
            }
        }
    }

    fn insert(&self, entry: Arc<CachedDelta>) {
        self.insert_entry(HistogramCacheEntry::Rows(entry));
    }

    fn insert_entry(&self, entry: HistogramCacheEntry) {
        let size = entry.bytes();
        if size > Self::MAX_RESIDENT_BYTES {
            return;
        }
        let mut entries = self.entries.lock();
        // Concurrent builders can finish the same snapshot. Keep only one resident copy.
        entries.retain(|existing| !existing.same_snapshot(&entry));
        let mut resident: usize = entries.iter().map(HistogramCacheEntry::bytes).sum();
        while resident > Self::MAX_RESIDENT_BYTES - size || entries.len() >= 32 {
            let Some(oldest) = entries.pop_front() else { break };
            resident -= oldest.bytes();
        }
        entries.push_back(entry);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HistogramDmlScope {
    All,
    Timestamps(std::ops::RangeInclusive<i64>),
}

impl HistogramDmlScope {
    fn overlaps(&self, range: &std::ops::RangeInclusive<i64>) -> bool {
        match self {
            Self::All => true,
            Self::Timestamps(active) => active.start() <= range.end() && range.start() <= active.end(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct HistogramDmlGuard(HistogramDmlScope);

#[derive(Debug)]
struct HistogramDmlCapture {
    range: std::ops::RangeInclusive<i64>,
    invalidated: AtomicBool,
}

impl HistogramDmlCapture {
    fn validate(&self) -> Result<()> {
        ensure!(!self.invalidated.load(Ordering::SeqCst), "SQL DML changed while capturing histogram sources");
        Ok(())
    }
}

#[derive(Debug, Default)]
struct HistogramDmlActivity {
    writes: Vec<std::sync::Weak<HistogramDmlGuard>>,
    captures: Vec<std::sync::Weak<HistogramDmlCapture>>,
}

#[derive(Debug, Default)]
pub(super) struct HistogramDmlState(parking_lot::Mutex<HistogramDmlActivity>);

impl HistogramDmlState {
    fn enter(&self, scope: HistogramDmlScope) -> Arc<HistogramDmlGuard> {
        // Registration and invalidation are atomic with respect to capture.
        // Weak entries retain no completed statements or finished read views.
        let mut activity = self.0.lock();
        activity.captures.retain(|weak| {
            let Some(capture) = weak.upgrade() else { return false };
            if scope.overlaps(&capture.range) {
                capture.invalidated.store(true, Ordering::SeqCst);
            }
            true
        });
        activity.writes.retain(|weak| weak.strong_count() != 0);
        let guard = Arc::new(HistogramDmlGuard(scope));
        activity.writes.push(Arc::downgrade(&guard));
        guard
    }

    fn capture(&self, range: std::ops::RangeInclusive<i64>) -> Result<Arc<HistogramDmlCapture>> {
        let mut activity = self.0.lock();
        activity.writes.retain(|weak| weak.strong_count() != 0);
        ensure!(
            !activity.writes.iter().filter_map(std::sync::Weak::upgrade).any(|guard| guard.0.overlaps(&range)),
            "histogram capture overlaps active SQL DML"
        );
        activity.captures.retain(|weak| weak.strong_count() != 0);
        let capture = Arc::new(HistogramDmlCapture { range, invalidated: AtomicBool::new(false) });
        activity.captures.push(Arc::downgrade(&capture));
        Ok(capture)
    }
}

/// An owned histogram read view. Later writes cannot replace its sources,
/// query bounds or manifest generations. Daily winners are resolved during execution.
pub struct CapturedHistogram {
    search: Arc<TantivySearchService>,
    table: String,
    project: String,
    root: url::Url,
    window: HistogramWindow,
    membership: Option<Membership>,
    partitions: std::collections::BTreeMap<i64, Vec<SnapshotFile>>,
    logical_counts: std::collections::BTreeMap<i64, u64>,
    manifest: Arc<crate::tantivy::Manifest>,
    memory: crate::write::mem_buffer::MemSnapshot,
    log_store: deltalake::logstore::LogStoreRef,
    projected: arrow::datatypes::SchemaRef,
    keys: Vec<String>,
    tiebreak: Option<String>,
    tombstone: Option<String>,
    cache: Arc<HistogramDeltaCache>,
    max_decoded_bytes: usize,
    context: Arc<TaskContext>,
}

const DAY_MICROS: i64 = 86_400_000_000;

/// Admission and recent-attempt history for query-triggered maintenance.
/// A failed large day must give other requested days a chance to build.
#[derive(Debug)]
pub(super) struct HistogramProofBuilds {
    slot: Arc<tokio::sync::Semaphore>,
    attempts: parking_lot::Mutex<std::collections::VecDeque<(crate::read::CountPartition, std::time::Instant)>>,
}

impl Default for HistogramProofBuilds {
    fn default() -> Self {
        Self { slot: Arc::new(tokio::sync::Semaphore::new(1)), attempts: Default::default() }
    }
}

fn histogram_timestamps(batch: &RecordBatch) -> Result<&[i64]> {
    let column = batch.column_by_name("timestamp").context("histogram source is missing timestamp")?;
    ensure!(column.null_count() == 0, "histogram timestamp contains nulls");
    ensure!(
        matches!(column.data_type(), arrow::datatypes::DataType::Int64 | arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)),
        "histogram timestamp must use microseconds"
    );
    crate::read::bound_slice(column).context("invalid histogram timestamp representation")
}

impl CapturedHistogram {
    /// Request one missing completed-day proof without delaying this query.
    /// The builder uses the maintenance runtime and shared count-build semaphore.
    /// The extra permit prevents a chart workload from queuing many days.
    fn seed_missing_proof(&self, database: &super::Database, day: i64) -> Option<tokio::task::JoinHandle<()>> {
        database.tantivy_indexer()?;
        let state = &database.histogram_proof_build;
        let permit = state.slot.clone().try_acquire_owned().ok()?;
        let today = chrono::Utc::now().timestamp_micros().div_euclid(DAY_MICROS);
        let key = {
            let now = std::time::Instant::now();
            let mut attempts = state.attempts.lock();
            attempts.retain(|(_, at)| now.duration_since(*at) < std::time::Duration::from_secs(60));
            let files = self.partitions.get(&day)?;
            if day >= today || self.logical_counts.contains_key(&day) {
                return None;
            }
            let key = crate::read::CountPartition {
                project_id: self.project.clone(),
                table_name: self.table.clone(),
                date: files.first()?.partition_values.get("date")?.clone()?,
            };
            if attempts.iter().any(|(attempt, _)| attempt == &key) {
                return None;
            }
            if attempts.len() == 256 {
                attempts.pop_front();
            }
            attempts.push_back((key.clone(), now));
            key
        };
        let database = Arc::new(database.background_clone());
        Some(tokio::spawn(async move {
            let _permit = permit;
            match database.build_histogram_count_proof(&key).await {
                Ok(Some(_)) => {}
                Ok(None) => tracing::debug!(project_id = key.project_id, table = key.table_name, date = key.date, "histogram uniqueness proof declined"),
                Err(error) => {
                    tracing::warn!(%error, project_id = key.project_id, table = key.table_name, date = key.date, "histogram proof builder task failed")
                }
            }
        }))
    }

    /// Counts daily partitions sequentially against the same captured read view.
    pub async fn count(&self) -> Result<HistogramSnapshotResult> {
        let mut total = HistogramSnapshotResult::default();
        for (&day, files) in &self.partitions {
            let part = self.count_partition(day, files).await?;
            crate::tantivy::histogram::merge_counts(&mut total.counts, part.counts)?;
            total.indexed_sources += part.indexed_sources;
            total.scanned_sources += part.scanned_sources;
            total.index_errors.extend(part.index_errors);
        }
        Ok(total)
    }

    /// Counts with streamed visibility columns and the query memory pool.
    /// Unlike `count`, this limits each decoded batch rather than total decoded data.
    pub async fn count_streaming(&self) -> Result<HistogramSnapshotResult> {
        self.count_streaming_before_visibility(|_| {}).await
    }

    async fn count_streaming_before_visibility(&self, mut before_visibility: impl FnMut(i64)) -> Result<HistogramSnapshotResult> {
        let mut total = HistogramSnapshotResult::default();
        for (&day, files) in &self.partitions {
            let indexed = match self.count_index_only_partition(day, files).await {
                Ok(result) => result,
                Err(error) => {
                    tracing::warn!(%error, "indexed histogram declined; streaming captured visibility");
                    None
                }
            };
            let part = match indexed {
                Some(result) => result,
                None => {
                    before_visibility(day);
                    self.count_streaming_partition(day, files).await?
                }
            };
            crate::tantivy::histogram::merge_counts(&mut total.counts, part.counts)?;
            total.indexed_sources += part.indexed_sources;
            total.scanned_sources += part.scanned_sources;
            total.index_errors.extend(part.index_errors);
        }
        Ok(total)
    }

    fn visibility_cache_key(&self, day: i64, files: &[SnapshotFile]) -> Result<Option<VisibilityCacheKey>> {
        let lo = day.checked_mul(DAY_MICROS).context("histogram date overflow")?;
        let hi = lo.checked_add(DAY_MICROS).context("histogram date overflow")?;
        if self.memory.covered_ranges.iter().any(|&(start, end)| start < hi && end > lo) {
            return Ok(None);
        }
        for batch in &self.memory.batches {
            if histogram_timestamps(batch)?.iter().any(|&timestamp| timestamp >= lo && timestamp < hi) {
                return Ok(None);
            }
        }
        let (start, end) = self.window.bounds();
        Ok(Some(VisibilityCacheKey {
            delta: DeltaCacheKey {
                root: self.log_store.root_url().clone(),
                files: files.to_vec(),
                schema: self.projected.clone(),
                keys: self.keys.clone(),
                tiebreak: self.tiebreak.clone(),
            },
            bounds: start.max(lo)..end.min(hi),
            tombstone: self.tombstone.clone(),
        }))
    }

    async fn prepare_file(
        &self, file: &SnapshotFile,
    ) -> Result<(Arc<crate::tantivy::visibility::PreparedFileRows>, Arc<datafusion::execution::memory_pool::MemoryReservation>)> {
        self.search.stats.histogram_parquet_prepares.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("timefusion_tantivy_histogram_parquet_prepares_total").increment(1);
        let file = Arc::new(crate::tantivy::visibility::PreparedFileRows::open(self.log_store.clone(), file).await?);
        let owner = Arc::new(self.cache.reserve(file.retained_bytes()?, self.context.memory_pool())?);
        Ok((file, owner))
    }

    async fn count_streaming_partition(&self, day: i64, files: &[SnapshotFile]) -> Result<HistogramSnapshotResult> {
        use futures::TryStreamExt;

        let key = self.visibility_cache_key(day, files)?;
        let cached = key.as_ref().and_then(|key| self.cache.visibility(key));
        let (delta, memory, _memory_owner, prepared) = match cached {
            Some(delta) => {
                self.search.stats.histogram_delta_cache_hits.fetch_add(1, Ordering::Relaxed);
                metrics::counter!("timefusion_tantivy_histogram_delta_cache_hits_total").increment(1);
                let rows =
                    self.memory.batches.iter().map(RecordBatch::num_rows).try_fold(0_usize, usize::checked_add).context("memory visibility row overflow")?;
                let owner = self.cache.reserve(rows.div_ceil(8), self.context.memory_pool())?;
                let memory = arrow::buffer::BooleanBuffer::new_unset(rows);
                owner.try_resize(memory.inner().capacity())?;
                (delta, memory, owner, None)
            }
            None => {
                let mut prepared = Vec::with_capacity(files.len());
                for file in files {
                    prepared.push(self.prepare_file(file).await?);
                }
                let mut masks = self.stream_partition_masks(day, &prepared).await?;
                let memory = masks.pop().context("missing memory winner mask")?;
                let owner = self.cache.reserve(memory.inner().capacity(), self.context.memory_pool())?;
                let header = masks
                    .capacity()
                    .checked_mul(std::mem::size_of::<arrow::buffer::BooleanBuffer>())
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of::<CapturedDeltaMasks>() + 2 * std::mem::size_of::<usize>()))
                    .context("histogram mask capacity overflow")?;
                let bytes = masks.iter().map(|mask| mask.inner().capacity()).try_fold(header, usize::checked_add).context("histogram mask size overflow")?;
                let bytes = bytes
                    .checked_add(key.as_ref().map(VisibilityCacheKey::retained_bytes).transpose()?.unwrap_or(0))
                    .context("visibility cache size overflow")?;
                let reservation = self.cache.reserve(bytes, self.context.memory_pool())?;
                let delta = Arc::new(CapturedDeltaMasks { masks, reservation });
                if let Some(key) = key {
                    self.cache.insert_entry(HistogramCacheEntry::Visibility { key: Box::new(key), masks: delta.clone() });
                }
                (delta, memory, owner, Some(prepared))
            }
        };
        let entries = self.manifest.histogram_entries(&self.root, files)?;
        let mut result = HistogramSnapshotResult::default();
        for (index, file) in files.iter().enumerate() {
            let indexed = match &entries[index] {
                Some(entry) => {
                    self.search
                        .histogram_file(
                            &self.table,
                            &self.project,
                            self.window,
                            self.membership.as_ref(),
                            HistogramFile {
                                table_root: &self.root,
                                manifest_key: entry.key,
                                entry: entry.entry,
                                source_file: &file.path,
                                visible: delta.masks[index].clone(),
                            },
                        )
                        .await
                }
                None => Err(anyhow::anyhow!("snapshot file has no histogram index")),
            };
            let counts = match indexed {
                Ok(counts) => {
                    result.indexed_sources += 1;
                    counts
                }
                Err(error) => {
                    result.index_errors.push(error);
                    result.scanned_sources += 1;
                    // A cached mask already pins physical row identity. Read
                    // Parquet only when this source cannot use its index.
                    let (file, _owner) = match &prepared {
                        Some(prepared) => prepared[index].clone(),
                        None => self.prepare_file(file).await?,
                    };
                    let mut counts = std::collections::BTreeMap::<i64, u64>::new();
                    let mut rows = file.stream(self.projected.clone())?;
                    let mut offset = 0;
                    while let Some(batch) = rows.try_next().await? {
                        let bytes = batch.get_array_memory_size();
                        ensure!(bytes <= self.max_decoded_bytes, "histogram fallback batch exceeds decoded budget");
                        let _owner = self.cache.reserve(bytes, self.context.memory_pool())?;
                        let visible = delta.masks[index].slice(offset, batch.num_rows());
                        crate::tantivy::histogram::merge_counts(
                            &mut counts,
                            self.window.count_rows(std::slice::from_ref(&batch), &visible, self.membership.as_ref())?,
                        )?;
                        offset += batch.num_rows();
                    }
                    counts
                }
            };
            crate::tantivy::histogram::merge_counts(&mut result.counts, counts)?;
        }
        crate::tantivy::histogram::merge_counts(&mut result.counts, self.window.count_rows(&self.memory.batches, &memory, self.membership.as_ref())?)?;
        if !self.memory.batches.is_empty() {
            result.scanned_sources += 1;
        }
        TantivySearchService::record_histogram_snapshot(&self.search.stats);
        Ok(result)
    }

    async fn stream_partition_masks(
        &self, day: i64, prepared: &[(Arc<crate::tantivy::visibility::PreparedFileRows>, Arc<datafusion::execution::memory_pool::MemoryReservation>)],
    ) -> Result<Vec<arrow::buffer::BooleanBuffer>> {
        use crate::tantivy::visibility::{FileVisibilitySource, VisibilityFiles, lineage_batch, lineage_schema, stream_winner_masks};
        use datafusion::physical_plan::{streaming::StreamingTableExec, union::UnionExec};

        let lo = day.checked_mul(DAY_MICROS).context("histogram date overflow")?;
        let hi = lo.checked_add(DAY_MICROS).context("histogram date overflow")?;
        let columns =
            self.keys.iter().map(String::as_str).chain(self.tiebreak.as_deref()).chain(self.tombstone.as_deref()).collect::<std::collections::BTreeSet<_>>();
        let projection = columns.into_iter().map(|name| self.projected.index_of(name)).collect::<std::result::Result<Vec<_>, _>>()?;
        let narrow = Arc::new(self.projected.project(&projection)?);
        let schema = lineage_schema(&narrow)?;
        let ranges = Arc::new(self.memory.covered_ranges.clone());
        let partitions = prepared
            .iter()
            .enumerate()
            .map(|(source, (file, owner))| {
                Ok(FileVisibilitySource {
                    file: file.clone(),
                    schema: schema.clone(),
                    source: u32::try_from(source)?,
                    covered_ranges: ranges.clone(),
                    partition: (lo, hi),
                    max_batch_bytes: self.max_decoded_bytes,
                    owner: owner.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut memory = Vec::new();
        let mut offset = 0_usize;
        for batch in &self.memory.batches {
            let arrays = narrow
                .fields()
                .iter()
                .map(|field| -> Result<_> {
                    match batch.column_by_name(field.name()) {
                        Some(array) => Ok(arrow::compute::cast(array, field.data_type())?),
                        None => {
                            ensure!(field.is_nullable(), "missing required memory visibility column");
                            Ok(arrow::array::new_null_array(field.data_type(), batch.num_rows()))
                        }
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let batch = RecordBatch::try_new(narrow.clone(), arrays)?;
            let live = arrow::buffer::BooleanBuffer::from_iter(histogram_timestamps(&batch)?.iter().map(|&t| t >= lo && t < hi));
            memory.push(lineage_batch(&batch, &live, schema.clone(), u32::try_from(prepared.len())?, offset)?);
            offset = offset.checked_add(batch.num_rows()).context("memory visibility row overflow")?;
        }
        let source_rows = prepared.iter().map(|(file, _)| file.live().len()).chain(std::iter::once(offset)).collect();
        let memory_bytes =
            memory.iter().map(RecordBatch::get_array_memory_size).try_fold(0_usize, usize::checked_add).context("memory visibility size overflow")?;
        let _memory_owner = self.cache.reserve(memory_bytes, self.context.memory_pool())?;
        let memory_plan = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(&[memory], schema.clone(), None)?;
        let input: Arc<dyn datafusion::physical_plan::ExecutionPlan> = if partitions.is_empty() {
            memory_plan
        } else {
            UnionExec::try_new(vec![
                Arc::new(StreamingTableExec::try_new(schema.clone(), vec![Arc::new(VisibilityFiles { schema, sources: partitions })], None, [], false, None)?),
                memory_plan,
            ])?
        };
        // Timestamp is part of the immutable key, so out-of-window rows
        // cannot defeat an in-window version. Filter before retaining sort rows.
        let (start, end) = self.window.bounds();
        let input: Arc<dyn datafusion::physical_plan::ExecutionPlan> = if start > lo || end < hi {
            use datafusion::{
                common::ScalarValue,
                logical_expr::Operator,
                physical_expr::expressions::{Column, binary, lit},
            };
            let schema = input.schema();
            let timestamp = Arc::new(Column::new_with_schema("timestamp", &schema)?);
            let data_type = schema.field_with_name("timestamp")?.data_type();
            let bound = |value, op| -> Result<_> { Ok(binary(timestamp.clone(), op, lit(ScalarValue::Int64(Some(value)).cast_to(data_type)?), &schema)?) };
            let predicate = binary(bound(start, Operator::GtEq)?, Operator::And, bound(end, Operator::Lt)?, &schema)?;
            Arc::new(datafusion::physical_plan::filter::FilterExec::try_new(predicate, input)?)
        } else {
            input
        };
        stream_winner_masks(input, source_rows, &self.keys, self.tiebreak.as_deref(), self.tombstone.as_deref(), self.context.clone()).await
    }

    async fn count_empty_partition(&self, day: i64, files: &[SnapshotFile]) -> Result<Option<HistogramSnapshotResult>> {
        let Some(membership) = &self.membership else { return Ok(None) };
        let lo = day.checked_mul(DAY_MICROS).context("histogram date overflow")?;
        let hi = lo.checked_add(DAY_MICROS).context("histogram date overflow")?;
        for batch in &self.memory.batches {
            if histogram_timestamps(batch)?.iter().any(|&timestamp| timestamp >= lo && timestamp < hi) {
                return Ok(None);
            }
        }
        let entries = self.manifest.histogram_entries(&self.root, files)?;
        let columns = membership.columns();
        for (file, entry) in files.iter().zip(entries) {
            let Some(entry) = entry else { return Ok(None) };
            if entry.entry.min_timestamp_micros.is_none_or(|min| min < lo)
                || entry.entry.max_timestamp_micros.is_none_or(|max| max >= hi)
                || columns.iter().any(|column| !entry.entry.element_fields.contains(*column))
            {
                return Ok(None);
            }
            let indexed_rows = entry.entry.rows;
            if self.search.histogram_file_has_matches(&self.table, &self.project, self.window, membership, entry).await? {
                return Ok(None);
            }
            // Metadata validates complete coverage; no event rows are decoded.
            let prepared = crate::tantivy::visibility::PreparedFileRows::open(self.log_store.clone(), file).await?;
            let _owner = self.cache.reserve(prepared.retained_bytes()?, self.context.memory_pool())?;
            ensure!(u64::try_from(prepared.live().len())? == indexed_rows, "empty index does not cover every physical row");
        }
        TantivySearchService::record_histogram_snapshot(&self.search.stats);
        Ok(Some(HistogramSnapshotResult { indexed_sources: files.len(), ..Default::default() }))
    }

    /// Prefer the existing count witness; probe absence only when it declines.
    async fn count_index_only_partition(&self, day: i64, files: &[SnapshotFile]) -> Result<Option<HistogramSnapshotResult>> {
        match self.count_unique_partition(day, files).await? {
            Some(result) => Ok(Some(result)),
            None => self.count_empty_partition(day, files).await,
        }
    }

    /// Equality with an exact logical count proves there are no live duplicate
    /// versions or tombstones: either would make the logical count smaller.
    async fn count_unique_partition(&self, day: i64, files: &[SnapshotFile]) -> Result<Option<HistogramSnapshotResult>> {
        let Some(&logical_count) = self.logical_counts.get(&day) else { return Ok(None) };
        let lo = day.checked_mul(DAY_MICROS).context("histogram date overflow")?;
        let hi = lo.checked_add(DAY_MICROS).context("histogram date overflow")?;
        if self.memory.covered_ranges.iter().any(|&(start, end)| start < hi && end > lo) {
            return Ok(None);
        }
        for batch in &self.memory.batches {
            if histogram_timestamps(batch)?.iter().any(|&timestamp| timestamp >= lo && timestamp < hi) {
                return Ok(None);
            }
        }
        let entries = self.manifest.histogram_entries(&self.root, files)?;
        let mut physical_count = 0_u64;
        for (file, entry) in files.iter().zip(&entries) {
            let Some(entry) = entry else { return Ok(None) };
            if self.membership.as_ref().is_some_and(|predicate| predicate.columns().iter().any(|column| !entry.entry.element_fields.contains(*column))) {
                return Ok(None);
            }
            if entry.entry.min_timestamp_micros.is_none_or(|min| min < lo) || entry.entry.max_timestamp_micros.is_none_or(|max| max >= hi) {
                return Ok(None);
            }
            let deleted = file.deletion_vector.as_ref().map_or(Ok(0), |dv| u64::try_from(dv.cardinality))?;
            let live = entry.entry.rows.checked_sub(deleted).context("deletion vector exceeds index row count")?;
            physical_count = physical_count.checked_add(live).context("histogram physical count overflow")?;
        }
        if physical_count != logical_count {
            return Ok(None);
        }
        let mut result = HistogramSnapshotResult::default();
        for (file, entry) in files.iter().zip(entries) {
            let entry = entry.context("validated histogram entry disappeared")?;
            let rows = usize::try_from(entry.entry.rows)?;
            let bytes = rows.div_ceil(8);
            ensure!(bytes <= self.max_decoded_bytes, "histogram visibility bitmap exceeds budget");
            let visible = crate::tantivy::visibility::deletion_vector_mask(self.log_store.clone(), file.deletion_vector.as_ref(), rows).await?;
            let bytes = visible.inner().capacity();
            ensure!(bytes <= self.max_decoded_bytes, "histogram visibility allocation exceeds budget");
            let _reservation = self.cache.reserve(bytes, self.context.memory_pool())?;
            let counts = self
                .search
                .histogram_file(
                    &self.table,
                    &self.project,
                    self.window,
                    self.membership.as_ref(),
                    HistogramFile { table_root: &self.root, manifest_key: entry.key, entry: entry.entry, source_file: &file.path, visible },
                )
                .await?;
            crate::tantivy::histogram::merge_counts(&mut result.counts, counts)?;
            result.indexed_sources += 1;
        }
        TantivySearchService::record_histogram_snapshot(&self.search.stats);
        self.search.stats.histogram_unique_partitions.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("timefusion_tantivy_histogram_unique_partitions_total").increment(1);
        Ok(Some(result))
    }

    async fn count_partition(&self, day: i64, files: &[SnapshotFile]) -> Result<HistogramSnapshotResult> {
        match self.count_index_only_partition(day, files).await {
            Ok(Some(result)) => return Ok(result),
            Ok(None) => {}
            Err(error) => tracing::warn!(%error, "index-only histogram declined; resolving captured rows"),
        }

        let mut remaining = self.max_decoded_bytes;
        let mut batches = Vec::new();
        for batch in &self.memory.batches {
            let timestamps = histogram_timestamps(batch)?;
            let selected = arrow::array::BooleanArray::from_iter(timestamps.iter().map(|timestamp| Some(timestamp.div_euclid(DAY_MICROS) == day)));
            if selected.true_count() == 0 {
                continue;
            }
            let arrays = self
                .projected
                .fields()
                .iter()
                .map(|field| -> Result<_> {
                    let array = match batch.column_by_name(field.name()) {
                        Some(array) => arrow::compute::cast(array, field.data_type())?,
                        None => {
                            ensure!(field.is_nullable(), "memory is missing required column {}", field.name());
                            arrow::array::new_null_array(field.data_type(), batch.num_rows())
                        }
                    };
                    ensure!(field.is_nullable() || array.null_count() == 0, "memory required column contains nulls");
                    Ok(array)
                })
                .collect::<Result<Vec<_>>>()?;
            let narrow = RecordBatch::try_new(self.projected.clone(), arrays)?;
            let batch = arrow::compute::filter_record_batch(&narrow, &selected)?;
            remaining = remaining.checked_sub(batch.get_array_memory_size()).context("histogram memory exceeds decoded budget")?;
            batches.push(batch);
        }
        let memory = crate::write::mem_buffer::MemSnapshot { batches, covered_ranges: self.memory.covered_ranges.clone() };
        let cache_key = DeltaCacheKey {
            root: self.log_store.root_url().clone(),
            files: files.to_vec(),
            schema: self.projected.clone(),
            keys: self.keys.clone(),
            tiebreak: self.tiebreak.clone(),
        };
        let delta = match self.cache.get(&cache_key) {
            Some(delta) => {
                ensure!(delta.decoded_bytes <= remaining, "cached histogram files exceed decoded budget");
                self.search.stats.histogram_delta_cache_hits.fetch_add(1, Ordering::Relaxed);
                metrics::counter!("timefusion_tantivy_histogram_delta_cache_hits_total").increment(1);
                delta
            }
            None => {
                let mut sources = Vec::with_capacity(files.len());
                let initial = remaining;
                for file in files {
                    let source = read_file_rows(self.log_store.clone(), file, self.projected.clone(), remaining).await?;
                    for batch in &source.batches {
                        ensure!(
                            histogram_timestamps(batch)?.iter().all(|value| value.div_euclid(DAY_MICROS) == day),
                            "histogram file timestamp differs from its date partition"
                        );
                        remaining = remaining.checked_sub(batch.get_array_memory_size()).context("histogram files exceed decoded budget")?;
                    }
                    sources.push(source);
                }
                // Do not discard tombstones before comparing with the memory overlay.
                let winners = winner_masks(&sources, &self.keys, self.tiebreak.as_deref(), None, self.context.clone()).await?;
                for (source, winner) in sources.iter_mut().zip(winners) {
                    source.live = winner;
                }
                let decoded_bytes = initial - remaining;
                let mask_bytes: usize = sources.iter().map(|source| source.live.inner().capacity()).sum();
                let reservation =
                    self.cache.reserve(decoded_bytes.checked_add(mask_bytes).context("histogram cache size overflow")?, self.context.memory_pool())?;
                let delta = Arc::new(CachedDelta { key: cache_key, sources, decoded_bytes, reservation });
                self.cache.insert(delta.clone());
                delta
            }
        };
        let rows =
            resolve_with_memory(delta.sources.clone(), memory, &self.keys, self.tiebreak.as_deref(), self.tombstone.as_deref(), self.context.clone()).await?;
        self.search
            .histogram_snapshot(
                &self.table,
                &self.project,
                self.window,
                self.membership.as_ref(),
                HistogramSnapshot { table_root: &self.root, files, manifest: &self.manifest, rows: &rows },
            )
            .await
    }
}

impl super::Database {
    /// Prove a complete Delta partition is unique and contains no tombstones.
    /// Sorting can spill through the maintenance runtime; the checker retains
    /// only one batch of encoded keys and the preceding batch's final key.
    /// Non-unique partitions decline without publishing a count witness.
    async fn build_histogram_count_proof(&self, key: &crate::read::CountPartition) -> Result<Option<u64>> {
        use arrow::row::{RowConverter, SortField};
        use datafusion::{execution::memory_pool::MemoryConsumer, prelude::SessionContext};
        use futures::TryStreamExt;

        let _permit = tokio::select! {
            permit = self.logical_count_build_sem.acquire() => permit?,
            () = self.maintenance_shutdown.cancelled() => return Ok(None),
        };
        let indexer = self.tantivy_indexer().context("histogram proof requires an index publisher")?;
        tracing::info!(project_id = key.project_id, table = key.table_name, date = key.date, "histogram uniqueness proof build started");
        let schema = crate::schema::get_schema(&key.table_name).context("unknown histogram proof table")?;
        ensure!(schema.dedup_keys.iter().any(|key| key == "timestamp"), "histogram proof requires timestamp in the key");
        let date = key.date.parse::<chrono::NaiveDate>()?;
        let lo = date.and_hms_opt(0, 0, 0).context("invalid proof date")?.and_utc().timestamp_micros();
        let hi = lo.checked_add(DAY_MICROS).context("proof date overflow")?;
        let table_ref = self.resolve_table(&key.project_id, &key.table_name).await?;
        let (files, snapshot, log_store) = {
            let table = table_ref.read().await;
            let (_, files) = Self::logical_count_partition_snapshot(&table, &key.project_id, &key.date)?;
            (files, Arc::new(table.snapshot()?.snapshot().clone()), table.log_store())
        };
        let root = log_store.root_url().clone();
        if files.is_empty() {
            return Ok(None);
        }
        let provider = Self::narrow_provider(log_store, snapshot, files.keys().cloned().collect(), None, None).await?;
        let context = SessionContext::new_with_state(super::build_optimize_session_state(1, self.maintenance_runtime_env()));
        let columns: Vec<_> = schema.dedup_keys.iter().chain(schema.tombstone_column.iter()).map(String::as_str).collect();
        let frame = context
            .read_table(provider)?
            .select_columns(&columns)?
            .sort(schema.dedup_keys.iter().map(|key| datafusion::logical_expr::col(key).sort(true, true)).collect())?;
        let arrow_schema = frame.schema().as_arrow();
        let key_indices = schema.dedup_keys.iter().map(|key| arrow_schema.index_of(key)).collect::<std::result::Result<Vec<_>, _>>()?;
        let converter = RowConverter::new(key_indices.iter().map(|&index| SortField::new(arrow_schema.field(index).data_type().clone())).collect())?;
        let task = context.task_ctx();
        let reservation = MemoryConsumer::new("TantivyProofKeys").register(task.memory_pool());
        let mut stream = frame.execute_stream().await?;
        let mut previous: Option<Vec<u8>> = None;
        let mut count = 0_u64;
        let mut brake = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            let batch = tokio::select! {
                () = self.maintenance_shutdown.cancelled() => return Ok(None),
                _ = brake.tick() => {
                    ensure!(
                        super::process_memory_bytes().is_none_or(|used| used <= self.config.derived.memory_brake_limit_bytes()),
                        "histogram proof stopped at the host memory brake"
                    );
                    continue;
                }
                batch = stream.try_next() => batch?,
            };
            let Some(batch) = batch else { break };
            ensure!(histogram_timestamps(&batch)?.iter().all(|&timestamp| timestamp >= lo && timestamp < hi), "proof timestamp differs from its partition");
            if let Some(name) = &schema.tombstone_column {
                let deleted = batch.column_by_name(name).context("missing proof tombstone")?;
                let deleted = deleted.as_any().downcast_ref::<arrow::array::BooleanArray>().context("proof tombstone must be Boolean")?;
                if deleted.iter().any(|value| value == Some(true)) {
                    return Ok(None);
                }
            }
            let keys = converter.convert_columns(&key_indices.iter().map(|&index| batch.column(index).clone()).collect::<Vec<_>>())?;
            let bytes = [batch.get_array_memory_size(), keys.size(), converter.size(), previous.as_ref().map_or(0, Vec::capacity)]
                .into_iter()
                .try_fold(0_usize, usize::checked_add)
                .context("proof buffer size overflow")?;
            reservation.try_resize(bytes)?;
            if keys.num_rows() != 0 {
                let first = previous.as_ref().map(|last| last.as_slice().cmp(keys.row(0).as_ref()));
                let adjacent = keys.iter().zip(keys.iter().skip(1)).map(|(left, right)| left.as_ref().cmp(right.as_ref()));
                for order in first.into_iter().chain(adjacent) {
                    if order.is_eq() {
                        return Ok(None);
                    }
                    ensure!(order.is_lt(), "histogram proof source is not sorted by its complete key");
                }
                previous = Some(keys.row(keys.num_rows() - 1).as_ref().to_vec());
                count = count.checked_add(u64::try_from(keys.num_rows())?).context("histogram proof count overflow")?;
            }
            drop(keys);
            drop(batch);
            reservation.try_resize(converter.size().checked_add(previous.as_ref().map_or(0, Vec::capacity)).context("proof key size overflow")?)?;
        }
        let current = {
            let table = table_ref.read().await;
            Self::logical_count_partition_snapshot(&table, &key.project_id, &key.date)?.1
        };
        ensure!(files == current, "histogram proof partition changed during build");
        let proof = crate::tantivy::visibility::PartitionCountProof::new(root, files, schema, count)?;
        indexer.publish_count_proof(&key.table_name, &key.project_id, date, proof).await?;
        tracing::info!(project_id = key.project_id, table = key.table_name, %date, count, "histogram unique partition proof ready");
        Ok(Some(count))
    }

    pub(crate) fn histogram_dml_guard(&self, project: &str, table: &str, scope: HistogramDmlScope) -> Arc<HistogramDmlGuard> {
        self.histogram_dml.entry((project.to_owned(), table.to_owned())).or_default().enter(scope)
    }

    pub(crate) async fn histogram_plan(
        &self, plan: &datafusion::logical_expr::LogicalPlan, session: &datafusion::execution::context::SessionState,
    ) -> Result<Option<datafusion::logical_expr::LogicalPlan>> {
        use arrow::array::{ArrayRef, Int64Array, TimestampMicrosecondArray};
        use datafusion::{
            datasource::{MemTable, provider_as_source},
            logical_expr::LogicalPlanBuilder,
        };
        let Some(query) = crate::tantivy::planner::match_query(plan) else { return Ok(None) };
        let captured =
            self.capture_histogram(&query.project, &query.table, query.window, Some(&query.membership), 64 * 1024 * 1024, session.task_ctx()).await?;
        // With no usable element index, ordinary SQL can prune a narrow time
        // window without sorting daily visibility or starting a daily proof.
        let columns = query.membership.columns();
        let has_index = captured.partitions.values().try_fold(false, |found, files| -> Result<bool> {
            Ok(found
                || captured
                    .manifest
                    .histogram_entries(&captured.root, files)?
                    .iter()
                    .flatten()
                    .any(|entry| columns.iter().all(|column| entry.entry.element_fields.contains(*column))))
        })?;
        if !has_index {
            return Ok(None);
        }
        let mut proof = None;
        let result = captured
            .count_streaming_before_visibility(|day| {
                if proof.is_none() {
                    proof = captured.seed_missing_proof(self, day);
                }
            })
            .await?;
        for error in &result.index_errors {
            tracing::warn!(error = %error, "histogram used captured-row fallback");
        }
        let schema = Arc::new(query.matched.schema().as_arrow().clone());
        let counts = result.counts.values().map(|count| i64::try_from(*count)).collect::<std::result::Result<Vec<_>, _>>()?;
        let columns: Vec<ArrayRef> =
            vec![Arc::new(TimestampMicrosecondArray::from_iter_values(result.counts.keys().copied()).with_timezone("UTC")), Arc::new(Int64Array::from(counts))];
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let source = Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);
        let replacement = LogicalPlanBuilder::scan("__tantivy_histogram", provider_as_source(source), None)?.build()?;
        let replacement = crate::dml::requalified(replacement, query.matched.schema())?;
        Ok(Some(crate::dml::substitute(plan, query.matched, replacement)?))
    }

    /// Captures sources before resolving versions and counting indexed timestamps.
    /// The decoded budget bounds each daily partition, not index/cache memory or the captured memory view.
    pub async fn indexed_histogram(
        &self, project: &str, table_name: &str, window: HistogramWindow, membership: Option<&Membership>, max_decoded_bytes: usize, context: Arc<TaskContext>,
    ) -> Result<HistogramSnapshotResult> {
        self.capture_histogram(project, table_name, window, membership, max_decoded_bytes, context).await?.count().await
    }

    /// Retains the read view before index execution. All later counting uses it.
    pub async fn capture_histogram(
        &self, project: &str, table_name: &str, window: HistogramWindow, membership: Option<&Membership>, max_decoded_bytes: usize, context: Arc<TaskContext>,
    ) -> Result<CapturedHistogram> {
        let search = self.tantivy_search().context("Tantivy search service is unavailable")?.clone();
        let dml = self.histogram_dml.entry((project.to_owned(), table_name.to_owned())).or_default().clone();
        let (lo, hi) = window.bounds();
        let capture = dml.capture(lo..=hi - 1)?;
        let schema = crate::schema::get_schema(table_name).context("unknown histogram table")?;
        ensure!(schema.dedup_keys.iter().any(|key| key == "timestamp"), "histogram requires timestamp in the immutable key");
        ensure!(
            schema.partitions.iter().any(|key| key == "project_id") && schema.partitions.iter().any(|key| key == "date"),
            "histogram requires project/date partitions"
        );
        let first_date = chrono::DateTime::from_timestamp_micros(lo).context("histogram start is outside calendar range")?.date_naive().to_string();
        let last_date = chrono::DateTime::from_timestamp_micros(hi - 1).context("histogram end is outside calendar range")?.date_naive().to_string();
        // Flush removes memory only after publishing Delta. Capture memory first
        // and retain its authority ranges, including ranges emptied by DELETE.
        let memory = self.buffered_layer().map(|layer| layer.snapshot_for_merge(project, table_name, lo, hi)).transpose()?.unwrap_or_default();
        let table = self.resolve_table(project, table_name).await?;
        let (files, log_store) = {
            let table = table.read().await;
            let mut files = Vec::new();
            for view in table.snapshot()?.log_data().iter() {
                let file = SnapshotFile::capture(&view, &schema.partitions)?;
                let partition = file.partition_values.get("project_id").and_then(Option::as_deref).context("histogram file has no project partition")?;
                let date = file.partition_values.get("date").and_then(Option::as_deref).context("histogram file has no date partition")?;
                if partition == project && date >= first_date.as_str() && date <= last_date.as_str() {
                    files.push(file);
                }
            }
            (files, table.log_store())
        };
        // All subsequent source reads use pinned immutable file metadata and
        // retained Arrow batches. Only the capture interval needs this fence.
        capture.validate()?;
        drop(capture);
        let mut columns = schema
            .dedup_keys
            .iter()
            .map(String::as_str)
            .chain(schema.dedup_tiebreak.as_deref())
            .chain(schema.tombstone_column.as_deref())
            .collect::<std::collections::BTreeSet<_>>();
        if let Some(predicate) = membership {
            columns.extend(predicate.columns());
        }
        let full_schema = schema.schema_ref();
        let projection = columns.into_iter().map(|column| full_schema.index_of(column)).collect::<std::result::Result<Vec<_>, _>>()?;
        let projected = Arc::new(full_schema.project(&projection)?);
        let mut partitions: std::collections::BTreeMap<i64, Vec<SnapshotFile>> = Default::default();
        for file in files {
            let date =
                file.partition_values.get("date").and_then(Option::as_deref).context("missing histogram partition date")?.parse::<chrono::NaiveDate>()?;
            let day = date.signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
            partitions.entry(day).or_default().push(file);
        }
        for batch in &memory.batches {
            for &timestamp in histogram_timestamps(batch)? {
                if timestamp >= lo && timestamp < hi {
                    partitions.entry(timestamp.div_euclid(DAY_MICROS)).or_default();
                }
            }
        }
        // Preserve an empty result through the same execution and diagnostics path.
        if partitions.is_empty() {
            partitions.entry(lo.div_euclid(DAY_MICROS)).or_default();
        }
        let manifest = search.load_manifest_cached(table_name, project).await?;
        let mut logical_counts = std::collections::BTreeMap::new();
        for (&day, files) in &partitions {
            let current: crate::read::CountFiles = files.iter().map(|file| (file.path.clone(), file.deletion_vector.clone())).collect();
            let Some(date) = files.first().and_then(|file| file.partition_values.get("date")).and_then(Option::as_deref) else { continue };
            if let Some((index, added)) = self.logical_count_memory_for_files(project, table_name, date, &current)
                && added.is_empty()
                && let Some(lo) = day.checked_mul(DAY_MICROS)
                && let Some(hi) = lo.checked_add(DAY_MICROS)
            {
                logical_counts.insert(day, index.count(lo, hi));
            } else if let Some(proof) = manifest.count_proofs.get(&date.parse::<chrono::NaiveDate>()?)
                && proof.matches(log_store.root_url(), &current, schema)
            {
                logical_counts.insert(day, proof.logical_count);
            }
        }
        Ok(CapturedHistogram {
            search,
            table: table_name.into(),
            project: project.into(),
            root: log_store.root_url().clone(),
            window,
            membership: membership.cloned(),
            partitions,
            logical_counts,
            manifest,
            memory,
            log_store,
            projected,
            keys: schema.dedup_keys.clone(),
            tiebreak: schema.dedup_tiebreak.clone(),
            tombstone: schema.tombstone_column.clone(),
            cache: self.histogram_delta.clone(),
            max_decoded_bytes,
            context,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tantivy_unique_partition_uses_exact_count_proof_and_declines_new_versions() -> Result<()> {
        use crate::support::test_helpers::{json_to_batch_for, minio_test_config};
        use crate::tantivy::search::{TantivyIndexService, parquet_rel_of_uri};
        let dir = tempfile::tempdir()?;
        let project = format!("p-{}", uuid::Uuid::new_v4());
        let config = minio_test_config(&project, &dir.path().to_string_lossy());
        let store = Arc::new(object_store::memory::InMemory::new());
        let search = Arc::new(TantivySearchService::new(store.clone(), dir.path().join("indexes"), Arc::new(config.tantivy.clone())));
        let indexer = Arc::new(TantivyIndexService::new(store.clone(), Arc::new(config.tantivy.clone())));
        indexer.with_reader(&search);
        let db = super::super::Database::with_config(config.clone()).await?.with_tantivy_search(search.clone()).with_tantivy_indexer(indexer.clone());
        let table = "mor_versioned";
        let timestamp = chrono::Utc::now().timestamp_micros() - DAY_MICROS;
        let date = chrono::DateTime::from_timestamp_micros(timestamp).unwrap().date_naive().to_string();
        let hash = "a".repeat(2048);
        let row =
            |id: &str, tag: &str| serde_json::json!({"project_id": project, "timestamp": timestamp, "date": date, "id": id, "name": tag, "hashes": [tag]});
        let records = (0..4).map(|id| row(&id.to_string(), &hash)).collect();
        db.insert_records_batch(&project, table, vec![json_to_batch_for(table, records)?], true, None).await?;
        let build_indexes = || async {
            let table_ref = db.resolve_table(&project, table).await?;
            let store = table_ref.read().await.log_store().object_store(None);
            for uri in db.list_file_uris(&project, table).await? {
                indexer.build_index_for_file(table, &project, parquet_rel_of_uri(&uri).context("missing relative path")?, &uri, store.clone()).await?;
            }
            Ok::<_, anyhow::Error>(())
        };
        build_indexes().await?;
        let table_ref = db.resolve_table(&project, table).await?;
        let (original_add, original_file, log_store) = {
            let table = table_ref.read().await;
            let log_store = table.log_store();
            let commit = log_store.read_commit_entry(table.version().context("missing version")?).await?.context("missing commit")?;
            let add = commit
                .split(|&byte| byte == b'\n')
                .filter(|line| !line.is_empty())
                .map(serde_json::from_slice::<deltalake::kernel::Action>)
                .collect::<std::result::Result<Vec<_>, _>>()?
                .into_iter()
                .find_map(|action| match action {
                    deltalake::kernel::Action::Add(add) => Some(add),
                    _ => None,
                })
                .context("missing Add")?;
            let file = SnapshotFile::capture(&table.snapshot()?.log_data().iter().next().context("missing file")?, &["project_id".into(), "date".into()])?;
            (add, file, log_store)
        };
        let ids = read_file_rows(
            log_store.clone(),
            &original_file,
            Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Utf8, false)])),
            1024 * 1024,
        )
        .await?;
        let old_ordinal = ids
            .batches
            .iter()
            .flat_map(|batch| batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap().iter())
            .position(|id| id == Some("0"))
            .context("missing old row")?;
        let window = HistogramWindow::new(timestamp, timestamp + 1, 1_000_000, 0, 2)?;
        let membership = Membership::Contains { column: "hashes".into(), value: hash.clone() };
        let context = Arc::new(TaskContext::default());
        assert!(
            db.indexed_histogram(&project, table, window, Some(&membership), 64, context.clone()).await.is_err(),
            "index coverage alone cannot prove visibility"
        );
        let absent = Membership::Contains { column: "hashes".into(), value: "absent".into() };
        let mut empty = db.capture_histogram(&project, table, window, Some(&absent), 64, context.clone()).await?;
        for result in [empty.count().await, empty.count_streaming().await] {
            let result = result?;
            assert!(result.counts.is_empty());
            assert_eq!(result.scanned_sources, 0, "complete empty postings must avoid decoding visibility rows");
            assert_eq!(result.indexed_sources, 1);
        }
        assert_eq!(search.stats.histogram_unique_partitions.load(Ordering::Relaxed), 0, "absence is not a uniqueness proof");
        let mut session = Arc::new(db.clone()).create_session_context();
        db.setup_session_context(&mut session)?;
        let sql = format!(
            "SELECT time_bucket('1 second', timestamp), count(*) FROM {table} \
             WHERE project_id = '{project}' AND timestamp >= to_timestamp_micros({timestamp}) \
             AND timestamp < to_timestamp_micros({}) AND hashes @> ARRAY['absent'] GROUP BY 1",
            timestamp + 1
        );
        let unrelated_window = datafusion::prelude::col("timestamp")
            .gt_eq(datafusion::prelude::lit(datafusion::common::ScalarValue::TimestampMicrosecond(Some(timestamp + DAY_MICROS), None)))
            .and(
                datafusion::prelude::col("timestamp")
                    .lt(datafusion::prelude::lit(datafusion::common::ScalarValue::TimestampMicrosecond(Some(timestamp + DAY_MICROS + 1), None))),
            );
        let unrelated_update = db.histogram_dml_guard(&project, table, crate::dml::histogram_dml_scope(Some(&unrelated_window), &[], None));
        let before = search.stats.histogram_snapshots.load(Ordering::Relaxed);
        assert_eq!(session.sql(&sql).await?.collect().await?.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        assert!(search.stats.histogram_snapshots.load(Ordering::Relaxed) > before, "SQL must use the indexed histogram");
        assert!(db.histogram_proof_build.attempts.lock().is_empty(), "indexed absence must not schedule a daily visibility scan");
        drop(unrelated_update);
        empty.memory.batches.push(json_to_batch_for(table, vec![row("memory", "absent")])?);
        empty.max_decoded_bytes = 1024 * 1024;
        assert_eq!(empty.count_streaming().await?.counts.values().sum::<u64>(), 1, "a matching memory row must defeat indexed absence");
        empty.memory.batches.clear();
        empty.max_decoded_bytes = 64;
        Arc::make_mut(&mut empty.manifest).entries.clear();
        assert!(empty.count_streaming().await.is_err(), "missing index coverage cannot certify absence");
        let partition = crate::read::CountPartition { project_id: project.clone(), table_name: table.into(), date: date.clone() };
        let unproven = db.capture_histogram(&project, table, window, Some(&membership), 64, context.clone()).await?;
        let busy = db.histogram_proof_build.slot.clone().acquire_owned().await?;
        assert!(unproven.seed_missing_proof(&db, timestamp.div_euclid(DAY_MICROS)).is_none(), "busy proof builder must decline without queuing");
        drop(busy);
        let build = unproven.seed_missing_proof(&db, timestamp.div_euclid(DAY_MICROS)).context("completed partition must schedule a proof")?;
        assert!(unproven.seed_missing_proof(&db, timestamp.div_euclid(DAY_MICROS)).is_none(), "one query-triggered proof build holds the admission slot");
        build.await?;
        assert!(unproven.seed_missing_proof(&db, timestamp.div_euclid(DAY_MICROS)).is_none(), "a recent attempt must not rebuild from a stale query capture");
        assert_eq!(db.logical_count_cache.stats().0, 0, "proof seeding must not retain the complete winner index");
        let captured = db.capture_histogram(&project, table, window, Some(&membership), 64, context.clone()).await?;
        let result = captured.count().await?;
        assert_eq!(result.counts.values().sum::<u64>(), 4);
        assert_eq!(result.scanned_sources, 0, "unique partition needs no row fallback");
        assert_eq!(search.stats.histogram_unique_partitions.load(Ordering::Relaxed), 1);
        db.logical_count_cache.invalidate(&partition);
        search.invalidate_manifest(table, &project);
        let persisted = db.indexed_histogram(&project, table, window, Some(&membership), 64, context.clone()).await?;
        assert_eq!(persisted.counts, result.counts, "persisted proof survives winner-cache eviction and manifest reload");
        assert_eq!(persisted.scanned_sources, 0);
        db.insert_records_batch(&project, table, vec![json_to_batch_for(table, vec![row("0", "b")])?], true, None).await?;
        build_indexes().await?;
        assert_eq!(captured.count().await?.counts.values().sum::<u64>(), 4, "the retained proof belongs to the old file set");
        assert!(db.indexed_histogram(&project, table, window, Some(&membership), 64, context.clone()).await.is_err(), "new files invalidate the old proof");
        assert_eq!(db.build_histogram_count_proof(&partition).await?, None, "duplicate physical keys cannot receive a uniqueness proof");
        db.build_logical_count_partition(&partition, true).await?;
        assert!(
            db.indexed_histogram(&project, table, window, Some(&membership), 64, context.clone()).await.is_err(),
            "logical count smaller than physical count cannot authorize index-only counting"
        );
        assert_eq!(db.indexed_histogram(&project, table, window, Some(&membership), 1024 * 1024, context.clone()).await?.counts.values().sum::<u64>(), 3);
        // One dirty day's hash arrays exceed the former 64 MiB decoded limit.
        let wide_timestamp = timestamp - 3 * DAY_MICROS;
        let wide_date = chrono::DateTime::from_timestamp_micros(wide_timestamp).unwrap().date_naive().to_string();
        let wide_row = |id: usize, tag: &str| {
            let mut record = row(&id.to_string(), tag);
            record["timestamp"] = serde_json::json!(wide_timestamp);
            record["date"] = serde_json::json!(wide_date);
            record
        };
        let records = (0..65)
            .map(|id| {
                let mut record = wide_row(id, &hash);
                record["hashes"] = serde_json::json!(vec![hash.as_str(); 512]);
                record
            })
            .collect();
        db.insert_records_batch(&project, table, vec![json_to_batch_for(table, records)?], true, None).await?;
        db.insert_records_batch(&project, table, vec![json_to_batch_for(table, vec![wide_row(0, "b")])?], true, None).await?;
        build_indexes().await?;
        let wide_window = HistogramWindow::new(wide_timestamp, wide_timestamp + 1, 1_000_000, 0, 2)?;
        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
            Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(32 * 1024 * 1024));
        let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .with_disk_manager_builder(
                datafusion::execution::disk_manager::DiskManagerBuilder::default().with_mode(datafusion::execution::disk_manager::DiskManagerMode::Disabled),
            )
            .build_arc()?;
        let wide_context = datafusion::prelude::SessionContext::new_with_config_rt(Default::default(), runtime).task_ctx();
        let wide = db.capture_histogram(&project, table, wide_window, Some(&membership), 64 * 1024 * 1024, wide_context.clone()).await?;
        let Err(error) = wide.count().await else { anyhow::bail!("wide hashes must exceed the old decoded budget") };
        assert!(error.to_string().contains("budget"), "the old path must fail at its decoded budget: {error}");
        let result = wide.count_streaming().await?;
        assert_eq!(result.counts.values().sum::<u64>(), 64);
        assert_eq!(result.scanned_sources, 0, "wide hashes stay in the index during version resolution");
        db.insert_records_batch(&project, table, vec![json_to_batch_for(table, vec![wide_row(1, "b")])?], true, None).await?;
        let mut partial = db.capture_histogram(&project, table, wide_window, Some(&membership), 64 * 1024 * 1024, wide_context.clone()).await?;
        let result = partial.count_streaming().await?;
        assert_eq!(result.counts.values().sum::<u64>(), 63, "an unindexed replacement must suppress its indexed old version");
        assert_eq!(result.scanned_sources, 1);
        let cache_hits = search.stats.histogram_delta_cache_hits.load(Ordering::Relaxed);
        let prepares = search.stats.histogram_parquet_prepares.load(Ordering::Relaxed);
        assert_eq!(partial.count_streaming().await?.counts, result.counts, "repeated partial histograms preserve exact buckets");
        assert_eq!(search.stats.histogram_parquet_prepares.load(Ordering::Relaxed) - prepares, 1, "only the unindexed replacement needs Parquet metadata");
        assert!(
            search.stats.histogram_delta_cache_hits.load(Ordering::Relaxed) > cache_hits,
            "repeated streaming histograms must reuse captured Delta visibility"
        );
        let saved_memory = std::mem::take(&mut partial.memory);
        for (id, tag, deleted, expected) in [(66, hash.as_str(), false, 64), (2, "b", false, 62), (2, hash.as_str(), true, 62)] {
            let mut record = wide_row(id, tag);
            record["updated_at"] = serde_json::json!(chrono::Utc::now().timestamp_micros() + DAY_MICROS);
            record["deleted"] = serde_json::json!(deleted);
            partial.memory.batches = vec![json_to_batch_for(table, vec![record])?];
            let before = search.stats.histogram_delta_cache_hits.load(Ordering::Relaxed);
            assert_eq!(partial.count_streaming().await?.counts.values().sum::<u64>(), expected, "fresh memory insert/update/delete overrides cached Delta");
            assert_eq!(search.stats.histogram_delta_cache_hits.load(Ordering::Relaxed), before, "overlapping memory must decline cached masks");
        }
        partial.memory.batches.clear();
        partial.memory.covered_ranges = vec![(wide_timestamp, wide_timestamp + 1)];
        assert!(partial.count_streaming().await?.counts.is_empty(), "covered memory ranges suppress Delta even without surviving memory rows");
        partial.memory.covered_ranges.clear();
        for rows in 1..=2 {
            let mut record = wide_row(66, &hash);
            record["timestamp"] = serde_json::json!(wide_timestamp + DAY_MICROS);
            partial.memory.batches = vec![json_to_batch_for(table, vec![record; rows])?];
            let before = search.stats.histogram_delta_cache_hits.load(Ordering::Relaxed);
            assert_eq!(partial.count_streaming().await?.counts, result.counts, "unrelated memory has a fresh correctly sized mask");
            assert!(search.stats.histogram_delta_cache_hits.load(Ordering::Relaxed) > before);
        }
        partial.memory = saved_memory;
        assert_eq!(wide.count_streaming().await?.counts.values().sum::<u64>(), 64, "later writes cannot replace captured sources");
        assert!(pool.reserved() > 0, "resident streamed masks must remain charged after execution");
        drop(wide.cache.reserve(32 * 1024 * 1024, &pool)?);
        assert_eq!(pool.reserved(), 0, "pressure must release unpinned cached masks");
        // Irrelevant keys in the same day must not consume the query's sort
        // budget. Keep a partial index and the same one-microsecond window.
        let outside_timestamp = wide_timestamp.div_euclid(DAY_MICROS) * DAY_MICROS + (wide_timestamp.rem_euclid(DAY_MICROS) + 1) % DAY_MICROS;
        let records = (0..20_000)
            .map(|id| {
                let mut record = wide_row(id, "outside");
                record["id"] = serde_json::json!(format!("outside-{id}-{}", "x".repeat(2048)));
                record["timestamp"] = serde_json::json!(outside_timestamp);
                record
            })
            .collect();
        db.insert_records_batch(&project, table, vec![json_to_batch_for(table, records)?], true, None).await?;
        let narrow = db.capture_histogram(&project, table, wide_window, Some(&membership), 64 * 1024 * 1024, wide_context).await?;
        assert_eq!(narrow.count_streaming().await?.counts.values().sum::<u64>(), 63, "out-of-window keys must not force a daily sort or spill");
        drop(narrow.cache.reserve(32 * 1024 * 1024, &pool)?);
        assert_eq!(pool.reserved(), 0, "narrow-window cache reservations release after eviction");
        // Removing the superseded physical row makes the partition unique.
        // The same Parquet paths now have a different DV identity.
        let mut table_guard = table_ref.write().await;
        let actions = deltalake::operations::deletion_vectors::write_deletion_vectors(
            log_store.as_ref(),
            log_store.root_url(),
            vec![deltalake::operations::deletion_vectors::FileDeletion { add: original_add, deleted_indexes: vec![u64::try_from(old_ordinal)?] }],
        )
        .await?;
        let committed = deltalake::kernel::transaction::CommitBuilder::default()
            .with_actions(actions)
            .build(Some(table_guard.snapshot()?), log_store, deltalake::protocol::DeltaOperation::Delete { predicate: None })
            .await?;
        table_guard.state = Some(committed.snapshot().clone());
        drop(table_guard);
        assert!(
            db.indexed_histogram(&project, table, window, Some(&membership), 64, context.clone()).await.is_err(),
            "a same-path DV change invalidates the cached proof"
        );
        assert_eq!(db.build_histogram_count_proof(&partition).await?, Some(4));
        let result = db.indexed_histogram(&project, table, window, Some(&membership), 64, context.clone()).await?;
        assert_eq!(result.counts.values().sum::<u64>(), 3);
        assert_eq!(result.scanned_sources, 0, "a newly proven unique partition counts through its pinned DV mask");
        assert_eq!(captured.count().await?.counts.values().sum::<u64>(), 4, "old capture retains pre-DV visibility");
        let restart_dir = tempfile::tempdir()?;
        let mut restart_config = (*config).clone();
        restart_config.core.timefusion_data_dir = restart_dir.path().into();
        let restart_search = Arc::new(TantivySearchService::new(store, restart_dir.path().join("indexes"), Arc::new(config.tantivy.clone())));
        let mut restarted =
            super::super::Database::with_config(Arc::new(restart_config)).await?.with_tantivy_search(restart_search).with_tantivy_indexer(indexer);
        restarted.logical_count_cache = Arc::new(crate::read::LogicalCountCache::new(restart_dir.path().join("counts"), 0));
        let result = restarted.indexed_histogram(&project, table, window, Some(&membership), 64, context).await?;
        assert_eq!(result.counts.values().sum::<u64>(), 3, "cold database and index reader recover the exact persisted DV proof");
        assert_eq!(result.scanned_sources, 0);
        assert_eq!(restarted.build_histogram_count_proof(&partition).await?, Some(4), "uniqueness proof needs no winner-cache capacity");
        let error = restarted.build_logical_count_partition(&partition, true).await.unwrap_err();
        assert!(error.to_string().contains("resident cache budget"), "the same input must exceed the disabled winner cache: {error}");
        let mut tombstone = row("deleted-row", "b");
        tombstone["deleted"] = serde_json::json!(true);
        db.insert_records_batch(&project, table, vec![json_to_batch_for(table, vec![tombstone])?], true, None).await?;
        assert_eq!(db.build_histogram_count_proof(&partition).await?, None, "physical tombstones cannot be certified as live rows");
        let batch_rows = super::super::build_optimize_session_state(1, db.maintenance_runtime_env()).config().options().execution.batch_size;
        let dense_timestamp = timestamp - DAY_MICROS;
        let dense_date = chrono::DateTime::from_timestamp_micros(dense_timestamp).unwrap().date_naive().to_string();
        let dense_row = |id: usize| serde_json::json!({"project_id": project, "timestamp": dense_timestamp, "date": dense_date, "id": format!("{id:08}")});
        let rows = 2 * batch_rows + 1;
        db.insert_records_batch(&project, table, vec![json_to_batch_for(table, (0..rows).map(dense_row).collect())?], true, None).await?;
        let dense_partition = crate::read::CountPartition { date: dense_date.clone(), ..partition.clone() };
        assert_eq!(db.build_histogram_count_proof(&dense_partition).await?, Some(u64::try_from(rows)?), "unique keys span several output batches");
        db.insert_records_batch(&project, table, vec![json_to_batch_for(table, vec![dense_row(batch_rows - 1)])?], true, None).await?;
        assert_eq!(db.build_histogram_count_proof(&dense_partition).await?, None, "duplicate keys at a sorted batch boundary must decline");
        Ok(())
    }

    #[test]
    fn delta_cache_checks_exact_identity_and_retains_evicted_reservations() -> Result<()> {
        use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
        let cache = HistogramDeltaCache::default();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256 * 1024 * 1024));
        let key = DeltaCacheKey {
            root: url::Url::parse("memory:///table")?,
            files: vec![SnapshotFile {
                path: "file.parquet".into(),
                size: 1,
                partition_values: [("project".into(), Some("p".into()))].into(),
                deletion_vector: None,
            }],
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            keys: vec!["timestamp".into()],
            tiebreak: None,
        };
        let entry = |key| -> Result<Arc<CachedDelta>> {
            let reservation = MemoryConsumer::new("cache-test").register(&pool);
            reservation.try_grow(HistogramDeltaCache::MAX_RESIDENT_BYTES)?;
            Ok(Arc::new(CachedDelta { key, sources: vec![], decoded_bytes: 0, reservation }))
        };
        cache.insert(entry(key.clone())?);
        let pinned = cache.get(&key).expect("same snapshot must hit");
        for field in 0..8 {
            let mut changed = key.clone();
            match field {
                0 => changed.root = url::Url::parse("memory:///other")?,
                1 => changed.files[0].size += 1,
                2 => changed.files[0].path.push('2'),
                3 => changed.files[0].partition_values.insert("project".into(), Some("other".into())).map(|_| ()).unwrap(),
                4 => changed.keys.push("id".into()),
                5 => changed.tiebreak = Some("updated_at".into()),
                6 => {
                    changed.files[0].deletion_vector = Some(deltalake::kernel::DeletionVectorDescriptor {
                        storage_type: deltalake::kernel::StorageType::UuidRelativePath,
                        path_or_inline_dv: "dv".into(),
                        offset: Some(1),
                        size_in_bytes: 10,
                        cardinality: 1,
                    })
                }
                _ => changed.schema = Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Utf8, true)])),
            }
            assert!(cache.get(&changed).is_none(), "changed identity field {field} must miss");
        }
        let mut next = key.clone();
        next.files[0].size += 1;
        cache.insert(entry(next.clone())?);
        assert!(cache.get(&key).is_none(), "resident budget must evict the old entry");
        assert_eq!(pool.reserved(), 2 * HistogramDeltaCache::MAX_RESIDENT_BYTES, "captured queries retain reservations after eviction");
        drop(pinned);
        assert_eq!(pool.reserved(), HistogramDeltaCache::MAX_RESIDENT_BYTES);
        let pressure = cache.reserve(256 * 1024 * 1024, &pool)?;
        assert!(cache.get(&next).is_none(), "memory pressure must evict unpinned cache ownership");
        drop(pressure);
        drop(cache);
        assert_eq!(pool.reserved(), 0);
        let cache = HistogramDeltaCache::default();
        let visibility = VisibilityCacheKey { delta: key, bounds: 0..2, tombstone: None };
        let masks = Arc::new(CapturedDeltaMasks { masks: vec![], reservation: cache.reserve(64, &pool)? });
        cache.insert_entry(HistogramCacheEntry::Visibility { key: Box::new(visibility.clone()), masks: masks.clone() });
        assert!(Arc::ptr_eq(&cache.visibility(&visibility).context("same visibility identity must hit")?, &masks));
        for field in 0..4 {
            let mut changed = visibility.clone();
            match field {
                0 => changed.bounds.start += 1,
                1 => changed.bounds.end += 1,
                2 => changed.tombstone = Some("deleted".into()),
                _ => {
                    changed.delta.files[0].deletion_vector = Some(deltalake::kernel::DeletionVectorDescriptor {
                        storage_type: deltalake::kernel::StorageType::UuidRelativePath,
                        path_or_inline_dv: "new-dv".into(),
                        offset: Some(0),
                        size_in_bytes: 8,
                        cardinality: 1,
                    })
                }
            }
            assert!(cache.visibility(&changed).is_none(), "visibility identity field {field} must invalidate reuse");
        }
        drop(cache);
        assert_eq!(pool.reserved(), 64, "captured visibility retains its pool ownership after eviction");
        drop(masks);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[test]
    fn histogram_capture_detects_overlapping_dml_and_guard_cancellation() {
        let state = super::HistogramDmlState::default();
        let before = state.capture(0..=9).unwrap();
        let unrelated = state.enter(HistogramDmlScope::Timestamps(10..=19));
        before.validate().unwrap();
        state.capture(0..=9).unwrap().validate().unwrap();
        let first = state.enter(HistogramDmlScope::Timestamps(9..=10));
        let second = state.enter(HistogramDmlScope::All);
        let queued = second.clone();
        assert!(state.capture(0..=9).is_err());
        drop(first);
        assert!(before.validate().is_err(), "another DML remains active");
        drop(second);
        assert!(state.capture(0..=9).is_err(), "queued work must remain fenced after the statement returns");
        drop(queued);
        assert!(before.validate().is_err(), "completed writes must invalidate an earlier capture");
        state.capture(0..=9).unwrap().validate().unwrap();
        drop(unrelated);
        drop(before);
        let fresh = state.capture(10..=19).unwrap();
        let overlapping = state.enter(HistogramDmlScope::Timestamps(19..=20));
        drop(overlapping);
        assert!(fresh.validate().is_err(), "even a completed overlap must invalidate capture");
    }
}
