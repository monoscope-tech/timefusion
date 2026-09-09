use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

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

/// Small resident LRU. Eviction releases its ownership, while captured queries
/// retain the entry and its memory-pool reservation until execution finishes.
#[derive(Debug, Default)]
pub(super) struct HistogramDeltaCache {
    entries: parking_lot::Mutex<std::collections::VecDeque<Arc<CachedDelta>>>,
}

impl HistogramDeltaCache {
    const MAX_RESIDENT_BYTES: usize = 64 * 1024 * 1024;

    fn get(&self, key: &DeltaCacheKey) -> Option<Arc<CachedDelta>> {
        let mut entries = self.entries.lock();
        let position = entries.iter().position(|entry| entry.key == *key)?;
        let entry = entries.remove(position)?;
        entries.push_back(entry.clone());
        Some(entry)
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
        let size = entry.reservation.size();
        if size > Self::MAX_RESIDENT_BYTES {
            return;
        }
        let mut entries = self.entries.lock();
        // Concurrent builders can finish the same snapshot. Keep only one resident copy.
        entries.retain(|existing| existing.key != entry.key);
        let mut resident: usize = entries.iter().map(|entry| entry.reservation.size()).sum();
        while resident > Self::MAX_RESIDENT_BYTES - size || entries.len() >= 32 {
            let Some(oldest) = entries.pop_front() else { break };
            resident -= oldest.reservation.size();
        }
        entries.push_back(entry);
    }
}

#[derive(Debug, Default)]
pub(super) struct HistogramDmlState {
    generation: AtomicU64,
    active: AtomicUsize,
}

pub(crate) struct HistogramDmlGuard(Arc<HistogramDmlState>);

impl Drop for HistogramDmlGuard {
    fn drop(&mut self) {
        self.0.generation.fetch_add(1, Ordering::SeqCst);
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl HistogramDmlState {
    fn enter(self: Arc<Self>) -> HistogramDmlGuard {
        self.active.fetch_add(1, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
        HistogramDmlGuard(self)
    }

    fn stamp(&self) -> Result<u64> {
        ensure!(self.active.load(Ordering::SeqCst) == 0, "histogram capture overlaps active SQL DML");
        Ok(self.generation.load(Ordering::SeqCst))
    }

    fn validate(&self, stamp: u64) -> Result<()> {
        ensure!(self.stamp()? == stamp, "SQL DML changed while capturing histogram sources");
        Ok(())
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
    fn seed_missing_proof(&self, database: &super::Database) -> Option<tokio::task::JoinHandle<()>> {
        database.tantivy_indexer()?;
        let state = &database.histogram_proof_build;
        let permit = state.slot.clone().try_acquire_owned().ok()?;
        let today = chrono::Utc::now().timestamp_micros().div_euclid(DAY_MICROS);
        let key = {
            let now = std::time::Instant::now();
            let mut attempts = state.attempts.lock();
            attempts.retain(|(_, at)| now.duration_since(*at) < std::time::Duration::from_secs(60));
            let key = self
                .partitions
                .iter()
                .rev()
                .filter(|(day, files)| **day < today && !files.is_empty() && !self.logical_counts.contains_key(*day))
                .filter_map(|(_, files)| {
                    Some(crate::read::CountPartition {
                        project_id: self.project.clone(),
                        table_name: self.table.clone(),
                        date: files.first()?.partition_values.get("date")?.clone()?,
                    })
                })
                .find(|key| !attempts.iter().any(|(attempt, _)| attempt == key))?;
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
        let mut total = HistogramSnapshotResult { counts: Default::default(), indexed_sources: 0, scanned_sources: 0, index_errors: Vec::new() };
        for (&day, files) in &self.partitions {
            let part = self.count_partition(day, files).await?;
            for (bucket, count) in part.counts {
                let value = total.counts.entry(bucket).or_default();
                *value = value.checked_add(count).context("histogram partition count overflow")?;
            }
            total.indexed_sources += part.indexed_sources;
            total.scanned_sources += part.scanned_sources;
            total.index_errors.extend(part.index_errors);
        }
        Ok(total)
    }

    /// Counts with streamed visibility columns and the query memory pool.
    /// Unlike `count`, this limits each decoded batch rather than total decoded data.
    pub async fn count_streaming(&self) -> Result<HistogramSnapshotResult> {
        let mut total = HistogramSnapshotResult { counts: Default::default(), indexed_sources: 0, scanned_sources: 0, index_errors: Vec::new() };
        for (&day, files) in &self.partitions {
            let part = self.count_streaming_partition(day, files).await?;
            for (bucket, count) in part.counts {
                let value = total.counts.entry(bucket).or_default();
                *value = value.checked_add(count).context("streaming histogram count overflow")?;
            }
            total.indexed_sources += part.indexed_sources;
            total.scanned_sources += part.scanned_sources;
            total.index_errors.extend(part.index_errors);
        }
        Ok(total)
    }

    async fn count_streaming_partition(&self, day: i64, files: &[SnapshotFile]) -> Result<HistogramSnapshotResult> {
        use crate::tantivy::visibility::{FileVisibilitySource, PreparedFileRows, VisibilityFiles, lineage_batch, lineage_schema, stream_winner_masks};
        use datafusion::physical_plan::{streaming::StreamingTableExec, union::UnionExec};
        use futures::TryStreamExt;

        match self.count_unique_partition(day, files).await {
            Ok(Some(result)) => return Ok(result),
            Ok(None) => {}
            Err(error) => tracing::warn!(%error, "indexed histogram declined; streaming captured visibility"),
        }
        let lo = day.checked_mul(DAY_MICROS).context("histogram date overflow")?;
        let hi = lo.checked_add(DAY_MICROS).context("histogram date overflow")?;
        let columns =
            self.keys.iter().map(String::as_str).chain(self.tiebreak.as_deref()).chain(self.tombstone.as_deref()).collect::<std::collections::BTreeSet<_>>();
        let projection = columns.into_iter().map(|name| self.projected.index_of(name)).collect::<std::result::Result<Vec<_>, _>>()?;
        let narrow = Arc::new(self.projected.project(&projection)?);
        let schema = lineage_schema(&narrow)?;
        let ranges = Arc::new(self.memory.covered_ranges.clone());
        let mut prepared = Vec::new();
        let mut owners = Vec::new();
        let mut partitions = Vec::new();
        let mut source_rows = Vec::new();
        for (source, file) in files.iter().enumerate() {
            let file = Arc::new(PreparedFileRows::open(self.log_store.clone(), file).await?);
            let owner = Arc::new(self.cache.reserve(file.retained_bytes()?, self.context.memory_pool())?);
            source_rows.push(file.live().len());
            partitions.push(FileVisibilitySource {
                file: file.clone(),
                schema: schema.clone(),
                source: u32::try_from(source)?,
                covered_ranges: ranges.clone(),
                partition: (lo, hi),
                max_batch_bytes: self.max_decoded_bytes,
                owner: owner.clone(),
            });
            prepared.push(file);
            owners.push(owner);
        }
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
            memory.push(lineage_batch(&batch, &live, schema.clone(), u32::try_from(files.len())?, offset)?);
            offset = offset.checked_add(batch.num_rows()).context("memory visibility row overflow")?;
        }
        source_rows.push(offset);
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
        let masks = stream_winner_masks(input, source_rows, &self.keys, self.tiebreak.as_deref(), self.tombstone.as_deref(), self.context.clone()).await?;
        let mask_bytes = masks.iter().map(|mask| mask.inner().capacity()).try_fold(0_usize, usize::checked_add).context("histogram mask size overflow")?;
        let _mask_owner = self.cache.reserve(mask_bytes, self.context.memory_pool())?;
        let entries = self.manifest.histogram_entries(&self.root, files)?;
        let mut result = HistogramSnapshotResult { counts: Default::default(), indexed_sources: 0, scanned_sources: 0, index_errors: Vec::new() };
        for (index, file) in prepared.iter().enumerate() {
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
                                source_file: &files[index].path,
                                visible: masks[index].clone(),
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
                    let mut counts = std::collections::BTreeMap::<i64, u64>::new();
                    let mut rows = file.stream(self.projected.clone())?;
                    let mut offset = 0;
                    while let Some(batch) = rows.try_next().await? {
                        let bytes = batch.get_array_memory_size();
                        ensure!(bytes <= self.max_decoded_bytes, "histogram fallback batch exceeds decoded budget");
                        let _owner = self.cache.reserve(bytes, self.context.memory_pool())?;
                        let visible = masks[index].slice(offset, batch.num_rows());
                        for (bucket, count) in self.window.count_rows(std::slice::from_ref(&batch), &visible, self.membership.as_ref())? {
                            let total = counts.entry(bucket).or_default();
                            *total = total.checked_add(count).context("streaming fallback count overflow")?;
                        }
                        offset += batch.num_rows();
                    }
                    counts
                }
            };
            for (bucket, count) in counts {
                let total = result.counts.entry(bucket).or_default();
                *total = total.checked_add(count).context("streaming indexed count overflow")?;
            }
        }
        for (bucket, count) in self.window.count_rows(&self.memory.batches, masks.last().context("missing memory winner mask")?, self.membership.as_ref())? {
            let total = result.counts.entry(bucket).or_default();
            *total = total.checked_add(count).context("streaming memory count overflow")?;
        }
        if !self.memory.batches.is_empty() {
            result.scanned_sources += 1;
        }
        TantivySearchService::record_histogram_snapshot(&self.search.stats);
        Ok(result)
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
        let mut result = HistogramSnapshotResult { counts: Default::default(), indexed_sources: 0, scanned_sources: 0, index_errors: Vec::new() };
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
            for (bucket, count) in counts {
                let total = result.counts.entry(bucket).or_default();
                *total = total.checked_add(count).context("histogram unique partition overflow")?;
            }
            result.indexed_sources += 1;
        }
        TantivySearchService::record_histogram_snapshot(&self.search.stats);
        self.search.stats.histogram_unique_partitions.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("timefusion_tantivy_histogram_unique_partitions_total").increment(1);
        Ok(Some(result))
    }

    async fn count_partition(&self, day: i64, files: &[SnapshotFile]) -> Result<HistogramSnapshotResult> {
        match self.count_unique_partition(day, files).await {
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

    pub(crate) fn histogram_dml_guard(&self, project: &str, table: &str) -> HistogramDmlGuard {
        self.histogram_dml.entry((project.to_owned(), table.to_owned())).or_default().clone().enter()
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
        drop(captured.seed_missing_proof(self));
        let result = captured.count_streaming().await?;
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
        let stamp = dml.stamp()?;
        let schema = crate::schema::get_schema(table_name).context("unknown histogram table")?;
        ensure!(schema.dedup_keys.iter().any(|key| key == "timestamp"), "histogram requires timestamp in the immutable key");
        ensure!(
            schema.partitions.iter().any(|key| key == "project_id") && schema.partitions.iter().any(|key| key == "date"),
            "histogram requires project/date partitions"
        );
        let (lo, hi) = window.bounds();
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
        dml.validate(stamp)?;
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
        let partition = crate::read::CountPartition { project_id: project.clone(), table_name: table.into(), date: date.clone() };
        let unproven = db.capture_histogram(&project, table, window, Some(&membership), 64, context.clone()).await?;
        let busy = db.histogram_proof_build.slot.clone().acquire_owned().await?;
        assert!(unproven.seed_missing_proof(&db).is_none(), "busy proof builder must decline without queuing");
        drop(busy);
        let build = unproven.seed_missing_proof(&db).context("completed partition must schedule a proof")?;
        assert!(unproven.seed_missing_proof(&db).is_none(), "one query-triggered proof build holds the admission slot");
        build.await?;
        assert!(unproven.seed_missing_proof(&db).is_none(), "a recent attempt must not rebuild from a stale query capture");
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
        let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new().with_memory_pool(pool.clone()).build_arc()?;
        let wide_context = datafusion::prelude::SessionContext::new_with_config_rt(Default::default(), runtime).task_ctx();
        let wide = db.capture_histogram(&project, table, wide_window, Some(&membership), 64 * 1024 * 1024, wide_context.clone()).await?;
        let Err(error) = wide.count().await else { anyhow::bail!("wide hashes must exceed the old decoded budget") };
        assert!(error.to_string().contains("budget"), "the old path must fail at its decoded budget: {error}");
        let result = wide.count_streaming().await?;
        assert_eq!(result.counts.values().sum::<u64>(), 64);
        assert_eq!(result.scanned_sources, 0, "wide hashes stay in the index during version resolution");
        db.insert_records_batch(&project, table, vec![json_to_batch_for(table, vec![wide_row(1, "b")])?], true, None).await?;
        let partial = db.capture_histogram(&project, table, wide_window, Some(&membership), 64 * 1024 * 1024, wide_context).await?;
        let result = partial.count_streaming().await?;
        assert_eq!(result.counts.values().sum::<u64>(), 63, "an unindexed replacement must suppress its indexed old version");
        assert_eq!(result.scanned_sources, 1);
        assert_eq!(wide.count_streaming().await?.counts.values().sum::<u64>(), 64, "later writes cannot replace captured sources");
        assert_eq!(pool.reserved(), 0, "streaming query reservations must release after counting");
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
        Ok(())
    }

    #[test]
    fn histogram_capture_detects_overlapping_dml_and_guard_cancellation() {
        let state = std::sync::Arc::new(super::HistogramDmlState::default());
        let before = state.stamp().unwrap();
        let first = state.clone().enter();
        let second = std::sync::Arc::new(state.clone().enter());
        let queued = second.clone();
        assert!(state.stamp().is_err());
        drop(first);
        assert!(state.validate(before).is_err(), "another DML remains active");
        drop(second);
        assert!(state.stamp().is_err(), "queued work must remain fenced after the statement returns");
        drop(queued);
        assert!(state.validate(before).is_err(), "completed writes must invalidate an earlier capture");
        state.validate(state.stamp().unwrap()).unwrap();
    }
}
