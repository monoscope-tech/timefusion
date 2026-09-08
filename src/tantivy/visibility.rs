//! Builds physical-row winner masks through the canonical merge-on-read operator.

use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use arrow::{
    array::BooleanBufferBuilder,
    array::{Array, ArrayRef, BooleanArray, UInt32Array, UInt64Array},
    buffer::BooleanBuffer,
    compute::filter_record_batch,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    datasource::memory::MemorySourceConfig,
    execution::context::TaskContext,
    physical_plan::{ExecutionPlan, execute_stream},
};

/// Exact daily count derived from complete Delta winner resolution. File names
/// alone are insufficient: a same-path deletion-vector update changes the proof.
/// Keeping this small result in the manifest avoids retaining every event key.
#[serde_with::serde_as]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct PartitionCountProof {
    version: u32,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    root: url::Url,
    files: crate::read::CountFiles,
    keys: Vec<String>,
    tiebreak: Option<String>,
    tombstone: Option<String>,
    schema: Schema,
    pub logical_count: u64,
}

impl PartitionCountProof {
    fn visibility_schema(table: &crate::schema::TableSchema) -> Result<Schema> {
        let schema = table.schema_ref();
        let columns = table.dedup_keys.iter().chain(table.dedup_tiebreak.iter()).chain(table.tombstone_column.iter());
        let projection = columns.map(|column| schema.index_of(column)).collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(schema.project(&projection)?)
    }

    /// The caller must have resolved the entire partition represented by `files`.
    pub fn new(root: url::Url, files: crate::read::CountFiles, table: &crate::schema::TableSchema, logical_count: u64) -> Result<Self> {
        Ok(Self {
            version: 1,
            root,
            files,
            keys: table.dedup_keys.clone(),
            tiebreak: table.dedup_tiebreak.clone(),
            tombstone: table.tombstone_column.clone(),
            schema: Self::visibility_schema(table)?,
            logical_count,
        })
    }

    pub fn matches(&self, root: &url::Url, files: &crate::read::CountFiles, table: &crate::schema::TableSchema) -> bool {
        self.version == 1
            && self.root == *root
            && self.files == *files
            && self.keys == table.dedup_keys
            && self.tiebreak == table.dedup_tiebreak
            && self.tombstone == table.tombstone_column
            && Self::visibility_schema(table).is_ok_and(|schema| schema == self.schema)
    }
}

/// Resolves an index source URI against the captured table root.
/// Absolute URIs outside this store or table cannot establish coverage.
pub fn relative_source_path(root: &url::Url, source: &str) -> Result<Option<String>> {
    use object_store::path::Path;
    if !source.contains("://") {
        ensure!(!source.starts_with('/'), "histogram source path must be table-relative or an absolute URI");
        return Ok(Some(Path::parse(source)?.to_string()));
    }
    let source = url::Url::parse(source)?;
    if source.scheme() != root.scheme()
        || source.host_str() != root.host_str()
        || source.port() != root.port()
        || source.username() != root.username()
        || source.password() != root.password()
        || source.query().is_some()
        || source.fragment().is_some()
    {
        return Ok(None);
    }
    let path = Path::from_url_path(source.path())?;
    let root = Path::from_url_path(root.path())?;
    Ok(path.prefix_match(&root).map(|parts| parts.map(|part| part.as_ref().to_owned()).collect::<Vec<_>>().join("/")))
}

/// The physical identity and logical constants of a file in a captured Delta view.
#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotFile {
    pub path: String,
    pub size: i64,
    pub partition_values: std::collections::HashMap<String, Option<String>>,
    pub deletion_vector: Option<deltalake::kernel::DeletionVectorDescriptor>,
}

impl SnapshotFile {
    /// Captures public file metadata without depending on deprecated Add conversion.
    /// Refuses a narrowed file view that omits any table partition column.
    pub fn capture(file: &deltalake::kernel::LogicalFileView, partition_columns: &[String]) -> Result<Self> {
        let partition_values: std::collections::HashMap<_, _> = file
            .partition_values()
            .map(|values| {
                values
                    .fields()
                    .iter()
                    .zip(values.values())
                    .map(|(field, value)| (field.name().to_owned(), (!value.is_null()).then(|| deltalake::kernel::scalars::ScalarExt::serialize(value))))
                    .collect()
            })
            .unwrap_or_default();
        ensure!(partition_columns.iter().all(|name| partition_values.contains_key(name)), "snapshot file is missing partition constants");
        Ok(Self { path: file.path().into_owned(), size: file.size(), partition_values, deletion_vector: file.deletion_vector_descriptor() })
    }
}

/// Physical batches from one file or memory snapshot, in source row order.
/// `live` identifies rows eligible for subsequent version resolution. It starts
/// with deletion-vector visibility and can also exclude superseded Delta versions
/// or rows replaced by memory authority ranges.
#[derive(Clone, Debug)]
pub struct SourceRows {
    pub batches: Vec<RecordBatch>,
    pub live: BooleanBuffer,
}

impl SourceRows {
    /// Excludes Delta rows replaced by the captured memory authority ranges.
    /// This preserves physical ordinals and existing deletion-vector exclusions.
    pub fn exclude_memory_ranges(&mut self, timestamp: &str, ranges: &[(i64, i64)]) -> Result<()> {
        if ranges.is_empty() {
            return Ok(());
        }
        ensure!(ranges.iter().all(|(lo, hi)| lo < hi), "memory authority ranges must be nonempty");
        let ranges = crate::write::mem_buffer::merge_ranges(ranges.to_vec());
        let rows = self.batches.iter().try_fold(0_usize, |sum, batch| sum.checked_add(batch.num_rows())).context("visibility row count overflow")?;
        ensure!(rows == self.live.len(), "visibility mask length differs from physical row count");
        let mut live = BooleanBufferBuilder::new(rows);
        let mut ordinal = 0;
        for batch in &self.batches {
            let column = batch.column_by_name(timestamp).context("visibility source is missing its timestamp")?;
            ensure!(
                matches!(column.data_type(), DataType::Int64 | DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)),
                "visibility timestamp must use microseconds"
            );
            let timestamps = crate::read::bound_slice(column).context("invalid visibility timestamp representation")?;
            for (row, &value) in timestamps.iter().enumerate() {
                ensure!(!column.is_null(row), "visibility timestamp contains nulls");
                let next = ranges.partition_point(|&(lo, _)| lo <= value);
                let covered = next > 0 && value < ranges[next - 1].1;
                live.append(self.live.value(ordinal) && !covered);
                ordinal += 1;
            }
        }
        self.live = live.finish();
        Ok(())
    }
}

/// Captured sources and their winning physical rows, with memory last.
/// The batches remain available for fallback without reading a newer snapshot.
pub struct ResolvedSnapshot {
    pub sources: Vec<SourceRows>,
    pub winners: Vec<BooleanBuffer>,
}

/// Resolves one project's pinned Delta sources and its captured memory overlay.
/// Exclusions run before deduplication, including ranges emptied by memory DML.
/// The caller must capture memory before Delta and include complete key groups.
pub async fn resolve_with_memory(
    mut delta: Vec<SourceRows>, memory: crate::write::mem_buffer::MemSnapshot, keys: &[String], tiebreak: Option<&str>, tombstone: Option<&str>,
    context: Arc<TaskContext>,
) -> Result<ResolvedSnapshot> {
    ensure!(keys.iter().any(|key| key == "timestamp"), "time-window visibility requires timestamp in the immutable key");
    for source in &mut delta {
        source.exclude_memory_ranges("timestamp", &memory.covered_ranges)?;
    }
    let rows = memory.batches.iter().try_fold(0_usize, |sum, batch| sum.checked_add(batch.num_rows())).context("memory snapshot row count overflow")?;
    delta.push(SourceRows { batches: memory.batches, live: BooleanBuffer::new_set(rows) });
    let winners = winner_masks(&delta, keys, tiebreak, tombstone, context).await?;
    Ok(ResolvedSnapshot { sources: delta, winners })
}

/// Reads selected logical columns in physical Parquet order from a pinned file.
/// Partition columns are reconstructed from that snapshot, as in the Delta reader.
/// No predicate runs before ordinals have been assigned by the visibility resolver.
pub async fn read_file_rows(
    log_store: deltalake::logstore::LogStoreRef, add: &SnapshotFile, schema: arrow::datatypes::SchemaRef, max_decoded_bytes: usize,
) -> Result<SourceRows> {
    use futures::TryStreamExt;

    let (mut stream, live) = stream_file_rows(log_store, add, schema).await?;
    let mut batches = Vec::new();
    let mut decoded_bytes = 0_usize;
    while let Some(batch) = stream.try_next().await? {
        decoded_bytes = decoded_bytes.checked_add(batch.get_array_memory_size()).context("visibility decode size overflow")?;
        ensure!(decoded_bytes <= max_decoded_bytes, "visibility source exceeds its decoded memory budget");
        batches.push(batch);
    }
    Ok(SourceRows { batches, live })
}

/// Streams logical columns in physical order without collecting output batches.
/// The separate DV mask never filters the stream or changes physical ordinals.
/// Consumers must account for retained batches and the mask in their memory budget.
pub async fn stream_file_rows(
    log_store: deltalake::logstore::LogStoreRef, add: &SnapshotFile, schema: arrow::datatypes::SchemaRef,
) -> Result<(futures::stream::BoxStream<'static, Result<RecordBatch>>, BooleanBuffer)> {
    let prepared = PreparedFileRows::open(log_store, add).await?;
    Ok((prepared.stream(schema)?, prepared.live))
}

/// Immutable Parquet metadata and DV visibility shared by repeatable source scans.
/// Each scan creates its own reader; no execution consumes another query's stream.
#[derive(Clone, Debug)]
pub(crate) struct PreparedFileRows {
    store: Arc<dyn object_store::ObjectStore>,
    path: object_store::path::Path,
    size: u64,
    metadata: deltalake::datafusion::parquet::arrow::arrow_reader::ArrowReaderMetadata,
    partitions: std::collections::HashMap<String, Option<String>>,
    live: BooleanBuffer,
}

impl PreparedFileRows {
    pub async fn open(log_store: deltalake::logstore::LogStoreRef, add: &SnapshotFile) -> Result<Self> {
        use deltalake::datafusion::parquet::arrow::{arrow_reader::ArrowReaderMetadata, async_reader::ParquetObjectReader};
        use object_store::ObjectStoreExt;

        let store = log_store.object_store(None);
        let path = object_store::path::Path::from(add.path.as_str());
        let meta = store.head(&path).await?;
        ensure!(i64::try_from(meta.size)? == add.size, "Parquet object size differs from snapshot Add");
        let mut reader = ParquetObjectReader::new(store.clone(), path.clone()).with_file_size(meta.size);
        let metadata = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
        let rows = usize::try_from(metadata.metadata().file_metadata().num_rows())?;
        let live = deletion_vector_mask(log_store, add.deletion_vector.as_ref(), rows).await?;
        Ok(Self { store, path, size: meta.size, metadata, partitions: add.partition_values.clone(), live })
    }

    pub fn stream(&self, schema: arrow::datatypes::SchemaRef) -> Result<futures::stream::BoxStream<'static, Result<RecordBatch>>> {
        use deltalake::datafusion::parquet::arrow::{
            ProjectionMask,
            async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder},
        };
        use futures::TryStreamExt;

        let reader = ParquetObjectReader::new(self.store.clone(), self.path.clone()).with_file_size(self.size);
        let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, self.metadata.clone());
        let rows = self.live.len();
        let mut projection = Vec::new();
        for field in schema.fields() {
            if !self.partitions.contains_key(field.name()) {
                match builder.schema().index_of(field.name()) {
                    Ok(index) => projection.push(index),
                    Err(_) => ensure!(field.is_nullable(), "required visibility column is absent from Parquet: {}", field.name()),
                }
            }
        }
        let projection = ProjectionMask::roots(builder.parquet_schema(), projection);
        let stream = builder.with_projection(projection).build()?;
        let partitions = self.partitions.clone();
        let batches = futures::stream::try_unfold((stream, schema, partitions, 0_usize), move |(mut stream, schema, partitions, decoded_rows)| async move {
            let Some(batch) = stream.try_next().await? else {
                ensure!(decoded_rows == rows, "visibility read did not preserve every physical source ordinal");
                return Ok(None);
            };
            let columns = schema
                .fields()
                .iter()
                .map(|field| -> Result<ArrayRef> {
                    let array: ArrayRef = if let Some(partition) = partitions.get(field.name()) {
                        Arc::new(arrow::array::StringArray::from_iter(std::iter::repeat_n(partition.as_deref(), batch.num_rows())))
                    } else if let Some(column) = batch.column_by_name(field.name()) {
                        column.clone()
                    } else {
                        arrow::array::new_null_array(field.data_type(), batch.num_rows())
                    };
                    let array = arrow::compute::cast(&array, field.data_type())?;
                    ensure!(field.is_nullable() || array.null_count() == 0, "required visibility column contains nulls: {}", field.name());
                    Ok(array)
                })
                .collect::<Result<Vec<_>>>()?;
            let batch = RecordBatch::try_new(schema.clone(), columns)?;
            let decoded_rows = decoded_rows.checked_add(batch.num_rows()).context("visibility source row count overflow")?;
            ensure!(decoded_rows <= rows, "visibility read exceeds physical source row count");
            Ok(Some((batch, (stream, schema, partitions, decoded_rows))))
        });
        Ok(Box::pin(batches))
    }
}

/// Reads the deletion vector pinned in a Delta snapshot, preserving physical ordinals.
/// Uses the same kernel reader as Delta, including inline and absolute-path vectors.
pub async fn deletion_vector_mask(
    log_store: deltalake::logstore::LogStoreRef, descriptor: Option<&deltalake::kernel::DeletionVectorDescriptor>, rows: usize,
) -> Result<BooleanBuffer> {
    use buoyant_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
    use deltalake::kernel::StorageType;

    let Some(descriptor) = descriptor else {
        return Ok(BooleanBuffer::new_set(rows));
    };
    let storage_type = match descriptor.storage_type {
        StorageType::UuidRelativePath => DeletionVectorStorageType::PersistedRelative,
        StorageType::Inline => DeletionVectorStorageType::Inline,
        StorageType::AbsolutePath => DeletionVectorStorageType::PersistedAbsolute,
    };
    let descriptor =
        DeletionVectorDescriptor::try_new(storage_type, &descriptor.path_or_inline_dv, descriptor.offset, descriptor.size_in_bytes, descriptor.cardinality)?;
    tokio::task::spawn_blocking(move || {
        let deleted = descriptor.read(log_store.engine(None).storage_handler(), log_store.root_url())?;
        ensure!(i64::try_from(deleted.len())? == descriptor.cardinality, "deletion vector cardinality differs from its snapshot descriptor");
        let mut live = BooleanBufferBuilder::new(rows);
        live.append_n(rows, true);
        for row in deleted.iter() {
            let row = usize::try_from(row)?;
            ensure!(row < rows, "deletion vector ordinal exceeds its physical file");
            live.set_bit(row, false);
        }
        Ok(live.finish())
    })
    .await
    .context("deletion vector reader failed")?
}

/// Resolves complete key groups and returns one physical-row mask per source.
///
/// The caller must supply all competing versions from the same query snapshot,
/// including uncovered files and memory. Projection must retain the complete
/// deduplication key, version, and tombstone columns. Source order establishes
/// the canonical operator's first-wins rule for equal versions.
///
/// Source ordinals are attached before deletion-vector filtering. Consequently a
/// surviving row keeps its physical ordinal even when earlier rows are deleted.
pub async fn winner_masks(
    sources: &[SourceRows], keys: &[String], tiebreak: Option<&str>, tombstone: Option<&str>, context: Arc<TaskContext>,
) -> Result<Vec<BooleanBuffer>> {
    let Some(first) = sources.iter().flat_map(|source| &source.batches).next() else {
        ensure!(sources.iter().all(|source| source.live.is_empty()), "visibility source has a mask but no physical rows");
        return Ok(sources.iter().map(|source| BooleanBuffer::new_unset(source.live.len())).collect());
    };
    ensure!(!keys.is_empty(), "winner masks require a complete deduplication key");
    let source_schema = first.schema();
    let mut projection = Vec::new();
    for name in keys.iter().map(String::as_str).chain(tiebreak).chain(tombstone) {
        let index = source_schema.index_of(name).with_context(|| format!("missing visibility column: {name}"))?;
        if !projection.contains(&index) {
            projection.push(index);
        }
    }
    let narrow_schema = source_schema.project(&projection)?;
    // These are physical lineage columns, carried through DedupExec alongside the
    // keys. They never enter the user schema or influence version comparison.
    let lineage_names = VISIBILITY_LINEAGE;
    ensure!(lineage_names.iter().all(|name| narrow_schema.index_of(name).is_err()), "visibility lineage column conflicts with source schema");
    let schema = Arc::new(Schema::new(
        narrow_schema
            .fields()
            .iter()
            .cloned()
            .chain([Arc::new(Field::new(lineage_names[0], DataType::UInt32, false)), Arc::new(Field::new(lineage_names[1], DataType::UInt64, false))])
            .collect::<Vec<_>>(),
    ));
    let mut batches = Vec::new();
    for (source_id, source) in sources.iter().enumerate() {
        let source_id = u32::try_from(source_id)?;
        let physical_rows =
            source.batches.iter().try_fold(0_usize, |total, batch| total.checked_add(batch.num_rows())).context("visibility row count overflow")?;
        ensure!(physical_rows == source.live.len(), "visibility mask length differs from physical row count");
        let mut offset = 0;
        for batch in &source.batches {
            ensure!(batch.schema() == source_schema, "visibility sources have incompatible schemas");
            let len = batch.num_rows();
            let mut columns = batch.project(&projection)?.columns().to_vec();
            columns.push(Arc::new(UInt32Array::from_value(source_id, len)) as ArrayRef);
            columns.push(Arc::new(UInt64Array::from_iter_values((offset..offset + len).map(|row| row as u64))) as ArrayRef);
            let augmented = RecordBatch::try_new(schema.clone(), columns)?;
            let live = BooleanArray::new(source.live.slice(offset, len), None);
            batches.push(filter_record_batch(&augmented, &live)?);
            offset += len;
        }
    }
    let input = MemorySourceConfig::try_new_exec(&[batches], schema, None)?;
    stream_winner_masks(input, sources.iter().map(|source| source.live.len()).collect(), keys, tiebreak, tombstone, context).await
}

pub(crate) const VISIBILITY_LINEAGE: [&str; 2] = ["__timefusion_visibility_source", "__timefusion_visibility_ordinal"];

/// Resolve physical lineage from a complete source plan without collecting its output.
/// The explicit sort preserves source/ordinal ties and lets canonical deduplication
/// release timestamp runs. Its memory pool and spill limits come from `context`.
pub(crate) async fn stream_winner_masks(
    input: Arc<dyn ExecutionPlan>, source_rows: Vec<usize>, keys: &[String], tiebreak: Option<&str>, tombstone: Option<&str>, context: Arc<TaskContext>,
) -> Result<Vec<BooleanBuffer>> {
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column};
    use datafusion::physical_plan::sorts::sort::SortExec;
    use futures::TryStreamExt;

    ensure!(!keys.is_empty(), "streamed visibility requires a complete key");
    let schema = input.schema();
    let [source_column, ordinal_column] = VISIBILITY_LINEAGE.map(|name| schema.index_of(name));
    let (source_column, ordinal_column) = (source_column?, ordinal_column?);
    // Greatest-first within a key also keeps canonical early run emission exact
    // when a single timestamp group exceeds its retained-buffer ceiling.
    let ascending = arrow::compute::SortOptions::default();
    let ordering = keys
        .iter()
        .map(String::as_str)
        .filter(|key| *key == "timestamp")
        .chain(keys.iter().map(String::as_str).filter(|key| *key != "timestamp"))
        .map(|name| (name, ascending))
        .chain(tiebreak.map(|name| (name, arrow::compute::SortOptions { descending: true, nulls_first: false })))
        .chain(VISIBILITY_LINEAGE.map(|name| (name, ascending)))
        .map(|(name, options)| Ok(PhysicalSortExpr::new(Arc::new(Column::new_with_schema(name, &schema)?), options)))
        .collect::<Result<Vec<_>>>()?;
    let ordering = LexOrdering::new(ordering).context("visibility sort requires keys")?;
    let input = Arc::new(datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(input));
    let sorted = Arc::new(SortExec::new(ordering.clone(), input));
    let plan = crate::read::DedupExec::with_tiebreak(sorted, keys.to_vec(), tiebreak.map(str::to_owned), None)?.requiring(Some(ordering));
    let reservation = datafusion::execution::memory_pool::MemoryConsumer::new("TantivyWinnerMasks").register(context.memory_pool());
    let bytes = source_rows.iter().map(|rows| rows.div_ceil(8)).try_fold(0_usize, usize::checked_add).context("winner mask size overflow")?;
    reservation.try_resize(bytes)?;
    let mut masks = source_rows
        .iter()
        .map(|&rows| {
            let mut mask = BooleanBufferBuilder::new(rows);
            mask.append_n(rows, false);
            mask
        })
        .collect::<Vec<_>>();
    let allocated = masks.iter().map(|mask| mask.capacity().div_ceil(8)).try_fold(0_usize, usize::checked_add).context("winner mask allocation overflow")?;
    reservation.try_resize(allocated)?;
    let mut stream = execute_stream(Arc::new(plan), context)?;
    while let Some(batch) = stream.try_next().await? {
        let source_ids = batch.column(source_column).as_any().downcast_ref::<UInt32Array>().context("visibility source column is not UInt32")?;
        let ordinals = batch.column(ordinal_column).as_any().downcast_ref::<UInt64Array>().context("visibility ordinal column is not UInt64")?;
        let deleted = tombstone
            .map(|name| -> Result<&BooleanArray> {
                batch.column(batch.schema().index_of(name)?).as_any().downcast_ref::<BooleanArray>().context("tombstone column is not Boolean")
            })
            .transpose()?;
        for row in 0..batch.num_rows() {
            if deleted.is_none_or(|column| column.is_null(row) || !column.value(row)) {
                let source = usize::try_from(source_ids.value(row))?;
                let ordinal = usize::try_from(ordinals.value(row))?;
                ensure!(source < masks.len() && ordinal < source_rows[source], "winner lineage is outside its source");
                masks[source].set_bit(ordinal, true);
            }
        }
    }
    Ok(masks.iter_mut().map(BooleanBufferBuilder::finish).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    #[tokio::test]
    async fn count_proofs_bind_visibility_and_retain_thirty_day_windows() -> Result<()> {
        let root = url::Url::parse("s3://bucket/table")?;
        let schema = crate::schema::get_schema("mor_versioned").unwrap();
        let files: crate::read::CountFiles = [("part/file.parquet".into(), None)].into();
        let proof = PartitionCountProof::new(root.clone(), files.clone(), schema, 7)?;
        for change in 0..8 {
            let mut changed = proof.clone();
            match change {
                0 => changed.version += 1,
                1 => changed.root = url::Url::parse("s3://other/table")?,
                2 => {
                    changed.files.insert("other.parquet".into(), None);
                }
                3 => {
                    changed.files.insert(
                        "part/file.parquet".into(),
                        Some(deltalake::kernel::DeletionVectorDescriptor {
                            storage_type: deltalake::kernel::StorageType::UuidRelativePath,
                            path_or_inline_dv: "dv".into(),
                            offset: Some(1),
                            size_in_bytes: 10,
                            cardinality: 1,
                        }),
                    );
                }
                4 => changed.keys.reverse(),
                5 => changed.tiebreak = None,
                6 => changed.tombstone = None,
                _ => changed.schema = Schema::empty(),
            }
            assert!(!changed.matches(&root, &files, schema), "proof must reject visibility change {change}");
        }
        let store = Arc::new(object_store::memory::InMemory::new());
        let indexer = crate::tantivy::search::TantivyIndexService::new(store.clone(), Arc::new(Default::default()));
        let start = chrono::NaiveDate::from_ymd_opt(2026, 7, 1).unwrap();
        for offset in 0..35 {
            indexer.publish_count_proof("mor_versioned", "project", start + chrono::Duration::days(offset), proof.clone()).await?;
        }
        let manifest = crate::tantivy::load_manifest(store.as_ref(), "mor_versioned", "project").await?;
        assert_eq!(manifest.count_proofs.len(), 32);
        assert_eq!(manifest.count_proofs.first_key_value().unwrap().0, &(start + chrono::Duration::days(3)));
        assert!(manifest.count_proofs.values().all(|loaded| loaded.matches(&root, &files, schema) && loaded.logical_count == 7));
        Ok(())
    }

    #[test_case::test_case("part/a%20b.parquet", Some("part/a%20b.parquet"))]
    #[test_case::test_case("s3://bucket/table/part/a%20b.parquet", Some("part/a b.parquet"))]
    #[test_case::test_case("s3://bucket/table/part/a%2520b.parquet", Some("part/a%20b.parquet"))]
    #[test_case::test_case("s3://bucket/table-other/part/file", None)]
    #[test_case::test_case("s3://other/table/part/file", None)]
    #[test_case::test_case("s3://bucket/table/part/file?version=2", None)]
    #[test_case::test_case("s3://bucket/elsewhere/file", None)]
    fn source_paths_keep_store_and_table_identity(source: &str, expected: Option<&str>) -> Result<()> {
        let root = url::Url::parse("s3://bucket/table")?;
        assert_eq!(relative_source_path(&root, source)?.as_deref(), expected);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn committed_deletion_vectors_keep_old_and_new_snapshot_masks_distinct() -> Result<()> {
        use deltalake::{
            DeltaTableBuilder,
            kernel::{Action, transaction::CommitBuilder},
            operations::deletion_vectors::{FileDeletion, write_deletion_vectors},
        };
        use futures::TryStreamExt;
        let store = Arc::new(object_store::memory::InMemory::new());
        let url = url::Url::parse("memory:///histogram-visibility")?;
        let mut table = DeltaTableBuilder::from_url(url.clone())?
            .with_storage_backend(store, url)
            .build()?
            .create()
            .with_columns(vec![
                deltalake::kernel::StructField::new("id", deltalake::kernel::DataType::LONG, true),
                deltalake::kernel::StructField::new("project", deltalake::kernel::DataType::STRING, true),
            ])
            .with_partition_columns(vec!["project"])
            .with_configuration([("delta.enableDeletionVectors", Some("true"))])
            .await?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true), Field::new("project", DataType::Utf8, true)]));
        table = table
            .write(vec![RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4])), Arc::new(StringArray::from(vec!["p"; 4]))])?])
            .await?;
        let log_store = table.log_store();
        let commit = log_store.read_commit_entry(table.version().context("missing table version")?).await?.context("missing write commit")?;
        let actions = commit
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(serde_json::from_slice::<Action>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut add = actions
            .into_iter()
            .find_map(|action| match action {
                Action::Add(add) => Some(add),
                _ => None,
            })
            .context("write commit has no file")?;
        let mut previous = None;
        let mut captured = SnapshotFile::capture(&table.snapshot()?.log_data().iter().next().context("missing snapshot file")?, &["project".into()])?;
        let original = captured.clone();
        let pinned = PreparedFileRows::open(log_store.clone(), &original).await?;
        assert!(SnapshotFile::capture(&table.snapshot()?.log_data().iter().next().unwrap(), &["missing".into()]).is_err());
        for (row, expected) in [(1, vec![true, false, true, true]), (3, vec![true, false, true, false])] {
            let actions =
                write_deletion_vectors(log_store.as_ref(), log_store.root_url(), vec![FileDeletion { add: add.clone(), deleted_indexes: vec![row] }]).await?;
            add = actions
                .iter()
                .find_map(|action| match action {
                    Action::Add(add) => Some(add.clone()),
                    _ => None,
                })
                .context("DV commit has no file")?;
            let descriptor = actions
                .iter()
                .find_map(|action| match action {
                    Action::Add(add) => add.deletion_vector.clone(),
                    _ => None,
                })
                .context("missing DV")?;
            let committed = CommitBuilder::default()
                .with_actions(actions)
                .build(Some(table.snapshot()?), log_store.clone(), deltalake::protocol::DeltaOperation::Delete { predicate: None })
                .await?;
            table.state = Some(committed.snapshot().clone());
            captured = SnapshotFile::capture(&table.snapshot()?.log_data().iter().next().context("missing snapshot file")?, &["project".into()])?;
            let mask = deletion_vector_mask(log_store.clone(), Some(&descriptor), 4).await?;
            assert_eq!(mask.iter().collect::<Vec<_>>(), expected);
            for projection in [schema.clone(), Arc::new(schema.project(&[0])?)] {
                let batches = pinned.clone().stream(projection)?.try_collect::<Vec<_>>().await?;
                let ids = batches
                    .iter()
                    .flat_map(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().iter().copied())
                    .collect::<Vec<_>>();
                assert_eq!(ids, [1, 2, 3, 4], "repeated scans must retain original physical row order");
                assert_eq!(pinned.live, BooleanBuffer::new_set(4), "later DV commits must not replace prepared visibility");
            }
            let source = read_file_rows(log_store.clone(), &captured, schema.clone(), 1_000_000).await?;
            assert_eq!(source.live, mask);
            assert_eq!(source.batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4, "DV filtering must not rebase physical ordinals");
            for batch in source.batches {
                let project = batch.column_by_name("project").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
                assert!(project.iter().all(|value| value == Some("p")), "partition keys must be reconstructed from the snapshot Add");
            }
            if let Some(previous) = &previous {
                assert_eq!(deletion_vector_mask(log_store.clone(), Some(previous), 4).await?.iter().collect::<Vec<_>>(), vec![true, false, true, true]);
                assert!(deletion_vector_mask(log_store.clone(), Some(&descriptor), 3).await.is_err(), "out-of-file ordinals must not silently disappear");
            }
            previous = Some(descriptor);
        }
        assert_eq!(read_file_rows(log_store.clone(), &original, schema.clone(), 1_000_000).await?.live, BooleanBuffer::new_set(4));
        assert!(read_file_rows(log_store.clone(), &captured, schema, 0).await.is_err(), "decoding must honor its memory budget");
        assert_eq!(deletion_vector_mask(log_store, None, 4).await?, BooleanBuffer::new_set(4));
        Ok(())
    }

    type Row<'a> = (&'a str, &'a str, &'a str, i64, Option<i64>, Option<bool>);

    fn batch(rows: &[Row<'_>]) -> RecordBatch {
        let fields = [
            ("project", DataType::Utf8),
            ("service", DataType::Utf8),
            ("id", DataType::Utf8),
            ("timestamp", DataType::Int64),
            ("version", DataType::Int64),
            ("deleted", DataType::Boolean),
        ];
        RecordBatch::try_new(
            Arc::new(Schema::new(fields.into_iter().map(|(name, ty)| Field::new(name, ty, true)).collect::<Vec<_>>())),
            vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.3))),
                Arc::new(Int64Array::from(rows.iter().map(|row| row.4).collect::<Vec<_>>())),
                Arc::new(BooleanArray::from(rows.iter().map(|row| row.5).collect::<Vec<_>>())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn memory_authority_precedes_version_resolution_without_rebasing_ordinals() -> Result<()> {
        let delta = batch(&[
            ("p", "s", "before", 9, Some(1), None),
            ("p", "s", "deleted", 10, Some(1), None),
            ("p", "s", "updated", 11, Some(100), None),
            ("p", "s", "dv", 12, Some(1), None),
            ("p", "s", "end", 13, Some(1), None),
        ]);
        let memory = crate::write::mem_buffer::MemSnapshot {
            batches: vec![batch(&[("p", "s", "updated", 11, Some(2), None)])],
            covered_ranges: vec![(11, 13), (10, 12)],
        };
        let sources = vec![SourceRows { batches: vec![delta.slice(0, 2), delta.slice(2, 3)], live: BooleanBuffer::from(vec![true, true, true, false, true]) }];
        let keys = ["project", "timestamp", "service", "id"].map(str::to_owned);
        let resolved = resolve_with_memory(sources, memory, &keys, Some("version"), Some("deleted"), Arc::new(TaskContext::default())).await?;
        assert_eq!(resolved.winners[0].iter().collect::<Vec<_>>(), vec![true, false, false, false, true]);
        assert_eq!(resolved.winners[1].iter().collect::<Vec<_>>(), vec![true], "memory authority suppresses even a higher version from an older flush");
        assert_eq!(resolved.sources[0].batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5, "physical rows must remain available for fallback");
        let mut source = resolved.sources.into_iter().next().unwrap();
        source.exclude_memory_ranges("timestamp", &[(i64::MIN, 9), (13, i64::MAX)])?;
        assert_eq!(source.live.iter().collect::<Vec<_>>(), vec![true, false, false, false, false]);
        let saved = source.live.clone();
        assert!(source.exclude_memory_ranges("missing", &[(0, 1)]).is_err());
        assert_eq!(source.live, saved, "a failed exclusion must not partially publish its mask");
        assert!(source.exclude_memory_ranges("timestamp", &[(10, 10)]).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn cached_delta_winners_match_full_resolution_with_memory_overlays() -> Result<()> {
        let keys = ["project", "timestamp", "service", "id"].map(str::to_owned);
        let original = vec![SourceRows {
            batches: vec![batch(&[
                ("p", "s", "a", 10, Some(1), None),
                ("p", "s", "a", 10, Some(3), Some(true)),
                ("p", "s", "b", 11, Some(2), None),
                ("p", "s", "b", 11, Some(9), None),
            ])],
            live: BooleanBuffer::from(vec![true, true, true, false]),
        }];
        let mut cached = original.clone();
        let winners = winner_masks(&cached, &keys, Some("version"), None, Arc::new(TaskContext::default())).await?;
        for (source, live) in cached.iter_mut().zip(winners) {
            source.live = live;
        }
        for version in [None, Some(1), Some(3), Some(4)] {
            for deleted in [None, Some(false), Some(true)] {
                for ranges in [vec![], vec![(10, 11)], vec![(10, 12)]] {
                    let batches = vec![batch(&[("p", "s", "a", 10, version, deleted)])];
                    let memory = || crate::write::mem_buffer::MemSnapshot { batches: batches.clone(), covered_ranges: ranges.clone() };
                    let expected =
                        resolve_with_memory(original.clone(), memory(), &keys, Some("version"), Some("deleted"), Arc::new(TaskContext::default())).await?;
                    let actual =
                        resolve_with_memory(cached.clone(), memory(), &keys, Some("version"), Some("deleted"), Arc::new(TaskContext::default())).await?;
                    assert_eq!(actual.winners, expected.winners, "version={version:?}, deleted={deleted:?}, ranges={ranges:?}");
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn streamed_winners_preserve_versions_across_large_timestamp_run() -> Result<()> {
        // Cross the canonical 64 MiB run buffer at an output-batch boundary.
        // The same key's newer version must still beat the preceding batch.
        let rows = 8192;
        let ids = (0..rows).map(|i| format!("{i:05}{}", "x".repeat(8200))).collect::<Vec<_>>();
        let old = batch(&ids.iter().map(|id| ("p", "s", id.as_str(), 10, Some(1), None)).collect::<Vec<_>>());
        let newer = batch(&[("p", "s", ids.last().unwrap(), 10, Some(2), None)]);
        let sources =
            [SourceRows { batches: vec![old], live: BooleanBuffer::new_set(rows) }, SourceRows { batches: vec![newer], live: BooleanBuffer::new_set(1) }];
        let context = datafusion::prelude::SessionContext::new_with_config(datafusion::prelude::SessionConfig::new().with_batch_size(rows));
        let keys = ["project", "timestamp", "service", "id"].map(str::to_owned);
        let masks = winner_masks(&sources, &keys, Some("version"), Some("deleted"), context.task_ctx()).await?;
        assert_eq!(masks[0].count_set_bits(), rows - 1);
        assert!(!masks[0].value(rows - 1), "the pre-boundary version must lose");
        assert!(masks[1].value(0), "the post-boundary newer version must win");
        Ok(())
    }

    #[tokio::test]
    async fn masks_resolve_complete_keys_versions_tombstones_and_physical_ordinals() -> Result<()> {
        let old = batch(&[
            ("p", "s", "a", 10, Some(1), None),
            ("p", "other", "a", 10, Some(1), None),
            ("other", "s", "a", 10, Some(1), None),
            ("p", "s", "gone", 10, Some(1), None),
            ("p", "s", "tie", 10, Some(2), None),
            ("p", "s", "dv", 10, Some(100), None),
            ("p", "s", "null", 10, None, None),
        ]);
        let newer = batch(&[
            ("p", "s", "a", 10, Some(2), None),
            ("p", "s", "gone", 10, Some(2), Some(true)),
            ("p", "s", "tie", 10, Some(2), Some(true)),
            ("p", "s", "dv", 10, Some(1), None),
            ("p", "s", "null", 10, Some(0), Some(false)),
        ]);
        let memory = batch(&[("p", "s", "a", 10, Some(3), None), ("p", "s", "a", 11, Some(1), None)]);
        let mut sources = vec![
            SourceRows { batches: vec![old.slice(0, 3), old.slice(3, 4)], live: BooleanBuffer::from(vec![true, true, true, true, true, false, true]) },
            SourceRows { batches: vec![newer], live: BooleanBuffer::new_set(5) },
            SourceRows { batches: vec![memory], live: BooleanBuffer::new_set(2) },
        ];
        let keys = ["project", "timestamp", "service", "id"].map(str::to_owned);
        let masks = winner_masks(&sources, &keys, Some("version"), Some("deleted"), Arc::new(TaskContext::default())).await?;
        assert_eq!(
            masks.iter().map(|mask| mask.iter().collect::<Vec<_>>()).collect::<Vec<_>>(),
            vec![vec![false, true, true, false, true, false, false], vec![false, false, false, true, true], vec![true, true],]
        );
        sources[0].live = BooleanBuffer::new_set(1);
        assert!(winner_masks(&sources, &keys, Some("version"), Some("deleted"), Arc::new(TaskContext::default())).await.is_err());
        Ok(())
    }
}
