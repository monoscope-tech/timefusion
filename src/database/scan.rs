use super::*;

#[derive(Debug, Clone)]
pub struct ProjectRoutingTable {
    default_project: String,
    database: Arc<Database>,
    schema: SchemaRef,
    _batch_queue: Option<Arc<crate::write::BatchQueue>>,
    table_name: String,
    /// When true, INSERTs commit straight to Delta, bypassing the
    /// BufferedWriteLayer. Backs the `{table}__bulk` alias; reads are unaffected.
    skip_queue: bool,
}

impl ProjectRoutingTable {
    pub fn new(
        default_project: String, database: Arc<Database>, schema: SchemaRef, batch_queue: Option<Arc<crate::write::BatchQueue>>, table_name: String,
    ) -> Self {
        Self { default_project, database, schema, _batch_queue: batch_queue, table_name, skip_queue: false }
    }

    /// Route this provider's INSERTs straight to Delta, bypassing the
    /// BufferedWriteLayer. Backs the `{table}__bulk` alias.
    pub fn with_skip_queue(mut self, skip_queue: bool) -> Self {
        self.skip_queue = skip_queue;
        self
    }

    fn extract_project_id_from_filters(&self, filters: &[Expr]) -> Option<String> {
        filters.iter().find_map(crate::read::optimizers::extract_project_id_from_expr)
    }

    fn bounded_otel_scan_reason(&self, filters: &[Expr], limit: Option<usize>) -> Option<&'static str> {
        let conjuncts: Vec<&Expr> = filters.iter().flat_map(datafusion::logical_expr::utils::split_conjunction).collect();
        let bounded = conjuncts.iter().any(|expr| Self::is_bounding_predicate(expr))
            || self.extract_time_range_from_filters(&conjuncts.into_iter().cloned().collect::<Vec<_>>()).is_some_and(|(lower, _)| lower != i64::MIN);
        Self::raw_otel_scan_reason(&self.table_name, filters, limit, bounded)
    }

    /// Predicates that bound a scan but that `extract_time_range_from_filters`
    /// does not decompose: `date = '…'` (one partition) and `BETWEEN`.
    pub(crate) fn is_bounding_predicate(expr: &Expr) -> bool {
        matches!(expr, Expr::Between(between) if !between.negated && matches!(between.expr.as_ref(), Expr::Column(c) if c.name == "timestamp"))
            || matches!(expr, Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, .. }) if matches!(left.as_ref(), Expr::Column(c) if c.name == "date"))
    }

    pub(crate) fn raw_otel_scan_reason(table_name: &str, filters: &[Expr], limit: Option<usize>, lower_timestamp_bound: bool) -> Option<&'static str> {
        if !matches!(table_name, "otel_logs_and_spans" | "otel_metrics") {
            return None;
        }
        if !filters.iter().any(|filter| crate::read::optimizers::extract_project_id_from_expr(filter).is_some()) {
            return Some("missing exact project_id filter");
        }
        (limit.is_none() && !lower_timestamp_bound).then_some("missing timestamp lower bound or scan limit")
    }

    /// pgwire-INSERT fast path: skips `DataSinkExec`/`ValuesExec`, taking an
    /// already-materialized batch straight to `insert_records_batch`. Returns
    /// the inserted row count.
    pub async fn fast_insert_batch(&self, batch: RecordBatch) -> DFResult<u64> {
        let total_rows = batch.num_rows() as u64;
        if total_rows == 0 {
            return Ok(0);
        }
        let target_schema = self.real_schema();
        // Partition row-wise: one INSERT may carry rows for many projects, each
        // landing in its own Delta table. Distinct projects write concurrently.
        let writes = partition_batch_by_project(batch, &self.default_project)?
            .into_iter()
            .map(|(project_id, sub)| {
                let converted = convert_variant_columns(sub, &target_schema)?;
                Ok(async move {
                    self.database
                        .insert_records_batch(&project_id, &self.table_name, vec![converted], self.skip_queue, None)
                        .await
                        .map_err(|e| DataFusionError::Execution(format!("fast_insert_batch for project {} table {}: {}", project_id, self.table_name, e)))
                })
            })
            .collect::<DFResult<Vec<_>>>()?;
        futures::future::try_join_all(writes).await?;
        Ok(total_rows)
    }

    fn schema(&self) -> SchemaRef {
        // Present Variant cols as Utf8View at the table-provider boundary so the SQL planner's
        // INSERT VALUES type check accepts JSON string literals (arrow has no Utf8→Struct cast).
        // `write_all` converts these Utf8 columns back to Variant structs before the Delta write.
        create_insert_compatible_schema(&self.schema)
    }

    /// Real (Variant-typed) schema for internal use.
    pub fn real_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Determines if a filter can be pushed down exactly to Delta Lake
    fn is_exact_pushdown_filter(expr: &Expr) -> bool {
        match expr {
            // AND expressions are exact if all parts are exact (check this first)
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => Self::is_exact_pushdown_filter(left) && Self::is_exact_pushdown_filter(right),
            // A supported comparison between a pushdown column and a literal.
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                matches!(op, Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq)
                    && matches!(
                        (left.as_ref(), right.as_ref()),
                        (Expr::Column(col), Expr::Literal(_, _)) | (Expr::Literal(_, _), Expr::Column(col)) if Self::is_pushdown_column(&col.name)
                    )
            }
            // IS NULL/IS NOT NULL are exact
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                matches!(inner.as_ref(), Expr::Column(col) if Self::is_pushdown_column(&col.name))
            }
            // IN lists are exact for pushdown columns
            Expr::InList(in_list) => {
                matches!(in_list.expr.as_ref(), Expr::Column(col) if Self::is_pushdown_column(&col.name))
            }
            _ => false,
        }
    }

    /// True for columns that support exact pushdown — the table provider fully applies the
    /// filter and DataFusion can drop the `FilterExec`.
    ///
    /// Only true partition columns qualify. MemBuffer's best-effort physical-expr
    /// compilation can silently fall back to "no filter", and with exact pushdown
    /// DataFusion has already dropped the `FilterExec`, so rows would leak through.
    fn is_pushdown_column(column_name: &str) -> bool {
        matches!(column_name, "project_id" | "date")
    }

    /// Apply time-series specific optimizations to filters
    fn apply_time_series_optimizations(&self, filters: &[Expr]) -> DFResult<Vec<Expr>> {
        use crate::read::optimizers::time_range_partition_pruner;

        // Falls back to "timestamp" when the schema isn't registered (custom tables).
        let time_column = crate::schema::get_schema(&self.table_name).map(|s| s.time_column_name().to_string()).unwrap_or_else(|| "timestamp".to_string());

        let optimized_filters: Vec<Expr> = filters
            .iter()
            .cloned()
            .chain(filters.iter().flat_map(|filter| {
                let date_filters = time_range_partition_pruner::timestamp_to_date_filters(filter, &time_column);
                if !date_filters.is_empty() {
                    debug!("Added {} date partition filter(s) for {} on column {}", date_filters.len(), self.table_name, time_column);
                }
                date_filters
            }))
            .collect();

        if !crate::read::optimizers::ProjectIdPushdown::has_project_id_filter(&optimized_filters) {
            debug!("Query missing project_id filter - may scan all partitions");
        }

        Ok(optimized_filters)
    }

    /// Create a MemorySourceConfig-based execution plan with multiple partitions.
    ///
    /// `sorted` is the caller's claim that every partition is already ordered by
    /// the table's declared `sorting_columns`; declaring it is what stops a
    /// blocking `SortExec` being injected over this leg.
    fn create_memory_exec(&self, partitions: &[Vec<RecordBatch>], projection: Option<&Vec<usize>>, sorted: bool) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mem_source =
            MemorySourceConfig::try_new(partitions, self.schema.clone(), projection.cloned()).map_err(|e| DataFusionError::External(Box::new(e)))?;

        // The UNPROJECTED schema on purpose: `try_with_sort_information` validates
        // each sort column's (name, index) against the source's ORIGINAL schema
        // and maps it through the projection itself. Projected indices silently
        // drop the claim. `sort_partition` uses the same unprojected schema.
        Ok(Arc::new(DataSourceExec::new(Arc::new(Self::declare_ordering(mem_source, sorted, &self.table_name, &self.schema)))))
    }

    /// Attach the table's declared ordering to an in-memory source. Failure to
    /// attach is not fatal — the source still serves its rows, just unordered.
    fn declare_ordering(source: MemorySourceConfig, sorted: bool, table_name: &str, out: &SchemaRef) -> MemorySourceConfig {
        let Some(ordering) = table_ordering(table_name, out, sorted) else {
            metrics::counter!(scan_metric_names::MEM_ORDERING_UNSORTED).increment(1);
            return source;
        };
        // `try_with_sort_information` consumes the source, so keep a copy to
        // fall back to — the undeclared source must still serve its rows.
        let undeclared = source.clone();
        match source.try_with_sort_information(vec![ordering]) {
            Ok(declared) => {
                metrics::counter!(scan_metric_names::MEM_ORDERING_DECLARED).increment(1);
                declared
            }
            Err(e) => {
                // Losing the claim costs the whole query its streaming merge: a
                // union advertises an ordering only when EVERY child does.
                metrics::counter!(scan_metric_names::MEM_ORDERING_REJECTED).increment(1);
                warn!(
                    table_name,
                    error = %e,
                    event = "mem_ordering_rejected",
                    "the in-memory leg could not declare its ordering, so the union loses it and ORDER BY ... LIMIT becomes a blocking sort"
                );
                undeclared
            }
        }
    }

    /// Scan a Delta table and coerce output schema to match our expected types.
    /// Handles object store registration, projection translation, and type coercion (e.g., Utf8 -> Utf8View).
    ///
    /// `exclude_files`: parquet URIs the tantivy prefilter proved hold no matching rows.
    /// The selection is computed against THIS `table`'s snapshot, so a concurrent
    /// compaction cannot shift rows out of it.
    #[allow(clippy::too_many_arguments)]
    async fn scan_delta_table(
        &self, table: &DeltaTable, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>,
        include_files: Option<&HashSet<String>>, exclude_files: Option<&HashSet<String>>, row_selections: Option<&HashMap<String, Vec<u64>>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let project_id = self.extract_project_id_from_filters(filters).unwrap_or_else(|| self.default_project.clone());
        let cache_key = (project_id, self.table_name.clone());

        table.update_datafusion_session(state).map_err(|e| DataFusionError::External(Box::new(e)))?;

        // File-pruned scans bypass the provider cache (the selection is
        // query-specific). Bail to the unrestricted path unless EVERY
        // surviving live file maps to a table-relative path — a restriction
        // that silently missed an unmappable file would drop its rows.
        let select_started = std::time::Instant::now();
        let file_selection: Option<Vec<String>> = include_files.or_else(|| exclude_files.filter(|e| !e.is_empty())).and_then(|_selection| {
            // Scoped so the (non-Send) file-view iterator drops before any await.
            // `collect::<Option<_>>` bails the whole selection on one unmappable URI.
            table
                .get_file_uris()
                .ok()?
                .filter(|u| u.ends_with(".parquet") && include_files.map_or_else(|| !exclude_files.is_some_and(|e| e.contains(u)), |files| files.contains(u)))
                .map(|u| crate::tantivy::search::parquet_rel_of_uri(&u).map(str::to_string))
                .collect::<Option<Vec<String>>>()
        });
        // Row-selection pushdown: per-file matching ordinals keyed by rel path.
        // Purely narrowing — files without an entry scan normally — so unlike
        // `file_selection` an unmappable URI just drops that file's selection.
        let ordinal_selections: HashMap<String, Vec<u64>> = row_selections
            .into_iter()
            .flatten()
            .filter_map(|(uri, ords)| Some((crate::tantivy::search::parquet_rel_of_uri(uri)?.to_string(), ords.clone())))
            .collect();
        if file_selection.is_some() || !ordinal_selections.is_empty() {
            use deltalake::delta_datafusion::{FileSelection, MissingSelectedFilePolicy};
            if let Some(sel) = &file_selection {
                metrics::counter!(scan_metric_names::PRUNED_FILES).increment(sel.len() as u64);
                debug!(
                    "tantivy file pruning: {}/{} scanning {} files (excluded {})",
                    cache_key.0,
                    self.table_name,
                    sel.len(),
                    exclude_files.map_or(0, |e| e.len())
                );
            }
            let session_state = state.as_any().downcast_ref::<datafusion::execution::context::SessionState>().cloned();
            // Same DV opt-in as the cached-provider path below: without it,
            // `parquet_pushdown_enabled` is false on any DeletionVectors-feature
            // table and the leg scans with no parquet predicate. DV-bearing FILES
            // still disable the predicate per-file inside the fork.
            let mut builder = table.table_provider().with_pushdown_with_deletion_vectors(true);
            if let Some(selected) = file_selection {
                builder = builder.with_file_selection(FileSelection::from_file_paths(selected).with_missing_file_policy(MissingSelectedFilePolicy::Ignore));
            }
            if !ordinal_selections.is_empty() {
                debug!("tantivy row selection: {}/{} selections for {} files", cache_key.0, self.table_name, ordinal_selections.len());
                builder = builder.with_row_ordinal_selections(ordinal_selections);
            }
            if let Some(ss) = session_state {
                builder = builder.with_session(Arc::new(ss));
            }
            metrics::counter!(scan_metric_names::PRUNED_SELECT_US).increment(select_started.elapsed().as_micros() as u64);
            let build_started = std::time::Instant::now();
            let provider: Arc<dyn TableProvider> = Arc::new(builder.build().await.map_err(|e| DataFusionError::External(Box::new(e)))?);
            metrics::counter!(scan_metric_names::PRUNED_BUILD_US).increment(build_started.elapsed().as_micros() as u64);
            let scan_started = std::time::Instant::now();
            let plan = self.scan_via_provider(provider, state, projection, filters, limit).await;
            metrics::counter!(scan_metric_names::PRUNED_SCAN_US).increment(scan_started.elapsed().as_micros() as u64);
            metrics::counter!(scan_metric_names::PRUNED_CALLS).increment(1);
            return plan;
        }

        // Per-(project,table) provider cache: only rebuild when the Delta snapshot
        // version changes. Provider construction is parameter-independent, so the
        // cached value is correct for every query at the same version.
        let current_version = table.version().unwrap_or(0);
        // Optimistic read path: `get()` takes only a per-shard READ lock so
        // concurrent hits don't serialize. The provider build runs OUTSIDE any
        // lock — concurrent tasks await the same cell's single init.
        let ttl = self.database.config.cache.provider_cache_ttl();
        // Lookup is by EXACT version against the key's recent-version ring, so a
        // query never gets an older retained provider.
        let read_hit = self.database.delta_provider_cache.get(&cache_key).and_then(|entry| entry.get(current_version, ttl));
        let (cell, was_fresh_cell, brand_new_entry) = if let Some(c) = read_hit {
            (c, false, false)
        } else {
            // Eviction is deliberately miss-only: scanning the whole map on every
            // warm request would cost more than the build this cache removes.
            self.database.trim_delta_provider_cache();
            // Re-check after acquiring the entry lock: another thread may have
            // populated it between get() and entry() (DashMap can't upgrade locks).
            let entry = self.database.delta_provider_cache.entry(cache_key.clone());
            let brand_new = matches!(entry, dashmap::Entry::Vacant(_));
            let mut e = entry.or_default();
            match e.get(current_version, ttl) {
                Some(c) => (c, false, brand_new),
                None => (e.install(current_version, ttl), true, brand_new),
            }
        };
        let miss = was_fresh_cell || !cell.initialized();
        metrics::counter!(if miss { scan_metric_names::PROVIDER_CACHE_MISSES } else { scan_metric_names::PROVIDER_CACHE_HITS }).increment(1);
        // Threshold-multiple cadence keeps log volume tracking tenant growth,
        // not query rate.
        if brand_new_entry {
            let size = self.database.delta_provider_cache.len();
            if size >= CACHE_SOFT_LIMIT_WARN && size.is_multiple_of(CACHE_SOFT_LIMIT_WARN) {
                tracing::warn!(
                    target = "table_caches",
                    provider_cache_keys = size,
                    threshold = CACHE_SOFT_LIMIT_WARN,
                    "delta_provider_cache crossed soft limit (no eviction by design). Watch scan.provider_cache_entries in timefusion_stats."
                );
            }
        }
        // Bounded staleness: a task that captured the v=N cell before a concurrent
        // flush bumped the entry to v=N+1 still completes against v=N, so that one
        // query returns pre-flush data. Re-checking the version after the await
        // would reintroduce the per-query rebuild this cache exists to remove.
        let provider = cell
            .get_or_try_init(|| async {
                let started = std::time::Instant::now();
                let session_state = state.as_any().downcast_ref::<datafusion::execution::context::SessionState>().cloned();
                // Build with our session so the scan inherits
                // `schema_force_view_types=false`; delta-rs defaults to `true`
                // (BinaryView), which mismatches our Binary-typed MemBuffer at the
                // union and panics in physical planning. DV pushdown is safe here
                // because this is a READ-ONLY scan (DV writes need row positions).
                let result = if let Some(ss) = session_state {
                    table.table_provider().with_session(Arc::new(ss)).with_pushdown_with_deletion_vectors(true).await
                } else {
                    table.table_provider().with_pushdown_with_deletion_vectors(true).await
                };
                metrics::counter!(scan_metric_names::PROVIDER_BUILD_TOTAL).increment(1);
                metrics::counter!(scan_metric_names::PROVIDER_BUILD_US_TOTAL).increment(started.elapsed().as_micros() as u64);
                result.map_err(|e| DataFusionError::External(Box::new(e)))
            })
            .await?
            .clone();
        // Abandoned-build detection: the key's version ring no longer holds the
        // cell we built into, so the work was wasted. Non-zero means churn deeper
        // than `PROVIDER_VERSION_RETENTION`, or a TTL/capacity eviction.
        if let Some(current_entry) = self.database.delta_provider_cache.get(&cache_key)
            && !current_entry.holds(&cell)
        {
            metrics::counter!(scan_metric_names::PROVIDER_BUILD_ABANDONED).increment(1);
        }

        self.scan_via_provider(provider, state, projection, filters, limit).await
    }

    /// Build one Delta leg for complete/no Tantivy coverage, or two disjoint
    /// legs when sidecar coverage is partial. The covered leg is narrowed by
    /// the Tantivy id set; the uncovered leg evaluates the original filters.
    /// Both selections come from the exact snapshot held by `table`, so a
    /// concurrent compaction can at worst make the query use the older
    /// snapshot—it cannot move a file between the two legs or drop it.
    #[allow(clippy::too_many_arguments)]
    async fn scan_delta_with_tantivy(
        &self, table: &DeltaTable, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>, id_filter: Option<&Expr>,
        covered_files: Option<&HashSet<String>>, zero_hit_files: Option<&HashSet<String>>, row_selections: Option<&HashMap<String, Vec<u64>>>,
        query_time_range: Option<(i64, i64)>, bloom_rejected: Option<&HashSet<String>>, date_restrict: Option<&HashSet<String>>,
        file_restrict: Option<&HashSet<String>>,
    ) -> DFResult<Vec<Arc<dyn ExecutionPlan>>> {
        let narrow = |filters: &[Expr]| filters.iter().cloned().chain(id_filter.cloned()).collect::<Vec<_>>();
        // Per-date dedup skip: restrict this call's file universe to one side of
        // the certified/uncertified split, so two calls partition the in-window
        // files exactly once. An unattributable URI fails the test and lands on
        // the uncertified (still-deduped) side — the safe default.
        let in_dates = |uri: &String| date_restrict.is_none_or(|dates| crate::storage::date_partition_of(uri).is_some_and(|d| dates.contains(&d.to_string())));
        // Per-FILE dedup skip: the same restriction, one level finer.
        let in_files = |uri: &String| file_restrict.is_none_or(|files| crate::tantivy::search::parquet_rel_of_uri(uri).is_some_and(|rel| files.contains(rel)));
        let in_leg = |uri: &String| in_dates(uri) && in_files(uri);
        // Bloom-rejected rels apply to EVERY branch, including the raw
        // (uncovered) leg. `scan_delta_table` ignores `exclude_files` whenever an
        // include set exists, so the split path filters the live universe instead.
        let is_rejected = |uri: &String| bloom_rejected.is_some_and(|r| crate::tantivy::search::parquet_rel_of_uri(uri).is_some_and(|rel| r.contains(rel)));
        // Full-URI exclude set for the single-provider paths (include=None,
        // so exclude semantics apply), merged with any zero-hit excludes.
        let merged_exclude = |table: &DeltaTable| -> Option<HashSet<String>> {
            let rejected: HashSet<String> = bloom_rejected
                .filter(|r| !r.is_empty())
                .map(|_| table.get_file_uris().ok().map(|uris| uris.filter(|u| u.ends_with(".parquet") && is_rejected(u)).collect()).unwrap_or_default())
                .unwrap_or_default();
            match (rejected.is_empty(), zero_hit_files) {
                (true, _) => None, // caller falls back to zero_hit_files alone
                (false, Some(zero)) => Some(rejected.union(zero).cloned().collect()),
                (false, None) => Some(rejected),
            }
        };
        let (lo, hi) = query_time_range.unwrap_or((i64::MIN, i64::MAX));
        let Some(covered) = covered_files else {
            let narrowed = narrow(filters);
            let merged = merged_exclude(table);
            let exclude = merged.as_ref().or(zero_hit_files);
            // Under a split an exclude is not enough — an include is what bounds
            // this call to its side. Keyed on EITHER restriction: keying on
            // `date_restrict` alone leaves a file-only split unbounded, and both
            // legs then scan the whole table and double-count.
            let include: Option<HashSet<String>> = (date_restrict.is_some() || file_restrict.is_some())
                .then(|| {
                    Ok::<_, DataFusionError>(
                        table
                            .get_file_uris()
                            .map_err(|e| DataFusionError::External(Box::new(e)))?
                            .filter(|u| u.ends_with(".parquet") && uri_date_in_window(u, lo, hi) && in_leg(u) && !is_rejected(u))
                            .filter(|u| !exclude.is_some_and(|e| e.contains(u)))
                            .collect::<HashSet<String>>(),
                    )
                })
                .transpose()?;
            if include.as_ref().is_some_and(HashSet::is_empty) {
                return Ok(Vec::new()); // nothing on this side of the split
            }
            return Ok(vec![self.scan_delta_table(table, state, projection, &narrowed, limit, include.as_ref(), exclude, row_selections).await?]);
        };

        // The file-listing step is timed separately: materializing every URI is
        // the one step whose cost scales with the table, not with the query.
        let scan_started = std::time::Instant::now();
        let mut bloom_pruned_any = false;
        let uris_started = std::time::Instant::now();
        let all_uris = table.get_file_uris().map_err(|e| DataFusionError::External(Box::new(e)))?.collect::<Vec<_>>();
        metrics::counter!(scan_metric_names::TANTIVY_URIS_US).increment(uris_started.elapsed().as_micros() as u64);
        metrics::counter!(scan_metric_names::TANTIVY_LIVE_FILES).increment(all_uris.len() as u64);
        let live = all_uris
            .into_iter()
            .filter(|uri| uri.ends_with(".parquet") && uri_date_in_window(uri, lo, hi))
            .filter(|uri| {
                let rejected = is_rejected(uri);
                bloom_pruned_any |= rejected;
                !rejected
            })
            .filter(in_leg);
        let (mut indexed, raw): (HashSet<String>, HashSet<String>) = live.partition(|uri| covered.contains(uri));
        // One counter per failing conjunct; they are not exclusive, a scan can be
        // defeated by several at once.
        metrics::counter!(scan_metric_names::TANTIVY_SCAN_CALLS).increment(1);
        metrics::counter!(scan_metric_names::TANTIVY_RAW_FILES).increment(raw.len() as u64);
        for (defeated, name) in [
            (!raw.is_empty(), scan_metric_names::TANTIVY_SPLIT_RAW),
            (bloom_pruned_any, scan_metric_names::TANTIVY_SPLIT_BLOOM),
            (date_restrict.is_some() || file_restrict.is_some(), scan_metric_names::TANTIVY_SPLIT_DATE),
        ] {
            if defeated {
                metrics::counter!(name).increment(1);
            }
        }
        // Complete coverage keeps the single-provider fast path; the split is only
        // needed when the snapshot has raw debt.
        if raw.is_empty() && !indexed.is_empty() && !bloom_pruned_any && date_restrict.is_none() {
            let narrowed = narrow(filters);
            metrics::counter!(scan_metric_names::TANTIVY_FASTPATH).increment(1);
            let plan = self.scan_delta_table(table, state, projection, &narrowed, limit, None, zero_hit_files, row_selections).await?;
            metrics::counter!(scan_metric_names::TANTIVY_SCAN_US).increment(scan_started.elapsed().as_micros() as u64);
            return Ok(vec![plan]);
        }
        if let Some(zero) = zero_hit_files {
            indexed.retain(|uri| !zero.contains(uri));
        }

        let mut plans = Vec::with_capacity(2);
        if !indexed.is_empty() {
            let narrowed = narrow(filters);
            plans.push(self.scan_delta_table(table, state, projection, &narrowed, limit, Some(&indexed), None, row_selections).await?);
        }
        if !raw.is_empty() {
            plans.push(self.scan_delta_table(table, state, projection, filters, limit, Some(&raw), None, None).await?);
        }
        // Under a date split an empty side is a real empty side; the
        // unrestricted fallback below would read the OTHER side's files.
        if plans.is_empty() && date_restrict.is_some() {
            metrics::counter!(scan_metric_names::TANTIVY_SCAN_US).increment(scan_started.elapsed().as_micros() as u64);
            return Ok(Vec::new());
        }
        if plans.is_empty() {
            // Every in-window file bloom-rejected: an EMPTY include selection
            // yields a schema-correct zero-file scan, where falling through to the
            // unrestricted path would undo the pruning. Without bloom pruning,
            // `None` — the provider path is the conservative fallback.
            let none: HashSet<String> = HashSet::new();
            plans.push(self.scan_delta_table(table, state, projection, filters, limit, bloom_pruned_any.then_some(&none), None, None).await?);
        }
        metrics::counter!(scan_metric_names::TANTIVY_SCAN_US).increment(scan_started.elapsed().as_micros() as u64);
        Ok(plans)
    }

    /// Delta-only scan shared by `scan()`'s two Delta-alone callers.
    ///
    /// Re-derives `skip_dedup` against the resolved table so the verdict applies to the exact
    /// snapshot being read. When granted, restores the pushed `limit` — sound because nothing
    /// above a Delta-only scan drops rows except the tombstone filter.
    /// `readmit_mutable_filters` re-admits version-mutable-column predicates, safe only because
    /// a granted skip means no MemBuffer leg is in play.
    #[allow(clippy::too_many_arguments)]
    async fn scan_delta_only(
        &self, state: &dyn Session, projection: Option<&Vec<usize>>, optimized_filters: &[Expr], unstripped_filters: &[Expr], project_id: &str,
        query_time_range: Option<(i64, i64)>, dedup_keys: &[String], pre_skip_dedup: bool, tombstone: &Option<String>, orig_limit: Option<usize>,
        limit: Option<usize>, readmit_mutable_filters: bool, tantivy_id_filter: Option<&Expr>, tantivy_covered_files: Option<&HashSet<String>>,
        tantivy_exclude: Option<&HashSet<String>>, tantivy_row_selections: Option<&HashMap<String, Vec<u64>>>, bloom_rejected: Option<&HashSet<String>>,
    ) -> DFResult<(bool, Vec<Arc<dyn ExecutionPlan>>, Vec<Arc<dyn ExecutionPlan>>)> {
        let mut delta_only_filters = optimized_filters.to_vec();
        let delta_table = self.database.resolve_table(project_id, &self.table_name).await?;
        let table = delta_table.read().await;
        let (verdict, certified_dates) = self.dedup_skip_certified(&table, project_id, query_time_range, dedup_keys);
        let skip_dedup = pre_skip_dedup && verdict.granted();
        // Partial certification → the certified dates still skip. Only sound on
        // the Delta-only path, where no MemBuffer leg can hold an uncertified
        // newer version.
        let per_date_dates: HashSet<String> = if !skip_dedup && self.database.config.maintenance.timefusion_read_dedup_skip_per_date && !dedup_keys.is_empty() {
            certified_dates
        } else {
            HashSet::new()
        };
        // Complement within the window: what the DedupExec leg must still read.
        // Uses the same `window_dates` enumeration as certification, so the two
        // sides provably partition the window's dates.
        let uncertified_dates: HashSet<String> = query_time_range
            .filter(|_| !per_date_dates.is_empty())
            .and_then(|(lo, hi)| window_dates(lo, hi))
            .into_iter()
            .flatten()
            .map(|d| d.to_string())
            .filter(|d| !per_date_dates.contains(d))
            .collect();
        // Per-FILE split, tried only where the per-DATE one did not claim the
        // window: within an uncertified date, files a sweep proved clean can still
        // skip when no uncertified file overlaps them.
        let (certified_files, uncertified_files) = query_time_range
            .filter(|_| !skip_dedup && per_date_dates.is_empty() && !dedup_keys.is_empty())
            .map_or_else(Default::default, |window| self.database.certified_file_split(&table, project_id, &self.table_name, window));
        if skip_dedup && readmit_mutable_filters {
            let mutable = Self::version_mutable_columns(&self.table_name);
            let leg_safe = |f: &Expr| {
                !Self::references_tombstone(&self.table_name, f) && !mutable.as_ref().is_some_and(|m| f.column_refs().iter().any(|c| m.contains(&c.name)))
            };
            delta_only_filters.extend(unstripped_filters.iter().filter(|f| !leg_safe(f) && !Self::references_tombstone(&self.table_name, f)).cloned());
        }
        // Restoring the pushed limit is only sound when nothing above the
        // scan drops rows — the tombstone filter does, regardless of dedup.
        let eff_limit = if skip_dedup && tombstone.is_none() { orig_limit } else { limit };
        // Both sides of the split share every argument but the trailing
        // restrictions. A macro, not a closure: the closure form needs an HRTB
        // over the borrowed restriction sets.
        macro_rules! scan_side {
            ($dates:expr, $files:expr) => {
                self.scan_delta_with_tantivy(
                    &table,
                    state,
                    projection,
                    &delta_only_filters,
                    eff_limit,
                    tantivy_id_filter,
                    tantivy_covered_files,
                    tantivy_exclude,
                    tantivy_row_selections,
                    query_time_range,
                    bloom_rejected,
                    $dates,
                    $files,
                )
            };
        }
        let plans = scan_side!((!per_date_dates.is_empty()).then_some(&uncertified_dates), (!certified_files.is_empty()).then_some(&uncertified_files)).await?;
        // Returned separately so the caller can union the certified side ABOVE
        // DedupExec instead of feeding it through.
        let certified_plans = match (per_date_dates.is_empty(), certified_files.is_empty()) {
            (true, true) => Vec::new(),
            (by_date_empty, _) => scan_side!((!by_date_empty).then_some(&per_date_dates), (!certified_files.is_empty()).then_some(&certified_files)).await?,
        };
        if !certified_plans.is_empty() {
            let metric = if certified_files.is_empty() { scan_metric_names::DEDUP_SKIPPED_PER_DATE } else { scan_metric_names::DEDUP_SKIPPED_PER_FILE };
            metrics::counter!(metric).increment(1);
        }
        Ok((skip_dedup, plans, certified_plans))
    }

    /// Shared tail of the Delta scan: projection-index translation into the
    /// provider's schema, the provider scan itself, and type coercion.
    async fn scan_via_provider(
        &self, provider: Arc<dyn TableProvider>, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // DataFusion passes indices against `ProjectRoutingTable.schema`; the delta
        // provider expects indices against its own schema.
        let delta_schema = provider.schema();
        let translated_projection = projection.map(|proj| {
            proj.iter()
                .filter_map(|&idx| {
                    let col_name = self.schema.field(idx).name();
                    delta_schema.fields().iter().position(|f| f.name() == col_name).or_else(|| {
                        warn!("Column '{}' requested in projection but not found in Delta schema for table '{}'", col_name, self.table_name);
                        None
                    })
                })
                .collect::<Vec<_>>()
        });

        let started = std::time::Instant::now();
        let delta_plan = provider.scan(state, translated_projection.as_ref(), filters, limit).await;
        metrics::counter!(scan_metric_names::PROVIDER_SCAN_TOTAL).increment(1);
        metrics::counter!(scan_metric_names::PROVIDER_SCAN_US_TOTAL).increment(started.elapsed().as_micros() as u64);
        // Must run before anything reads the leg's ordering.
        let delta_plan = crate::read::optimizers::repair_isolated_scan_ordering(
            delta_plan?,
            self.database.config.memory.timefusion_read_sort_unordered_leg_max_mb.saturating_mul(1 << 20),
        )?;

        let target_schema = match projection {
            Some(proj) => Arc::new(arrow_schema::Schema::new(proj.iter().map(|&idx| self.schema.field(idx).clone()).collect::<Vec<_>>())),
            None => self.schema.clone(),
        };

        let coerced = Self::coerce_plan_to_schema(delta_plan, &target_schema)?;
        // Delta may leave predicates inexact, especially when a deletion vector
        // prevents Parquet filtering. Apply immutable predicates AFTER its row
        // masks, but BEFORE dedup has to retain every row in the selected files.
        let mutable = Self::version_mutable_columns(&self.table_name);
        let schema = coerced.schema();
        let predicate = filters
            .iter()
            .filter(|f| {
                !Self::references_tombstone(&self.table_name, f)
                    && f.column_refs().iter().all(|c| schema.index_of(&c.name).is_ok() && !mutable.as_ref().is_some_and(|m| m.contains(&c.name)))
            })
            .cloned()
            .reduce(Expr::and);
        let filtered: Arc<dyn ExecutionPlan> = if let Some(predicate) = predicate {
            let predicate = state.create_physical_expr(predicate, &schema.as_ref().clone().try_into()?)?;
            Arc::new(datafusion::physical_plan::filter::FilterExec::try_new(predicate, coerced)?)
        } else {
            coerced
        };
        self.gate_if_wide(filtered, filters)
    }

    /// How far back a scan reaches (`now - min_ts`), in micros. `None` = no
    /// lower time bound, i.e. infinitely deep. Depth, not raw window width, so
    /// the hot one-sided `>= now()-1h` dashboard (whose max is open-ended)
    /// reads as shallow while a `[30d ago, 29d ago]` history slice does not.
    fn scan_lookback_micros(&self, filters: &[Expr]) -> Option<i64> {
        self.extract_time_range_from_filters(filters).and_then(|(min, _)| (min != i64::MIN).then(|| crate::support::now_micros().saturating_sub(min)))
    }

    /// Selected file bytes above which a scan is recorded as a process-risk candidate. Not a
    /// limit: nothing is refused at this size.
    const WIDE_SCAN_OVERSIZE_BYTES: u64 = 1024 * 1024 * 1024;

    /// Wrap a "wide" Delta scan — one reaching further back than the configured
    /// lookback, or with no lower time bound at all, where a one-sided
    /// `timestamp >= cutoff` can't prune files past the date cut and every
    /// file's row groups are fully decoded — so its Parquet decoding draws from
    /// the shared `heavy_scan_sem`, bounding concurrent decode heap across all
    /// queries.
    fn gate_if_wide(&self, plan: Arc<dyn ExecutionPlan>, filters: &[Expr]) -> DFResult<Arc<dyn ExecutionPlan>> {
        let depth = self.scan_lookback_micros(filters);
        let deeper_than = |micros: i64| depth.is_none_or(|d| d > micros);
        let mem = &self.database.config.memory;
        if !deeper_than((mem.timefusion_wide_scan_lookback_hours as i64).saturating_mul(3_600_000_000)) {
            return Ok(plan);
        }
        // Depth is only a proxy for decode heap, and pruning breaks it: a deep
        // query on a well-pruned partition may select one small file. Refine with
        // what the plan ACTUALLY selected (pruning has already run). This only
        // ever *releases* a scan the depth rule would gate, never adds one.
        // `None` = no readable file groups, so fall back to depth alone.
        let selected = selected_file_work(&plan);
        if let Some((files, bytes)) = selected
            && files <= mem.timefusion_wide_scan_max_files
            && bytes <= mem.timefusion_wide_scan_max_mb.saturating_mul(1 << 20)
        {
            return Ok(plan);
        }
        // Record every gated scan's size; nothing is refused on it. The gate below
        // bounds how MANY wide scans decode at once, never how much any one of
        // them decodes, so this is the only place that sees a single query large
        // enough to take the process down.
        if let Some((files, bytes)) = selected {
            metrics::histogram!(scan_metric_names::WIDE_SCAN_SELECTED_MB).record((bytes / (1 << 20)) as f64);
            if bytes > Self::WIDE_SCAN_OVERSIZE_BYTES {
                metrics::counter!(scan_metric_names::WIDE_SCAN_OVERSIZE_TOTAL).increment(1);
                warn!(
                    event = "wide_scan_oversize",
                    table.name = %self.table_name,
                    selected_files = files,
                    selected_mb = bytes / (1 << 20),
                    threshold_mb = Self::WIDE_SCAN_OVERSIZE_BYTES / (1 << 20),
                    "wide scan selected more than the oversize threshold"
                );
            }
        }
        let bypass_cache = self.database.config.cache.cache_bypass_scan_micros().is_some_and(deeper_than);
        Ok(Arc::new(GatedScanExec::new(
            plan,
            self.database.heavy_scan_sem.clone(),
            Some(self.database.scan_metrics.clone()),
            bypass_cache,
            mem.timefusion_max_concurrent_scan_readers.max(1) as u32 * DECODE_UNITS_PER_READER,
        )))
    }

    /// Lead sort key that makes `DedupExec`'s keep-greatest engage.
    ///
    /// The table's first declared sorting column, but only when the table declares a `dedup_tiebreak`
    /// and that column is itself a dedup key of an i64-backed type. Equal dedup keys then share the
    /// bound value, so all versions of a row live in one contiguous run and the operator can emit
    /// without buffering the scan. One column, not the whole sort order, keeps the injected sort
    /// cheap.
    pub(crate) fn keep_greatest_ordering(table: &crate::schema::TableSchema, leg_schema: &SchemaRef) -> Option<datafusion::physical_expr::LexOrdering> {
        use datafusion::{
            arrow::{compute::SortOptions, datatypes::DataType},
            physical_expr::{LexOrdering, PhysicalSortExpr},
        };
        table.dedup_tiebreak.as_ref()?;
        let sc = table.sorting_columns.first().filter(|sc| table.dedup_keys.contains(&sc.name))?;
        let idx = leg_schema.index_of(&sc.name).ok()?;
        matches!(leg_schema.field(idx).data_type(), DataType::Int64 | DataType::Timestamp(..)).then_some(())?;
        let opts = SortOptions { descending: sc.descending, nulls_first: sc.nulls_first };
        LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(PhysicalColumn::new(&sc.name, idx)), opts)])
    }

    /// Columns whose value can differ between versions of one row (`None` for
    /// tables that append no versions).
    ///
    /// A filter on a column NOT in this set is safe below the merge-on-read
    /// `DedupExec` (all versions agree, so dedup-then-filter equals
    /// filter-then-dedup); a filter on a column IN it must stay above, or a
    /// stale version could match a predicate the winner no longer satisfies.
    ///
    /// Immutable is the default; mutable is declared. The tiebreak and tombstone
    /// are mutable by construction.
    pub(crate) fn version_mutable_columns(table_name: &str) -> Option<HashSet<String>> {
        let schema = crate::schema::get_schema(table_name).filter(|s| s.version_append)?;
        Some(
            schema
                .fields
                .iter()
                .filter(|field| field.mutable)
                .map(|field| field.name.clone())
                .chain(schema.dedup_tiebreak.clone())
                .chain(schema.tombstone_column.clone())
                .collect(),
        )
    }

    /// Does `f` mention the table's tombstone marker? Such a predicate must never reach a scan leg.
    ///
    /// Applied at the source it would drop the tombstone row before the dedup, letting the older
    /// live version win keep-greatest and resurrecting a deleted row.
    pub(crate) fn references_tombstone(table_name: &str, f: &Expr) -> bool {
        crate::schema::get_schema(table_name).and_then(|s| s.tombstone_column.as_deref()).is_some_and(|t| f.column_refs().iter().any(|c| c.name == t))
    }

    /// `(column, name)` pairs for an identity projection over `idxs` — each index
    /// kept as-is, by position, with its own field name.
    fn identity_projection_exprs(schema: &SchemaRef, idxs: impl Iterator<Item = usize>) -> Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> {
        idxs.map(|i| {
            (Arc::new(PhysicalColumn::new(schema.field(i).name(), i)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>, schema.field(i).name().clone())
        })
        .collect()
    }

    /// Restore a requested column set from an augmented scan, by index into the input.
    /// Mirrors what `DedupExec`'s `output_projection` does for paths that skip it.
    fn project_indices(plan: Arc<dyn ExecutionPlan>, idxs: &[usize]) -> DFResult<Arc<dyn ExecutionPlan>> {
        let schema = plan.schema();
        if idxs.len() == schema.fields().len() && idxs.iter().enumerate().all(|(i, &j)| i == j) {
            return Ok(plan);
        }
        let exprs = Self::identity_projection_exprs(&schema, idxs.iter().copied());
        Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
    }

    fn filter_tombstones(plan: Arc<dyn ExecutionPlan>, marker: &str, keep: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
        use datafusion::{physical_expr::expressions::binary, physical_plan::filter::FilterExec};
        let schema = plan.schema();
        let Ok(idx) = schema.index_of(marker) else { return Ok(plan) };
        let live = binary(
            Arc::new(PhysicalColumn::new(marker, idx)),
            Operator::IsDistinctFrom,
            datafusion::physical_expr::expressions::lit(ScalarValue::Boolean(Some(true))),
            &schema,
        )?;
        let filtered = Arc::new(FilterExec::try_new(live, plan)?) as Arc<dyn ExecutionPlan>;
        let Some(k) = keep.filter(|&k| k < schema.fields().len()) else { return Ok(filtered) };
        let exprs = Self::identity_projection_exprs(&schema, 0..k);
        Ok(Arc::new(ProjectionExec::try_new(exprs, filtered)?))
    }

    /// Wrap an execution plan with type coercion if the output schema doesn't match the target.
    /// This handles cases like Delta returning Utf8 when we expect Utf8View.
    fn coerce_plan_to_schema(plan: Arc<dyn ExecutionPlan>, target_schema: &SchemaRef) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plan_schema = plan.schema();
        if plan_schema.fields().len() != target_schema.fields().len() {
            return Ok(plan);
        }

        // Variant inner storage may be Struct{Binary,Binary} or
        // Struct{BinaryView,BinaryView} depending on which session built the plan;
        // the kernels accept both, so coercing Variant fields is pure overhead.
        let differs = |plan_field: &arrow_schema::Field, target_field: &arrow_schema::Field| {
            plan_field.data_type() != target_field.data_type() && !crate::schema::is_variant_type(target_field.data_type())
        };

        if !plan_schema.fields().iter().zip(target_schema.fields()).any(|(plan_field, target_field)| differs(plan_field, target_field)) {
            return Ok(plan);
        }

        let cast_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> = plan_schema
            .fields()
            .iter()
            .enumerate()
            .zip(target_schema.fields())
            .map(|((idx, plan_field), target_field)| {
                let col_expr = Arc::new(PhysicalColumn::new(plan_field.name(), idx)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
                let expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                    if differs(plan_field, target_field) { Arc::new(CastExpr::new(col_expr, target_field.data_type().clone(), None)) } else { col_expr };
                (expr, target_field.name().clone())
            })
            .collect();

        Ok(Arc::new(ProjectionExec::try_new(cast_exprs, plan)?))
    }

    /// True iff every `(project, date)` partition in the query window carries a clean fingerprint
    /// that still matches the live file set, plus the certified subset for a partial (per-date) skip.
    ///
    /// Only consulted on Delta-only paths; mem∪delta overlap still needs `DedupExec`.
    ///
    /// `version_append` (merge-on-read) is NOT a reason to refuse: on a partition
    /// the sweep certified duplicate-free there is exactly one winning row per key
    /// (the sweep collapses versions keep-greatest), and `filter_tombstones` runs
    /// outside `match dedup_on`, so a skip cannot resurrect a deleted row.
    fn dedup_skip_certified(
        &self, table: &DeltaTable, project_id: &str, window: Option<(i64, i64)>, dedup_keys: &[String],
    ) -> (DedupSkipVerdict, HashSet<String>) {
        match window {
            _ if dedup_keys.is_empty() || !self.database.config.maintenance.timefusion_read_dedup_skip_swept => (DedupSkipVerdict::Disabled, HashSet::new()),
            None => (DedupSkipVerdict::NoWindow, HashSet::new()),
            Some(w) => self.database.dedup_window_certified(table, project_id, &self.table_name, w),
        }
    }

    fn dedup_skip_allowed(&self, table: &DeltaTable, project_id: &str, window: Option<(i64, i64)>, dedup_keys: &[String]) -> DedupSkipVerdict {
        self.dedup_skip_certified(table, project_id, window, dedup_keys).0
    }

    /// Extract time range (min, max) from query filters.
    /// Returns None if no time constraints found.
    fn extract_time_range_from_filters(&self, filters: &[Expr]) -> Option<(i64, i64)> {
        use crate::read::optimizers::{is_col_through_cast, swap_comparison};
        // Literal bound → microseconds. Strict (no Cast unwrap) so a cast-to-a-
        // different-unit literal yields None (→ widest window) rather than a
        // wrong-narrow one that could prune indexes holding matching rows.
        fn literal_micros(e: &Expr) -> Option<i64> {
            match e {
                Expr::Literal(ScalarValue::TimestampMicrosecond(Some(ts), _), _) => Some(*ts),
                Expr::Literal(ScalarValue::TimestampNanosecond(Some(ts), _), _) => Some(*ts / 1000),
                Expr::Literal(ScalarValue::TimestampMillisecond(Some(ts), _), _) => Some(*ts * 1000),
                Expr::Literal(ScalarValue::TimestampSecond(Some(ts), _), _) => Some(*ts * 1_000_000),
                _ => None,
            }
        }

        let (min_ts, max_ts) = filters.iter().fold((None::<i64>, None::<i64>), |acc @ (min_ts, max_ts), filter| {
            let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter else { return acc };
            // Accept `timestamp <op> lit`, `lit <op> timestamp` (operands
            // reversed → flip the comparison), and a Cast-wrapped column.
            let (ts_value, op) = if is_col_through_cast(left, "timestamp") {
                (literal_micros(right), *op)
            } else if is_col_through_cast(right, "timestamp") {
                (literal_micros(left), swap_comparison(*op))
            } else {
                return acc;
            };
            let Some(ts) = ts_value else { return acc };
            match op {
                Operator::Gt | Operator::GtEq => (Some(min_ts.map_or(ts, |m| m.max(ts))), max_ts),
                Operator::Lt | Operator::LtEq => (min_ts, Some(max_ts.map_or(ts, |m| m.min(ts)))),
                Operator::Eq => (Some(ts), Some(ts)),
                _ => acc,
            }
        });

        if min_ts.is_some() || max_ts.is_some() {
            return Some((min_ts.unwrap_or(i64::MIN), max_ts.unwrap_or(i64::MAX)));
        }
        // No timestamp bound — fall back to a `date = X` partition equality
        // (exactly one day). Sound because certification is keyed by (project, date).
        date_partition_window(filters)
    }
}

/// The micros span of a `date = <Date32>` equality, if the filters carry one.
pub(crate) fn date_partition_window(filters: &[Expr]) -> Option<(i64, i64)> {
    use crate::read::optimizers::is_col_through_cast;
    const DAY_MICROS: i64 = 86_400_000_000;
    filters.iter().find_map(|filter| {
        let Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) = filter else { return None };
        let literal = match (is_col_through_cast(left, "date"), is_col_through_cast(right, "date")) {
            (true, _) => right.as_ref(),
            (_, true) => left.as_ref(),
            _ => return None,
        };
        let Expr::Literal(ScalarValue::Date32(Some(days)), _) = literal else { return None };
        let start = i64::from(*days).checked_mul(DAY_MICROS)?;
        // Inclusive end, one microsecond short of the next day.
        Some((start, start.checked_add(DAY_MICROS - 1)?))
    })
}

/// Files and bytes a scan will actually open, read off the plan's file groups
/// AFTER pruning. `None` when the plan carries no file scan (so the caller must
/// not read "no files" as "no work").
pub(crate) fn selected_file_work(plan: &Arc<dyn ExecutionPlan>) -> Option<(usize, u64)> {
    if let Some(src) = (plan.as_ref() as &dyn std::any::Any).downcast_ref::<DataSourceExec>()
        && let Some(conf) = (src.data_source().as_ref() as &dyn std::any::Any).downcast_ref::<FileScanConfig>()
    {
        let files = conf.file_groups.iter().flat_map(|g| g.files());
        return Some(files.fold((0, 0), |(n, b), f| (n + 1, b + f.object_meta.size)));
    }
    // Fold children so a `None` child (a leg with no file scan, e.g. the
    // in-memory leg) doesn't erase a sibling's real work.
    plan.children().into_iter().filter_map(selected_file_work).reduce(|(n, b), (n2, b2)| (n + n2, b + b2))
}

/// Decode-admission pressure valve: how many of the wide-scan semaphore's `total` permits one decode
/// poll must claim. 1 normally; a quarter of the pool at tier 1; the whole pool at tier 2.
///
/// Decode heap is untracked by any DataFusion pool, so the only OOM lever is concurrency. Tiers
/// fire on projected seconds to the limit, with absolute percentage backstops. Pressure is memcg
/// usage minus reclaimable cache, sampled at most every 250ms.
pub(crate) fn scan_pressure_permits(total: u32) -> u32 {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    static EPOCH: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    static SAMPLED_AT_MS: AtomicU64 = AtomicU64::new(u64::MAX);
    static USAGE_PCT: AtomicU64 = AtomicU64::new(0);
    static PREV_USED: AtomicU64 = AtomicU64::new(0);
    /// Smoothed growth rate in bytes/sec. Saturating at 0 on a fall: a
    /// *shrinking* process is never heading for the wall, and letting negative
    /// rates into the average would mask a burst that follows a big free.
    static RATE_BPS: AtomicU64 = AtomicU64::new(0);
    /// Projected seconds until usage reaches the limit at the current rate.
    static ETA_SECS: AtomicU64 = AtomicU64::new(u64::MAX);
    let now_ms = EPOCH.get_or_init(std::time::Instant::now).elapsed().as_millis() as u64;
    let last = SAMPLED_AT_MS.load(Relaxed);
    if last == u64::MAX || now_ms.saturating_sub(last) >= 250 {
        SAMPLED_AT_MS.store(now_ms, Relaxed);
        let limit = crate::config::try_config().map_or(0, |c| c.derived.memory_limit_bytes);
        let used = process_memory_bytes().unwrap_or(0);
        let pct = (used * 100).checked_div(limit).unwrap_or(0) as u64;
        let prev_used = PREV_USED.swap(used as u64, Relaxed);
        let dt_ms = now_ms.saturating_sub(last);
        // First sample (no `last`) has no interval to differentiate over.
        if last != u64::MAX && dt_ms > 0 && prev_used > 0 {
            let gained = (used as u64).saturating_sub(prev_used);
            let sample_bps = gained.saturating_mul(1000) / dt_ms;
            // EWMA over ~4 samples (~1s): rejects a noisy read without blunting a burst.
            let prev_rate = RATE_BPS.load(Relaxed);
            RATE_BPS.store((prev_rate * 3 + sample_bps) / 4, Relaxed);
        }
        let rate = RATE_BPS.load(Relaxed);
        let headroom = (limit as u64).saturating_sub(used as u64);
        let eta = headroom.checked_div(rate).unwrap_or(u64::MAX);
        ETA_SECS.store(eta, Relaxed);
        let prev_pct = USAGE_PCT.swap(pct, Relaxed);
        // Logged, not just counted: counters die with the process, an OOM post-mortem needs the log.
        let was = pressure_permit_claim_at(prev_pct, u64::MAX, total);
        let now = pressure_permit_claim_at(pct, eta, total);
        if was != now {
            warn!(
                "scan pressure valve: {prev_pct}% -> {pct}% of cgroup limit, growth {} MB/s, projected {} to limit, decode permit claim {was} -> {now} (of {total})",
                rate / (1024 * 1024),
                if eta == u64::MAX { "never".to_string() } else { format!("{eta}s") }
            );
        }
    }
    pressure_permit_claim_at(USAGE_PCT.load(Relaxed), ETA_SECS.load(Relaxed), total)
}

/// First tier: long enough to drain in-flight decodes and have the throttle mean
/// something (a ~450 MB/s burst crosses 40 GB of headroom in ~90s).
const VALVE_ETA_TIER1_SECS: u64 = 90;
/// Second tier: fully serialize.
const VALVE_ETA_TIER2_SECS: u64 = 30;
/// The projected tiers apply only from this share of the limit; below it a short
/// projection means "filling fast from cold", not "about to hit the wall".
const VALVE_RATE_FLOOR_PCT: u64 = 50;

/// Sub-divisions of one reader slot in the wide-scan gate.
///
/// The gate bounds Parquet decode heap, which is untracked by the DataFusion memory pool.
/// A poll is charged for the heap it actually produced: a full-size batch claims all `K` units,
/// a tiny batch claims one, so the ceiling is unchanged. Do not raise
/// `timefusion_max_concurrent_scan_readers` instead — that would raise the heap ceiling.
pub(crate) const DECODE_UNITS_PER_READER: u32 = 16;

/// Worst-case decoded batch size one reader slot is sized for: 8192-row batches of wide OTel
/// rows reach ~145 MB.
pub(crate) const NOMINAL_DECODE_BATCH_BYTES: u64 = 145 * 1024 * 1024;

/// Units a poll must claim to cover `last_batch_bytes` of decoded Arrow.
///
/// `0` means "not yet known" (a stream's first poll) and claims a whole reader slot; the claim
/// adapts down once the stream has shown what its batches weigh. Never exceeds one slot (heap
/// ceiling) and never zero (progress).
pub(crate) fn decode_units(last_batch_bytes: u64) -> u32 {
    let k = u64::from(DECODE_UNITS_PER_READER);
    match last_batch_bytes {
        0 => DECODE_UNITS_PER_READER,
        b => (b.saturating_mul(k).div_ceil(NOMINAL_DECODE_BATCH_BYTES)).clamp(1, k) as u32,
    }
}

/// Tier math for `scan_pressure_permits`, separated for testability.
///
/// Permits one decode poll must claim, given both current usage and its rate. Whichever tier is
/// reached first wins. The projection is gated on [`VALVE_RATE_FLOOR_PCT`] so a process filling
/// from cold — fast growth, plateauing far from the limit — is not throttled.
pub(crate) fn pressure_permit_claim_at(usage_pct: u64, eta_secs: u64, total: u32) -> u32 {
    let projected = usage_pct >= VALVE_RATE_FLOOR_PCT;
    if usage_pct >= 95 || (projected && eta_secs <= VALVE_ETA_TIER2_SECS) {
        total
    } else if usage_pct >= 88 || (projected && eta_secs <= VALVE_ETA_TIER1_SECS) {
        (total / 4).max(1)
    } else {
        1
    }
}

/// Concurrency-gate a wide read scan.
///
/// Each output partition acquires a permit around every batch decode, bounding the number of
/// Parquet row groups decoded at once across all wide queries. Parquet decode heap is untracked by
/// the DataFusion memory pool, so unbounded parallelism can OOM the process. Acquisition is
/// per-batch, not per-stream: holding a permit for a partition's whole lifetime would deadlock
/// `SortPreservingMergeExec`, which needs one batch from every input before it can emit.
#[derive(Debug)]
pub(crate) struct GatedScanExec {
    input: Arc<dyn ExecutionPlan>,
    sem: Arc<tokio::sync::Semaphore>,
    properties: Arc<PlanProperties>,
    /// Decode accounting only — this operator never denies on memory.
    metrics: Option<Arc<ScanMetrics>>,
    /// Scan-resistant admission: a scan reading history must not evict the hot tail.
    bypass_cache: bool,
    /// Size of `sem`'s pool — `scan_pressure_permits` scales its claim off it
    /// (tokio semaphores don't expose their initial size).
    pool_size: u32,
}

impl GatedScanExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>, sem: Arc<tokio::sync::Semaphore>, metrics: Option<Arc<ScanMetrics>>, bypass_cache: bool, pool_size: u32,
    ) -> Self {
        let properties = input.properties().clone();
        Self { input, sem, properties, metrics, bypass_cache, pool_size }
    }
}

impl DisplayAs for GatedScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(f, "GatedScanExec: permits={}", self.sem.available_permits()),
            _ => write!(f, "GatedScanExec"),
        }
    }
}

impl ExecutionPlan for GatedScanExec {
    fn name(&self) -> &'static str {
        "GatedScanExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone(), self.sem.clone(), self.metrics.clone(), self.bypass_cache, self.pool_size)))
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let inner = self.input.execute(partition, context)?;
        let schema = inner.schema();
        let sem = self.sem.clone();
        let metrics = self.metrics.clone();
        let bypass = self.bypass_cache;
        let pool_size = self.pool_size;
        // Hold a permit only across each `poll_next` (one batch decode), then release so other
        // partitions/queries can proceed. `last_bytes` is this stream's most recent decoded
        // batch size, so the claim adapts to what this scan actually produces.
        let gated = futures::stream::unfold((inner, 0u64), move |(mut inner, last_bytes)| {
            let sem = sem.clone();
            let metrics = metrics.clone();
            async move {
                // Near the OOM line each poll claims more of the pool, shrinking effective
                // decode concurrency. The claim never exceeds the pool size, so progress
                // is guaranteed.
                let want = scan_pressure_permits(pool_size).max(decode_units(last_bytes)).min(pool_size);
                let _permit = sem.acquire_many_owned(want).await.ok()?;
                if let Some(m) = &metrics {
                    m.decode_begin();
                    if want > DECODE_UNITS_PER_READER {
                        metrics::counter!(scan_metric_names::DECODE_PRESSURE_THROTTLED).increment(1);
                    }
                }
                // The object-store fetches for this batch happen inside the poll, so the
                // bypass scope covers exactly them.
                let next = match bypass {
                    true => crate::storage::scan_bypass_scope(true, futures::StreamExt::next(&mut inner)).await,
                    false => futures::StreamExt::next(&mut inner).await,
                };
                let produced = next.as_ref().and_then(|r| r.as_ref().ok()).map_or(0, |b: &RecordBatch| b.get_array_memory_size() as u64);
                if let Some(m) = &metrics {
                    m.decode_end(produced);
                }
                next.map(|item| (item, (inner, produced)))
            }
        });
        // Same cancellation contract as `DedupExec`: the wrapper guarantees a budget is
        // consumed per batch so the statement deadline stays observable.
        Ok(datafusion::physical_plan::coop::make_cooperative(Box::pin(RecordBatchStreamAdapter::new(schema, gated))))
    }
}

// Needed by DataSink
impl DisplayAs for ProjectRoutingTable {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ProjectRoutingTable ")
    }
}

#[async_trait]
impl DataSink for ProjectRoutingTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    #[instrument(
        name = "datafusion.table.write",
        skip_all,
        fields(
            table.name = %self.table_name,
            operation = "INSERT",
            rows.count = Empty,
            projects.count = Empty,
        )
    )]
    async fn write_all(&self, mut data: SendableRecordBatchStream, _context: &Arc<TaskContext>) -> DFResult<u64> {
        let span = tracing::Span::current();
        let mut total_row_count = 0;
        let mut project_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let target_schema = self.real_schema();
        // Convert Utf8/Utf8View columns into Variant structs where the target schema expects
        // Variant (schema() presents Variant cols as Utf8View so INSERT literals type-check),
        // then partition each batch row-wise by project_id — one batch may carry many projects.
        while let Some(batch) = data.next().await.transpose()? {
            let batch_rows = batch.num_rows();
            debug!("write_all: received batch with {} rows", batch_rows);
            total_row_count += batch_rows;
            let batch = normalize_timestamp_tz(batch)?;
            let converted = convert_variant_columns(batch, &target_schema)?;
            for (project_id, sub) in partition_batch_by_project(converted, &self.default_project)? {
                project_batches.entry(project_id).or_default().push(sub);
            }
        }

        span.record("rows.count", total_row_count);
        span.record("projects.count", project_batches.len());

        if project_batches.is_empty() {
            return Ok(0);
        }

        // Distinct projects → distinct Delta tables/WAL shards, so these can run concurrently.
        let writes = project_batches.into_iter().map(|(project_id, batches)| {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            debug!("write_all: inserting {} batches with {} total rows for project {}", batches.len(), row_count, project_id);
            let insert_span = tracing::trace_span!(parent: &span, "delta_table.insert", project_id = %project_id, rows = row_count);
            async move {
                self.database
                    .insert_records_batch(&project_id, &self.table_name, batches, self.skip_queue, None)
                    .instrument(insert_span)
                    .await
                    .map_err(|e| DataFusionError::Execution(format!("Insert error for project {} table {}: {}", project_id, self.table_name, e)))
            }
        });
        futures::future::try_join_all(writes).await?;

        debug!("write_all: completed insertion of {} total rows", total_row_count);
        Ok(total_row_count as u64)
    }
}

/// Outcome of [`decide_prefilter`]: either why the tantivy prefilter was
/// skipped, or the narrowing it proved sound to apply.
enum PrefilterDecision {
    Skipped(&'static str),
    Used { ids: HashSet<String>, covered_files: HashSet<String>, exclude_files: Option<HashSet<String>>, row_selections: Option<HashMap<String, Vec<u64>>> },
}

/// Whether the routed predicate can differ between versions of the same key.
/// Such predicates cannot safely narrow only the indexed storage leg: a newer
/// nonmatching row could be removed while an older matching row remains in memory
/// or an uncovered file. They require global candidate discovery or winner masks.
fn routed_touches_mutable(mutable: Option<&HashSet<String>>, tree: Option<&crate::tantivy::udf::PredNode>) -> bool {
    mutable.zip(tree).is_some_and(|(m, t)| t.columns().iter().any(|c| m.contains(*c)))
}

/// Why one slice's coverage was refused. Mirrors `slice_coverage_agrees`'s FALSE branches.
///
/// `num_records` counts PHYSICAL rows (tombstones and every merge-on-read version) while the
/// build aggregated LOGICAL rows through `DedupExec`, so `grew` means rows really arrived while
/// `shrank` may just be dedup/compaction/vacuum collapsing rows the logical set never held.
/// Both are still refused — this only names them.
pub(crate) fn stale_coverage_metric(witness: Option<u64>, current: Option<u64>) -> &'static str {
    match (witness, current) {
        (None, _) => scan_metric_names::ROLLUP_STALE_NO_WITNESS,
        (_, None) => scan_metric_names::ROLLUP_STALE_NO_SOURCE_ROWS,
        (Some(witness), Some(current)) if current < witness => scan_metric_names::ROLLUP_STALE_SHRANK,
        _ => scan_metric_names::ROLLUP_STALE_GREW,
    }
}

#[cfg(test)]
mod stale_coverage_metric_tests {
    use super::*;

    /// Every case that reaches the classifier is one `slice_coverage_agrees`
    /// rejected, so the two must not drift: anything this names must be a slice
    /// the read path actually refused.
    #[test]
    fn each_refusal_gets_its_own_name_and_only_refusals_reach_it() {
        for (witness, current, expected) in [
            (None, Some(10), scan_metric_names::ROLLUP_STALE_NO_WITNESS),
            (None, None, scan_metric_names::ROLLUP_STALE_NO_WITNESS),
            (Some(10), None, scan_metric_names::ROLLUP_STALE_NO_SOURCE_ROWS),
            // Witness 9, live 10: rows ARRIVED since the build — really stale.
            (Some(9), Some(10), scan_metric_names::ROLLUP_STALE_GREW),
            // Witness 11, live 10: physical rows were collapsed; the logical set may be untouched.
            (Some(11), Some(10), scan_metric_names::ROLLUP_STALE_SHRANK),
        ] {
            assert!(!crate::rollup::slice_coverage_agrees(&[witness], current), "{witness:?}/{current:?} must be a refusal to be classified");
            assert_eq!(stale_coverage_metric(witness, current), expected, "for witness={witness:?} current={current:?}");
        }
        // The one agreeing case never reaches the classifier.
        assert!(crate::rollup::slice_coverage_agrees(&[Some(10)], Some(10)));
    }

    /// Every refusal `search_detailed` can return must have a metric, or it
    /// vanishes from the breakdown — the `debug_assert` in
    /// `record_prefilter_skip` only fires if a query happens to hit that path.
    #[test]
    fn every_search_refusal_is_a_registered_prefilter_reason() {
        for reason in ["delta_no_index", "delta_no_usable_index", "delta_cap_exceeded_one_index", "delta_cap_exceeded_combined", "delta_error"] {
            assert!(scan_metric_names::prefilter_skip_metric(reason).is_some(), "{reason} would vanish from the breakdown");
        }
        // Distinct names, or the split buys nothing.
        let names: std::collections::HashSet<_> = scan_metric_names::PREFILTER_SKIP_REASONS.iter().map(|(_, metric)| *metric).collect();
        assert_eq!(names.len(), scan_metric_names::PREFILTER_SKIP_REASONS.len());
    }
}

/// Charge one prefilter skip to the total AND to its reason.
///
/// The total alone is not attributable: it is incremented from two call sites, so a high skip
/// rate could be a decision or a missing index, and the fix differs.
fn record_prefilter_skip(reason: &'static str) {
    crate::observability::record_tantivy_prefilter_skipped();
    metrics::counter!(scan_metric_names::PREFILTER_SKIPPED).increment(1);
    let named = scan_metric_names::prefilter_skip_metric(reason);
    debug_assert!(named.is_some(), "unregistered prefilter skip reason {reason:?} — it would vanish from the breakdown");
    if let Some(name) = named {
        metrics::counter!(name).increment(1);
    }
}

#[allow(clippy::too_many_arguments)]
fn decide_prefilter(
    ids: HashSet<String>, indexed_rows: u64, min_selectivity_pct: u64, field_gap: bool, covered_files: HashSet<String>, zero_hit_files: HashSet<String>,
    row_selections: HashMap<String, Vec<u64>>, is_mutable: bool, file_pruning_enabled: bool, row_selection_enabled: bool,
) -> PrefilterDecision {
    // No indexed rows = no useful prefilter. Without this guard we'd emit an
    // empty IN(...) list that zeros the Delta scan even when matching rows
    // exist there (e.g. data written directly without triggering an index build).
    if indexed_rows == 0 {
        return PrefilterDecision::Skipped("empty_index");
    }
    // Selectivity cutoff: if the hit set covers most of the indexed rows, the IN-list won't
    // prune enough to be worth its planning cost. The original predicate is the backstop.
    if (ids.len() as u64) * 100 >= indexed_rows * min_selectivity_pct {
        return PrefilterDecision::Skipped("low_selectivity");
    }
    // An in-window index lacked one of the queried fields (schema evolution
    // added a tantivy column after it was built). It can't answer that
    // predicate yet appears "covered", so the IN-list would drop its rows.
    if field_gap {
        return PrefilterDecision::Skipped("field_coverage_gap");
    }
    if is_mutable {
        return PrefilterDecision::Skipped("mutable_visibility");
    }
    let exclude_files = (file_pruning_enabled && !zero_hit_files.is_empty()).then_some(zero_hit_files);
    let row_selections = (row_selection_enabled && !row_selections.is_empty()).then_some(row_selections);
    PrefilterDecision::Used { ids, covered_files, exclude_files, row_selections }
}

#[cfg(test)]
mod decide_prefilter_tests {
    use super::*;
    use test_case::test_case;

    /// The narrowing itself: which predicates count as touching a mutable
    /// column. Getting this wrong in the permissive direction is a correctness
    /// bug on a merge-on-read table, so pin every arm.
    #[test]
    fn routed_touches_mutable_only_when_a_predicate_column_is_mutable() {
        use crate::tantivy::udf::{PredNode, TextMatchPred};
        let leaf = |c: &str| PredNode::Leaf(TextMatchPred { column: c.into(), query: "x".into() });
        let mutable: HashSet<String> = ["status".to_string(), "_version".to_string()].into_iter().collect();

        assert!(!routed_touches_mutable(Some(&mutable), Some(&leaf("trace_id"))), "immutable column");
        assert!(routed_touches_mutable(Some(&mutable), Some(&leaf("status"))), "mutable column");
        // A conjunction is only as safe as its least safe branch.
        assert!(routed_touches_mutable(Some(&mutable), Some(&PredNode::And(vec![leaf("trace_id"), leaf("status")]))), "AND with a mutable branch");
        assert!(routed_touches_mutable(Some(&mutable), Some(&PredNode::Or(vec![leaf("trace_id"), leaf("_version")]))), "OR with a mutable branch");
        // Not merge-on-read, or nothing routed: nothing to be unsound about.
        assert!(!routed_touches_mutable(None, Some(&leaf("status"))), "non-MOR table");
        assert!(!routed_touches_mutable(Some(&mutable), None), "nothing routed");
    }

    /// Against the REAL schema: fires if an indexed column is later marked `mutable: true`,
    /// which would silently make zero-hit pruning unsound on a merge-on-read table.
    #[test]
    fn otel_routes_on_immutable_columns_so_pruning_stays_sound() {
        use crate::tantivy::udf::{PredNode, TextMatchPred};
        let mutable = ProjectRoutingTable::version_mutable_columns("otel_logs_and_spans").expect("otel is version_append, so it has a mutable set");
        let leaf = |c: &str| PredNode::Leaf(TextMatchPred { column: c.into(), query: "x".into() });
        for column in ["context___trace_id", "id", "name"] {
            assert!(
                !routed_touches_mutable(Some(&mutable), Some(&leaf(column))),
                "`{column}` is routed by real queries and must stay immutable, or zero-hit file pruning is unsound"
            );
        }
        // ...and the set is not vacuously empty, which would make the assertions above meaningless.
        assert!(!mutable.is_empty(), "otel declares mutable columns; an empty set would make this test prove nothing");
    }

    /// Rendered as a stable string so a case line pins the decision *and* the exact
    /// exclude/row-selection payload (`PrefilterDecision` is neither `Debug` nor `Eq`).
    #[test_case(&["a"], 100, 50, false, false, true, true => "used ids=a covered=covered.parquet exclude=zero_hit.parquet rows=row_sel.parquet:[1, 2]".to_string() ; "only a mutable predicate blocks file pruning")]
    #[test_case(&["a"], 100, 50, false, true, true, true => "skipped:mutable_visibility".to_string() ; "a mutable predicate is refused")]
    // Skip reasons fire in priority order. The empty-index case ALSO satisfies the
    // low-selectivity condition (1*100 >= 0*50), so it proves `empty_index` is tested first.
    #[test_case(&["a"], 0, 50, false, false, true, true => "skipped:empty_index".to_string() ; "empty index wins over low selectivity")]
    #[test_case(&["a", "b"], 2, 50, false, false, true, true => "skipped:low_selectivity".to_string() ; "2 of 2 indexed rows hit == 100% >= 50%")]
    #[test_case(&["a"], 10, 50, true, false, true, true => "skipped:field_coverage_gap".to_string() ; "an in-window index missing a queried field")]
    // A 0% selectivity floor is the prefilter's off switch: it must skip for EVERY hit set,
    // the empty one included, which would otherwise push an `id IN ()` that prunes every file.
    #[test_case(&[] as &[&str], 100, 0, false, false, true, true => "skipped:low_selectivity".to_string() ; "zero selectivity floor skips the empty hit set")]
    #[test_case(&["a"], 100, 0, false, false, true, true => "skipped:low_selectivity".to_string() ; "zero selectivity floor skips a non-empty hit set")]
    #[test_case(&["a"], 10, 50, false, false, true, true => "used ids=a covered=covered.parquet exclude=zero_hit.parquet rows=row_sel.parquet:[1, 2]".to_string() ; "non mutable table gets file exclusion and row selection")]
    // Disabled config knobs suppress narrowing even with non-empty inputs.
    #[test_case(&["a"], 10, 50, false, false, false, false => "used ids=a covered=covered.parquet exclude=none rows=none".to_string() ; "narrowing respects its own config toggle")]
    fn prefilter_decision(
        ids: &[&str], indexed_rows: u64, min_selectivity_pct: u64, field_gap: bool, is_mutable: bool, file_pruning: bool, row_selection: bool,
    ) -> String {
        let sorted = |s: HashSet<String>| s.into_iter().sorted().join(",");
        match decide_prefilter(
            ids.iter().map(|s| s.to_string()).collect(),
            indexed_rows,
            min_selectivity_pct,
            field_gap,
            HashSet::from(["covered.parquet".to_string()]),
            HashSet::from(["zero_hit.parquet".to_string()]),
            HashMap::from([("row_sel.parquet".to_string(), vec![1, 2])]),
            is_mutable,
            file_pruning,
            row_selection,
        ) {
            PrefilterDecision::Skipped(reason) => format!("skipped:{reason}"),
            PrefilterDecision::Used { ids, covered_files, exclude_files, row_selections } => {
                let rows = row_selections.map_or("none".to_string(), |m| m.into_iter().map(|(f, r)| format!("{f}:{r:?}")).sorted().join(","));
                format!("used ids={} covered={} exclude={} rows={rows}", sorted(ids), sorted(covered_files), exclude_files.map_or("none".to_string(), sorted))
            }
        }
    }
}

#[async_trait]
impl TableProvider for ProjectRoutingTable {
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        self.schema()
    }

    async fn insert_into(&self, _state: &dyn Session, input: Arc<dyn ExecutionPlan>, insert_op: InsertOp) -> DFResult<Arc<dyn ExecutionPlan>> {
        if insert_op != InsertOp::Append {
            error!("Unsupported insert operation: {:?}", insert_op);
            return not_impl_err!("{insert_op} not implemented for MemoryTable yet");
        }
        // No `logically_equivalent_names_and_types` check: `self.schema()` presents Variant
        // columns as Utf8View so VALUES literals type-check, and validating against that shape
        // would reject the real downstream batches. `write_all` coerces back before the commit.
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(self.clone()), None)))
    }

    fn supports_filters_pushdown(&self, filter: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // The delta-kernel scan cannot evaluate predicates on Variant columns, so mark those
        // filters `Unsupported` and let DataFusion apply them in a FilterExec above the scan.
        let variant_cols: HashSet<String> = crate::schema::registry()
            .get(&self.table_name)
            .map(|s| s.schema_ref().fields().iter().filter(|f| crate::schema::is_variant_type(f.data_type())).map(|f| f.name().clone()).collect())
            .unwrap_or_default();
        let mutable = Self::version_mutable_columns(&self.table_name);
        Ok(filter
            .iter()
            .map(|f| {
                if Self::references_tombstone(&self.table_name, f)
                    || (!variant_cols.is_empty() && f.column_refs().iter().any(|c| variant_cols.contains(&c.name)))
                {
                    TableProviderFilterPushDown::Unsupported
                } else if mutable.as_ref().is_some_and(|m| f.column_refs().iter().any(|c| m.contains(&c.name))) {
                    // `Inexact`, not `Unsupported`: both keep a FilterExec above the scan, but
                    // `Unsupported` also withholds the predicate from `scan()`, which is the
                    // only place that knows whether the window is sweep-certified (it re-strips
                    // it otherwise, see `leg_safe`). NEVER `Exact` — that deletes the
                    // above-dedup FilterExec that makes a stale-version match impossible.
                    TableProviderFilterPushDown::Inexact
                } else if Self::is_exact_pushdown_filter(f) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }

    #[instrument(
        name = "datafusion.table.scan",
        skip_all,
        fields(
            table.name = %self.table_name,
            table.project_id = Empty,
            scan.filters_count = filters.len(),
            scan.has_limit = limit.is_some(),
            scan.limit = limit.unwrap_or(0),
            scan.has_projection = projection.is_some(),
            scan.uses_mem_buffer = false,
            scan.skipped_delta = false,
            parquet.files = Empty,
            parquet.bytes = Empty,
            parquet.file_ids = Empty,
            parquet.selected_row_groups = Empty,
        )
    )]
    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let span = tracing::Span::current();
        let scan_start = std::time::Instant::now();
        let scan_metrics = self.database.scan_metrics.clone();

        // Internal Delta-only reads (rollup builds, maintenance) are not the
        // unbounded client scans this guard exists to reject.
        if !self.database.bypass_rollup
            && let Some(reason) = self.bounded_otel_scan_reason(filters, limit)
        {
            match self.database.config.core.timefusion_otel_scan_guard {
                config::OtelScanGuard::Off => {}
                config::OtelScanGuard::Observe => {
                    metrics::counter!(scan_metric_names::BOUNDED_OTEL_SCAN_CANDIDATES).increment(1);
                    warn!(event = "otel_scan_guard_candidate", table.name = %self.table_name, reason, "raw OTel scan would be rejected");
                }
                config::OtelScanGuard::Enforce => {
                    metrics::counter!(scan_metric_names::BOUNDED_OTEL_SCAN_REJECTIONS).increment(1);
                    return Err(DataFusionError::Plan("raw OTel queries require project_id = <value> and a timestamp lower bound or LIMIT".to_string()));
                }
            }
        }

        // Mutable predicates must run after version resolution. Narrowing only
        // the indexed leg can remove the winning version of a matching raw row.
        // `decide_prefilter` therefore refuses mutable predicates entirely.
        let mutable = Self::version_mutable_columns(&self.table_name);
        let unstripped_filters = filters;
        let leg_safe = |f: &Expr| {
            !Self::references_tombstone(&self.table_name, f) && !mutable.as_ref().is_some_and(|m| f.column_refs().iter().any(|c| m.contains(&c.name)))
        };
        let filters: Vec<Expr> = filters.iter().filter(|f| leg_safe(f)).cloned().collect();
        let optimized_filters = self.apply_time_series_optimizations(&filters)?;

        let project_id = self.extract_project_id_from_filters(&optimized_filters).unwrap_or_else(|| self.default_project.clone());
        span.record("table.project_id", project_id.as_str());

        // Tantivy prefilter, two independent paths: the Delta side builds `id IN (delta_ids)`
        // for the Delta scan only (MemBuffer rows are never indexed, so applying it there would
        // drop valid rows); the MemBuffer side prefilters under its own bucket lock. On a MOR
        // table keep the unstripped tree so mutable predicates can be detected.
        let text_match_tree = match mutable.is_some() {
            false => crate::tantivy::udf::collect_text_match_tree(&optimized_filters),
            true => crate::tantivy::udf::collect_text_match_tree(&self.apply_time_series_optimizations(unstripped_filters)?),
        };
        // Query [lo,hi] timestamp window, shared by the tantivy prefilter and the skip-delta
        // watermark check below.
        let query_time_range = self.extract_time_range_from_filters(&optimized_filters);
        let mut tantivy_id_filter: Option<Expr> = None;
        // When index coverage is partial, indexed and raw files are read as separate Delta legs.
        // Only the indexed leg receives the narrowing id-set; uncovered files retain the
        // original predicate and therefore cannot lose rows.
        let mut tantivy_covered_files: Option<HashSet<String>> = None;
        // Files the prefilter proved hold no matches (zero-hit covering
        // index) — excluded from the Delta scan when file pruning is on.
        let mut tantivy_exclude: Option<HashSet<String>> = None;
        // Per-file matching row ordinals (row-selection pushdown), for files
        // whose covering index was built in parquet row order.
        let mut tantivy_row_selections: Option<HashMap<String, Vec<u64>>> = None;
        // File-level needle pruning: table-relative paths the resident bloom registry proves
        // cannot contain the query's equality/IN needles. Memory-only consult — a cold registry
        // prunes nothing. Unlike zero_hit_files this must reach the RAW leg too.
        let bloom_rejected: Option<HashSet<String>> = (|| {
            let reg = self.database.bloom_prune()?;
            let dates = crate::read::bloom_prune::dates_in_range(query_time_range?)?;
            let schema = crate::schema::get_schema(&self.table_name)?;
            let needles = crate::read::bloom_prune::extract_needles(&optimized_filters, schema, Self::version_mutable_columns(&self.table_name).as_ref());
            if needles.is_empty() {
                return None;
            }
            let rejected = reg.rejected_rels(&self.table_name, &project_id, &dates, &needles);
            (!rejected.is_empty()).then_some(rejected)
        })();
        if let Some(tree) = text_match_tree.as_ref()
            && let Some(svc) = self.database.tantivy_search()
        {
            let tcfg = &self.database.config().tantivy;
            let max_hits = tcfg.prefilter_max_hits();
            let min_sel_pct = tcfg.prefilter_min_selectivity_pct() as u64;
            crate::observability::record_tantivy_prefilter_attempt();
            metrics::counter!(scan_metric_names::PREFILTER_ATTEMPTS).increment(1);

            let skip = |reason: &'static str| {
                record_prefilter_skip(reason);
                debug!("Tantivy prefilter skipped for {}/{}: {}", project_id, self.table_name, reason);
            };
            // ONE pass over the in-window index set: the predicate tree compiles to a single
            // tantivy BooleanQuery per index (And→Must, Or→Should), hits unioned across indexes
            // (they cover disjoint row sets).
            match svc.search_detailed(&self.table_name, &project_id, tree, max_hits, query_time_range).await {
                Ok(Ok(r)) => match decide_prefilter(
                    r.hits.into_iter().map(|h| h.id).collect(),
                    r.indexed_rows,
                    min_sel_pct,
                    r.field_coverage_gap,
                    r.covered_files,
                    r.zero_hit_files,
                    r.row_selections,
                    // Predicate-aware, not table-wide: if every ROUTED predicate column is
                    // immutable, all versions of a matching row carry the same values, so a
                    // file whose index found no match cannot hold any version of one
                    // (tombstones included — same keys). A predicate touching a mutable column
                    // (or the tiebreak/tombstone) takes the conservative path.
                    routed_touches_mutable(mutable.as_ref(), text_match_tree.as_ref()),
                    tcfg.timefusion_tantivy_file_pruning,
                    tcfg.timefusion_tantivy_row_selection,
                ) {
                    PrefilterDecision::Skipped(reason) => skip(reason),
                    PrefilterDecision::Used { ids, covered_files, exclude_files, row_selections } => {
                        crate::observability::record_tantivy_prefilter_used();
                        metrics::counter!(scan_metric_names::PREFILTER_USED).increment(1);
                        tantivy_id_filter = Some(col("id").in_list(ids.into_iter().map(lit).collect(), false));
                        // Carry the coverage set forward and split against the snapshot taken at
                        // scan construction: a flush or compaction can commit in between.
                        tantivy_covered_files = Some(covered_files);
                        tantivy_exclude = exclude_files;
                        tantivy_row_selections = row_selections;
                    }
                },
                Ok(Err(reason)) => skip(reason),
                Err(e) => {
                    warn!("tantivy search failed for {}/{}: {:#} — falling back to full scan", project_id, self.table_name, e);
                    crate::observability::record_tantivy_prefilter_error();
                    skip("delta_error");
                }
            }
        }

        // Read-side dedup setup: collapse physical duplicates of dedup-key rows
        // over the routed/pruned union at query time, so COUNT(*) is correct
        // regardless of sweep timing. The pushed projection is augmented with
        // any dedup-key columns the query projected away; `output_projection`
        // restores the requested set. No-op without declared dedup_keys.
        let table_schema = crate::schema::get_schema(&self.table_name);
        let dedup_keys: Vec<String> = table_schema.as_ref().map(|s| s.dedup_keys.clone()).unwrap_or_default();
        // The tiebreak rides in with the keys ONLY for merge-on-read tables (DedupExec keeps
        // the greatest version per key and must see the column). Elsewhere keep-greatest cannot
        // engage, so the column would be read and never used.
        let dedup_tiebreak: Option<String> = table_schema.as_ref().filter(|s| s.version_append).and_then(|s| s.dedup_tiebreak.clone());
        // Merge-on-read DELETE: a tombstone version must reach the filter ABOVE
        // the dedup, so its marker column rides in with the keys and is stripped
        // again afterwards. `None` on every table that declares none.
        let tombstone: Option<String> = table_schema.and_then(|s| s.tombstone_column.clone());
        // `tombstone_keep` is the requested width when the marker rode in purely for the filter
        // (one trailing column the post-filter projection removes). The dedup skip must be
        // decided BEFORE the projection is built, or augmenting with the keys disables it.
        // A fast-resolve miss simply declines — the skip is an optimisation, never correctness.
        let skip_verdict = match dedup_keys.is_empty() {
            true => DedupSkipVerdict::Disabled,
            false => self
                .database
                .try_fast_resolve(&project_id, &self.table_name)
                .and_then(|t| t.try_read().ok().map(|table| self.dedup_skip_allowed(&table, &project_id, query_time_range, &dedup_keys)))
                // A resolve miss is its own denial reason (cold provider cache), not an
                // uncertified partition.
                .unwrap_or(DedupSkipVerdict::Unresolved),
        };
        let pre_skip_dedup = skip_verdict.granted();
        let (scan_projection, output_projection, tombstone_keep): (Option<Vec<usize>>, Option<Vec<usize>>, Option<usize>) = match projection {
            Some(p) if !dedup_keys.is_empty() || tombstone.is_some() => {
                let full_schema = self.schema();
                // The dedup keys ALWAYS ride in, even when `pre_skip_dedup` says the window is
                // certified: the skip is granted PER LEG below and the mem ∪ delta union path
                // never grants it, so dropping the keys here yields DedupExecs over scans that
                // cannot feed them.
                let augment = dedup_keys.iter().chain(dedup_tiebreak.iter()).chain(tombstone.iter());
                let missing: Vec<usize> = augment.filter_map(|k| full_schema.index_of(k).ok()).filter(|i| !p.contains(i)).collect();
                if missing.is_empty() {
                    (Some(p.clone()), None, None)
                } else {
                    let aug: Vec<usize> = p.iter().chain(&missing).copied().collect();
                    // The marker alone must survive DedupExec's projection restore; its index is
                    // in `missing`, hence in `aug`, by construction. Requested columns occupy the
                    // first p.len() positions of the augmented output.
                    let extra = tombstone.as_ref().and_then(|t| full_schema.index_of(t).ok()).filter(|i| !p.contains(i));
                    let out: Vec<usize> = (0..p.len()).chain(extra.and_then(|ti| aug.iter().position(|&i| i == ti))).collect();
                    (Some(aug), Some(out), extra.map(|_| p.len()))
                }
            }
            _ => (projection.cloned(), None, None),
        };
        let projection = scan_projection.as_ref();
        // DedupExec drops rows AFTER the scan, so a pushed `limit` must NOT truncate the
        // underlying scans — the deduped result could then yield < limit rows even when more
        // exist below the cut. The outer limit still caps; `orig_limit` is restored on
        // Delta-only paths that skip DedupExec. The tombstone filter suppresses it for the
        // same reason even where dedup doesn't.
        let orig_limit = limit;
        let limit = limit.filter(|_| dedup_keys.is_empty() && tombstone.is_none());

        let scan_state = parking_lot::Mutex::new(ScanShape::default());
        // DedupExec restores the requested columns when it runs; every leg that bypasses it
        // still owes that debt, or augmented key columns leak into the result and the two
        // sides of a union disagree on schema.
        let pay_projection = |leg: Arc<dyn ExecutionPlan>| match &output_projection {
            Some(idxs) => Self::project_indices(leg, idxs),
            None => Ok(leg),
        };
        let finish = |plan: Arc<dyn ExecutionPlan>| match &tombstone {
            Some(marker) => Self::filter_tombstones(plan, marker, tombstone_keep),
            None => Ok(plan),
        };
        // Legs of the mem ∪ hot ∪ delta union, in recency order. `skip_legs` are Delta legs over
        // date partitions certified duplicate-free: they are unioned ABOVE DedupExec rather than
        // fed through it. Sound because `date` derives from `timestamp` and DML re-appends
        // preserve it, so no dedup key spans a date boundary.
        let wrap_result_split =
            |mut legs: Vec<(Arc<dyn ExecutionPlan>, crate::read::LegKind)>, skip_legs: Vec<Arc<dyn ExecutionPlan>>| -> DFResult<Arc<dyn ExecutionPlan>> {
                fn union_or_single(mut plans: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
                    Ok(if plans.len() == 1 { plans.remove(0) } else { UnionExec::try_new(plans)? as Arc<dyn ExecutionPlan> })
                }
                // A leg pruned to nothing bottoms out in an EmptyExec, which declares no output
                // ordering, and one such leg would veto `merge_req` below (Delta legs are
                // unsortable) — costing the SPM and forcing DedupExec into full-set mode. An
                // empty leg contributes no rows; drop it. Keep one if all are empty so the
                // single-plan path stays valid.
                fn provably_empty(plan: &dyn ExecutionPlan) -> bool {
                    plan.is::<datafusion::physical_plan::empty::EmptyExec>() || matches!(plan.children().as_slice(), [child] if provably_empty(child.as_ref()))
                }
                if legs.len() > 1 && legs.iter().any(|(p, _)| provably_empty(p.as_ref())) {
                    match legs.iter().any(|(p, _)| !provably_empty(p.as_ref())) {
                        true => legs.retain(|(p, _)| !provably_empty(p.as_ref())),
                        false => legs.truncate(1),
                    }
                }
                // Under a per-date split the deduped side can end up with NO legs while the
                // certified side carries every row (file-level pruning removes the uncertified
                // dates' files entirely). Everything below indexes `plans[0]`, so without this
                // the scan panics. The certified legs need no dedup, only the projection debt.
                if legs.is_empty() && !skip_legs.is_empty() {
                    return finish(union_or_single(skip_legs.into_iter().map(&pay_projection).collect::<DFResult<Vec<_>>>()?)?);
                }
                let leg_sortable: Vec<bool> = legs.iter().map(|(_, k)| k.sortable()).collect();
                let legs: Vec<Arc<dyn ExecutionPlan>> = legs
                    .into_iter()
                    .map(|(plan, kind)| match crate::read::ordering_probe_enabled() {
                        true => Arc::new(crate::read::OrderingProbeExec::new(plan, kind)) as Arc<dyn ExecutionPlan>,
                        false => plan,
                    })
                    .collect();
                let shape = *scan_state.lock();
                let us = scan_start.elapsed().as_micros() as u64;
                scan_metrics.record_scan(us, shape, skip_verdict);
                let dedup_on = !dedup_keys.is_empty() && !shape.skip_dedup;
                let mut plans = legs;
                // Merge-on-read prerequisite: keep-greatest only engages while the input still
                // declares an ordering on the leading dedup key, so the in-memory legs are
                // sorted up to the Delta leg's footer ordering and merged explicitly. The SPM
                // is built HERE, not left to EnforceDistribution — DedupExec declares no
                // required input ordering, so EnforceSorting would delete the injected sorts.
                // Gated on `version_append` so non-MOR scans pay no sort.
                let mut merge_req = None;
                if dedup_on
                    && table_schema.is_some_and(|t| t.version_append)
                    && let Some(req) = plans.first().and_then(|p| table_schema.and_then(|t| Self::keep_greatest_ordering(t, &p.schema())))
                {
                    // Per-leg sortability: the DELTA leg is NEVER sortable — MOR UPDATEs make
                    // files overlap and a read-time SortExec over them exhausts the query pool;
                    // footer-less files need REPAIR, not read-time sorting. `ordered_children`
                    // bails whenever an unsortable leg misses `req`, so a Delta sort is
                    // structurally impossible here. The in-memory legs ARE sortable.
                    match crate::read::optimizers::ordered_children(&plans, &req, None, &leg_sortable, false)? {
                        Some(ordered) => {
                            plans = ordered;
                            merge_req = Some(req);
                        }
                        // `None` is either "every leg already satisfies `req`" (merge anyway)
                        // or "an unsortable leg doesn't" (bail; keep-greatest stays dormant and
                        // keep-first is still sound).
                        None => {
                            let all = plans
                                .iter()
                                .map(|p| p.properties().equivalence_properties().ordering_satisfy(req.iter().cloned()))
                                .collect::<DFResult<Vec<_>>>()?;
                            merge_req = all.iter().all(|&s| s).then_some(req);
                        }
                    }
                }
                // `plans` is non-empty on every known path; erroring rather than indexing turns
                // an impossible state into a failed query instead of a panicked one.
                if plans.is_empty() {
                    return Err(datafusion::error::DataFusionError::Execution(format!("scan produced no legs to union (project_id={project_id})")));
                }
                let plan = union_or_single(plans)?;
                let plan = match merge_req.clone() {
                    Some(req) => Arc::new(datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec::new(req, plan)),
                    None => plan,
                };
                let plan = match dedup_on {
                    true => Arc::new(
                        crate::read::DedupExec::with_tiebreak(plan, dedup_keys.clone(), dedup_tiebreak.clone(), output_projection.clone())?
                            // Declaring it REQUIRED stops EnforceSorting deleting the merge above.
                            .requiring(merge_req.clone()),
                    ) as Arc<dyn ExecutionPlan>,
                    false => pay_projection(plan)?,
                };
                // Union the certified-date legs on top; they owe the same
                // projection debt as the `dedup_on == false` branch above.
                let plan = match skip_legs.is_empty() {
                    true => plan,
                    false => union_or_single(std::iter::once(Ok(plan)).chain(skip_legs.into_iter().map(&pay_projection)).collect::<DFResult<Vec<_>>>()?)?,
                };
                finish(plan)
            };
        let wrap_result = |legs: Vec<(Arc<dyn ExecutionPlan>, crate::read::LegKind)>| wrap_result_split(legs, Vec::new());
        // Both Delta-only exits are the same scan and the same bookkeeping; they differ only in
        // whether mutable filters are readmitted.
        let delta_only = async |readmit_mutable_filters: bool| -> DFResult<Arc<dyn ExecutionPlan>> {
            let (skip_dedup, plans, certified_plans) = self
                .scan_delta_only(
                    state,
                    projection,
                    &optimized_filters,
                    unstripped_filters,
                    &project_id,
                    query_time_range,
                    &dedup_keys,
                    pre_skip_dedup,
                    &tombstone,
                    orig_limit,
                    limit,
                    readmit_mutable_filters,
                    tantivy_id_filter.as_ref(),
                    tantivy_covered_files.as_ref(),
                    tantivy_exclude.as_ref(),
                    tantivy_row_selections.as_ref(),
                    bloom_rejected.as_ref(),
                )
                .await?;
            {
                let mut shape = scan_state.lock();
                shape.skip_dedup |= skip_dedup;
                shape.has_delta = true;
            }
            wrap_result_split(plans.into_iter().map(|plan| (plan, crate::read::LegKind::Delta)).collect(), certified_plans)
        };
        let layer = self.database.buffered_layer();
        debug!("ProjectRoutingTable::scan - buffered_layer present: {}, project_id: {}", layer.is_some(), project_id);
        let Some(layer) = layer else {
            debug!("No buffered layer, querying Delta only");
            // A sweep-certified window holds exactly one winning row per key, so the
            // stale-version hazard that keeps mutable predicates above DedupExec has no
            // instance and they may be pushed down. Delta-only by construction: the skip is
            // never granted while the MemBuffer leg is in play.
            return delta_only(true).await;
        };

        span.record("scan.uses_mem_buffer", true);

        // Skip Delta when the query's lower bound is strictly above the per-table flushed
        // watermark (max row ts ever handed to a Delta commit, floored at boot): Delta provably
        // holds nothing newer. Do NOT weaken this to `query_min >= mem_oldest` — that hides
        // rows whenever Delta holds data inside MemBuffer's range.
        //
        // Second disjunct: if no flush has ever committed for this (project, table), Delta is
        // empty. Flipped by the flush callback after a successful commit, never flipped back.
        let skip_delta = query_time_range.is_some_and(|(query_min, _)| query_min > layer.delta_flushed_watermark(&project_id, &self.table_name))
            || self.database.delta_scan_can_be_skipped(&project_id, &self.table_name);
        scan_state.lock().skipped_delta = skip_delta;

        // `query_partitioned_with_text_match` runs its own per-bucket prefilter inside the
        // bucket lock. Never prepend `tantivy_id_filter` here — it is derived from delta-side
        // IDs and would drop legitimate MemBuffer rows. On a MOR table the mem leg gets no tree
        // at all: the per-bucket row prefilter sits below DedupExec, and dropping a stale
        // version's row while its match-bearing sibling is in another leg breaks keep-greatest.
        let mem_tree = text_match_tree.as_ref().filter(|_| mutable.is_none());
        let mem_plan_started = std::time::Instant::now();
        let mem_leg = layer.query_partitioned_with_text_match(&project_id, &self.table_name, &optimized_filters, mem_tree).unwrap_or_else(|e| {
            warn!("Failed to query mem buffer: {}", e);
            Default::default()
        });
        metrics::counter!(scan_metric_names::MEM_PLAN_TOTAL).increment(1);
        metrics::counter!(scan_metric_names::MEM_PLAN_US_TOTAL).increment(mem_plan_started.elapsed().as_micros() as u64);
        let mem_partitions = mem_leg.partitions;

        let mem_ranges = layer.get_bucket_ranges(&project_id, &self.table_name);

        debug!("MemBuffer partitions count: {} for {}/{}", mem_partitions.len(), project_id, self.table_name);
        if mem_partitions.is_empty() {
            debug!("No MemBuffer data, querying Delta only for {}/{}", project_id, self.table_name);
            return delta_only(false).await;
        }

        scan_state.lock().has_mem = true;
        let mem_plan = self.create_memory_exec(&mem_partitions, projection, mem_leg.sorted)?;

        if skip_delta {
            span.record("scan.skipped_delta", true);
            debug!("Skipping Delta scan - query time range entirely within MemBuffer for {}/{}", project_id, self.table_name);
            return wrap_result(vec![(mem_plan, crate::read::LegKind::Mem)]);
        }

        // Build Delta filters with per-bucket exclusion so the union doesn't double-count:
        // Delta excludes the mem row ranges where those legs are authoritative
        // (`get_bucket_ranges` skips open and force-flushed buckets, whose windows legitimately
        // straddle stores). MOR UPDATEs land at the row's ORIGINAL timestamp, inside an excluded
        // range, so each conjunct is weakened with `OR stamp > gate`. Weakening is safe in one
        // direction only: an over-admitted row is a duplicate DedupExec collapses, an
        // under-admitted one is a stale read.
        let mut delta_filters = optimized_filters.clone();
        let ts_us = |t: i64| lit(ScalarValue::TimestampMicrosecond(Some(t), Some("UTC".into())));
        let ts_cmp = |op: Operator, t: i64| Expr::BinaryExpr(BinaryExpr { left: Box::new(col("timestamp")), op, right: Box::new(ts_us(t)) });
        // NOT (ts >= start AND ts < end)  ≡  (ts < start) OR (ts >= end)
        delta_filters.extend(
            crate::write::mem_buffer::merge_ranges(mem_ranges).into_iter().map(|(start, end)| ts_cmp(Operator::Lt, start).or(ts_cmp(Operator::GtEq, end))),
        );
        let resolve_span = tracing::trace_span!(parent: &span, "resolve_delta_table");
        // A query executed through a retained pgwire plan must still see a
        // committed ingest from another connection. `try_fast_resolve` opts
        // into an explicitly stale-tolerant snapshot, which is not valid for
        // an investigation read: it can turn a successful write into apparent
        // “no data”. `resolve_table` is lock-local on the common path and only
        // refreshes the Delta snapshot when a newer committed version is known.
        // No fast resolve was attempted, so do not turn this into a synthetic
        // cache miss in the scan telemetry.
        scan_state.lock().fast_resolve_hit = None;
        let delta_table = self.database.resolve_table(&project_id, &self.table_name).instrument(resolve_span).await?;
        let table = delta_table.read().await;
        let delta_plans = self
            .scan_delta_with_tantivy(
                &table,
                state,
                projection,
                &delta_filters,
                limit,
                tantivy_id_filter.as_ref(),
                tantivy_covered_files.as_ref(),
                tantivy_exclude.as_ref(),
                tantivy_row_selections.as_ref(),
                query_time_range,
                bloom_rejected.as_ref(),
                None,
                None,
            )
            .await?;
        scan_state.lock().has_delta = true;

        // Union the legs in recency order — mem, then Delta — so DedupExec's keep-first
        // favours the freshest copy of a row.
        use crate::read::LegKind;
        wrap_result(std::iter::once((mem_plan, LegKind::Mem)).chain(delta_plans.into_iter().map(|p| (p, LegKind::Delta))).collect())
    }
}

#[cfg(test)]
mod decode_tests {
    use test_case::test_case;

    /// The gate bounds decode heap, not polls: a worst-case batch still claims a whole slot, so N
    /// concurrent worst-case decodes stay capped at N readers, while unknown size (a stream's
    /// first poll) is charged conservatively.
    #[test_case(crate::database::NOMINAL_DECODE_BATCH_BYTES => crate::database::DECODE_UNITS_PER_READER ; "a full-size batch must still cost a full slot")]
    #[test_case(crate::database::NOMINAL_DECODE_BATCH_BYTES * 4 => crate::database::DECODE_UNITS_PER_READER ; "an oversized batch is clamped, never exceeding one slot")]
    #[test_case(0 => crate::database::DECODE_UNITS_PER_READER ; "unknown batch size claims a whole slot")]
    #[test_case(210 * 1024 => 1 ; "a 0.21 MB prod batch costs one unit, not a whole slot")]
    fn a_decode_claim_is_proportional_to_heap_and_never_exceeds_one_reader_slot(bytes: u64) -> u32 {
        super::decode_units(bytes)
    }

    #[test]
    fn a_decode_claim_is_monotonic_and_never_zero() {
        let k = super::DECODE_UNITS_PER_READER;
        // Monotonic, and never zero (zero would break progress guarantees).
        let mut prev = 0;
        for mb in [0.2_f64, 1.0, 10.0, 50.0, 100.0, 145.0] {
            let u = super::decode_units((mb * 1024.0 * 1024.0) as u64);
            assert!(u >= 1 && u <= k, "{mb} MB -> {u} units, out of range 1..={k}");
            assert!(u >= prev, "claim must not decrease as batches grow: {mb} MB -> {u} after {prev}");
            prev = u;
        }
    }

    /// `None` here wipes EVERY date's rollup coverage for the table, so only a
    /// batch that genuinely cannot name a partition may reach it — at ANY
    /// timestamp precision.
    #[test]
    fn a_write_batch_names_its_partitions_at_any_timestamp_precision() {
        use arrow::{
            array::{ArrayRef, Date32Array, RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray},
            datatypes::{Field, Schema},
        };
        use std::sync::Arc;
        let day = 86_400_000_000i64;
        let batch = |name: &str, column: ArrayRef| {
            RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new(name, column.data_type().clone(), true)])), vec![column]).expect("batch")
        };
        let expect = |batch: RecordBatch| {
            let mut dates = super::batch_hours(&batch).expect("the batch names its partitions").into_keys().collect::<Vec<_>>();
            dates.sort();
            dates
        };

        // Two rows a day apart, written at microsecond and millisecond precision.
        let micros = TimestampMicrosecondArray::from(vec![0i64, day]).with_timezone("UTC");
        assert_eq!(expect(batch("timestamp", Arc::new(micros))), ["1970-01-01", "1970-01-02"]);
        let millis = TimestampMillisecondArray::from(vec![0i64, day / 1_000]).with_timezone("UTC");
        assert_eq!(expect(batch("timestamp", Arc::new(millis))), ["1970-01-01", "1970-01-02"], "a millisecond column used to wipe the whole table");

        // No timestamp: the `date` partition column answers, in both encodings.
        assert_eq!(expect(batch("date", Arc::new(Date32Array::from(vec![0, 1])))), ["1970-01-01", "1970-01-02"]);
        assert_eq!(expect(batch("date", Arc::new(StringArray::from(vec!["2026-08-01"])))), ["2026-08-01"]);

        // Only a batch that carries neither may force the source-wide wipe.
        let opaque = batch("name", Arc::new(StringArray::from(vec!["x"])));
        assert!(super::batch_hours(&opaque).is_none(), "a batch with no date and no timestamp must still fall back");

        // A batch confined to 14:00 marks one hour, not the day; a batch with only
        // a `date` cannot name an hour and must mark all of them.
        let at = |hours: i64| TimestampMicrosecondArray::from(vec![hours * 3_600_000_000]).with_timezone("UTC");
        let hours = |batch: RecordBatch| super::batch_hours(&batch).expect("hours").into_values().fold(0u32, |mask, hour| mask | hour);
        assert_eq!(hours(batch("timestamp", Arc::new(at(14)))), 1 << 14, "one hour of enrichment must mark one hour");
        assert_eq!(hours(batch("date", Arc::new(Date32Array::from(vec![0])))), crate::rollup::ALL_HOURS, "no timestamp means no hour to name");
    }

    #[test]
    fn bounded_dml_windows_mark_only_overlapping_hours() {
        const HOUR: i64 = 3_600_000_000;
        let masks = super::window_hour_masks(14 * HOUR + 30, 16 * HOUR).expect("bounded window");
        assert_eq!(masks, vec![("1970-01-01".into(), (1 << 14) | (1 << 15))]);

        let masks = super::window_hour_masks(23 * HOUR, 25 * HOUR).expect("cross-day window");
        assert_eq!(masks, vec![("1970-01-01".into(), 1 << 23), ("1970-01-02".into(), 1)]);
        assert!(super::window_hour_masks(HOUR, HOUR).is_none());
    }

    /// A pool exhaustion must be retried at a parallelism that can fit before the candidate is
    /// quarantined; the `false` rows cover every level of the 3-rung `REPAIR_SORT_PARTITION_LADDER`.
    #[test_case(true, 0 => (Some(4), 1) ; "top of the ladder: retry lower, charging ONE strike so this alone cannot park the file")]
    #[test_case(true, 1 => (Some(1), 1) ; "mid ladder: retry at the floor, still one strike")]
    #[test_case(true, 2 => (None, crate::database::REPAIR_QUARANTINE_AFTER) ; "single-partition sort is the floor: the exhaustion is believed and charges the whole threshold")]
    #[test_case(false, 0 => (None, 1) ; "a transient failure at the top never escalates")]
    #[test_case(false, 1 => (None, 1) ; "a transient failure mid ladder never escalates")]
    #[test_case(false, 2 => (None, 1) ; "a transient failure at the floor never parks early")]
    fn a_pool_exhaustion_walks_down_the_parallelism_ladder_before_it_is_believed(exhausted: bool, level: usize) -> (Option<usize>, u32) {
        super::repair_failure_action(exhausted, level)
    }

    /// The ladder is only reachable if the staging site RECOGNISES a pool
    /// exhaustion: a spilling sort dies with wording that does not contain
    /// "Resources exhausted", so the shared classifier must be used.
    #[test]
    fn the_sort_oom_prod_text_reaches_the_parallelism_ladder() {
        let prod = "Not enough memory to continue external sort. Consider increasing the memory limit config: \
                    'datafusion.runtime.memory_limit', or decreasing the config: \
                    'datafusion.execution.sort_spill_reservation_bytes'.";
        assert!(crate::maintenance_coordinator::is_capacity_failure(prod), "the sort-OOM prod text is a capacity failure");
        assert!(!prod.contains("Resources exhausted"), "the fixture must be the wording the old local check MISSED");
        assert_eq!(
            super::repair_failure_action(crate::maintenance_coordinator::is_capacity_failure(prod), 0),
            (Some(4), 1),
            "a sort OOM must buy a cheaper retry, not be filed as transient"
        );
    }

    /// Regression: a dedup rewrite must not retry at the SAME parallelism forever. `attempts`
    /// is POST-CLAIM, so a first-ever run arrives as 1, not 0; getting that boundary wrong puts
    /// EVERY dedup on the floor.
    #[test_case(0 => crate::database::MAINTENANCE_MAX_PARTITIONS ; "a hypothetical pre-claim 0 must not narrow")]
    #[test_case(1 => crate::database::MAINTENANCE_MAX_PARTITIONS ; "a first-ever run keeps today's width")]
    #[test_case(2 => 1 ; "the first retry must narrow: the merge exec cannot spill")]
    #[test_case(8 => 1 ; "a late retry stays on the floor")]
    fn a_retried_dedup_rewrite_narrows_to_a_single_partition(attempts: u32) -> usize {
        super::dedup_sort_partitions(attempts)
    }

    /// Every ladder must end at 1: a single-partition sort is the only setting with no
    /// `SortPreservingMergeExec` at all, and that exec cannot spill.
    #[test_case(&crate::database::REPAIR_SORT_PARTITION_LADDER, crate::database::REPAIR_SORT_PARTITIONS ; "the repair ladder")]
    #[test_case(&crate::database::DEDUP_SORT_PARTITION_LADDER, crate::database::MAINTENANCE_MAX_PARTITIONS ; "the dedup ladder")]
    fn a_parallelism_ladder_descends_to_a_single_partition(ladder: &[usize], top: usize) {
        assert_eq!(*ladder.last().unwrap(), 1, "the floor must have no merge exec");
        assert_eq!(ladder[0], top, "the ladder starts at the fast setting");
        assert!(ladder.windows(2).all(|w| w[0] > w[1]), "strictly descending: {ladder:?}");
    }

    #[test]
    fn repair_bin_date_extracts_the_partition_and_orders_recent_first() {
        let bin = |d: &str| vec![format!("project_id=abc/date={d}/part-00000-x.zstd.parquet")];
        assert_eq!(super::repair_bin_date(&bin("2026-07-30")), "2026-07-30");
        assert_eq!(super::repair_bin_date(&[]), "", "no file sorts last");
        assert_eq!(super::repair_bin_date(&["no-date-here.parquet".to_string()]), "", "no date sorts last");

        let mut planned: Vec<(String, Vec<String>)> =
            vec![("old".into(), bin("2026-05-30")), ("blocking".into(), bin("2026-07-30")), ("older".into(), bin("2026-06-09"))];
        planned.sort_by(|a, b| super::repair_bin_date(&b.1).cmp(super::repair_bin_date(&a.1)));
        assert_eq!(planned.iter().map(|(p, _)| p.as_str()).collect::<Vec<_>>(), vec!["blocking", "older", "old"]);
    }
}
