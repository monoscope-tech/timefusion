//! Tantivy and bloom sidecar index maintenance: backfill fair-queueing,
//! reconcile/GC passes and the coverage census.
//!
//! A slice of `database`, not a layer over it.

use super::*;

pub(crate) fn sort_backfill_uris_newest_first(uris: &mut [String]) {
    // Paths contain `date=YYYY-MM-DD`, so reverse lexical order is chronological newest-first.
    uris.sort_by(|a, b| b.cmp(a));
}

/// Wall-clock the GC phase of one reconcile pass may spend before yielding to the backfill.
const GC_PHASE_BUDGET: std::time::Duration = std::time::Duration::from_secs(120);

/// Tables reconciled at once; bounded low on purpose.
pub(crate) const TANTIVY_RECONCILE_CONCURRENCY: usize = 3;

pub(crate) type TantivyBackfillWork = (String, String, String); // project, relative parquet, absolute URI

/// Bound a backfill pass by input bytes, keeping fair-ordering intact.
///
/// `sizes` is keyed by the snapshot-relative path; files with no known size count as free.
/// At least one file always survives, so an over-budget file makes progress instead of
/// wedging the queue behind it forever.
///
/// ```
/// # use timefusion::database::truncate_to_byte_budget;
/// # use std::collections::HashMap;
/// let w = |n: &str| (n.to_string(), n.to_string(), format!("s3://b/{n}"));
/// let sizes = HashMap::from([("a".to_string(), 60u64), ("b".to_string(), 60), ("c".to_string(), 60)]);
/// let got = truncate_to_byte_budget(vec![w("a"), w("b"), w("c")], &sizes, 130);
/// assert_eq!(got.len(), 2, "a and b fit in 130; c would exceed it");
///
/// let huge = HashMap::from([("a".to_string(), 9_000u64)]);
/// assert_eq!(truncate_to_byte_budget(vec![w("a")], &huge, 10).len(), 1, "never yield an empty pass");
/// assert_eq!(truncate_to_byte_budget(vec![w("a")], &sizes, 0).len(), 1, "a zero budget still makes progress");
/// ```
pub fn truncate_to_byte_budget(work: Vec<TantivyBackfillWork>, sizes: &HashMap<String, u64>, budget_bytes: u64) -> Vec<TantivyBackfillWork> {
    let mut spent = 0u64;
    let keep = work
        .iter()
        .position(|(_, rel, _)| {
            spent = spent.saturating_add(sizes.get(rel).copied().unwrap_or(0));
            spent > budget_bytes
        })
        .unwrap_or(work.len())
        .max(1);
    work.into_iter().take(keep).collect()
}

/// `fair_tantivy_backfill_work`, but reserving `tail_pct` of the pass for the OLDEST uncovered
/// files — newest-first plus a per-pass cap would starve the tail outright.
///
/// Returns the pass's work list plus the URIs that came from the reserved tail.
pub(crate) fn fair_tantivy_backfill_work_split(
    queues: Vec<(String, VecDeque<(String, String)>)>, cap: usize, tail_pct: u8,
) -> (Vec<TantivyBackfillWork>, HashSet<String>) {
    let tail_n = (cap > 0).then(|| cap * usize::from(tail_pct.min(100)) / 100).filter(|n| *n > 0);
    let Some(tail_n) = tail_n else {
        let mut work = fair_tantivy_backfill_work(queues);
        if cap > 0 {
            work.truncate(cap);
        }
        return (work, HashSet::new());
    };
    let oldest_first: Vec<_> = queues.iter().map(|(p, q)| (p.clone(), q.iter().rev().cloned().collect())).collect();
    let mut work = fair_tantivy_backfill_work(queues);
    let head_n = cap.saturating_sub(tail_n);
    let tail: Vec<_> = {
        let head_uris: HashSet<&str> = work.iter().take(head_n).map(|(_, _, uri)| uri.as_str()).collect();
        fair_tantivy_backfill_work(oldest_first).into_iter().filter(|(_, _, uri)| !head_uris.contains(uri.as_str())).take(tail_n).collect()
    };
    // Interleave, do not append: a pass is routinely killed before it finishes, so anything
    // placed at the end is the first work lost.
    work.truncate(head_n);
    let (mut head, mut tail): (VecDeque<_>, VecDeque<_>) = (work.into(), tail.into());
    let per_tail = head.len().div_ceil(tail.len().max(1)).max(1);
    let mut out = Vec::with_capacity(head.len() + tail.len());
    let reserved: HashSet<String> = tail.iter().map(|(_, _, uri)| uri.clone()).collect();
    while !head.is_empty() || !tail.is_empty() {
        out.extend(tail.pop_front());
        out.extend((0..per_tail).filter_map(|_| head.pop_front()));
    }
    (out, reserved)
}
pub(crate) fn fair_tantivy_backfill_work(mut queues: Vec<(String, VecDeque<(String, String)>)>) -> Vec<TantivyBackfillWork> {
    // Weighted fair queueing: a project's Nth file gets virtual time `N * total / its_backlog`.
    // Project order is pinned first: HashMap iteration is unstable and the sort is stable.
    queues.sort_by(|a, b| a.0.cmp(&b.0));
    let total = queues.iter().map(|(_, queue)| queue.len() as u64).sum::<u64>();
    queues
        .into_iter()
        .flat_map(|(project, queue)| {
            let backlog = (queue.len() as u64).max(1);
            queue.into_iter().enumerate().map(move |(position, (rel, uri))| (position as u64 * total / backlog, (project.clone(), rel, uri)))
        })
        .sorted_by_key(|(virtual_time, _)| *virtual_time)
        .map(|(_, work)| work)
        .collect()
}

impl Database {
    /// Build partition-mirrored indexes, newest partition first, for live parquet files no
    /// successful manifest entry covers. No-op when no indexer is attached.
    pub fn spawn_tantivy_backfill(&self) {
        let Some(svc) = self.tantivy_indexer().cloned() else { return };
        let db = self.clone();
        tokio::spawn(async move {
            for table_name in svc.config.indexed_tables() {
                match db.backfill_table_indexes(&svc, &table_name).await {
                    Ok(0) => {}
                    Ok(n) => info!("tantivy backfill: table={} built={}", table_name, n),
                    Err(e) => warn!("tantivy backfill failed for {}: {}", table_name, e),
                }
            }
        });
    }

    /// Rebuild only files deferred by WAL replay, after replay has completed. Queue entries are
    /// removed only after a successful build, so a second restart resumes the remaining work.
    pub fn spawn_deferred_tantivy_reindex(self: &Arc<Self>, layer: Arc<crate::write::BufferedWriteLayer>) {
        let Some(svc) = self.tantivy_indexer().cloned() else { return };
        if layer.deferred_tantivy_files().is_empty() {
            return;
        }
        let db = Arc::clone(self);
        tokio::spawn(async move {
            for file in layer.deferred_tantivy_files() {
                let result = async {
                    let table = match db.resolve_table("default", &file.table_name).await {
                        Ok(table) => table,
                        Err(_) => db.resolve_table(&file.project_id, &file.table_name).await?,
                    };
                    let store = table.read().await.log_store().object_store(None);
                    let rel =
                        crate::tantivy::search::parquet_rel_of_uri(&file.uri).ok_or_else(|| anyhow::anyhow!("invalid deferred parquet URI {}", file.uri))?;
                    svc.build_index_for_file(&file.table_name, &file.project_id, rel, &file.uri, store).await
                }
                .await;
                match result {
                    Ok(()) => layer.complete_deferred_tantivy_file(&file),
                    Err(e) => warn!("tantivy recovery reindex failed for {}/{} {}: {e:#}", file.project_id, file.table_name, file.uri),
                }
            }
        });
    }

    /// Startup cache warmer: pull recent index blobs into the local disk cache in the
    /// background. Gated on `timefusion_tantivy_prefetch_days`.
    pub fn spawn_tantivy_prefetch(&self) {
        let days = self.config.tantivy.timefusion_tantivy_prefetch_days;
        let Some(search) = self.tantivy_search().cloned() else { return };
        if days == 0 {
            return;
        }
        let tables = self.config.tantivy.indexed_tables();
        tokio::spawn(async move {
            for t in tables {
                match search.warm_recent(&t, days).await {
                    Ok(0) => {}
                    Ok(n) => info!("tantivy prefetch: table={} blobs_warmed={}", t, n),
                    Err(e) => warn!("tantivy prefetch failed for {}: {}", t, e),
                }
            }
        });
    }

    /// Make the hot window resident: pull every index blob covering the last `prefetch_days`
    /// into the local extraction cache. Idempotent, so it is safe to run on a cron.
    pub async fn tantivy_warm_hot_window(&self) {
        let days = self.config.tantivy.timefusion_tantivy_prefetch_days;
        let Some(svc) = self.tantivy_search().cloned() else { return };
        if days == 0 {
            return;
        }
        for table in self.config.tantivy.indexed_tables() {
            match svc.warm_recent(&table, days).await {
                Ok(n) => debug!("tantivy hot warm: table={table} days={days} blobs_resident={n}"),
                Err(e) => warn!("tantivy hot warm failed for {table}: {e}"),
            }
        }
    }

    /// Synchronous backfill + GC for one table: build indexes for live parquet
    /// no manifest covers, then prune entries/blobs for files that are gone.
    /// Returns (indexes_built, manifest_entries_removed, blobs_deleted).
    pub async fn tantivy_reconcile_table(&self, table_name: &str) -> anyhow::Result<(usize, usize, usize)> {
        let Some(svc) = self.tantivy_indexer().cloned() else { return Ok((0, 0, 0)) };
        if !svc.config.is_table_indexed(table_name) {
            return Ok((0, 0, 0));
        }
        // GC first: stale entries only cover dead files, so pruning cannot regress coverage.
        let (mut removed, mut blobs) = (0usize, 0usize);
        let gc_deadline = std::time::Instant::now() + GC_PHASE_BUDGET;
        let mut live_cache: HashMap<usize, Arc<Vec<String>>> = HashMap::new();
        let projects = crate::tantivy::list_manifest_projects(svc.object_store.as_ref(), table_name).await?;
        let mut gc_deferred = 0usize;
        for (i, pid) in projects.iter().enumerate() {
            if std::time::Instant::now() >= gc_deadline {
                gc_deferred = projects.len() - i;
                break;
            }
            let pid = pid.as_str();
            let Ok(table_ref) = self.resolve_table(pid, table_name).await else { continue };
            let live_uris = match live_cache.entry(Arc::as_ptr(&table_ref) as usize) {
                std::collections::hash_map::Entry::Occupied(e) => Arc::clone(e.get()),
                std::collections::hash_map::Entry::Vacant(e) => Arc::clone(e.insert(Arc::new(table_ref.read().await.get_file_uris()?.collect()))),
            };
            let report = svc.gc_after_compaction(table_name, pid, &live_uris).await?;
            if report.entries_removed > 0 || report.blob_delete_errors > 0 {
                info!(
                    "tantivy reconcile gc: table={table_name} project={pid} entries_removed={} blobs_deleted={} delete_errors={}",
                    report.entries_removed, report.blobs_deleted, report.blob_delete_errors
                );
            }
            removed += report.entries_removed;
            blobs += report.blobs_deleted;
        }
        if gc_deferred > 0 {
            warn!(table_name, gc_deferred, event = "tantivy_reconcile_gc_deferred");
        }
        let built = self.backfill_table_indexes(&svc, table_name).await?;
        Ok((built, removed, blobs))
    }

    /// Live parquet for one table root, grouped by project, minus files already covered by that
    /// project's tantivy manifest or over the backfill size cap. Returns per-project uncovered
    /// URIs, per-file sizes and the object store; bumps `*oversized` by the size-skipped count.
    async fn group_uncovered_files_by_project(
        &self, svc: &crate::tantivy::search::TantivyIndexService, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, oversized: &mut u64,
        warn_skipped: bool,
    ) -> anyhow::Result<(HashMap<String, Vec<String>>, HashMap<String, u64>, Arc<dyn object_store::ObjectStore>)> {
        let schema = crate::schema::get_schema(table_name).ok_or_else(|| anyhow::anyhow!("missing schema for index coverage: {table_name}"))?;
        let (uris, sizes, delta_store) = {
            let t = table_ref.read().await;
            let sizes: HashMap<String, u64> =
                t.snapshot().map(|s| s.log_data().iter().map(|f| (f.path().into_owned(), f.size() as u64)).collect()).unwrap_or_default();
            (t.get_file_uris()?.collect::<Vec<String>>(), sizes, t.log_store().object_store(None))
        };
        let by_pid: HashMap<String, Vec<String>> = uris
            .into_iter()
            .filter(|uri| uri.ends_with(".parquet"))
            .filter_map(|uri| Some((crate::tantivy::search::project_id_of_uri(&uri)?.to_string(), uri)))
            .into_group_map();
        let max_bytes = self.config.tantivy.timefusion_tantivy_backfill_max_file_mb * 1024 * 1024;
        let mut result: HashMap<String, Vec<String>> = HashMap::with_capacity(by_pid.len());
        for (pid, mut uris) in by_pid {
            let manifest = crate::tantivy::load_manifest(svc.object_store.as_ref(), table_name, &pid).await?;
            let covered: HashSet<&String> =
                manifest.entries.values().filter(|e| e.covers_current_elements(schema)).flat_map(|e| e.covered_files.iter()).collect();
            uris.retain(|uri| !covered.contains(uri));
            if max_bytes > 0 {
                let before = uris.len();
                uris.retain(|uri| crate::tantivy::search::parquet_rel_of_uri(uri).and_then(|rel| sizes.get(rel)).is_none_or(|size| *size <= max_bytes));
                let skipped = before - uris.len();
                *oversized = oversized.saturating_add(skipped as u64);
                if warn_skipped && skipped > 0 {
                    warn!(table_name, project_id = %pid, skipped, "tantivy backfill skipped files over TIMEFUSION_TANTIVY_BACKFILL_MAX_FILE_MB");
                }
            }
            result.insert(pid, uris);
        }
        Ok((result, sizes, delta_store))
    }

    /// Commit one project's deferred manifest entries. Best-effort: the blobs are already
    /// uploaded, so a failed commit only costs a rebuild next pass.
    async fn commit_manifest_batch(
        svc: &crate::tantivy::search::TantivyIndexService, table_name: &str, project_id: &str,
        pending: &mut HashMap<String, Vec<(String, crate::tantivy::ManifestEntry)>>,
    ) {
        let Some(entries) = pending.remove(project_id).filter(|e| !e.is_empty()) else { return };
        let (n, started) = (entries.len(), std::time::Instant::now());
        match crate::tantivy::upsert_manifest_many(svc.object_store.as_ref(), table_name, project_id, entries).await {
            Ok(()) => {
                metrics::counter!(scan_metric_names::TANTIVY_MANIFEST_COMMITS).increment(1);
                metrics::counter!(scan_metric_names::TANTIVY_MANIFEST_COMMIT_US).increment(started.elapsed().as_micros() as u64);
            }
            Err(e) => warn!(table_name, project_id, entries = n, %e, "tantivy backfill manifest batch commit failed; files stay uncovered for the next pass"),
        }
    }

    /// Every physical table holding `table_name`'s files: `"default"` (the unified table) plus
    /// each custom-storage project with its own copy.
    async fn table_roots(&self, table_name: &str) -> Vec<String> {
        let customs = self.custom_project_tables.read().await;
        std::iter::once("default".to_string()).chain(customs.keys().filter(|(_, t)| t == table_name).map(|(p, _)| p.clone())).collect()
    }

    /// Refresh global Tantivy coverage gauges without building indexes. Reads Delta and index
    /// metadata only. Returns `(uncovered, oversized, by_age)`, `by_age` = `[today, 1-7d, older]`.
    pub async fn tantivy_coverage_census(&self) -> anyhow::Result<(u64, u64, [u64; 3])> {
        let Some(svc) = self.tantivy_indexer().cloned() else { return Ok((0, 0, [0; 3])) };
        let (mut uncovered, mut oversized) = (0u64, 0u64);
        let mut by_age = [0u64; 3];
        let today = crate::support::now_micros() / 86_400_000_000;
        for table_name in svc.config.indexed_tables().into_iter().filter(|t| svc.config.is_table_indexed(t)) {
            for root in self.table_roots(&table_name).await {
                let Ok(table_ref) = self.resolve_table(&root, &table_name).await else { continue };
                let (by_pid, ..) = self.group_uncovered_files_by_project(&svc, &table_ref, &table_name, &mut oversized, false).await?;
                for uri in by_pid.into_values().flatten() {
                    uncovered = uncovered.saturating_add(1);
                    // Partition date, not file mtime: a rewrite of old data is still old.
                    let age_days = crate::storage::date_partition_of(&uri)
                        .map_or(i64::MAX, |d| today - d.and_hms_opt(0, 0, 0).map_or(0, |t| t.and_utc().timestamp_micros() / 86_400_000_000));
                    by_age[usize::from(age_days > 0) + usize::from(age_days > 7)] += 1;
                }
            }
        }
        let stats = crate::observability::maintenance_stats();
        stats.tantivy_uncovered_files.store(uncovered, std::sync::atomic::Ordering::Relaxed);
        stats.tantivy_oversized_skipped.store(oversized, std::sync::atomic::Ordering::Relaxed);
        Ok((uncovered, oversized, by_age))
    }

    /// For every table with bloom-enabled columns, diff live parquet against the
    /// per-(project,date) sidecars, lift the missing files' parquet blooms (footer + bloom
    /// ranges, no row decode) and GC entries for retired files. Newest dates first, bounded
    /// per pass. Returns `(built, errors)`.
    pub async fn bloom_sidecar_reconcile(&self) -> anyhow::Result<(usize, usize)> {
        use crate::read::bloom_prune;
        let Some(reg) = self.bloom_prune().cloned() else { return Ok((0, 0)) };
        let mut budget = self.config.maintenance.timefusion_bloom_sidecar_files_per_pass;
        let (mut built, mut errors) = (0usize, 0usize);
        for table_name in crate::schema::registry().list_tables() {
            let Some(schema) = crate::schema::get_schema(&table_name) else { continue };
            let cols: Vec<String> = schema.fields.iter().filter(|f| f.bloom_filter).map(|f| f.name.clone()).collect();
            if cols.is_empty() {
                continue;
            }
            for root in self.table_roots(&table_name).await {
                if budget == 0 {
                    return Ok((built, errors));
                }
                let Ok(table_ref) = self.resolve_table(&root, &table_name).await else { continue };
                let (rels, delta_store) = {
                    let t = table_ref.read().await;
                    let Ok(snapshot) = t.snapshot() else { continue };
                    let rels: Vec<(String, u64)> = snapshot.log_data().iter().map(|f| (f.path().into_owned(), f.size() as u64)).collect();
                    (rels, t.log_store().object_store(None))
                };
                // Group by (project, date); newest dates first so hot partitions converge first.
                let mut cells: Vec<_> = rels
                    .into_iter()
                    .filter(|(rel, _)| rel.ends_with(".parquet"))
                    .filter_map(|(rel, size)| Some((bloom_prune::project_date_of_rel(&rel).map(|(p, d)| (p.to_string(), d.to_string()))?, (rel, size))))
                    .into_group_map()
                    .into_iter()
                    .collect();
                cells.sort_by(|a, b| b.0.1.cmp(&a.0.1));
                for ((pid, date), files) in cells {
                    if budget == 0 {
                        return Ok((built, errors));
                    }
                    let existing = reg.load_sidecar_raw(&table_name, &pid, &date).await.unwrap_or_default();
                    let existing_count = existing.files.len();
                    let live: HashSet<&str> = files.iter().map(|(rel, _)| rel.as_str()).collect();
                    let mut kept: Vec<bloom_prune::FileBlooms> = existing.files.into_iter().filter(|f| live.contains(f.rel.as_str())).collect();
                    let known: HashSet<&str> = kept.iter().map(|f| f.rel.as_str()).collect();
                    let missing: Vec<(String, u64)> =
                        files.iter().filter(|(rel, _)| !known.contains(rel.as_str())).map(|(rel, size)| (rel.clone(), *size)).take(budget).collect();
                    if missing.is_empty() && kept.len() == existing_count {
                        continue; // converged: nothing new, nothing retired
                    }
                    budget = budget.saturating_sub(missing.len());
                    let mut results = futures::stream::iter(missing.into_iter().map(|(rel, size)| {
                        let store = delta_store.clone();
                        let cols = cols.clone();
                        async move { bloom_prune::build_file_blooms(store, &rel, size, &cols).await }
                    }))
                    .buffer_unordered(4);
                    while let Some(res) = results.next().await {
                        match res {
                            Ok(fb) => {
                                built += 1;
                                reg.stats.build_files.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                kept.push(fb);
                            }
                            Err(e) => {
                                errors += 1;
                                reg.stats.build_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                debug!(table_name, project_id = %pid, date, "bloom sidecar build failed: {e:#}");
                            }
                        }
                    }
                    if let Err(e) = reg.store_sidecar(&table_name, &pid, &date, bloom_prune::DateSidecar { files: kept }).await {
                        warn!(table_name, project_id = %pid, date, "bloom sidecar store failed: {e:#}");
                    }
                }
            }
        }
        Ok((built, errors))
    }

    async fn backfill_table_indexes(&self, svc: &Arc<crate::tantivy::search::TantivyIndexService>, table_name: &str) -> anyhow::Result<usize> {
        use crate::tantivy::search::parquet_rel_of_uri;
        let slot = self.tantivy_backfill_slots.entry(table_name.to_owned()).or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(1))).clone();
        let Ok(_pass) = slot.try_acquire_owned() else {
            info!(table_name, event = "tantivy_backfill_already_active");
            return Ok(0);
        };
        let roots = self.table_roots(table_name).await;
        let mut built = 0usize;
        let (mut uncovered_total, mut oversized_total) = (0u64, 0u64);
        let mut deferred_total = 0u64;
        for root in roots {
            let Ok(table_ref) = self.resolve_table(&root, table_name).await else {
                continue;
            };
            let (by_pid, sizes, delta_store) = self.group_uncovered_files_by_project(svc, &table_ref, table_name, &mut oversized_total, true).await?;
            let mut queues = Vec::with_capacity(by_pid.len());
            // `skip_today` keeps the backfill from racing the hot-tail packer, which rewrites
            // today's files and would void the index. Only base tables are hot-packed.
            let hot_packed = crate::schema::registry().get(table_name).is_some_and(|s| !s.fields.iter().any(|f| f.name == "rollup_generation"));
            let skip_today =
                (self.config.tantivy.timefusion_tantivy_backfill_skip_today && hot_packed).then(|| format!("date={}", chrono::Utc::now().date_naive()));
            let mut skipped_today = 0u64;
            for (pid, mut uris) in by_pid {
                // Counted before the hot-tail skip, so the gauge reports the true total.
                uncovered_total = uncovered_total.saturating_add(uris.len() as u64);
                skipped_today = skipped_today.saturating_add(drop_hot_partition(&mut uris, skip_today.as_deref()));
                sort_backfill_uris_newest_first(&mut uris);
                let queue = uris.into_iter().filter_map(|uri| Some((parquet_rel_of_uri(&uri)?.to_string(), uri))).collect::<VecDeque<_>>();
                queues.push((pid, queue));
            }
            let table_owned = table_name.to_string();
            // Fair round-robin ordering means truncation drops the oldest round across every
            // project, never one project's whole queue.
            let cap = self.config.tantivy.timefusion_tantivy_backfill_max_files_per_pass;
            let available: usize = queues.iter().map(|(_, q)| q.len()).sum();
            let (work, reserved_tail) = fair_tantivy_backfill_work_split(queues, cap, self.config.tantivy.timefusion_tantivy_backfill_tail_share_pct);
            // Applied after the fair split, so ordering picks the files and the byte budget
            // only decides how far down that order the pass gets.
            let budget = self.config.tantivy.timefusion_tantivy_backfill_max_bytes_per_pass_mb * 1024 * 1024;
            let work = truncate_to_byte_budget(work, &sizes, budget);
            deferred_total = deferred_total.saturating_add((available - work.len()) as u64);
            let planned = work.len();
            let planned_mb = work.iter().filter_map(|(_, rel, _)| sizes.get(rel)).sum::<u64>() / (1024 * 1024);
            info!(table_name, root = %root, planned, planned_mb, cap, budget_mb = budget / (1024 * 1024), skipped_today, event = "tantivy_backfill_started");
            let mut done = 0usize;
            // Batched per project: each manifest write is a full read-modify-write under a
            // per-project lock, so publishing per build would pay that cost per file.
            let mut pending: HashMap<String, Vec<(String, crate::tantivy::ManifestEntry)>> = HashMap::new();
            let mut pending_since: HashMap<String, std::time::Instant> = HashMap::new();
            let mut jobs = futures::stream::iter(work.into_iter().map(|(pid, rel, uri)| {
                let (svc, store, table) = (svc.clone(), delta_store.clone(), table_owned.clone());
                async move { (pid.clone(), svc.build_index_for_file_deferred(&table, &pid, &rel, &uri, store).await) }
            }))
            .buffer_unordered(self.config.tantivy.timefusion_tantivy_build_concurrency.max(1));
            // Flush on a timer, not only on build completions: builds take minutes, so a
            // completion-driven sweep would never fire in the gap the age bound protects.
            let mut flush_tick = tokio::time::interval(MANIFEST_MAX_AGE / 2);
            flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                let (pid, result) = tokio::select! {
                    item = jobs.next() => match item {
                        Some(v) => v,
                        None => break,
                    },
                    _ = flush_tick.tick() => {
                        for project in due_manifest_flushes(&pending, &pending_since) {
                            pending_since.remove(&project);
                            Self::commit_manifest_batch(svc, table_name, &project, &mut pending).await;
                        }
                        continue;
                    }
                };
                match result {
                    Ok(entry) => {
                        built += 1;
                        let from_reserved_tail = entry.1.covered_files.iter().any(|u| reserved_tail.contains(u));
                        info!(table_name, project_id = %pid, from_reserved_tail, event = "tantivy_backfill_unit");
                        metrics::counter!(scan_metric_names::TANTIVY_BACKFILL_BUILT).increment(1);
                        pending.entry(pid.clone()).or_default().push(entry);
                        pending_since.entry(pid.clone()).or_insert_with(std::time::Instant::now);
                    }
                    Err(e) => warn!("tantivy backfill build failed table={} project={}: {}", table_name, pid, e),
                }
                // Swept over EVERY pending project: checking only `pid` leaves the age bound
                // unreachable for a project with one build per pass.
                for project in due_manifest_flushes(&pending, &pending_since) {
                    pending_since.remove(&project);
                    Self::commit_manifest_batch(svc, table_name, &project, &mut pending).await;
                }
                done += 1;
                if done.is_multiple_of(25) {
                    info!(table_name, root = %root, done, planned, built, event = "tantivy_backfill_progress");
                }
            }
            for pid in pending.keys().cloned().collect::<Vec<_>>() {
                Self::commit_manifest_batch(svc, table_name, &pid, &mut pending).await;
            }
        }
        info!(
            table_name,
            built,
            uncovered_before = uncovered_total,
            oversized_skipped = oversized_total,
            deferred_to_next_pass = deferred_total,
            event = "tantivy_backfill_pass"
        );
        Ok(built)
    }
}
