//! Shared server-bootstrap wiring used by both `main.rs` and the E2E test
//! harness. Keeping this in one place guarantees the test path matches prod
//! — the whole point of the E2E suite is to catch the class of bug that
//! "CI doesn't reproduce because the harness skips half the wiring".
//!
//! Returns the fully wired pieces; the caller decides what to do with them
//! (serve pgwire, expose for assertions, etc.).

use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use datafusion::execution::context::SessionContext;
use tokio_util::sync::CancellationToken;

use crate::{
    buffered_write_layer::{BufferedWriteLayer, DeltaWatermark},
    config::AppConfig,
    database::Database,
};

/// Everything a serving process needs after bootstrap is done.
pub struct Bootstrapped {
    pub db: Arc<Database>,
    pub buffered_layer: Arc<BufferedWriteLayer>,
    /// The SessionContext used by the pgwire handlers — UDFs and table
    /// providers are already registered.
    pub session_ctx: Arc<SessionContext>,
    /// Cancel to signal shutdown to anything we spawned.
    pub shutdown: CancellationToken,
}

/// Raise the open-file soft limit to the hard limit.
///
/// Docker hands a process the daemon's default soft limit — 1024 in prod, against
/// a 524288 hard limit — and a database that memory-maps parquet, holds a tantivy
/// index per partition and serves pgwire exhausts that in seconds. On 2026-08-12
/// prod spent its first 5.5 minutes after boot emitting 1.6M `Too many open files`
/// lines and REFUSING pgwire connections (`Error accept socket`), which also
/// pushed every other line out of the log ring buffer.
///
/// This belongs in the process, not the service definition: CapRover rewrites the
/// service config on every deploy, so a ulimit set out of band does not survive.
/// Best-effort by design — a platform that refuses the raise is not a reason to
/// fail boot, and the log line is enough to diagnose it.
pub fn raise_file_limit() {
    // SAFETY: both calls take a valid, fully-initialized `rlimit`, and neither
    // retains the pointer past the call.
    unsafe {
        let mut limit = std::mem::zeroed::<libc::rlimit>();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 || limit.rlim_cur >= limit.rlim_max {
            return;
        }
        let (previous, target) = (limit.rlim_cur, limit.rlim_max);
        limit.rlim_cur = target;
        match libc::setrlimit(libc::RLIMIT_NOFILE, &limit) {
            0 => tracing::info!(previous, target, "raised the open-file soft limit to the hard limit"),
            _ => tracing::warn!(previous, target, error = %std::io::Error::last_os_error(), "could not raise the open-file soft limit"),
        }
    }
}

/// Build the BufferedWriteLayer + Database wiring exactly as `main.rs` does,
/// minus listener binding / signal handling / telemetry init (the caller
/// owns those — they differ between prod and test).
///
/// Side effects: spawns the flush + eviction background tasks, performs
/// WAL recovery, and starts maintenance schedulers. The returned
/// `CancellationToken` can be triggered to ask spawned work to wind down.
pub async fn bootstrap(cfg: Arc<AppConfig>) -> Result<Bootstrapped> {
    crate::clock::init_from_env();
    raise_file_limit();

    let t_db = std::time::Instant::now();
    let mut db = Database::with_config(Arc::clone(&cfg)).await?;
    tracing::info!("bootstrap.phase=database_init elapsed_ms={}", t_db.elapsed().as_millis());

    let delta_write_callback = delta_write_callback(&db);

    let mut session_context = Arc::new(db.clone()).create_session_context();
    db.setup_session_udfs(&mut session_context)?;
    let registry: Arc<crate::functions::FnRegistry> = Arc::new(session_context.state());

    // Pre-init WAL GC (gated + drained-flag consumption inside the helper —
    // same call as main.rs so the e2e path mirrors prod).
    crate::wal::boot_wal_gc(&cfg.core.wal_dir());

    let t_layer = std::time::Instant::now();
    let mut layer = BufferedWriteLayer::with_config(Arc::clone(&cfg), registry)?
        .with_delta_writer(delta_write_callback)
        .with_coalesced_delta_writer(coalesced_delta_write_callback(&db));
    tracing::info!("bootstrap.phase=buffered_write_layer_init elapsed_ms={}", t_layer.elapsed().as_millis());

    // Optional tantivy sidecar (mirrors main.rs). Disabled when no indexed
    // tables OR the bucket is unset (tests with foyer-only setups).
    let bucket = cfg.aws.aws_s3_bucket.clone().unwrap_or_default();
    if !cfg.tantivy.indexed_tables().is_empty() && !bucket.is_empty() {
        let storage_uri = format!("s3://{}/{}/tantivy", bucket, cfg.core.timefusion_table_prefix);
        let obj_store = db.create_object_store(&storage_uri, &cfg.aws.build_storage_options(None)).await?;
        let svc = Arc::new(crate::tantivy_index::service::TantivyIndexService::new(obj_store.clone(), Arc::new(cfg.tantivy.clone())));
        layer = layer.with_tantivy_indexer(svc.clone().callback());
        let search = Arc::new(crate::tantivy_index::search::TantivySearchService::new(obj_store, cfg.core.timefusion_data_dir.clone()));
        db = db.with_tantivy_search(search).with_tantivy_indexer(svc);
    }

    let buffered_layer = Arc::new(layer);

    // Mirror main.rs: clean snapshot → skip the Delta cursor scan; dirty/missing
    // snapshot → derive cursors from Delta so WAL replay doesn't re-inject
    // entries Delta already has. Keeping this in the test-shared bootstrap
    // means e2e startup-time assertions exercise the same path as prod.
    // Per-phase timing is emitted at INFO so cold-start regressions surface
    // without needing trace-level enabled.
    let wal_ref = buffered_layer.wal();
    let t_snap = std::time::Instant::now();
    let clean_snapshot = wal_ref.load_cursor_snapshot().is_some_and(|snap| wal_ref.restore_cursor_snapshot(&snap).is_ok() && snap.clean_shutdown);
    let local_wal_consumed = !clean_snapshot && wal_ref.can_skip_delta_reconcile().unwrap_or(false);
    let skip_delta_scan = clean_snapshot || local_wal_consumed;
    tracing::info!(
        "bootstrap.phase=cursor_snapshot skip_delta_scan={skip_delta_scan} clean_snapshot={clean_snapshot} local_wal_consumed={local_wal_consumed} elapsed_ms={}",
        t_snap.elapsed().as_millis()
    );
    if !skip_delta_scan {
        let t_delta = std::time::Instant::now();
        let advanced = db.derive_wal_cursors_from_delta(wal_ref).await.unwrap_or(0);
        tracing::info!("bootstrap.phase=delta_cursor_reconcile shards_advanced={advanced} elapsed_ms={}", t_delta.elapsed().as_millis());
    }

    let t_wal = std::time::Instant::now();
    buffered_layer.recover_from_wal().await?;
    tracing::info!("bootstrap.phase=wal_replay elapsed_ms={}", t_wal.elapsed().as_millis());
    buffered_layer.start_background_tasks().await;

    db = db.with_buffered_layer(Arc::clone(&buffered_layer));
    db.start_dml_coalescer();
    db = db.start_maintenance_schedulers().await?;
    let db = Arc::new(db);
    db.setup_session_tables(&mut session_context)?;
    // Non-blocking: snapshot load + footer warm-up off the first query's path.
    db.preload_tables();
    // Non-blocking: index live files no manifest entry covers (config-gated),
    // and warm the local index cache with recent blobs (config-gated).
    db.spawn_tantivy_backfill();
    db.spawn_tantivy_prefetch();
    db.spawn_deferred_tantivy_reindex(Arc::clone(&buffered_layer));

    Ok(Bootstrapped { db, buffered_layer, session_ctx: Arc::new(session_context), shutdown: CancellationToken::new() })
}

/// The C3 coalescing flush writer: hands the whole tick's groups to
/// The per-bucket Delta write a flush hands its rows to. Shared by `bootstrap`,
/// `main` AND the test harnesses, because a layer built WITHOUT it does not
/// fail — `flush_bucket` logs "No delta write callback configured, skipping
/// flush" and drains the bucket anyway. A test layer missing this therefore
/// silently discards every flushed row while `is_empty()` reports success
/// (2026-08-02: `buffer_consistency_test` asserted exactly that and passed for
/// as long as its DML never routed through a flush).
pub fn delta_write_callback(db: &crate::database::Database) -> crate::buffered_write_layer::DeltaWriteCallback {
    let db = db.clone();
    Arc::new(move |project_id: String, table_name: String, batches: Vec<RecordBatch>, wal_watermark: DeltaWatermark| {
        let db = db.clone();
        // insert_records_batch warms the just-flushed files itself
        // (watermark-gated) — warming here too would double the GETs.
        Box::pin(async move { db.insert_records_batch(&project_id, &table_name, batches, true, Some(&wal_watermark)).await })
    })
}

/// `insert_records_batches_coalesced`, which emits one Delta commit per
/// PHYSICAL table. Shared by `bootstrap` and `main` so the e2e path and prod
/// wire the identical callback. Only used when
/// `TIMEFUSION_FLUSH_COALESCE_COMMITS` is on.
pub fn coalesced_delta_write_callback(db: &crate::database::Database) -> crate::buffered_write_layer::DeltaCoalescedWriteCallback {
    let db = db.clone();
    Arc::new(move |units: Vec<crate::buffered_write_layer::FlushUnit>| {
        let db = db.clone();
        Box::pin(async move {
            let (topics, units): (Vec<(String, String)>, Vec<_>) = units
                .into_iter()
                .map(|u| {
                    (
                        (u.project_id.clone(), u.table_name.clone()),
                        crate::database::CoalescedWriteUnit { project_id: u.project_id, table_name: u.table_name, batches: u.batches, watermark: u.watermark },
                    )
                })
                .unzip();
            let results = db.insert_records_batches_coalesced(units).await;
            // `insert_records_batches_coalesced` warms the flushed files itself
            // (as insert_records_batch does) — no warm here, or every flush
            // would issue the warm GETs twice.
            // Mark on ANY settled commit, not just one with a non-empty added
            // list: the flag means "this (project, table) has Delta files", which
            // is true the moment the commit lands. Attribution can legitimately
            // return an empty list for a project that did flush (e.g. a
            // concurrent snapshot re-materialize makes the pre/post diff miss),
            // and gating on it would leave `delta_scan_can_be_skipped` true —
            // queries would skip Delta and read only MemBuffer. Tantivy indexing
            // stays driven by the actual file list inside the insert path.
            // Identical to the per-project callback in `main.rs`.
            topics
                .iter()
                .zip(&results)
                .filter(|(_, result)| result.is_ok())
                .for_each(|((project_id, table_name), _)| db.mark_delta_has_files(project_id, table_name));
            results
        })
    })
}
