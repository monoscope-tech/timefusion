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
    buffered_write_layer::{BufferedWriteLayer, DeltaWatermark, DeltaWriteCallback},
    config::AppConfig,
    database::Database,
};

/// Everything a serving process needs after bootstrap is done.
pub struct Bootstrapped {
    pub db:             Arc<Database>,
    pub buffered_layer: Arc<BufferedWriteLayer>,
    /// The SessionContext used by the pgwire handlers — UDFs and table
    /// providers are already registered.
    pub session_ctx:    Arc<SessionContext>,
    /// Cancel to signal shutdown to anything we spawned.
    pub shutdown:       CancellationToken,
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

    let t_db = std::time::Instant::now();
    let mut db = Database::with_config(Arc::clone(&cfg)).await?;
    tracing::info!("bootstrap.phase=database_init elapsed_ms={}", t_db.elapsed().as_millis());

    let db_for_callback = db.clone();
    let delta_write_callback: DeltaWriteCallback = Arc::new(
        move |project_id: String, table_name: String, batches: Vec<RecordBatch>, wal_watermark: DeltaWatermark| {
            let db = db_for_callback.clone();
            Box::pin(async move {
                let added = db
                    .insert_records_batch(&project_id, &table_name, batches, true, Some(&wal_watermark))
                    .await?;
                db.warm_cache_for_table(&project_id, &table_name, added.clone());
                Ok(added)
            })
        },
    );

    let mut session_context = Arc::new(db.clone()).create_session_context();
    db.setup_session_udfs(&mut session_context)?;
    let registry: Arc<crate::functions::FnRegistry> = Arc::new(session_context.state());

    // Pre-init WAL GC: delete files older than 2× retention before walrus
    // enumerates the dir on construction. Without this, accumulated leaks
    // from prior process lifetimes dominate startup (see memory
    // `wal_bloat_startup.md` — prod hit 467 GB / 12-min startup).
    let t_gc = std::time::Instant::now();
    let max_age = std::time::Duration::from_secs(cfg.buffer.retention_mins() * 60 * 2);
    match crate::wal::gc_wal_files(&cfg.core.wal_dir(), max_age) {
        Ok((deleted, bytes_freed)) => tracing::info!(
            "bootstrap.phase=wal_gc deleted={deleted} bytes_freed={bytes_freed} elapsed_ms={}",
            t_gc.elapsed().as_millis()
        ),
        Err(e) => tracing::warn!("bootstrap.phase=wal_gc error={e} elapsed_ms={}", t_gc.elapsed().as_millis()),
    }

    let t_layer = std::time::Instant::now();
    let mut layer = BufferedWriteLayer::with_config(Arc::clone(&cfg), registry)?.with_delta_writer(delta_write_callback);
    tracing::info!("bootstrap.phase=buffered_write_layer_init elapsed_ms={}", t_layer.elapsed().as_millis());

    // Optional tantivy sidecar (mirrors main.rs). Disabled when no indexed
    // tables OR the bucket is unset (tests with foyer-only setups).
    let indexed_tables = cfg.tantivy.indexed_tables();
    if !indexed_tables.is_empty() {
        let bucket = cfg.aws.aws_s3_bucket.clone().unwrap_or_default();
        if !bucket.is_empty() {
            let storage_uri = format!("s3://{}/{}/tantivy", bucket, cfg.core.timefusion_table_prefix);
            let storage_opts = cfg.aws.build_storage_options(None);
            let obj_store = db.create_object_store(&storage_uri, &storage_opts).await?;
            let svc = Arc::new(crate::tantivy_index::service::TantivyIndexService::new(
                obj_store.clone(),
                Arc::new(cfg.tantivy.clone()),
            ));
            layer = layer.with_tantivy_indexer(svc.clone().callback());
            let cache_root = cfg.core.timefusion_data_dir.clone();
            let search = Arc::new(crate::tantivy_index::search::TantivySearchService::new(obj_store, cache_root));
            db = db.with_tantivy_search(search).with_tantivy_indexer(svc);
        }
    }

    let buffered_layer = Arc::new(layer);
    // Tantivy init may also dominate on cold start if many indexed tables
    // exist; bracket it independently from the WAL/Delta phases below.

    // Mirror main.rs: clean snapshot → skip the Delta cursor scan; dirty/missing
    // snapshot → derive cursors from Delta so WAL replay doesn't re-inject
    // entries Delta already has. Keeping this in the test-shared bootstrap
    // means e2e startup-time assertions exercise the same path as prod.
    // Per-phase timing is emitted at INFO so cold-start regressions surface
    // without needing trace-level enabled.
    let wal_ref = buffered_layer.wal();
    let t_snap = std::time::Instant::now();
    let skip_delta_scan = if let Some(snap) = wal_ref.load_cursor_snapshot() {
        wal_ref.restore_cursor_snapshot(&snap).is_ok() && snap.clean_shutdown
    } else {
        false
    };
    tracing::info!(
        "bootstrap.phase=cursor_snapshot skip_delta_scan={skip_delta_scan} elapsed_ms={}",
        t_snap.elapsed().as_millis()
    );
    if !skip_delta_scan {
        let t_delta = std::time::Instant::now();
        let advanced = db.derive_wal_cursors_from_delta(wal_ref).await.unwrap_or(0);
        tracing::info!(
            "bootstrap.phase=delta_cursor_reconcile shards_advanced={advanced} elapsed_ms={}",
            t_delta.elapsed().as_millis()
        );
    }

    let t_wal = std::time::Instant::now();
    buffered_layer.recover_from_wal().await?;
    tracing::info!("bootstrap.phase=wal_replay elapsed_ms={}", t_wal.elapsed().as_millis());
    buffered_layer.start_background_tasks().await;

    db = db.with_buffered_layer(Arc::clone(&buffered_layer));
    db = db.start_maintenance_schedulers().await?;
    let db = Arc::new(db);
    db.setup_session_tables(&mut session_context)?;

    Ok(Bootstrapped {
        db,
        buffered_layer,
        session_ctx: Arc::new(session_context),
        shutdown: CancellationToken::new(),
    })
}
