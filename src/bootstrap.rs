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

    let mut db = Database::with_config(Arc::clone(&cfg)).await?;

    let db_for_callback = db.clone();
    let delta_write_callback: DeltaWriteCallback = Arc::new(
        move |project_id: String, table_name: String, batches: Vec<RecordBatch>, wal_watermark: DeltaWatermark| {
            let db = db_for_callback.clone();
            Box::pin(async move {
                let pre = db.list_file_uris(&project_id, &table_name).await.unwrap_or_default();
                db.insert_records_batch(&project_id, &table_name, batches, true, Some(&wal_watermark)).await?;
                let post = db.list_file_uris(&project_id, &table_name).await.unwrap_or_default();
                let pre_set: std::collections::HashSet<String> = pre.into_iter().collect();
                let added: Vec<String> = post.into_iter().filter(|u| !pre_set.contains(u)).collect();
                db.warm_cache_for_table(&project_id, &table_name, added.clone());
                Ok(added)
            })
        },
    );

    let mut session_context = Arc::new(db.clone()).create_session_context();
    db.setup_session_udfs(&mut session_context)?;
    let registry: Arc<crate::functions::FnRegistry> = Arc::new(session_context.state());

    let mut layer = BufferedWriteLayer::with_config(Arc::clone(&cfg), registry)?.with_delta_writer(delta_write_callback);

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

    // WAL replay before background tasks so we don't race spawned flushes.
    buffered_layer.recover_from_wal().await?;
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
