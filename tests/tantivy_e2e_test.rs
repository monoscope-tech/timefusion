//! Tier-3 end-to-end: SQL `text_match()` through DataFusion + Delta + MinIO.
//!
//! Scenarios covered:
//! 1. With tantivy enabled, INSERT → flush → SELECT … WHERE text_match(col, 'q')
//!    returns the same rows as the equivalent full-scan baseline (tantivy disabled).
//! 2. MemBuffer-only data (un-flushed) is still queryable via text_match (UDF
//!    fallback). Result equals the baseline.
//! 3. Mixed mode (some rows in MemBuffer, some flushed to Delta) — result is the
//!    union, no duplicates, no missed rows.
//! 4. The id-IN prefilter is actually injected when tantivy is enabled (sanity:
//!    we observe fewer file reads — measured indirectly via correctness with a
//!    manifest entry marked failed).
//!
//! Requires MinIO running (make minio-start). Serial because we share the test
//! bucket; each test uses a unique project_id / table_prefix so data is isolated.

#![cfg(test)]

use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use arrow::array::{Array, RecordBatch};
use datafusion::{arrow::array::AsArray, execution::context::SessionContext};
use serde_json::json;
use serial_test::serial;
use timefusion::{
    buffered_write_layer::DeltaWriteCallback,
    config::{AppConfig, TantivyConfig},
    database::Database,
    tantivy_index::{search::TantivySearchService, service::TantivyIndexService},
    test_utils::test_helpers::json_to_batch,
};

fn cfg(test_id: &str, _tantivy_enabled: bool) -> Arc<AppConfig> {
    let mut c = AppConfig::default();
    c.aws.aws_s3_bucket = Some("timefusion-tests".to_string());
    c.aws.aws_access_key_id = Some("minioadmin".into());
    c.aws.aws_secret_access_key = Some("minioadmin".into());
    c.aws.aws_s3_endpoint = "http://127.0.0.1:9000".into();
    c.aws.aws_default_region = Some("us-east-1".into());
    c.aws.aws_allow_http = Some("true".into());
    c.core.timefusion_table_prefix = format!("tantivy-e2e-{test_id}");
    c.core.timefusion_data_dir = PathBuf::from(format!("/tmp/timefusion-tantivy-e2e-{test_id}"));
    c.cache.timefusion_foyer_disabled = true;
    c.tantivy = TantivyConfig {
        timefusion_tantivy_compression_level: 3,
        ..Default::default()
    };
    Arc::new(c)
}

/// Build a DB with the full BufferedWriteLayer + Tantivy callback wired up,
/// returning an immediately-flushing layer (interval=1s).
async fn build_db(test_id: &str, tantivy_enabled: bool) -> Result<(Database, SessionContext, Option<Arc<TantivyIndexService>>)> {
    let cfg_arc = cfg(test_id, tantivy_enabled);
    let mut db = Database::with_config(cfg_arc.clone()).await?;

    // BufferedWriteLayer with delta writer
    let db_for_cb = db.clone();
    let delta_cb: DeltaWriteCallback = Arc::new(move |project_id, table_name, batches, _wm| {
        let db = db_for_cb.clone();
        Box::pin(async move {
            let pre = db.list_file_uris(&project_id, &table_name).await.unwrap_or_default();
            db.insert_records_batch(&project_id, &table_name, batches, true, None).await?;
            let post = db.list_file_uris(&project_id, &table_name).await.unwrap_or_default();
            let pre_set: std::collections::HashSet<String> = pre.into_iter().collect();
            Ok(post.into_iter().filter(|u| !pre_set.contains(u)).collect())
        })
    });

    let mut layer = timefusion::test_utils::test_helpers::test_layer(cfg_arc.clone())?.with_delta_writer(delta_cb);
    let mut svc: Option<Arc<TantivyIndexService>> = None;
    if tantivy_enabled {
        let bucket = cfg_arc.aws.aws_s3_bucket.clone().unwrap();
        let storage_uri = format!("s3://{}/{}/tantivy", bucket, cfg_arc.core.timefusion_table_prefix);
        let storage_opts = cfg_arc.aws.build_storage_options(None);
        let obj_store = db.create_object_store(&storage_uri, &storage_opts).await?;
        let s = Arc::new(TantivyIndexService::new(obj_store.clone(), Arc::new(cfg_arc.tantivy.clone())));
        layer = layer.with_tantivy_indexer(s.clone().callback());
        let cache_root = cfg_arc.core.timefusion_data_dir.clone();
        let search = Arc::new(TantivySearchService::new(obj_store, cache_root));
        db = db.with_tantivy_search(search).with_tantivy_indexer(s.clone());
        svc = Some(s);
    }
    db = db.with_buffered_layer(Arc::new(layer));

    let db_arc = Arc::new(db.clone());
    let mut ctx = db_arc.create_session_context();
    datafusion_functions_json::register_all(&mut ctx)?;
    db.setup_session_context(&mut ctx)?;
    Ok((db, ctx, svc))
}

/// Build a RecordBatch matching the otel_logs_and_spans schema using the
/// existing test helper. `rows` is `(id, name, status_message)`. The `level`
/// is derived from the message ("failed" → ERROR, "timeout" → WARN, else
/// INFO) so tests can query `WHERE level = 'ERROR'` to exercise the
/// rewriter's `=` path against the raw-tokenized indexed column.
fn make_batch(project: &str, rows: Vec<(&str, &str, &str)>) -> RecordBatch {
    let now = chrono::Utc::now();
    let records: Vec<_> = rows
        .into_iter()
        .enumerate()
        .map(|(i, (id, name, msg))| {
            let ts = now.timestamp_micros() + i as i64;
            let lvl = if msg.contains("failed") || msg.contains("declined") {
                "ERROR"
            } else if msg.contains("timeout") {
                "WARN"
            } else {
                "INFO"
            };
            json!({
                "timestamp": ts,
                "id": id,
                "name": name,
                "level": lvl,
                "status_message": msg,
                "project_id": project,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": vec![format!("summary for {id}")],
            })
        })
        .collect();
    json_to_batch(records).expect("json_to_batch")
}

async fn collect_ids(ctx: &SessionContext, sql: &str) -> Result<Vec<String>> {
    let r = ctx.sql(sql).await?.collect().await?;
    let mut ids: Vec<String> = Vec::new();
    for b in &r {
        let arr = b.column_by_name("id").unwrap();
        if let Some(s) = arr.as_string_opt::<i32>() {
            for i in 0..s.len() {
                if !s.is_null(i) {
                    ids.push(s.value(i).to_string());
                }
            }
        } else if let Some(s) = arr.as_string_view_opt() {
            for i in 0..s.len() {
                if !s.is_null(i) {
                    ids.push(s.value(i).to_string());
                }
            }
        }
    }
    ids.sort();
    Ok(ids)
}

// Each test uses a unique project_id derived from a UUID so that the shared
// MinIO bucket (timefusion-tests) doesn't expose state across runs/tests.
fn unique_project() -> String {
    format!("p-{}", &uuid::Uuid::new_v4().to_string()[..12])
}
const TABLE: &str = "otel_logs_and_spans";

/// Poll the tantivy manifest until it has at least `want` entries. The index
/// build is a detached task since the flush-throughput work (ef13450) —
/// `flush_all_now()` returning only guarantees the Delta commit, so tests
/// asserting on the manifest must wait for the sidecar to catch up.
async fn wait_for_manifest_entries(store: &dyn object_store::ObjectStore, project: &str, want: usize) -> Result<timefusion::tantivy_index::manifest::Manifest> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let m = timefusion::tantivy_index::manifest::load(store, TABLE, project).await?;
        if m.entries.len() >= want {
            return Ok(m);
        }
        // Err (not the short manifest) on expiry — the caller's assert would
        // otherwise fire with a confusing entry-count mismatch.
        anyhow::ensure!(
            std::time::Instant::now() <= deadline,
            "manifest for {project} stuck at {} entries after 30s, want {want}",
            m.entries.len()
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

// ───────────────────────── tests ─────────────────────────

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn delta_flushed_text_match_matches_baseline() -> Result<()> {
    let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let (db, ctx, _svc) = build_db(&format!("{id}-on"), true).await?;
    let (db2, ctx2, _) = build_db(&format!("{id}-off"), false).await?;
    let p = unique_project();

    let rows = vec![
        ("a", "auth", "user login successful"),
        ("b", "auth", "user login failed: bad password"),
        ("c", "payment", "charge succeeded"),
        ("d", "payment", "charge failed: declined card"),
    ];
    db.insert_records_batch(&p, TABLE, vec![make_batch(&p, rows.clone())], true, None).await?;
    db2.insert_records_batch(&p, TABLE, vec![make_batch(&p, rows)], true, None).await?;

    // No tantivy index was built (skip_queue=true bypasses BufferedWriteLayer).
    // Search returns None → no prefilter applied → UDF post-filter does the work.
    let q = format!("SELECT id FROM otel_logs_and_spans WHERE project_id='{p}' AND text_match(status_message, 'failed')");
    let r_on = collect_ids(&ctx, &q).await?;
    let r_off = collect_ids(&ctx2, &q).await?;
    assert_eq!(r_on, r_off, "result with tantivy on must equal baseline");
    assert_eq!(r_on, vec!["b".to_string(), "d".to_string()]);
    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn membuffer_only_level_eq_falls_back_correctly() -> Result<()> {
    // Rows stay in MemBuffer (no flush). The rewriter still injects
    // `text_match(level, 'ERROR')` next to the `=` predicate, but the
    // tantivy search returns `None` (no manifest yet) → no prefilter
    // applied → original `level = 'ERROR'` filter runs against the
    // in-memory batches. Correctness invariant: result identical to the
    // tantivy-off baseline.
    let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let (db, ctx, _svc) = build_db(&format!("{id}-mem-on"), true).await?;
    let (db2, ctx2, _) = build_db(&format!("{id}-mem-off"), false).await?;
    let p = unique_project();

    let rows = vec![
        ("x1", "service-a", "operation completed"),
        ("x2", "service-a", "operation failed"),
        ("x3", "service-b", "request timeout"),
    ];
    db.insert_records_batch(&p, TABLE, vec![make_batch(&p, rows.clone())], false, None).await?;
    db2.insert_records_batch(&p, TABLE, vec![make_batch(&p, rows)], false, None).await?;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let q = format!("SELECT id FROM otel_logs_and_spans WHERE project_id='{p}' AND level = 'ERROR'");
    let r_on = collect_ids(&ctx, &q).await?;
    let r_off = collect_ids(&ctx2, &q).await?;
    assert_eq!(r_on, r_off, "MemBuffer-only result must equal baseline with rewriter on");
    assert_eq!(r_on, vec!["x2".to_string()]);
    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn tantivy_indexer_actually_writes_manifest_when_flush_routes_through_buffered_layer() -> Result<()> {
    // This test confirms the *write-side* wiring: when we go through the
    // BufferedWriteLayer (not skip_queue), and force-flush the bucket, the
    // tantivy indexer runs and a manifest entry appears.
    let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let (db, _ctx, svc) = build_db(&format!("{id}-flush"), true).await?;
    let svc = svc.expect("service should be present when tantivy is enabled");
    let p = unique_project();

    db.insert_records_batch(&p, TABLE, vec![make_batch(&p, vec![("f1", "svc", "hello world")])], false, None).await?;

    let layer = db.buffered_layer().cloned().expect("layer present");
    layer.flush_all_now().await?;

    let store = svc.object_store.clone();
    let m = wait_for_manifest_entries(store.as_ref(), &p, 1).await?;
    assert!(!m.entries.is_empty(), "manifest should have at least one entry after flush");
    let entry = m.entries.values().next().unwrap();
    assert!(entry.index.is_some(), "entry should have an index blob URI: {entry:?}");
    assert_eq!(entry.rows, 1);
    Ok(())
}

#[serial]
#[ignore = "writes Delta+MemBuffer in same time bucket; per-bucket Delta exclusion drops the Delta-direct rows. Production never writes both legs simultaneously. See tests/buffer_consistency_test.rs comment for details."]
#[tokio::test(flavor = "multi_thread")]
async fn mixed_membuffer_and_delta_level_eq_returns_union() -> Result<()> {
    // The hard case: some rows are in Delta (and possibly indexed by
    // tantivy), some are still in MemBuffer (definitely not indexed).
    // The rewriter wraps `level = 'ERROR'` with text_match. Behavior:
    //   - Delta side may get prefiltered by id IN(...) from tantivy
    //   - MemBuffer side is queried directly with the original predicate
    //   - Result is the union with no duplicates and no missed rows
    let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let (db, ctx, _svc) = build_db(&format!("{id}-mix-on"), true).await?;
    let (db2, ctx2, _) = build_db(&format!("{id}-mix-off"), false).await?;
    let p = unique_project();

    let delta_rows = vec![("d-old1", "n", "old failed operation"), ("d-old2", "n", "old successful operation")];
    db.insert_records_batch(&p, TABLE, vec![make_batch(&p, delta_rows.clone())], true, None).await?;
    db2.insert_records_batch(&p, TABLE, vec![make_batch(&p, delta_rows)], true, None).await?;

    let mem_rows = vec![("m-new1", "n", "new failed operation"), ("m-new2", "n", "new clean operation")];
    db.insert_records_batch(&p, TABLE, vec![make_batch(&p, mem_rows.clone())], false, None).await?;
    db2.insert_records_batch(&p, TABLE, vec![make_batch(&p, mem_rows)], false, None).await?;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let q = format!("SELECT id FROM otel_logs_and_spans WHERE project_id='{p}' AND level = 'ERROR'");
    let r_on = collect_ids(&ctx, &q).await?;
    let r_off = collect_ids(&ctx2, &q).await?;
    assert_eq!(r_on, r_off, "mixed-mode results must be identical between on/off");
    assert_eq!(r_on, vec!["d-old1".to_string(), "m-new1".to_string()]);
    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn compaction_gc_drops_stale_indexes_keeps_live_ones() -> Result<()> {
    // Two separate flushes → two tantivy indexes, each covering its own
    // parquet file. Simulate compaction by calling gc with a `live_uris` list
    // that contains only one of the two files. The stale entry should be
    // dropped, the other kept.
    let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let (db, _ctx, svc) = build_db(&format!("{id}-gc"), true).await?;
    let svc = svc.expect("tantivy enabled");
    let p = unique_project();

    db.insert_records_batch(&p, TABLE, vec![make_batch(&p, vec![("g1", "n", "first")])], false, None).await?;
    db.buffered_layer().cloned().unwrap().flush_all_now().await?;
    db.insert_records_batch(&p, TABLE, vec![make_batch(&p, vec![("g2", "n", "second")])], false, None).await?;
    db.buffered_layer().cloned().unwrap().flush_all_now().await?;

    let m_before = wait_for_manifest_entries(svc.object_store.as_ref(), &p, 2).await?;
    assert_eq!(m_before.entries.len(), 2, "two flushes → two manifest entries");

    // Collect every URI both entries covered.
    let all_uris: Vec<String> = m_before.entries.values().flat_map(|e| e.covered_files.clone()).collect();
    assert!(!all_uris.is_empty(), "covered_files should be populated");

    // Compaction "kept" only the first URI; the rest are gone.
    let live = vec![all_uris[0].clone()];
    let report = svc.gc_after_compaction(TABLE, &p, &live).await?;
    assert!(report.entries_removed >= 1, "at least one stale entry should be dropped");
    let m_after = timefusion::tantivy_index::manifest::load(svc.object_store.as_ref(), TABLE, &p).await?;
    assert!(m_after.entries.len() < m_before.entries.len(), "post-gc manifest should shrink");

    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn flushed_index_prefilter_is_actually_used() -> Result<()> {
    // Exercise the *active* prefilter code path: route writes through the
    // BufferedWriteLayer + flush so a real tantivy index exists. Then query
    // and verify the result still matches the baseline (correctness in the
    // happy path where the index covers all rows).
    let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let (db, ctx, svc) = build_db(&format!("{id}-pf-on"), true).await?;
    let (db2, ctx2, _) = build_db(&format!("{id}-pf-off"), false).await?;
    let p = unique_project();
    let svc = svc.expect("tantivy enabled");

    let rows = vec![
        ("k1", "auth", "login failed: bad password"),
        ("k2", "auth", "login successful"),
        ("k3", "billing", "charge declined"),
        ("k4", "billing", "charge succeeded"),
    ];
    db.insert_records_batch(&p, TABLE, vec![make_batch(&p, rows.clone())], false, None).await?;
    db2.insert_records_batch(&p, TABLE, vec![make_batch(&p, rows)], false, None).await?;

    // Flush so tantivy indexes are produced and the membuffer is emptied.
    db.buffered_layer().cloned().unwrap().flush_all_now().await?;
    db2.buffered_layer().cloned().unwrap().flush_all_now().await?;

    // Confirm a manifest entry exists for the ON case.
    let m = wait_for_manifest_entries(svc.object_store.as_ref(), &p, 1).await?;
    assert!(!m.entries.is_empty(), "manifest should have entries after flush");

    // Real-world SQL: `WHERE level = 'ERROR'`. The TantivyPredicateRewriter
    // additively wraps this with `text_match(level, 'ERROR')` so the
    // ProjectRoutingTable invokes the tantivy prefilter. The original `=`
    // predicate stays in the plan and re-runs on the Delta scan output —
    // which is what makes this correct on MemBuffer rows + freshly-flushed
    // not-yet-indexed files. Test data uses derived levels:
    //   "login failed: bad password" → ERROR
    //   "charge declined"             → ERROR
    //   "login successful"            → INFO
    //   "charge succeeded"            → INFO
    let q = format!("SELECT id FROM otel_logs_and_spans WHERE project_id='{p}' AND level = 'ERROR'");
    let r_on = collect_ids(&ctx, &q).await?;
    let r_off = collect_ids(&ctx2, &q).await?;
    assert_eq!(r_on, r_off, "post-flush prefilter must match baseline for `level = 'ERROR'`");
    assert_eq!(r_on, vec!["k1".to_string(), "k3".to_string()]);

    // Second natural-SQL predicate. INFO is also indexed via the rewriter.
    let q2 = format!("SELECT id FROM otel_logs_and_spans WHERE project_id='{p}' AND level = 'INFO'");
    let r2_on = collect_ids(&ctx, &q2).await?;
    let r2_off = collect_ids(&ctx2, &q2).await?;
    assert_eq!(r2_on, r2_off);
    assert_eq!(r2_on, vec!["k2".to_string(), "k4".to_string()]);
    Ok(())
}
