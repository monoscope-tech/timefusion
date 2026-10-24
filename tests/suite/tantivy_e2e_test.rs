//! End-to-end: SQL `text_match()` through DataFusion + Delta + MinIO.
//!
//! Requires MinIO running (make minio-start). Serial because the test bucket is
//! shared; each test uses a unique project_id / table_prefix for isolation.

#![cfg(test)]

use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use arrow::array::{Array, RecordBatch};
use datafusion::{arrow::array::AsArray, execution::context::SessionContext};
use serde_json::json;
use serial_test::serial;
use test_case::test_case;
use timefusion::{
    config::{AppConfig, TantivyConfig},
    database::Database,
    support::test_helpers::json_to_batch,
    tantivy::{
        search::{TantivyIndexService, TantivySearchService},
        udf::{PredNode, TextMatchPred},
    },
    write::DeltaWriteCallback,
};

fn cfg(test_id: &str) -> AppConfig {
    let mut c = AppConfig::default();
    c.aws.aws_s3_bucket = Some("timefusion-tests".to_string());
    c.aws.aws_access_key_id = Some("minioadmin".into());
    c.aws.aws_secret_access_key = Some("minioadmin".into());
    c.aws.aws_s3_endpoint = timefusion::support::test_helpers::minio_endpoint();
    c.aws.aws_default_region = Some("us-east-1".into());
    c.aws.aws_allow_http = Some("true".into());
    c.core.timefusion_table_prefix = format!("tantivy-e2e-{test_id}");
    c.core.timefusion_data_dir = data_dir(test_id);
    c.cache.timefusion_foyer_disabled = true;
    c.tantivy = TantivyConfig {
        timefusion_tantivy_compression_level: 3,
        timefusion_tantivy_route_equality: true,
        timefusion_tantivy_prefilter_min_selectivity_pct: 50,
        timefusion_tantivy_prefilter_max_hits: 2_000,
        timefusion_tantivy_file_pruning: true,
        timefusion_tantivy_row_selection: true,
        // As in prod: a cold cache skips the prefilter, so unseeded tests would never exercise it.
        timefusion_tantivy_seed_cache_on_publish: true,
        ..Default::default()
    };
    c
}

/// The on-disk cache root `cfg` gives a test id; a hand-built
/// `TantivySearchService` must point at the same directory.
fn data_dir(test_id: &str) -> PathBuf {
    PathBuf::from(format!("/tmp/timefusion-tantivy-e2e-{test_id}"))
}

/// Build a DB with the full BufferedWriteLayer + Tantivy callback wired up,
/// returning an immediately-flushing layer (interval=1s).
async fn build_db(test_id: &str, tantivy_enabled: bool) -> Result<(Database, SessionContext, Option<Arc<TantivyIndexService>>)> {
    build_db_with(test_id, tantivy_enabled, |_| {}).await
}

async fn build_db_with(
    test_id: &str, tantivy_enabled: bool, tweak: impl FnOnce(&mut AppConfig),
) -> Result<(Database, SessionContext, Option<Arc<TantivyIndexService>>)> {
    let mut cfg_arc = cfg(test_id);
    tweak(&mut cfg_arc);
    let cfg_arc = Arc::new(cfg_arc);
    let mut db = Database::with_config(cfg_arc.clone()).await?;

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

    let mut layer = timefusion::support::test_helpers::test_layer(cfg_arc.clone())?.with_delta_writer(delta_cb);
    let mut svc: Option<Arc<TantivyIndexService>> = None;
    if tantivy_enabled {
        let bucket = cfg_arc.aws.aws_s3_bucket.clone().unwrap();
        let storage_uri = format!("s3://{}/{}/tantivy", bucket, cfg_arc.core.timefusion_table_prefix);
        let storage_opts = cfg_arc.aws.build_storage_options(None);
        let obj_store = db.create_object_store(&storage_uri, &storage_opts).await?;
        let s = Arc::new(TantivyIndexService::new(
            obj_store.clone(),
            Arc::new(cfg_arc.tantivy.clone()),
            std::env::temp_dir().join(format!("tf-scratch-{}", uuid::Uuid::new_v4())),
        ));
        layer = layer.with_tantivy_indexer(timefusion::server::tantivy_index_callback(&db, Arc::clone(&s)));
        let cache_root = cfg_arc.core.timefusion_data_dir.clone();
        let search = Arc::new(TantivySearchService::new(obj_store, cache_root, Arc::new(cfg_arc.tantivy.clone())));
        s.with_reader(&search);
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

/// Build an otel_logs_and_spans RecordBatch from `(id, name, status_message)`
/// rows. `level` is derived from the message ("failed"/"declined" → ERROR,
/// "timeout" → WARN, else INFO) so tests can query `WHERE level = 'ERROR'`.
fn make_batch<S: AsRef<str>>(project: &str, rows: &[(S, S, S)]) -> RecordBatch {
    let now = chrono::Utc::now();
    let records: Vec<_> = rows
        .iter()
        .enumerate()
        .map(|(i, (id, name, msg))| {
            let (id, name, msg) = (id.as_ref(), name.as_ref(), msg.as_ref());
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
                "attributes___http___response___status_code": if lvl == "ERROR" { 500 } else { 200 },
                "attributes___http___route": format!("/v1/{name}"),
                "project_id": project,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": vec![format!("summary for {id}")],
            })
        })
        .collect();
    json_to_batch(records).expect("json_to_batch")
}

/// Sorted non-null `id` values (the column may arrive as Utf8 or Utf8View).
async fn collect_ids(ctx: &SessionContext, sql: &str) -> Result<Vec<String>> {
    let mut ids: Vec<String> = ctx
        .sql(sql)
        .await?
        .collect()
        .await?
        .iter()
        .filter_map(|b| arrow::compute::cast(b.column_by_name("id").unwrap(), &arrow::datatypes::DataType::Utf8).ok())
        .flat_map(|a| a.as_string::<i32>().iter().flatten().map(ToString::to_string).collect::<Vec<_>>())
        .collect();
    ids.sort();
    Ok(ids)
}

/// Sum of column 1 (the `count(*)` of a `time_bucket` histogram query).
async fn sum_counts(ctx: &SessionContext, sql: &str) -> Result<i64> {
    let batches = ctx.sql(sql).await?.collect().await?;
    Ok(batches.iter().flat_map(|b| b.column(1).as_primitive::<arrow::datatypes::Int64Type>().values()).sum())
}

async fn row_count(ctx: &SessionContext, sql: &str) -> Result<usize> {
    Ok(ctx.sql(sql).await?.collect().await?.iter().map(RecordBatch::num_rows).sum())
}

fn leaf(column: &str, query: &str) -> PredNode {
    PredNode::Leaf(TextMatchPred { column: column.into(), query: query.into() })
}

fn snapshots(db: &Database) -> u64 {
    db.tantivy_search().unwrap().stats.histogram_snapshots.load(std::sync::atomic::Ordering::Relaxed)
}

async fn flush_all(db: &Database) -> Result<()> {
    db.buffered_layer().cloned().expect("layer present").flush_all_now().await?;
    Ok(())
}

// Unique per test so the shared MinIO bucket can't leak state across runs.
fn unique_project() -> String {
    format!("p-{}", &uuid::Uuid::new_v4().to_string()[..12])
}
const TABLE: &str = "otel_logs_and_spans";

/// A flush between the Delta-empty proof and the memory snapshot must not hide
/// rows acknowledged before the query started.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn flush_between_delta_empty_check_and_mem_snapshot_keeps_acknowledged_rows() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Notify;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::{Layer, layer::SubscriberExt};

    struct PauseMemSnapshot {
        entered: Arc<Notify>,
        resume: Arc<Notify>,
        once: AtomicBool,
    }
    impl<S: tracing::Subscriber> Layer<S> for PauseMemSnapshot {
        fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, _: &tracing::span::Id, _: tracing_subscriber::layer::Context<'_, S>) {
            if attrs.metadata().name() == "query_partitioned_with_text_match" && !self.once.swap(true, Ordering::SeqCst) {
                self.entered.notify_one();
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(tokio::time::timeout(std::time::Duration::from_secs(10), self.resume.notified()))
                })
                .expect("writer must finish the controlled flush");
            }
        }
    }

    let project = unique_project();
    let (db, ctx, _) = build_db(&format!("flush-snapshot-{project}"), false).await?;
    db.insert_records_batch(&project, TABLE, vec![make_batch(&project, &[("before", "span", "body")])], false, None).await?;
    db.resolve_table(&project, TABLE).await?;
    assert!(db.delta_scan_can_be_skipped(&project, TABLE));
    let entered = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let subscriber = tracing_subscriber::registry().with(PauseMemSnapshot { entered: entered.clone(), resume: resume.clone(), once: AtomicBool::new(false) });
    let sql = format!("SELECT id FROM {TABLE} WHERE project_id = '{project}'");
    let reader = tokio::spawn(async move { collect_ids(&ctx, &sql).await }.with_subscriber(subscriber));
    tokio::time::timeout(std::time::Duration::from_secs(10), entered.notified()).await?;
    let flushed = db.buffered_layer().unwrap().flush_all_now().await?;
    assert_eq!(flushed.total_rows, 1, "{flushed:?}");
    assert!(!db.delta_scan_can_be_skipped(&project, TABLE));
    db.insert_records_batch(&project, TABLE, vec![make_batch(&project, &[("after", "span", "body")])], false, None).await?;
    resume.notify_one();
    assert_eq!(reader.await??, ["after", "before"]);
    Ok(())
}

/// Where the fixture rows live when the query runs.
#[derive(Clone, Copy)]
enum Land {
    /// Straight to Delta (skip_queue) — bypasses the BufferedWriteLayer, so no
    /// tantivy index is built.
    Delta,
    /// Left in MemBuffer, never flushed — not indexed.
    Mem,
    /// Through the BufferedWriteLayer and force-flushed, so a real index exists.
    Flushed,
}

/// A tantivy-on / tantivy-off pair holding identical rows in one project, so the
/// shared invariant (routed result == full-scan baseline) is asserted in one place.
struct Pair {
    on: Database,
    off: Database,
    ctx: SessionContext,
    ctx_off: SessionContext,
    svc: Arc<TantivyIndexService>,
    cache_root: PathBuf,
    p: String,
}

impl Pair {
    async fn new<S: AsRef<str>>(tag: &str, land: Land, rows: &[(S, S, S)]) -> Result<Self> {
        Self::with(tag, land, rows, |_| {}).await
    }

    /// `tweak` applies to the tantivy-enabled side only; the baseline needs none.
    async fn with<S: AsRef<str>>(tag: &str, land: Land, rows: &[(S, S, S)], tweak: impl FnOnce(&mut AppConfig)) -> Result<Self> {
        let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let on_id = format!("{id}-{tag}-on");
        let (on, ctx, svc) = build_db_with(&on_id, true, tweak).await?;
        let (off, ctx_off, _) = build_db(&format!("{id}-{tag}-off"), false).await?;
        let pair = Self { on, off, ctx, ctx_off, svc: svc.expect("tantivy enabled"), cache_root: data_dir(&on_id), p: unique_project() };
        pair.write(land, rows).await?;
        Ok(pair)
    }

    async fn write<S: AsRef<str>>(&self, land: Land, rows: &[(S, S, S)]) -> Result<()> {
        for db in [&self.on, &self.off] {
            db.insert_records_batch(&self.p, TABLE, vec![make_batch(&self.p, rows)], matches!(land, Land::Delta), None).await?;
            if matches!(land, Land::Flushed) {
                flush_all(db).await?;
            }
        }
        if matches!(land, Land::Mem) {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Ok(())
    }

    fn id_sql(&self, predicate: &str) -> String {
        format!("SELECT id FROM {TABLE} WHERE project_id='{}' AND {predicate}", self.p)
    }

    /// The tantivy-off full-scan result. Must not touch the enabled context:
    /// querying it would populate its five-second manifest cache.
    async fn baseline_ids(&self, predicate: &str) -> Result<Vec<String>> {
        collect_ids(&self.ctx_off, &self.id_sql(predicate)).await
    }

    async fn assert_ids(&self, predicate: &str, want: &[&str], why: &str) -> Result<()> {
        let sql = self.id_sql(predicate);
        let on = collect_ids(&self.ctx, &sql).await?;
        assert_eq!(on, collect_ids(&self.ctx_off, &sql).await?, "{why}: routed result must equal the full-scan baseline [{predicate}]");
        assert_eq!(on, want.iter().map(ToString::to_string).collect::<Vec<_>>(), "{why} [{predicate}]");
        Ok(())
    }

    async fn wait_manifest(&self, want: usize) -> Result<timefusion::tantivy::Manifest> {
        wait_for_manifest_entries(self.svc.object_store.as_ref(), &self.p, want).await
    }
}

/// One matching row plus nine fillers, so the index stays under
/// `prefilter_min_selectivity_pct` and the prefilter engages.
fn flush_group(hit_id: &str, hit_msg: &str, filler_prefix: &str) -> Vec<(String, String, String)> {
    std::iter::once((hit_id.to_string(), "n".to_string(), hit_msg.to_string()))
        .chain((1..10).map(|i| (format!("{filler_prefix}{i}"), "n".to_string(), "ordinary".to_string())))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn tantivy_histogram_daily_budget_preserves_buckets_and_captured_files() -> Result<()> {
    use timefusion::{
        support::test_helpers::json_to_batch_for,
        tantivy::histogram::{HistogramWindow, Membership},
    };
    let (db, ctx, _) = build_db(&uuid::Uuid::new_v4().to_string(), true).await?;
    let project = unique_project();
    let table = "mor_versioned";
    let day = 86_400_000_000_i64;
    let start = (chrono::Utc::now().timestamp_micros().div_euclid(day) - 3) * day;
    let width = 17 * 3_600_000_000_i64;
    let hash = "x".repeat(4096);
    let row = |id: String, timestamp: i64| json!({"project_id": project, "timestamp": timestamp, "date": chrono::DateTime::from_timestamp_micros(timestamp).unwrap().date_naive().to_string(), "id": id, "name": "match", "hashes": [hash]});
    let mut expected = std::collections::BTreeMap::new();
    let mut records = Vec::new();
    for date in 0..3 {
        for n in 0..32 {
            let timestamp = start + date * day + if n < 16 { n } else { day - n };
            *expected.entry(timestamp.div_euclid(width) * width).or_insert(0_u64) += 1;
            records.push(row(format!("{date}-{n}"), timestamp));
        }
    }
    let batch = json_to_batch_for(table, records)?;
    assert!(batch.column_by_name("hashes").unwrap().get_array_memory_size() > 192 * 1024, "the test's combined hash data must exceed its daily budget");
    db.insert_records_batch(&project, table, vec![batch], true, None).await?;
    let window = HistogramWindow::new(start, start + 3 * day, width, 0, 16)?;
    let membership = Membership::Contains { column: "hashes".into(), value: hash.clone() };
    // One date fits the budget but all three together do not: execution must
    // release each day's sources.
    let captured = db.capture_histogram(&project, table, window, Some(&membership), 192 * 1024, ctx.task_ctx()).await?;
    assert_eq!(captured.count().await?.counts, expected);
    assert!(db.indexed_histogram(&project, table, window, Some(&membership), 1024, ctx.task_ctx()).await.is_err());
    db.insert_records_batch(&project, table, vec![json_to_batch_for(table, vec![row("after-capture".into(), start + 1_000_000)])?], true, None).await?;
    assert_eq!(captured.count().await?.counts, expected, "later commits must not change any captured daily file set");
    *expected.entry(start.div_euclid(width) * width).or_default() += 1;
    assert_eq!(db.indexed_histogram(&project, table, window, Some(&membership), 192 * 1024, ctx.task_ctx()).await?.counts, expected);
    Ok(())
}

/// A newer nonmatching indexed version must defeat an older matching version
/// outside index coverage.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn mutable_index_filter_cannot_resurrect_an_uncovered_version() -> Result<()> {
    use timefusion::support::test_helpers::json_to_batch_for;

    let id = uuid::Uuid::new_v4().to_string();
    let (db, ctx, svc) = build_db(&format!("{id}-mutable"), true).await?;
    let project = unique_project();
    let table = "mor_versioned";
    let now = chrono::Utc::now();
    let row = |id: &str, name: &str| json!({"timestamp": now.timestamp_micros(), "date": now.date_naive().to_string(), "id": id, "name": name, "hashes": [name], "project_id": project});
    // Direct writes bypass index construction; only the buffered update below
    // produces an indexed file.
    db.insert_records_batch(&project, table, vec![json_to_batch_for(table, vec![row("changed", "a")])?], true, None).await?;
    let unindexed_sql = format!(
        "SELECT time_bucket('1 second', timestamp), count(*) FROM {table} WHERE project_id='{project}' AND timestamp >= TIMESTAMP '{}' AND timestamp < TIMESTAMP '{}' AND array_has(hashes, 'a') GROUP BY 1",
        now.format("%Y-%m-%d %H:%M:%S%.6f"),
        (now + chrono::Duration::microseconds(1)).format("%Y-%m-%d %H:%M:%S%.6f")
    );
    let before = snapshots(&db);
    assert_eq!(sum_counts(&ctx, &unindexed_sql).await?, 1);
    assert_eq!(snapshots(&db), before, "without usable indexes, narrow SQL must avoid whole-day histogram visibility work");
    let mut newer = vec![row("changed", "b")];
    newer.extend((0..9).map(|i| row(&format!("filler-{i}"), "b")));
    db.insert_records_batch(&project, table, vec![json_to_batch_for(table, newer)?], false, None).await?;
    flush_all(&db).await?;
    let svc = svc.unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let manifest = loop {
        let manifest = timefusion::tantivy::load_manifest(svc.object_store.as_ref(), table, &project).await?;
        if manifest.entries.values().any(|entry| entry.index.is_some()) {
            break manifest;
        }
        anyhow::ensure!(tokio::time::Instant::now() < deadline, "index publication timed out");
        tokio::task::yield_now().await;
    };
    assert!(
        manifest.entries.values().all(|entry| entry.ordinals_valid && entry.covered_files.len() == 1),
        "flush-created indexes must preserve physical Parquet row ordinals without manual backfill"
    );
    assert_eq!(row_count(&ctx, &unindexed_sql).await?, 0, "the newer nonmatching version must suppress the old match");
    // One file here is outside index coverage, so the window takes the ordinary scan: counting
    // it on the histogram path read its rows inside planning (10-02: 7-31 s for a 1h chart).
    assert_eq!(snapshots(&db), before, "a window holding any unindexed file must take the ordinary scan");
    let result = db.tantivy_search().unwrap().search_with_stats(table, &project, &leaf("name", "a"), 100, None).await?.expect("usable newer index");
    assert!(result.hits.is_empty(), "only the uncovered old version matches");
    assert_eq!(result.indexed_rows, 10);
    assert_eq!(db.list_file_uris(&project, table).await?.len(), 2, "both physical versions must survive in separate files");
    assert!(
        collect_ids(&ctx, &format!("SELECT id FROM {table} WHERE project_id='{project}' AND name='a'")).await?.is_empty(),
        "superseded match must not return"
    );
    assert_eq!(collect_ids(&ctx, &format!("SELECT id FROM {table} WHERE project_id='{project}' AND id='changed' AND name='b'")).await?, vec!["changed"]);
    let window = timefusion::tantivy::histogram::HistogramWindow::new(now.timestamp_micros(), now.timestamp_micros() + 1, 1_000_000, 0, 2)?;
    let histogram = db.indexed_histogram(&project, table, window, None, 16 * 1024 * 1024, ctx.task_ctx()).await?;
    assert_eq!(histogram.counts.values().sum::<u64>(), 10, "captured Delta versions must count only their winners");
    let cache_hits = db.tantivy_search().unwrap().stats.histogram_delta_cache_hits.load(std::sync::atomic::Ordering::Relaxed);
    db.insert_records_batch(&project, table, vec![json_to_batch_for(table, vec![row("memory-only", "c")])?], false, None).await?;
    let histogram = db.indexed_histogram(&project, table, window, None, 16 * 1024 * 1024, ctx.task_ctx()).await?;
    assert_eq!(histogram.counts.values().sum::<u64>(), 11, "captured memory must participate alongside Delta");
    assert_eq!(
        db.tantivy_search().unwrap().stats.histogram_delta_cache_hits.load(std::sync::atomic::Ordering::Relaxed),
        cache_hits + 1,
        "memory changes must reuse the unchanged Delta snapshot"
    );
    assert!(db.indexed_histogram(&project, table, window, None, 0, ctx.task_ctx()).await.is_err(), "snapshot capture must enforce its decoded budget");
    // Index the direct write too: SQL takes the histogram path only over a fully indexed window.
    let store = db.resolve_table(&project, table).await?.read().await.log_store().object_store(None);
    let covered = timefusion::tantivy::load_manifest(svc.object_store.as_ref(), table, &project)
        .await?
        .entries
        .into_values()
        .flat_map(|entry| entry.covered_files)
        .collect::<Vec<_>>();
    for uri in db.list_file_uris(&project, table).await? {
        let rel = timefusion::tantivy::search::parquet_rel_of_uri(&uri).expect("relative parquet path");
        if !covered.iter().any(|file| file.ends_with(rel)) {
            svc.build_index_for_file(table, &project, rel, &uri, store.clone()).await?;
        }
    }
    for width in ["'1 second'", "INTERVAL '1 second'"] {
        let sql = format!(
            "SELECT time_bucket({width}, timestamp) AS bucket, count(*) AS n FROM {table} WHERE project_id='{project}' AND timestamp >= TIMESTAMP '{}' AND timestamp < TIMESTAMP '{}' AND array_has(hashes, 'b') GROUP BY 1 ORDER BY 1",
            now.format("%Y-%m-%d %H:%M:%S%.6f"),
            (now + chrono::Duration::microseconds(1)).format("%Y-%m-%d %H:%M:%S%.6f")
        );
        let before = snapshots(&db);
        assert_eq!(sum_counts(&ctx, &sql).await?, 10);
        let logical = ctx.state().create_logical_plan(&sql).await?;
        assert_eq!(snapshots(&db), before + 1, "SQL must reach the histogram service: {}", ctx.state().optimize(&logical)?.display_indent());
        let union = sql.replace("array_has(hashes, 'b')", "(array_has(hashes, 'b') OR array_has(hashes, 'c'))");
        assert_eq!(sum_counts(&ctx, &union).await?, 11);
        assert_eq!(snapshots(&db), before + 2);
        let unsupported = sql.replace("AND array_has", "AND name = 'does-not-match' AND array_has");
        assert_eq!(row_count(&ctx, &unsupported).await?, 0);
        assert_eq!(snapshots(&db), before + 2, "unsupported filters must remain on the ordinary plan");
        for (predicate, expected, routed) in [
            ("hashes @> ARRAY['b']", 10, true),
            ("hashes @> ARRAY['b', 'b']", 10, true),
            ("hashes @> ARRAY['b', 'c']", 0, true),
            ("hashes && ARRAY['b', 'c']", 11, true),
            (r#"jsonb_path_exists(to_jsonb(hashes), '$[*] ? (@ == "b")'::jsonpath)"#, 10, true),
            (r#"jsonb_path_exists(to_json(hashes), '$[*] ? (@ == "c")')"#, 1, true),
            (r#"jsonb_path_exists(to_jsonb(hashes), '$[*] ? (@ != "b")')"#, 1, false),
            ("hashes @> ARRAY[]::text[]", 11, false),
            ("hashes && ARRAY[NULL]::text[]", 0, false),
        ] {
            let query = sql.replace("array_has(hashes, 'b')", predicate);
            let before = snapshots(&db);
            assert_eq!(sum_counts(&ctx, &query).await?, expected, "{predicate}");
            let logical = ctx.state().create_logical_plan(&query).await?;
            assert_eq!(snapshots(&db), before + u64::from(routed), "{predicate}: {}", ctx.state().optimize(&logical)?.display_indent());
        }
    }
    let lo = (now - chrono::Duration::days(30)).timestamp_micros();
    let mid = (now - chrono::Duration::days(15)).timestamp_micros();
    let hi = now.timestamp_micros() + 1;
    let branch = |lo, hi, hash| {
        format!(
            "SELECT timestamp FROM {table} WHERE project_id='{project}' AND timestamp >= TIMESTAMP '{}' AND timestamp < TIMESTAMP '{}' AND array_has(hashes, '{hash}')",
            chrono::DateTime::from_timestamp_micros(lo).unwrap().format("%Y-%m-%d %H:%M:%S%.6f"),
            chrono::DateTime::from_timestamp_micros(hi).unwrap().format("%Y-%m-%d %H:%M:%S%.6f")
        )
    };
    let union = |left, right| format!("SELECT time_bucket('1 hour', timestamp), count(*) FROM ({left} UNION ALL {right}) q GROUP BY 1");
    let left = branch(lo, mid, "b");
    let right = branch(mid, hi, "b");
    for (query, expected, routed) in [
        (format!("{} GROUP BY 1", branch(lo, hi, "b").replacen("SELECT timestamp", "SELECT time_bucket('1 hour', timestamp), count(*)", 1)), 10, true),
        (union(&right, &left), 10, true),
        (union(&right, &right), 20, false),
        (union(&branch(lo, mid - 1, "b"), &right), 10, false),
        (union(&left, &branch(mid, hi, "c")), 1, false),
        (union(&left, &right.replacen("SELECT timestamp", "SELECT updated_at", 1)), 10, false),
    ] {
        let before = snapshots(&db);
        assert_eq!(sum_counts(&ctx, &query).await?, expected, "{query}");
        assert_eq!(snapshots(&db), before + u64::from(routed), "{query}");
    }
    let predicate = timefusion::tantivy::histogram::Membership::Contains { column: "hashes".into(), value: "c".into() };
    let captured = db.capture_histogram(&project, table, window, Some(&predicate), 16 * 1024 * 1024, ctx.task_ctx()).await?;
    let (count, write) = tokio::join!(captured.count(), async {
        db.insert_records_batch(&project, table, vec![json_to_batch_for(table, vec![row("after-capture", "b")])?], false, None).await?;
        flush_all(&db).await?;
        Ok::<_, anyhow::Error>(())
    });
    write?;
    assert_eq!(count?.counts.values().sum::<u64>(), 1, "a concurrent flush must not change the captured population");
    ctx.sql(&format!("DELETE FROM {table} WHERE project_id='{project}' AND id='memory-only'")).await?.collect().await?;
    assert_eq!(captured.count().await?.counts.values().sum::<u64>(), 1, "a later delete must not alter the retained view");
    let fresh = db.indexed_histogram(&project, table, window, Some(&predicate), 16 * 1024 * 1024, ctx.task_ctx()).await?;
    assert_eq!(fresh.counts.values().sum::<u64>(), 0, "a new query must see the committed delete");
    Ok(())
}

/// Poll the tantivy manifest until it has at least `want` entries. The index
/// build is a detached task: `flush_all_now()` only guarantees the Delta commit,
/// so manifest assertions must wait for the sidecar. Errors on a 30s timeout.
async fn wait_for_manifest_entries(store: &dyn object_store::ObjectStore, project: &str, want: usize) -> Result<timefusion::tantivy::Manifest> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let m = timefusion::tantivy::load_manifest(store, TABLE, project).await?;
        if m.entries.len() >= want {
            return Ok(m);
        }
        anyhow::ensure!(std::time::Instant::now() <= deadline, "manifest for {project} stuck at {} entries after 30s, want {want}", m.entries.len());
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

// ───────────────────────── tests ─────────────────────────

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn delta_flushed_text_match_matches_baseline() -> Result<()> {
    let rows = [
        ("a", "auth", "user login successful"),
        ("b", "auth", "user login failed: bad password"),
        ("c", "payment", "charge succeeded"),
        ("d", "payment", "charge failed: declined card"),
    ];
    let pair = Pair::new("direct", Land::Delta, &rows).await?;
    // skip_queue=true bypasses the BufferedWriteLayer, so no index exists and
    // the UDF post-filter must do the work.
    pair.assert_ids("text_match(status_message, 'failed')", &["b", "d"], "unindexed text_match falls back to the UDF post-filter").await
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn membuffer_only_level_eq_falls_back_correctly() -> Result<()> {
    // Rows stay in MemBuffer, so `level = 'ERROR'` runs directly against the
    // in-memory batches and must still equal the tantivy-off baseline.
    let rows = [("x1", "service-a", "operation completed"), ("x2", "service-a", "operation failed"), ("x3", "service-b", "request timeout")];
    let pair = Pair::new("mem", Land::Mem, &rows).await?;
    pair.assert_ids("level = 'ERROR'", &["x2"], "MemBuffer-only result must equal baseline with rewriter on").await
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn tantivy_indexer_actually_writes_manifest_when_flush_routes_through_buffered_layer() -> Result<()> {
    // Write-side wiring: a force-flushed bucket must run the tantivy indexer
    // and publish a manifest entry.
    let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let (db, _ctx, svc) = build_db(&format!("{id}-flush"), true).await?;
    let svc = svc.expect("service should be present when tantivy is enabled");
    let p = unique_project();

    db.insert_records_batch(&p, TABLE, vec![make_batch(&p, &[("f1", "svc", "hello world")])], false, None).await?;
    flush_all(&db).await?;

    let store = svc.object_store.clone();
    let m = wait_for_manifest_entries(store.as_ref(), &p, 1).await?;
    assert!(!m.entries.is_empty(), "manifest should have at least one entry after flush");
    let entry = m.entries.values().next().unwrap();
    assert!(entry.index.is_some(), "entry should have an index blob URI: {entry:?}");
    assert_eq!(entry.rows, 1);
    assert!(entry.ordinals_valid, "server flushes must index physical file order");

    // A single callback may receive several date-partitioned files.
    let now = chrono::Utc::now();
    let batch = timefusion::support::test_helpers::json_to_batch(
        [1, 2]
            .into_iter()
            .map(|days| {
                timefusion::support::test_helpers::test_span_ts(&format!("multi-{days}"), "n", &p, (now - chrono::Duration::days(days)).timestamp_micros())
            })
            .collect(),
    )?;
    let added = db.insert_records_batch(&p, TABLE, vec![batch.clone()], true, None).await?;
    assert_eq!(added.len(), 2, "the fixture must commit two physical files");
    timefusion::server::tantivy_index_callback(&db, Arc::clone(&svc))(p.clone(), TABLE.into(), vec![batch], added.clone()).await?;
    let manifest = timefusion::tantivy::load_manifest(store.as_ref(), TABLE, &p).await?;
    for file in added {
        let entry = manifest.entries.values().find(|entry| entry.covered_files.as_slice() == [file.as_str()]).expect("every file needs its own index");
        assert!(entry.index.is_some() && entry.ordinals_valid);
        assert_eq!(entry.rows, 1);
    }
    Ok(())
}

#[serial]
#[ignore = "writes Delta+MemBuffer in same time bucket; per-bucket Delta exclusion drops the Delta-direct rows. Production never writes both legs simultaneously. See tests/buffer_consistency_test.rs comment for details."]
#[tokio::test(flavor = "multi_thread")]
async fn mixed_membuffer_and_delta_level_eq_returns_union() -> Result<()> {
    // Some rows in Delta, some still in MemBuffer: the result must be the union
    // with no duplicates and no missed rows.
    let pair = Pair::new("mix", Land::Delta, &[("d-old1", "n", "old failed operation"), ("d-old2", "n", "old successful operation")]).await?;
    pair.write(Land::Mem, &[("m-new1", "n", "new failed operation"), ("m-new2", "n", "new clean operation")]).await?;
    pair.assert_ids("level = 'ERROR'", &["d-old1", "m-new1"], "mixed-mode results must be identical between on/off").await
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn compaction_gc_drops_stale_indexes_keeps_live_ones() -> Result<()> {
    // Two flushes → two indexes; gc with only one file live must drop the stale
    // entry and keep the other.
    let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let (db, _ctx, svc) = build_db(&format!("{id}-gc"), true).await?;
    let svc = svc.expect("tantivy enabled");
    let p = unique_project();

    for (gid, msg) in [("g1", "first"), ("g2", "second")] {
        db.insert_records_batch(&p, TABLE, vec![make_batch(&p, &[(gid, "n", msg)])], false, None).await?;
        flush_all(&db).await?;
    }

    let m_before = wait_for_manifest_entries(svc.object_store.as_ref(), &p, 2).await?;
    assert_eq!(m_before.entries.len(), 2, "two flushes → two manifest entries");

    let all_uris: Vec<String> = m_before.entries.values().flat_map(|e| e.covered_files.clone()).collect();
    assert!(!all_uris.is_empty(), "covered_files should be populated");

    // Compaction "kept" only the first URI; the rest are gone.
    let live = vec![all_uris[0].clone()];
    let report = svc.gc_after_compaction(TABLE, &p, &live).await?;
    assert!(report.entries_removed >= 1, "at least one stale entry should be dropped");
    let m_after = timefusion::tantivy::load_manifest(svc.object_store.as_ref(), TABLE, &p).await?;
    assert!(m_after.entries.len() < m_before.entries.len(), "post-gc manifest should shrink");

    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn flushed_index_prefilter_is_actually_used() -> Result<()> {
    // Active prefilter path: a real index covering all rows must still produce
    // the baseline result.
    let rows = [
        ("k1", "auth", "login failed: bad password"),
        ("k2", "auth", "login successful"),
        ("k3", "billing", "charge declined"),
        ("k4", "billing", "charge succeeded"),
        // Literals whose text contains tantivy query-grammar tokens.
        ("k5", "auth", "accept -header now"),
        ("k6", "billing", "err -1234 code"),
        ("k7", "auth", "foo NOT bar baz"),
    ];
    let pair = Pair::new("pf", Land::Flushed, &rows).await?;

    assert!(!pair.wait_manifest(1).await?.entries.is_empty(), "manifest should have entries after flush");

    // A substring LIKE is rewritten into `text_match(...)` so the routing table
    // invokes the prefilter; the original LIKE re-runs on the scan output. The
    // last three patterns pin that query-grammar tokens in a routed literal
    // (whitespace-adjacent `-`, bare `NOT`) are escaped, not parsed as operators.
    for (pat, want) in
        [("login", vec!["k1", "k2"]), ("charge", vec!["k3", "k4"]), ("accept -header", vec!["k5"]), ("err -1234", vec!["k6"]), ("foo NOT bar", vec!["k7"])]
    {
        pair.assert_ids(&format!("status_message LIKE '%{pat}%'"), &want, "post-flush prefilter must match baseline").await?;
    }
    Ok(())
}

/// A cold local index cache must not make SQL wait on blob installs: the scan answers with the
/// original predicate (a user-written `text_match` included — it has a row evaluator, so no
/// predicate is tantivy-only), and the next query after the background warm uses the prefilter.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn cold_index_cache_scans_unfiltered_then_prefilters_once_warm() -> Result<()> {
    let pair = Pair::new("cold", Land::Flushed, &flush_group("c1", "login failed", "d")).await?;
    pair.wait_manifest(1).await?;
    let stats = &pair.on.tantivy_search().expect("search service").stats;
    let load = |c: &std::sync::atomic::AtomicU64| c.load(std::sync::atomic::Ordering::Relaxed);

    for (warms, predicate) in (1..).zip(["level = 'ERROR'", "text_match(status_message, 'failed')"]) {
        std::fs::remove_dir_all(pair.cache_root.join("tantivy_cache"))?;
        let prepares = load(&stats.prepares);
        pair.assert_ids(predicate, &["c1"], "a cold window scans with the original predicate").await?;
        assert_eq!(load(&stats.prepares), prepares, "a cold window must not open an index on the query path [{predicate}]");
        // Re-query while waiting: a query landing before the previous warm left the dedup map
        // spawns nothing, and the next cold one must.
        let warmed = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while load(&stats.blob_fetches) < warms {
                pair.assert_ids(predicate, &["c1"], "a cold window scans with the original predicate").await?;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            anyhow::Ok(())
        });
        warmed.await.expect("background warm must install the blob")?;
        assert!(load(&stats.cold_warms_spawned) >= warms, "a cold window must queue a background warm [{predicate}]");
    }
    let prepares = load(&stats.prepares);
    pair.assert_ids("level = 'ERROR'", &["c1"], "the warm window must still match the baseline").await?;
    assert!(load(&stats.prepares) > prepares, "once warm, the prefilter must search the index");
    Ok(())
}

/// A point lookup over a narrow window skips the index: the timestamp stats already prune it
/// to a few row groups, while a term query on a high-cardinality field loads that field's
/// whole term dictionary (prod: 49-119 MB, 3.9-7.4 s for a ±5 min trace-id lookup, against
/// 0.1-0.2 s raw). A wide window still searches.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_narrow_window_skips_the_prefilter_and_a_wide_one_uses_it() -> Result<()> {
    let pair = Pair::new("narrow", Land::Flushed, &flush_group("n1", "login failed", "n")).await?;
    pair.wait_manifest(1).await?;
    let stats = &pair.on.tantivy_search().expect("search service").stats;
    let searched = || stats.indexes_searched.load(std::sync::atomic::Ordering::Relaxed);
    let now = chrono::Utc::now();
    let window = |mins: i64| {
        let (lo, hi) = (now - chrono::Duration::minutes(mins), now + chrono::Duration::minutes(mins));
        format!("level = 'ERROR' AND timestamp BETWEEN '{}' AND '{}'", lo.to_rfc3339(), hi.to_rfc3339())
    };
    let before = searched();
    pair.assert_ids(&window(5), &["n1"], "a narrow window answers from the scan").await?;
    assert_eq!(searched(), before, "a ±5 min window must not search the index");
    pair.assert_ids(&window(7 * 24 * 60), &["n1"], "a wide window still prefilters").await?;
    assert!(searched() > before, "a 7-day window must search the index");
    Ok(())
}

/// Exact `=`, OR-disjunctions and `IN` lists routed through the id-prefilter
/// must all equal the full-scan baseline. In particular an OR whose other side
/// is unroutable must fall back, never intersect down to ∅.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn flushed_eq_or_and_in_list_prefilters_match_baseline() -> Result<()> {
    let rows = [
        ("k1", "auth", "login failed: bad password"), // ERROR
        ("k2", "auth", "login successful"),           // INFO
        ("k3", "billing", "charge declined"),         // ERROR
        ("k4", "billing", "charge succeeded"),        // INFO
    ];
    let pair = Pair::new("eq", Land::Flushed, &rows).await?;
    pair.wait_manifest(1).await?;

    for (predicate, want, why) in [
        ("level = 'ERROR'", vec!["k1", "k3"], "`level='ERROR'` prefilter must match the full-scan baseline"),
        ("(level = 'ERROR' OR name = 'billing')", vec!["k1", "k3", "k4"], "2026-06-16: OR of two `=`s must not empty-intersect"),
        ("(level = 'ERROR' OR id = 'k4')", vec!["k1", "k3", "k4"], "routable OR must union, not intersect"),
        ("id IN ('k2','k3')", vec!["k2", "k3"], "IN-list routing must match baseline"),
        ("id NOT IN ('k2','k3')", vec!["k1", "k4"], "NOT IN must never be routed (no term form) — baseline correctness only"),
    ] {
        pair.assert_ids(predicate, &want, why).await?;
    }
    Ok(())
}

/// Integer and path-valued `=` on raw-indexed columns route through the index, and
/// equal the full-scan baseline. MemBuffer rows hold the same columns unindexed: the
/// routed `text_match` must row-evaluate an Int32 column, not drop every row.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn status_and_route_equality_prefilter_and_match_baseline() -> Result<()> {
    let pair = Pair::new("http", Land::Flushed, &flush_group("h1", "checkout failed", "h")).await?;
    pair.wait_manifest(1).await?;
    let queries = || pair.on.tantivy_search().expect("search service").stats.queries.load(std::sync::atomic::Ordering::Relaxed);
    for predicate in
        ["attributes___http___response___status_code = 500", "attributes___http___route = '/v1/n' AND attributes___http___response___status_code = 500"]
    {
        let before = queries();
        pair.assert_ids(predicate, &["h1"], "flushed rows").await?;
        assert!(queries() > before, "the predicate must search the index [{predicate}]");
    }
    pair.write(Land::Mem, &[("m1", "pay", "card failed")]).await?;
    pair.assert_ids("attributes___http___response___status_code = 500", &["h1", "m1"], "unindexed MemBuffer rows").await?;
    Ok(())
}

/// Exact `id = '<uuid>'` must still match: a `-` is a NOT operator to tantivy's
/// QueryParser, so an unescaped dashed UUID yields empty hits → `id IN ()` →
/// the real row silently dropped.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn flushed_eq_on_uuid_id_with_dashes_matches_baseline() -> Result<()> {
    let uid = "0fee13b9-ac71-5c55-acd1-109542595054";
    let other = "11111111-2222-3333-4444-555555555555";
    let pair = Pair::new("uuideq", Land::Flushed, &[(uid, "auth", "login ok"), (other, "auth", "other")]).await?;
    pair.wait_manifest(1).await?;

    pair.assert_ids(&format!("id = '{uid}'"), &[uid], "exact `id=` on a dashed UUID must match baseline (QueryParser must not eat the `-`)").await?;

    // Query the search service directly: the SQL assert above can be rescued by
    // the full-scan fallback, so only this proves the prefilter itself fires.
    let search = Arc::new(TantivySearchService::new(pair.svc.object_store.clone(), pair.cache_root.clone(), Arc::new(TantivyConfig::default())));
    let hits: Vec<String> =
        search.search_with_stats(TABLE, &pair.p, &leaf("id", uid), 1000, None).await?.map(|r| r.hits.into_iter().map(|h| h.id).collect()).unwrap_or_default();
    assert!(hits.contains(&uid.to_string()), "tantivy exact search on `id` must return the dashed UUID, not fall back; got {hits:?}");
    assert!(!hits.contains(&other.to_string()), "must not over-match other ids; got {hits:?}");
    Ok(())
}

/// When a live Delta file is not covered by a successful index, the
/// `id IN (hits)` prefilter may narrow only the covered file; the uncovered file
/// must stay a separate raw leg or its matching rows vanish.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn uncovered_live_file_uses_hybrid_prefilter_without_dropping_rows() -> Result<()> {
    use timefusion::tantivy::{load_manifest, save_manifest};

    let pair = Pair::new("cov", Land::Flushed, &flush_group("c1", "login alpha", "d1")).await?;
    pair.write(Land::Flushed, &flush_group("c2", "login beta", "d2")).await?;
    let store = pair.svc.object_store.clone();
    assert_eq!(pair.wait_manifest(2).await?.entries.len(), 2, "two flushes → two entries");

    // Do not query the enabled context yet: it would cache the manifest before
    // coverage is mutated below, making this a false-positive test.
    let predicate = "(id = 'c1' OR status_message LIKE '%login%')";
    assert_eq!(pair.baseline_ids(predicate).await?, vec!["c1".to_string(), "c2".to_string()]);

    // Neuter one entry: its parquet stays live in Delta but is now uncovered.
    let mut m2 = load_manifest(store.as_ref(), TABLE, &pair.p).await?;
    let first_key = m2.entries.keys().next().cloned().unwrap();
    let e = m2.entries.get_mut(&first_key).unwrap();
    e.index = None;
    e.error = Some("simulated uncovered file".into());
    save_manifest(store.as_ref(), TABLE, &pair.p, &m2).await?;

    // Covered file uses tantivy, uncovered file scans raw; applying the covered
    // file's id set globally would drop the other file's row.
    let direct = pair
        .on
        .tantivy_search()
        .expect("search service")
        .search_with_stats(TABLE, &pair.p, &leaf("status_message", "login"), 1000, None)
        .await?
        .expect("one usable covered index");
    assert_eq!(direct.covered_files.len(), 1, "the fixture must have exactly one covered and one uncovered file");
    assert_eq!(direct.hits.len(), 1, "the covered index must be selective enough to engage the prefilter");
    assert_eq!(direct.indexed_rows, 10);
    let explain = pair.ctx.sql(&format!("EXPLAIN {}", pair.id_sql(predicate))).await?.collect().await?;
    let rendered = datafusion::arrow::util::pretty::pretty_format_batches(&explain)?.to_string();
    assert!(rendered.contains("UnionExec"), "partial coverage must produce indexed+raw Delta legs, not a global full-scan fallback:\n{rendered}");
    pair.assert_ids(predicate, &["c1", "c2"], "uncovered live file must not drop rows; both matching rows survive the coverage gate").await
}

#[tokio::test(flavor = "multi_thread")]
async fn startup_backfills_existing_hashes_without_an_opt_in() -> Result<()> {
    use timefusion::support::test_helpers::{json_to_batch_for, minio_test_config};

    let dir = tempfile::tempdir()?;
    let project = unique_project();
    let mut config = (*minio_test_config(&project, dir.path().to_str().unwrap())).clone();
    config.tantivy = serde_json::from_str("{}")?;
    let config = Arc::new(config);
    let db = Database::with_config(config.clone()).await?;
    let uri = format!("s3://timefusion-tests/{}/tantivy", config.core.timefusion_table_prefix);
    let store = db.create_object_store(&uri, &config.aws.build_storage_options(None)).await?;
    let indexer = Arc::new(TantivyIndexService::new(
        store.clone(),
        Arc::new(config.tantivy.clone()),
        std::env::temp_dir().join(format!("tf-scratch-{}", uuid::Uuid::new_v4())),
    ));
    let search = Arc::new(TantivySearchService::new(store.clone(), dir.path().join("indexes"), Arc::new(config.tantivy.clone())));
    indexer.with_reader(&search);
    let db = Arc::new(db.with_tantivy_indexer(indexer).with_tantivy_search(search.clone()));
    let table = "mor_versioned";
    let timestamp = chrono::Utc::now() - chrono::Duration::days(2);
    let row = json!({"project_id": project, "id": "existing", "timestamp": timestamp.timestamp_micros(), "date": timestamp.date_naive().to_string(), "hashes": ["needle", "needle"]});
    db.insert_records_batch(&project, table, vec![json_to_batch_for(table, vec![row])?], true, None).await?;
    assert!(timefusion::tantivy::load_manifest(store.as_ref(), table, &project).await?.entries.is_empty());
    db.spawn_tantivy_backfill();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let manifest = timefusion::tantivy::load_manifest(store.as_ref(), table, &project).await?;
        if manifest.entries.values().any(|entry| entry.covers_current_elements(timefusion::schema::get_schema(table).unwrap())) {
            break;
        }
        anyhow::ensure!(tokio::time::Instant::now() < deadline, "default startup must publish a physical hash index without an opt-in");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let mut ctx = db.clone().create_session_context();
    db.setup_session_context(&mut ctx)?;
    let sql = format!(
        "SELECT time_bucket('1 hour', timestamp), count(*) FROM {table} WHERE project_id='{project}' AND timestamp >= TIMESTAMP '{}' AND timestamp < TIMESTAMP '{}' AND hashes @> ARRAY['needle'] GROUP BY 1",
        timestamp.format("%Y-%m-%d %H:%M:%S%.6f"),
        (timestamp + chrono::Duration::microseconds(1)).format("%Y-%m-%d %H:%M:%S%.6f")
    );
    assert_eq!(sum_counts(&ctx, &sql).await?, 1);
    assert_eq!(search.stats.histogram_snapshots.load(std::sync::atomic::Ordering::Relaxed), 1, "backfilled hashes must reach native SQL counting");
    db.shutdown().await?;
    Ok(())
}

/// `metric` summed over the Delta leg's parquet reads, from `EXPLAIN ANALYZE`.
async fn scan_metric(ctx: &SessionContext, sql: &str, metric: &str) -> Result<usize> {
    let txt = ctx
        .sql(&format!("EXPLAIN ANALYZE {sql}"))
        .await?
        .collect()
        .await?
        .iter()
        .map(|b| arrow::util::pretty::pretty_format_batches(std::slice::from_ref(b)).unwrap().to_string())
        .collect::<String>();
    let key = format!("{metric}=");
    Ok(txt
        .lines()
        .filter(|l| l.contains("DataSourceExec: file_groups"))
        .filter_map(|l| {
            let v = l.split(&key).nth(1)?.split([',', ']']).next()?.trim();
            let (n, scale) = v.strip_suffix(" K").map_or((v, 1.0), |n| (n, 1e3));
            Some(n.parse::<f64>().ok()? * scale)
        })
        .sum::<f64>()
        .round() as usize)
}

/// Min-of-5 physical planning time.
async fn plan_time(ctx: &SessionContext, sql: &str) -> Result<std::time::Duration> {
    let mut best = std::time::Duration::MAX;
    for _ in 0..5 {
        let t = std::time::Instant::now();
        ctx.sql(sql).await?.create_physical_plan().await?;
        best = best.min(t.elapsed());
    }
    Ok(best)
}

/// A 1,000-hit prefilter must cost decoded rows, not planning: delta-kernel used to replay the
/// `id IN (...)` list as an N-way OR against every file's stats, planning a prod
/// `status_code = 'ERROR'` dashboard query in 9s. The list must still reach parquet: one
/// deletion vector drops row ordinals for the WHOLE scan, and the list's `id` bloom filter is
/// then the only thing skipping the row groups a `LIKE` cannot prune. Dropping it when ordinals
/// existed made prod's row filter evaluate ~6x the rows and saturated heavy-query admission.
#[test_case(true, false ; "row ordinals")]
#[test_case(false, false ; "id list only")]
#[test_case(true, true ; "a deletion vector disables the ordinals")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn tantivy_id_list_costs_rows_not_planning(row_selection: bool, with_dv: bool) -> Result<()> {
    const FILES: usize = 50;
    const PER: usize = 400;
    const HITS: usize = 20 * FILES;
    // Hits are one contiguous block per file, so every other row group can be skipped.
    let group = |f: usize| {
        (0..PER)
            .map(move |i| (format!("f{f}-r{i}"), "n".to_string(), if i < 20 { "op failed".to_string() } else { "ordinary".to_string() }))
            .collect::<Vec<_>>()
    };
    let pair = Pair::with("idlist", Land::Flushed, &group(0), |c| {
        c.tantivy.timefusion_tantivy_row_selection = row_selection;
        c.parquet.timefusion_max_row_group_size = 64 * 1024;
    })
    .await?;
    for f in 1..FILES {
        pair.write(Land::Flushed, &group(f)).await?;
    }
    pair.wait_manifest(FILES).await?;
    if with_dv {
        let table = pair.on.resolve_table(&pair.p, TABLE).await?;
        let dt = table.read().await.clone();
        let (dt, m) = dt.delete().with_predicate(format!("id = 'f0-r399' and project_id = '{}'", pair.p)).with_deletion_vectors(true).await?;
        assert_eq!(m.num_deleted_rows, Some(1), "the deletion vector must land");
        *table.write().await = dt;
    }
    let warm = pair.on.tantivy_search().unwrap().search_detailed(TABLE, &pair.p, &leaf("status_message", "failed"), 2_000, None, true).await?;
    assert!(warm.is_ok_and(|r| r.hits.len() == HITS), "the prefilter must answer from a warm index");

    let sql = pair.id_sql("status_message LIKE '%failed%'");
    assert_eq!(collect_ids(&pair.ctx, &sql).await?, pair.baseline_ids("status_message LIKE '%failed%'").await?, "routed result must equal the baseline");
    // Rows the parquet row filter evaluated: ~3.3K with the list, ~19.6K without it.
    let evaluated = scan_metric(&pair.ctx, &sql, "pushdown_rows_matched").await? + scan_metric(&pair.ctx, &sql, "pushdown_rows_pruned").await?;
    assert!(evaluated <= 4 * HITS, "the id list must keep pruning row groups: the row filter evaluated {evaluated} rows for {HITS} hits");
    let routed = plan_time(&pair.ctx, &sql).await?;
    let bypass = plan_time(&pair.ctx, &pair.id_sql("status_message || '' LIKE '%failed%'")).await?;
    // Debug build, 50 files: ~20x the unrouted plan before the fix, ~5-7x after (the rest is the
    // tantivy search itself). A timing bound, so it is a cost guard with margin, not a proof.
    assert!(routed < bypass * 12, "a 1,000-id prefilter must not dominate planning: routed {routed:?} vs unrouted {bypass:?}");
    Ok(())
}

/// `(timestamp, id, level)` rows, each in its own day's partition.
fn timed_batch(project: &str, rows: &[(i64, String, &str)]) -> RecordBatch {
    let records = rows
        .iter()
        .map(|(ts, id, level)| {
            json!({
                "timestamp": ts, "id": id, "name": "n", "level": level, "status_message": "m",
                "project_id": project, "hashes": [], "summary": vec![format!("summary for {id}")],
                "date": chrono::DateTime::from_timestamp_micros(*ts).expect("valid").date_naive().to_string(),
            })
        })
        .collect();
    json_to_batch(records).expect("json_to_batch")
}

/// Ids in result order.
async fn ordered_ids(ctx: &SessionContext, sql: &str) -> Result<Vec<String>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    Ok(batches
        .iter()
        .flat_map(|b| {
            let ids = arrow::compute::cast(b.column(0), &arrow::datatypes::DataType::Utf8).expect("utf8");
            (0..ids.len()).map(move |i| ids.as_string::<i32>().value(i).to_string()).collect::<Vec<_>>()
        })
        .collect())
}

/// Ids of `sql` and the `fell_back` count of its `NarrowedFetchExec`, which must be planned.
async fn narrowed_page(ctx: &SessionContext, sql: &str) -> Result<(Vec<String>, usize)> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let shown = datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
    assert!(shown.contains("NarrowedFetchExec"), "the page must be narrowed to the index bound:\n{shown}");
    let batches = datafusion::physical_plan::collect(std::sync::Arc::clone(&plan), ctx.task_ctx()).await?;
    let ids = batches
        .iter()
        .flat_map(|b| {
            let ids = arrow::compute::cast(b.column(0), &arrow::datatypes::DataType::Utf8).expect("utf8");
            (0..ids.len()).map(move |i| ids.as_string::<i32>().value(i).to_string()).collect::<Vec<_>>()
        })
        .collect();
    let mut fell_back = 0;
    datafusion::physical_plan::accept(plan.as_ref(), &mut FellBack(&mut fell_back))?;
    Ok((ids, fell_back))
}

struct FellBack<'a>(&'a mut usize);

impl datafusion::physical_plan::ExecutionPlanVisitor for FellBack<'_> {
    type Error = datafusion::error::DataFusionError;
    fn pre_visit(&mut self, plan: &dyn datafusion::physical_plan::ExecutionPlan) -> Result<bool, Self::Error> {
        if plan.name() == "NarrowedFetchExec" {
            *self.0 += plan.metrics().and_then(|m| m.sum_by_name("fell_back")).map_or(0, |v| v.as_usize());
        }
        Ok(true)
    }
}

/// A newest-first page under an indexed predicate over a wide window is planned again from
/// the index's newest matches (`ts >= bound`), fills without falling back, and must equal the
/// full scan, deletions included.
#[tokio::test(flavor = "multi_thread")]
async fn a_newest_first_page_narrows_to_the_index_bound_and_matches_baseline() -> Result<()> {
    let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let (on, ctx, svc) = build_db(&format!("{id}-desc-on"), true).await?;
    let (off, ctx_off, _) = build_db(&format!("{id}-desc-off"), false).await?;
    let p = unique_project();
    let (now, step) = (chrono::Utc::now().timestamp_micros(), 80 * 60_000_000i64);
    // 120 errors, one every 80 minutes over ~6.7 days, each beside an INFO filler.
    let rows: Vec<(i64, String, &str)> =
        (0..120i64).flat_map(|i| [(now - i * step, format!("e{i}"), "ERROR"), (now - i * step - 1, format!("i{i}"), "INFO")]).collect();
    for db in [&on, &off] {
        db.insert_records_batch(&p, TABLE, vec![timed_batch(&p, &rows)], false, None).await?;
        flush_all(db).await?;
    }
    // Every flushed row must be indexed before the page asks the index for its bound.
    let store = svc.expect("tantivy").object_store.clone();
    for _ in 0..150 {
        if wait_for_manifest_entries(store.as_ref(), &p, 1).await?.entries.values().map(|e| e.rows).sum::<u64>() >= rows.len() as u64 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let page = format!(
        "SELECT id FROM {TABLE} WHERE project_id='{p}' AND timestamp BETWEEN to_timestamp_micros({}) AND to_timestamp_micros({now}) AND level = 'ERROR' ORDER BY timestamp DESC LIMIT 2",
        now - 7 * 24 * 3_600_000_000
    );
    assert_eq!(narrowed_page(&ctx, &page).await?, (vec!["e0".to_string(), "e1".to_string()], 0), "narrowed, and filled without falling back");
    assert_eq!(ordered_ids(&ctx, &page).await?, ordered_ids(&ctx_off, &page).await?);

    // Deleting the eight newest errors must not surface them, whichever way the page is filled
    // (the short-page fallback itself is `NarrowedFetchExec`'s, shared with the ascending case).
    let gone = (0..8).map(|i| format!("'e{i}'")).collect::<Vec<_>>().join(",");
    for ctx in [&ctx, &ctx_off] {
        ctx.sql(&format!("DELETE FROM {TABLE} WHERE project_id='{p}' AND id IN ({gone})")).await?.collect().await?;
    }
    assert_eq!(ordered_ids(&ctx, &page).await?, ["e8", "e9"]);
    assert_eq!(ordered_ids(&ctx, &page).await?, ordered_ids(&ctx_off, &page).await?);
    Ok(())
}

/// Rows reaching every `DedupExec` in `sql`'s physical plan, with its ids.
async fn ids_and_dedup_input(ctx: &SessionContext, sql: &str) -> Result<(Vec<String>, usize)> {
    struct DedupInput(usize);
    impl datafusion::physical_plan::ExecutionPlanVisitor for DedupInput {
        type Error = datafusion::error::DataFusionError;
        fn pre_visit(&mut self, plan: &dyn datafusion::physical_plan::ExecutionPlan) -> Result<bool, Self::Error> {
            if plan.name() == "DedupExec" {
                self.0 += plan.children().iter().filter_map(|c| c.metrics()?.output_rows()).sum::<usize>();
            }
            Ok(true)
        }
    }
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    let mut ids: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let ids = arrow::compute::cast(b.column(0), &arrow::datatypes::DataType::Utf8).expect("utf8");
            (0..ids.len()).map(move |i| ids.as_string::<i32>().value(i).to_string()).collect::<Vec<_>>()
        })
        .collect();
    ids.sort();
    let mut input = DedupInput(0);
    datafusion::physical_plan::accept(plan.as_ref(), &mut input)?;
    Ok((ids, input.0))
}

#[derive(Clone, Copy, Debug)]
enum PageContract {
    IssueSample,
    LogExplorer,
    RouteLogExplorer,
}

/// Decode the actual wire result without truncating at the client. The prepared
/// browser contract binds its page size and records time through its first row.
async fn wire_page(client: &tokio_postgres::Client, sql: &str, contract: PageContract) -> Result<(Vec<serde_json::Value>, Option<std::time::Duration>)> {
    match contract {
        PageContract::IssueSample => Ok((
            client
                .simple_query(sql)
                .await?
                .into_iter()
                .filter_map(|message| match message {
                    tokio_postgres::SimpleQueryMessage::Row(row) => Some(json!([row.get(0), row.get(1), row.get(2)])),
                    _ => None,
                })
                .collect(),
            None,
        )),
        PageContract::LogExplorer | PageContract::RouteLogExplorer => {
            use futures::{StreamExt, TryStreamExt};
            let started = std::time::Instant::now();
            let stream = client.query_raw(sql, [&501i64 as &(dyn tokio_postgres::types::ToSql + Sync)]).await?.fuse();
            futures::pin_mut!(stream);
            let first = stream.try_next().await?;
            let first_row = first.as_ref().map(|_| started.elapsed());
            let mut rows: Vec<serde_json::Value> = futures::stream::iter(first.into_iter().map(Ok))
                .chain(stream)
                .and_then(|row| async move { row.try_get::<_, serde_json::Value>(0) })
                .try_collect()
                .await?;
            assert!(rows.windows(2).all(|pair| pair[0][1].as_str() >= pair[1][1].as_str()), "browser rows must retain descending timestamps");
            // The caller orders only by timestamp; compare tied rows as a set
            // without discarding the order between distinct timestamps.
            for tied in rows.chunk_by_mut(|a, b| a[1] == b[1]) {
                tied.sort_by_cached_key(|row| row[0].as_str().expect("browser id").to_owned());
            }
            Ok((rows, first_row))
        }
    }
}

/// A predicate on a mutable column narrows every leg to the timestamps where some version
/// matches (indexed hits, unindexed files, MemBuffer), so the newest version of each candidate
/// still decides: no superseded match resurfaces and no newly matching version is lost.
#[test_case(false, false, PageContract::IssueSample; "partial coverage with seeded disk")]
#[test_case(true, false, PageContract::IssueSample; "complete coverage with seeded disk")]
#[test_case(false, true, PageContract::IssueSample; "partial coverage with cold disk")]
#[test_case(true, true, PageContract::IssueSample; "complete coverage with cold disk")]
#[test_case(false, false, PageContract::LogExplorer; "browser partial coverage with seeded disk")]
#[test_case(true, false, PageContract::LogExplorer; "browser complete coverage with seeded disk")]
#[test_case(false, true, PageContract::LogExplorer; "browser partial coverage with cold disk")]
#[test_case(true, true, PageContract::LogExplorer; "browser complete coverage with cold disk")]
#[test_case(false, false, PageContract::RouteLogExplorer; "route partial coverage with seeded disk")]
#[test_case(true, false, PageContract::RouteLogExplorer; "route complete coverage with seeded disk")]
#[test_case(false, true, PageContract::RouteLogExplorer; "route partial coverage with cold disk")]
#[test_case(true, true, PageContract::RouteLogExplorer; "route complete coverage with cold disk")]
#[tokio::test(flavor = "multi_thread")]
async fn a_mutable_predicate_narrows_to_candidate_timestamps_without_ghosts(complete: bool, cold_disk: bool, contract: PageContract) -> Result<()> {
    mutable_candidate_fixture(complete, cold_disk, contract, None).await
}

#[test_case(false, true, 23; "partial cold at proportional volume")]
#[test_case(true, true, 23; "complete cold at proportional volume")]
#[test_case(false, true, 1; "partial cold at calibrated volume")]
#[test_case(true, true, 1; "complete cold at calibrated volume")]
#[ignore = "streams a calibrated production-volume browser workload to local MinIO"]
#[tokio::test(flavor = "multi_thread")]
async fn log_explorer_at_volume(complete: bool, cold_disk: bool, divisor: usize) -> Result<()> {
    mutable_candidate_fixture(complete, cold_disk, PageContract::LogExplorer, Some(divisor)).await
}

/// Background rows never match any registered predicate. Their summaries have
/// the observed decoded width; exact pages still come from the mutable fixture.
async fn append_browser_background(db: &Database, project: &str, now: i64, divisor: usize) -> Result<()> {
    use arrow::array::{ArrayRef, Int64Array, ListBuilder, StringArray, StringBuilder, TimestampMicrosecondArray, UInt32Array, new_null_array};
    use deltalake::{
        datafusion::parquet::{
            basic::{Compression, Encoding, ZstdLevel},
            file::properties::{EnabledStatistics, WriterProperties},
            schema::types::ColumnPath,
        },
        writer::{DeltaWriter, RecordBatchWriter},
    };
    const TABLE: &str = "otel_logs_and_spans";
    // (days before query end, physical rows, row groups), from the retained
    // 37-file metadata calibration. The exact failed historical snapshot is unavailable.
    let layout: Vec<(i64, usize, usize)> = serde_json::from_str(include_str!("../fixtures/log_explorer_volume.json"))?;
    let schema = timefusion::schema::get_schema(TABLE).expect("schema");
    let table = db.resolve_table(project, TABLE).await?;
    let day_micros = 86_400_000_000;
    let today = now.div_euclid(day_micros) * day_micros;
    let padding = "x".repeat(479);
    let started = std::time::Instant::now();
    let mut total_rows = 0;
    for (file, &(age, physical_rows, groups)) in layout.iter().enumerate() {
        let rows = physical_rows.div_ceil(divisor);
        let lower = (today - age * day_micros).max(now - 14 * day_micros);
        let upper = (today - (age - 1) * day_micros).min(now);
        let step = (upper - lower) / i64::try_from(rows + 1)?;
        anyhow::ensure!(step > 0, "the selected date span must fit strictly ordered synthetic timestamps");
        let date = chrono::DateTime::from_timestamp_micros(lower).unwrap().date_naive().to_string();
        let template = timefusion::support::test_helpers::json_to_batch_for(
            TABLE,
            vec![json!({
                "project_id": project, "date": date, "timestamp": lower, "id": "template", "name": "background",
                "hashes": ["volume-nonmatching"], "summary": [], "resource___service___name": "browser-background",
                "duration": 1200, "start_time": lower, "kind": "span", "level": "ERROR", "severity___severity_number": 17
            })],
        )?;
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_max_row_group_row_count(Some(rows.div_ceil(groups)))
            .set_dictionary_page_size_limit(8_388_608)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_column_statistics_enabled(ColumnPath::from("timestamp"), EnabledStatistics::Page)
            .set_data_page_row_count_limit(db.config().parquet.timefusion_page_row_count_limit)
            .set_sorting_columns(Some(schema.sorting_columns()));
        // Match the production writer's encoding choices without exposing its
        // private factory just for this integration fixture.
        let properties = schema
            .fields
            .iter()
            .fold(properties, |builder, field| {
                let column = ColumnPath::from(field.name.as_str());
                let time_like = field.data_type.starts_with("Timestamp") || field.data_type == "Date32";
                let sort_key = schema.sorting_columns.iter().any(|sort| sort.name == field.name);
                let builder = if time_like || sort_key { builder.set_column_statistics_enabled(column.clone(), EnabledStatistics::Page) } else { builder };
                let builder = match field.data_type.as_str() {
                    _ if time_like => {
                        builder.set_column_encoding(column.clone(), Encoding::DELTA_BINARY_PACKED).set_column_dictionary_enabled(column.clone(), false)
                    }
                    "Int32" | "Int64" | "UInt32" | "UInt64" => builder.set_column_encoding(column.clone(), Encoding::DELTA_BINARY_PACKED),
                    "Utf8" if sort_key => {
                        builder.set_column_encoding(column.clone(), Encoding::DELTA_BYTE_ARRAY).set_column_dictionary_enabled(column.clone(), false)
                    }
                    _ => builder,
                };
                if field.dictionary == Some(false) { builder.set_column_dictionary_enabled(column, false) } else { builder }
            })
            .build();
        let mut delta = table.write().await;
        let mut writer = RecordBatchWriter::for_table(&delta)?.with_writer_properties(properties);
        for high in (0..rows).rev().step_by(8192) {
            let low = (high + 1).saturating_sub(8192);
            let keys: Vec<_> = (low..=high).rev().collect();
            let hash = |seed: u64, row: usize| twox_hash::XxHash64::oneshot(seed + 2 * file as u64, &row.to_le_bytes());
            let timestamps: ArrayRef = Arc::new(
                TimestampMicrosecondArray::from_iter_values(keys.iter().map(|&row| lower + (row + 1) as i64 * step + (hash(0, row) % step as u64) as i64))
                    .with_timezone("UTC"),
            );
            let ids = StringArray::from_iter_values(keys.iter().map(|&row| uuid::Uuid::from_u64_pair(hash(0, row), hash(1, row)).to_string()));
            let mut summaries = ListBuilder::new(StringBuilder::new());
            let mut hashes = ListBuilder::new(StringBuilder::new());
            for (row, &key) in keys.iter().enumerate() {
                summaries.values().append_value(format!("{} {padding}", ids.value(row)));
                summaries.append(true);
                hashes.values().append_value(format!("noise-{:03x}", hash(6, key / 2) % 4096));
                hashes.values().append_value("volume-nonmatching");
                hashes.append(true);
            }
            let sources: std::collections::HashMap<&str, ArrayRef> = [
                ("timestamp", Arc::clone(&timestamps)),
                ("start_time", timestamps),
                ("id", Arc::new(ids) as ArrayRef),
                ("summary", Arc::new(summaries.finish()) as ArrayRef),
                ("hashes", Arc::new(hashes.finish()) as ArrayRef),
                (
                    "context___span_id",
                    Arc::new(StringArray::from_iter(keys.iter().map(|&row| (row % 5 != 0).then(|| format!("{:016x}", hash(2, row)))))) as ArrayRef,
                ),
                ("parent_id", Arc::new(StringArray::from_iter(keys.iter().map(|&row| (row % 9 < 5).then(|| format!("{:016x}", hash(3, row)))))) as ArrayRef),
                (
                    "context___trace_id",
                    Arc::new(StringArray::from_iter_values(
                        keys.iter().map(|&row| uuid::Uuid::from_u64_pair(hash(4, row / 8), hash(7, row / 8)).simple().to_string()),
                    )) as ArrayRef,
                ),
                ("duration", Arc::new(Int64Array::from_iter_values(keys.iter().map(|&row| (hash(5, row) % 3_600_000_000) as i64))) as ArrayRef),
            ]
            .into();
            let indices = UInt32Array::from(vec![0; keys.len()]);
            let mut nulls = std::collections::HashMap::new();
            let columns = template
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| match sources.get(field.name().as_str()) {
                    Some(array) => arrow::compute::cast(array, field.data_type()),
                    None if template.column(index).is_null(0) => {
                        Ok(Arc::clone(nulls.entry(field.data_type().clone()).or_insert_with(|| new_null_array(field.data_type(), keys.len()))))
                    }
                    None => arrow::compute::take(template.column(index).as_ref(), &indices, None),
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let batch = RecordBatch::try_new(template.schema(), columns)?;
            writer.write(deltalake::kernel::schema::cast_record_batch(&batch, writer.arrow_schema(), true, true)?).await?;
        }
        writer.flush_and_commit(&mut delta).await?;
        total_rows += rows;
        eprintln!("browser_volume_prepare file={} rows={rows} total_rows={total_rows} elapsed_ms={}", file + 1, started.elapsed().as_millis());
    }
    eprintln!(
        "browser_volume_input background_rows={total_rows} background_files={} divisor={divisor} summary_bytes=516 table_uri={}",
        layout.len(),
        table.read().await.table_url()
    );
    Ok(())
}

/// A retained calibrated fixture must survive canonical WAL recovery before it
/// can supply a matched performance control. Preparation copies the archived
/// local state and supplies read-only credentials for the local object store.
#[derive(serde::Deserialize)]
struct BrowserReplay {
    application_now_micros: i64,
    prepared_end_micros: i64,
    project: String,
    stores: Vec<BrowserReplayStore>,
}

#[derive(serde::Deserialize)]
struct BrowserReplayStore {
    name: String,
    uri: String,
    delta_version: u64,
    active_project_files: usize,
    local_state: PathBuf,
}

#[derive(serde::Deserialize)]
struct BrowserReplayCredentials {
    endpoint: String,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "requires copied calibrated browser artifacts and local read-only MinIO credentials"]
async fn retained_browser_fixture_recovers_exact_buffered_versions() -> Result<()> {
    retained_browser_fixture(RetainedCheck::Recovery).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "requires retained calibrated browser inputs; keeps every query at 90 seconds"]
async fn retained_browser_fixture_preserves_full_wire_contract() -> Result<()> {
    retained_browser_fixture(RetainedCheck::Pages(PageRun::Contract)).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "focused retained-input comparison; full wire matrix remains a separate gate"]
async fn retained_browser_fixture_first_wire_pair() -> Result<()> {
    retained_browser_fixture(RetainedCheck::Pages(PageRun::FirstPair)).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "focused cold/warm profiling of the registered other predicate; full contract remains required"]
async fn retained_browser_fixture_other_wire_pairs() -> Result<()> {
    retained_browser_fixture(RetainedCheck::Pages(PageRun::OtherPairs)).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "requires retained calibrated browser inputs; isolates the three-hour admission and overflow guards"]
async fn retained_browser_fixture_preserves_three_hour_candidate_guards() -> Result<()> {
    retained_browser_fixture(RetainedCheck::PredicateGuards).await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PageRun {
    FirstPair,
    OtherPairs,
    Contract,
}

enum RetainedCheck {
    Recovery,
    Pages(PageRun),
    PredicateGuards,
}

async fn retained_browser_fixture(check: RetainedCheck) -> Result<()> {
    use object_store::ObjectStoreExt;
    timefusion::observability::init_local_metrics_for_test();
    let directory = PathBuf::from(
        std::env::var_os("TIMEFUSION_BROWSER_REPLAY_DIR").ok_or_else(|| anyhow::anyhow!("set TIMEFUSION_BROWSER_REPLAY_DIR to prepared replay artifacts"))?,
    );
    let replay: BrowserReplay = serde_json::from_slice(&std::fs::read(directory.join("replay-context.json"))?)?;
    let credentials: BrowserReplayCredentials = serde_json::from_slice(&std::fs::read(directory.join("credentials.json"))?)?;
    anyhow::ensure!(
        matches!(credentials.endpoint.as_str(), "http://127.0.0.1:9000" | "http://127.0.0.1:19000") && credentials.bucket == "timefusion-tests",
        "replay requires the isolated local store"
    );
    // SAFETY: serial, ignored fixture runs alone; restore the application clock
    // even on failure. Tokio and statement deadlines retain real elapsed time.
    anyhow::ensure!(!timefusion::support::is_frozen(), "another fixture owns the clock");
    timefusion::support::set_micros(replay.application_now_micros);
    let _clock = scopeguard::guard((), |_| timefusion::support::unfreeze());
    assert_eq!(replay.stores.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(), ["on", "off"]);
    let mut arms = Vec::new();
    for input in &replay.stores {
        let prefix = input
            .uri
            .strip_prefix("s3://timefusion-tests/")
            .and_then(|uri| uri.strip_suffix("/otel_logs_and_spans"))
            .ok_or_else(|| anyhow::anyhow!("invalid retained table URI"))?;
        let (db, ctx, _) = build_db_with("retained-browser", input.name == "on", |cfg| {
            cfg.aws.aws_s3_endpoint = credentials.endpoint.clone();
            cfg.aws.aws_access_key_id = Some(credentials.access_key.clone());
            cfg.aws.aws_secret_access_key = Some(credentials.secret_key.clone());
            cfg.aws.aws_default_region = Some(credentials.region.clone());
            cfg.core.timefusion_table_prefix = prefix.into();
            cfg.core.timefusion_data_dir = input.local_state.clone();
            cfg.core.timefusion_pgwire_max_statement_secs = 90;
            let production = AppConfig::default().tantivy;
            cfg.tantivy.timefusion_tantivy_reader_cache_entries = production.timefusion_tantivy_reader_cache_entries;
            cfg.tantivy.timefusion_tantivy_bundle_writes = production.timefusion_tantivy_bundle_writes;
            cfg.tantivy.timefusion_tantivy_range_reads = production.timefusion_tantivy_range_reads;
            cfg.tantivy.timefusion_tantivy_block_cache_mb = production.timefusion_tantivy_block_cache_mb;
        })
        .await?;
        let layer = db.buffered_layer().expect("buffered replay layer");
        let recovered = layer.recover_from_wal().await?;
        assert_eq!(recovered.corrupted_entries_skipped, 0);
        let batches = layer.query(&replay.project, "otel_logs_and_spans", &[])?;
        let mut actual = Vec::new();
        for batch in &batches {
            let ids = arrow::compute::cast(batch.column_by_name("id").expect("id"), &arrow::datatypes::DataType::Utf8)?;
            let timestamps = arrow::compute::cast(batch.column_by_name("timestamp").expect("timestamp"), &arrow::datatypes::DataType::Int64)?;
            let deleted = batch.column_by_name("deleted").expect("tombstone").as_boolean();
            for i in 0..batch.num_rows() {
                actual.push((
                    ids.as_string::<i32>().value(i).to_string(),
                    timestamps.as_primitive::<arrow::datatypes::Int64Type>().value(i),
                    !deleted.is_null(i) && deleted.value(i),
                ));
            }
        }
        actual.sort();
        let expected = [("deleted-only", 7, true), ("late", 4, false), ("stale", 2, false)]
            .map(|(id, minutes, deleted)| (id.to_string(), replay.prepared_end_micros - minutes * 60_000_000, deleted));
        assert_eq!(actual, expected, "{}: preserve buffered winners and tombstone", input.name);
        let target = db.resolve_table(&replay.project, "otel_logs_and_spans").await?;
        let table = target.read().await;
        assert_eq!(table.version(), Some(input.delta_version), "retained Delta snapshot");
        let own_files = table
            .snapshot()?
            .log_data()
            .iter()
            .filter(|file| file.partition_values_map().get("project_id").and_then(Option::as_deref) == Some(replay.project.as_str()))
            .count();
        assert_eq!(own_files, input.active_project_files, "partition metadata must exclude the misleading foreign path");
        eprintln!(
            "browser_replay_recovered arm={} buffered_rows={} entries={} delta_version={} own_files={}",
            input.name,
            actual.len(),
            recovered.entries_replayed,
            input.delta_version,
            own_files
        );
        drop(table);
        arms.push((db, ctx));
    }
    if matches!(&check, RetainedCheck::PredicateGuards) {
        let (on, ctx) = &arms[0];
        let (_, ctx_off) = &arms[1];
        return verify_candidate_predicates(on, ctx, ctx_off, &replay.project, replay.prepared_end_micros, 90, true).await;
    }
    if let RetainedCheck::Pages(run) = check {
        let mut arms = arms.into_iter();
        let (on, ctx) = arms.next().expect("indexed arm");
        let (off, ctx_off) = arms.next().expect("raw arm");
        let target = on.resolve_table(&replay.project, "otel_logs_and_spans").await?;
        let own_delta_bytes = target
            .read()
            .await
            .snapshot()?
            .log_data()
            .iter()
            .filter(|file| file.partition_values_map().get("project_id").and_then(Option::as_deref) == Some(replay.project.as_str()))
            .map(|file| file.size() as u64)
            .sum();
        let search = on.tantivy_search().expect("indexed search service");
        let manifest = timefusion::tantivy::load_manifest(search.object_store.as_ref(), "otel_logs_and_spans", &replay.project).await?;
        let mut index_bytes = 0;
        for entry in manifest.entries.into_values() {
            if let Some(path) = entry.index {
                index_bytes += search.object_store.head(&object_store::path::Path::from(path)).await?.size;
            }
        }
        anyhow::ensure!(index_bytes > 0, "the retained indexed arm must have real indexes");
        verify_mutable_candidate_input(
            MutableCandidateInput {
                on,
                ctx,
                off,
                ctx_off,
                p: replay.project,
                now: replay.prepared_end_micros,
                index_bytes,
                own_delta_bytes,
                has_background: true,
            },
            false,
            true,
            PageContract::LogExplorer,
            true,
            run,
        )
        .await?;
    }
    Ok(())
}

async fn mutable_candidate_fixture(complete: bool, cold_disk: bool, contract: PageContract, background_divisor: Option<usize>) -> Result<()> {
    use object_store::ObjectStoreExt;
    use timefusion::{observability::init_local_metrics_for_test, support::test_helpers::json_to_batch_for};
    init_local_metrics_for_test();
    let budget_secs = match contract {
        PageContract::IssueSample => 4,
        PageContract::LogExplorer | PageContract::RouteLogExplorer => 90,
    };
    // The calibrated partial case leaves over 6 GiB unindexed, above the
    // existing 512 MiB planning-read cap. It must keep the exact raw fallback.
    // Normal and proportional cases still require timestamp narrowing.
    let requires_raw_fallback = !complete && background_divisor == Some(1);
    let route = matches!(contract, PageContract::RouteLogExplorer);
    let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let (on, ctx, svc) = build_db_with(&format!("{id}-mutcand-on"), true, |cfg| {
        cfg.core.timefusion_pgwire_max_statement_secs = budget_secs;
        // The fixture's derived TantivyConfig::default has a one-reader floor;
        // use the production capacity so the warm pass does not thrash it.
        let production = AppConfig::default().tantivy;
        cfg.tantivy.timefusion_tantivy_reader_cache_entries = production.timefusion_tantivy_reader_cache_entries;
        cfg.tantivy.timefusion_tantivy_bundle_writes = production.timefusion_tantivy_bundle_writes;
        cfg.tantivy.timefusion_tantivy_range_reads = production.timefusion_tantivy_range_reads;
        cfg.tantivy.timefusion_tantivy_block_cache_mb = production.timefusion_tantivy_block_cache_mb;
    })
    .await?;
    let (off, ctx_off, _) = build_db_with(&format!("{id}-mutcand-off"), false, |cfg| cfg.core.timefusion_pgwire_max_statement_secs = budget_secs).await?;
    let (p, table, hash) = (unique_project(), "otel_logs_and_spans", "pat:E12e74a9");
    let now = chrono::Utc::now().timestamp_micros();
    let row = |id: &str, minutes: i64, h: &str| {
        let ts = now - minutes * 60_000_000;
        let date = chrono::DateTime::from_timestamp_micros(ts).unwrap().date_naive().to_string();
        let hashes = match (id, h) {
            ("solo", _) => vec![h, "unindexed"],
            ("late", h) if h == hash => vec![h, "buffered"],
            _ => vec![h],
        };
        let mut record = json!({"timestamp": ts, "date": date, "id": id, "name": id, "hashes": hashes, "project_id": p, "summary": [id], "context___trace_id": format!("trace-{id}"), "message_size_bytes": 1});
        if matches!(contract, PageContract::LogExplorer | PageContract::RouteLogExplorer) && (route || id != "solo") {
            record["duration"] = json!(1200);
            record["resource___service___name"] = json!("browser-api");
            record["parent_id"] = json!(format!("parent-{id}"));
            record["start_time"] = json!(ts);
            record["context___span_id"] = json!(format!("span-{id}"));
            record["kind"] = json!("span");
            record["level"] = json!("ERROR");
            record["severity___severity_number"] = json!(17);
            record["summary"] = json!([id, "quoted \"summary\"\nnext line"]);
        }
        if route {
            record["kind"] = json!("server");
            record["resource___deployment___environment___name"] = if id.len().is_multiple_of(2) { json!("") } else { serde_json::Value::Null };
            record["attributes___server___address"] = json!("browser-proxy");
            record["attributes___http___request___method"] = json!("GET");
            // Route fields are immutable; only hashes change across these versions.
            let route_tag = match id {
                "ghost" | "stale" => "other",
                "late" | "deleted-only" => hash,
                _ => h,
            };
            record["attributes___url___path"] = json!(format!("/fixture/{route_tag}"));
        }
        record
    };
    let fillers = |n: i64| (0..n).map(|i| row(&format!("f{i}"), 60 + i, "other")).collect::<Vec<_>>();
    for db in [&on, &off] {
        if let Some(divisor) = background_divisor {
            append_browser_background(db, &p, now, divisor).await?;
        }
        // Unindexed (direct) file: `ghost`'s older matching version, and `solo`, matching only here.
        let older = match contract {
            PageContract::IssueSample => Vec::new(),
            PageContract::LogExplorer | PageContract::RouteLogExplorer => (0..512)
                .map(|i| row(&format!("page-{i}"), 181 + i * if route { 18 } else { 36 }, hash))
                .chain([row("tie-a", 8, "ties"), row("tie-b", 8, "ties")])
                .collect(),
        };
        let excluded = if route {
            [
                ("attributes___server___address", serde_json::Value::Null),
                ("resource___deployment___environment___name", json!("other")),
                ("resource___service___name", json!("browser-proxy")),
                ("kind", json!("client")),
                ("attributes___http___request___method", json!("POST")),
                ("attributes___url___path", json!("/other")),
            ]
            .into_iter()
            .map(|(column, value)| {
                // Separate timestamps keep these route-only controls outside the
                // membership fixture's eight-version candidate budget.
                let mut record = row(&format!("excluded-{column}"), 9, hash);
                record[column] = value;
                record["hashes"] = json!([]);
                record
            })
            .collect()
        } else {
            Vec::new()
        };
        let direct = [vec![row("ghost", 1, hash), row("solo", 5, hash), row("removed-only", 6, "removed")], fillers(30), older, excluded].concat();
        db.insert_records_batch(&p, table, vec![json_to_batch_for(table, direct)?], true, None).await?;
        // Indexed file: `ghost` stops matching, `stale` and `kept` match, `late` does not yet.
        let indexed = [
            vec![
                row("ghost", 1, "other"),
                row("stale", 2, hash),
                row("kept", 3, hash),
                row("late", 4, "other"),
                row("removed-only", 6, "other"),
                row("deleted-only", 7, "deleted"),
            ],
            fillers(30),
        ]
        .concat();
        db.insert_records_batch(&p, table, vec![json_to_batch_for(table, indexed)?], false, None).await?;
        flush_all(db).await?;
    }
    let service = svc.expect("tantivy");
    let store = service.object_store.clone();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while timefusion::tantivy::load_manifest(store.as_ref(), table, &p).await?.entries.values().map(|e| e.rows).sum::<u64>() < 36 {
        anyhow::ensure!(tokio::time::Instant::now() < deadline, "the flushed file must be indexed, or the covered path goes untested");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let uris = on.list_file_uris(&p, table).await?;
    if complete {
        let covered = timefusion::tantivy::load_manifest(store.as_ref(), table, &p)
            .await?
            .entries
            .into_values()
            .flat_map(|entry| entry.covered_files)
            .collect::<std::collections::HashSet<_>>();
        let delta_store = on.resolve_table(&p, table).await?.read().await.log_store().object_store(None);
        for uri in &uris {
            if !covered.contains(uri) {
                let rel = timefusion::tantivy::search::parquet_rel_of_uri(uri).expect("relative parquet path");
                service.build_index_for_file(table, &p, rel, uri, delta_store.clone()).await?;
            }
        }
    }
    let covered = timefusion::tantivy::load_manifest(store.as_ref(), table, &p)
        .await?
        .entries
        .into_values()
        .flat_map(|entry| entry.covered_files)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(uris.iter().all(|uri| covered.contains(uri)), complete, "the requested coverage state must be real");
    let mut index_bytes = 0;
    for entry in timefusion::tantivy::load_manifest(store.as_ref(), table, &p).await?.entries.into_values() {
        if let Some(path) = entry.index {
            index_bytes += store.head(&object_store::path::Path::from(path)).await?.size;
        }
    }
    assert!(index_bytes > 0);
    let search = on.tantivy_search().expect("search service");
    assert!(search.stats.cache_seeded.load(std::sync::atomic::Ordering::Relaxed) > 0, "publication must seed real local indexes");
    if cold_disk {
        std::fs::remove_dir_all(on.config().core.timefusion_data_dir.join("tantivy_cache"))?;
    }
    for db in [&on, &off] {
        // MemBuffer: `stale` stops matching, `late` starts.
        let mut deleted = row("deleted-only", 7, "deleted");
        deleted["deleted"] = json!(true);
        db.insert_records_batch(&p, table, vec![json_to_batch_for(table, vec![row("stale", 2, "other"), row("late", 4, hash), deleted])?], false, None).await?;
    }
    // Another tenant's large file must not consume this project's planning budget.
    // The misleading path label pins that partition metadata is authoritative.
    // Its absent object also proves partition pruning does not read that tenant.
    let own_delta_bytes: u64 = {
        let table_ref = on.resolve_table(&p, table).await?;
        let mut target = table_ref.read().await.clone();
        let own_bytes = target.snapshot()?.log_data().iter().map(|file| file.size() as u64).sum();
        let foreign = unique_project();
        let date = chrono::DateTime::from_timestamp_micros(now).unwrap().date_naive().to_string();
        let add = deltalake::kernel::Add {
            path: format!("project_id={p}/date={date}/foreign-unreadable.parquet"),
            partition_values: [("project_id".into(), Some(foreign)), ("date".into(), Some(date))].into(),
            size: 1 << 30,
            data_change: true,
            ..Default::default()
        };
        let operation = deltalake::protocol::DeltaOperation::Write { mode: deltalake::protocol::SaveMode::Append, partition_by: None, predicate: None };
        let committed = deltalake::kernel::transaction::CommitBuilder::default()
            .with_actions(vec![deltalake::kernel::Action::Add(add)])
            .build(Some(target.snapshot()? as &dyn deltalake::kernel::transaction::TableReference), target.log_store(), operation)
            .await?;
        target.state = Some(committed.snapshot());
        *table_ref.write().await = target;
        own_bytes
    };
    verify_mutable_candidate_input(
        MutableCandidateInput { on, ctx, off, ctx_off, p, now, index_bytes, own_delta_bytes, has_background: background_divisor.is_some() },
        complete,
        cold_disk,
        contract,
        requires_raw_fallback,
        PageRun::Contract,
    )
    .await
}

struct MutableCandidateInput {
    on: Database,
    ctx: SessionContext,
    off: Database,
    ctx_off: SessionContext,
    p: String,
    now: i64,
    index_bytes: u64,
    own_delta_bytes: u64,
    has_background: bool,
}

async fn verify_mutable_candidate_input(
    input: MutableCandidateInput, complete: bool, cold_disk: bool, contract: PageContract, requires_raw_fallback: bool, run: PageRun,
) -> Result<()> {
    use timefusion::{database::scan_metric_names::PREFILTER_MUTABLE_CANDIDATES, observability::counter_value};
    let MutableCandidateInput { on, ctx, off, ctx_off, p, now, index_bytes, own_delta_bytes, has_background } = input;
    let hash = "pat:E12e74a9";
    let (window_hours, budget_secs) = match contract {
        PageContract::IssueSample => (3, 4),
        PageContract::LogExplorer => (14 * 24, 90),
        PageContract::RouteLogExplorer => (7 * 24, 90),
    };
    let route = matches!(contract, PageContract::RouteLogExplorer);
    // Exercise each captured caller through its actual protocol, against an
    // independent server whose prefilter is disabled as the oracle.
    let stop = tokio_util::sync::CancellationToken::new();
    let mut tasks = tokio::task::JoinSet::new();
    let mut clients = Vec::new();
    for (context, db) in [(ctx.clone(), on.clone()), (ctx_off.clone(), off.clone())] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let shutdown = stop.clone().cancelled_owned();
        tasks.spawn(async move {
            timefusion::server::serve_with_listener(
                listener,
                Arc::new(context),
                &datafusion_postgres::ServerOptions::new(),
                timefusion::server::AuthConfig { username: "postgres".into(), password: Some("test".into()) },
                None,
                Some(Arc::new(db)),
                shutdown,
            )
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))
        });
        let (client, connection) = tokio_postgres::connect(&format!("host=127.0.0.1 port={port} user=postgres password=test"), tokio_postgres::NoTls).await?;
        tasks.spawn(async move { connection.await.map_err(anyhow::Error::from) });
        client.batch_execute(&format!("SET statement_timeout = '{budget_secs}s'")).await?;
        let expected_timeout = format!("{}ms", budget_secs * 1000);
        assert!(
            client
                .simple_query("SHOW statement_timeout")
                .await?
                .iter()
                .any(|message| matches!(message, tokio_postgres::SimpleQueryMessage::Row(row) if row.get(0) == Some(expected_timeout.as_str()))),
            "the wire server must enforce the captured {budget_secs}-second deadline"
        );
        clients.push(client);
    }
    let iso = |time| chrono::DateTime::from_timestamp_micros(time).unwrap().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
    let search = on.tantivy_search().expect("search service");
    let opens = || search.stats.index_opens.load(std::sync::atomic::Ordering::Relaxed) + search.stats.bundle_opens.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(opens(), 0, "first query must exercise a cold index reader");
    let range_bytes = || search.stats.range_read_bytes.load(std::sync::atomic::Ordering::Relaxed);
    let mut warm_opens = 0;
    let mut warm_bytes = 0;
    let mut cancellation_sql = None;
    let passes = if run == PageRun::FirstPair { 1 } else { 2 };
    for pass in 0..passes {
        for (tag, trace) in [
            (hash, Some("trace-kept")),
            ("other", Some("trace-ghost")),
            ("unindexed", Some("trace-solo")),
            ("buffered", Some("trace-late")),
            ("absent", None),
            ("removed", None),
            ("deleted", None),
        ]
        .into_iter()
        .chain(matches!(contract, PageContract::LogExplorer | PageContract::RouteLogExplorer).then_some(("ties", Some("trace-tie-a"))))
        .filter(|(tag, _)| !route || *tag == hash)
        .filter(|(tag, _)| match run {
            PageRun::Contract => true,
            PageRun::FirstPair => *tag == hash,
            PageRun::OtherPairs => *tag == "other",
        }) {
            let projection = match contract {
                PageContract::IssueSample => "timestamp, summary, context___trace_id".to_owned(),
                PageContract::LogExplorer | PageContract::RouteLogExplorer => {
                    let (time_format, scale, error_kind) =
                        if route { ("YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"", 1_000_000_000, "log") } else { ("YYYY-MM-DD HH24:MI:SS.US", 1_000_000, "span") };
                    format!(
                        "jsonb_build_array(id,to_char(timestamp at time zone 'UTC', '{time_format}'),context___trace_id,name,duration,resource___service___name,parent_id,cast(extract(epoch from (start_time)) * {scale} as bigint),coalesce(errors is not null or (kind = '{error_kind}' and (lower(level) = 'error' or severity___severity_number >= 17 or status_code = 'ERROR')), false),to_jsonb(summary),context___span_id,kind)"
                    )
                }
            };
            let (predicate, limit) = match contract {
                PageContract::IssueSample => (format!("'{tag}' = ANY(hashes)"), "1"),
                PageContract::LogExplorer => (format!("array_has(hashes, '{tag}')"), "$1"),
                PageContract::RouteLogExplorer => (
                    format!(
                        "resource___service___name = 'browser-api' AND (resource___deployment___environment___name IS NULL OR resource___deployment___environment___name = '') AND (attributes___server___address = 'browser-proxy' OR (attributes___server___address IS NULL AND resource___service___name = 'browser-proxy')) AND kind = 'server' AND attributes___http___request___method = 'GET' AND attributes___url___path = '/fixture/{tag}'"
                    ),
                    "$1",
                ),
            };
            let sql = format!(
                "SELECT {projection} FROM otel_logs_and_spans WHERE project_id='{p}' \
                AND timestamp BETWEEN '{}' AND '{}' AND {predicate} ORDER BY timestamp DESC LIMIT {limit}",
                iso(now - window_hours * 3_600_000_000),
                iso(now)
            );
            if has_background && pass == 0 && tag == hash {
                cancellation_sql = Some(sql.clone());
            }
            let before = counter_value(PREFILTER_MUTABLE_CANDIDATES);
            let bytes = deltalake::delta_datafusion::parquet_metrics::snapshot().bytes_read;
            let index_before = range_bytes();
            let phases: Vec<_> = timefusion::observability::plan_phases().collect();
            let started = std::time::Instant::now();
            let page = tokio::time::timeout(std::time::Duration::from_secs(budget_secs + 1), wire_page(&clients[0], &sql, contract)).await;
            let elapsed = started.elapsed();
            let cost: Vec<_> = timefusion::observability::plan_phases()
                .zip(phases)
                .map(|((phase, us, calls), (_, old_us, old_calls))| (phase, us - old_us, calls - old_calls))
                .collect();
            let read_bytes = deltalake::delta_datafusion::parquet_metrics::snapshot().bytes_read - bytes;
            let index_read_bytes = range_bytes() - index_before;
            if !matches!(page, Ok(Ok(_))) {
                eprintln!(
                    "{contract:?} failed complete={complete} cold_disk={cold_disk} pass={pass} tag={tag} elapsed_us={} parquet_bytes={read_bytes} index_range_bytes={index_read_bytes} phases={cost:?} outcome={:?}",
                    elapsed.as_micros(),
                    page.as_ref().map(|result| result.as_ref().map(|_| ()))
                );
            }
            let (actual, first_row) = page??;
            assert!(elapsed < std::time::Duration::from_secs(budget_secs), "{contract:?} must retain its captured deadline");
            assert_eq!(
                counter_value(PREFILTER_MUTABLE_CANDIDATES),
                before + u64::from(!requires_raw_fallback && !route),
                "candidate admission must preserve the planning-read cap"
            );
            assert!(read_bytes <= 4 * own_delta_bytes, "planning plus execution must stay bounded by this project's physical files");
            assert!(index_read_bytes <= 4 * index_bytes, "range reads must stay bounded by this project's index objects");
            assert_eq!(search.stats.blob_fetches.load(std::sync::atomic::Ordering::Relaxed), 0, "sample queries must not install whole blobs");
            eprintln!(
                "{contract:?} complete={complete} cold_disk={cold_disk} pass={pass} tag={tag} elapsed_us={} first_row={first_row:?} parquet_bytes={read_bytes} index_range_bytes={index_read_bytes} phases={cost:?}",
                elapsed.as_micros()
            );
            let oracle_bytes = deltalake::delta_datafusion::parquet_metrics::snapshot().bytes_read;
            let phases: Vec<_> = timefusion::observability::plan_phases().collect();
            let oracle_started = std::time::Instant::now();
            let oracle_page = tokio::time::timeout(std::time::Duration::from_secs(budget_secs + 1), wire_page(&clients[1], &sql, contract)).await;
            let cost: Vec<_> = timefusion::observability::plan_phases()
                .zip(phases)
                .map(|((phase, us, calls), (_, old_us, old_calls))| (phase, us - old_us, calls - old_calls))
                .collect();
            eprintln!(
                "{contract:?} raw_oracle complete={complete} cold_disk={cold_disk} pass={pass} tag={tag} elapsed_us={} first_row={:?} parquet_bytes={} phases={cost:?} reserved_bytes={} spill_bytes={} outcome={:?}",
                oracle_started.elapsed().as_micros(),
                oracle_page.as_ref().ok().and_then(|page| page.as_ref().ok()).and_then(|(_, first_row)| *first_row),
                deltalake::delta_datafusion::parquet_metrics::snapshot().bytes_read - oracle_bytes,
                ctx_off.runtime_env().memory_pool.reserved(),
                ctx_off.runtime_env().disk_manager.used_disk_space(),
                oracle_page.as_ref().map(|result| result.as_ref().map(|_| ()))
            );
            let (oracle, _) = oracle_page??;
            assert_eq!(
                counter_value(PREFILTER_MUTABLE_CANDIDATES),
                before + u64::from(!requires_raw_fallback && !route),
                "oracle must not use the candidate path"
            );
            assert_eq!(actual, oracle, "pass {pass}: all projected values must match the full latest-visible-row oracle for {tag}");
            assert_eq!(actual.first().and_then(|row| row[2].as_str()), trace, "pass {pass}: expected visible winner for {tag}");
            let expected_len = match (contract, tag) {
                (PageContract::LogExplorer | PageContract::RouteLogExplorer, tag) if tag == hash => 501,
                (PageContract::LogExplorer | PageContract::RouteLogExplorer, "other") => 33,
                (PageContract::LogExplorer | PageContract::RouteLogExplorer, "ties") => 2,
                _ => usize::from(trace.is_some()),
            };
            assert_eq!(actual.len(), expected_len);
            if matches!(contract, PageContract::LogExplorer | PageContract::RouteLogExplorer) && tag == hash {
                let expected: Vec<_> =
                    ["kept".to_string(), "late".to_string(), "solo".to_string()].into_iter().chain((0..498).map(|i| format!("page-{i}"))).collect();
                assert_eq!(actual.iter().map(|row| row[0].as_str().expect("browser id")).collect::<Vec<_>>(), expected);
                let ts = now - 3 * 60_000_000;
                assert_eq!(
                    actual[0],
                    json!([
                        "kept",
                        chrono::DateTime::from_timestamp_micros(ts)
                            .unwrap()
                            .format(if route { "%Y-%m-%dT%H:%M:%S%.6fZ" } else { "%Y-%m-%d %H:%M:%S%.6f" })
                            .to_string(),
                        "trace-kept",
                        "kept",
                        1200,
                        "browser-api",
                        "parent-kept",
                        ts * if route { 1000 } else { 1 },
                        !route,
                        ["kept", "quoted \"summary\"\nnext line"],
                        "span-kept",
                        if route { "server" } else { "span" }
                    ]),
                    "all twelve prepared JSON fields must preserve their values and types"
                );
            }
            let runtimes = [ctx.runtime_env(), ctx_off.runtime_env()];
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while runtimes.iter().any(|r| r.memory_pool.reserved() != 0 || r.disk_manager.used_disk_space() != 0) {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            eprintln!(
                "browser_pair_complete pass={pass} tag={tag} rows={} reserved_after={} spill_after={}",
                actual.len(),
                runtimes.iter().map(|r| r.memory_pool.reserved()).sum::<usize>(),
                runtimes.iter().map(|r| r.disk_manager.used_disk_space()).sum::<u64>()
            );
        }
        if pass == 0 {
            warm_opens = opens();
            warm_bytes = range_bytes();
            assert!(warm_opens > 0);
            if cold_disk {
                assert!(
                    search.stats.bundle_opens.load(std::sync::atomic::Ordering::Relaxed) > 0 && warm_bytes > 0,
                    "cold disk must exercise remote bundle reads"
                );
                assert_eq!(search.stats.index_opens.load(std::sync::atomic::Ordering::Relaxed), 0);
            } else {
                assert!(search.stats.index_opens.load(std::sync::atomic::Ordering::Relaxed) > 0, "seeded disk must open installed readers");
                assert_eq!(warm_bytes, 0);
            }
        } else {
            assert_eq!(opens(), warm_opens, "warm repetition must reuse open index readers");
            assert_eq!(range_bytes(), warm_bytes, "warm repetition must reuse bundle blocks");
        }
    }
    if run == PageRun::Contract
        && let Some(sql) = cancellation_sql
    {
        let runtime = ctx_off.runtime_env();
        let token = clients[1].cancel_token();
        let query = wire_page(&clients[1], &sql, contract);
        futures::pin_mut!(query);
        let reserved = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(1));
            loop {
                tokio::select! {
                    result = &mut query => { result?; anyhow::bail!("browser query completed before an observable reservation"); },
                    _ = tick.tick() => {
                        let bytes = runtime.memory_pool.reserved();
                        if bytes > 0 { break anyhow::Ok(bytes); }
                    }
                }
            }
        })
        .await??;
        let canceled = std::time::Instant::now();
        token.cancel_query(tokio_postgres::NoTls).await?;
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), &mut query).await?.expect_err("the browser query must acknowledge cancellation");
        assert_eq!(error.downcast_ref::<tokio_postgres::Error>().and_then(|error| error.code()), Some(&tokio_postgres::error::SqlState::QUERY_CANCELED));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while runtime.memory_pool.reserved() != 0 || runtime.disk_manager.used_disk_space() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let release_ms = canceled.elapsed().as_millis();
        assert_eq!(clients[1].query_one("SELECT 42::BIGINT", &[]).await?.get::<_, i64>(0), 42, "the canceled connection must remain usable");
        eprintln!(
            "browser_volume_canceled reserved_before={reserved} release_ms={release_ms} reserved_after={} spill_after={}",
            runtime.memory_pool.reserved(),
            runtime.disk_manager.used_disk_space()
        );
    }
    drop(clients);
    stop.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        anyhow::Ok(())
    })
    .await??;
    if run != PageRun::Contract {
        return Ok(());
    }
    verify_candidate_predicates(&on, &ctx, &ctx_off, &p, now, budget_secs, requires_raw_fallback).await
}

async fn verify_candidate_predicates(
    on: &Database, ctx: &SessionContext, ctx_off: &SessionContext, p: &str, now: i64, budget_secs: u64, wide_requires_raw_fallback: bool,
) -> Result<()> {
    use timefusion::{database::scan_metric_names::PREFILTER_MUTABLE_CANDIDATES, observability::counter_value};
    let (table, hash) = ("otel_logs_and_spans", "pat:E12e74a9");
    let search = on.tantivy_search().expect("search service");
    let covered = timefusion::tantivy::load_manifest(search.object_store.as_ref(), table, p)
        .await?
        .entries
        .into_values()
        .flat_map(|entry| entry.covered_files)
        .filter_map(|uri| timefusion::tantivy::search::parquet_rel_of_uri(&uri).map(str::to_owned))
        .collect::<std::collections::HashSet<_>>();
    let target = on.resolve_table(p, table).await?;
    let target = target.read().await;
    let upper = chrono::DateTime::from_timestamp_micros(now).unwrap().date_naive().to_string();
    let raw_bytes = |lo| -> u64 {
        let lower = chrono::DateTime::from_timestamp_micros(lo).unwrap().date_naive().to_string();
        target
            .snapshot()
            .unwrap()
            .log_data()
            .iter()
            .filter(|file| {
                let partitions = file.partition_values_map();
                partitions.get("project_id").and_then(Option::as_deref).is_none_or(|project| project == p)
                    && partitions.get("date").and_then(Option::as_deref).is_none_or(|date| date >= lower.as_str() && date <= upper.as_str())
                    && !covered.contains(file.path().as_ref())
            })
            .map(|file| file.size() as u64)
            .sum()
    };
    let (wide_bytes, narrow_bytes) = (raw_bytes(now - 14 * 86_400_000_000), raw_bytes(now - 3 * 3_600_000_000));
    let cap = 512 << 20;
    assert_eq!(wide_bytes > cap, wide_requires_raw_fallback, "preserve the broad-window corpus budget");
    assert!(narrow_bytes <= cap, "the separate three-hour guard must fit the unchanged planning cap: {narrow_bytes}");
    eprintln!("browser_candidate_guard_budget wide_bytes={wide_bytes} three_hour_bytes={narrow_bytes} cap={cap}");
    drop(target);
    for predicate in [
        format!("array_has(hashes, '{hash}')"),
        format!("'{hash}' = ANY(hashes)"),
        format!("hashes @> ARRAY['{hash}']"),
        format!("hashes && ARRAY['{hash}', 'absent']"),
    ] {
        let sql = format!(
            "SELECT id FROM {table} WHERE project_id='{p}' AND timestamp BETWEEN to_timestamp_micros({}) AND to_timestamp_micros({now}) AND {predicate} ORDER BY timestamp DESC",
            now - 3 * 3_600_000_000
        );
        let before = counter_value(PREFILTER_MUTABLE_CANDIDATES);
        let (ids, narrowed) = tokio::time::timeout(std::time::Duration::from_secs(budget_secs), ids_and_dedup_input(ctx, &sql)).await??;
        assert_eq!(counter_value(PREFILTER_MUTABLE_CANDIDATES), before + 1, "{predicate} must preserve candidate admission");
        assert_eq!(ids, ["kept", "late", "solo"], "{predicate}");
        let (baseline, full) = tokio::time::timeout(std::time::Duration::from_secs(budget_secs), ids_and_dedup_input(ctx_off, &sql)).await??;
        assert_eq!(ids, baseline);
        assert!(narrowed <= 8, "{predicate}: the bounded three-hour query must dedup only candidate versions: {narrowed} vs {full}");
        assert!(full >= 60, "{predicate}: the raw oracle must retain its full input");
    }
    // Over the cap the search is refused, and a page polling the same hash must not pay it again.
    let (other, window) =
        (timefusion::tantivy::histogram::Membership::Contains { column: "hashes".into(), value: "other".into() }, Some((now - 3 * 3_600_000_000, now)));
    let touched = || search.stats.reader_hits.load(std::sync::atomic::Ordering::Relaxed) + search.stats.index_opens.load(std::sync::atomic::Ordering::Relaxed);
    assert!(search.membership_timestamps(table, p, &other, 5, window).await?.is_none(), "60 fillers exceed a cap of 5");
    let before = touched();
    assert!(search.membership_timestamps(table, p, &other, 5, window).await?.is_none());
    assert_eq!(touched(), before, "a remembered overflow must not open an index");
    Ok(())
}

/// `body contains "x"` reaches TimeFusion as `body::text ~* 'x'`, planned as
/// `variant_to_json(body)`; the JSON body index holds that same text, so the search routes
/// and must equal the unindexed answer, MemBuffer rows and case variants included.
#[tokio::test(flavor = "multi_thread")]
async fn a_body_text_search_routes_through_the_json_index() -> Result<()> {
    use timefusion::{
        database::scan_metric_names::PREFILTER_USED,
        observability::{counter_value, init_local_metrics_for_test},
    };
    init_local_metrics_for_test();
    let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let (on, ctx, svc) = build_db(&format!("{id}-body-on"), true).await?;
    let (off, ctx_off, _) = build_db(&format!("{id}-body-off"), false).await?;
    let p = unique_project();
    let now = chrono::Utc::now().timestamp_micros();
    let batch = |rows: &[(i64, &str, &str)]| -> Result<RecordBatch> {
        let base = timed_batch(&p, &rows.iter().map(|(minutes, id, _)| (now - minutes * 60_000_000, id.to_string(), "INFO")).collect::<Vec<_>>());
        let at = base.schema().index_of("body")?;
        let mut columns = base.columns().to_vec();
        columns[at] = super::tantivy_index_test::build_variant(rows.iter().map(|(.., body)| *body).collect());
        Ok(RecordBatch::try_new(base.schema(), columns)?)
    };
    let mut flushed = vec![(1, "hit-upper", r#"{"msg":"upstream TIMEOUT"}"#), (2, "hit-nested", r#"{"err":{"kind":"timeout"}}"#)];
    flushed.extend((0..40).map(|i| (3 + i, "", r#"{"msg":"ok"}"#)));
    let ids: Vec<String> = (0..40).map(|i| format!("ok{i}")).collect();
    flushed.iter_mut().skip(2).zip(&ids).for_each(|(row, id)| row.1 = id);
    for db in [&on, &off] {
        db.insert_records_batch(&p, TABLE, vec![batch(&flushed)?], false, None).await?;
        flush_all(db).await?;
    }
    let store = svc.expect("tantivy").object_store.clone();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while timefusion::tantivy::load_manifest(store.as_ref(), TABLE, &p).await?.entries.values().map(|e| e.rows).sum::<u64>() < flushed.len() as u64 {
        anyhow::ensure!(tokio::time::Instant::now() < deadline, "the flushed body must be indexed");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    for db in [&on, &off] {
        db.insert_records_batch(&p, TABLE, vec![batch(&[(0, "hit-buffered", r#"{"msg":"read timeout"}"#)])?], false, None).await?;
    }
    for (predicate, expected, routed) in [
        ("body::text ~* 'timeout'", &["hit-buffered", "hit-upper", "hit-nested"][..], true),
        ("body::text ILIKE '%timeout%'", &["hit-buffered", "hit-upper", "hit-nested"], true),
        // JSON punctuation is not a routable term character, so no rendering difference can drop a row.
        (r#"body::text ILIKE '%"kind":"timeout"%'"#, &["hit-nested"], false),
    ] {
        let sql = format!(
            "SELECT id FROM {TABLE} WHERE project_id='{p}' AND timestamp BETWEEN to_timestamp_micros({}) AND to_timestamp_micros({now}) AND {predicate} ORDER BY timestamp DESC",
            now - 3 * 3_600_000_000
        );
        let before = counter_value(PREFILTER_USED);
        let ids = ordered_ids(&ctx, &sql).await?;
        assert_eq!(counter_value(PREFILTER_USED), before + u64::from(routed), "{predicate}: routed must be {routed}");
        assert_eq!(ids, expected, "{predicate}");
        assert_eq!(ids, ordered_ids(&ctx_off, &sql).await?);
    }
    Ok(())
}
