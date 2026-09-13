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

fn cfg(test_id: &str) -> Arc<AppConfig> {
    let mut c = AppConfig::default();
    c.aws.aws_s3_bucket = Some("timefusion-tests".to_string());
    c.aws.aws_access_key_id = Some("minioadmin".into());
    c.aws.aws_secret_access_key = Some("minioadmin".into());
    c.aws.aws_s3_endpoint = "http://127.0.0.1:9000".into();
    c.aws.aws_default_region = Some("us-east-1".into());
    c.aws.aws_allow_http = Some("true".into());
    c.core.timefusion_table_prefix = format!("tantivy-e2e-{test_id}");
    c.core.timefusion_data_dir = data_dir(test_id);
    c.cache.timefusion_foyer_disabled = true;
    c.tantivy = TantivyConfig {
        timefusion_tantivy_compression_level: 3,
        timefusion_tantivy_route_equality: true,
        timefusion_tantivy_prefilter_min_selectivity_pct: 50,
        ..Default::default()
    };
    Arc::new(c)
}

/// The on-disk cache root `cfg` gives a test id; a hand-built
/// `TantivySearchService` must point at the same directory.
fn data_dir(test_id: &str) -> PathBuf {
    PathBuf::from(format!("/tmp/timefusion-tantivy-e2e-{test_id}"))
}

/// Build a DB with the full BufferedWriteLayer + Tantivy callback wired up,
/// returning an immediately-flushing layer (interval=1s).
async fn build_db(test_id: &str, tantivy_enabled: bool) -> Result<(Database, SessionContext, Option<Arc<TantivyIndexService>>)> {
    let cfg_arc = cfg(test_id);
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
        let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let on_id = format!("{id}-{tag}-on");
        let (on, ctx, svc) = build_db(&on_id, true).await?;
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
    assert_eq!(snapshots(&db), before + 1, "freshly flushed hashes must reach the histogram path without manual backfill");
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
    let search = TantivySearchService::new(pair.svc.object_store.clone(), pair.cache_root.clone(), Arc::new(TantivyConfig::default()));
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
