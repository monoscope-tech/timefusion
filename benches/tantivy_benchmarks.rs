//! Tier-5 tantivy benchmarks.
//!
//! Measures:
//! 1. Index-build throughput: rows/sec for `build_in_memory`.
//! 2. Index size: ratio of packed (`tar.zst`) bytes to source row count.
//! 3. Query latency: term query against a 100k-row index.
//!
//! Real "scan-with-vs-without" benches against Delta are intentionally
//! deferred — they require a running MinIO and add minutes to CI. The
//! tantivy-only benches here are sufficient to detect regressions in
//! the indexing/query layer itself.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit},
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use tantivy::{Term, query::TermQuery, schema::IndexRecordOption};
use timefusion::{
    schema_loader::{FieldDef, SortingColumnDef, TableSchema, TantivyFieldConfig},
    tantivy_index::{builder::build_in_memory, reader::query_index, store},
};

fn table() -> TableSchema {
    TableSchema {
        table_name:      "bench".into(),
        partitions:      vec![],
        sorting_columns: vec![SortingColumnDef {
            name:        "timestamp".into(),
            descending:  false,
            nulls_first: false,
        }],
        z_order_columns: vec![],
        time_column:     None,
        dedup_keys:      vec![],
        fields:          vec![
            FieldDef {
                name:         "timestamp".into(),
                data_type:    "Timestamp(Microsecond, Some(\"UTC\"))".into(),
                nullable:     false,
                tantivy:      None,
                dictionary:   None,
                bloom_filter: false,
            },
            FieldDef {
                name:         "id".into(),
                data_type:    "Utf8".into(),
                nullable:     false,
                tantivy:      None,
                dictionary:   None,
                bloom_filter: false,
            },
            FieldDef {
                name:         "level".into(),
                data_type:    "Utf8".into(),
                nullable:     true,
                tantivy:      Some(TantivyFieldConfig {
                    indexed:   true,
                    tokenizer: Some("raw".into()),
                    flatten:   None,
                }),
                dictionary:   None,
                bloom_filter: false,
            },
            FieldDef {
                name:         "message".into(),
                data_type:    "Utf8".into(),
                nullable:     true,
                tantivy:      Some(TantivyFieldConfig {
                    indexed:   true,
                    tokenizer: Some("default".into()),
                    flatten:   None,
                }),
                dictionary:   None,
                bloom_filter: false,
            },
        ],
    }
}

fn synthetic_batch(n: usize) -> RecordBatch {
    let levels = ["INFO", "WARN", "ERROR", "DEBUG", "TRACE"];
    let words = ["request", "completed", "panic", "shutdown", "timeout", "connection", "lost", "recovered"];
    let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from((0..n as i64).map(|i| 1_000_000 + i * 1000).collect::<Vec<_>>()).with_timezone("UTC"));
    let id: ArrayRef = Arc::new(StringArray::from((0..n).map(|i| format!("id-{i}")).collect::<Vec<_>>()));
    let level: ArrayRef = Arc::new(StringArray::from((0..n).map(|i| levels[i % levels.len()]).collect::<Vec<_>>()));
    let msg: ArrayRef = Arc::new(StringArray::from(
        (0..n).map(|i| format!("{} {}", words[i % words.len()], words[(i + 3) % words.len()])).collect::<Vec<_>>(),
    ));
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("id", DataType::Utf8, false),
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(schema, vec![ts, id, level, msg]).unwrap()
}

fn bench_build(c: &mut Criterion) {
    let table = table();
    let mut g = c.benchmark_group("tantivy_build");
    for &n in &[10_000usize, 100_000] {
        let b = synthetic_batch(n);
        g.throughput(Throughput::Elements(n as u64));
        g.bench_function(format!("build_in_memory/{n}"), |bench| {
            bench.iter(|| {
                let _ = build_in_memory(&table, std::slice::from_ref(&b)).unwrap();
            });
        });
    }
    g.finish();
}

fn bench_query(c: &mut Criterion) {
    let table = table();
    let b = synthetic_batch(100_000);
    let (idx, built, _) = build_in_memory(&table, std::slice::from_ref(&b)).unwrap();
    let level = built.user_fields.get("level").unwrap().field;
    c.bench_function("tantivy_query_term_100k", |bench| {
        bench.iter(|| {
            let q = TermQuery::new(Term::from_field_text(level, "ERROR"), IndexRecordOption::Basic);
            let _ = query_index(&idx, &q, None).unwrap();
        });
    });
}

fn bench_size_ratio(c: &mut Criterion) {
    let table = table();
    let n = 100_000usize;
    let b = synthetic_batch(n);
    let (blob, stats) = store::build_and_pack(&table, std::slice::from_ref(&b), 19).unwrap();
    let bytes_per_row = blob.len() as f64 / stats.rows as f64;
    println!(
        "tantivy index size: {} bytes for {} rows ({:.2} bytes/row)",
        blob.len(),
        stats.rows,
        bytes_per_row
    );
    c.bench_function("tantivy_pack_100k_zstd_19", |bench| {
        bench.iter(|| {
            let _ = store::build_and_pack(&table, std::slice::from_ref(&b), 19).unwrap();
        });
    });
}

// ────────────────────────────────────────────────────────────────────────────
// End-to-end scan bench: text_match with tantivy prefilter ON vs OFF.
// Requires MinIO. Skipped if AWS_S3_ENDPOINT isn't reachable.
// ────────────────────────────────────────────────────────────────────────────

use std::{path::PathBuf, time::Duration};

use serde_json::json;
use timefusion::{
    buffered_write_layer::DeltaWriteCallback,
    config::{AppConfig, TantivyConfig},
    database::Database,
    tantivy_index::{search::TantivySearchService, service::TantivyIndexService},
    test_utils::test_helpers::json_to_batch,
};

fn make_app_cfg(test_id: &str, _tantivy_enabled: bool) -> Arc<AppConfig> {
    let mut c = AppConfig::default();
    c.aws.aws_s3_bucket = Some("timefusion-tests".to_string());
    c.aws.aws_access_key_id = Some("minioadmin".into());
    c.aws.aws_secret_access_key = Some("minioadmin".into());
    c.aws.aws_s3_endpoint = "http://127.0.0.1:9000".into();
    c.aws.aws_default_region = Some("us-east-1".into());
    c.aws.aws_allow_http = Some("true".into());
    c.core.timefusion_table_prefix = format!("tantivy-bench-{test_id}");
    c.core.timefusion_data_dir = PathBuf::from(format!("/tmp/timefusion-tantivy-bench-{test_id}"));
    c.cache.timefusion_foyer_disabled = true;
    c.tantivy = TantivyConfig {
        timefusion_tantivy_compression_level: 3,
        ..Default::default()
    };
    Arc::new(c)
}

async fn setup_bench_db(test_id: &str, tantivy_enabled: bool, rows: usize) -> Option<(Database, datafusion::execution::context::SessionContext, String)> {
    let cfg_arc = make_app_cfg(test_id, tantivy_enabled);
    let mut db = Database::with_config(cfg_arc.clone()).await.ok()?;
    let db_for_cb = db.clone();
    let delta_cb: DeltaWriteCallback = Arc::new(move |project_id, table_name, batches, _watermark| {
        let db = db_for_cb.clone();
        Box::pin(async move {
            let pre = db.list_file_uris(&project_id, &table_name).await.unwrap_or_default();
            db.insert_records_batch(&project_id, &table_name, batches, true, None).await?;
            let post = db.list_file_uris(&project_id, &table_name).await.unwrap_or_default();
            let pre_set: std::collections::HashSet<String> = pre.into_iter().collect();
            Ok(post.into_iter().filter(|u| !pre_set.contains(u)).collect())
        })
    });
    let mut layer = timefusion::test_utils::test_helpers::test_layer(cfg_arc.clone()).ok()?.with_delta_writer(delta_cb);
    if tantivy_enabled {
        let bucket = cfg_arc.aws.aws_s3_bucket.clone().unwrap();
        let storage_uri = format!("s3://{}/{}/tantivy", bucket, cfg_arc.core.timefusion_table_prefix);
        let storage_opts = cfg_arc.aws.build_storage_options(None);
        let obj_store = db.create_object_store(&storage_uri, &storage_opts).await.ok()?;
        let s = Arc::new(TantivyIndexService::new(obj_store.clone(), Arc::new(cfg_arc.tantivy.clone())));
        layer = layer.with_tantivy_indexer(s.clone().callback());
        let cache_root = cfg_arc.core.timefusion_data_dir.clone();
        let search = Arc::new(TantivySearchService::new(obj_store, cache_root));
        db = db.with_tantivy_search(search).with_tantivy_indexer(s);
    }
    db = db.with_buffered_layer(Arc::new(layer));

    let db_arc = Arc::new(db.clone());
    let mut ctx = db_arc.create_session_context();
    datafusion_functions_json::register_all(&mut ctx).ok()?;
    db.setup_session_context(&mut ctx).ok()?;

    // Insert `rows` rows; only ~1% will match the query "panic" → high selectivity.
    let project = format!("p-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let words = ["request completed", "shutdown clean", "timeout connection", "request received", "panic occurred"];
    let now = chrono::Utc::now();
    let recs: Vec<_> = (0..rows)
        .map(|i| {
            json!({
                "timestamp": now.timestamp_micros() + i as i64,
                "id": format!("r{i}"),
                "project_id": project,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": vec![format!("row {i}")],
                "status_message": words[i % words.len()],
            })
        })
        .collect();
    let batch = json_to_batch(recs).ok()?;
    db.insert_records_batch(&project, "otel_logs_and_spans", vec![batch], false, None).await.ok()?;
    db.buffered_layer().cloned()?.flush_all_now().await.ok()?;
    Some((db, ctx, project))
}

fn minio_reachable() -> bool {
    std::net::TcpStream::connect_timeout(&"127.0.0.1:9000".parse().unwrap(), Duration::from_millis(200)).is_ok()
}

fn bench_e2e_scan(c: &mut Criterion) {
    if !minio_reachable() {
        eprintln!("tantivy_benchmarks: MinIO not reachable on 127.0.0.1:9000; skipping e2e bench");
        return;
    }
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();

    let id_on = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let id_off = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let (_db_on, ctx_on, p_on) = rt.block_on(async { setup_bench_db(&id_on, true, 10_000).await.expect("setup ON") });
    let (_db_off, ctx_off, p_off) = rt.block_on(async { setup_bench_db(&id_off, false, 10_000).await.expect("setup OFF") });
    let q_on = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id='{p_on}' AND text_match(status_message, 'panic')");
    let q_off = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id='{p_off}' AND text_match(status_message, 'panic')");

    let mut g = c.benchmark_group("tantivy_scan_e2e");
    g.measurement_time(Duration::from_secs(15));
    g.bench_function("scan_10k_with_prefilter", |b| {
        b.to_async(&rt).iter(|| async {
            let _ = ctx_on.sql(&q_on).await.unwrap().collect().await.unwrap();
        });
    });
    g.bench_function("scan_10k_without_prefilter", |b| {
        b.to_async(&rt).iter(|| async {
            let _ = ctx_off.sql(&q_off).await.unwrap().collect().await.unwrap();
        });
    });
    g.finish();
}

criterion_group!(benches, bench_build, bench_query, bench_size_ratio, bench_e2e_scan);
criterion_main!(benches);
