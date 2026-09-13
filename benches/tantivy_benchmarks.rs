//! Tantivy benchmarks: index-build throughput, packed index size, and query latency.

use std::{path::PathBuf, sync::Arc, time::Duration};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit},
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use serde_json::json;
use tantivy::{Term, query::TermQuery, schema::IndexRecordOption};
use timefusion::{
    config::{AppConfig, TantivyConfig},
    database::Database,
    schema::{FieldDef, SortingColumnDef, TableSchema, TantivyFieldConfig},
    support::test_helpers::json_to_batch,
    tantivy::{
        build_in_memory,
        search::{TantivyIndexService, TantivySearchService, query_index},
    },
    write::DeltaWriteCallback,
};

/// `tokenizer` present means the column is tantivy-indexed with it.
fn field(name: &str, data_type: &str, nullable: bool, tokenizer: Option<&str>) -> FieldDef {
    FieldDef {
        name: name.into(),
        data_type: data_type.into(),
        nullable,
        tantivy: tokenizer.map(|t| TantivyFieldConfig { indexed: true, tokenizer: Some(t.into()), ..Default::default() }),
        ..Default::default()
    }
}

fn table() -> TableSchema {
    TableSchema {
        rollups: vec![],
        table_name: "bench".into(),
        partitions: vec![],
        sorting_columns: vec![SortingColumnDef { name: "timestamp".into(), descending: false, nulls_first: false }],
        z_order_columns: vec![],
        time_column: None,
        dedup_keys: vec![],
        dedup_tiebreak: None,
        tombstone_column: None,
        version_append: false,
        // `bench_variant_build` retypes index 3, so "message" must stay last.
        fields: vec![
            field("timestamp", "Timestamp(Microsecond, Some(\"UTC\"))", false, None),
            field("id", "Utf8", false, None),
            field("level", "Utf8", true, Some("raw")),
            field("message", "Utf8", true, Some("default")),
        ],
    }
}

fn synthetic_batch(n: usize) -> RecordBatch {
    let levels = ["INFO", "WARN", "ERROR", "DEBUG", "TRACE"];
    let words = ["request", "completed", "panic", "shutdown", "timeout", "connection", "lost", "recovered"];
    let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from((0..n as i64).map(|i| 1_000_000 + i * 1000).collect::<Vec<_>>()).with_timezone("UTC"));
    let id: ArrayRef = Arc::new(StringArray::from((0..n).map(|i| format!("id-{i}")).collect::<Vec<_>>()));
    let level: ArrayRef = Arc::new(StringArray::from((0..n).map(|i| levels[i % levels.len()]).collect::<Vec<_>>()));
    let msg: ArrayRef = Arc::new(StringArray::from((0..n).map(|i| format!("{} {}", words[i % words.len()], words[(i + 3) % words.len()])).collect::<Vec<_>>()));
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
            bench.iter(|| drop(build_in_memory(&table, std::slice::from_ref(&b)).unwrap()));
        });
    }
    g.finish();
}

fn bench_variant_build(c: &mut Criterion) {
    use parquet_variant_compute::VariantArrayBuilder;
    use parquet_variant_json::JsonToVariant;
    use timefusion::tantivy::{MergeMode, build_and_pack};

    let rows = 100_000;
    let base = synthetic_batch(rows);
    let mut variants = VariantArrayBuilder::new(rows);
    for row in 0..rows {
        variants
            .append_json(&format!(r#"{{"http":{{"method":"GET","status":200}},"path":"/events/{row}","tags":["api","production"],"duration":12345}}"#))
            .unwrap();
    }
    let column: ArrayRef = Arc::new(arrow::array::StructArray::from(variants.build()));
    let schema = Arc::new(ArrowSchema::new(
        base.schema().fields().iter().take(3).cloned().chain([Arc::new(Field::new("message", column.data_type().clone(), true))]).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema, base.columns().iter().take(3).cloned().chain([column]).collect()).unwrap();
    let mut group = c.benchmark_group("tantivy_variant_full_build");
    group.sample_size(10);
    group.throughput(Throughput::Elements(rows as u64));
    for flatten in ["json", "kv"] {
        let mut table = table();
        table.fields[3].data_type = "Variant".into();
        table.fields[3].tantivy.as_mut().unwrap().flatten = Some(flatten.into());
        for merge in [MergeMode::Now, MergeMode::Deferred] {
            group.bench_function(format!("{flatten}/{merge:?}"), |bench| {
                bench.iter(|| build_and_pack(&table, std::slice::from_ref(&batch), 3, merge, &std::env::temp_dir()).unwrap());
            });
        }
    }
    group.finish();
}

fn bench_query(c: &mut Criterion) {
    let table = table();
    let b = synthetic_batch(100_000);
    let (idx, built, _) = build_in_memory(&table, std::slice::from_ref(&b)).unwrap();
    let level = built.user_fields.get("level").unwrap().field;
    c.bench_function("tantivy_query_term_100k", |bench| {
        bench.iter(|| {
            let q = TermQuery::new(Term::from_field_text(level, "ERROR"), IndexRecordOption::Basic);
            query_index(&idx, &q, None).unwrap();
        });
    });
}

fn bench_size_ratio(c: &mut Criterion) {
    let table = table();
    let n = 100_000usize;
    let b = synthetic_batch(n);
    let pack =
        || timefusion::tantivy::build_and_pack(&table, std::slice::from_ref(&b), 19, timefusion::tantivy::MergeMode::Now, &std::env::temp_dir()).unwrap();
    let (blob, stats) = pack();
    let bytes_per_row = blob.len() as f64 / stats.rows as f64;
    println!("tantivy index size: {} bytes for {} rows ({:.2} bytes/row)", blob.len(), stats.rows, bytes_per_row);
    c.bench_function("tantivy_pack_100k_zstd_19", |bench| {
        bench.iter(|| drop(pack()));
    });
}

// End-to-end scan bench (text_match prefilter ON vs OFF); skipped unless MinIO is reachable.

fn make_app_cfg(test_id: &str) -> Arc<AppConfig> {
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
    c.tantivy = TantivyConfig { timefusion_tantivy_compression_level: 3, ..Default::default() };
    Arc::new(c)
}

async fn setup_bench_db(test_id: &str, tantivy_enabled: bool, rows: usize) -> Option<(Database, datafusion::execution::context::SessionContext, String)> {
    let cfg_arc = make_app_cfg(test_id);
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
    let mut layer = timefusion::support::test_helpers::test_layer(cfg_arc.clone()).ok()?.with_delta_writer(delta_cb);
    if tantivy_enabled {
        let storage_uri = format!("s3://{}/{}/tantivy", cfg_arc.aws.aws_s3_bucket.clone().unwrap(), cfg_arc.core.timefusion_table_prefix);
        let obj_store = db.create_object_store(&storage_uri, &cfg_arc.aws.build_storage_options(None)).await.ok()?;
        let s = Arc::new(TantivyIndexService::new(
            obj_store.clone(),
            Arc::new(cfg_arc.tantivy.clone()),
            std::env::temp_dir().join(format!("tf-scratch-{}", uuid::Uuid::new_v4())),
        ));
        layer = layer.with_tantivy_indexer(timefusion::server::tantivy_index_callback(&db, Arc::clone(&s)));
        let search = Arc::new(TantivySearchService::new(obj_store, cfg_arc.core.timefusion_data_dir.clone(), Arc::new(cfg_arc.tantivy.clone())));
        s.with_reader(&search);
        db = db.with_tantivy_search(search).with_tantivy_indexer(s);
    }
    db = db.with_buffered_layer(Arc::new(layer));

    let db_arc = Arc::new(db.clone());
    let mut ctx = db_arc.create_session_context();
    datafusion_functions_json::register_all(&mut ctx).ok()?;
    db.setup_session_context(&mut ctx).ok()?;

    // One word in five matches "panic", keeping the query highly selective.
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

    // `_db` must stay alive for the whole bench: dropping the Database tears down
    // the layer the context queries through.
    let arms = [("scan_10k_with_prefilter", true), ("scan_10k_without_prefilter", false)].map(|(label, tantivy_enabled)| {
        let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let (db, ctx, project) = rt.block_on(setup_bench_db(&id, tantivy_enabled, 10_000)).unwrap_or_else(|| panic!("setup {label}"));
        let sql = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id='{project}' AND text_match(status_message, 'panic')");
        (label, db, ctx, sql)
    });

    let mut g = c.benchmark_group("tantivy_scan_e2e");
    g.measurement_time(Duration::from_secs(15));
    for (label, _db, ctx, sql) in &arms {
        g.bench_function(*label, |b| {
            b.to_async(&rt).iter(|| async {
                ctx.sql(sql).await.unwrap().collect().await.unwrap();
            });
        });
    }
    g.finish();
}

criterion_group!(benches, bench_build, bench_variant_build, bench_query, bench_size_ratio, bench_e2e_scan);
criterion_main!(benches);
