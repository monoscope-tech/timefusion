use std::{path::PathBuf, sync::Arc};

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::execution::context::SessionContext;
use timefusion::{
    config::AppConfig,
    database::Database,
    support::test_helpers::{json_to_batch, test_span},
};

fn bench_config(name: &str) -> Arc<AppConfig> {
    let id = uuid::Uuid::new_v4().to_string()[..8].to_owned();
    let mut cfg = AppConfig::default();
    cfg.cache.timefusion_foyer_disabled = true;
    cfg.core.timefusion_table_prefix = format!("bench-{name}-{id}");
    cfg.core.timefusion_data_dir = PathBuf::from(format!("/tmp/timefusion-bench-{name}-{id}"));
    Arc::new(cfg)
}

fn minio_config(name: &str) -> Arc<AppConfig> {
    let mut cfg = (*bench_config(name)).clone();
    cfg.aws.aws_s3_bucket = Some("timefusion-tests".to_string());
    cfg.aws.aws_access_key_id = Some("minioadmin".to_string());
    cfg.aws.aws_secret_access_key = Some("minioadmin".to_string());
    cfg.aws.aws_s3_endpoint = "http://127.0.0.1:9000".to_string();
    cfg.aws.aws_default_region = Some("us-east-1".to_string());
    cfg.aws.aws_allow_http = Some("true".to_string());
    Arc::new(cfg)
}

fn minio_flush_config(name: &str) -> Arc<AppConfig> {
    let mut cfg = (*minio_config(name)).clone();
    cfg.buffer.timefusion_flush_immediately = true;
    Arc::new(cfg)
}

fn is_minio_available() -> bool {
    std::net::TcpStream::connect("127.0.0.1:9000").is_ok()
}

async fn setup_write_bench(name: &str) -> (SessionContext, Arc<Database>, String) {
    let cfg = bench_config(name);
    unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };
    let layer = Arc::new(timefusion::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap());
    let db = Arc::new(Database::with_config(cfg).await.unwrap().with_buffered_layer(layer));
    let mut ctx = db.clone().create_session_context();
    db.setup_session_context(&mut ctx).unwrap();
    let pid = format!("bench_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    (ctx, db, pid)
}

async fn setup_read_bench(name: &str, pre_insert: usize) -> (SessionContext, Arc<Database>, String) {
    let cfg = minio_config(name);
    unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };
    let layer = Arc::new(timefusion::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap());
    let db = Arc::new(Database::with_config(cfg).await.unwrap().with_buffered_layer(layer));
    let mut ctx = db.clone().create_session_context();
    db.setup_session_context(&mut ctx).unwrap();

    let pid = format!("bench_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    for i in 0..pre_insert {
        let batch = json_to_batch(vec![test_span(&format!("id_{i}"), &format!("span_{i}"), &pid)]).unwrap();
        db.insert_records_batch(&pid, "otel_logs_and_spans", vec![batch], false, None).await.unwrap();
    }
    (ctx, db, pid)
}

async fn setup_s3_bench(name: &str) -> (SessionContext, Arc<Database>, String) {
    let cfg = minio_flush_config(name);
    unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

    let db_for_cb = Database::with_config(Arc::clone(&cfg)).await.unwrap();
    let db_clone = db_for_cb.clone();
    let delta_cb: timefusion::write::DeltaWriteCallback = Arc::new(move |project_id, table_name, batches, _watermark| {
        let db = db_clone.clone();
        Box::pin(async move {
            db.insert_records_batch(&project_id, &table_name, batches, true, None).await?;
            Ok(Vec::new())
        })
    });
    let layer = Arc::new(timefusion::support::test_helpers::test_layer(cfg).unwrap().with_delta_writer(delta_cb));
    let db = db_for_cb.with_buffered_layer(layer);

    let pid = format!("bench_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    db.get_or_create_table(&pid, "otel_logs_and_spans").await.unwrap();

    let db = Arc::new(db);
    let mut ctx = db.clone().create_session_context();
    db.setup_session_context(&mut ctx).unwrap();
    (ctx, db, pid)
}

macro_rules! bench_sql {
    ($group:expr, $rt:expr, $name:expr, $ctx:expr, $sql:expr) => {
        $group.bench_function($name, |b| {
            let (ctx, sql) = ($ctx.clone(), $sql.clone());
            b.to_async($rt).iter(|| {
                let (ctx, sql) = (ctx.clone(), sql.clone());
                async move { ctx.sql(&sql).await.unwrap().collect().await.unwrap() }
            })
        });
    };
}

fn now_ts() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string()
}

fn today() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

fn insert_sql(project_id: &str, n: usize) -> String {
    let date = today();
    let values: Vec<_> = (0..n)
        .map(|i| {
            let ts = now_ts();
            format!("('{}', '{}', TIMESTAMP '{}', 'id_{i}', 'bench_span', 'INFO', ARRAY[]::varchar[], ARRAY['summary'])", project_id, date, ts)
        })
        .collect();
    format!("INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, level, hashes, summary) VALUES {}", values.join(", "))
}

fn bench_inmemory_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("inmemory_write");

    let (ctx, _db, pid) = rt.block_on(setup_write_bench("w1"));
    bench_sql!(group, &rt, "sql_insert_1_row", ctx, insert_sql(&pid, 1));

    let (ctx, _db, pid) = rt.block_on(setup_write_bench("w100"));
    bench_sql!(group, &rt, "sql_insert_100_rows", ctx, insert_sql(&pid, 100));

    {
        let cfg = bench_config("wapi");
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };
        let layer = rt.block_on(async { Arc::new(timefusion::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap()) });
        let db = rt.block_on(async { Arc::new(Database::with_config(cfg).await.unwrap().with_buffered_layer(layer)) });
        let pid = format!("bench_{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let batches: Vec<_> = (0..10).map(|i| json_to_batch(vec![test_span(&format!("id_{i}"), "span", &pid)]).unwrap()).collect();
        group.bench_function("batch_api_insert_10_rows", |b| {
            let (db, pid, batches) = (db.clone(), pid.clone(), batches.clone());
            b.to_async(&rt).iter(|| {
                let (db, pid, batches) = (db.clone(), pid.clone(), batches.clone());
                async move { db.insert_records_batch(&pid, "otel_logs_and_spans", batches, false, None).await.unwrap() }
            })
        });
    }

    {
        let (ctx, _db, pid) = rt.block_on(setup_write_bench("wconc"));
        let sqls: Vec<_> = (0..4).map(|_| insert_sql(&pid, 1)).collect();
        group.bench_function("sql_insert_concurrent_4", |b| {
            b.to_async(&rt).iter(|| {
                let (ctx, sqls) = (ctx.clone(), sqls.clone());
                async move {
                    futures::future::join_all(sqls.iter().map(|s| {
                        let (ctx, s) = (ctx.clone(), s.clone());
                        async move { ctx.sql(&s).await.unwrap().collect().await.unwrap() }
                    }))
                    .await;
                }
            })
        });
    }

    group.finish();
}

fn bench_reads(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("read");

    if !is_minio_available() {
        eprintln!("MinIO not available at 127.0.0.1:9000, skipping read benchmarks");
        group.finish();
        return;
    }

    let (ctx, _db, pid) = rt.block_on(setup_read_bench("read", 1000));

    bench_sql!(group, &rt, "sql_select_count", ctx, format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{pid}'"));
    bench_sql!(group, &rt, "sql_select_filter_level", ctx, format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{pid}' AND level = 'ERROR'"));
    bench_sql!(
        group,
        &rt,
        "sql_select_time_range",
        ctx,
        format!("SELECT id, name, timestamp FROM otel_logs_and_spans WHERE project_id = '{pid}' AND timestamp <= TIMESTAMP '{}' LIMIT 100", now_ts())
    );
    bench_sql!(
        group,
        &rt,
        "sql_select_aggregation",
        ctx,
        format!("SELECT level, COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{pid}' GROUP BY level")
    );

    group.finish();
}

fn bench_s3_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("s3_write");
    group.sample_size(10);

    if !is_minio_available() {
        eprintln!("MinIO not available at 127.0.0.1:9000, skipping S3 write benchmarks");
        group.finish();
        return;
    }

    let (ctx, _db, pid) = rt.block_on(setup_s3_bench("s3w"));
    let sql = insert_sql(&pid, 100);
    bench_sql!(group, &rt, "s3_insert_and_flush_100", ctx, sql);

    group.finish();
}

fn bench_s3_reads(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("s3_read");
    group.sample_size(10);

    if !is_minio_available() {
        eprintln!("MinIO not available at 127.0.0.1:9000, skipping S3 read benchmarks");
        group.finish();
        return;
    }

    let (ctx, _db, pid) = rt.block_on(setup_s3_bench("s3r"));

    let insert = insert_sql(&pid, 100);
    rt.block_on(async { ctx.sql(&insert).await.unwrap().collect().await.unwrap() });

    bench_sql!(group, &rt, "s3_select_count", ctx, format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{pid}'"));
    bench_sql!(group, &rt, "s3_select_filter", ctx, format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{pid}' AND level = 'INFO'"));
    bench_sql!(
        group,
        &rt,
        "s3_select_time_range",
        ctx,
        format!("SELECT id, name, timestamp FROM otel_logs_and_spans WHERE project_id = '{pid}' AND timestamp <= TIMESTAMP '{}' LIMIT 100", now_ts())
    );

    group.finish();
}

criterion_group!(benches, bench_inmemory_writes, bench_reads, bench_s3_writes, bench_s3_reads);
criterion_main!(benches);
