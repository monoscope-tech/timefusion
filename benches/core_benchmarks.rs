use std::{path::PathBuf, sync::Arc};

use criterion::{BenchmarkGroup, Criterion, criterion_group, criterion_main, measurement::WallTime};
use datafusion::execution::context::SessionContext;
use tokio::runtime::Runtime;

use timefusion::{
    config::AppConfig,
    database::Database,
    support::test_helpers::{json_to_batch, test_span},
};

/// Bench config with a unique prefix + data dir; `minio` points storage at local MinIO.
fn bench_config(name: &str, minio: bool, flush_immediately: bool) -> Arc<AppConfig> {
    let id = uuid::Uuid::new_v4().to_string()[..8].to_owned();
    let mut cfg = AppConfig::default();
    cfg.cache.timefusion_foyer_disabled = true;
    cfg.core.timefusion_table_prefix = format!("bench-{name}-{id}");
    cfg.core.timefusion_data_dir = PathBuf::from(format!("/tmp/timefusion-bench-{name}-{id}"));
    cfg.buffer.timefusion_flush_immediately = flush_immediately;
    if minio {
        cfg.aws.aws_s3_bucket = Some("timefusion-tests".to_string());
        cfg.aws.aws_access_key_id = Some("minioadmin".to_string());
        cfg.aws.aws_secret_access_key = Some("minioadmin".to_string());
        cfg.aws.aws_s3_endpoint = "http://127.0.0.1:9000".to_string();
        cfg.aws.aws_default_region = Some("us-east-1".to_string());
        cfg.aws.aws_allow_http = Some("true".to_string());
    }
    Arc::new(cfg)
}

/// Shared bench wiring. The `delta_writer` flush callback must close over the SAME
/// `Database` the layer is later bound to.
async fn setup(cfg: Arc<AppConfig>, delta_writer: bool) -> (SessionContext, Arc<Database>, String) {
    unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };
    let base = Database::with_config(Arc::clone(&cfg)).await.unwrap();
    let mut layer = timefusion::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
    if delta_writer {
        let db = base.clone();
        let cb: timefusion::write::DeltaWriteCallback = Arc::new(move |project_id, table_name, batches, _watermark| {
            let db = db.clone();
            Box::pin(async move {
                db.insert_records_batch(&project_id, &table_name, batches, true, None).await?;
                Ok(Vec::new())
            })
        });
        layer = layer.with_delta_writer(cb);
    }
    let db = Arc::new(base.with_buffered_layer(Arc::new(layer)));
    let mut ctx = db.clone().create_session_context();
    db.setup_session_context(&mut ctx).unwrap();
    (ctx, db, format!("bench_{}", &uuid::Uuid::new_v4().to_string()[..8]))
}

async fn setup_write_bench(name: &str) -> (SessionContext, Arc<Database>, String) {
    setup(bench_config(name, false, false), false).await
}

async fn setup_read_bench(name: &str, pre_insert: usize) -> (SessionContext, Arc<Database>, String) {
    let (ctx, db, pid) = setup(bench_config(name, true, false), false).await;
    for i in 0..pre_insert {
        let batch = json_to_batch(vec![test_span(&format!("id_{i}"), &format!("span_{i}"), &pid)]).unwrap();
        db.insert_records_batch(&pid, "otel_logs_and_spans", vec![batch], false, None).await.unwrap();
    }
    (ctx, db, pid)
}

async fn setup_s3_bench(name: &str) -> (SessionContext, Arc<Database>, String) {
    let (ctx, db, pid) = setup(bench_config(name, true, true), true).await;
    db.get_or_create_table(&pid, "otel_logs_and_spans").await.unwrap();
    (ctx, db, pid)
}

/// One runtime + one criterion group per bench fn. A group needing MinIO is skipped, not failed, when MinIO is unreachable.
fn bench_group(c: &mut Criterion, name: &str, sample_size: Option<usize>, needs_minio: bool, body: impl FnOnce(&Runtime, &mut BenchmarkGroup<'_, WallTime>)) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group(name);
    if let Some(n) = sample_size {
        group.sample_size(n);
    }
    if needs_minio && std::net::TcpStream::connect("127.0.0.1:9000").is_err() {
        eprintln!("MinIO not available at 127.0.0.1:9000, skipping {name} benchmarks");
    } else {
        body(&rt, &mut group);
    }
    group.finish();
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

fn insert_sql(project_id: &str, n: usize) -> String {
    let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let values = (0..n)
        .map(|i| format!("('{project_id}', '{date}', TIMESTAMP '{}', 'id_{i}', 'bench_span', 'INFO', ARRAY[]::varchar[], ARRAY['summary'])", now_ts()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, level, hashes, summary) VALUES {values}")
}

fn bench_inmemory_writes(c: &mut Criterion) {
    bench_group(c, "inmemory_write", None, false, |rt, group| {
        let (ctx, _db, pid) = rt.block_on(setup_write_bench("w1"));
        bench_sql!(group, rt, "sql_insert_1_row", ctx, insert_sql(&pid, 1));

        let (ctx, _db, pid) = rt.block_on(setup_write_bench("w100"));
        bench_sql!(group, rt, "sql_insert_100_rows", ctx, insert_sql(&pid, 100));

        let (_ctx, db, pid) = rt.block_on(setup_write_bench("wapi"));
        let batches: Vec<_> = (0..10).map(|i| json_to_batch(vec![test_span(&format!("id_{i}"), "span", &pid)]).unwrap()).collect();
        group.bench_function("batch_api_insert_10_rows", |b| {
            let (db, pid, batches) = (db.clone(), pid.clone(), batches.clone());
            b.to_async(rt).iter(|| {
                let (db, pid, batches) = (db.clone(), pid.clone(), batches.clone());
                async move { db.insert_records_batch(&pid, "otel_logs_and_spans", batches, false, None).await.unwrap() }
            })
        });

        let (ctx, _db, pid) = rt.block_on(setup_write_bench("wconc"));
        let sqls: Vec<_> = (0..4).map(|_| insert_sql(&pid, 1)).collect();
        group.bench_function("sql_insert_concurrent_4", |b| {
            b.to_async(rt).iter(|| {
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
    });
}

fn bench_reads(c: &mut Criterion) {
    bench_group(c, "read", None, true, |rt, group| {
        let (ctx, _db, pid) = rt.block_on(setup_read_bench("read", 1000));
        bench_sql!(group, rt, "sql_select_count", ctx, format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{pid}'"));
        bench_sql!(
            group,
            rt,
            "sql_select_filter_level",
            ctx,
            format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{pid}' AND level = 'ERROR'")
        );
        bench_sql!(
            group,
            rt,
            "sql_select_time_range",
            ctx,
            format!("SELECT id, name, timestamp FROM otel_logs_and_spans WHERE project_id = '{pid}' AND timestamp <= TIMESTAMP '{}' LIMIT 100", now_ts())
        );
        bench_sql!(
            group,
            rt,
            "sql_select_aggregation",
            ctx,
            format!("SELECT level, COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{pid}' GROUP BY level")
        );
    });
}

fn bench_s3_writes(c: &mut Criterion) {
    bench_group(c, "s3_write", Some(10), true, |rt, group| {
        let (ctx, _db, pid) = rt.block_on(setup_s3_bench("s3w"));
        bench_sql!(group, rt, "s3_insert_and_flush_100", ctx, insert_sql(&pid, 100));
    });
}

fn bench_s3_reads(c: &mut Criterion) {
    bench_group(c, "s3_read", Some(10), true, |rt, group| {
        let (ctx, _db, pid) = rt.block_on(setup_s3_bench("s3r"));
        let insert = insert_sql(&pid, 100);
        rt.block_on(async { ctx.sql(&insert).await.unwrap().collect().await.unwrap() });

        bench_sql!(group, rt, "s3_select_count", ctx, format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{pid}'"));
        bench_sql!(group, rt, "s3_select_filter", ctx, format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{pid}' AND level = 'INFO'"));
        bench_sql!(
            group,
            rt,
            "s3_select_time_range",
            ctx,
            format!("SELECT id, name, timestamp FROM otel_logs_and_spans WHERE project_id = '{pid}' AND timestamp <= TIMESTAMP '{}' LIMIT 100", now_ts())
        );
    });
}

criterion_group!(benches, bench_inmemory_writes, bench_reads, bench_s3_writes, bench_s3_reads);
criterion_main!(benches);
