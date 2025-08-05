use criterion::{black_box, criterion_group, criterion_main, Criterion};
use timefusion::database::Database;
use timefusion::test_utils::test_helpers::*;
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tokio::runtime::Runtime;

async fn setup_benchmark_data() -> (Database, Vec<RecordBatch>) {
    // Setup test database
    unsafe {
        std::env::set_var("AWS_S3_BUCKET", "timefusion-bench");
        std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("bench-{}", uuid::Uuid::new_v4()));
    }
    
    let db = Database::new().await.unwrap();
    
    // Generate test data
    let mut batches = Vec::new();
    for i in 0..10 {
        let batch = json_to_batch(vec![test_span(
            &format!("id_{}", i),
            &format!("span_{}", i),
            "benchmark_project"
        )]).unwrap();
        batches.push(batch);
    }
    
    // Insert test data
    db.insert_records_batch("benchmark_project", "otel_logs_and_spans", batches.clone(), true).await.unwrap();
    
    (db, batches)
}

fn query_with_statistics(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _) = rt.block_on(setup_benchmark_data());
    let db = Arc::new(db);
    
    c.bench_function("query_with_optimized_statistics", |b| {
        b.to_async(&rt).iter(|| {
            let db = db.clone();
            async move {
                let ctx = db.create_session_context();
                datafusion::functions_json::register_all(&mut ctx).unwrap();
                db.setup_session_context(&ctx).unwrap();
                
                // Execute a query that benefits from statistics
                let result = ctx.sql(
                    "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'benchmark_project' AND timestamp > now() - interval '1 hour'"
                ).await.unwrap().collect().await.unwrap();
                
                black_box(result);
            }
        });
    });
}

fn query_with_physical_optimization(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _) = rt.block_on(setup_benchmark_data());
    let db = Arc::new(db);
    
    c.bench_function("query_with_physical_optimizers", |b| {
        b.to_async(&rt).iter(|| {
            let db = db.clone();
            async move {
                let ctx = db.create_session_context();
                datafusion::functions_json::register_all(&mut ctx).unwrap();
                db.setup_session_context(&ctx).unwrap();
                
                // Execute a time-bucketed aggregation that benefits from physical optimizers
                let result = ctx.sql(
                    "SELECT date_trunc('minute', timestamp) as minute, COUNT(*) 
                     FROM otel_logs_and_spans 
                     WHERE project_id = 'benchmark_project' 
                     GROUP BY minute 
                     ORDER BY minute"
                ).await.unwrap().collect().await.unwrap();
                
                black_box(result);
            }
        });
    });
}

criterion_group!(benches, query_with_statistics, query_with_physical_optimization);
criterion_main!(benches);