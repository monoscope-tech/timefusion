// benches/insert_bench.rs

use criterion::{criterion_group, criterion_main, Criterion};
use criterion::async_executor::AsyncExecutor;
use timefusion::database::Database;
use timefusion::persistent_queue::IngestRecord;
use uuid::Uuid;
use chrono::Utc;
use std::sync::Arc;

/// A custom Tokio executor that implements Criterion's AsyncExecutor trait.
#[derive(Clone)]
struct TokioExecutor(Arc<tokio::runtime::Runtime>);

impl TokioExecutor {
    fn new() -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
        TokioExecutor(Arc::new(runtime))
    }
}

impl AsyncExecutor for TokioExecutor {
    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        self.0.block_on(future)
    }
}

/// Generates a vector of dummy IngestRecord values.
fn generate_records(count: usize) -> Vec<IngestRecord> {
    (0..count)
        .map(|_| IngestRecord {
            project_id: "events".to_string(),
            id: Uuid::new_v4().to_string(),
            timestamp: Utc::now().to_rfc3339(),
            trace_id: Uuid::new_v4().to_string(),
            span_id: Uuid::new_v4().to_string(),
            parent_span_id: None,
            trace_state: None,
            start_time: Utc::now().to_rfc3339(),
            end_time: None,
            duration_ns: 1000,
            span_name: "test_span".to_string(),
            span_kind: "internal".to_string(),
            span_type: "test".to_string(),
            status: None,
            status_code: 0,
            status_message: "".to_string(),
            severity_text: None,
            severity_number: 0,
            host: "localhost".to_string(),
            url_path: "/test".to_string(),
            raw_url: "/test".to_string(),
            method: "GET".to_string(),
            referer: "".to_string(),
            path_params: None,
            query_params: None,
            request_headers: None,
            response_headers: None,
            request_body: None,
            response_body: None,
            endpoint_hash: "hash".to_string(),
            shape_hash: "hash".to_string(),
            format_hashes: vec![],
            field_hashes: vec![],
            sdk_type: "test".to_string(),
            service_version: None,
            attributes: None,
            events: None,
            links: None,
            resource: None,
            instrumentation_scope: None,
            errors: None,
            tags: vec![],
        })
        .collect()
}

/// Asynchronously initialize your Database.
async fn setup_database() -> Arc<Database> {
    Arc::new(Database::new().await.expect("Failed to initialize database"))
}

/// Benchmark insertion performance for varying record counts.
fn benchmark_insertion(c: &mut Criterion) {
    // Create our custom Tokio executor.
    let executor = TokioExecutor::new();
    // Initialize the database.
    let db = futures::executor::block_on(setup_database());

    // Define record counts.
    let sizes = vec![
        1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000, 10_000_000,
    ];

    for &size in &sizes {
        // Generate records once per benchmark.
        let records = generate_records(size);
        let bench_name = format!("Insertion of {} records", size);
        // Move the records into the closure.
        c.bench_function(&bench_name, move |b| {
            // Clone executor and db inside the closure so each iteration gets its own copy.
            let exec = executor.clone();
            let db_clone = db.clone();
            b.to_async(exec).iter(|| async {
                for record in &records {
                    let _ = db_clone.write(record).await;
                }
            });
        });
    }
}

criterion_group!(benches, benchmark_insertion);
criterion_main!(benches);
