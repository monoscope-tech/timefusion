// benches/benchmarks.rs

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use timefusion::database::Database;
use timefusion::persistent_queue::{PersistentQueue, IngestRecord};
use uuid::Uuid;
use tokio::runtime::Runtime;
use tempfile::tempdir;

fn bench_database_query(c: &mut Criterion) {
    // Create a Tokio runtime.
    let rt = Runtime::new().unwrap();
    // Create a dummy database instance.
    let db = rt.block_on(Database::new()).unwrap();

    c.bench_function("database query - SELECT 1", |b| {
        b.iter(|| {
            // Run a simple query.
            let df = rt.block_on(db.query("SELECT 1 AS test")).unwrap();
            let result = rt.block_on(df.collect()).unwrap();
            black_box(result);
        })
    });
}

fn bench_batch_ingestion(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let temp_dir = tempdir().unwrap();
    // Create a persistent queue using a temporary directory.
    let queue = PersistentQueue::new(temp_dir.path()).unwrap();
    let batch_size = 1_000;
    let mut records = Vec::with_capacity(batch_size);
    for _ in 0..batch_size {
        records.push(IngestRecord {
            table_name: "bench_table".to_string(),
            project_id: "bench_project".to_string(),
            id: Uuid::new_v4().to_string(),
            version: 1,
            event_type: "bench_event".to_string(),
            timestamp: "2025-03-11T12:00:00Z".to_string(),
            trace_id: "trace".to_string(),
            span_id: "span".to_string(),
            parent_span_id: None,
            trace_state: None,
            start_time: "2025-03-11T12:00:00Z".to_string(),
            end_time: Some("2025-03-11T12:00:01Z".to_string()),
            duration_ns: 1_000_000_000,
            span_name: "span_name".to_string(),
            span_kind: "client".to_string(),
            span_type: "bench".to_string(),
            status: None,
            status_code: 0,
            status_message: "OK".to_string(),
            severity_text: None,
            severity_number: 0,
            host: "localhost".to_string(),
            url_path: "/".to_string(),
            raw_url: "/".to_string(),
            method: "GET".to_string(),
            referer: "".to_string(),
            path_params: None,
            query_params: None,
            request_headers: None,
            response_headers: None,
            request_body: None,
            response_body: None,
            endpoint_hash: "hash".to_string(),
            shape_hash: "shape".to_string(),
            format_hashes: vec!["fmt".to_string()],
            field_hashes: vec!["field".to_string()],
            sdk_type: "rust".to_string(),
            service_version: None,
            attributes: None,
            events: None,
            links: None,
            resource: None,
            instrumentation_scope: None,
            errors: None,
            tags: vec!["tag".to_string()],
        });
    }

    c.bench_function("batch ingestion 1k records", |b| {
        b.iter(|| {
            // For each record, run the async enqueue function.
            for record in records.iter() {
                let res = rt.block_on(queue.enqueue(record)).unwrap();
                black_box(res);
            }
        })
    });
}

fn bench_insertion_range(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    // Define different sizes to test. Adjust these values as needed.
    let sizes = vec![1_000, 10_000, 100_000, 1_000_000];
    let mut group = c.benchmark_group("insertion range");
    group.sample_size(10);

    for &size in sizes.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            // Reinitialize the queue for each size measurement.
            let temp_dir = tempdir().unwrap();
            let queue = PersistentQueue::new(temp_dir.path()).unwrap();

            // Generate a batch of `size` records.
            let mut records = Vec::with_capacity(size);
            for _ in 0..size {
                records.push(IngestRecord {
                    table_name: "bench_table".to_string(),
                    project_id: "bench_project".to_string(),
                    id: Uuid::new_v4().to_string(),
                    version: 1,
                    event_type: "bench_event".to_string(),
                    timestamp: "2025-03-11T12:00:00Z".to_string(),
                    trace_id: "trace".to_string(),
                    span_id: "span".to_string(),
                    parent_span_id: None,
                    trace_state: None,
                    start_time: "2025-03-11T12:00:00Z".to_string(),
                    end_time: Some("2025-03-11T12:00:01Z".to_string()),
                    duration_ns: 1_000_000_000,
                    span_name: "span_name".to_string(),
                    span_kind: "client".to_string(),
                    span_type: "bench".to_string(),
                    status: None,
                    status_code: 0,
                    status_message: "OK".to_string(),
                    severity_text: None,
                    severity_number: 0,
                    host: "localhost".to_string(),
                    url_path: "/".to_string(),
                    raw_url: "/".to_string(),
                    method: "GET".to_string(),
                    referer: "".to_string(),
                    path_params: None,
                    query_params: None,
                    request_headers: None,
                    response_headers: None,
                    request_body: None,
                    response_body: None,
                    endpoint_hash: "hash".to_string(),
                    shape_hash: "shape".to_string(),
                    format_hashes: vec!["fmt".to_string()],
                    field_hashes: vec!["field".to_string()],
                    sdk_type: "rust".to_string(),
                    service_version: None,
                    attributes: None,
                    events: None,
                    links: None,
                    resource: None,
                    instrumentation_scope: None,
                    errors: None,
                    tags: vec!["tag".to_string()],
                });
            }

            b.iter(|| {
                for record in records.iter() {
                    let res = rt.block_on(queue.enqueue(record)).unwrap();
                    black_box(res);
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_database_query, bench_batch_ingestion, bench_insertion_range);
criterion_main!(benches);
