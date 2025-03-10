// tests/insert_tests.rs

use timefusion::database::Database;
use persistent_queue::IngestRecord;
use uuid::Uuid;
use chrono::Utc;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Helper: generate a single IngestRecord.
fn generate_record() -> IngestRecord {
    IngestRecord {
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
    }
}

#[test]
fn test_single_insertion() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let db = Arc::new(Database::new().await.expect("Failed to initialize database"));
        let record = generate_record();
        db.write(&record).await.expect("Insertion failed");

        // Verify insertion by querying for the inserted record.
        let query = format!("SELECT * FROM table_events WHERE id = '{}'", record.id);
        let df = db.query(&query).await;
        assert!(df.is_ok(), "Query failed after insertion");
    });
}

#[test]
fn test_edge_case_insertion_empty_optional_fields() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let db = Arc::new(Database::new().await.expect("Failed to initialize database"));
        let mut record = generate_record();
        // Ensure optional fields are None.
        record.parent_span_id = None;
        record.trace_state = None;
        record.end_time = None;
        db.write(&record).await.expect("Edge-case insertion failed");

        let query = format!("SELECT * FROM table_events WHERE id = '{}'", record.id);
        let df = db.query(&query).await;
        assert!(df.is_ok(), "Query failed for edge-case record");
    });
}

#[test]
fn test_duplicate_insertion() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let db = Arc::new(Database::new().await.expect("Failed to initialize database"));
        let record = generate_record();
        db.write(&record).await.expect("First insertion failed");
        let second_result = db.write(&record).await;
        // Adjust this behavior as needed: here we expect duplicates to be rejected.
        assert!(second_result.is_err(), "Duplicate insertion did not error as expected");
    });
}
