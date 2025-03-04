// src/ingest.rs
use actix_web::{get, post, web, HttpResponse, Responder};
use chrono::DateTime;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use crate::persistent_queue::{IngestRecord, PersistentQueue};
use crate::database::Database;
use tracing::{error, info};
use datafusion::arrow::record_batch::RecordBatch;

#[derive(Clone)]
pub struct IngestStatusStore {
    pub inner: Arc<RwLock<HashMap<String, String>>>,
}

impl IngestStatusStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn set_status(&self, id: String, status: String) {
        if let Ok(mut map) = self.inner.write() {
            map.insert(id, status);
        } else {
            error!("Failed to acquire lock in set_status");
        }
    }

    pub fn get_status(&self, id: &str) -> Option<String> {
        self.inner.read().ok().and_then(|map| map.get(id).cloned())
    }
}

#[derive(Deserialize)]
pub struct IngestData {
    pub project_id: String,
    pub id: Option<String>,
    pub timestamp: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub trace_state: Option<String>,
    pub start_time: String,
    pub end_time: Option<String>,
    pub duration_ns: i64,
    pub span_name: String,
    pub span_kind: String,
    pub span_type: String,
    pub status: Option<String>,
    pub status_code: i32,
    pub status_message: String,
    pub severity_text: Option<String>,
    pub severity_number: i32,
    pub host: String,
    pub url_path: String,
    pub raw_url: String,
    pub method: String,
    pub referer: String,
    pub path_params: Option<String>,
    pub query_params: Option<String>,
    pub request_headers: Option<String>,
    pub response_headers: Option<String>,
    pub request_body: Option<String>,
    pub response_body: Option<String>,
    pub endpoint_hash: String,
    pub shape_hash: String,
    pub format_hashes: Vec<String>,
    pub field_hashes: Vec<String>,
    pub sdk_type: String,
    pub service_version: Option<String>,
    pub attributes: Option<String>,
    pub events: Option<String>,
    pub links: Option<String>,
    pub resource: Option<String>,
    pub instrumentation_scope: Option<String>,
    pub errors: Option<String>,
    pub tags: Vec<String>,
}

#[post("/ingest")]
pub async fn ingest(
    data: web::Json<IngestData>,
    db: web::Data<Arc<Database>>,
    queue: web::Data<Arc<PersistentQueue>>,
    status_store: web::Data<Arc<IngestStatusStore>>,
) -> impl Responder {
    if !db.has_project(&data.project_id) {
        error!("Invalid project_id: {}", data.project_id);
        return HttpResponse::BadRequest().body("Invalid project_id");
    }
    if DateTime::parse_from_rfc3339(&data.timestamp).is_err() {
        error!("Invalid timestamp: {}", data.timestamp);
        return HttpResponse::BadRequest().body("Invalid timestamp format");
    }
    if DateTime::parse_from_rfc3339(&data.start_time).is_err() {
        error!("Invalid start_time: {}", data.start_time);
        return HttpResponse::BadRequest().body("Invalid start_time format");
    }
    if let Some(end_time) = &data.end_time {
        if DateTime::parse_from_rfc3339(end_time).is_err() {
            error!("Invalid end_time: {}", end_time);
            return HttpResponse::BadRequest().body("Invalid end_time format");
        }
    }
    let record = IngestRecord {
        project_id: data.project_id.clone(),
        id: data.id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        timestamp: data.timestamp.clone(),
        trace_id: data.trace_id.clone(),
        span_id: data.span_id.clone(),
        parent_span_id: data.parent_span_id.clone(),
        trace_state: data.trace_state.clone(),
        start_time: data.start_time.clone(),
        end_time: data.end_time.clone(),
        duration_ns: data.duration_ns,
        span_name: data.span_name.clone(),
        span_kind: data.span_kind.clone(),
        span_type: data.span_type.clone(),
        status: data.status.clone(),
        status_code: data.status_code,
        status_message: data.status_message.clone(),
        severity_text: data.severity_text.clone(),
        severity_number: data.severity_number,
        host: data.host.clone(),
        url_path: data.url_path.clone(),
        raw_url: data.raw_url.clone(),
        method: data.method.clone(),
        referer: data.referer.clone(),
        path_params: data.path_params.clone(),
        query_params: data.query_params.clone(),
        request_headers: data.request_headers.clone(),
        response_headers: data.response_headers.clone(),
        request_body: data.request_body.clone(),
        response_body: data.response_body.clone(),
        endpoint_hash: data.endpoint_hash.clone(),
        shape_hash: data.shape_hash.clone(),
        format_hashes: data.format_hashes.clone(),
        field_hashes: data.field_hashes.clone(),
        sdk_type: data.sdk_type.clone(),
        service_version: data.service_version.clone(),
        attributes: data.attributes.clone(),
        events: data.events.clone(),
        links: data.links.clone(),
        resource: data.resource.clone(),
        instrumentation_scope: data.instrumentation_scope.clone(),
        errors: data.errors.clone(),
        tags: data.tags.clone(),
    };
    match queue.enqueue(&record).await {
        Ok(receipt) => {
            status_store.set_status(receipt.clone(), "Enqueued".to_string());
            info!("Record enqueued with receipt: {}", receipt);
            HttpResponse::Ok().body(format!("Record enqueued. Receipt: {}", receipt))
        }
        Err(e) => {
            error!("Error enqueuing record: {:?}", e);
            HttpResponse::InternalServerError().body("Error enqueuing record")
        }
    }
}

#[post("/ingest/batch")]
pub async fn ingest_batch(
    data: web::Json<Vec<IngestData>>,
    db: web::Data<Arc<Database>>,
    queue: web::Data<Arc<PersistentQueue>>,
    status_store: web::Data<Arc<IngestStatusStore>>,
) -> impl Responder {
    let mut results = Vec::new();
    for (idx, rec) in data.iter().enumerate() {
        if !db.has_project(&rec.project_id) {
            results.push(json!({"index": idx, "error": "Invalid project_id"}));
            continue;
        }
        if DateTime::parse_from_rfc3339(&rec.timestamp).is_err() {
            results.push(json!({"index": idx, "error": "Invalid timestamp format"}));
            continue;
        }
        if DateTime::parse_from_rfc3339(&rec.start_time).is_err() {
            results.push(json!({"index": idx, "error": "Invalid start_time format"}));
            continue;
        }
        if let Some(end_time) = &rec.end_time {
            if DateTime::parse_from_rfc3339(end_time).is_err() {
                results.push(json!({"index": idx, "error": "Invalid end_time format"}));
                continue;
            }
        }
        let record = IngestRecord {
            project_id: rec.project_id.clone(),
            id: rec.id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            timestamp: rec.timestamp.clone(),
            trace_id: rec.trace_id.clone(),
            span_id: rec.span_id.clone(),
            parent_span_id: rec.parent_span_id.clone(),
            trace_state: rec.trace_state.clone(),
            start_time: rec.start_time.clone(),
            end_time: rec.end_time.clone(),
            duration_ns: rec.duration_ns,
            span_name: rec.span_name.clone(),
            span_kind: rec.span_kind.clone(),
            span_type: rec.span_type.clone(),
            status: rec.status.clone(),
            status_code: rec.status_code,
            status_message: rec.status_message.clone(),
            severity_text: rec.severity_text.clone(),
            severity_number: rec.severity_number,
            host: rec.host.clone(),
            url_path: rec.url_path.clone(),
            raw_url: rec.raw_url.clone(),
            method: rec.method.clone(),
            referer: rec.referer.clone(),
            path_params: rec.path_params.clone(),
            query_params: rec.query_params.clone(),
            request_headers: rec.request_headers.clone(),
            response_headers: rec.response_headers.clone(),
            request_body: rec.request_body.clone(),
            response_body: rec.response_body.clone(),
            endpoint_hash: rec.endpoint_hash.clone(),
            shape_hash: rec.shape_hash.clone(),
            format_hashes: rec.format_hashes.clone(),
            field_hashes: rec.field_hashes.clone(),
            sdk_type: rec.sdk_type.clone(),
            service_version: rec.service_version.clone(),
            attributes: rec.attributes.clone(),
            events: rec.events.clone(),
            links: rec.links.clone(),
            resource: rec.resource.clone(),
            instrumentation_scope: rec.instrumentation_scope.clone(),
            errors: rec.errors.clone(),
            tags: rec.tags.clone(),
        };
        match queue.enqueue(&record).await {
            Ok(receipt) => {
                status_store.set_status(receipt.clone(), "Enqueued".to_string());
                results.push(json!({"index": idx, "receipt": receipt}));
            }
            Err(e) => {
                error!("Error enqueuing record at index {}: {:?}", idx, e);
                results.push(json!({"index": idx, "error": format!("{:?}", e)}));
            }
        }
    }
    HttpResponse::Ok().json(results)
}

#[get("/ingest/status/{id}")]
pub async fn get_status(
    path: web::Path<String>,
    status_store: web::Data<Arc<IngestStatusStore>>,
) -> impl Responder {
    let id = path.into_inner();
    if let Some(status) = status_store.get_status(&id) {
        info!("Status for ID {}: {}", id, status);
        HttpResponse::Ok().body(status)
    } else {
        error!("Record ID not found: {}", id);
        HttpResponse::NotFound().body("Record ID not found")
    }
}

pub fn record_batches_to_json_rows(batches: &[RecordBatch]) -> serde_json::Result<Vec<Value>> {
    let mut results = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();
        for row in 0..num_rows {
            let mut map = serde_json::Map::new();
            for col in 0..num_cols {
                let field = schema.field(col);
                let value = crate::utils::value_to_string(batch.column(col).as_ref(), row);
                map.insert(field.name().clone(), Value::String(value));
            }
            results.push(Value::Object(map));
        }
    }
    Ok(results)
}

#[get("/data")]
pub async fn get_all_data(
    db: web::Data<Arc<Database>>,
    query: web::Query<HashMap<String, String>>,
) -> impl Responder {
    if let Some(project_id) = query.get("project_id") {
        if !db.has_project(project_id) {
            error!("Invalid project_id: {}", project_id);
            return HttpResponse::BadRequest().body("Invalid project_id");
        }
        let mut sql = format!("SELECT * FROM table_events WHERE project_id = '{}'", project_id);
        if let Some(limit_str) = query.get("limit") {
            if let Ok(limit) = limit_str.parse::<u32>() {
                sql.push_str(&format!(" LIMIT {}", limit));
            }
        }
        if let Some(offset_str) = query.get("offset") {
            if let Ok(offset) = offset_str.parse::<u32>() {
                sql.push_str(&format!(" OFFSET {}", offset));
            }
        }
        info!("Executing query: {}", sql);
        match db.query(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    let batches: Vec<RecordBatch> = batches;
                    match record_batches_to_json_rows(&batches) {
                        Ok(rows) => HttpResponse::Ok().json(rows),
                        Err(e) => {
                            error!("JSON conversion error: {:?}", e);
                            HttpResponse::InternalServerError().body("Error converting data to JSON")
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to collect DataFrame: {:?}", e);
                    HttpResponse::InternalServerError().body("Error collecting data")
                }
            },
            Err(e) => {
                error!("Query error: {:?}", e);
                HttpResponse::InternalServerError().body("Error executing query")
            }
        }
    } else {
        error!("Missing project_id parameter");
        HttpResponse::BadRequest().body("Missing project_id parameter")
    }
}

#[get("/data/{id}")]
pub async fn get_data_by_id(
    db: web::Data<Arc<Database>>,
    path: web::Path<String>,
    query: web::Query<HashMap<String, String>>,
) -> impl Responder {
    let record_id = path.into_inner();
    if let Some(project_id) = query.get("project_id") {
        if !db.has_project(project_id) {
            error!("Invalid project_id: {}", project_id);
            return HttpResponse::BadRequest().body("Invalid project_id");
        }
        let sql = format!(
            "SELECT * FROM table_events WHERE project_id = '{}' AND id = '{}'",
            project_id, record_id
        );
        info!("Executing query: {}", sql);
        match db.query(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    let batches: Vec<RecordBatch> = batches;
                    match record_batches_to_json_rows(&batches) {
                        Ok(rows) => HttpResponse::Ok().json(rows),
                        Err(e) => {
                            error!("JSON conversion error: {:?}", e);
                            HttpResponse::InternalServerError().body("Error converting data to JSON")
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to collect DataFrame: {:?}", e);
                    HttpResponse::InternalServerError().body("Error collecting data")
                }
            },
            Err(e) => {
                error!("Query error: {:?}", e);
                HttpResponse::InternalServerError().body("Error executing query")
            }
        }
    } else {
        error!("Missing project_id parameter");
        HttpResponse::BadRequest().body("Missing project_id parameter")
    }
}