use actix_web::{get, post, web, HttpResponse, Responder};
use chrono::DateTime;
use serde::Deserialize;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use crate::persistent_queue::{IngestRecord, PersistentQueue};
use crate::database::Database;
use tracing::error;

#[derive(Clone)]
pub struct IngestStatusStore {
    inner: Arc<RwLock<HashMap<String, String>>>,
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
            eprintln!("Failed to acquire lock in set_status");
        }
    }

    pub fn get_status(&self, id: &str) -> Option<String> {
        self.inner.read().ok().and_then(|map| map.get(id).cloned())
    }
}

#[derive(Deserialize)]
pub struct IngestData {
    pub project_id: String,
    pub id: Option<String>, // Optional, defaults to gen_random_uuid()
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
    pub path_params: Option<String>, // JSONB stored as String
    pub query_params: Option<String>, // JSONB
    pub request_headers: Option<String>, // JSONB
    pub response_headers: Option<String>, // JSONB
    pub request_body: Option<String>, // JSONB
    pub response_body: Option<String>, // JSONB
    pub endpoint_hash: String,
    pub shape_hash: String,
    pub format_hashes: Vec<String>,
    pub field_hashes: Vec<String>,
    pub sdk_type: String,
    pub service_version: Option<String>,
    pub attributes: Option<String>, // JSONB
    pub events: Option<String>, // JSONB
    pub links: Option<String>, // JSONB
    pub resource: Option<String>, // JSONB
    pub instrumentation_scope: Option<String>, // JSONB
    pub errors: Option<String>, // JSONB
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
            HttpResponse::Ok().body(format!("Record enqueued. Receipt: {}", receipt))
        }
        Err(e) => {
            error!("Error enqueuing record: {:?}", e);
            HttpResponse::InternalServerError().body("Error enqueuing record")
        }
    }
}

#[get("/ingest/status/{id}")]
pub async fn get_status(path: web::Path<String>, status_store: web::Data<Arc<IngestStatusStore>>) -> impl Responder {
    let id = path.into_inner();
    if let Some(status) = status_store.get_status(&id) {
        HttpResponse::Ok().body(status)
    } else {
        HttpResponse::NotFound().body("Record ID not found")
    }
}