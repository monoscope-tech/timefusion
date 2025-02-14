use actix_web::{get, post, web, HttpResponse, Responder};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::persistent_queue::{IngestRecord, PersistentQueue};
use crate::database::Database;

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
        let mut map = self.inner.write().unwrap();
        map.insert(id, status);
    }

    pub fn get_status(&self, id: &str) -> Option<String> {
        let map = self.inner.read().unwrap();
        map.get(id).cloned()
    }
}

#[derive(Deserialize)]
pub struct IngestData {
    pub project_id: String,
    pub timestamp: String,  // RFC3339 format
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub payload: Option<String>,
}

#[post("/ingest")]
pub async fn ingest(
    data: web::Json<IngestData>,
    _db: web::Data<Arc<Database>>,
    queue: web::Data<Arc<PersistentQueue>>,
) -> impl Responder {
    // Validate the timestamp.
    if DateTime::parse_from_rfc3339(&data.timestamp).is_err() {
        return HttpResponse::BadRequest().body("Invalid timestamp format");
    }
    let record = IngestRecord {
        project_id: data.project_id.clone(),
        timestamp: data.timestamp.clone(),
        start_time: data.start_time.clone(),
        end_time: data.end_time.clone(),
        payload: data.payload.clone(),
    };
    match queue.enqueue(&record).await {
        Ok(receipt) => HttpResponse::Ok().body(format!("Record enqueued. Receipt: {}", receipt)),
        Err(e) => HttpResponse::InternalServerError().body(format!("Error enqueuing record: {}", e)),
    }
}

#[get("/ingest/status/{id}")]
pub async fn get_status(path: web::Path<String>, status_store: web::Data<IngestStatusStore>) -> impl Responder {
    let id = path.into_inner();
    if let Some(status) = status_store.get_status(&id) {
        HttpResponse::Ok().body(status)
    } else {
        HttpResponse::NotFound().body("Record ID not found")
    }
}
