// ingest.rs

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
use futures::future::join_all;

/// IngestStatusStore remains unchanged.
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

/// New ingestion data structure matching your new DB schema.
/// All records will be inserted into the fixed "telemetry.events" table.
#[derive(Deserialize)]
pub struct IngestData {
    pub projectId: String,
    pub id: Option<String>,
    pub timestamp: String,
    pub traceId: String,
    pub spanId: String,
    pub eventType: String,
    pub status: Option<String>,
    pub endTime: Option<String>,
    pub durationNs: i64,
    pub spanName: String,
    pub spanKind: Option<String>,
    pub parentSpanId: Option<String>,
    pub traceState: Option<String>,
    pub hasError: bool,
    pub severityText: Option<String>,
    pub severityNumber: i32,
    pub body: Option<String>,
    pub httpMethod: Option<String>,
    pub httpUrl: Option<String>,
    pub httpRoute: Option<String>,
    pub httpHost: Option<String>,
    pub httpStatusCode: Option<i32>,
    pub pathParams: Option<String>,
    pub queryParams: Option<String>,
    pub requestBody: Option<String>,
    pub responseBody: Option<String>,
    pub sdkType: String,
    pub serviceVersion: Option<String>,
    pub errors: Option<String>,
    pub tags: Vec<String>,
    pub parentId: Option<String>,
    pub dbSystem: Option<String>,
    pub dbName: Option<String>,
    pub dbStatement: Option<String>,
    pub dbOperation: Option<String>,
    pub rpcSystem: Option<String>,
    pub rpcService: Option<String>,
    pub rpcMethod: Option<String>,
    pub endpointHash: String,
    pub shapeHash: String,
    pub formatHashes: Vec<String>,
    pub fieldHashes: Vec<String>,
    pub attributes: Option<String>,
    pub events: Option<String>,
    pub links: Option<String>,
    pub resource: Option<String>,
    pub instrumentationScope: Option<String>,
}

/// Single record ingestion endpoint.
#[post("/ingest")]
pub async fn ingest(
    data: web::Json<IngestData>,
    _db: web::Data<Arc<Database>>, // Unused here (the DB is used later when flushing the queue)
    queue: web::Data<Arc<PersistentQueue>>,
    status_store: web::Data<Arc<IngestStatusStore>>,
) -> impl Responder {
    // Validate projectId is a valid UUID.
    if uuid::Uuid::parse_str(&data.projectId).is_err() {
        error!("Invalid projectId: {}", data.projectId);
        return HttpResponse::BadRequest().body("Invalid projectId");
    }
    // Validate timestamps.
    if DateTime::parse_from_rfc3339(&data.timestamp).is_err() {
        error!("Invalid timestamp: {}", data.timestamp);
        return HttpResponse::BadRequest().body("Invalid timestamp format");
    }
    if let Some(end_time) = &data.endTime {
        if DateTime::parse_from_rfc3339(end_time).is_err() {
            error!("Invalid endTime: {}", end_time);
            return HttpResponse::BadRequest().body("Invalid endTime format");
        }
    }
    // Map fields from IngestData into IngestRecord.
    let record = IngestRecord {
        projectId: data.projectId.clone(),
        id: data.id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        timestamp: data.timestamp.clone(),
        traceId: data.traceId.clone(),
        spanId: data.spanId.clone(),
        eventType: data.eventType.clone(),
        status: data.status.clone(),
        endTime: data.endTime.clone(),
        durationNs: data.durationNs,
        spanName: data.spanName.clone(),
        spanKind: data.spanKind.clone(),
        parentSpanId: data.parentSpanId.clone(),
        traceState: data.traceState.clone(),
        hasError: data.hasError,
        severityText: data.severityText.clone(),
        severityNumber: data.severityNumber,
        body: data.body.clone(),
        httpMethod: data.httpMethod.clone(),
        httpUrl: data.httpUrl.clone(),
        httpRoute: data.httpRoute.clone(),
        httpHost: data.httpHost.clone(),
        httpStatusCode: data.httpStatusCode,
        pathParams: data.pathParams.clone(),
        queryParams: data.queryParams.clone(),
        requestBody: data.requestBody.clone(),
        responseBody: data.responseBody.clone(),
        sdkType: data.sdkType.clone(),
        serviceVersion: data.serviceVersion.clone(),
        errors: data.errors.clone(),
        tags: data.tags.clone(),
        parentId: data.parentId.clone(),
        dbSystem: data.dbSystem.clone(),
        dbName: data.dbName.clone(),
        dbStatement: data.dbStatement.clone(),
        dbOperation: data.dbOperation.clone(),
        rpcSystem: data.rpcSystem.clone(),
        rpcService: data.rpcService.clone(),
        rpcMethod: data.rpcMethod.clone(),
        endpointHash: data.endpointHash.clone(),
        shapeHash: data.shapeHash.clone(),
        formatHashes: data.formatHashes.clone(),
        fieldHashes: data.fieldHashes.clone(),
        attributes: data.attributes.clone(),
        events: data.events.clone(),
        links: data.links.clone(),
        resource: data.resource.clone(),
        instrumentationScope: data.instrumentationScope.clone(),
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

/// Batch ingestion endpoint using concurrent processing.
#[post("/ingest/batch")]
pub async fn ingest_batch(
    data: web::Json<Vec<IngestData>>,
    _db: web::Data<Arc<Database>>,
    queue: web::Data<Arc<PersistentQueue>>,
    status_store: web::Data<Arc<IngestStatusStore>>,
) -> impl Responder {
    let tasks = data.iter().enumerate().map(|(idx, rec)| {
        let queue = queue.clone();
        let status_store = status_store.clone();
        async move {
            if uuid::Uuid::parse_str(&rec.projectId).is_err() {
                return json!({"index": idx, "error": "Invalid projectId"});
            }
            if DateTime::parse_from_rfc3339(&rec.timestamp).is_err() {
                return json!({"index": idx, "error": "Invalid timestamp format"});
            }
            if let Some(end_time) = &rec.endTime {
                if DateTime::parse_from_rfc3339(end_time).is_err() {
                    return json!({"index": idx, "error": "Invalid endTime format"});
                }
            }
            let record = IngestRecord {
                projectId: rec.projectId.clone(),
                id: rec.id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                timestamp: rec.timestamp.clone(),
                traceId: rec.traceId.clone(),
                spanId: rec.spanId.clone(),
                eventType: rec.eventType.clone(),
                status: rec.status.clone(),
                endTime: rec.endTime.clone(),
                durationNs: rec.durationNs,
                spanName: rec.spanName.clone(),
                spanKind: rec.spanKind.clone(),
                parentSpanId: rec.parentSpanId.clone(),
                traceState: rec.traceState.clone(),
                hasError: rec.hasError,
                severityText: rec.severityText.clone(),
                severityNumber: rec.severityNumber,
                body: rec.body.clone(),
                httpMethod: rec.httpMethod.clone(),
                httpUrl: rec.httpUrl.clone(),
                httpRoute: rec.httpRoute.clone(),
                httpHost: rec.httpHost.clone(),
                httpStatusCode: rec.httpStatusCode,
                pathParams: rec.pathParams.clone(),
                queryParams: rec.queryParams.clone(),
                requestBody: rec.requestBody.clone(),
                responseBody: rec.responseBody.clone(),
                sdkType: rec.sdkType.clone(),
                serviceVersion: rec.serviceVersion.clone(),
                errors: rec.errors.clone(),
                tags: rec.tags.clone(),
                parentId: rec.parentId.clone(),
                dbSystem: rec.dbSystem.clone(),
                dbName: rec.dbName.clone(),
                dbStatement: rec.dbStatement.clone(),
                dbOperation: rec.dbOperation.clone(),
                rpcSystem: rec.rpcSystem.clone(),
                rpcService: rec.rpcService.clone(),
                rpcMethod: rec.rpcMethod.clone(),
                endpointHash: rec.endpointHash.clone(),
                shapeHash: rec.shapeHash.clone(),
                formatHashes: rec.formatHashes.clone(),
                fieldHashes: rec.fieldHashes.clone(),
                attributes: rec.attributes.clone(),
                events: rec.events.clone(),
                links: rec.links.clone(),
                resource: rec.resource.clone(),
                instrumentationScope: rec.instrumentationScope.clone(),
            };
            match queue.enqueue(&record).await {
                Ok(receipt) => {
                    status_store.set_status(receipt.clone(), "Enqueued".to_string());
                    json!({"index": idx, "receipt": receipt})
                }
                Err(e) => {
                    error!("Error enqueuing record at index {}: {:?}", idx, e);
                    json!({"index": idx, "error": format!("{:?}", e)})
                }
            }
        }
    });
    let results: Vec<Value> = join_all(tasks).await;
    HttpResponse::Ok().json(results)
}

/// Retrieve the status of a record by its receipt.
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

/// Query all records for a given projectId from telemetry.events.
#[get("/data")]
pub async fn get_all_data(
    db: web::Data<Arc<Database>>,
    query: web::Query<HashMap<String, String>>,
) -> impl Responder {
    if let Some(project_id) = query.get("projectId") {
        let mut sql = format!("SELECT * FROM telemetry.events WHERE projectId = '{}'", project_id);
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
        error!("Missing projectId parameter");
        HttpResponse::BadRequest().body("Missing projectId parameter")
    }
}

/// Retrieve a single record by projectId and record id.
#[get("/data/{id}")]
pub async fn get_data_by_id(
    db: web::Data<Arc<Database>>,
    path: web::Path<String>,
    query: web::Query<HashMap<String, String>>,
) -> impl Responder {
    let record_id = path.into_inner();
    if let Some(project_id) = query.get("projectId") {
        let sql = format!(
            "SELECT * FROM telemetry.events WHERE projectId = '{}' AND id = '{}'",
            project_id, record_id
        );
        info!("Executing query: {}", sql);
        match db.query(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
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
        error!("Missing projectId parameter");
        HttpResponse::BadRequest().body("Missing projectId parameter")
    }
}

/// Convert record batches into a JSON array of objects.
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
