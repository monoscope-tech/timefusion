use std::collections::HashMap;
use std::sync::Arc;

use actix_web::{get, web, HttpResponse, Responder};
use anyhow::Result;
use serde_json::Value;
use tracing::error;

use crate::database::Database;

#[derive(Clone)]
pub struct IngestStatusStore {
    pub inner: Arc<std::sync::RwLock<Vec<(String, String)>>>,
}

impl IngestStatusStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(std::sync::RwLock::new(Vec::new())),
        }
    }

    pub fn set_status(&self, id: String, status: String) {
        let mut inner = self.inner.write().unwrap();
        if let Some(entry) = inner.iter_mut().find(|(existing_id, _)| existing_id == &id) {
            entry.1 = status;
        } else {
            inner.push((id, status));
        }
    }

    pub fn get_status(&self, id: &str) -> Option<String> {
        let inner = self.inner.read().unwrap();
        inner
            .iter()
            .find(|(existing_id, _)| existing_id == id)
            .map(|(_, status)| status.clone())
    }
}

#[get("/status/{id}")]
pub async fn get_status(
    path: web::Path<String>,
    status_store: web::Data<Arc<IngestStatusStore>>,
) -> impl Responder {
    let id = path.into_inner();
    match status_store.get_status(&id) {
        Some(status) => HttpResponse::Ok().json(serde_json::json!({ "id": id, "status": status })),
        None => HttpResponse::NotFound()
            .json(serde_json::json!({ "error": "Status not found" })),
    }
}

#[get("/data")]
pub async fn get_all_data(
    db: web::Data<Arc<Database>>,
    query: web::Query<HashMap<String, String>>,
) -> impl Responder {
    let project_id = query.get("project_id").unwrap_or(&"default".to_string()).clone();
    let query_str = "SELECT * FROM otel_logs_and_spans ORDER BY startTimeUnixNano DESC LIMIT 100";

    let df = match db.query(&project_id, query_str).await {
        Ok(df) => df,
        Err(e) => {
            error!("Failed to query data for project '{}': {:?}", project_id, e);
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to query data"
            }));
        }
    };

    let batches: Vec<datafusion::arrow::record_batch::RecordBatch> = match df.collect().await {
        Ok(batches) => batches,
        Err(e) => {
            error!("Failed to collect data for project '{}': {:?}", project_id, e);
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to fetch data"
            }));
        }
    };

    let rows = record_batches_to_json_rows(&batches).unwrap_or_default();
    HttpResponse::Ok().json(rows)
}

#[get("/data/{id}")]
pub async fn get_data_by_id(
    path: web::Path<String>,
    db: web::Data<Arc<Database>>,
    query: web::Query<HashMap<String, String>>,
) -> impl Responder {
    let id = path.into_inner();
    let project_id = query.get("project_id").unwrap_or(&"default".to_string()).clone();
    let query = format!("SELECT * FROM otel_logs_and_spans WHERE traceId = '{}'", id);

    let df = match db.query(&project_id, &query).await {
        Ok(df) => df,
        Err(e) => {
            error!("Failed to query data by ID for project '{}': {:?}", project_id, e);
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to query data"
            }));
        }
    };

    let batches: Vec<datafusion::arrow::record_batch::RecordBatch> = match df.collect().await {
        Ok(batches) => batches,
        Err(e) => {
            error!("Failed to collect data by ID for project '{}': {:?}", project_id, e);
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to fetch data"
            }));
        }
    };

    let rows = record_batches_to_json_rows(&batches).unwrap_or_default();
    if rows.is_empty() {
        HttpResponse::NotFound().json(serde_json::json!({
            "error": "Record not found"
        }))
    } else {
        HttpResponse::Ok().json(rows)
    }
}

pub fn record_batches_to_json_rows(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> Result<Vec<Value>> {
    use datafusion::arrow::array::Array;

    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for i in 0..batch.num_rows() {
            let mut row = serde_json::Map::new();
            for (field, column) in schema.fields().iter().zip(batch.columns()) {
                let value = match column.data_type() {
                    datafusion::arrow::datatypes::DataType::Utf8 => {
                        let array = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                            .unwrap();
                        if array.is_null(i) {
                            Value::Null
                        } else {
                            Value::String(array.value(i).to_string())
                        }
                    }
                    datafusion::arrow::datatypes::DataType::Int32 => {
                        let array = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::Int32Array>()
                            .unwrap();
                        if array.is_null(i) {
                            Value::Null
                        } else {
                            Value::Number(array.value(i).into())
                        }
                    }
                    datafusion::arrow::datatypes::DataType::Int64 => {
                        let array = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::Int64Array>()
                            .unwrap();
                        if array.is_null(i) {
                            Value::Null
                        } else {
                            Value::Number(array.value(i).into())
                        }
                    }
                    datafusion::arrow::datatypes::DataType::Float64 => {
                        let array = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::Float64Array>()
                            .unwrap();
                        if array.is_null(i) {
                            Value::Null
                        } else {
                            Value::Number(
                                serde_json::Number::from_f64(array.value(i))
                                    .expect("Invalid f64 value"),
                            )
                        }
                    }
                    datafusion::arrow::datatypes::DataType::Boolean => {
                        let array = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                            .unwrap();
                        if array.is_null(i) {
                            Value::Null
                        } else {
                            Value::Bool(array.value(i))
                        }
                    }
                    datafusion::arrow::datatypes::DataType::Timestamp(_, _) => {
                        let array = column
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::TimestampNanosecondArray>()
                            .unwrap();
                        if array.is_null(i) {
                            Value::Null
                        } else {
                            Value::Number(array.value(i).into())
                        }
                    }
                    _ => Value::Null,
                };
                row.insert(field.name().clone(), value);
            }
            rows.push(Value::Object(row));
        }
    }
    Ok(rows)
}