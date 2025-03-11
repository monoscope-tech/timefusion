// persistent_queue.rs

use serde::{Serialize, Deserialize};
use sled::{Db, IVec};
use std::path::Path;

#[derive(Serialize, Deserialize, Clone)]
pub struct IngestRecord {
    pub table_name: String,
    pub project_id: String,
    pub id: String,
    pub version: i64,
    pub event_type: String,
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

pub struct PersistentQueue {
    db: Db,
}

impl PersistentQueue {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    /// Enqueue an ingest record. Here we use the record's id as the key.
    pub async fn enqueue(&self, record: &IngestRecord) -> Result<String, sled::Error> {
        let key = record.id.clone();
        let value = serde_json::to_vec(record).unwrap();
        self.db.insert(key.as_bytes(), value)?;
        Ok(key)
    }

    /// Dequeue all records.
    pub async fn dequeue_all(&self) -> Result<Vec<(IVec, IngestRecord)>, sled::Error> {
        let mut records = Vec::new();
        for result in self.db.iter() {
            let (key, value) = result?;
            let record: IngestRecord = serde_json::from_slice(&value).unwrap();
            records.push((key, record));
        }
        Ok(records)
    }

    pub fn remove_sync(&self, key: IVec) -> Result<(), sled::Error> {
        self.db.remove(key)?;
        Ok(())
    }

    pub async fn len(&self) -> Result<usize, sled::Error> {
        Ok(self.db.len())
    }
}
