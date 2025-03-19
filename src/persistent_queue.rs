use serde::{Serialize, Deserialize};
use sled::{Db, IVec};
use std::path::Path;
use anyhow::Result;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IngestRecord {
    pub project_id: String,
    pub id: String,
    pub timestamp: String,
    pub trace_id: String,
    pub span_id: String,
    pub event_type: String,
    pub status: Option<String>,
    pub end_time: Option<String>,
    pub duration_ns: i64,
    pub span_name: String,
    pub span_kind: Option<String>,
    pub parent_span_id: Option<String>,
    pub trace_state: Option<String>,
    pub has_error: bool,
    pub severity_text: Option<String>,
    pub severity_number: i32,
    pub body: Option<String>,
    pub http_method: Option<String>,
    pub http_url: Option<String>,
    pub http_route: Option<String>,
    pub http_host: Option<String>,
    pub http_status_code: Option<i32>,
    pub path_params: Option<String>,
    pub query_params: Option<String>,
    pub request_body: Option<String>,
    pub response_body: Option<String>,
    pub sdk_type: String,
    pub service_version: Option<String>,
    pub errors: Option<String>,
    pub tags: Vec<String>,
    pub parent_id: Option<String>,
    pub db_system: Option<String>,
    pub db_name: Option<String>,
    pub db_statement: Option<String>,
    pub db_operation: Option<String>,
    pub rpc_system: Option<String>,
    pub rpc_service: Option<String>,
    pub rpc_method: Option<String>,
    pub endpoint_hash: String,
    pub shape_hash: String,
    pub format_hashes: Vec<String>,
    pub field_hashes: Vec<String>,
    pub attributes: Option<String>,
    pub events: Option<String>,
    pub links: Option<String>,
    pub resource: Option<String>,
    pub instrumentation_scope: Option<String>,
}

pub struct PersistentQueue {
    db: Db,
}

impl PersistentQueue {
    /// Creates a new persistent queue at the specified path.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    /// Enqueues an ingest record using its `id` as the key.
    pub async fn enqueue(&self, record: &IngestRecord) -> Result<String> {
        let key = record.id.clone();
        let value = serde_json::to_vec(record)?;
        self.db.insert(key.as_bytes(), value)?;
        Ok(key)
    }

    /// Dequeues all records from the queue.
    pub async fn dequeue_all(&self) -> Result<Vec<(IVec, IngestRecord)>> {
        let mut records = Vec::new();
        for result in self.db.iter() {
            let (key, value) = result?;
            let record: IngestRecord = serde_json::from_slice(&value)?;
            records.push((key, record));
        }
        Ok(records)
    }

    /// Removes a record from the queue by key synchronously.
    pub fn remove_sync(&self, key: IVec) -> Result<()> {
        self.db.remove(key)?;
        Ok(())
    }

    /// Returns the number of records in the queue.
    pub async fn len(&self) -> Result<usize> {
        Ok(self.db.len())
    }
}