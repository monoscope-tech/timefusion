use sled;
use serde::{Serialize, Deserialize};
use anyhow::Result;

#[derive(Clone, Serialize, Deserialize)]
pub struct IngestRecord {
    pub project_id: String,
    pub id: String,
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
    db: sled::Db,
}

impl PersistentQueue {
    pub fn new(path: &str) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    pub async fn enqueue(&self, record: &IngestRecord) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let serialized = bincode::serialize(record)?;
        self.db.insert(id.as_bytes(), serialized)?;
        Ok(id)
    }

    pub async fn dequeue_all(&self) -> Result<Vec<(sled::IVec, IngestRecord)>> {
        let mut records = Vec::new();
        for item in self.db.iter() {
            let (key, value) = item?;
            let record: IngestRecord = bincode::deserialize(&value)?;
            records.push((key, record));
        }
        Ok(records)
    }

    pub fn remove_sync(&self, key: sled::IVec) -> Result<()> {
        self.db.remove(key)?;
        Ok(())
    }

    pub async fn len(&self) -> Result<usize> {
        Ok(self.db.len())
    }
}