use sled::{Db, IVec};
use serde::{Serialize, Deserialize};
use anyhow::Context;
use tokio::task;

#[derive(Serialize, Deserialize, Debug)]
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
    pub db: Db,
}

impl PersistentQueue {
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let db = sled::open(path)
            .with_context(|| format!("Failed to open Sled DB at path: {}", path))?;
        Ok(Self { db })
    }

    pub async fn enqueue(&self, record: &IngestRecord) -> anyhow::Result<String> {
        let serialized = serde_json::to_vec(record)
            .context("Failed to serialize IngestRecord")?;
        let id = uuid::Uuid::new_v4().to_string();
        let db = self.db.clone();
        let id_clone = id.clone();
        task::spawn_blocking(move || {
            db.insert(id_clone.as_bytes(), serialized)
                .context("Failed to insert record into Sled DB")?;
            db.flush().context("Failed to flush Sled DB")?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;
        Ok(id)
    }

    pub async fn dequeue_all(&self) -> anyhow::Result<Vec<(IVec, IngestRecord)>> {
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let mut records = Vec::new();
            for item in db.iter() {
                let (key, value) = item.context("Error iterating over Sled DB")?;
                let record: IngestRecord = serde_json::from_slice(&value)
                    .context("Failed to deserialize IngestRecord")?;
                records.push((key, record));
            }
            Ok(records)
        })
        .await?
    }

    pub fn remove_sync(&self, key: IVec) -> anyhow::Result<()> {
        self.db
            .remove(key)
            .context("Failed to remove key from Sled DB")?;
        self.db.flush().context("Failed to flush Sled DB after removal")?;
        Ok(())
    }
}