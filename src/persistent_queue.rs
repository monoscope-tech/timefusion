use sled::{Db, IVec};
use serde::{Serialize, Deserialize};
use anyhow::Context;

#[derive(Serialize, Deserialize, Debug)]
pub struct IngestRecord {
    pub project_id: String,
    pub timestamp: String,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub payload: Option<String>,
}

pub struct PersistentQueue {
    pub db: Db,
}

impl PersistentQueue {
    /// Opens a Sled database at the given path. Returns an error with context if the database cannot be opened.
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let db = sled::open(path)
            .with_context(|| format!("Failed to open Sled DB at path: {}", path))?;
        Ok(Self { db })
    }

    /// Enqueue a record and return a unique receipt ID.
    pub async fn enqueue(&self, record: &IngestRecord) -> anyhow::Result<String> {
        let serialized = serde_json::to_vec(record)
            .context("Failed to serialize IngestRecord")?;
        let id = uuid::Uuid::new_v4().to_string();
        tokio::task::spawn_blocking({
            let db = self.db.clone();
            let id_clone = id.clone();
            move || {
                db.insert(id_clone.as_bytes(), serialized)
                    .context("Failed to insert record into Sled DB")?;
                db.flush().context("Failed to flush Sled DB")?;
                Ok::<(), anyhow::Error>(())
            }
        })
        .await??;
        Ok(id)
    }

    /// Dequeues all records from the Sled database.
    pub async fn dequeue_all(&self) -> anyhow::Result<Vec<(IVec, IngestRecord)>> {
        tokio::task::spawn_blocking({
            let db = self.db.clone();
            move || {
                let mut records = Vec::new();
                for item in db.iter() {
                    let (key, value) = item.context("Error iterating over Sled DB")?;
                    let record: IngestRecord = serde_json::from_slice(&value)
                        .context("Failed to deserialize IngestRecord")?;
                    records.push((key, record));
                }
                Ok(records)
            }
        })
        .await?
    }

    /// Synchronously removes a record with the given key from the Sled database.
    pub fn remove_sync(&self, key: IVec) -> anyhow::Result<()> {
        self.db
            .remove(key)
            .context("Failed to remove key from Sled DB")?;
        self.db.flush().context("Failed to flush Sled DB after removal")?;
        Ok(())
    }

    #[allow(dead_code)]
    /// Asynchronously removes a record with the given key from the Sled database.
    pub async fn remove(&self, key: IVec) -> anyhow::Result<()> {
        tokio::task::spawn_blocking({
            let db = self.db.clone();
            move || {
                db.remove(key)
                    .context("Failed to remove key from Sled DB")?;
                db.flush()
                    .context("Failed to flush Sled DB after removal")?;
                Ok::<(), anyhow::Error>(())
            }
        })
        .await??;
        Ok(())
    }
}
