use sled::{Db, IVec};
use serde::{Serialize, Deserialize};

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
    pub fn new(path: &str) -> Self {
        let db = sled::open(path).expect("Failed to open Sled DB");
        Self { db }
    }

    /// Enqueue a record and return a unique receipt ID.
    pub async fn enqueue(&self, record: &IngestRecord) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let serialized = serde_json::to_vec(record)?;
        let id = uuid::Uuid::new_v4().to_string();
        tokio::task::spawn_blocking({
            let db = self.db.clone();
            let id_clone = id.clone();
            move || {
                db.insert(id_clone.as_bytes(), serialized)?;
                db.flush()?;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }
        }).await??;
        Ok(id)
    }

    pub async fn dequeue_all(&self) -> Result<Vec<(IVec, IngestRecord)>, Box<dyn std::error::Error + Send + Sync>> {
        tokio::task::spawn_blocking({
            let db = self.db.clone();
            move || {
                let mut records = Vec::new();
                for item in db.iter() {
                    let (key, value) = item?;
                    let record: IngestRecord = serde_json::from_slice(&value)?;
                    records.push((key, record));
                }
                Ok(records)
            }
        }).await?
    }

    /// Synchronous removal function.
    pub fn remove_sync(&self, key: IVec) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.db.remove(key)?;
        self.db.flush()?;
        Ok(())
    }

    pub async fn remove(&self, key: IVec) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tokio::task::spawn_blocking({
            let db = self.db.clone();
            move || {
                db.remove(key)?;
                db.flush()?;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }
        }).await??;
        Ok(())
    }
}
