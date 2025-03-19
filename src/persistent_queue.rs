use std::path::Path;

use serde::{Deserialize, Serialize};
use sled::{Db, IVec};

#[derive(Serialize, Deserialize, Clone)]
pub struct IngestRecord {
    pub projectId:            String,
    pub id:                   String,
    pub timestamp:            String,
    pub traceId:              String,
    pub spanId:               String,
    pub eventType:            String,
    pub status:               Option<String>,
    pub endTime:              Option<String>,
    pub durationNs:           i64,
    pub spanName:             String,
    pub spanKind:             Option<String>,
    pub parentSpanId:         Option<String>,
    pub traceState:           Option<String>,
    pub hasError:             bool,
    pub severityText:         Option<String>,
    pub severityNumber:       i32,
    pub body:                 Option<String>,
    pub httpMethod:           Option<String>,
    pub httpUrl:              Option<String>,
    pub httpRoute:            Option<String>,
    pub httpHost:             Option<String>,
    pub httpStatusCode:       Option<i32>,
    pub pathParams:           Option<String>,
    pub queryParams:          Option<String>,
    pub requestBody:          Option<String>,
    pub responseBody:         Option<String>,
    pub sdkType:              String,
    pub serviceVersion:       Option<String>,
    pub errors:               Option<String>,
    pub tags:                 Vec<String>,
    pub parentId:             Option<String>,
    pub dbSystem:             Option<String>,
    pub dbName:               Option<String>,
    pub dbStatement:          Option<String>,
    pub dbOperation:          Option<String>,
    pub rpcSystem:            Option<String>,
    pub rpcService:           Option<String>,
    pub rpcMethod:            Option<String>,
    pub endpointHash:         String,
    pub shapeHash:            String,
    pub formatHashes:         Vec<String>,
    pub fieldHashes:          Vec<String>,
    pub attributes:           Option<String>,
    pub events:               Option<String>,
    pub links:                Option<String>,
    pub resource:             Option<String>,
    pub instrumentationScope: Option<String>,
}

pub struct PersistentQueue {
    db: Db,
}

impl PersistentQueue {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    /// Enqueue an ingest record. Uses the record's `id` as the key.
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

    /// Remove a record by key.
    pub fn remove_sync(&self, key: IVec) -> Result<(), sled::Error> {
        self.db.remove(key)?;
        Ok(())
    }

    /// Returns the number of records in the queue.
    pub async fn len(&self) -> Result<usize, sled::Error> {
        Ok(self.db.len())
    }
}
