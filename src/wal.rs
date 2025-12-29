use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use bincode::{Decode, Encode};
use dashmap::DashSet;
use std::io::Cursor;
use std::path::PathBuf;
use thiserror::Error;
use tracing::{debug, error, info, instrument, warn};
use walrus_rust::{FsyncSchedule, ReadConsistency, Walrus};

#[derive(Debug, Error)]
pub enum WalError {
    #[error("WAL entry too short: {len} bytes")]
    TooShort { len: usize },
    #[error("Invalid WAL operation type: {0}")]
    InvalidOperation(u8),
    #[error("Bincode decode error: {0}")]
    BincodeDecode(#[from] bincode::error::DecodeError),
    #[error("Bincode encode error: {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),
    #[error("Arrow IPC error: {0}")]
    ArrowIpc(#[from] arrow::error::ArrowError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("No record batch found in data")]
    EmptyBatch,
}

/// Magic bytes to identify new WAL format with DML support
const WAL_MAGIC: [u8; 4] = [0x57, 0x41, 0x4C, 0x32]; // "WAL2"
const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(u8)]
pub enum WalOperation {
    Insert = 0,
    Delete = 1,
    Update = 2,
}

impl TryFrom<u8> for WalOperation {
    type Error = WalError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(WalOperation::Insert),
            1 => Ok(WalOperation::Delete),
            2 => Ok(WalOperation::Update),
            _ => Err(WalError::InvalidOperation(value)),
        }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct WalEntry {
    pub timestamp_micros: i64,
    pub project_id: String,
    pub table_name: String,
    pub operation: WalOperation,
    #[bincode(with_serde)]
    pub data: Vec<u8>,
}

impl WalEntry {
    fn new(project_id: &str, table_name: &str, operation: WalOperation, data: Vec<u8>) -> Self {
        Self {
            timestamp_micros: chrono::Utc::now().timestamp_micros(),
            project_id: project_id.into(),
            table_name: table_name.into(),
            operation,
            data,
        }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct DeletePayload {
    pub predicate_sql: Option<String>,
}

#[derive(Debug, Encode, Decode)]
pub struct UpdatePayload {
    pub predicate_sql: Option<String>,
    pub assignments: Vec<(String, String)>,
}

pub struct WalManager {
    wal: Walrus,
    data_dir: PathBuf,
    known_topics: DashSet<String>,
}

impl WalManager {
    pub fn new(data_dir: PathBuf) -> Result<Self, WalError> {
        std::fs::create_dir_all(&data_dir)?;

        let wal = Walrus::with_consistency_and_schedule(ReadConsistency::StrictlyAtOnce, FsyncSchedule::Milliseconds(200))?;

        // Load known topics from index file
        let meta_dir = data_dir.join(".timefusion_meta");
        let _ = std::fs::create_dir_all(&meta_dir);
        let topics_file = meta_dir.join("topics");

        let known_topics = DashSet::new();
        if let Ok(content) = std::fs::read_to_string(&topics_file) {
            for topic in content.lines().filter(|l| !l.is_empty()) {
                known_topics.insert(topic.to_string());
            }
        }

        info!("WAL initialized at {:?}, known topics: {}", data_dir, known_topics.len());
        Ok(Self { wal, data_dir, known_topics })
    }

    fn persist_topic(&self, topic: &str) {
        if self.known_topics.insert(topic.to_string()) {
            let meta_dir = self.data_dir.join(".timefusion_meta");
            let _ = std::fs::create_dir_all(&meta_dir);
            if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(meta_dir.join("topics")) {
                use std::io::Write;
                let _ = writeln!(file, "{}", topic);
            }
        }
    }

    fn make_topic(project_id: &str, table_name: &str) -> String {
        format!("{}:{}", project_id, table_name)
    }

    fn parse_topic(topic: &str) -> Option<(String, String)> {
        topic.split_once(':').map(|(p, t)| (p.to_string(), t.to_string()))
    }

    #[instrument(skip(self, batch), fields(project_id, table_name, rows))]
    pub fn append(&self, project_id: &str, table_name: &str, batch: &RecordBatch) -> Result<(), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let entry = WalEntry::new(project_id, table_name, WalOperation::Insert, serialize_record_batch(batch)?);
        self.wal.append_for_topic(&topic, &serialize_wal_entry(&entry)?)?;
        self.persist_topic(&topic);
        debug!("WAL append INSERT: topic={}, rows={}", topic, batch.num_rows());
        Ok(())
    }

    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub fn append_batch(&self, project_id: &str, table_name: &str, batches: &[RecordBatch]) -> Result<(), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let payloads: Vec<Vec<u8>> = batches
            .iter()
            .map(|batch| serialize_wal_entry(&WalEntry::new(project_id, table_name, WalOperation::Insert, serialize_record_batch(batch)?)))
            .collect::<Result<_, _>>()?;

        let payload_refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        self.wal.batch_append_for_topic(&topic, &payload_refs)?;
        self.persist_topic(&topic);
        debug!("WAL batch append INSERT: topic={}, batches={}", topic, batches.len());
        Ok(())
    }

    #[instrument(skip(self), fields(project_id, table_name))]
    pub fn append_delete(&self, project_id: &str, table_name: &str, predicate_sql: Option<&str>) -> Result<(), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let data = bincode::encode_to_vec(
            &DeletePayload {
                predicate_sql: predicate_sql.map(String::from),
            },
            BINCODE_CONFIG,
        )?;
        let entry = WalEntry::new(project_id, table_name, WalOperation::Delete, data);
        self.wal.append_for_topic(&topic, &serialize_wal_entry(&entry)?)?;
        self.persist_topic(&topic);
        debug!("WAL append DELETE: topic={}, predicate={:?}", topic, predicate_sql);
        Ok(())
    }

    #[instrument(skip(self, assignments), fields(project_id, table_name))]
    pub fn append_update(&self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, assignments: &[(String, String)]) -> Result<(), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let payload = UpdatePayload {
            predicate_sql: predicate_sql.map(String::from),
            assignments: assignments.to_vec(),
        };
        let entry = WalEntry::new(project_id, table_name, WalOperation::Update, bincode::encode_to_vec(&payload, BINCODE_CONFIG)?);
        self.wal.append_for_topic(&topic, &serialize_wal_entry(&entry)?)?;
        self.persist_topic(&topic);
        debug!(
            "WAL append UPDATE: topic={}, predicate={:?}, assignments={}",
            topic,
            predicate_sql,
            assignments.len()
        );
        Ok(())
    }

    #[instrument(skip(self), fields(project_id, table_name))]
    pub fn read_entries_raw(
        &self, project_id: &str, table_name: &str, since_timestamp_micros: Option<i64>, checkpoint: bool,
    ) -> Result<(Vec<WalEntry>, usize), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let cutoff = since_timestamp_micros.unwrap_or(0);
        let mut results = Vec::new();
        let mut error_count = 0usize;

        loop {
            match self.wal.read_next(&topic, checkpoint) {
                Ok(Some(entry_data)) => match deserialize_wal_entry(&entry_data.data) {
                    Ok(entry) if entry.timestamp_micros >= cutoff => results.push(entry),
                    Ok(_) => {} // Skip old entries
                    Err(e) => {
                        warn!("Skipping corrupted WAL entry: {}", e);
                        error_count += 1;
                    }
                },
                Ok(None) => break,
                Err(e) => {
                    error!("I/O error reading WAL: {}", e);
                    error_count += 1;
                    break;
                }
            }
        }

        if error_count > 0 {
            warn!("WAL read: topic={}, entries={}, errors={}", topic, results.len(), error_count);
        } else {
            debug!("WAL read: topic={}, entries={}", topic, results.len());
        }
        Ok((results, error_count))
    }

    #[instrument(skip(self))]
    pub fn read_all_entries_raw(&self, since_timestamp_micros: Option<i64>, checkpoint: bool) -> Result<(Vec<WalEntry>, usize), WalError> {
        let cutoff = since_timestamp_micros.unwrap_or(0);

        let (mut all_results, total_errors) = self.list_topics()?.into_iter().filter_map(|topic| Self::parse_topic(&topic).map(|(p, t)| (topic, p, t))).fold(
            (Vec::new(), 0usize),
            |(mut results, mut errors), (topic, project_id, table_name)| {
                match self.read_entries_raw(&project_id, &table_name, Some(cutoff), checkpoint) {
                    Ok((entries, err_count)) => {
                        results.extend(entries);
                        errors += err_count;
                    }
                    Err(e) => {
                        warn!("Failed to read entries for topic {}: {}", topic, e);
                        errors += 1;
                    }
                }
                (results, errors)
            },
        );

        all_results.sort_by_key(|e| e.timestamp_micros);

        if total_errors > 0 {
            warn!("WAL read all: total_entries={}, cutoff={}, errors={}", all_results.len(), cutoff, total_errors);
        } else {
            info!("WAL read all: total_entries={}, cutoff={}", all_results.len(), cutoff);
        }
        Ok((all_results, total_errors))
    }

    pub fn deserialize_batch(data: &[u8]) -> Result<RecordBatch, WalError> {
        deserialize_record_batch(data)
    }

    pub fn list_topics(&self) -> Result<Vec<String>, WalError> {
        Ok(self.known_topics.iter().map(|t| t.clone()).collect())
    }

    #[instrument(skip(self))]
    pub fn checkpoint(&self, project_id: &str, table_name: &str) -> Result<(), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let mut count = 0;
        loop {
            match self.wal.read_next(&topic, true) {
                Ok(Some(_)) => count += 1,
                Ok(None) => break,
                Err(e) => {
                    warn!("Error during checkpoint for {}: {}", topic, e);
                    break;
                }
            }
        }
        if count > 0 {
            debug!("WAL checkpoint: topic={}, consumed={}", topic, count);
        }
        Ok(())
    }

    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }
}

fn serialize_record_batch(batch: &RecordBatch) -> Result<Vec<u8>, WalError> {
    let mut buffer = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buffer)
}

fn deserialize_record_batch(data: &[u8]) -> Result<RecordBatch, WalError> {
    StreamReader::try_new(Cursor::new(data), None)?.next().ok_or(WalError::EmptyBatch)?.map_err(WalError::ArrowIpc)
}

fn serialize_wal_entry(entry: &WalEntry) -> Result<Vec<u8>, WalError> {
    let mut buffer = WAL_MAGIC.to_vec();
    buffer.push(entry.operation as u8);
    buffer.extend(bincode::encode_to_vec(entry, BINCODE_CONFIG)?);
    Ok(buffer)
}

fn deserialize_wal_entry(data: &[u8]) -> Result<WalEntry, WalError> {
    if data.len() < 5 {
        return Err(WalError::TooShort { len: data.len() });
    }

    // Check for new format (magic header)
    if data[0..4] == WAL_MAGIC {
        WalOperation::try_from(data[4])?; // Validate operation type
        let (entry, _): (WalEntry, _) = bincode::decode_from_slice(&data[5..], BINCODE_CONFIG)?;
        Ok(entry)
    } else {
        // Old format - decode without magic header, assume INSERT
        let (mut entry, _): (WalEntry, _) = bincode::decode_from_slice(data, BINCODE_CONFIG)?;
        entry.operation = WalOperation::Insert;
        Ok(entry)
    }
}

pub fn deserialize_delete_payload(data: &[u8]) -> Result<DeletePayload, WalError> {
    let (payload, _) = bincode::decode_from_slice(data, BINCODE_CONFIG)?;
    Ok(payload)
}

pub fn deserialize_update_payload(data: &[u8]) -> Result<UpdatePayload, WalError> {
    let (payload, _) = bincode::decode_from_slice(data, BINCODE_CONFIG)?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])), Arc::new(StringArray::from(vec!["a", "b", "c"]))],
        )
        .unwrap()
    }

    #[test]
    fn test_record_batch_serialization() {
        let batch = create_test_batch();
        let serialized = serialize_record_batch(&batch).unwrap();
        let deserialized = deserialize_record_batch(&serialized).unwrap();
        assert_eq!(batch.num_rows(), deserialized.num_rows());
        assert_eq!(batch.num_columns(), deserialized.num_columns());
    }

    #[test]
    fn test_wal_entry_serialization() {
        let entry = WalEntry {
            timestamp_micros: 1234567890,
            project_id: "project-123".to_string(),
            table_name: "test_table".to_string(),
            operation: WalOperation::Insert,
            data: vec![1, 2, 3, 4, 5],
        };
        let serialized = serialize_wal_entry(&entry).unwrap();
        let deserialized = deserialize_wal_entry(&serialized).unwrap();
        assert_eq!(entry.timestamp_micros, deserialized.timestamp_micros);
        assert_eq!(entry.project_id, deserialized.project_id);
        assert_eq!(entry.table_name, deserialized.table_name);
        assert_eq!(entry.operation, deserialized.operation);
        assert_eq!(entry.data, deserialized.data);
    }

    #[test]
    fn test_delete_payload_serialization() {
        let payload = DeletePayload {
            predicate_sql: Some("id = 1".to_string()),
        };
        let serialized = bincode::encode_to_vec(&payload, BINCODE_CONFIG).unwrap();
        let deserialized = deserialize_delete_payload(&serialized).unwrap();
        assert_eq!(payload.predicate_sql, deserialized.predicate_sql);

        let payload_none = DeletePayload { predicate_sql: None };
        let serialized_none = bincode::encode_to_vec(&payload_none, BINCODE_CONFIG).unwrap();
        let deserialized_none = deserialize_delete_payload(&serialized_none).unwrap();
        assert_eq!(payload_none.predicate_sql, deserialized_none.predicate_sql);
    }

    #[test]
    fn test_update_payload_serialization() {
        let payload = UpdatePayload {
            predicate_sql: Some("id = 1".to_string()),
            assignments: vec![("name".to_string(), "'updated'".to_string())],
        };
        let serialized = bincode::encode_to_vec(&payload, BINCODE_CONFIG).unwrap();
        let deserialized = deserialize_update_payload(&serialized).unwrap();
        assert_eq!(payload.predicate_sql, deserialized.predicate_sql);
        assert_eq!(payload.assignments, deserialized.assignments);
    }
}
