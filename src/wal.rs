use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use dashmap::DashSet;
use std::io::Cursor;
use std::path::PathBuf;
use tracing::{debug, error, info, instrument, warn};
use walrus_rust::{FsyncSchedule, ReadConsistency, Walrus};

#[derive(Debug)]
pub struct WalEntry {
    pub timestamp_micros: i64,
    pub project_id: String,
    pub table_name: String,
    pub data: Vec<u8>,
}

pub struct WalManager {
    wal: Walrus,
    data_dir: PathBuf,
    known_topics: DashSet<String>,
}

impl WalManager {
    pub fn new(data_dir: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&data_dir)?;
        // Note: WALRUS_DATA_DIR must be set before creating WalManager.
        // This is done in main.rs before any threads spawn.

        let wal = Walrus::with_consistency_and_schedule(ReadConsistency::StrictlyAtOnce, FsyncSchedule::Milliseconds(200))?;

        // Load known topics from index file (stored in meta subdirectory to avoid walrus scanning)
        let meta_dir = data_dir.join(".timefusion_meta");
        let _ = std::fs::create_dir_all(&meta_dir);
        let topics_file = meta_dir.join("topics");

        let known_topics = DashSet::new();
        if topics_file.exists()
            && let Ok(content) = std::fs::read_to_string(&topics_file)
        {
            for line in content.lines() {
                if !line.is_empty() {
                    known_topics.insert(line.to_string());
                }
            }
        }

        info!("WAL initialized at {:?}, known topics: {}", data_dir, known_topics.len());
        Ok(Self { wal, data_dir, known_topics })
    }

    fn persist_topic(&self, topic: &str) {
        if self.known_topics.insert(topic.to_string()) {
            // New topic, persist to file in meta directory
            let meta_dir = self.data_dir.join(".timefusion_meta");
            let _ = std::fs::create_dir_all(&meta_dir);
            let topics_file = meta_dir.join("topics");
            if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&topics_file) {
                use std::io::Write;
                let _ = writeln!(file, "{}", topic);
            }
        }
    }

    fn make_topic(project_id: &str, table_name: &str) -> String {
        format!("{}:{}", project_id, table_name)
    }

    fn parse_topic(topic: &str) -> Option<(String, String)> {
        let parts: Vec<&str> = topic.splitn(2, ':').collect();
        if parts.len() == 2 { Some((parts[0].to_string(), parts[1].to_string())) } else { None }
    }

    #[instrument(skip(self, batch), fields(project_id, table_name, rows))]
    pub fn append(&self, project_id: &str, table_name: &str, batch: &RecordBatch) -> anyhow::Result<()> {
        let timestamp_micros = chrono::Utc::now().timestamp_micros();
        let topic = Self::make_topic(project_id, table_name);

        let entry = WalEntry {
            timestamp_micros,
            project_id: project_id.to_string(),
            table_name: table_name.to_string(),
            data: serialize_record_batch(batch)?,
        };

        let payload = serialize_wal_entry(&entry)?;

        self.wal.append_for_topic(&topic, &payload)?;
        self.persist_topic(&topic);

        debug!("WAL append: topic={}, timestamp={}, rows={}", topic, timestamp_micros, batch.num_rows());
        Ok(())
    }

    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub fn append_batch(&self, project_id: &str, table_name: &str, batches: &[RecordBatch]) -> anyhow::Result<()> {
        let timestamp_micros = chrono::Utc::now().timestamp_micros();
        let topic = Self::make_topic(project_id, table_name);

        let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(batches.len());
        for batch in batches {
            let data = serialize_record_batch(batch)?;
            let entry = WalEntry {
                timestamp_micros,
                project_id: project_id.to_string(),
                table_name: table_name.to_string(),
                data,
            };
            payloads.push(serialize_wal_entry(&entry)?);
        }

        let payload_refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
        self.wal.batch_append_for_topic(&topic, &payload_refs)?;
        self.persist_topic(&topic);

        debug!("WAL batch append: topic={}, batches={}", topic, batches.len());
        Ok(())
    }

    #[instrument(skip(self), fields(project_id, table_name))]
    pub fn read_entries(
        &self, project_id: &str, table_name: &str, since_timestamp_micros: Option<i64>, checkpoint: bool,
    ) -> anyhow::Result<(Vec<(WalEntry, RecordBatch)>, usize)> {
        let topic = Self::make_topic(project_id, table_name);
        let mut results = Vec::new();
        let mut error_count = 0usize;
        let cutoff = since_timestamp_micros.unwrap_or(0);

        loop {
            match self.wal.read_next(&topic, checkpoint) {
                Ok(Some(entry_data)) => match deserialize_wal_entry(&entry_data.data) {
                    Ok(entry) => {
                        if entry.timestamp_micros >= cutoff {
                            match deserialize_record_batch(&entry.data) {
                                Ok(batch) => results.push((entry, batch)),
                                Err(e) => {
                                    warn!("Skipping corrupted batch in WAL: {}", e);
                                    error_count += 1;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Skipping corrupted WAL entry: {}", e);
                        error_count += 1;
                    }
                },
                Ok(None) => break,
                Err(e) => {
                    // I/O error - log and continue to try remaining entries
                    error!("I/O error reading WAL (continuing): {}", e);
                    error_count += 1;
                    // Try to continue reading - some WAL implementations recover after errors
                    continue;
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
    pub fn read_all_entries(&self, since_timestamp_micros: Option<i64>, checkpoint: bool) -> anyhow::Result<(Vec<(WalEntry, RecordBatch)>, usize)> {
        let mut all_results = Vec::new();
        let mut total_errors = 0usize;
        let cutoff = since_timestamp_micros.unwrap_or(0);

        let topics = self.list_topics()?;

        for topic in topics {
            if let Some((project_id, table_name)) = Self::parse_topic(&topic) {
                match self.read_entries(&project_id, &table_name, Some(cutoff), checkpoint) {
                    Ok((entries, errors)) => {
                        all_results.extend(entries);
                        total_errors += errors;
                    }
                    Err(e) => {
                        warn!("Failed to read entries for topic {}: {}", topic, e);
                        total_errors += 1;
                    }
                }
            }
        }

        if total_errors > 0 {
            warn!("WAL read all: total_entries={}, cutoff={}, errors={}", all_results.len(), cutoff, total_errors);
        } else {
            info!("WAL read all: total_entries={}, cutoff={}", all_results.len(), cutoff);
        }
        Ok((all_results, total_errors))
    }

    pub fn list_topics(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.known_topics.iter().map(|t| t.clone()).collect())
    }

    #[instrument(skip(self))]
    pub fn checkpoint(&self, project_id: &str, table_name: &str) -> anyhow::Result<()> {
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

fn serialize_record_batch(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

fn deserialize_record_batch(data: &[u8]) -> anyhow::Result<RecordBatch> {
    let cursor = Cursor::new(data);
    let mut reader = StreamReader::try_new(cursor, None)?;
    reader
        .next()
        .ok_or_else(|| anyhow::anyhow!("No record batch found in data"))?
        .map_err(|e| anyhow::anyhow!("Failed to deserialize record batch: {}", e))
}

fn serialize_wal_entry(entry: &WalEntry) -> anyhow::Result<Vec<u8>> {
    let mut buffer = Vec::new();

    buffer.extend_from_slice(&entry.timestamp_micros.to_le_bytes());

    let project_id_bytes = entry.project_id.as_bytes();
    buffer.extend_from_slice(&(project_id_bytes.len() as u16).to_le_bytes());
    buffer.extend_from_slice(project_id_bytes);

    let table_name_bytes = entry.table_name.as_bytes();
    buffer.extend_from_slice(&(table_name_bytes.len() as u16).to_le_bytes());
    buffer.extend_from_slice(table_name_bytes);

    buffer.extend_from_slice(&entry.data);

    Ok(buffer)
}

fn deserialize_wal_entry(data: &[u8]) -> anyhow::Result<WalEntry> {
    if data.len() < 12 {
        anyhow::bail!("WAL entry too short");
    }

    let mut offset = 0;

    let timestamp_micros = i64::from_le_bytes(data[offset..offset + 8].try_into()?);
    offset += 8;

    let project_id_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;

    if data.len() < offset + project_id_len + 2 {
        anyhow::bail!("WAL entry truncated at project_id");
    }
    let project_id = String::from_utf8(data[offset..offset + project_id_len].to_vec())?;
    offset += project_id_len;

    let table_name_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;

    if data.len() < offset + table_name_len {
        anyhow::bail!("WAL entry truncated at table_name");
    }
    let table_name = String::from_utf8(data[offset..offset + table_name_len].to_vec())?;
    offset += table_name_len;

    let entry_data = data[offset..].to_vec();

    Ok(WalEntry {
        timestamp_micros,
        project_id,
        table_name,
        data: entry_data,
    })
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
        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["a", "b", "c"]);
        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
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
            data: vec![1, 2, 3, 4, 5],
        };
        let serialized = serialize_wal_entry(&entry).unwrap();
        let deserialized = deserialize_wal_entry(&serialized).unwrap();
        assert_eq!(entry.timestamp_micros, deserialized.timestamp_micros);
        assert_eq!(entry.project_id, deserialized.project_id);
        assert_eq!(entry.table_name, deserialized.table_name);
        assert_eq!(entry.data, deserialized.data);
    }
}
