use super::Walrus;

impl Walrus {
    pub fn append_for_topic(&self, col_name: &str, raw_bytes: &[u8]) -> std::io::Result<()> {
        let writer = self.get_or_create_writer(col_name)?;
        writer.write(raw_bytes)
    }

    pub fn batch_append_for_topic(&self, col_name: &str, batch: &[&[u8]]) -> std::io::Result<()> {
        let writer = self.get_or_create_writer(col_name)?;
        writer.batch_write(batch)
    }

    /// Durably flush a topic's pending single-entry writes to disk.
    /// `batch_append_for_topic` already flushes what it touches before
    /// returning; `append_for_topic` under a `Milliseconds` schedule defers
    /// to the background fsync thread — call this to make those writes
    /// durable before acking. No-op for a topic that was never written.
    pub fn sync_topic(&self, col_name: &str) -> std::io::Result<()> {
        let writer = {
            let map = self.writers.read().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "writers read lock poisoned"))?;
            map.get(col_name).cloned()
        };
        writer.map_or(Ok(()), |w| w.sync())
    }
}
