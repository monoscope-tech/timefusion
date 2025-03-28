use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex; // Use tokio's Mutex
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, SeekFrom},
};

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Clone)]
pub struct OtelLogsAndSpans {
    // Top-level fields
    pub project_id:         String,
    pub timestamp:          chrono::DateTime<chrono::Utc>,
    pub observed_timestamp: chrono::DateTime<chrono::Utc>,

    pub id:             String,
    pub parent_id:      Option<String>,
    pub name:           String,
    pub kind:           Option<String>,
    pub status_code:    Option<String>,
    pub status_message: Option<String>,

    // Logs specific
    pub level:                      Option<String>, // same as severity text
    pub severity___severity_text:   Option<String>,
    pub severity___severity_number: Option<String>,
    pub body:                       Option<String>, // body as json json

    pub duration:   u64, // nanoseconds
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub end_time:   Option<chrono::DateTime<chrono::Utc>>,

    // Context
    pub context___trace_id:    String,
    pub context___span_id:     String,
    pub context___trace_state: Option<String>,
    pub context___trace_flags: Option<String>,
    pub context___is_remote:   Option<String>,

    // Events
    pub events: Option<String>, // events json

    // Links
    pub links: Option<String>, // links json

    // Attributes

    // Server and client
    pub attributes___client___address: Option<String>,
    pub attributes___client___port:    Option<u32>,
    pub attributes___server___address: Option<String>,
    pub attributes___server___port:    Option<u32>,

    // network https://opentelemetry.io/docs/specs/semconv/attributes-registry/network/
    pub attributes___network___local__address:     Option<String>,
    pub attributes___network___local__port:        Option<u32>,
    pub attributes___network___peer___address:     Option<String>,
    pub attributes___network___peer__port:         Option<u32>,
    pub attributes___network___protocol___name:    Option<String>,
    pub attributes___network___protocol___version: Option<String>,
    pub attributes___network___transport:          Option<String>,
    pub attributes___network___type:               Option<String>,

    // Source Code Attributes
    pub attributes___code___number:          Option<u32>,
    pub attributes___code___file___path:     Option<u32>,
    pub attributes___code___function___name: Option<u32>,
    pub attributes___code___line___number:   Option<u32>,
    pub attributes___code___stacktrace:      Option<u32>,

    // Log records. https://opentelemetry.io/docs/specs/semconv/general/logs/
    pub attributes___log__record___original: Option<String>,
    pub attributes___log__record___uid:      Option<String>,

    // Exception https://opentelemetry.io/docs/specs/semconv/exceptions/exceptions-logs/
    pub attributes___error___type:           Option<String>,
    pub attributes___exception___type:       Option<String>,
    pub attributes___exception___message:    Option<String>,
    pub attributes___exception___stacktrace: Option<String>,

    // URL https://opentelemetry.io/docs/specs/semconv/attributes-registry/url/
    pub attributes___url___fragment: Option<String>,
    pub attributes___url___full:     Option<String>,
    pub attributes___url___path:     Option<String>,
    pub attributes___url___query:    Option<String>,
    pub attributes___url___scheme:   Option<String>,

    // Useragent https://opentelemetry.io/docs/specs/semconv/attributes-registry/user-agent/
    pub attributes___user_agent___original: Option<String>,

    // HTTP https://opentelemetry.io/docs/specs/semconv/http/http-spans/
    pub attributes___http___request___method:          Option<String>,
    pub attributes___http___request___method_original: Option<String>,
    pub attributes___http___response___status_code:    Option<String>,
    pub attributes___http___request___resend_count:    Option<String>,
    pub attributes___http___request___body___size:     Option<String>,

    // Session https://opentelemetry.io/docs/specs/semconv/general/session/
    pub attributes___session___id:            Option<String>,
    pub attributes___session___previous___id: Option<String>,

    // Database https://opentelemetry.io/docs/specs/semconv/database/database-spans/
    pub attributes___db___system___name:            Option<String>,
    pub attributes___db___collection___name:        Option<String>,
    pub attributes___db___namespace:                Option<String>,
    pub attributes___db___operation___name:         Option<String>,
    pub attributes___db___response___status_code:   Option<String>,
    pub attributes___db___operation___batch___size: Option<u32>,
    pub attributes___db___query___summary:          Option<String>,
    pub attributes___db___query___text:             Option<String>,

    // https://opentelemetry.io/docs/specs/semconv/attributes-registry/user/
    pub attributes___user___id:        Option<String>,
    pub attributes___user___email:     Option<String>,
    pub attributes___user___full_name: Option<String>,
    pub attributes___user___name:      Option<String>,
    pub attributes___user___hash:      Option<String>,

    // Resource Attributes (subset) https://opentelemetry.io/docs/specs/semconv/resource/
    pub resource___attributes___service___name:          Option<String>,
    pub resource___attributes___service___version:       Option<String>,
    pub resource___attributes___service___instance___id: Option<String>,
    pub resource___attributes___service___namespace:     Option<String>,

    pub resource___attributes___telemetry___sdk___language: Option<String>,
    pub resource___attributes___telemetry___sdk___name:     Option<String>,
    pub resource___attributes___telemetry___sdk___version:  Option<String>,

    pub resource___attributes___user_agent___original: Option<String>,
}

#[derive(Clone)]
pub struct PersistentQueue {
    path:     PathBuf,
    file:     Arc<Mutex<File>>, // Using tokio::sync::Mutex
    position: Arc<Mutex<u64>>,  // Using tokio::sync::Mutex
}

impl PersistentQueue {
    pub async fn new(path: &str) -> Result<Self> {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        let file = OpenOptions::new().read(true).write(true).create(true).open(&path).await?;
        Ok(Self {
            path,
            file: Arc::new(Mutex::new(file)),
            position: Arc::new(Mutex::new(0)),
        })
    }

    pub async fn dequeue(&self) -> Result<Option<OtelLogsAndSpans>> {
        let mut file = self.file.lock().await; // Lock async
        let mut pos = self.position.lock().await; // Lock async

        if *pos == 0 {
            return Ok(None);
        }

        file.seek(SeekFrom::Start(0)).await?;
        let mut reader = BufReader::new(&mut *file);
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            return Ok(None);
        }

        let record: OtelLogsAndSpans = serde_json::from_str(&line.trim_end())?;
        let consumed = bytes_read as u64;

        *pos -= consumed;
        file.set_len(*pos).await?;
        file.seek(SeekFrom::Start(0)).await?;

        if *pos > 0 {
            let mut remaining = Vec::new();
            let mut new_reader = BufReader::new(&mut *file);
            new_reader.seek(SeekFrom::Start(consumed)).await?;
            new_reader.read_to_end(&mut remaining).await?;

            file.seek(SeekFrom::Start(0)).await?;
            file.write_all(&remaining).await?;
            file.flush().await?;
        }

        Ok(Some(record))
    }
}
