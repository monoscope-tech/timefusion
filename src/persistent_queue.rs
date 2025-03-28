use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, SeekFrom};
use serde::{Deserialize, Serialize};
use anyhow::Result;

#[derive(Serialize, Deserialize, Clone)]
pub struct IngestRecord {
    // Top-level fields
    pub traceId: String,
    pub spanId: String,
    pub traceState: Option<String>,
    pub parentSpanId: Option<String>,
    pub name: String,
    pub kind: Option<String>,
    pub startTimeUnixNano: i64,
    pub endTimeUnixNano: Option<i64>,

    // Attributes – HTTP
    pub attributes____http___method: Option<String>,
    pub attributes____http___url: Option<String>,
    pub attributes____http___status___code: Option<i32>,
    pub attributes____http___request___content___length: Option<i64>,
    pub attributes____http___response___content___length: Option<i64>,
    pub attributes____http___route: Option<String>,
    pub attributes____http___scheme: Option<String>,
    pub attributes____http___client___ip: Option<String>,
    pub attributes____http___user___agent: Option<String>,
    pub attributes____http___flavor: Option<String>,
    pub attributes____http___target: Option<String>,
    pub attributes____http___host: Option<String>,

    // Attributes – RPC
    pub attributes____rpc___system: Option<String>,
    pub attributes____rpc___service: Option<String>,
    pub attributes____rpc___method: Option<String>,
    pub attributes____rpc___grpc___status___code: Option<i32>,

    // Attributes – Database
    pub attributes____db___system: Option<String>,
    pub attributes____db___connection___string: Option<String>,
    pub attributes____db___user: Option<String>,
    pub attributes____db___name: Option<String>,
    pub attributes____db___statement: Option<String>,
    pub attributes____db___operation: Option<String>,
    pub attributes____db___sql___table: Option<String>,

    // Attributes – Messaging
    pub attributes____messaging___system: Option<String>,
    pub attributes____messaging___destination: Option<String>,
    pub attributes____messaging___destination___kind: Option<String>,
    pub attributes____messaging___message___id: Option<String>,
    pub attributes____messaging___operation: Option<String>,
    pub attributes____messaging___url: Option<String>,
    pub attributes____messaging___client___id: Option<String>,
    pub attributes____messaging___kafka___partition: Option<i32>,
    pub attributes____messaging___kafka___offset: Option<i64>,
    pub attributes____messaging___kafka___consumer___group: Option<String>,
    pub attributes____messaging___message___payload___size___bytes: Option<i64>,
    pub attributes____messaging___protocol: Option<String>,
    pub attributes____messaging___protocol___version: Option<String>,

    // Attributes – Cache
    pub attributes____cache___system: Option<String>,
    pub attributes____cache___operation: Option<String>,
    pub attributes____cache___key: Option<String>,
    pub attributes____cache___hit: Option<bool>,

    // Attributes – Network
    pub attributes____net___peer___ip: Option<String>,
    pub attributes____net___peer___port: Option<i32>,
    pub attributes____net___host___ip: Option<String>,
    pub attributes____net___host___port: Option<i32>,
    pub attributes____net___transport: Option<String>,

    // Attributes – Enduser
    pub attributes____enduser___id: Option<String>,
    pub attributes____enduser___role: Option<String>,
    pub attributes____enduser___scope: Option<String>,

    // Attributes – Exception
    pub attributes____exception___type: Option<String>,
    pub attributes____exception___message: Option<String>,
    pub attributes____exception___stacktrace: Option<String>,
    pub attributes____exception___escaped: Option<bool>,

    // Attributes – Thread
    pub attributes____thread___id: Option<i64>,
    pub attributes____thread___name: Option<String>,

    // Attributes – Code
    pub attributes____code___function: Option<String>,
    pub attributes____code___filepath: Option<String>,
    pub attributes____code___namespace: Option<String>,
    pub attributes____code___lineno: Option<i32>,

    // Attributes – Deployment
    pub attributes____deployment___environment: Option<String>,
    pub attributes____deployment___version: Option<String>,

    // Attributes – Service
    pub attributes____service___name: Option<String>,
    pub attributes____service___version: Option<String>,
    pub attributes____service___instance___id: Option<String>,

    // Attributes – OTel Library
    pub attributes____otel___library___name: Option<String>,
    pub attributes____otel___library___version: Option<String>,

    // Attributes – Kubernetes
    pub attributes____k8s___pod___name: Option<String>,
    pub attributes____k8s___namespace___name: Option<String>,
    pub attributes____k8s___deployment___name: Option<String>,

    // Attributes – Container, Host, OS, Process
    pub attributes____container___id: Option<String>,
    pub attributes____host___name: Option<String>,
    pub attributes____os___type: Option<String>,
    pub attributes____os___version: Option<String>,
    pub attributes____process___pid: Option<i64>,
    pub attributes____process___command___line: Option<String>,
    pub attributes____process___runtime___name: Option<String>,
    pub attributes____process___runtime___version: Option<String>,

    // Attributes – AWS
    pub attributes____aws___region: Option<String>,
    pub attributes____aws___account___id: Option<String>,
    pub attributes____aws___dynamodb___table___name: Option<String>,
    pub attributes____aws___dynamodb___operation: Option<String>,
    pub attributes____aws___dynamodb___consumed___capacity___total: Option<f64>,
    pub attributes____aws___sqs___queue___url: Option<String>,
    pub attributes____aws___sqs___message___id: Option<String>,

    // Attributes – Azure
    pub attributes____azure___resource___id: Option<String>,
    pub attributes____azure___storage___container___name: Option<String>,
    pub attributes____azure___storage___blob___name: Option<String>,

    // Attributes – GCP
    pub attributes____gcp___project___id: Option<String>,
    pub attributes____gcp___cloudsql___instance___id: Option<String>,
    pub attributes____gcp___pubsub___message___id: Option<String>,

    // Attributes – Custom
    pub attributes____custom___attribute___1: Option<String>,
    pub attributes____custom___attribute___2: Option<i64>,
    pub attributes____custom___attribute___3: Option<bool>,

    // Events
    pub events____timeUnixNano: Option<i64>,
    pub events____name: Option<String>,
    pub events____attributes____db___statement: Option<String>,
    pub events____attributes____db___rows___affected: Option<i64>,
    pub events____attributes____messaging___message___id: Option<String>,
    pub events____attributes____cache___key: Option<String>,

    // Links
    pub links____traceId: Option<String>,
    pub links____spanId: Option<String>,
    pub links____traceState: Option<String>,
    pub links____attributes____link___attribute: Option<String>,

    // Status
    pub status____code: Option<String>,
    pub status____message: Option<String>,

    // Resource Attributes (subset)
    pub resource____attributes____service___name: Option<String>,
    pub resource____attributes____service___version: Option<String>,
    pub resource____attributes____service___instance___id: Option<String>,
    pub resource____attributes____service___namespace: Option<String>,
    pub resource____attributes____host___name: Option<String>,
    pub resource____attributes____host___id: Option<String>,
    pub resource____attributes____host___type: Option<String>,
    pub resource____attributes____host___arch: Option<String>,
    pub resource____attributes____os___type: Option<String>,
    pub resource____attributes____os___version: Option<String>,
    pub resource____attributes____process___pid: Option<i64>,
    pub resource____attributes____process___executable___name: Option<String>,
    pub resource____attributes____process___command___line: Option<String>,
    pub resource____attributes____process___runtime___name: Option<String>,
    pub resource____attributes____process___runtime___version: Option<String>,
    pub resource____attributes____process___runtime___description: Option<String>,
    pub resource____attributes____process___executable___path: Option<String>,
    pub resource____attributes____k8s___cluster___name: Option<String>,
    pub resource____attributes____k8s___namespace___name: Option<String>,
    pub resource____attributes____k8s___deployment___name: Option<String>,
    pub resource____attributes____k8s___pod___name: Option<String>,
    pub resource____attributes____k8s___pod___uid: Option<String>,
    pub resource____attributes____k8s___replicaset___name: Option<String>,
    pub resource____attributes____k8s___deployment___strategy: Option<String>,
    pub resource____attributes____k8s___container___name: Option<String>,
    pub resource____attributes____k8s___node___name: Option<String>,
    pub resource____attributes____container___id: Option<String>,
    pub resource____attributes____container___image___name: Option<String>,
    pub resource____attributes____container___image___tag: Option<String>,
    pub resource____attributes____deployment___environment: Option<String>,
    pub resource____attributes____deployment___version: Option<String>,
    pub resource____attributes____cloud___provider: Option<String>,
    pub resource____attributes____cloud___platform: Option<String>,
    pub resource____attributes____cloud___region: Option<String>,
    pub resource____attributes____cloud___availability___zone: Option<String>,
    pub resource____attributes____cloud___account___id: Option<String>,
    pub resource____attributes____cloud___resource___id: Option<String>,
    pub resource____attributes____cloud___instance___type: Option<String>,
    pub resource____attributes____telemetry___sdk___name: Option<String>,
    pub resource____attributes____telemetry___sdk___language: Option<String>,
    pub resource____attributes____telemetry___sdk___version: Option<String>,
    pub resource____attributes____application___name: Option<String>,
    pub resource____attributes____application___version: Option<String>,
    pub resource____attributes____application___tier: Option<String>,
    pub resource____attributes____application___owner: Option<String>,
    pub resource____attributes____customer___id: Option<String>,
    pub resource____attributes____tenant___id: Option<String>,
    pub resource____attributes____feature___flag___enabled: Option<bool>,
    pub resource____attributes____payment___gateway: Option<String>,
    pub resource____attributes____database___type: Option<String>,
    pub resource____attributes____database___instance: Option<String>,
    pub resource____attributes____cache___provider: Option<String>,
    pub resource____attributes____message___queue___type: Option<String>,
    pub resource____attributes____http___route: Option<String>,
    pub resource____attributes____aws___ecs___cluster___arn: Option<String>,
    pub resource____attributes____aws___ecs___container___arn: Option<String>,
    pub resource____attributes____aws___ecs___task___arn: Option<String>,
    pub resource____attributes____aws___ecs___task___family: Option<String>,
    pub resource____attributes____aws___ec2___instance___id: Option<String>,
    pub resource____attributes____gcp___project___id: Option<String>,
    pub resource____attributes____gcp___zone: Option<String>,
    pub resource____attributes____azure___resource___id: Option<String>,
    pub resource____attributes____dynatrace___entity___process___id: Option<String>,
    pub resource____attributes____elastic___node___name: Option<String>,
    pub resource____attributes____istio___mesh___id: Option<String>,
    pub resource____attributes____cloudfoundry___application___id: Option<String>,
    pub resource____attributes____cloudfoundry___space___id: Option<String>,
    pub resource____attributes____opentelemetry___collector___name: Option<String>,
    pub resource____attributes____instrumentation___name: Option<String>,
    pub resource____attributes____instrumentation___version: Option<String>,
    pub resource____attributes____log___source: Option<String>,

    // Instrumentation Library
    pub instrumentationLibrary____name: Option<String>,
    pub instrumentationLibrary____version: Option<String>,
}

#[derive(Clone)]
pub struct PersistentQueue {
    path: PathBuf,
    file: Arc<Mutex<File>>,
    position: Arc<Mutex<u64>>,
}

impl PersistentQueue {
    pub async fn new(path: &str) -> Result<Self> {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await?;
        Ok(Self {
            path,
            file: Arc::new(Mutex::new(file)),
            position: Arc::new(Mutex::new(0)),
        })
    }

    pub async fn enqueue(&self, record: IngestRecord) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        let serialized = serde_json::to_string(&record)?;
        let len = serialized.len() as u64;
        file.write_all(serialized.as_bytes()).await?;
        file.write_all(b"\n").await?;
        file.flush().await?;

        let mut pos = self.position.lock().unwrap();
        *pos += len + 1; // +1 for newline
        Ok(())
    }

    pub async fn dequeue(&self) -> Result<Option<IngestRecord>> {
        let mut file = self.file.lock().unwrap();
        let mut pos = self.position.lock().unwrap();

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

        let record: IngestRecord = serde_json::from_str(&line.trim_end())?;
        let consumed = bytes_read as u64;
        drop(reader);

        *pos -= consumed;
        file.set_len(*pos).await?;
        file.seek(SeekFrom::Start(0)).await?;

        if *pos > 0 {
            let mut remaining = Vec::new();
            let mut new_reader = BufReader::new(&mut *file);
            new_reader.seek(SeekFrom::Start(consumed)).await?;
            new_reader.read_to_end(&mut remaining).await?;
            drop(new_reader);

            file.seek(SeekFrom::Start(0)).await?;
            file.write_all(&remaining).await?;
            file.flush().await?;
        }

        Ok(Some(record))
    }

    pub async fn len(&self) -> Result<usize> {
        // Reopen the file to avoid borrowing issues with the locked file
        let file = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .await?;
        let mut reader = BufReader::new(file);
        let mut count = 0;
        let mut line = String::new();

        reader.seek(SeekFrom::Start(0)).await?;
        while reader.read_line(&mut line).await? > 0 {
            if !line.trim().is_empty() {
                count += 1;
            }
            line.clear();
        }

        Ok(count)
    }
}