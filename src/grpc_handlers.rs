//! gRPC ingestion service. Bidi-streaming endpoint that accepts Arrow IPC
//! payloads and forwards them to the BufferedWriteLayer via Database.
//!
//! Auth: optional static bearer token in `authorization: Bearer <token>` metadata,
//! validated against `CoreConfig::grpc_token`. When unset, the endpoint is open
//! (intended for trusted-network deployments / development).

use std::{io::Cursor, sync::Arc};

use anyhow::Context;
use arrow::array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};

use crate::database::Database;

/// Pressure threshold above which we soft-reject with RETRY instead of
/// admitting the write. Keeps a margin below the hard reservation limit so
/// well-behaved clients throttle before any write actually fails.
const RETRY_PRESSURE_PCT: u32 = 85;
/// Max concurrent in-flight decode+insert tasks per stream. Bounds memory
/// amplification from a single misbehaving client.
const STREAM_CONCURRENCY: usize = 16;

pub mod pb {
    tonic::include_proto!("timefusion.v1");
}

use pb::{
    WriteAck, WriteBatch,
    ingest_server::{Ingest, IngestServer},
    write_ack::Status as AckStatus,
};

pub struct IngestService {
    db: Arc<Database>,
    token: Option<String>,
}

impl IngestService {
    pub fn new(db: Arc<Database>, token: Option<String>) -> Self {
        Self { db, token }
    }

    pub fn into_server(self) -> IngestServer<Self> {
        IngestServer::new(self)
    }

    fn check_auth<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let got = req.metadata().get("authorization").and_then(|v| v.to_str().ok()).and_then(|s| s.strip_prefix("Bearer "));
        verify_bearer(self.token.as_deref(), got)
    }
}

/// Stream-decode an Arrow IPC payload and forward each batch to `sink` as it
/// is materialized. Bounded peak memory: only one decoded batch is alive at a
/// time on top of the encoded bytes. Empty / row-less batches are skipped.
/// Returns the number of non-empty batches inserted.
async fn decode_and_insert<F, Fut>(bytes: &[u8], mut sink: F) -> anyhow::Result<usize>
where
    F: FnMut(RecordBatch) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    // Imperative: each item awaits the sink and short-circuits on error, and the
    // one-batch-alive-at-a-time property depends on not collecting the reader.
    let mut count = 0usize;
    for batch in StreamReader::try_new(Cursor::new(bytes), None).context("arrow ipc reader")? {
        let batch = batch.context("arrow ipc decode")?;
        if batch.num_rows() > 0 {
            sink(batch).await?;
            count += 1;
        }
    }
    Ok(count)
}

#[tonic::async_trait]
impl Ingest for IngestService {
    type WriteStream = ReceiverStream<Result<WriteAck, Status>>;

    async fn write(&self, req: Request<Streaming<WriteBatch>>) -> Result<Response<Self::WriteStream>, Status> {
        self.check_auth(&req)?;
        let inbound = req.into_inner();
        let (tx, rx) = mpsc::channel::<Result<WriteAck, Status>>(64);
        let db = Arc::clone(&self.db);

        tokio::spawn(async move {
            // Process batches concurrently within a single stream. `buffer_unordered`
            // caps in-flight work; acks may arrive out of seq order — clients track
            // outstanding seqs themselves.
            let mut acks = inbound
                .map(|item| {
                    let db = Arc::clone(&db);
                    async move { Ok(process_one(&db, item?).await) }
                })
                .buffer_unordered(STREAM_CONCURRENCY);

            while let Some(result) = acks.next().await {
                if let Ok(ack) = &result {
                    debug!(seq = ack.seq, status = ?ack.status, pct = ack.mem_pressure_pct, "grpc write ack");
                }
                if tx.send(result).await.is_err() {
                    warn!("grpc client dropped stream mid-flight");
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn process_one(db: &Database, msg: WriteBatch) -> WriteAck {
    let WriteBatch { seq, project_id, table_name, arrow_ipc } = msg;
    let pressure = db.buffered_layer().map_or(0, |l| l.pressure_pct());
    let ack = |status: AckStatus, error: String| WriteAck { seq, status: status as i32, mem_pressure_pct: pressure, error };

    // Soft backpressure: refuse before the hard limit so clients throttle gracefully.
    if pressure >= RETRY_PRESSURE_PCT {
        return ack(AckStatus::Retry, format!("mem pressure {pressure}% ≥ {RETRY_PRESSURE_PCT}%"));
    }

    // Stream batches into the buffered layer one at a time so peak memory per
    // request is one decoded batch (plus the encoded payload), not the entire
    // decoded set. Any decode or insert error fails the whole request — the
    // client retries the seq. The per-batch clones are required: the sink's
    // future must own its inputs (it cannot borrow the closure's captures).
    let inserted = decode_and_insert(&arrow_ipc, |batch| {
        let (project_id, table_name) = (project_id.clone(), table_name.clone());
        async move {
            db.insert_records_batch(&project_id, &table_name, vec![batch], false, None).await?;
            Ok(())
        }
    })
    .await;

    match inserted {
        Ok(0) => ack(AckStatus::Reject, "empty arrow ipc payload".into()),
        Ok(_) => ack(AckStatus::Ok, String::new()),
        Err(e) => ack(AckStatus::Reject, format!("decode/insert: {e:#}")),
    }
}

/// Constant-time bearer-token check. When `expected` is `None`, auth is open.
/// Both sides are SHA-256-hashed first so the constant-time compare runs over
/// fixed-length 32-byte digests — this removes the token-length side channel
/// that `ct_eq` on raw bytes would leak via the early length-mismatch exit.
fn verify_bearer(expected: Option<&str>, got: Option<&str>) -> Result<(), Status> {
    let Some(expected) = expected else { return Ok(()) };
    let digest = |s: &str| Sha256::digest(s.as_bytes());
    got.is_some_and(|g| bool::from(digest(g).ct_eq(&digest(expected)))).then_some(()).ok_or_else(|| Status::unauthenticated("invalid or missing bearer token"))
}

#[cfg(test)]
mod auth_tests {
    use super::verify_bearer;

    #[test]
    fn rejects_wrong_same_length_token() {
        let err = verify_bearer(Some("abcdef"), Some("zzzzzz")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }
    #[test]
    fn rejects_different_length_token() {
        // Hashing both sides means length differences don't short-circuit:
        // the ct_eq still runs over 32-byte digests.
        let err = verify_bearer(Some("abcdef"), Some("zzz")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        let err = verify_bearer(Some("abc"), Some("abcdefghij")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }
    #[test]
    fn rejects_missing_token() {
        assert!(verify_bearer(Some("abcdef"), None).is_err());
    }
    #[test]
    fn accepts_correct_token() {
        assert!(verify_bearer(Some("abcdef"), Some("abcdef")).is_ok());
    }
    #[test]
    fn open_when_unconfigured() {
        assert!(verify_bearer(None, None).is_ok());
        assert!(verify_bearer(None, Some("anything")).is_ok());
    }
}
