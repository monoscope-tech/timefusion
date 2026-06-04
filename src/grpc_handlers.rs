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
    db:    Arc<Database>,
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
    let reader = StreamReader::try_new(Cursor::new(bytes), None).context("arrow ipc reader")?;
    let mut count = 0usize;
    for batch in reader {
        let batch = batch.context("arrow ipc decode")?;
        if batch.num_rows() == 0 {
            continue;
        }
        sink(batch).await?;
        count += 1;
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
                    async move {
                        match item {
                            Ok(msg) => Ok(process_one(&db, msg).await),
                            Err(e) => Err(e),
                        }
                    }
                })
                .buffer_unordered(STREAM_CONCURRENCY);

            while let Some(result) = acks.next().await {
                let send = match result {
                    Ok(ack) => {
                        debug!(seq = ack.seq, status = ?ack.status, pct = ack.mem_pressure_pct, "grpc write ack");
                        tx.send(Ok(ack)).await
                    }
                    Err(e) => tx.send(Err(e)).await,
                };
                if send.is_err() {
                    warn!("grpc client dropped stream mid-flight");
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn process_one(db: &Database, msg: WriteBatch) -> WriteAck {
    let seq = msg.seq;
    let pressure = db.buffered_layer().map(|l| l.pressure_pct()).unwrap_or(0);

    // Soft backpressure: refuse before the hard limit so clients throttle gracefully.
    if pressure >= RETRY_PRESSURE_PCT {
        return WriteAck {
            seq,
            status: AckStatus::Retry as i32,
            mem_pressure_pct: pressure,
            error: format!("mem pressure {pressure}% ≥ {RETRY_PRESSURE_PCT}%"),
        };
    }

    // Stream batches into the buffered layer one at a time so peak memory per
    // request is one decoded batch (plus the encoded payload), not the entire
    // decoded set. Any decode or insert error fails the whole request — the
    // client retries the seq.
    let project_id = msg.project_id;
    let table_name = msg.table_name;
    let result = decode_and_insert(&msg.arrow_ipc, |batch| {
        let project_id = project_id.clone();
        let table_name = table_name.clone();
        async move { db.insert_records_batch(&project_id, &table_name, vec![batch], false, None).await }
    })
    .await;

    match result {
        Ok(0) => ack_err(seq, pressure, "empty arrow ipc payload"),
        Ok(_) => WriteAck {
            seq,
            status: AckStatus::Ok as i32,
            mem_pressure_pct: pressure,
            error: String::new(),
        },
        Err(e) => ack_err(seq, pressure, &format!("decode/insert: {e:#}")),
    }
}

fn ack_err(seq: u64, pressure: u32, err: &str) -> WriteAck {
    WriteAck {
        seq,
        status: AckStatus::Reject as i32,
        mem_pressure_pct: pressure,
        error: err.into(),
    }
}

/// Constant-time bearer-token check. When `expected` is `None`, auth is open.
/// Both sides are SHA-256-hashed first so the constant-time compare runs over
/// fixed-length 32-byte digests — this removes the token-length side channel
/// that `ct_eq` on raw bytes would leak via the early length-mismatch exit.
fn verify_bearer(expected: Option<&str>, got: Option<&str>) -> Result<(), Status> {
    let Some(expected) = expected else { return Ok(()) };
    let Some(got) = got else {
        return Err(Status::unauthenticated("invalid or missing bearer token"));
    };
    let e = Sha256::digest(expected.as_bytes());
    let g = Sha256::digest(got.as_bytes());
    if bool::from(e.ct_eq(&g)) {
        Ok(())
    } else {
        Err(Status::unauthenticated("invalid or missing bearer token"))
    }
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
