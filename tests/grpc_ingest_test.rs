//! Integration test for gRPC ingestion: spins up the IngestService against a
//! real Database+BufferedWriteLayer, drives it via an in-memory duplex transport,
//! and verifies Arrow IPC payloads land in the buffer.

use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use serial_test::serial;
use timefusion::{
    buffered_write_layer::BufferedWriteLayer,
    database::Database,
    grpc_handlers::{
        IngestService,
        pb::{WriteBatch, ingest_client::IngestClient, write_ack::Status as AckStatus},
    },
    test_utils::test_helpers::{BufferMode, TestConfigBuilder, json_to_batch, test_span},
};
use tokio::io::DuplexStream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Endpoint, Server, Uri};

fn encode_ipc(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

async fn make_client(svc: IngestService) -> IngestClient<tonic::transport::Channel> {
    let (client, server) = tokio::io::duplex(64 * 1024);
    let mut server = Some(server);
    tokio::spawn(async move {
        Server::builder()
            .add_service(svc.into_server())
            .serve_with_incoming(tokio_stream::once(Ok::<DuplexStream, std::io::Error>(server.take().unwrap())))
            .await
            .unwrap();
    });

    let mut client = Some(client);
    let channel = Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let c = client.take().unwrap();
            async move { Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(c)) }
        }))
        .await
        .unwrap();
    IngestClient::new(channel)
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn grpc_write_round_trip() -> Result<()> {
    let cfg = TestConfigBuilder::new("grpc_test").with_buffer_mode(BufferMode::Enabled).build();
    // SAFETY: walrus-rust uses a process-global env var; #[serial] guards it.
    unsafe { std::env::set_var("WALRUS_DATA_DIR", &cfg.core.timefusion_data_dir) };
    let layer = Arc::new(BufferedWriteLayer::with_config(Arc::clone(&cfg), timefusion::functions::function_registry()?)?);
    let db = Arc::new(Database::with_config(cfg).await?.with_buffered_layer(Arc::clone(&layer)));

    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let table_name = "otel_traces_and_logs".to_string();
    let batch = json_to_batch(vec![test_span("t1", "s1", &project_id), test_span("t2", "s2", &project_id)])?;
    let payload = encode_ipc(&batch);

    let mut client = make_client(IngestService::new(Arc::clone(&db), None)).await;

    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(WriteBatch {
        seq:        1,
        project_id: project_id.clone(),
        table_name: table_name.clone(),
        arrow_ipc:  payload.clone(),
    })
    .await?;
    tx.send(WriteBatch {
        seq: 2,
        project_id: project_id.clone(),
        table_name,
        arrow_ipc: payload,
    })
    .await?;
    drop(tx);

    let mut acks = client.write(ReceiverStream::new(rx)).await?.into_inner();
    let mut ok_count = 0;
    while let Some(ack) = acks.message().await? {
        assert_eq!(ack.status, AckStatus::Ok as i32, "expected OK, got {:?} err={}", ack.status, ack.error);
        assert!(ack.mem_pressure_pct <= 100);
        ok_count += 1;
    }
    assert_eq!(ok_count, 2);

    // Verify rows landed in the buffer
    let results = layer.query(&project_id, "otel_traces_and_logs", &[])?;
    let total: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 4, "expected 2 batches × 2 rows");
    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn grpc_rejects_bad_payload() -> Result<()> {
    let cfg = TestConfigBuilder::new("grpc_test").with_buffer_mode(BufferMode::Enabled).build();
    unsafe { std::env::set_var("WALRUS_DATA_DIR", &cfg.core.timefusion_data_dir) };
    let layer = Arc::new(BufferedWriteLayer::with_config(Arc::clone(&cfg), timefusion::functions::function_registry()?)?);
    let db = Arc::new(Database::with_config(cfg).await?.with_buffered_layer(layer));

    let mut client = make_client(IngestService::new(db, None)).await;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(WriteBatch {
        seq:        7,
        project_id: "p".into(),
        table_name: "otel_traces_and_logs".into(),
        arrow_ipc:  vec![0xde, 0xad],
    })
    .await?;
    drop(tx);

    let mut acks = client.write(ReceiverStream::new(rx)).await?.into_inner();
    let ack = acks.message().await?.expect("expected one ack");
    assert_eq!(ack.seq, 7);
    assert_eq!(ack.status, AckStatus::Reject as i32);
    assert!(ack.error.contains("decode"), "expected decode error, got: {}", ack.error);
    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn grpc_auth_rejects_missing_token() -> Result<()> {
    let cfg = TestConfigBuilder::new("grpc_test").with_buffer_mode(BufferMode::Enabled).build();
    unsafe { std::env::set_var("WALRUS_DATA_DIR", &cfg.core.timefusion_data_dir) };
    let layer = Arc::new(BufferedWriteLayer::with_config(Arc::clone(&cfg), timefusion::functions::function_registry()?)?);
    let db = Arc::new(Database::with_config(cfg).await?.with_buffered_layer(layer));

    let mut client = make_client(IngestService::new(db, Some("s3cret".into()))).await;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(WriteBatch {
        seq:        1,
        project_id: "p".into(),
        table_name: "otel_traces_and_logs".into(),
        arrow_ipc:  vec![],
    })
    .await?;
    drop(tx);

    let err = client.write(ReceiverStream::new(rx)).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    Ok(())
}
