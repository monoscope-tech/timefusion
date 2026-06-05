//! Early-bind responder that occupies `:5432` during the slow startup
//! window (WAL replay, Delta-table open, foyer init), returning the
//! Postgres SQLSTATE 57P03 "the database system is starting up" error
//! to every connection until the real server takes over the listener.
//!
//! Without this, packets DNAT'd into the container during the multi-minute
//! startup get RST'd back as ECONNREFUSED — clients like Hasql / pgjdbc /
//! libpq treat 57P03 as transient and back off cleanly, ECONNREFUSED as a
//! hard error. Hand the same `TcpListener` to `serve_with_listener` once
//! ready: no rebind, no ECONNREFUSED window.

use std::io;
use std::sync::Arc;

use datafusion_postgres::pgwire::messages::startup::{GssEncRequest, SslRequest};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

const SSL_REQUEST_CODE: u32 = SslRequest::BODY_MAGIC_NUMBER as u32;
const GSS_REQUEST_CODE: u32 = GssEncRequest::BODY_MAGIC_NUMBER as u32;
const MAX_STARTUP_BYTES: u64 = 64 * 1024;
const STARTUP_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Run the 57P03 acceptor on `listener` until `shutdown` is cancelled.
pub async fn run_until_ready(listener: &TcpListener, shutdown: CancellationToken) {
    let response: Arc<[u8]> = build_starting_up_response().into();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            res = listener.accept() => match res {
                Ok((sock, addr)) => {
                    let resp = Arc::clone(&response);
                    tokio::spawn(async move {
                        match tokio::time::timeout(STARTUP_READ_TIMEOUT, handle_one(sock, &resp)).await {
                            Err(_) => debug!("early-bind: timeout waiting for startup from {addr}"),
                            Ok(Err(e)) => debug!("early-bind: short-circuit conn from {addr}: {e}"),
                            Ok(Ok(())) => {}
                        }
                    });
                }
                Err(e) => warn!("early-bind: accept failed: {e}"),
            },
        }
    }
}

async fn handle_one(mut sock: TcpStream, response: &[u8]) -> io::Result<()> {
    // SSL/GSS negotiation precedes the real StartupMessage; both are 8 bytes
    // (length + magic). Drain whichever shape arrives, then send 57P03.
    let len = sock.read_u32().await? as u64;
    let code = sock.read_u32().await?;
    if code == SSL_REQUEST_CODE || code == GSS_REQUEST_CODE {
        sock.write_all(b"N").await?;
        let real_len = sock.read_u32().await? as u64;
        drain_body(&mut sock, real_len.checked_sub(4)).await?;
    } else {
        drain_body(&mut sock, len.checked_sub(8)).await?;
    }

    sock.write_all(response).await?;
    let _ = sock.shutdown().await;
    Ok(())
}

async fn drain_body(sock: &mut TcpStream, remaining: Option<u64>) -> io::Result<()> {
    let n = remaining.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "startup length below header"))?;
    if n > MAX_STARTUP_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("startup body too large: {n}")));
    }
    tokio::io::copy(&mut sock.take(n), &mut tokio::io::sink()).await?;
    Ok(())
}

/// Wire format: `Byte1('E') Int32(length) [Byte1(tag) String(value)]* Byte1(0)`
fn build_starting_up_response() -> Vec<u8> {
    let mut body: Vec<u8> = Vec::with_capacity(96);
    for (tag, value) in [(b'S', "FATAL"), (b'V', "FATAL"), (b'C', "57P03"), (b'M', "the database system is starting up")] {
        body.push(tag);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);

    let length = (body.len() + 4) as u32;
    let mut msg = Vec::with_capacity(1 + 4 + body.len());
    msg.push(b'E');
    msg.extend_from_slice(&length.to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_frame_has_expected_shape() {
        let msg = build_starting_up_response();
        assert_eq!(msg[0], b'E');
        let len = u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]) as usize;
        assert_eq!(len, msg.len() - 1);
        let body = &msg[5..];
        assert!(body.windows(5).any(|w| w == b"57P03"));
        assert_eq!(body.last(), Some(&0u8));
    }

    async fn spawn_acceptor() -> (u16, CancellationToken, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        let task = tokio::spawn(async move { run_until_ready(&listener, token).await });
        (port, shutdown, task)
    }

    async fn assert_57p03(client: &mut TcpStream) {
        let mut tag = [0u8; 1];
        client.read_exact(&mut tag).await.unwrap();
        assert_eq!(tag[0], b'E');
        let mut len_buf = [0u8; 4];
        client.read_exact(&mut len_buf).await.unwrap();
        let body_len = u32::from_be_bytes(len_buf) as usize - 4;
        let mut body = vec![0u8; body_len];
        client.read_exact(&mut body).await.unwrap();
        assert!(body.windows(5).any(|w| w == b"57P03"));
    }

    #[tokio::test]
    async fn responds_to_plain_startup_then_closes() {
        let (port, shutdown, task) = spawn_acceptor().await;
        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        client.write_all(&8u32.to_be_bytes()).await.unwrap();
        client.write_all(&196608u32.to_be_bytes()).await.unwrap();
        assert_57p03(&mut client).await;
        let mut tail = [0u8; 1];
        assert_eq!(client.read(&mut tail).await.unwrap(), 0, "server must close after error");
        shutdown.cancel();
        let _ = task.await;
    }

    async fn negotiation_then_startup(magic: u32) {
        let (port, shutdown, task) = spawn_acceptor().await;
        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        client.write_all(&8u32.to_be_bytes()).await.unwrap();
        client.write_all(&magic.to_be_bytes()).await.unwrap();
        let mut n_reply = [0u8; 1];
        client.read_exact(&mut n_reply).await.unwrap();
        assert_eq!(n_reply[0], b'N');
        client.write_all(&8u32.to_be_bytes()).await.unwrap();
        client.write_all(&196608u32.to_be_bytes()).await.unwrap();
        assert_57p03(&mut client).await;
        shutdown.cancel();
        let _ = task.await;
    }

    #[tokio::test]
    async fn responds_to_ssl_request_then_startup() {
        negotiation_then_startup(SSL_REQUEST_CODE).await;
    }

    #[tokio::test]
    async fn responds_to_gss_request_then_startup() {
        negotiation_then_startup(GSS_REQUEST_CODE).await;
    }
}
