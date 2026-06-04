//! Early-bind responder that occupies `:5432` during the slow startup
//! window (WAL replay + Delta-table open + foyer init), returning the
//! Postgres SQLSTATE 57P03 "the database system is starting up" error
//! to every connection until the real server is ready to take over.
//!
//! Without this, the kernel inside the timefusion container has no
//! listener on 5432 for the entire startup window (8–15 min in prod
//! after a 346 GB WAL has accumulated), so packets DNAT'd in by Docker
//! get RST'd back to the client as ECONNREFUSED. Background-job clients
//! like monoscope's Hasql see this as a hard error and burn their retry
//! budget on a startup window that's actually progressing fine.
//!
//! 57P03 is the canonical "I'm coming up" error class in Postgres —
//! clients (Hasql, pgjdbc, libpq) recognise it and back off sanely.
//!
//! The acceptor speaks just enough of the pgwire protocol to send the
//! error before closing:
//! - Reject SSL / GSSAPI requests with `N` (no encryption).
//! - Drain the StartupMessage payload.
//! - Emit a single `ErrorResponse(FATAL, 57P03, "...")`.
//! - Close the socket.
//!
//! It does NOT authenticate, query, or hold state. Pass the same
//! `TcpListener` to `datafusion_postgres::serve_with_listener` once
//! ready — no socket rebind, no ECONNREFUSED window.

use std::io;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

const SSL_REQUEST_CODE: u32 = 80877103;
const GSS_REQUEST_CODE: u32 = 80877104;
const MAX_STARTUP_BYTES: usize = 64 * 1024;

/// Run the 57P03 acceptor on `listener` until `shutdown` is cancelled.
/// Already-accepted connections finish on their own spawned tasks.
pub async fn run_until_ready(listener: &TcpListener, shutdown: CancellationToken) {
    let response = build_starting_up_response();
    let response = std::sync::Arc::<[u8]>::from(response.into_boxed_slice());
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                debug!("early-bind: shutdown signal received, stopping acceptor");
                return;
            }
            res = listener.accept() => {
                match res {
                    Ok((sock, addr)) => {
                        let resp = std::sync::Arc::clone(&response);
                        tokio::spawn(async move {
                            if let Err(e) = handle_one(sock, &resp).await {
                                debug!("early-bind: short-circuit conn from {addr}: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        warn!("early-bind: accept failed: {e}");
                    }
                }
            }
        }
    }
}

async fn handle_one(mut sock: TcpStream, response: &[u8]) -> io::Result<()> {
    // First message is either an SSLRequest (8 bytes), GSSENCRequest (8 bytes),
    // or a StartupMessage (length-prefixed). Both SSL/GSS requests start with
    // the same length=8, so we read the length, then the 4-byte code.
    let len = read_u32(&mut sock).await? as usize;
    if len < 8 || len > MAX_STARTUP_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("startup length out of range: {len}")));
    }
    let code = read_u32(&mut sock).await?;
    if code == SSL_REQUEST_CODE || code == GSS_REQUEST_CODE {
        // Refuse the optional encryption negotiation with a single byte 'N',
        // then expect the real StartupMessage to follow.
        sock.write_all(b"N").await?;
        sock.flush().await?;
        let real_len = read_u32(&mut sock).await? as usize;
        if real_len < 8 || real_len > MAX_STARTUP_BYTES {
            return Err(io::Error::new(io::ErrorKind::InvalidData, format!("startup length out of range: {real_len}")));
        }
        // Discard the rest of the StartupMessage body — we don't care which
        // user/database the client asked for; everyone gets 57P03 right now.
        let body_remaining = real_len.saturating_sub(4);
        discard(&mut sock, body_remaining).await?;
    } else {
        // First message WAS the StartupMessage. We've consumed length + code;
        // drain the parameter pairs that follow.
        let body_remaining = len.saturating_sub(8);
        discard(&mut sock, body_remaining).await?;
    }

    // Send the canned 57P03 ErrorResponse and close.
    sock.write_all(response).await?;
    sock.flush().await?;
    let _ = sock.shutdown().await;
    Ok(())
}

async fn read_u32(sock: &mut TcpStream) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    sock.read_exact(&mut buf).await?;
    Ok(u32::from_be_bytes(buf))
}

async fn discard(sock: &mut TcpStream, mut n: usize) -> io::Result<()> {
    let mut scratch = [0u8; 1024];
    while n > 0 {
        let take = scratch.len().min(n);
        let got = sock.read(&mut scratch[..take]).await?;
        if got == 0 {
            break;
        }
        n -= got;
    }
    Ok(())
}

/// Pre-built ErrorResponse frame for SQLSTATE 57P03 (cannot_connect_now —
/// "the database system is starting up"). Postgres clients (libpq, Hasql,
/// pgjdbc, asyncpg) all recognise this class and apply their reconnect
/// policy rather than treating it as a hard failure.
///
/// Wire format (per pg docs):
///   Byte1('E') Int32(length) [Field(byte tag) String(value)]* Byte1(0)
fn build_starting_up_response() -> Vec<u8> {
    let mut body: Vec<u8> = Vec::with_capacity(96);
    push_field(&mut body, b'S', b"FATAL");
    push_field(&mut body, b'V', b"FATAL");
    push_field(&mut body, b'C', b"57P03");
    push_field(&mut body, b'M', b"the database system is starting up");
    body.push(0); // terminator

    let length = (body.len() + 4) as u32;
    let mut msg = Vec::with_capacity(1 + 4 + body.len());
    msg.push(b'E');
    msg.extend_from_slice(&length.to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

fn push_field(out: &mut Vec<u8>, tag: u8, value: &[u8]) {
    out.push(tag);
    out.extend_from_slice(value);
    out.push(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_frame_has_expected_shape() {
        let msg = build_starting_up_response();
        assert_eq!(msg[0], b'E', "ErrorResponse frame tag");
        let len = u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]) as usize;
        assert_eq!(len, msg.len() - 1, "length field counts itself + body");
        let body = &msg[5..];
        assert!(body.windows(b"57P03".len()).any(|w| w == b"57P03"), "SQLSTATE 57P03 must be present");
        assert!(body.last() == Some(&0u8), "frame must end with terminator");
    }

    #[tokio::test]
    async fn responds_to_plain_startup_then_closes() {
        use tokio::io::AsyncReadExt;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown = CancellationToken::new();
        let acceptor_token = shutdown.clone();
        let acceptor = tokio::spawn(async move {
            run_until_ready(&listener, acceptor_token).await;
        });

        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        // Minimal StartupMessage: len=8 + protocol_version=196608 (3.0). No params.
        let len: u32 = 8;
        let proto: u32 = 196608;
        client.write_all(&len.to_be_bytes()).await.unwrap();
        client.write_all(&proto.to_be_bytes()).await.unwrap();

        let mut tag = [0u8; 1];
        client.read_exact(&mut tag).await.unwrap();
        assert_eq!(tag[0], b'E', "server must send ErrorResponse");
        let mut len_buf = [0u8; 4];
        client.read_exact(&mut len_buf).await.unwrap();
        let body_len = u32::from_be_bytes(len_buf) as usize - 4;
        let mut body = vec![0u8; body_len];
        client.read_exact(&mut body).await.unwrap();
        assert!(body.windows(5).any(|w| w == b"57P03"), "expected SQLSTATE 57P03 in body");

        // Server must close after the error.
        let mut tail = [0u8; 1];
        let n = client.read(&mut tail).await.unwrap();
        assert_eq!(n, 0, "server must close after sending the error");

        shutdown.cancel();
        let _ = acceptor.await;
    }

    #[tokio::test]
    async fn responds_to_ssl_request_then_startup() {
        use tokio::io::AsyncReadExt;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown = CancellationToken::new();
        let acceptor_token = shutdown.clone();
        let acceptor = tokio::spawn(async move {
            run_until_ready(&listener, acceptor_token).await;
        });

        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        // SSLRequest: len=8 + magic=80877103.
        let len: u32 = 8;
        let ssl_code: u32 = SSL_REQUEST_CODE;
        client.write_all(&len.to_be_bytes()).await.unwrap();
        client.write_all(&ssl_code.to_be_bytes()).await.unwrap();
        let mut n_reply = [0u8; 1];
        client.read_exact(&mut n_reply).await.unwrap();
        assert_eq!(n_reply[0], b'N', "server must refuse SSL with 'N'");

        // Now the real StartupMessage.
        let proto: u32 = 196608;
        client.write_all(&len.to_be_bytes()).await.unwrap();
        client.write_all(&proto.to_be_bytes()).await.unwrap();

        let mut tag = [0u8; 1];
        client.read_exact(&mut tag).await.unwrap();
        assert_eq!(tag[0], b'E', "server must send ErrorResponse after the SSL rejection");

        shutdown.cancel();
        let _ = acceptor.await;
    }
}
