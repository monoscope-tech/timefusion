pub mod auth;
pub(crate) mod client;
mod handlers;
pub mod hooks;
mod planner;
#[cfg(any(test, debug_assertions))]
pub mod testing;

use std::fs::File;
use std::io::{BufReader, Error as IOError, ErrorKind};
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use getset::{Getters, Setters, WithSetters};
use log::{info, warn};
use pgwire::api::PgWireServerHandlers;
use pgwire::tokio::process_socket;
use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::{lookup_host, TcpListener, TcpSocket};
use tokio::sync::Semaphore;
use tokio_rustls::rustls::{self, ServerConfig};
use tokio_rustls::TlsAcceptor;

use handlers::HandlerFactory;
pub use handlers::{DfSessionService, Parser};
pub use hooks::QueryHook;

/// re-exports
pub use arrow_pg;
pub use datafusion_pg_catalog;
pub use pgwire;

#[derive(Getters, Setters, WithSetters, Debug)]
#[getset(get = "pub", set = "pub", set_with = "pub")]
pub struct ServerOptions {
    host: String,
    port: u16,
    tls_cert_path: Option<String>,
    tls_key_path: Option<String>,
    max_connections: usize,
    /// Listen backlog passed to `listen(2)`. Default 4096 — well above mio's
    /// hardcoded 128 in `TcpListener::bind`. The OS will clamp to
    /// `net.core.somaxconn` (Linux) / `kern.ipc.somaxconn` (macOS), so
    /// raising both is required for the larger queue to take effect.
    backlog: u32,
}

impl ServerOptions {
    pub fn new() -> ServerOptions {
        ServerOptions::default()
    }
}

impl Default for ServerOptions {
    fn default() -> Self {
        ServerOptions {
            host: "127.0.0.1".to_string(),
            port: 5432,
            tls_cert_path: None,
            tls_key_path: None,
            max_connections: 0, // 0 = no limit
            backlog: 4096,
        }
    }
}

/// Set up TLS configuration if certificate and key paths are provided
fn setup_tls(cert_path: &str, key_path: &str) -> Result<TlsAcceptor, IOError> {
    // Install ring crypto provider for rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert = certs(&mut BufReader::new(File::open(cert_path)?)).collect::<Result<Vec<CertificateDer>, IOError>>()?;

    let key = pkcs8_private_keys(&mut BufReader::new(File::open(key_path)?))
        .map(|key| key.map(PrivateKeyDer::from))
        .collect::<Result<Vec<PrivateKeyDer>, IOError>>()?
        .into_iter()
        .next()
        .ok_or_else(|| IOError::new(ErrorKind::InvalidInput, "No private key found"))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert, key)
        .map_err(|err| IOError::new(ErrorKind::InvalidInput, err))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Serve the Datafusion `SessionContext` with Postgres protocol.
pub async fn serve(session_context: Arc<SessionContext>, opts: &ServerOptions) -> Result<(), std::io::Error> {
    #[cfg(feature = "postgis")]
    geodatafusion::register(&session_context);

    // Create the handler factory with authentication
    let factory = Arc::new(HandlerFactory::new(session_context));

    serve_with_handlers(factory, opts, std::future::pending::<()>()).await
}

/// Serve the Datafusion `SessionContext` with Postgres protocol, using custom
/// query processing hooks.
pub async fn serve_with_hooks(session_context: Arc<SessionContext>, opts: &ServerOptions, hooks: Vec<Arc<dyn QueryHook>>) -> Result<(), std::io::Error> {
    #[cfg(feature = "postgis")]
    geodatafusion::register(&session_context);

    // Create the handler factory with authentication
    let factory = Arc::new(HandlerFactory::new_with_hooks(session_context, hooks));

    serve_with_handlers(factory, opts, std::future::pending::<()>()).await
}

/// Serve with custom pgwire handlers
///
/// This function allows you to rewrite some of the built-in logic including
/// authentication and query processing. You can Implement your own
/// `PgWireServerHandlers` by reusing `DfSessionService`.
///
/// `shutdown` is a future that, when it resolves, stops the accept loop.
/// Already-accepted connections keep going on their spawned tasks — the
/// listener just stops minting new ones. Pass `std::future::pending()` (or
/// equivalent never-firing future) if you don't need shutdown signalling.
pub async fn serve_with_handlers(
    handlers: Arc<impl PgWireServerHandlers + Sync + Send + 'static>, opts: &ServerOptions, shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), std::io::Error> {
    // Bind to the specified host and port. Use `TcpSocket` instead of
    // `TcpListener::bind` so we can request a real backlog — mio hardcodes
    // 128, which trivially overflows under thundering-herd reconnects (e.g.
    // background jobs retrying on exp-backoff) and produces ECONNREFUSED on
    // hosts with `tcp_abort_on_overflow=1`. The kernel will still clamp to
    // `somaxconn`, so the host sysctl must also be raised in deploy config.
    let server_addr = format!("{}:{}", opts.host, opts.port);
    let addr = lookup_host(&server_addr)
        .await?
        .next()
        .ok_or_else(|| IOError::new(ErrorKind::InvalidInput, format!("could not resolve {server_addr}")))?;
    let socket = if addr.is_ipv4() { TcpSocket::new_v4()? } else { TcpSocket::new_v6()? };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    let listener = socket.listen(opts.backlog)?;
    serve_with_listener(listener, handlers, opts, shutdown).await
}

/// Like `serve_with_handlers`, but accepts an already-bound `TcpListener`.
/// Useful when the caller wants to bind the socket early (e.g. to handle the
/// pgwire startup-time window with a 57P03 "starting up" responder, then hand
/// the same listener to the real server once initialization is complete —
/// avoiding the unbound-port window that causes ECONNREFUSED at clients
/// during slow startup).
pub async fn serve_with_listener(
    listener: TcpListener, handlers: Arc<impl PgWireServerHandlers + Sync + Send + 'static>, opts: &ServerOptions,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), std::io::Error> {
    // Set up TLS if configured
    let tls_acceptor = if let (Some(cert_path), Some(key_path)) = (&opts.tls_cert_path, &opts.tls_key_path) {
        match setup_tls(cert_path, key_path) {
            Ok(acceptor) => {
                info!("TLS enabled using cert: {cert_path} and key: {key_path}");
                Some(acceptor)
            }
            Err(e) => {
                warn!("Failed to setup TLS: {e}. Running without encryption.");
                None
            }
        }
    } else {
        info!("TLS not configured. Running without encryption.");
        None
    };

    let local_addr = listener.local_addr().map(|a| a.to_string()).unwrap_or_else(|_| "<unknown>".to_string());
    if tls_acceptor.is_some() {
        info!("Listening on {local_addr} with TLS encryption");
    } else {
        info!("Listening on {local_addr} (unencrypted)");
    }

    // Connection limiter (if configured)
    let max_conn_count = opts.max_connections;
    let connection_limiter = if max_conn_count > 0 { Some(Arc::new(Semaphore::new(max_conn_count))) } else { None };

    // Accept incoming connections until `shutdown` resolves. Existing
    // connections keep going on their spawned tasks — they're not cancelled
    // here; the caller is responsible for waiting for them to drain.
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                info!("PGWire: shutdown signal received, stopping accept loop");
                break;
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((socket, addr)) => {
                        let factory_ref = handlers.clone();
                        let tls_acceptor_ref = tls_acceptor.clone();
                        let limiter_ref = connection_limiter.clone();

                        tokio::spawn(async move {
                            let _permit = if let Some(ref semaphore) = limiter_ref {
                                match semaphore.try_acquire() {
                                    Ok(permit) => Some(permit),
                                    Err(_) => {
                                        warn!("Connection rejected from {addr}: max connections ({max_conn_count}) reached");
                                        return;
                                    }
                                }
                            } else {
                                None
                            };

                            if let Err(e) = process_socket(socket, tls_acceptor_ref, factory_ref).await {
                                warn!("Error processing socket from {addr}: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        warn!("Error accept socket: {e}");
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_options_default_max_connections() {
        let opts = ServerOptions::default();
        assert_eq!(opts.max_connections, 0); // No limit by default
    }

    #[test]
    fn test_server_options_max_connections_configuration() {
        let opts = ServerOptions::new().with_max_connections(500);
        assert_eq!(opts.max_connections, 500);

        // Test that 0 means no limit
        let opts_no_limit = ServerOptions::new().with_max_connections(0);
        assert_eq!(opts_no_limit.max_connections, 0);
    }
}
