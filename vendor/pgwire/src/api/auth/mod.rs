use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};

use super::{
    ClientInfo, ConnectionGuard, ConnectionHandle, METADATA_APPLICATION_NAME, METADATA_CLIENT_ENCODING, METADATA_DATABASE, METADATA_USER, PgWireConnectionState,
};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::response::{ReadyForQuery, TransactionStatus};
use crate::messages::startup::{Authentication, BackendKeyData, NegotiateProtocolVersion, ParameterStatus, Startup};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion};
use crate::types::format::FormatOptions;

/// Handles startup process and frontend messages
#[async_trait]
pub trait StartupHandler: Send + Sync {
    /// A generic frontend message callback during startup phase.
    async fn on_startup<C>(&self, client: &mut C, message: PgWireFrontendMessage) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;
}

/// Provides server parameters to send to the client during startup.
pub trait ServerParameterProvider: Send + Sync {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo;
}

/// Default noop parameter provider.
///
/// This provider responds frontend with default parameters:
///
/// - `server_version`: We will use a pattern like `<postgres
///   version>-pgwire-<pgwire version>` for compatibility
/// - `server_encoding`: `UTF8`
/// - `integer_datetimes`: `on` by following postgres defaults.
/// - `in_hot_standby`: `off`
/// - `search_path`: `public` by default
/// - `is_superuser`: `on` by default
/// - `default_transaction_read_only`: `off` by default
/// - `scram_iteration`: `4096` by default
/// - `DateStyle`: `ISO YMD` the default text serialization in this library is
///   using `YMD` style date.
/// - `IntervalStyle`: `postgres`
/// - `TimeZone`: `Etc/UTC` by default
/// - `standard_conforming_string`: `on` by default
/// - `client_encoding`: `UTF-8`. You can override this by providing a value, or
///   set to `None` to use client provided value.
/// - `application_name`: Follow the client provided value by default. You can
///   override this by providing a value.
/// - `session_authorization`: The user name, following the client provided
///   value. You can override this by providing a value.
#[non_exhaustive]
#[derive(Debug)]
pub struct DefaultServerParameterProvider {
    pub server_version: String,
    pub server_encoding: String,
    pub integer_datetimes: bool,
    pub in_hot_standby: bool,
    pub search_path: String,
    pub is_superuser: bool,
    pub default_transaction_read_only: bool,
    #[cfg(any(feature = "_aws-lc-rs", feature = "_ring"))]
    pub scram_iterations: usize,
    // format settings
    pub time_zone: String,
    pub date_style: String,
    pub interval_style: String,
    pub standard_conforming_strings: bool,
    // user name and application name, follow client data
    pub client_encoding: Option<String>,
    pub application_name: Option<String>,
    pub session_authorization: Option<String>,
}

impl Default for DefaultServerParameterProvider {
    fn default() -> Self {
        let format_options = FormatOptions::default();

        Self {
            server_version: format!("16.6-pgwire-{}", env!("CARGO_PKG_VERSION").to_owned()),
            server_encoding: "UTF8".to_owned(),
            integer_datetimes: true,
            in_hot_standby: false,
            search_path: "public".to_owned(),
            is_superuser: true,
            default_transaction_read_only: false,
            #[cfg(any(feature = "_aws-lc-rs", feature = "_ring"))]
            scram_iterations: sasl::scram::SCRAM_ITERATIONS,

            time_zone: format_options.time_zone,
            date_style: format_options.date_style,
            interval_style: format_options.interval_style,
            standard_conforming_strings: true,

            client_encoding: Some("UTF8".to_owned()),
            application_name: None,
            session_authorization: None,
        }
    }
}

fn bool_to_string(v: bool) -> String {
    if v { "on".to_string() } else { "off".to_string() }
}

impl ServerParameterProvider for DefaultServerParameterProvider {
    fn server_parameters<C>(&self, client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let mut params = HashMap::with_capacity(16);
        params.insert("server_version".to_owned(), self.server_version.clone());
        params.insert("server_encoding".to_owned(), self.server_encoding.clone());
        params.insert("integer_datetimes".to_owned(), bool_to_string(self.integer_datetimes));
        params.insert("in_hot_standby".to_owned(), bool_to_string(self.in_hot_standby));
        params.insert("search_path".to_owned(), self.search_path.clone());
        params.insert("is_superuser".to_owned(), bool_to_string(self.is_superuser));
        params.insert("default_transaction_read_only".to_owned(), bool_to_string(self.default_transaction_read_only));
        #[cfg(any(feature = "_aws-lc-rs", feature = "_ring"))]
        params.insert("scram_iterations".to_owned(), self.scram_iterations.to_string());

        params.insert("TimeZone".to_owned(), self.time_zone.clone());
        params.insert("DateStyle".to_owned(), self.date_style.clone());
        params.insert("IntervalStyle".to_owned(), self.interval_style.clone());
        params.insert("standard_conforming_strings".to_owned(), bool_to_string(self.standard_conforming_strings));

        if let Some(client_encoding) = self.client_encoding.as_ref().or_else(|| client.metadata().get(METADATA_CLIENT_ENCODING)) {
            params.insert(METADATA_CLIENT_ENCODING.to_owned(), client_encoding.clone());
        }
        if let Some(application_name) = self.application_name.as_ref().or_else(|| client.metadata().get(METADATA_APPLICATION_NAME)) {
            params.insert(METADATA_APPLICATION_NAME.to_owned(), application_name.clone());
        }
        if let Some(user) = self.session_authorization.as_ref().or_else(|| client.metadata().get(METADATA_USER)) {
            params.insert("session_authorization".to_owned(), user.clone());
        }

        Some(params)
    }
}

/// Represents a password with an optional salt.
#[derive(Debug, new, Clone)]
pub struct Password {
    salt: Option<Vec<u8>>,
    password: Vec<u8>,
}

impl Password {
    /// Returns the salt, if any.
    pub fn salt(&self) -> Option<&[u8]> {
        self.salt.as_deref()
    }

    /// Returns the password bytes.
    pub fn password(&self) -> &[u8] {
        &self.password
    }
}

/// Login information extracted from the client connection.
#[derive(Debug, new)]
pub struct LoginInfo<'a> {
    user: Option<&'a str>,
    database: Option<&'a str>,
    host: String,
}

impl LoginInfo<'_> {
    /// Returns the user name.
    pub fn user(&self) -> Option<&str> {
        self.user
    }

    /// Returns the database name.
    pub fn database(&self) -> Option<&str> {
        self.database
    }

    /// Returns the client host address.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Creates `LoginInfo` from a client.
    pub fn from_client_info<C>(client: &C) -> LoginInfo<'_>
    where
        C: ClientInfo,
    {
        LoginInfo {
            user: client.metadata().get(METADATA_USER).map(|s| s.as_str()),
            database: client.metadata().get(METADATA_DATABASE).map(|s| s.as_str()),
            host: client.socket_addr().ip().to_string(),
        }
    }
}

/// Represents auth source, the source returns password either in cleartext or
/// hashed with salt.
///
/// When using with different authentication mechanism, the developer can choose
/// specific implementation of `AuthSource`. For example, with cleartext
/// authentication, salt is not required, while in md5pass, a 4-byte salt is
/// needed.
#[async_trait]
pub trait AuthSource: Send + Sync + Debug {
    /// Get password from the `AuthSource`.
    ///
    /// `Password` has a an optional salt field when it's hashed.
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password>;
}

/// Negotiates the protocol version with the client.
pub async fn protocol_negotiation<C>(client: &mut C, startup_message: &Startup) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let unsupported_options = startup_message.parameters.keys().filter(|name| name.starts_with("_pq_.")).cloned().collect::<Vec<_>>();
    if let Some(protocol_version) = ProtocolVersion::from_version_number(startup_message.protocol_number_major, startup_message.protocol_number_minor) {
        if !unsupported_options.is_empty() {
            client.send(PgWireBackendMessage::NegotiateProtocolVersion(NegotiateProtocolVersion::new(protocol_version.into(), unsupported_options))).await?;
        }
        client.set_protocol_version(protocol_version);
        Ok(())
    } else {
        let newest_server_version = ProtocolVersion::default();
        let (newest_major_version, _newest_minor_version) = newest_server_version.version_number();
        if newest_major_version == startup_message.protocol_number_major {
            client
                .send(PgWireBackendMessage::NegotiateProtocolVersion(NegotiateProtocolVersion::new(newest_server_version.into(), unsupported_options)))
                .await?;
            client.set_protocol_version(newest_server_version);
            Ok(())
        } else {
            Err(PgWireError::UnsupportedProtocolVersion(startup_message.protocol_number_major, startup_message.protocol_number_minor))
        }
    }
}

/// Saves startup message parameters into client metadata.
pub fn save_startup_parameters_to_metadata<C>(client: &mut C, startup_message: &Startup)
where
    C: ClientInfo,
{
    client.metadata_mut().extend(startup_message.parameters.iter().map(|(k, v)| (k.to_owned(), v.to_owned())));
}

pub(crate) fn register_connection<C>(client: &C, manager: &Arc<super::ConnectionManager>)
where
    C: ClientInfo,
{
    let (pid, secret_key) = client.pid_and_secret_key();
    let (handle, guard) = manager.register(pid, secret_key);
    client.session_extensions().insert::<Arc<ConnectionHandle>>(handle);
    client.session_extensions().insert::<ConnectionGuard>(guard);
}

pub(crate) async fn finish_authentication0<C, P>(client: &mut C, server_parameter_provider: &P) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    P: ServerParameterProvider,
{
    client.feed(PgWireBackendMessage::Authentication(Authentication::Ok)).await?;

    if let Some(parameters) = server_parameter_provider.server_parameters(client) {
        for (k, v) in parameters {
            client.feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(k, v))).await?;
        }
    }

    let (pid, secret_key) = client.pid_and_secret_key();
    client.feed(PgWireBackendMessage::BackendKeyData(BackendKeyData::new(pid, secret_key))).await?;

    Ok(())
}

/// Completes authentication by sending `AuthenticationOk`, parameter status, backend key data, and `ReadyForQuery`.
pub async fn finish_authentication<C, P>(client: &mut C, server_parameter_provider: &P) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    P: ServerParameterProvider,
{
    finish_authentication0(client, server_parameter_provider).await?;

    client.send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(TransactionStatus::Idle))).await?;

    client.set_state(PgWireConnectionState::ReadyForQuery);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        pin::Pin,
        task::{Context, Poll},
    };

    use futures::Sink;

    use super::*;
    use crate::{
        api::{DefaultClient, SessionExtensions},
        messages::response::TransactionStatus,
    };

    struct TestClient {
        inner: DefaultClient<()>,
        messages: Vec<PgWireBackendMessage>,
    }

    impl TestClient {
        fn new() -> Self {
            Self { inner: DefaultClient::new("127.0.0.1:5432".parse().unwrap(), false), messages: vec![] }
        }
    }

    impl ClientInfo for TestClient {
        fn socket_addr(&self) -> SocketAddr {
            self.inner.socket_addr()
        }
        fn is_secure(&self) -> bool {
            self.inner.is_secure()
        }
        fn protocol_version(&self) -> ProtocolVersion {
            self.inner.protocol_version()
        }
        fn set_protocol_version(&mut self, version: ProtocolVersion) {
            self.inner.set_protocol_version(version)
        }
        fn pid_and_secret_key(&self) -> (i32, SecretKey) {
            self.inner.pid_and_secret_key()
        }
        fn set_pid_and_secret_key(&mut self, pid: i32, secret_key: SecretKey) {
            self.inner.set_pid_and_secret_key(pid, secret_key)
        }
        fn state(&self) -> PgWireConnectionState {
            self.inner.state()
        }
        fn set_state(&mut self, state: PgWireConnectionState) {
            self.inner.set_state(state)
        }
        fn transaction_status(&self) -> TransactionStatus {
            self.inner.transaction_status()
        }
        fn set_transaction_status(&mut self, status: TransactionStatus) {
            self.inner.set_transaction_status(status)
        }
        fn metadata(&self) -> &HashMap<String, String> {
            self.inner.metadata()
        }
        fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
            self.inner.metadata_mut()
        }
        fn session_extensions(&self) -> &SessionExtensions {
            self.inner.session_extensions()
        }
        #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
        fn sni_server_name(&self) -> Option<&str> {
            self.inner.sni_server_name()
        }
        #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
        fn client_certificates<'a>(&self) -> Option<&[rustls_pki_types::CertificateDer<'a>]> {
            self.inner.client_certificates()
        }
    }

    impl Sink<PgWireBackendMessage> for TestClient {
        type Error = PgWireError;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(self: Pin<&mut Self>, message: PgWireBackendMessage) -> Result<(), Self::Error> {
            self.get_mut().messages.push(message);
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    fn startup(major: u16, minor: u16, options: &[&str]) -> Startup {
        let mut startup = Startup::default();
        startup.protocol_number_major = major;
        startup.protocol_number_minor = minor;
        startup.parameters.extend(options.iter().map(|option| (option.to_string(), String::new())));
        startup
    }

    #[tokio::test]
    async fn protocol_3_2_reports_all_unsupported_pq_options() {
        let mut client = TestClient::new();
        protocol_negotiation(&mut client, &startup(3, 2, &["_pq_.one", "_pq_.two"])).await.unwrap();

        let [PgWireBackendMessage::NegotiateProtocolVersion(message)] = client.messages.as_slice() else { panic!("expected protocol negotiation") };
        assert_eq!(message.newest_minor_protocol, ProtocolVersion::PROTOCOL3_2.into());
        assert_eq!(message.unsupported_options, ["_pq_.one", "_pq_.two"]);
    }

    #[tokio::test]
    async fn protocol_3_2_with_one_unsupported_pq_option_negotiates() {
        let mut client = TestClient::new();
        protocol_negotiation(&mut client, &startup(3, 2, &["_pq_.probe"])).await.unwrap();

        let [PgWireBackendMessage::NegotiateProtocolVersion(message)] = client.messages.as_slice() else { panic!("expected protocol negotiation") };
        assert_eq!(message.unsupported_options, ["_pq_.probe"]);
    }

    #[tokio::test]
    async fn protocol_3_2_without_pq_options_skips_negotiation() {
        let mut client = TestClient::new();
        protocol_negotiation(&mut client, &startup(3, 2, &[])).await.unwrap();
        assert!(client.messages.is_empty());
    }

    #[tokio::test]
    async fn newer_minor_reports_pq_options_and_server_version() {
        let mut client = TestClient::new();
        protocol_negotiation(&mut client, &startup(3, 3, &["_pq_.probe"])).await.unwrap();

        let [PgWireBackendMessage::NegotiateProtocolVersion(message)] = client.messages.as_slice() else { panic!("expected protocol negotiation") };
        assert_eq!(message.newest_minor_protocol, ProtocolVersion::PROTOCOL3_2.into());
        assert_eq!(message.unsupported_options, ["_pq_.probe"]);
    }

    #[tokio::test]
    async fn unsupported_major_is_rejected() {
        let mut client = TestClient::new();
        let error = protocol_negotiation(&mut client, &startup(4, 0, &[])).await.unwrap_err();
        assert!(matches!(error, PgWireError::UnsupportedProtocolVersion(4, 0)));
    }
}

pub mod cleartext;
pub mod md5pass;
pub mod noop;
#[cfg(any(feature = "_aws-lc-rs", feature = "_ring"))]
pub mod sasl;
#[cfg(feature = "simple-oidc-validator")]
pub mod simple_oidc_validator;
