//! Shared production and E2E server bootstrap wiring.

pub mod pg_compat;

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use datafusion::execution::context::SessionContext;
use tokio_util::sync::CancellationToken;

use crate::{
    config::AppConfig,
    database::Database,
    write::{BufferedWriteLayer, DeltaWatermark},
};

/// Fully initialized server state.
pub struct Bootstrapped {
    pub db: Arc<Database>,
    pub buffered_layer: Arc<BufferedWriteLayer>,
    /// Session context with providers and UDFs registered.
    pub session_ctx: Arc<SessionContext>,
    /// Cancellation signal for spawned tasks.
    pub shutdown: CancellationToken,
}

/// Best-effort raise of the open-file soft limit to the hard limit.
pub fn raise_file_limit() {
    // SAFETY: both calls take a valid, fully-initialized `rlimit`, and neither
    // retains the pointer past the call.
    unsafe {
        let mut limit = std::mem::zeroed::<libc::rlimit>();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 || limit.rlim_cur >= limit.rlim_max {
            return;
        }
        let (previous, target) = (limit.rlim_cur, limit.rlim_max);
        limit.rlim_cur = target;
        match libc::setrlimit(libc::RLIMIT_NOFILE, &limit) {
            0 => tracing::info!(previous, target, "raised the open-file soft limit to the hard limit"),
            _ => tracing::warn!(previous, target, error = %std::io::Error::last_os_error(), "could not raise the open-file soft limit"),
        }
    }
}

/// Initializes storage, recovery, background work, and query providers.
pub async fn bootstrap(cfg: Arc<AppConfig>) -> Result<Bootstrapped> {
    crate::observability::mark_process_start();
    crate::support::init_from_env();
    raise_file_limit();

    let t_db = std::time::Instant::now();
    let mut db = Database::with_config(Arc::clone(&cfg)).await?;
    tracing::info!("bootstrap.phase=database_init elapsed_ms={}", t_db.elapsed().as_millis());

    let delta_write_callback = delta_write_callback(&db);

    let mut session_context = Arc::new(db.clone()).create_session_context_for(true);
    db.setup_session_udfs(&mut session_context)?;
    let registry: Arc<crate::read::functions::FnRegistry> = Arc::new(session_context.state());

    crate::write::wal::boot_wal_gc(&cfg.core.wal_dir());

    let t_layer = std::time::Instant::now();
    let mut layer = BufferedWriteLayer::with_config(Arc::clone(&cfg), registry)?
        .with_delta_writer(delta_write_callback)
        .with_coalesced_delta_writer(coalesced_delta_write_callback(&db));
    tracing::info!("bootstrap.phase=buffered_write_layer_init elapsed_ms={}", t_layer.elapsed().as_millis());

    // The sidecar requires both indexed tables and object storage.
    let bucket = cfg.aws.aws_s3_bucket.as_deref().unwrap_or_default();
    if !cfg.tantivy.indexed_tables().is_empty() && !bucket.is_empty() {
        let storage_uri = format!("s3://{}/{}/tantivy", bucket, cfg.core.timefusion_table_prefix);
        let obj_store = db.create_object_store(&storage_uri, &cfg.aws.build_storage_options(None)).await?;
        let tcfg = Arc::new(cfg.tantivy.clone());
        let svc = Arc::new(crate::tantivy::search::TantivyIndexService::new(obj_store.clone(), tcfg.clone(), cfg.core.timefusion_data_dir.clone()));
        layer = layer.with_tantivy_indexer(tantivy_index_callback(&db, Arc::clone(&svc)));
        let search = Arc::new(crate::tantivy::search::TantivySearchService::new(obj_store, cfg.core.timefusion_data_dir.clone(), tcfg));
        // Lets a publish seed the reader's cache and invalidate its manifest
        // in-process instead of round-tripping through S3.
        svc.with_reader(&search);
        db = db.with_tantivy_search(search).with_tantivy_indexer(svc);
    }
    if cfg.maintenance.timefusion_file_bloom_pruning && !bucket.is_empty() {
        let storage_uri = format!("s3://{}/{}/bloom_sidecars", bucket, cfg.core.timefusion_table_prefix);
        let store = db.create_object_store(&storage_uri, &cfg.aws.build_storage_options(None)).await?;
        db = db.with_bloom_prune(Arc::new(crate::read::bloom_prune::BloomPruneRegistry::new(
            store,
            cfg.maintenance.timefusion_bloom_registry_cap_mb * 1024 * 1024,
            std::time::Duration::from_secs(cfg.maintenance.timefusion_bloom_registry_refresh_secs),
        )));
    }

    let buffered_layer = Arc::new(layer);

    // Clean snapshot → skip the Delta cursor scan; dirty/missing snapshot →
    // derive cursors from Delta so WAL replay doesn't re-inject entries Delta
    // already has.
    let wal_ref = buffered_layer.wal();
    let t_snap = std::time::Instant::now();
    let clean_snapshot = wal_ref.load_cursor_snapshot().is_some_and(|snap| wal_ref.restore_cursor_snapshot(&snap).is_ok() && snap.clean_shutdown);
    let local_wal_consumed = !clean_snapshot && wal_ref.can_skip_delta_reconcile().unwrap_or(false);
    let skip_delta_scan = clean_snapshot || local_wal_consumed;
    tracing::info!(
        "bootstrap.phase=cursor_snapshot skip_delta_scan={skip_delta_scan} clean_snapshot={clean_snapshot} local_wal_consumed={local_wal_consumed} elapsed_ms={}",
        t_snap.elapsed().as_millis()
    );
    if !skip_delta_scan {
        let t_delta = std::time::Instant::now();
        let advanced = db.derive_wal_cursors_from_delta(wal_ref, Some(buffered_layer.as_ref())).await.unwrap_or(0);
        tracing::info!("bootstrap.phase=delta_cursor_reconcile shards_advanced={advanced} elapsed_ms={}", t_delta.elapsed().as_millis());
    }

    let t_wal = std::time::Instant::now();
    buffered_layer.recover_from_wal().await?;
    tracing::info!("bootstrap.phase=wal_replay elapsed_ms={}", t_wal.elapsed().as_millis());
    buffered_layer.start_background_tasks().await;

    db = db.with_buffered_layer(Arc::clone(&buffered_layer));
    db.start_dml_coalescer();
    db = db.start_maintenance_schedulers().await?;
    let db = Arc::new(db);
    db.setup_session_tables(&mut session_context)?;
    db.preload_tables();
    db.spawn_tantivy_backfill();
    db.spawn_tantivy_prefetch();
    db.spawn_deferred_tantivy_reindex(Arc::clone(&buffered_layer));

    Ok(Bootstrapped { db, buffered_layer, session_ctx: Arc::new(session_context), shutdown: CancellationToken::new() })
}

/// Builds sidecar indexes from committed Parquet files with physical row ordinals.
/// The write layer invokes this callback after commit under its background semaphore.
pub fn tantivy_index_callback(db: &Database, indexer: Arc<crate::tantivy::search::TantivyIndexService>) -> crate::write::TantivyIndexCallback {
    let db = db.clone();
    Arc::new(move |project_id, table_name, _, added_files| {
        let db = db.clone();
        let indexer = Arc::clone(&indexer);
        Box::pin(async move {
            if added_files.is_empty() || !indexer.config.is_table_indexed(&table_name) {
                return Ok(());
            }
            let table = db.resolve_table(&project_id, &table_name).await?;
            let store = table.read().await.log_store().object_store(None);
            // One streamed build at a time per callback keeps multi-file commits bounded.
            for uri in added_files {
                let relative = crate::tantivy::search::parquet_rel_of_uri(&uri).context("committed file has no relative Parquet path")?;
                indexer
                    .build_index_for_file(&table_name, &project_id, relative, &uri, Arc::clone(&store))
                    .instrument(tracing::info_span!("tantivy_build", cause = "flush"))
                    .await?;
            }
            Ok(())
        })
    })
}

/// Creates the per-bucket Delta writer shared by production and tests.
pub fn delta_write_callback(db: &crate::database::Database) -> crate::write::DeltaWriteCallback {
    let db = db.clone();
    Arc::new(move |project_id: String, table_name: String, batches: Vec<RecordBatch>, wal_watermark: DeltaWatermark| {
        let db = db.clone();
        Box::pin(async move { db.insert_records_batch(&project_id, &table_name, batches, true, Some(&wal_watermark)).await })
    })
}

/// Creates the optional one-commit-per-physical-table flush writer.
pub fn coalesced_delta_write_callback(db: &crate::database::Database) -> crate::write::DeltaCoalescedWriteCallback {
    let db = db.clone();
    Arc::new(move |units: Vec<crate::write::FlushUnit>| {
        let db = db.clone();
        Box::pin(async move {
            let (topics, units): (Vec<(String, String)>, Vec<_>) = units
                .into_iter()
                .map(|u| {
                    (
                        (u.project_id.clone(), u.table_name.clone()),
                        crate::database::CoalescedWriteUnit { project_id: u.project_id, table_name: u.table_name, batches: u.batches, watermark: u.watermark },
                    )
                })
                .unzip();
            let results = db.insert_records_batches_coalesced(units).await;
            // A successful commit proves this topic has Delta files even when
            // concurrent snapshot attribution returns an empty added-file list.
            topics
                .iter()
                .zip(&results)
                .filter(|(_, result)| result.is_ok())
                .for_each(|((project_id, table_name), _)| db.mark_delta_has_files(project_id, table_name));
            results
        })
    })
}

// ===== pgwire_handlers =====
use std::{borrow::Cow, fmt::Debug, sync::LazyLock};

use async_trait::async_trait;
use datafusion_postgres::{
    DfSessionService,
    hooks::{QueryHook, cursor::CursorStatementHook, set_show::SetShowHook, transactions::TransactionStatementHook},
    pgwire::{
        api::{
            ClientInfo, ClientPortalStore, ErrorHandler, PgWireServerHandlers, Type,
            auth::{AuthSource, LoginInfo, Password, StartupHandler, cleartext::CleartextPasswordAuthStartupHandler},
            portal::Portal,
            query::{ExtendedQueryHandler, SimpleQueryHandler},
            results::{DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag},
            stmt::StoredStatement,
            store::PortalStore,
        },
        error::{ErrorInfo, PgWireError, PgWireResult},
        messages::PgWireBackendMessage,
    },
};
use futures::{Sink, StreamExt, TryStreamExt, stream};
use regex::Regex;
use tracing::{Instrument, error, field::Empty, info, instrument, warn};

use crate::{
    read::plan_cache::PlanCacheHook,
    server::pg_compat::{
        DEFAULT_MAX_STATEMENT_SECS, PgCompatibilityHook, TimeFusionServerParameterProvider, batch_statement_secs, effective_statement_timeout,
        statement_timeout_error,
    },
};

/// Auth configuration for PgWire server
#[derive(Debug, Clone, educe::Educe)]
#[educe(Default)]
pub struct AuthConfig {
    #[educe(Default = "postgres")]
    pub username: String,
    pub password: Option<String>,
}

impl AuthConfig {
    /// Construct from `CoreConfig`, erroring unless a password is set or
    /// `TIMEFUSION_ALLOW_INSECURE_AUTH=true`. The cleartext handler treats a
    /// `None` password as "accept any", i.e. an open endpoint.
    pub fn from_core(core: &crate::config::CoreConfig) -> anyhow::Result<Self> {
        let allow_insecure = crate::config::is_insecure_auth_allowed();
        match (&core.pgwire_password, allow_insecure) {
            (Some(p), _) if !p.is_empty() => Ok(Self { username: core.pgwire_user.clone(), password: Some(p.clone()) }),
            (_, true) => {
                tracing::warn!(
                    "PGWIRE_PASSWORD unset and TIMEFUSION_ALLOW_INSECURE_AUTH=true — pgwire endpoint accepts any password. Acceptable for local dev ONLY; never in production."
                );
                Ok(Self { username: core.pgwire_user.clone(), password: None })
            }
            _ => anyhow::bail!("PGWIRE_PASSWORD is required (set TIMEFUSION_ALLOW_INSECURE_AUTH=true to opt into open auth for local dev)"),
        }
    }
}

/// Validates a login against the configured credentials.
#[async_trait]
impl AuthSource for AuthConfig {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let username = login.user().unwrap_or("");
        (username == self.username).then(|| Password::new(None, self.password.clone().unwrap_or_default().into_bytes())).ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new("FATAL".into(), "28P01".into(), format!("password authentication failed for user \"{username}\""))))
        })
    }
}

/// Custom handler factory that creates handlers with logging and auth
pub struct LoggingHandlerFactory {
    session_context: Arc<SessionContext>,
    auth_config: AuthConfig,
    plan_cache: Arc<PlanCacheHook>,
    connections: Arc<datafusion_postgres::pgwire::api::ConnectionManager>,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>,
    db: Option<Arc<Database>>,
    max_statement_secs: u64,
}

#[bon::bon]
impl LoggingHandlerFactory {
    /// `db` enables the admin commands intercepted in the simple-query path;
    /// leave it unset to disable them.
    #[builder]
    pub fn new(
        session_context: Arc<SessionContext>, auth_config: AuthConfig, scan_metrics: Option<Arc<crate::database::ScanMetrics>>, db: Option<Arc<Database>>,
        #[builder(default = DEFAULT_MAX_STATEMENT_SECS)] max_statement_secs: u64,
    ) -> Self {
        let plan_cache = Arc::new(PlanCacheHook::default());
        crate::read::plan_cache::set_global(plan_cache.clone());
        Self { session_context, auth_config, plan_cache, connections: Arc::default(), scan_metrics, db, max_statement_secs }
    }

    /// Hooks for every `DfSessionService` this factory produces. Sharing one
    /// `plan_cache` Arc is what makes the LRU global rather than per-connection.
    fn hooks(&self) -> Vec<Arc<dyn QueryHook>> {
        vec![
            Arc::new(CursorStatementHook),
            Arc::new(PgCompatibilityHook::new(self.auth_config.username.clone(), self.max_statement_secs)),
            self.plan_cache.clone() as Arc<dyn QueryHook>,
            Arc::new(SetShowHook),
            Arc::new(TransactionStatementHook),
        ]
    }

    fn simple_for(&self, session_context: Arc<SessionContext>) -> Arc<LoggingSimpleQueryHandler> {
        let _t = crate::observability::BlockWatch::new("pgwire_simple_handler_build");
        Arc::new(
            LoggingSimpleQueryHandler::builder()
                .session_context(session_context)
                .hooks(self.hooks())
                .max_statement_secs(self.max_statement_secs)
                .maybe_scan_metrics(self.scan_metrics.clone())
                .maybe_db(self.db.clone())
                .build(),
        )
    }

    fn extended_for(&self, session_context: Arc<SessionContext>) -> Arc<LoggingExtendedQueryHandler> {
        let _t = crate::observability::BlockWatch::new("pgwire_extended_handler_build");
        Arc::new(
            LoggingExtendedQueryHandler::builder()
                .session_context(session_context)
                .hooks(self.hooks())
                .max_statement_secs(self.max_statement_secs)
                .maybe_scan_metrics(self.scan_metrics.clone())
                .build(),
        )
    }
}

/// pgwire calls these factory methods **per connection**. They are sync and
/// allocate a `DfSessionService` each time, so they occupy a worker for their
/// duration — hence `BlockWatch` rather than `TimedSection`.
impl PgWireServerHandlers for LoggingHandlerFactory {
    fn query_handlers(&self) -> (Arc<impl SimpleQueryHandler>, Arc<impl ExtendedQueryHandler>) {
        // SessionContext::clone shares its mutable state. Clone the SessionState
        // into a new context instead: catalogs/runtime/cache stay shared, while
        // SET changes remain local to this connection and both protocol paths.
        let session_context = Arc::new(SessionContext::new_with_state(self.session_context.state()));
        (self.simple_for(session_context.clone()), self.extended_for(session_context))
    }

    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.simple_for(self.session_context.clone())
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.extended_for(self.session_context.clone())
    }

    fn cancel_handler(&self) -> Arc<impl datafusion_postgres::pgwire::api::cancel::CancelHandler> {
        Arc::new(datafusion_postgres::pgwire::api::cancel::DefaultCancelHandler::new(self.connections.clone()))
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        let _t = crate::observability::BlockWatch::new("pgwire_startup_handler_build");
        Arc::new(
            CleartextPasswordAuthStartupHandler::new(self.auth_config.clone(), TimeFusionServerParameterProvider::default())
                .with_connection_manager(self.connections.clone()),
        )
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(LoggingErrorHandler)
    }
}

struct LoggingErrorHandler;

impl ErrorHandler for LoggingErrorHandler {
    fn on_error<C>(&self, _client: &C, error: &mut PgWireError)
    where
        C: ClientInfo,
    {
        // `ApiError` wraps an internal failure (a bug); everything else is
        // client error or connection noise.
        match error {
            PgWireError::ApiError(_) => error!("PgWire internal error: {}", error),
            _ => info!("PgWire error: {}", error),
        }
    }
}

/// Concurrent-giant-statement gate. A multi-MB statement materializes its
/// literals and bound parameters as ScalarValue arrays during plan + bind —
/// transient heap otherwise bounded only by connection concurrency. Two
/// permits: one giant always runs while another queues, capping that heap at
/// 2x one statement instead of Nx.
const GIANT_STMT_BYTES: usize = 2 * 1024 * 1024;
static GIANT_STMT_SEM: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

async fn giant_stmt_permit(len: usize) -> Option<tokio::sync::SemaphorePermit<'static>> {
    if len < GIANT_STMT_BYTES {
        return None;
    }
    let t0 = std::time::Instant::now();
    let permit = GIANT_STMT_SEM.acquire().await.expect("giant-stmt semaphore never closed");
    let waited = t0.elapsed();
    if waited.as_millis() > 50 {
        tracing::info!("giant statement ({len} B) queued {waited:?} behind the 2-permit parse gate");
    }
    Some(permit)
}

fn client_statement_timeout(client: &(impl ClientInfo + ?Sized)) -> Option<std::time::Duration> {
    client.metadata().get("statement_timeout_ms").and_then(|value| value.parse::<u64>().ok()).map(std::time::Duration::from_millis)
}

/// Writes are exempt from the statement timeout: the deadline is enforced by
/// DROPPING the in-flight future, and the DML path commits inside it, so
/// cancelling would report failure for a write that is already partly durable.
fn statement_timeout_applies(query: &str) -> bool {
    !matches!(classify_query(query).0, QueryKind::Dml | QueryKind::Ddl)
}

async fn run_with_statement_timeout<T>(
    timeout: Option<std::time::Duration>, query: impl std::future::Future<Output = PgWireResult<T>>,
) -> PgWireResult<(T, Option<tokio::time::Instant>)> {
    let deadline = timeout.map(|timeout| tokio::time::Instant::now() + timeout);
    let result = match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, query).await.map_err(|_| statement_timeout_error())?,
        None => query.await,
    }?;
    Ok((result, deadline))
}

#[derive(Clone)]
struct StreamFailureContext {
    fingerprint: String,
    template: String,
    tables: String,
    project_id: String,
    protocol: &'static str,
    deadline_ms: Option<u64>,
    started_at: std::time::Instant,
}

impl StreamFailureContext {
    fn new(query: &str, protocol: &'static str, timeout: Option<std::time::Duration>, started_at: std::time::Instant) -> Self {
        let (tables, project_id) = query_dimensions(query);
        Self {
            fingerprint: query_fingerprint(query),
            template: query_template(query),
            tables,
            project_id: project_id.to_owned(),
            protocol,
            deadline_ms: timeout.map(|timeout| timeout.as_millis().min(u128::from(u64::MAX)) as u64),
            started_at,
        }
    }
}

fn stream_failure_class(error: &PgWireError) -> &'static str {
    let error = error.to_string().to_ascii_lowercase();
    if error.contains("statement timeout") || error.contains("statement_timeout") {
        "statement_timeout"
    } else if error.contains("canceling statement") || error.contains("cancelled") || error.contains("canceled") {
        "client_cancel"
    } else if error.contains("admission") && (error.contains("timeout") || error.contains("timed out")) {
        "queue_timeout"
    } else if error.contains("resources exhausted")
        || error.contains("failed to allocate")
        || error.contains("memory reservation")
        || error.contains("spill limit")
    {
        "resource"
    } else {
        "other"
    }
}

fn with_response_deadline(response: Response, deadline: Option<tokio::time::Instant>, context: StreamFailureContext) -> Response {
    match response {
        Response::Query(QueryResponse { command_tag, row_schema, data_rows, .. }) => {
            // do_query returns before rows are consumed. Keep its scrubbed query
            // context for failures raised during execution, including timeouts.
            let span = tracing::Span::current();
            let data_rows = stream::unfold(Some(data_rows), move |rows| {
                let span = span.clone();
                let context = context.clone();
                async move {
                    let mut rows = rows?;
                    let next = match deadline {
                        Some(deadline) => match tokio::time::timeout_at(deadline, rows.next()).await {
                            Ok(Some(row)) => Some((row, Some(rows))),
                            Ok(None) => None,
                            Err(_) => Some((Err(statement_timeout_error()), None)),
                        },
                        None => rows.next().await.map(|row| (row, Some(rows))),
                    };
                    if let Some((Err(error), _)) = &next {
                        // `?` escapes multiline causes so line-based collectors keep the full error on one line.
                        crate::observability::maintenance_stats().pgwire_stream_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        warn!(
                            event = "pgwire.stream_failed",
                            failure.class = stream_failure_class(error),
                            query.fingerprint = %context.fingerprint,
                            query.template = %context.template,
                            query.tables = %context.tables,
                            project.id = %context.project_id,
                            protocol = context.protocol,
                            deadline_ms = ?context.deadline_ms,
                            duration_us = context.started_at.elapsed().as_micros() as u64,
                            error = ?error.to_string(),
                            "PostgreSQL row stream failed"
                        );
                    }
                    next
                }
                .instrument(span)
            });
            let mut response = QueryResponse::new(row_schema, data_rows);
            response.set_command_tag(&command_tag);
            Response::Query(response)
        }
        response => response,
    }
}

/// The shared tail of both protocol handlers: the giant-statement gate, the
/// `datafusion.execute` span, the statement deadline, and the latency/failure
/// events. `finish` applies the deadline to whatever shape of response the
/// protocol returns. Failures are logged from INSIDE the query span so the
/// span's `query.text` lands on the same line as the error.
async fn run_statement<T, R>(
    scan_metrics: Option<&crate::database::ScanMetrics>, max_statement_secs: u64, client_timeout: Option<std::time::Duration>, query: &str,
    protocol: &'static str, execute: impl std::future::Future<Output = PgWireResult<T>>,
    finish: impl FnOnce(T, Option<tokio::time::Instant>, StreamFailureContext) -> R,
) -> PgWireResult<R> {
    let _giant = giant_stmt_permit(query.len()).await;
    let execute_span = tracing::trace_span!(parent: &tracing::Span::current(), "datafusion.execute");
    let t0 = std::time::Instant::now();
    let timeout = effective_statement_timeout(client_timeout, max_statement_secs, batch_statement_secs()).filter(|_| statement_timeout_applies(query));
    let context = StreamFailureContext::new(query, protocol, timeout, t0);
    let result = run_with_statement_timeout(timeout, execute.instrument(execute_span)).await.map(|(value, deadline)| finish(value, deadline, context));
    record_statement_latency(scan_metrics, query, protocol, t0.elapsed().as_micros() as u64, result.as_ref().err());
    if let Err(error) = &result {
        warn!(protocol, error = %error, "statement failed");
    }
    result
}

/// Simple query handler with tracing
pub struct LoggingSimpleQueryHandler {
    inner: DfSessionService,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>,
    db: Option<Arc<Database>>,
    max_statement_secs: u64,
}

#[bon::bon]
impl LoggingSimpleQueryHandler {
    #[builder]
    pub fn new(
        session_context: Arc<SessionContext>, hooks: Vec<Arc<dyn QueryHook>>, scan_metrics: Option<Arc<crate::database::ScanMetrics>>,
        db: Option<Arc<Database>>, #[builder(default = DEFAULT_MAX_STATEMENT_SECS)] max_statement_secs: u64,
    ) -> Self {
        Self { inner: DfSessionService::new_with_hooks(session_context, hooks), scan_metrics, db, max_statement_secs }
    }

    /// Open the unified table an admin `command` names, with that command's
    /// standard "not available on this server" and "open table" errors.
    async fn admin_table(&self, command: &str, table: &str) -> PgWireResult<(Arc<Database>, Arc<tokio::sync::RwLock<deltalake::DeltaTable>>)> {
        let db = require_available(self.db.clone(), command)?;
        let table_ref = db.get_or_create_unified_table(table).await.map_err(|e| admin_err(format!("{command}: open table '{table}': {e}")))?;
        Ok((db, table_ref))
    }

    /// Execute an intercepted `OPTIMIZE <table> WHERE date = '...'`.
    async fn run_optimize(&self, cmd: OptimizeCmd) -> PgWireResult<Vec<Response>> {
        let (db, table_ref) = self.admin_table("OPTIMIZE", &cmd.table).await?;
        let (removed, added) = db.compact_date(&table_ref, &cmd.table, cmd.date, cmd.project_id.as_deref()).await.map_err(|e| admin_err(e.to_string()))?;
        info!("pgwire OPTIMIZE {} date={} project={:?}: {removed} removed, {added} added", cmd.table, cmd.date, cmd.project_id);
        Ok(vec![Response::Execution(Tag::new(&format!("OPTIMIZE {removed} {added}")))])
    }

    /// Execute an intercepted `VACUUM <table> [RETAIN <n> HOURS]`.
    async fn run_vacuum(&self, cmd: VacuumCmd) -> PgWireResult<Vec<Response>> {
        let db = require_available(self.db.as_ref(), "VACUUM")?;
        let deleted = db.vacuum_named(&cmd.table, cmd.retention_hours).await.map_err(|e| admin_err(format!("VACUUM '{}': {e}", cmd.table)))?;
        info!("pgwire VACUUM {} retention={:?}: {deleted} files deleted", cmd.table, cmd.retention_hours);
        Ok(vec![Response::Execution(Tag::new(&format!("VACUUM {deleted}")))])
    }

    /// Execute an intercepted `FLUSH` — drain the whole MemBuffer to Delta.
    /// Intended to be run once before a planned restart/deploy. Errors when any
    /// bucket fails so callers can gate on it.
    async fn run_flush(&self) -> PgWireResult<Vec<Response>> {
        let layer = require_available(self.db.as_ref().and_then(|d| d.buffered_layer()), "FLUSH")?;
        // Misuse guard: each FLUSH commits the open window per table (tiny
        // parquet files + tantivy builds) and contends flush_lock with routine
        // flushes, so a looping client would explode the file count.
        // Frozen-clock (test) harnesses are exempt.
        use std::sync::atomic::{AtomicI64, Ordering};
        const FLUSH_MIN_INTERVAL_SECS: i64 = 10;
        static LAST_FLUSH_MICROS: AtomicI64 = AtomicI64::new(i64::MIN);
        if !crate::support::is_frozen() {
            let now = chrono::Utc::now().timestamp_micros();
            let since = now.saturating_sub(LAST_FLUSH_MICROS.load(Ordering::Acquire));
            if since < FLUSH_MIN_INTERVAL_SECS * 1_000_000 {
                return Err(admin_err(format!("FLUSH rate-limited: last ran {}s ago (min interval {FLUSH_MIN_INTERVAL_SECS}s)", since / 1_000_000)));
            }
            LAST_FLUSH_MICROS.store(now, Ordering::Release);
        }
        let stats = layer.flush_all_now().await.map_err(|e| admin_err(format!("FLUSH: {e}")))?;
        info!("pgwire FLUSH: {} bucket(s) flushed ({} rows), {} failed", stats.buckets_flushed, stats.total_rows, stats.buckets_failed);
        if stats.buckets_failed > 0 {
            return Err(admin_err(format!(
                "FLUSH: {} bucket(s) failed to flush ({} flushed) — data stays buffered/WAL-durable",
                stats.buckets_failed, stats.buckets_flushed
            )));
        }
        // Reclaim the consumed WAL while this instance is still serving,
        // otherwise the replacement pays to scan it at boot.
        if !crate::support::is_frozen() {
            layer.reclaim_wal_after_flush().await;
        }
        Ok(vec![Response::Execution(Tag::new(&format!("FLUSH {}", stats.total_rows)))])
    }

    /// Execute `HANDOFF`: lease a write-admission fence, drain the finite tail,
    /// and keep serving reads until the orchestrator replaces this task.
    async fn run_handoff(&self) -> PgWireResult<Vec<Response>> {
        let layer = require_available(self.db.as_ref().and_then(|d| d.buffered_layer()), "HANDOFF")?;
        let stats = layer.prepare_deploy_handoff().await.map_err(|e| admin_err(format!("HANDOFF: {e}")))?;
        if !crate::support::is_frozen() {
            layer.reclaim_wal_after_flush().await;
        }
        Ok(vec![Response::Execution(Tag::new(&format!("HANDOFF {}", stats.total_rows)))])
    }

    /// Read recent Delta commit metadata over pgwire.
    async fn run_delta_history(&self, cmd: DeltaHistoryCmd) -> PgWireResult<Vec<Response>> {
        let (_, table_ref) = self.admin_table("DELTA HISTORY", &cmd.table).await?;
        let commits: Vec<_> =
            table_ref.read().await.history(Some(cmd.limit)).await.map_err(|e| admin_err(format!("DELTA HISTORY '{}': {e}", cmd.table)))?.collect();
        let rows = commits.into_iter().map(|commit| {
            let timestamp = commit.timestamp.and_then(chrono::DateTime::from_timestamp_millis).map(|v| v.to_rfc3339()).unwrap_or_default();
            let read_version = commit.read_version.map(|v| v.to_string()).unwrap_or_default();
            let version = commit.read_version.map(|v| (v + 1).to_string()).unwrap_or_default();
            let blind_append = commit.is_blind_append.map(|v| v.to_string()).unwrap_or_default();
            let parameters = serde_json::to_string(&commit.operation_parameters).unwrap_or_default();
            let info = serde_json::to_string(&commit).unwrap_or_default();
            // Last, so the field can be moved out rather than cloned.
            let operation = commit.operation.unwrap_or_default();
            Ok(vec![version, timestamp, operation, read_version, blind_append, parameters, info])
        });
        Ok(text_response(["version", "timestamp_utc", "operation", "read_version", "is_blind_append", "operation_parameters", "commit_info"], rows))
    }

    /// Return every raw action in one Delta commit. This is an audit primitive:
    /// it reads the transaction log only and never constructs a transaction.
    async fn run_delta_actions(&self, cmd: DeltaVersionCmd) -> PgWireResult<Vec<Response>> {
        let (_, table_ref) = self.admin_table("DELTA ACTIONS", &cmd.table).await?;
        let log_store = table_ref.read().await.log_store();
        let rows = commit_actions(&log_store, "DELTA ACTIONS", &cmd).await?.into_iter().map(move |action| {
            let (kind, path, size) = match &action {
                deltalake::kernel::Action::Add(add) => ("add", add.path.as_str(), add.size.to_string()),
                deltalake::kernel::Action::Remove(remove) => ("remove", remove.path.as_str(), remove.size.map(|v| v.to_string()).unwrap_or_default()),
                deltalake::kernel::Action::CommitInfo(_) => ("commitInfo", "", String::new()),
                _ => ("other", "", String::new()),
            };
            let json = serde_json::to_string(&action).map_err(|e| admin_err(format!("encode Delta action: {e}")))?;
            Ok(vec![cmd.version.to_string(), kind.to_string(), path.to_string(), size, json])
        });
        Ok(text_response(["version", "action", "path", "size_bytes", "action_json"], rows))
    }

    /// Reconstruct the full pre-commit Add actions for files removed by
    /// `version`. This is read-only and fails unless every removal has a source.
    async fn run_delta_recovery_audit(&self, cmd: DeltaVersionCmd) -> PgWireResult<Vec<Response>> {
        let (_, table_ref) = self.admin_table("DELTA RECOVERY AUDIT", &cmd.table).await?;
        let mut before = table_ref.read().await.clone();
        let removed = commit_actions(&before.log_store(), "DELTA RECOVERY AUDIT", &cmd)
            .await?
            .into_iter()
            .filter_map(|action| match action {
                deltalake::kernel::Action::Remove(remove) => Some(remove.path),
                _ => None,
            })
            .collect::<std::collections::HashSet<_>>();
        if removed.is_empty() {
            return Err(admin_err(format!("DELTA RECOVERY AUDIT '{}' VERSION {}: commit removed no files", cmd.table, cmd.version)));
        }
        let previous = cmd.version.checked_sub(1).ok_or_else(|| admin_err("DELTA RECOVERY AUDIT cannot inspect before version 0"))?;
        before.load_version(previous).await.map_err(|e| admin_err(format!("DELTA RECOVERY AUDIT '{}': load version {previous}: {e}", cmd.table)))?;
        let mut sources = before
            .get_active_add_actions_by_partitions(&[])
            .try_filter_map(|view| {
                let include = removed.contains(view.path().as_ref());
                // Do not "modernize" this: the replacement Arrow-table API does
                // not round-trip a complete Add action, and recovery must
                // preserve raw stats/tags byte-for-byte.
                #[allow(deprecated)]
                let source = include.then(|| view.add_action());
                futures::future::ready(Ok(source))
            })
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| admin_err(format!("DELTA RECOVERY AUDIT '{}': read source actions: {e}", cmd.table)))?;
        if sources.len() != removed.len() {
            return Err(admin_err(format!(
                "DELTA RECOVERY AUDIT '{}' VERSION {}: reconstructed {} of {} removed files",
                cmd.table,
                cmd.version,
                sources.len(),
                removed.len()
            )));
        }
        sources.sort_unstable_by(|a, b| a.path.cmp(&b.path));

        let rows = sources.into_iter().map(move |add| {
            let size = add.size.to_string();
            let json = serde_json::to_string(&deltalake::kernel::Action::Add(add.clone())).map_err(|e| admin_err(format!("encode source Add: {e}")))?;
            Ok(vec![cmd.version.to_string(), add.path, size, json])
        });
        Ok(text_response(["removed_by_version", "path", "size_bytes", "source_add_json"], rows))
    }
}

/// `DELTA HISTORY <table> [LIMIT <n>]` is deliberately read-only.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DeltaHistoryCmd {
    pub table: String,
    pub limit: usize,
}

pub(crate) fn parse_delta_history(query: &str) -> Result<Option<DeltaHistoryCmd>, String> {
    let Some(rest) = strip_command(query, "delta") else { return Ok(None) };
    let Some(rest) = strip_keyword(rest, "history", char::is_whitespace) else {
        return Err("DELTA supports only: DELTA HISTORY <table> [LIMIT <n>]".to_string());
    };
    let mut parts = rest.split_whitespace();
    let table = parts.next().ok_or("DELTA HISTORY requires a table: DELTA HISTORY <table> [LIMIT <n>]")?;
    let limit = match (parts.next(), parts.next(), parts.next()) {
        (None, None, None) => 100,
        (Some(keyword), Some(value), None) if keyword.eq_ignore_ascii_case("limit") => {
            let limit = value.parse::<usize>().map_err(|_| format!("invalid DELTA HISTORY limit '{value}'"))?;
            if !(1..=10_000).contains(&limit) {
                return Err("DELTA HISTORY limit must be between 1 and 10000".to_string());
            }
            limit
        }
        _ => return Err("expected: DELTA HISTORY <table> [LIMIT <n>]".to_string()),
    };
    Ok(Some(DeltaHistoryCmd { table: table.to_string(), limit }))
}

/// `<table> VERSION <n>`, shared by `DELTA ACTIONS` and `DELTA RECOVERY AUDIT`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DeltaVersionCmd {
    pub table: String,
    pub version: u64,
}

fn parse_delta_version_cmd(rest: &str, command: &str) -> Result<DeltaVersionCmd, String> {
    let usage = || format!("expected: {command} <table> VERSION <n>");
    let mut parts = rest.split_whitespace();
    let (Some(table), Some(keyword), Some(value), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
        return Err(usage());
    };
    if !keyword.eq_ignore_ascii_case("version") {
        return Err(usage());
    }
    Ok(DeltaVersionCmd { table: table.to_string(), version: value.parse().map_err(|_| format!("invalid Delta version '{value}'"))? })
}

pub(crate) fn parse_delta_actions(query: &str) -> Result<Option<DeltaVersionCmd>, String> {
    let Some(rest) = strip_command(query, "delta") else { return Ok(None) };
    let Some(rest) = strip_keyword(rest, "actions", char::is_whitespace) else { return Ok(None) };
    parse_delta_version_cmd(rest, "DELTA ACTIONS").map(Some)
}

pub(crate) fn parse_delta_recovery_audit(query: &str) -> Result<Option<DeltaVersionCmd>, String> {
    let Some(rest) = strip_command(query, "delta") else { return Ok(None) };
    let Some(rest) = strip_keyword(rest, "recovery", char::is_whitespace) else { return Ok(None) };
    let Some(rest) = strip_keyword(rest.trim(), "audit", char::is_whitespace) else {
        return Err("DELTA RECOVERY supports only: DELTA RECOVERY AUDIT <table> VERSION <n>".to_string());
    };
    parse_delta_version_cmd(rest, "DELTA RECOVERY AUDIT").map(Some)
}

/// An intercepted `OPTIMIZE <table> WHERE date = 'YYYY-MM-DD'` admin command.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OptimizeCmd {
    pub table: String,
    pub date: chrono::NaiveDate,
    /// Restrict the compaction to one tenant's partition. A whole-date optimize
    /// spans every project's files for that date and can OOM the instance; one
    /// (project, date) partition is the safe unit.
    pub project_id: Option<String>,
}

/// Read and decode the newline-delimited action list of the commit entry `cmd`
/// names, reporting failures as `command`.
async fn commit_actions(log_store: &deltalake::logstore::LogStoreRef, command: &str, cmd: &DeltaVersionCmd) -> PgWireResult<Vec<deltalake::kernel::Action>> {
    let (table, version) = (&cmd.table, cmd.version);
    let bytes = log_store
        .read_commit_entry(version)
        .await
        .map_err(|e| admin_err(format!("{command} '{table}' VERSION {version}: {e}")))?
        .ok_or_else(|| admin_err(format!("{command} '{table}' VERSION {version}: commit not found")))?;
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).map_err(|e| admin_err(format!("decode Delta action: {e}"))))
        .collect()
}

/// The all-VARCHAR `Response::Query` shared by the read-only admin commands:
/// `names` are the columns, and each row yields one already-formatted value per
/// column (or an error to surface in place of that row).
fn text_response<const N: usize>(names: [&str; N], rows: impl Iterator<Item = PgWireResult<Vec<String>>> + Send + 'static) -> Vec<Response> {
    let fields: Arc<Vec<FieldInfo>> =
        Arc::new(names.into_iter().map(|name| FieldInfo::new(name.to_string(), None, None, Type::VARCHAR, FieldFormat::Text)).collect());
    let row_fields = fields.clone();
    let rows = rows.map(move |values| {
        let mut encoder = DataRowEncoder::new(row_fields.clone());
        values?.iter().try_for_each(|value| encoder.encode_field(value))?;
        Ok(encoder.take_row())
    });
    vec![Response::Query(QueryResponse::new(fields, stream::iter(rows)))]
}

fn admin_err(msg: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new("ERROR".into(), "42601".into(), msg.into())))
}

/// `Ok(inner)` when the admin command's dependency (db handle, buffered layer)
/// was wired in, else the standard "not available on this server" error.
fn require_available<T>(opt: Option<T>, name: &str) -> PgWireResult<T> {
    opt.ok_or_else(|| admin_err(format!("{name} is not available on this server")))
}

/// Remainder after a leading case-insensitive `keyword` that ends at end-of-input
/// or at a `boundary` char — so identifiers merely starting with it
/// (`optimizer_stats`, `aborted`) don't match. Remainder is returned untrimmed.
fn strip_keyword<'a>(s: &'a str, keyword: &str, boundary: fn(char) -> bool) -> Option<&'a str> {
    let (head, rest) = s.split_at_checked(keyword.len())?;
    (head.eq_ignore_ascii_case(keyword) && (rest.is_empty() || rest.starts_with(boundary))).then_some(rest)
}

/// Strip a leading admin keyword plus any trailing `;`, returning the trimmed
/// remainder. `None` when `query` isn't that command.
fn strip_command<'a>(query: &'a str, keyword: &str) -> Option<&'a str> {
    strip_keyword(query.trim().trim_end_matches(';').trim(), keyword, char::is_whitespace).map(str::trim)
}

/// `= '<value>'` → `<value>`, tolerating either quote style and loose spacing.
fn filter_value(rest: &str) -> Result<&str, String> {
    Ok(rest.trim().strip_prefix('=').ok_or("expected: <col> = '<value>'")?.trim().trim_matches(['\'', '"']).trim())
}

/// Parse `OPTIMIZE <table> WHERE date = 'YYYY-MM-DD'`.
///
/// - `Ok(None)`: not an OPTIMIZE statement — fall through to DataFusion.
/// - `Ok(Some(_))`: valid, run it.
/// - `Err(msg)`: it *is* OPTIMIZE but malformed. A bare `OPTIMIZE <table>` is
///   rejected on purpose — an unbounded in-process compaction can OOM.
pub(crate) fn parse_optimize(query: &str) -> Result<Option<OptimizeCmd>, String> {
    let Some(rest) = strip_command(query, "optimize") else { return Ok(None) };
    let (table, where_part) = rest.split_once(char::is_whitespace).map(|(t, w)| (t.trim(), w.trim())).unwrap_or((rest, ""));
    if table.is_empty() {
        return Err("OPTIMIZE requires a table and date: OPTIMIZE <table> WHERE date = 'YYYY-MM-DD'".to_string());
    }
    let Some(conds) = strip_keyword(where_part, "where", char::is_whitespace) else {
        return Err(format!(
            "OPTIMIZE {table} needs a date filter: OPTIMIZE {table} WHERE date = 'YYYY-MM-DD' (bare OPTIMIZE is disabled — it would compact all history in-process)"
        ));
    };
    // Values are simple quoted literals, so splitting on ` AND ` needs no
    // nesting awareness.
    static AND: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\s+and\s+").unwrap());
    let (date, project_id) = AND.split(conds.trim()).try_fold((None, None), |(date, project_id), cond| {
        let cond = cond.trim();
        let column = |name| strip_keyword(cond, name, |c: char| c.is_whitespace() || c == '=');
        match (column("date"), column("project_id")) {
            (Some(rest), _) => {
                let val = filter_value(rest)?;
                Ok((Some(val.parse::<chrono::NaiveDate>().map_err(|_| format!("invalid date '{val}', expected YYYY-MM-DD"))?), project_id))
            }
            (_, Some(rest)) => Ok((date, Some(filter_value(rest)?.to_string()))),
            _ => Err("OPTIMIZE supports only `date` and `project_id` filters".to_string()),
        }
    })?;
    let date = date.ok_or("OPTIMIZE requires a date filter: WHERE date = 'YYYY-MM-DD'")?;
    Ok(Some(OptimizeCmd { table: table.to_string(), date, project_id }))
}

/// An intercepted `VACUUM <table> [RETAIN <n> HOURS]` admin command.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct VacuumCmd {
    pub table: String,
    /// `None` → use the configured default retention.
    pub retention_hours: Option<u64>,
}

/// Parse `VACUUM <table> [RETAIN <n> HOURS]`.
///
/// - `Ok(None)`: not a VACUUM statement — fall through to DataFusion.
/// - `Ok(Some(_))`: valid, run it.
/// - `Err(msg)`: it *is* VACUUM but malformed. A bare `VACUUM` (no table) is
///   rejected on purpose. VACUUM is table-wide and takes no date filter;
///   `RETAIN <n> HOURS` overrides the configured retention.
pub(crate) fn parse_vacuum(query: &str) -> Result<Option<VacuumCmd>, String> {
    let Some(rest) = strip_command(query, "vacuum") else { return Ok(None) };
    let (table, tail) = rest.split_once(char::is_whitespace).map(|(t, w)| (t.trim(), w.trim())).unwrap_or((rest, ""));
    if table.is_empty() {
        return Err("VACUUM requires a table: VACUUM <table> [RETAIN <n> HOURS] (bare VACUUM is disabled — name the table)".to_string());
    }
    let retention_hours = (!tail.is_empty())
        .then(|| {
            let lower = tail.to_ascii_lowercase();
            let after = strip_keyword(&lower, "retain", char::is_whitespace)
                .ok_or_else(|| format!("VACUUM {table}: expected optional `RETAIN <n> HOURS`, got '{tail}'"))?
                .trim();
            let num = after.strip_suffix("hours").or_else(|| after.strip_suffix("hour")).unwrap_or(after).trim();
            num.parse::<u64>().map_err(|_| format!("VACUUM {table}: invalid retention '{after}', expected `RETAIN <n> HOURS`"))
        })
        .transpose()?;
    Ok(Some(VacuumCmd { table: table.to_string(), retention_hours }))
}

/// Parse a bare `FLUSH` admin command (not a Postgres statement, so safe to
/// intercept). No arguments on purpose: it drains the whole MemBuffer.
pub(crate) fn parse_flush(query: &str) -> bool {
    strip_command(query, "flush").is_some_and(str::is_empty)
}

/// Parse the leased pre-deploy write fence. Kept distinct from `FLUSH`, which
/// remains an online maintenance command and must not change admission state.
pub(crate) fn parse_handoff(query: &str) -> bool {
    strip_command(query, "handoff").is_some_and(str::is_empty)
}

/// Rewrites Postgres synonyms that DataFusion's SQL parser doesn't accept:
/// `ABORT [ WORK | TRANSACTION ]` → `ROLLBACK` (some pools emit it on session
/// acquisition), plus the `row_to_json` rewrite below.
fn rewrite_pg_synonyms(query: &str) -> Cow<'_, str> {
    let query = strip_keyword(query.trim_start(), "ABORT", |c| c.is_whitespace() || c == ';')
        .map_or(Cow::Borrowed(query), |rest| Cow::Owned(format!("ROLLBACK{rest}")));
    rewrite_row_to_json_record(&query).map_or(query, Cow::Owned)
}

/// Rewrites `row_to_json(t)` over a derived-table alias, which DataFusion
/// rejects while planning the SQL — before any analyzer rule could see it.
///
/// An AST rewrite, not a text substitution: anything that fails to parse, or
/// that the visitor declines to touch, is returned unchanged, so a malformed
/// statement can never be turned into a different valid one here.
fn rewrite_row_to_json_record(query: &str) -> Option<String> {
    use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    if !crate::read::optimizers::might_need_rewrite(query) {
        return None;
    }
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, query).ok()?;
    let [statement] = statements.as_mut_slice() else {
        return None;
    };
    crate::read::optimizers::rewrite(statement).then(|| statement.to_string())
}

/// What a statement DOES, as far as the wire layer needs to know. A type, not a `&str`: the
/// statement-timeout exemption branches on it, and a string there can only be re-parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryKind {
    Select,
    Dml,
    Ddl,
    Other,
}

impl QueryKind {
    /// The `query.type` span field.
    fn as_str(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Dml => "DML",
            Self::Ddl => "DDL",
            Self::Other => "OTHER",
        }
    }
}

/// (keyword, space-padded keyword, kind, operation).
const QUERY_KINDS: [(&str, &str, QueryKind, &str); 7] = [
    ("select", " select ", QueryKind::Select, "SELECT"),
    ("update", " update ", QueryKind::Dml, "UPDATE"),
    ("delete", " delete ", QueryKind::Dml, "DELETE"),
    ("insert", " insert ", QueryKind::Dml, "INSERT"),
    ("create", " create ", QueryKind::Ddl, "CREATE"),
    ("drop", " drop ", QueryKind::Ddl, "DROP"),
    ("alter", " alter ", QueryKind::Ddl, "ALTER"),
];

/// Leading keyword first, across ALL kinds, before any embedded-keyword fallback: the fallback
/// exists for prefixed statements (`WITH ... SELECT`, `EXPLAIN SELECT`), and scanning it in table
/// order let `select` win on `INSERT ... SELECT` — handing a write the read-only statement timeout.
/// In the fallback pass a write outranks a read for the same reason.
fn classify_query(query: &str) -> (QueryKind, &'static str) {
    let q = query.trim().to_lowercase();
    let embedded = || QUERY_KINDS.iter().filter(|(_, _, kind, _)| *kind != QueryKind::Select).chain(QUERY_KINDS.iter());
    QUERY_KINDS
        .iter()
        .find(|(kw, ..)| q.starts_with(kw))
        .or_else(|| embedded().find(|(_, padded, ..)| q.contains(padded)))
        .map_or((QueryKind::Other, "UNKNOWN"), |&(.., kind, operation)| (kind, operation))
}

/// Redact literal values and comments so the result can safely be indexed and
/// used as a stable query fingerprint.
fn normalized_query(query: &str) -> String {
    static BLOCK_COMMENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?s)/\*.*?\*/").unwrap());
    static LINE_COMMENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"--[^\r\n]*").unwrap());
    static DOLLAR_STRING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?s)\$[A-Za-z_0-9]*\$.*?\$[A-Za-z_0-9]*\$").unwrap());
    static STRING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?is)(?:e|u&)?'(?:''|[^'])*'").unwrap());
    static NUMBER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\b").unwrap());
    static WHITESPACE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s+").unwrap());

    let query = BLOCK_COMMENT.replace_all(query, " ");
    let query = LINE_COMMENT.replace_all(&query, " ");
    let query = DOLLAR_STRING.replace_all(&query, "?");
    let query = STRING.replace_all(&query, "?");
    let query = NUMBER.replace_all(&query, "?");
    WHITESPACE.replace_all(&query, " ").trim().to_ascii_lowercase()
}

fn query_template(query: &str) -> String {
    const MAX_CHARS: usize = 512;
    let query = normalized_query(query);
    if query.chars().count() <= MAX_CHARS { query } else { query.chars().take(MAX_CHARS).chain(['.'; 3]).collect() }
}

/// Identity of a normalized query, for tracing and metrics only — never
/// persisted and never a security boundary, hence non-cryptographic XXH3.
fn query_fingerprint(query: &str) -> String {
    format!("{:032x}", twox_hash::XxHash3_128::oneshot(normalized_query(query).as_bytes()))
}

/// Classify `query` and stamp the standard query/db tracing fields onto `span`.
fn record_query_span(span: &tracing::Span, query: &str) {
    let (query_type, operation) = classify_query(query);
    span.record("query.type", query_type.as_str());
    span.record("query.operation", operation);
    span.record("db.operation", operation);
    span.record("query.text", query_template(query));
}

/// Emit one bounded event for statements slow enough to affect the tail. Table
/// and project dimensions are extracted only for diagnosis; raw SQL is never
/// included in this event.
fn record_statement_latency(
    metrics: Option<&crate::database::ScanMetrics>, query: &str, protocol: &'static str, duration_us: u64, failure: Option<&PgWireError>,
) {
    if let Some(metrics) = metrics {
        metrics.record_pgwire_query(duration_us);
    }
    // Emitted here, inside the query span, so a failure names the query that
    // caused it (`LoggingErrorHandler` runs outside the span and cannot).
    // Failures always, successes only when slow.
    const SLOW_QUERY_US: u64 = 1_000_000;
    let slow = duration_us >= SLOW_QUERY_US;
    let success = failure.is_none();
    if success && !slow {
        return;
    }
    let (_, operation) = classify_query(query);
    let (tables, project_id) = query_dimensions(query);
    let (fingerprint, template) = (query_fingerprint(query), query_template(query));
    macro_rules! statement_event {
        ($emit:ident, $event:literal, $message:literal $(, $field:ident = $value:expr)*) => {
            $emit!(
                event = $event,
                query.class = operation,
                query.fingerprint = %fingerprint,
                query.template = %template,
                query.tables = %tables,
                project.id = %project_id,
                protocol,
                duration_us,
                $($field = $value,)*
                $message
            )
        };
    }
    if let Some(error) = failure {
        warn!(
            event = "pgwire.failed_statement",
            failure.class = stream_failure_class(error),
            query.class = operation,
            query.fingerprint = %fingerprint,
            query.template = %template,
            query.tables = %tables,
            project.id = %project_id,
            protocol,
            duration_us,
            "PostgreSQL statement failed"
        );
    }
    if slow {
        statement_event!(info, "pgwire.slow_statement", "slow PostgreSQL statement", success = success);
    }
}

fn query_dimensions(query: &str) -> (String, &str) {
    static TABLES: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"(?i)\b(?:from|join|into|update|table)\s+([\w.\"]+)"#).unwrap());
    static PROJECT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"(?i)\bproject_id\s*=\s*'([^']{1,128})'"#).unwrap());
    let tables = TABLES.captures_iter(query).filter_map(|captures| Some(captures.get(1)?.as_str())).take(3).collect::<Vec<_>>().join(",");
    (tables, PROJECT.captures(query).and_then(|captures| captures.get(1)).map_or("", |m| m.as_str()))
}

#[async_trait]
impl SimpleQueryHandler for LoggingSimpleQueryHandler {
    #[instrument(
        name = "postgres.query.simple",
        skip_all,
        fields(query.text = Empty, query.type = Empty, query.operation = Empty, db.system = "postgresql", db.operation = Empty)
    )]
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let rewritten = rewrite_pg_synonyms(query);
        let query = rewritten.as_ref();

        // Admin commands, caught before DataFusion (whose parser rejects them).
        // Order is significant: `parse_delta_history` ERRORS on any other DELTA
        // statement, so it stays last of the three DELTA parsers.
        macro_rules! admin {
            ($parse:ident => $run:ident) => {
                if let Some(cmd) = $parse(query).map_err(admin_err)? {
                    return self.$run(cmd).await;
                }
            };
            ($parse:ident, $run:ident) => {
                if $parse(query) {
                    return self.$run().await;
                }
            };
        }
        admin!(parse_optimize => run_optimize);
        admin!(parse_vacuum => run_vacuum);
        admin!(parse_delta_recovery_audit => run_delta_recovery_audit);
        admin!(parse_delta_actions => run_delta_actions);
        admin!(parse_delta_history => run_delta_history);
        admin!(parse_flush, run_flush);
        admin!(parse_handoff, run_handoff);

        record_query_span(&tracing::Span::current(), query);
        let client_timeout = client_statement_timeout(client);
        run_statement(
            self.scan_metrics.as_deref(),
            self.max_statement_secs,
            client_timeout,
            query,
            "simple",
            <DfSessionService as SimpleQueryHandler>::do_query(&self.inner, client, query),
            |responses: Vec<Response>, deadline, context| {
                responses.into_iter().map(|response| with_response_deadline(response, deadline, context.clone())).collect()
            },
        )
        .await
    }
}

/// Extended query handler with tracing
pub struct LoggingExtendedQueryHandler {
    inner: DfSessionService,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>,
    max_statement_secs: u64,
    query_parser: Arc<RewritingQueryParser>,
}

#[bon::bon]
impl LoggingExtendedQueryHandler {
    #[builder]
    pub fn new(
        session_context: Arc<SessionContext>, hooks: Vec<Arc<dyn QueryHook>>, scan_metrics: Option<Arc<crate::database::ScanMetrics>>,
        #[builder(default = DEFAULT_MAX_STATEMENT_SECS)] max_statement_secs: u64,
    ) -> Self {
        let inner = DfSessionService::new_with_hooks(session_context, hooks);
        let query_parser = Arc::new(RewritingQueryParser { inner: ExtendedQueryHandler::query_parser(&inner) });
        Self { inner, scan_metrics, max_statement_secs, query_parser }
    }
}

/// Applies the same statement rewrites to the extended protocol that
/// `rewrite_pg_synonyms` applies to the simple one, so identical SQL does not
/// succeed or fail depending on how the client sent it.
pub struct RewritingQueryParser {
    inner: Arc<<DfSessionService as ExtendedQueryHandler>::QueryParser>,
}

#[async_trait]
impl datafusion_postgres::pgwire::api::stmt::QueryParser for RewritingQueryParser {
    type Statement = <DfSessionService as ExtendedQueryHandler>::Statement;

    async fn parse_sql<C>(&self, client: &C, sql: &str, types: &[Option<Type>]) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let rewritten = rewrite_row_to_json_record(sql);
        self.inner.parse_sql(client, rewritten.as_deref().unwrap_or(sql), types).await
    }

    fn get_parameter_types(&self, statement: &Self::Statement) -> PgWireResult<Vec<Type>> {
        self.inner.get_parameter_types(statement)
    }

    fn get_result_schema(
        &self, statement: &Self::Statement, format: Option<&datafusion_postgres::pgwire::api::portal::Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        self.inner.get_result_schema(statement, format)
    }
}

#[async_trait]
impl ExtendedQueryHandler for LoggingExtendedQueryHandler {
    type Statement = <DfSessionService as ExtendedQueryHandler>::Statement;
    type QueryParser = RewritingQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.query_parser)
    }

    async fn do_describe_statement<C>(&self, client: &mut C, statement: &StoredStatement<Self::Statement>) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.inner.do_describe_statement(client, statement).await
    }

    async fn do_describe_portal<C>(&self, client: &mut C, portal: &Portal<Self::Statement>) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.inner.do_describe_portal(client, portal).await
    }

    #[instrument(
        name = "postgres.query.extended",
        skip_all,
        fields(query.text = Empty, query.type = Empty, query.operation = Empty, query.portal = %portal.name, query.max_rows = max_rows, db.system = "postgresql", db.operation = Empty)
    )]
    async fn do_query<C>(&self, client: &mut C, portal: &Portal<Self::Statement>, max_rows: usize) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query = &portal.statement.statement.0;
        record_query_span(&tracing::Span::current(), query);
        let client_timeout = client_statement_timeout(client);
        run_statement(
            self.scan_metrics.as_deref(),
            self.max_statement_secs,
            client_timeout,
            query,
            "extended",
            <DfSessionService as ExtendedQueryHandler>::do_query(&self.inner, client, portal, max_rows),
            with_response_deadline,
        )
        .await
    }
}

fn handler_factory(
    session_context: Arc<SessionContext>, auth_config: AuthConfig, scan_metrics: Option<Arc<crate::database::ScanMetrics>>, db: Option<Arc<Database>>,
) -> Arc<LoggingHandlerFactory> {
    let max_statement_secs = db.as_ref().map_or(DEFAULT_MAX_STATEMENT_SECS, |db| db.config().core.timefusion_pgwire_max_statement_secs);
    Arc::new(
        LoggingHandlerFactory::builder()
            .session_context(session_context)
            .auth_config(auth_config)
            .max_statement_secs(max_statement_secs)
            .maybe_scan_metrics(scan_metrics)
            .maybe_db(db)
            .build(),
    )
}

/// Start the server with custom handlers. `db` enables the admin commands
/// (OPTIMIZE / VACUUM / FLUSH / HANDOFF); without it they error as unavailable.
pub async fn serve_with_logging(
    session_context: Arc<SessionContext>, options: &datafusion_postgres::ServerOptions, auth_config: AuthConfig,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>, db: Option<Arc<Database>>, shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    let handlers = handler_factory(session_context, auth_config, scan_metrics, db);
    datafusion_postgres::serve_with_handlers(handlers, options, shutdown).await?;
    Ok(())
}

/// Variant of `serve_with_logging` over a pre-bound listener. The listener's
/// host/port/backlog were set at bind time; `options` here contributes only
/// TLS config and connection-limit settings.
pub async fn serve_with_listener(
    listener: tokio::net::TcpListener, session_context: Arc<SessionContext>, options: &datafusion_postgres::ServerOptions, auth_config: AuthConfig,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>, db: Option<Arc<Database>>, shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    let handlers = handler_factory(session_context, auth_config, scan_metrics, db);
    datafusion_postgres::serve_with_listener(listener, handlers, options, shutdown).await?;
    Ok(())
}

#[cfg(test)]
mod pgwire_handlers_tests {
    use super::{
        StreamFailureContext, parse_delta_actions, parse_delta_history, parse_delta_recovery_audit, parse_flush, parse_handoff, parse_optimize, parse_vacuum,
        query_dimensions, query_fingerprint, query_template, rewrite_pg_synonyms, with_response_deadline,
    };
    use datafusion_postgres::pgwire::{
        api::results::{QueryResponse, Response},
        error::PgWireError,
        messages::data::DataRow,
    };
    use futures::StreamExt;
    use std::{sync::Arc, time::Duration};
    use test_case::test_case;

    /// Admin-command tables flatten the parsed fields into one `|`-joined literal (one expected value pins every field); a rejection becomes `Err(())`.
    type Parsed = Result<Option<String>, ()>;

    #[tokio::test]
    async fn query_stream_observes_the_server_deadline() {
        let rows = futures::stream::once(async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Err::<DataRow, _>(crate::server::pg_compat::statement_timeout_error())
        });
        let deadline = Some(tokio::time::Instant::now() + Duration::from_millis(1));
        let context = StreamFailureContext::new("SELECT pg_sleep(1)", "simple", Some(Duration::from_millis(1)), std::time::Instant::now());
        let Response::Query(mut response) = with_response_deadline(Response::Query(QueryResponse::new(Arc::new(vec![]), rows)), deadline, context) else {
            panic!("expected query response");
        };
        assert!(matches!(response.data_rows.next().await, Some(Err(PgWireError::UserError(_)))));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn late_stream_failures_keep_scrubbed_query_context() -> anyhow::Result<()> {
        let log = tempfile::NamedTempFile::new()?;
        let subscriber = tracing_subscriber::fmt().without_time().with_ansi(false).with_writer(std::sync::Mutex::new(log.reopen()?)).finish();
        {
            // A thread-local default plus the current-thread runtime makes the
            // capture independent of other tests installing tracing dispatchers.
            let _subscriber = tracing::subscriber::set_default(subscriber);
            for (case, deadline, fails, stalls) in [
                ("error", false, true, false),
                ("bounded_error", true, true, false),
                ("timeout", true, true, true),
                ("success", false, false, false),
                ("bounded_success", true, false, false),
            ] {
                let rows = if stalls {
                    futures::stream::pending().boxed()
                } else {
                    futures::stream::iter([if fails {
                        Err(PgWireError::ApiError(Box::new(
                            datafusion::error::DataFusionError::ResourcesExhausted("sort reservation detail".into()).context("outer sort context"),
                        )))
                    } else {
                        Ok(DataRow::new(bytes::BytesMut::new(), 0))
                    }])
                    .boxed()
                };
                let span = tracing::info_span!("query_context", case, query.text = tracing::field::Empty);
                let query = "SELECT 'private-literal-canary' AS value FROM otel_logs_and_spans";
                super::record_query_span(&span, query);
                let timeout = deadline.then_some(Duration::from_secs(1));
                let context = StreamFailureContext::new(query, "extended", timeout, std::time::Instant::now());
                let response = span.in_scope(|| {
                    with_response_deadline(
                        Response::Query(QueryResponse::new(Arc::new(vec![]), rows)),
                        timeout.map(|timeout| tokio::time::Instant::now() + timeout),
                        context,
                    )
                });
                drop(span);
                let Response::Query(mut response) = response else { panic!("expected query response") };
                assert_eq!(response.data_rows.next().await.unwrap().is_err(), fails, "{case}");
                assert!(response.data_rows.next().await.is_none(), "{case}");
            }
            let failure = crate::server::pg_compat::statement_timeout_error();
            super::record_statement_latency(None, "SELECT 'private-literal-canary' FROM otel_logs_and_spans", "simple", 10, Some(&failure));
        }
        let output = std::fs::read_to_string(log.path())?;
        let failures: Vec<_> = output.lines().filter(|line| line.contains("pgwire.stream_failed")).collect();
        assert_eq!(failures.len(), 3, "late failures must be attributed: {output}");
        assert!(failures.iter().all(|line| {
            line.contains("query_context")
                && line.contains("query.text")
                && line.contains("query.fingerprint")
                && line.contains("query.template")
                && line.contains("query.tables=otel_logs_and_spans")
                && line.contains("protocol=\"extended\"")
        }));
        assert_eq!(failures.iter().filter(|line| line.contains("failure.class=\"resource\"")).count(), 2, "resource failures must be classified: {output}");
        assert_eq!(
            failures.iter().filter(|line| line.contains("failure.class=\"statement_timeout\"")).count(),
            1,
            "stream timeout must be classified: {output}"
        );
        assert_eq!(
            failures.iter().filter(|line| line.contains("deadline_ms=Some(1000)")).count(),
            2,
            "bounded failures must carry the effective deadline: {output}"
        );
        assert_eq!(failures.iter().filter(|line| line.contains("sort reservation detail")).count(), 2, "resource cause must stay on the event line: {output}");
        let failed_statement = output.lines().find(|line| line.contains("pgwire.failed_statement")).expect("pre-stream failure event");
        assert!(
            failed_statement.contains("failure.class=\"statement_timeout\"")
                && failed_statement.contains("query.fingerprint")
                && failed_statement.contains("query.template")
                && failed_statement.contains("protocol=\"simple\""),
            "pre-stream failure must carry the same attribution: {failed_statement}"
        );
        assert!(!output.contains("private-literal-canary"));
        Ok(())
    }

    /// The statement timeout applies to reads only.
    #[test_case("SELECT count(*) FROM otel_logs_and_spans" => true ; "read")]
    #[test_case("SHOW server_version" => true ; "show")]
    #[test_case("INSERT INTO otel_logs_and_spans VALUES (1)" => false ; "insert")]
    #[test_case("UPDATE otel_logs_and_spans SET name = 'x' WHERE id = '1'" => false ; "update")]
    #[test_case("DELETE FROM otel_logs_and_spans WHERE id = '1'" => false ; "delete")]
    #[test_case("INSERT INTO otel_logs_and_spans SELECT * FROM staging" => false ; "INSERT ... SELECT is a write, not the SELECT its first keyword table-order match found")]
    #[test_case("WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x" => false ; "a data-modifying CTE is a write")]
    #[test_case("EXPLAIN SELECT 1" => true ; "a prefixed read still reaches the embedded-keyword fallback")]
    fn the_statement_timeout_never_applies_to_a_write(query: &str) -> bool {
        super::statement_timeout_applies(query)
    }

    /// Pins a LIMITATION: the deadline is cooperative (`timeout_at` only fires on a `Pending` poll), which is why long-running operators
    /// (`DedupExec`, `GatedScanExec`) wrap their output in `coop::make_cooperative`. Rewrite this only if the deadline is made preemptive.
    #[tokio::test(start_paused = true)]
    async fn the_statement_deadline_cannot_interrupt_a_future_that_never_yields() {
        use super::run_with_statement_timeout;
        // Awaits a timer ⇒ returns Pending, so the deadline is observed; the paused clock auto-advances when idle, so there is no real 10s wait.
        let yielding = async {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            Ok(())
        };
        let timed_out = run_with_statement_timeout(Some(std::time::Duration::from_secs(1)), yielding).await;
        assert!(timed_out.is_err(), "a future that yields must be interruptible by the deadline");

        // Never yields ⇒ runs to completion regardless of the deadline.
        let non_yielding = async { Ok(41 + 1) };
        let (value, _) = run_with_statement_timeout(Some(std::time::Duration::from_nanos(1)), non_yielding)
            .await
            .expect("a non-yielding future outruns the deadline instead of being cancelled");
        assert_eq!(value, 42, "it completed, which is precisely the failure mode");
    }

    // Case/spacing/quote/semicolon tolerance; bare OPTIMIZE (no date) is rejected — it would compact all history in-process.
    #[test_case("OPTIMIZE otel_logs_and_spans WHERE date = '2026-06-19'" => Ok(Some("otel_logs_and_spans|2026-06-19|None".to_string())) ; "table and date")]
    #[test_case("optimize t where DATE='2026-01-02';" => Ok(Some("t|2026-01-02|None".to_string())) ; "lowercase, unspaced, trailing semicolon")]
    #[test_case("  OPTIMIZE  t  WHERE  date  =  \"2026-01-02\"  " => Ok(Some("t|2026-01-02|None".to_string())) ; "loose spacing and double quotes")]
    // Tenant-scoped compaction: one (project, date) partition is the safe unit.
    #[test_case("OPTIMIZE t WHERE project_id = 'p-1' AND date = '2026-01-02'" => Ok(Some("t|2026-01-02|Some(\"p-1\")".to_string())) ; "project_id then date")]
    #[test_case("optimize t where date='2026-01-02' and PROJECT_ID=\"p-1\"" => Ok(Some("t|2026-01-02|Some(\"p-1\")".to_string())) ; "date then project_id, mixed case")]
    #[test_case("OPTIMIZE t WHERE date = '2026-01-02'" => Ok(Some("t|2026-01-02|None".to_string())) ; "date alone leaves project_id unset")]
    #[test_case("OPTIMIZE otel_logs_and_spans" => Err(()) ; "bare OPTIMIZE on a table is unbounded")]
    #[test_case("OPTIMIZE" => Err(()) ; "bare OPTIMIZE")]
    #[test_case("OPTIMIZE t WHERE project_id = 'x'" => Err(()) ; "project_id alone is not a date bound")]
    #[test_case("OPTIMIZE t WHERE date = 'not-a-date'" => Err(()) ; "bad date")]
    #[test_case("OPTIMIZE t WHERE date = '2026-01-02' AND name = 'x'" => Err(()) ; "unknown column")]
    #[test_case("SELECT 1" => Ok(None) ; "select falls through")]
    #[test_case("INSERT INTO t VALUES (1)" => Ok(None) ; "insert falls through")]
    // Don't false-match an identifier that merely starts with "optimize".
    #[test_case("SELECT optimizer FROM t" => Ok(None) ; "optimizer column is not a match")]
    #[test_case("optimizer_stats" => Ok(None) ; "optimizer_stats is not a match")]
    fn optimize_parses_bounded_tenant_scoped_compaction_only(query: &str) -> Parsed {
        parse_optimize(query).map_err(|_| ()).map(|cmd| cmd.map(|c| format!("{}|{}|{:?}", c.table, c.date, c.project_id)))
    }

    // RETAIN clause plus case/plural/semicolon tolerance; unlike OPTIMIZE, a bare VACUUM (no table) is rejected — name the table.
    #[test_case("VACUUM otel_logs_and_spans" => Ok(Some("otel_logs_and_spans|None".to_string())) ; "table, default retention")]
    #[test_case("vacuum t RETAIN 48 HOURS;" => Ok(Some("t|Some(48)".to_string())) ; "plural HOURS with trailing semicolon")]
    #[test_case("  VACUUM  t  retain  1  hour  " => Ok(Some("t|Some(1)".to_string())) ; "singular hour, loose spacing")]
    #[test_case("VACUUM" => Err(()) ; "bare VACUUM names no table")]
    #[test_case("VACUUM t WHERE date = '2026-01-01'" => Err(()) ; "unknown trailing clause")]
    #[test_case("VACUUM t RETAIN abc HOURS" => Err(()) ; "non-numeric retention")]
    #[test_case("SELECT 1" => Ok(None) ; "select falls through")]
    // Don't false-match an identifier that merely starts with "vacuum".
    #[test_case("SELECT vacuumed FROM t" => Ok(None) ; "vacuumed column is not a match")]
    #[test_case("vacuum_log" => Ok(None) ; "vacuum_log is not a match")]
    fn vacuum_parses_table_and_optional_retention(query: &str) -> Parsed {
        parse_vacuum(query).map_err(|_| ()).map(|cmd| cmd.map(|c| format!("{}|{:?}", c.table, c.retention_hours)))
    }

    // Both verbs are argument-free on purpose (FLUSH drains the MemBuffer, HANDOFF leases the pre-deploy write fence); arguments or a mere prefix fall through.
    #[test_case(parse_flush, "FLUSH" => true ; "bare FLUSH")]
    #[test_case(parse_flush, "  flush ; " => true ; "flush, padded and semicoloned")]
    #[test_case(parse_flush, "FLUSH t" => false ; "FLUSH takes no argument")]
    #[test_case(parse_flush, "SELECT flushed FROM t" => false ; "flushed column is not a match")]
    #[test_case(parse_flush, "flush_log" => false ; "flush_log is not a match")]
    #[test_case(parse_handoff, "HANDOFF" => true ; "bare HANDOFF")]
    #[test_case(parse_handoff, "  handoff ; " => true ; "handoff, padded and semicoloned")]
    #[test_case(parse_handoff, "HANDOFF now" => false ; "HANDOFF takes no argument")]
    #[test_case(parse_handoff, "SELECT handoff FROM t" => false ; "handoff column is not a match")]
    fn admin_verb_parses_bare_only(parse: fn(&str) -> bool, query: &str) -> bool {
        parse(query)
    }

    #[test_case("DELTA HISTORY otel_logs_and_spans LIMIT 250;" => Ok(Some("otel_logs_and_spans|250".to_string())) ; "explicit limit")]
    #[test_case("delta history t" => Ok(Some("t|100".to_string())) ; "default limit")]
    #[test_case("DELTA HISTORY t LIMIT 0" => Err(()) ; "limit below the bound")]
    #[test_case("DELTA HISTORY t LIMIT 10001" => Err(()) ; "limit above the bound")]
    #[test_case("DELTA RESTORE t" => Err(()) ; "DELTA is read-only: RESTORE is rejected, not run")]
    #[test_case("SELECT delta FROM t" => Ok(None) ; "delta column is not a match")]
    fn delta_history_parses_bounded_read_only_command(query: &str) -> Parsed {
        parse_delta_history(query).map_err(|_| ()).map(|cmd| cmd.map(|c| format!("{}|{}", c.table, c.limit)))
    }

    // `DELTA ACTIONS` and `DELTA RECOVERY AUDIT` share `<table> VERSION <n>`, so they share one table; `parse` selects the command under test.
    #[test_case(parse_delta_actions, "DELTA ACTIONS otel_logs_and_spans VERSION 462919;" => Ok(Some("otel_logs_and_spans|462919".to_string())) ; "actions at one exact version")]
    #[test_case(parse_delta_actions, "DELTA ACTIONS t" => Err(()) ; "actions without a version")]
    #[test_case(parse_delta_actions, "DELTA ACTIONS t VERSION nope" => Err(()) ; "actions with a non-numeric version")]
    #[test_case(parse_delta_actions, "SELECT 1" => Ok(None) ; "actions falls through on select")]
    #[test_case(parse_delta_recovery_audit, "DELTA RECOVERY AUDIT otel_logs_and_spans VERSION 462921;" => Ok(Some("otel_logs_and_spans|462921".to_string())) ; "audit at one exact version")]
    #[test_case(parse_delta_recovery_audit, "DELTA RECOVERY otel_logs_and_spans VERSION 462921" => Err(()) ; "DELTA RECOVERY must say AUDIT explicitly")]
    #[test_case(parse_delta_recovery_audit, "DELTA RECOVERY AUDIT t VERSION nope" => Err(()) ; "audit with a non-numeric version")]
    #[test_case(parse_delta_recovery_audit, "SELECT 1" => Ok(None) ; "audit falls through on select")]
    fn delta_version_commands_require_one_exact_version(parse: fn(&str) -> Result<Option<super::DeltaVersionCmd>, String>, query: &str) -> Parsed {
        parse(query).map_err(|_| ()).map(|cmd| cmd.map(|c| format!("{}|{}", c.table, c.version)))
    }

    #[test]
    fn slow_query_dimensions_are_bounded_and_sql_free() {
        let (tables, project_id) = query_dimensions("SELECT * FROM logs JOIN traces ON true WHERE project_id = 'project-123' AND body = 'secret'");
        assert_eq!(tables, "logs,traces");
        assert_eq!(project_id, "project-123");
        assert!(!tables.contains("secret"));
    }

    #[test]
    fn query_template_redacts_literals_and_has_a_stable_fingerprint() {
        let first = "SELECT * FROM logs WHERE project_id = 'project-123' AND body = 'secret' AND n = 42 -- do not log";
        let second = "select * from logs where project_id = 'project-456' and body = 'other' and n = 7";
        let first_template = query_template(first);
        assert_eq!(first_template, "select * from logs where project_id = ? and body = ? and n = ?");
        assert_eq!(first_template, query_template(second));
        assert_eq!(query_fingerprint(first), query_fingerprint(second));
        assert!(!first_template.contains("secret"));
        assert!(!first_template.contains("project-123"));
    }

    // Non-ABORT queries take the Cow::Borrowed fast path (content identical); identifiers merely starting with ABORT must not match.
    #[test_case("ABORT" => "ROLLBACK" ; "bare ABORT")]
    #[test_case("ABORT;" => "ROLLBACK;" ; "ABORT with semicolon")]
    #[test_case("  abort  " => "ROLLBACK  " ; "lowercase, padded")]
    #[test_case("Abort Work" => "ROLLBACK Work" ; "ABORT WORK")]
    #[test_case("ABORT TRANSACTION;" => "ROLLBACK TRANSACTION;" ; "ABORT TRANSACTION")]
    #[test_case("SELECT 1" => "SELECT 1" ; "select unchanged")]
    #[test_case("BEGIN" => "BEGIN" ; "begin unchanged")]
    #[test_case("ROLLBACK" => "ROLLBACK" ; "rollback unchanged")]
    #[test_case("SELECT aborted FROM t" => "SELECT aborted FROM t" ; "aborted column is not a match")]
    #[test_case("ABORTED" => "ABORTED" ; "ABORTED is not a match")]
    fn abort_rewrites_to_rollback_and_nothing_else_changes(query: &str) -> String {
        rewrite_pg_synonyms(query).into_owned()
    }
}

// ===== pgwire_early_bind =====
// Early-bind responder that occupies the pgwire port during the slow startup
// window, answering every connection with SQLSTATE 57P03 "the database system
// is starting up" — which clients treat as transient, unlike the ECONNREFUSED
// they would otherwise see. The same `TcpListener` is then handed to
// `serve_with_listener`, so there is no rebind gap.

use std::{io, time::Duration};

use datafusion_postgres::pgwire::messages::startup::{GssEncRequest, SslRequest};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::debug;

const SSL_REQUEST_CODE: u32 = SslRequest::BODY_MAGIC_NUMBER as u32;
const GSS_REQUEST_CODE: u32 = GssEncRequest::BODY_MAGIC_NUMBER as u32;
/// Cap on the StartupMessage we'll drain: well above any legitimate payload
/// (real clients send under 1 KiB), and bounds a hostile client's work.
const MAX_STARTUP_BYTES: u64 = 64 * 1024;
const STARTUP_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Budget for the over-cap fast path: response write only, no startup drain.
const CAP_RESPONSE_TIMEOUT: Duration = Duration::from_secs(1);
/// Hard cap on concurrent early-bind handlers: a reconnect storm would
/// otherwise spawn unbounded tasks, each holding an FD for STARTUP_READ_TIMEOUT.
const MAX_CONCURRENT_EARLY_HANDLERS: usize = 512;

/// Run the 57P03 acceptor on `listener` until `shutdown` is cancelled.
pub async fn run_until_ready(listener: &TcpListener, shutdown: CancellationToken) {
    accept_loop(listener, shutdown, MAX_CONCURRENT_EARLY_HANDLERS).await;
}

async fn accept_loop(listener: &TcpListener, shutdown: CancellationToken, max_handlers: usize) {
    let response: Arc<[u8]> = build_starting_up_response().into();
    let permits = Arc::new(tokio::sync::Semaphore::new(max_handlers));
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            res = listener.accept() => match res {
                Ok((sock, addr)) => {
                    // Over the cap we still send the canned 57P03 frame — dropping the
                    // socket unanswered would RST, the failure mode this responder exists
                    // to avoid — but skip the startup drain to keep the task ~ms-bounded.
                    let permit = Arc::clone(&permits).try_acquire_owned().ok();
                    let (limit, drain) = match &permit {
                        Some(_) => (STARTUP_READ_TIMEOUT, true),
                        None => {
                            warn!("early-bind: at {max_handlers}-handler cap, fast-responding to {addr}");
                            (CAP_RESPONSE_TIMEOUT, false)
                        }
                    };
                    let resp = Arc::clone(&response);
                    tokio::spawn(async move {
                        let _permit = permit;
                        match tokio::time::timeout(limit, handle_one(sock, &resp, drain)).await {
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

async fn handle_one(mut sock: TcpStream, response: &[u8], drain_startup: bool) -> io::Result<()> {
    // SSL/GSS negotiation precedes the real StartupMessage; both are 8 bytes
    // (length + magic). Drain whichever shape arrives, then send 57P03.
    // pg length fields include the 4-byte length itself. In the non-SSL branch
    // we've also consumed the 4-byte code → drain `len - 8`; in the SSL/GSS
    // branch we've consumed only the length of the *real* startup → drain
    // `real_len - 4`.
    if drain_startup {
        let len = sock.read_u32().await? as u64;
        let n = match sock.read_u32().await? {
            SSL_REQUEST_CODE | GSS_REQUEST_CODE => {
                sock.write_all(b"N").await?;
                (sock.read_u32().await? as u64).checked_sub(4)
            }
            _ => len.checked_sub(8),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "startup length below 8-byte header"))?;
        if n > MAX_STARTUP_BYTES {
            return Err(io::Error::new(io::ErrorKind::InvalidData, format!("startup body {n} exceeds {MAX_STARTUP_BYTES}-byte cap")));
        }
        tokio::io::copy(&mut (&mut sock).take(n), &mut tokio::io::sink()).await?;
    }

    sock.write_all(response).await?;
    let _ = sock.shutdown().await;
    Ok(())
}

/// Wire format: `Byte1('E') Int32(length) [Byte1(tag) String(value)]* Byte1(0)`
fn build_starting_up_response() -> Vec<u8> {
    let body: Vec<u8> = [(b'S', "FATAL"), (b'V', "FATAL"), (b'C', "57P03"), (b'M', "the database system is starting up")]
        .into_iter()
        .flat_map(|(tag, value)| [tag].into_iter().chain(value.bytes()).chain([0]))
        .chain([0])
        .collect();
    (*b"E").into_iter().chain(((body.len() + 4) as u32).to_be_bytes()).chain(body).collect()
}

#[cfg(test)]
mod pgwire_early_bind_tests {
    use super::*;
    use test_case::test_case;

    const PROTO_3_0: u32 = 0x0003_0000;

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

    async fn spawn_acceptor(max_handlers: usize) -> (u16, CancellationToken, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        let task = tokio::spawn(async move { accept_loop(&listener, token, max_handlers).await });
        (port, shutdown, task)
    }

    /// The server always closes after answering, so draining to EOF yields the
    /// whole reply; empty means the connection was dropped unanswered.
    async fn read_reply(client: &mut TcpStream) -> Vec<u8> {
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        buf
    }

    /// One startup shape per case: an optional SSL/GSS negotiation round, the
    /// declared StartupMessage length, and the params body. `true` means the
    /// canned 57P03 frame arrived (and the server then closed); `false` means
    /// the connection was dropped unanswered.
    #[test_case(None, 8, b"" => true ; "plain startup then close")]
    #[test_case(Some(SSL_REQUEST_CODE), 8, b"" => true ; "ssl request then startup")]
    #[test_case(Some(GSS_REQUEST_CODE), 8, b"" => true ; "gss request then startup")]
    // Exercises drain_body with n > 0 (the cases above send len=8, n=0).
    #[test_case(None, 8 + 23, b"user\0foo\0database\0bar\0\0" => true ; "drains startup params then responds")]
    // Oversized declared length must trip the MAX_STARTUP_BYTES guard;
    // the server drops the connection without sending a response.
    #[test_case(None, MAX_STARTUP_BYTES as u32 + 1024, b"" => false ; "rejects oversized startup")]
    #[tokio::test]
    async fn responds_to_every_startup_shape(negotiation: Option<u32>, declared_len: u32, params: &[u8]) -> bool {
        let (port, shutdown, task) = spawn_acceptor(MAX_CONCURRENT_EARLY_HANDLERS).await;
        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        if let Some(magic) = negotiation {
            client.write_all(&8u32.to_be_bytes()).await.unwrap();
            client.write_all(&magic.to_be_bytes()).await.unwrap();
            let mut n_reply = [0u8; 1];
            client.read_exact(&mut n_reply).await.unwrap();
            assert_eq!(n_reply[0], b'N', "negotiation must be declined with 'N'");
        }
        client.write_all(&declared_len.to_be_bytes()).await.unwrap();
        client.write_all(&PROTO_3_0.to_be_bytes()).await.unwrap();
        client.write_all(params).await.unwrap();
        let reply = read_reply(&mut client).await;
        let answered = !reply.is_empty();
        if answered {
            assert_eq!(reply, build_starting_up_response(), "must be the canned 57P03 frame, then close");
        }
        shutdown.cancel();
        let _ = task.await;
        answered
    }

    /// A client that connects but never sends a startup message must be
    /// closed after STARTUP_READ_TIMEOUT instead of holding the slot forever.
    #[tokio::test(start_paused = true)]
    async fn silent_client_closed_after_timeout() {
        let (port, shutdown, task) = spawn_acceptor(MAX_CONCURRENT_EARLY_HANDLERS).await;
        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        // No bytes sent; advance virtual time past the timeout.
        tokio::time::advance(STARTUP_READ_TIMEOUT + Duration::from_secs(1)).await;
        assert!(read_reply(&mut client).await.is_empty(), "server must close after timeout");
        shutdown.cancel();
        let _ = task.await;
    }

    /// At the handler cap, excess connections still receive 57P03 rather than
    /// an RST (which clients report as ECONNREFUSED).
    #[tokio::test]
    async fn cap_serves_57p03_without_waiting_for_startup() {
        let (port, shutdown, task) = spawn_acceptor(1).await;

        // First connection holds the only permit — never sends startup, so it
        // sits inside handle_one's read awaiting bytes.
        let _holder = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

        // Poll rather than sleep a fixed amount: open probes until one comes
        // back with the canned frame, confirming the cap fast-path is firing.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut probe = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            if let Ok(reply) = tokio::time::timeout(Duration::from_millis(200), read_reply(&mut probe)).await {
                assert_eq!(reply, build_starting_up_response(), "cap fast-path must serve the canned 57P03 frame");
                break;
            }
            assert!(std::time::Instant::now() < deadline, "cap fast-path never observed");
            tokio::task::yield_now().await;
        }

        shutdown.cancel();
        let _ = task.await;
    }
}

#[cfg(test)]
mod streaming_tests {
    use datafusion_postgres::pgwire::{
        api::{query::send_partial_query_response, results::QueryResponse},
        error::PgWireError,
        messages::{PgWireBackendMessage, data::DataRow},
    };
    use futures::{SinkExt, StreamExt, channel::mpsc};
    use std::sync::Arc;

    // The upstream stays open until the receiver sees a row. Collecting the
    // stream, or deferring the socket flush until completion, deadlocks here.
    #[tokio::test]
    async fn streams_before_completion_and_resumes_finite_fetches() {
        let (mut input, rows) = mpsc::channel(1);
        let (output, mut received) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            let mut output = output.sink_map_err(|_: mpsc::SendError| PgWireError::QueryCanceled);
            let mut response = QueryResponse::new(Arc::new(vec![]), rows);
            assert!(send_partial_query_response(&mut output, &mut response, 1).await.unwrap());
            assert!(!send_partial_query_response(&mut output, &mut response, 0).await.unwrap());
        });
        input.send(Ok(DataRow::new(bytes::BytesMut::new(), 0))).await.unwrap();
        assert!(matches!(tokio::time::timeout(std::time::Duration::from_secs(2), received.next()).await.unwrap(), Some(PgWireBackendMessage::DataRow(_))));
        assert!(matches!(received.next().await, Some(PgWireBackendMessage::PortalSuspended(_))));
        input.send(Ok(DataRow::new(bytes::BytesMut::new(), 0))).await.unwrap();
        assert!(matches!(tokio::time::timeout(std::time::Duration::from_secs(2), received.next()).await.unwrap(), Some(PgWireBackendMessage::DataRow(_))));
        drop(input);
        assert!(matches!(received.next().await, Some(PgWireBackendMessage::CommandComplete(_))));
        task.await.unwrap();
    }
    #[derive(Debug)]
    struct GatedInput {
        batch: arrow::record_batch::RecordBatch,
        release: Arc<tokio::sync::Notify>,
        fail_after: bool,
    }

    impl datafusion::physical_plan::streaming::PartitionStream for GatedInput {
        fn schema(&self) -> &arrow::datatypes::SchemaRef {
            self.batch.schema_ref()
        }
        fn execute(&self, _: Arc<datafusion::execution::TaskContext>) -> datafusion::physical_plan::SendableRecordBatchStream {
            let batch = self.batch.clone();
            let schema = batch.schema();
            let release = self.release.clone();
            let fail_after = self.fail_after;
            let rows = futures::stream::once(async move { Ok(batch) }).chain(futures::stream::once(async move {
                release.notified().await;
                if fail_after {
                    Err(datafusion::error::DataFusionError::Execution("late input failure".into()))
                } else {
                    Ok(arrow::record_batch::RecordBatch::new_empty(schema))
                }
            }));
            Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(self.batch.schema(), rows))
        }
    }

    /// Drains until the stream yields an error, asserting `expect` when the site
    /// pins a specific SQLSTATE. Panics with `why` if the stream ends cleanly.
    async fn drain_to_error<T>(
        rows: &mut (impl futures::Stream<Item = Result<T, tokio_postgres::Error>> + Unpin), expect: Option<tokio_postgres::error::SqlState>, why: &str,
    ) {
        while let Some(row) = rows.next().await {
            if let Err(error) = row {
                if let Some(code) = &expect {
                    assert_eq!(error.code(), Some(code));
                }
                return;
            }
        }
        panic!("{why}");
    }

    // End-to-end over real TCP with the production handlers; the gated input
    // cannot finish until a row has reached the client.
    #[tokio::test]
    async fn wire_streams_cancels_and_reuses_connection() -> anyhow::Result<()> {
        use super::{AuthConfig, handler_factory};
        use arrow::{
            array::Int64Array,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use datafusion::{catalog::streaming::StreamingTable, execution::context::SessionContext};
        use tokio_postgres::NoTls;
        let ctx = SessionContext::new_with_config(datafusion::prelude::SessionConfig::new().with_target_partitions(4));
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let release = Arc::new(tokio::sync::Notify::new());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])?;
        for (name, fail_after) in [("gated_error", true), ("gated", false)] {
            let input = GatedInput { batch: batch.clone(), release: release.clone(), fail_after };
            ctx.register_table(name, Arc::new(StreamingTable::try_new(schema.clone(), vec![Arc::new(input)])?))?;
        }
        let handlers = handler_factory(Arc::new(ctx), AuthConfig { username: "postgres".into(), password: Some("test".into()) }, None, None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let handlers = handlers.clone();
                connections.spawn(async move { datafusion_postgres::pgwire::tokio::process_socket(socket, None, handlers).await });
            }
        });
        macro_rules! connect {
            () => {{
                let (client, connection) = tokio_postgres::connect(&format!("host=127.0.0.1 port={port} user=postgres password=test"), NoTls).await?;
                tasks.spawn(async move {
                    let _ = connection.await;
                });
                client
            }};
        }
        let mut client = connect!();
        let other = connect!();
        let explain = "EXPLAIN SELECT n, count(*) FROM gated GROUP BY n";
        client.batch_execute("SET datafusion.execution.target_partitions = 1").await?;
        let plan = |rows: Vec<tokio_postgres::Row>| rows.iter().map(|row| row.get::<_, String>(1)).collect::<Vec<_>>().join("\n");
        assert!(plan(client.query(explain, &[]).await?).contains("mode=Single"), "extended queries must see this connection's simple SET");
        assert!(plan(other.query(explain, &[]).await?).contains("RepartitionExec"), "another connection must retain its own settings");
        other.batch_execute("SET datafusion.execution.target_partitions = 8").await?;
        assert!(plan(client.query(explain, &[]).await?).contains("mode=Single"), "another connection's SET must not leak back");

        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let rows = client.query_raw("SELECT n FROM gated", std::iter::empty::<&i32>()).await?;
            futures::pin_mut!(rows);
            assert_eq!(rows.next().await.unwrap()?.get::<_, i64>(0), 1);
            client.cancel_token().cancel_query(NoTls).await?;
            drain_to_error(&mut rows, Some(tokio_postgres::error::SqlState::QUERY_CANCELED), "extended query was never canceled").await;
            assert_eq!(client.query_one("SELECT 42::BIGINT", &[]).await?.get::<_, i64>(0), 42);
            let tx = client.transaction().await?;
            let stmt = tx.prepare("SELECT n FROM gated").await?;
            let portal = tx.bind(&stmt, &[]).await?;
            assert_eq!(tx.query_portal(&portal, 1).await?[0].get::<_, i64>(0), 1);
            release.notify_one();
            assert_eq!(tx.query_portal(&portal, 0).await?.len(), 2);
            assert!(tx.query_portal(&portal, 1).await?.is_empty());
            tx.commit().await?;
            let failed = client.query_raw("SELECT n FROM gated_error", std::iter::empty::<&i32>()).await?;
            futures::pin_mut!(failed);
            assert_eq!(failed.next().await.unwrap()?.get::<_, i64>(0), 1);
            release.notify_one();
            drain_to_error(&mut failed, None, "late input failure never reached the client").await;
            client.batch_execute("DECLARE exhausted CURSOR FOR SELECT 42::BIGINT AS n").await?;
            assert_eq!(client.query("FETCH ALL FROM exhausted", &[]).await?.len(), 1);
            for _ in 0..2 {
                let empty = client.simple_query("FETCH ALL FROM exhausted").await?;
                assert!(empty.iter().any(
                    |message| matches!(message, tokio_postgres::SimpleQueryMessage::RowDescription(columns) if columns.len() == 1 && columns[0].name() == "n")
                ));
            }
            client.batch_execute("CLOSE exhausted").await?;
            // The simple protocol must also flush and remain cancellable.
            let rows = client.simple_query_raw("SELECT n FROM gated").await?;
            futures::pin_mut!(rows);
            while !matches!(rows.next().await.unwrap()?, tokio_postgres::SimpleQueryMessage::Row(_)) {}
            client.cancel_token().cancel_query(NoTls).await?;
            drain_to_error(&mut rows, None, "simple query was never canceled").await;
            assert_eq!(client.query_one("SELECT 42::BIGINT", &[]).await?.get::<_, i64>(0), 42);
            anyhow::Ok(())
        })
        .await??;
        // Driver cleanup must invalidate both SQL PREPARE and protocol Parse
        // statements, whether sent through the simple or extended protocol.
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let peer_statement = other.prepare("SELECT 77::BIGINT").await?;
            for extended in [false, true] {
                let statement = client.prepare("SELECT 42::BIGINT").await?;
                client.batch_execute("PREPARE sql_statement AS SELECT 42::BIGINT").await?;
                client.simple_query("EXECUTE sql_statement").await?;
                if extended {
                    client.execute("DEALLOCATE ALL", &[]).await?;
                } else {
                    client.batch_execute("DEALLOCATE ALL").await?;
                }
                let error = client.query(&statement, &[]).await.unwrap_err();
                assert_eq!(error.code(), Some(&tokio_postgres::error::SqlState::INVALID_SQL_STATEMENT_NAME));
                let error = client.simple_query("EXECUTE sql_statement").await.unwrap_err();
                assert!(error.as_db_error().unwrap().message().contains("does not exist"));
                assert_eq!(other.query_one(&peer_statement, &[]).await?.get::<_, i64>(0), 77);
            }
            client.batch_execute("DEALLOCATE ALL; DEALLOCATE PREPARE ALL").await?;
            assert_eq!(client.query_one("SELECT 42::BIGINT", &[]).await?.get::<_, i64>(0), 42);
            anyhow::Ok(())
        })
        .await??;
        drop(tasks);
        Ok(())
    }
}
