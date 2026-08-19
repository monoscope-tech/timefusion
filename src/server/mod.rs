//! Shared server-bootstrap wiring used by both `main.rs` and the E2E test
//! harness. Keeping this in one place guarantees the test path matches prod
//! — the whole point of the E2E suite is to catch the class of bug that
//! "CI doesn't reproduce because the harness skips half the wiring".
//!
//! Returns the fully wired pieces; the caller decides what to do with them
//! (serve pgwire, expose for assertions, etc.).

pub mod pg_compat;

use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use datafusion::execution::context::SessionContext;
use tokio_util::sync::CancellationToken;

use crate::{
    config::AppConfig,
    database::Database,
    write::{BufferedWriteLayer, DeltaWatermark},
};

/// Everything a serving process needs after bootstrap is done.
pub struct Bootstrapped {
    pub db: Arc<Database>,
    pub buffered_layer: Arc<BufferedWriteLayer>,
    /// The SessionContext used by the pgwire handlers — UDFs and table
    /// providers are already registered.
    pub session_ctx: Arc<SessionContext>,
    /// Cancel to signal shutdown to anything we spawned.
    pub shutdown: CancellationToken,
}

/// Raise the open-file soft limit to the hard limit.
///
/// Docker hands a process the daemon's default soft limit — 1024 in prod, against
/// a 524288 hard limit — and a database that memory-maps parquet, holds a tantivy
/// index per partition and serves pgwire exhausts that in seconds. On 2026-08-12
/// prod spent its first 5.5 minutes after boot emitting 1.6M `Too many open files`
/// lines and REFUSING pgwire connections (`Error accept socket`), which also
/// pushed every other line out of the log ring buffer.
///
/// This belongs in the process, not the service definition: CapRover rewrites the
/// service config on every deploy, so a ulimit set out of band does not survive.
/// Best-effort by design — a platform that refuses the raise is not a reason to
/// fail boot, and the log line is enough to diagnose it.
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

/// Build the BufferedWriteLayer + Database wiring exactly as `main.rs` does,
/// minus listener binding / signal handling / telemetry init (the caller
/// owns those — they differ between prod and test).
///
/// Side effects: spawns the flush + eviction background tasks, performs
/// WAL recovery, and starts maintenance schedulers. The returned
/// `CancellationToken` can be triggered to ask spawned work to wind down.
pub async fn bootstrap(cfg: Arc<AppConfig>) -> Result<Bootstrapped> {
    crate::support::init_from_env();
    raise_file_limit();

    let t_db = std::time::Instant::now();
    let mut db = Database::with_config(Arc::clone(&cfg)).await?;
    tracing::info!("bootstrap.phase=database_init elapsed_ms={}", t_db.elapsed().as_millis());

    let delta_write_callback = delta_write_callback(&db);

    let mut session_context = Arc::new(db.clone()).create_session_context();
    db.setup_session_udfs(&mut session_context)?;
    let registry: Arc<crate::read::functions::FnRegistry> = Arc::new(session_context.state());

    // Pre-init WAL GC (gated + drained-flag consumption inside the helper —
    // same call as main.rs so the e2e path mirrors prod).
    crate::write::wal::boot_wal_gc(&cfg.core.wal_dir());

    let t_layer = std::time::Instant::now();
    let mut layer = BufferedWriteLayer::with_config(Arc::clone(&cfg), registry)?
        .with_delta_writer(delta_write_callback)
        .with_coalesced_delta_writer(coalesced_delta_write_callback(&db));
    tracing::info!("bootstrap.phase=buffered_write_layer_init elapsed_ms={}", t_layer.elapsed().as_millis());

    // Optional tantivy sidecar (mirrors main.rs). Disabled when no indexed
    // tables OR the bucket is unset (tests with foyer-only setups).
    let bucket = cfg.aws.aws_s3_bucket.clone().unwrap_or_default();
    if !cfg.tantivy.indexed_tables().is_empty() && !bucket.is_empty() {
        let storage_uri = format!("s3://{}/{}/tantivy", bucket, cfg.core.timefusion_table_prefix);
        let obj_store = db.create_object_store(&storage_uri, &cfg.aws.build_storage_options(None)).await?;
        let svc = Arc::new(crate::tantivy::search::TantivyIndexService::new(obj_store.clone(), Arc::new(cfg.tantivy.clone())));
        layer = layer.with_tantivy_indexer(svc.clone().callback());
        let search = Arc::new(crate::tantivy::search::TantivySearchService::new(obj_store, cfg.core.timefusion_data_dir.clone()));
        db = db.with_tantivy_search(search).with_tantivy_indexer(svc);
    }

    let buffered_layer = Arc::new(layer);

    // Mirror main.rs: clean snapshot → skip the Delta cursor scan; dirty/missing
    // snapshot → derive cursors from Delta so WAL replay doesn't re-inject
    // entries Delta already has. Keeping this in the test-shared bootstrap
    // means e2e startup-time assertions exercise the same path as prod.
    // Per-phase timing is emitted at INFO so cold-start regressions surface
    // without needing trace-level enabled.
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
        let advanced = db.derive_wal_cursors_from_delta(wal_ref).await.unwrap_or(0);
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
    // Non-blocking: snapshot load + footer warm-up off the first query's path.
    db.preload_tables();
    // Non-blocking: index live files no manifest entry covers (config-gated),
    // and warm the local index cache with recent blobs (config-gated).
    db.spawn_tantivy_backfill();
    db.spawn_tantivy_prefetch();
    db.spawn_deferred_tantivy_reindex(Arc::clone(&buffered_layer));

    Ok(Bootstrapped { db, buffered_layer, session_ctx: Arc::new(session_context), shutdown: CancellationToken::new() })
}

/// The C3 coalescing flush writer: hands the whole tick's groups to
/// The per-bucket Delta write a flush hands its rows to. Shared by `bootstrap`,
/// `main` AND the test harnesses, because a layer built WITHOUT it does not
/// fail — `flush_bucket` logs "No delta write callback configured, skipping
/// flush" and drains the bucket anyway. A test layer missing this therefore
/// silently discards every flushed row while `is_empty()` reports success
/// (2026-08-02: `buffer_consistency_test` asserted exactly that and passed for
/// as long as its DML never routed through a flush).
pub fn delta_write_callback(db: &crate::database::Database) -> crate::write::DeltaWriteCallback {
    let db = db.clone();
    Arc::new(move |project_id: String, table_name: String, batches: Vec<RecordBatch>, wal_watermark: DeltaWatermark| {
        let db = db.clone();
        // insert_records_batch warms the just-flushed files itself
        // (watermark-gated) — warming here too would double the GETs.
        Box::pin(async move { db.insert_records_batch(&project_id, &table_name, batches, true, Some(&wal_watermark)).await })
    })
}

/// `insert_records_batches_coalesced`, which emits one Delta commit per
/// PHYSICAL table. Shared by `bootstrap` and `main` so the e2e path and prod
/// wire the identical callback. Only used when
/// `TIMEFUSION_FLUSH_COALESCE_COMMITS` is on.
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
            // `insert_records_batches_coalesced` warms the flushed files itself
            // (as insert_records_batch does) — no warm here, or every flush
            // would issue the warm GETs twice.
            // Mark on ANY settled commit, not just one with a non-empty added
            // list: the flag means "this (project, table) has Delta files", which
            // is true the moment the commit lands. Attribution can legitimately
            // return an empty list for a project that did flush (e.g. a
            // concurrent snapshot re-materialize makes the pre/post diff miss),
            // and gating on it would leave `delta_scan_can_be_skipped` true —
            // queries would skip Delta and read only MemBuffer. Tantivy indexing
            // stays driven by the actual file list inside the insert path.
            // Identical to the per-project callback in `main.rs`.
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
            ClientInfo, ClientPortalStore, ErrorHandler, PgWireServerHandlers,
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
use sha2::{Digest, Sha256};
use tracing::{Instrument, error, field::Empty, info, instrument, warn};

use crate::{
    read::plan_cache::PlanCacheHook,
    server::pg_compat::{
        DEFAULT_MAX_STATEMENT_SECS, PgCompatibilityHook, TimeFusionServerParameterProvider, effective_statement_timeout, statement_timeout_error,
    },
};

/// Auth configuration for PgWire server
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub username: String,
    pub password: Option<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self { username: "postgres".into(), password: None }
    }
}

impl AuthConfig {
    /// Construct from `CoreConfig`, requiring an explicit password unless
    /// `TIMEFUSION_ALLOW_INSECURE_AUTH=true` is set. We hard-fail the
    /// startup path rather than silently accept an empty password — the
    /// PG wire protocol's cleartext handler treats `None` as "accept any",
    /// which is an open ingest endpoint when bound to 0.0.0.0.
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

/// AuthSource that validates against configured credentials
#[derive(Debug, Clone)]
pub struct ConfigAuthSource {
    config: AuthConfig,
}

impl ConfigAuthSource {
    pub fn new(config: AuthConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl AuthSource for ConfigAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let username = login.user().unwrap_or("");
        (username == self.config.username).then(|| Password::new(None, self.config.password.clone().unwrap_or_default().into_bytes())).ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new("FATAL".into(), "28P01".into(), format!("password authentication failed for user \"{username}\""))))
        })
    }
}

/// Custom handler factory that creates handlers with logging and auth
pub struct LoggingHandlerFactory {
    session_context: Arc<SessionContext>,
    auth_config: AuthConfig,
    plan_cache: Arc<PlanCacheHook>,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>,
    db: Option<Arc<Database>>,
    max_statement_secs: u64,
}

impl LoggingHandlerFactory {
    pub fn new(session_context: Arc<SessionContext>, auth_config: AuthConfig) -> Self {
        let plan_cache = Arc::new(PlanCacheHook::default());
        crate::read::plan_cache::set_global(plan_cache.clone());
        Self { session_context, auth_config, plan_cache, scan_metrics: None, db: None, max_statement_secs: DEFAULT_MAX_STATEMENT_SECS }
    }

    pub fn with_scan_metrics(mut self, m: Arc<crate::database::ScanMetrics>) -> Self {
        self.scan_metrics = Some(m);
        self
    }

    pub fn with_max_statement_secs(mut self, max_statement_secs: u64) -> Self {
        self.max_statement_secs = max_statement_secs;
        self
    }

    /// Enables the on-demand `OPTIMIZE <table> WHERE date = '...'` admin command
    /// (intercepted in the simple-query path). Unset in test servers, which
    /// don't need it.
    pub fn with_database(mut self, db: Arc<Database>) -> Self {
        self.db = Some(db);
        self
    }

    /// Hook list passed to every `DfSessionService` instance the factory
    /// produces. Sharing the single `plan_cache` Arc is what makes the LRU
    /// global rather than per-connection.
    fn hooks(&self) -> Vec<Arc<dyn QueryHook>> {
        vec![
            Arc::new(CursorStatementHook),
            Arc::new(PgCompatibilityHook::new(self.auth_config.username.clone(), self.max_statement_secs)),
            self.plan_cache.clone() as Arc<dyn QueryHook>,
            Arc::new(SetShowHook),
            Arc::new(TransactionStatementHook),
        ]
    }

    pub fn plan_cache(&self) -> Arc<PlanCacheHook> {
        self.plan_cache.clone()
    }
}

impl PgWireServerHandlers for LoggingHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        let h = LoggingSimpleQueryHandler::new_with_hooks(self.session_context.clone(), self.hooks()).with_max_statement_secs(self.max_statement_secs);
        let h = self.scan_metrics.iter().fold(h, |h, m| h.with_scan_metrics(m.clone()));
        Arc::new(self.db.iter().fold(h, |h, db| h.with_database(db.clone())))
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        let h = LoggingExtendedQueryHandler::new_with_hooks(self.session_context.clone(), self.hooks()).with_max_statement_secs(self.max_statement_secs);
        Arc::new(self.scan_metrics.iter().fold(h, |h, m| h.with_scan_metrics(m.clone())))
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(CleartextPasswordAuthStartupHandler::new(ConfigAuthSource::new(self.auth_config.clone()), TimeFusionServerParameterProvider::default()))
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
        // `ApiError` wraps an internal failure (DataFusion error, including
        // `Internal error` assertions that indicate a bug) — surface at error so
        // it isn't buried. Everything else (client `UserError`, `IoError` /
        // connection resets, protocol errors) is expected or infra noise — info.
        match error {
            PgWireError::ApiError(_) => error!("PgWire internal error: {}", error),
            _ => info!("PgWire error: {}", error),
        }
    }
}

/// Concurrent-giant-statement gate. A mega-statement (monoscope's multi-MB
/// INSERTs / unnest-array enrichment UPDATEs) materializes its literals and
/// bound parameters as ScalarValue arrays during plan + bind — tens to
/// hundreds of MB of transient heap per statement, bounded only by connection
/// concurrency. The 08:13Z 2026-08-03 OOM's pre-kill heap dumps were dominated
/// by exactly this (`ScalarValue::iter_to_array`/`make_run_array` under
/// pgwire). Two permits: one giant can always run while another queues, and
/// worst-case transient parse heap is 2x one statement instead of Nx.
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

/// Writes are exempt from the statement timeout.
///
/// `run_with_statement_timeout` enforces by DROPPING the in-flight future, and
/// the DML path runs its WAL append and Delta commit *inside* that future — so
/// cancelling a slow bulk INSERT or a MOR UPDATE reports failure to the client
/// for a write that is already partly durable, and it reappears on the next WAL
/// replay. PostgreSQL's `statement_timeout` aborts a statement transactionally;
/// dropping a future cannot, so the deadline only covers read-only statements.
/// `classify_query` errs toward matching DML, which errs toward no timeout.
fn statement_timeout_applies(query: &str) -> bool {
    let (kind, _) = classify_query(query);
    !matches!(kind, "DML" | "DDL")
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

fn with_response_deadline(response: Response, deadline: Option<tokio::time::Instant>) -> Response {
    match (response, deadline) {
        (Response::Query(QueryResponse { command_tag, row_schema, data_rows, .. }), Some(deadline)) => {
            let data_rows = stream::unfold(Some(data_rows), move |rows| async move {
                let mut rows = rows?;
                match tokio::time::timeout_at(deadline, rows.next()).await {
                    Ok(Some(row)) => Some((row, Some(rows))),
                    Ok(None) => None,
                    Err(_) => Some((Err(statement_timeout_error()), None)),
                }
            });
            let mut response = QueryResponse::new(row_schema, data_rows);
            response.set_command_tag(&command_tag);
            Response::Query(response)
        }
        (response, _) => response,
    }
}

fn with_response_deadlines(responses: Vec<Response>, deadline: Option<tokio::time::Instant>) -> Vec<Response> {
    responses.into_iter().map(|response| with_response_deadline(response, deadline)).collect()
}

/// Simple query handler with tracing
pub struct LoggingSimpleQueryHandler {
    inner: DfSessionService,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>,
    db: Option<Arc<Database>>,
    max_statement_secs: u64,
}

impl LoggingSimpleQueryHandler {
    pub fn new_with_hooks(session_context: Arc<SessionContext>, hooks: Vec<Arc<dyn QueryHook>>) -> Self {
        Self { inner: DfSessionService::new_with_hooks(session_context, hooks), scan_metrics: None, db: None, max_statement_secs: DEFAULT_MAX_STATEMENT_SECS }
    }

    pub fn with_max_statement_secs(mut self, max_statement_secs: u64) -> Self {
        self.max_statement_secs = max_statement_secs;
        self
    }

    pub fn with_scan_metrics(mut self, m: Arc<crate::database::ScanMetrics>) -> Self {
        self.scan_metrics = Some(m);
        self
    }

    pub fn with_database(mut self, db: Arc<Database>) -> Self {
        self.db = Some(db);
        self
    }

    /// Execute an intercepted `OPTIMIZE <table> WHERE date = '...'`.
    async fn run_optimize(&self, cmd: OptimizeCmd) -> PgWireResult<Vec<Response>> {
        let db = require_available(self.db.as_ref(), "OPTIMIZE")?;
        let table_ref = db.get_or_create_unified_table(&cmd.table).await.map_err(|e| admin_err(format!("OPTIMIZE: open table '{}': {e}", cmd.table)))?;
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
    /// Ops pre-restart hook: run it right before a planned restart/deploy so
    /// the stop grace never bounds the shutdown flush and the boot's WAL
    /// replay is near-empty. Errors when any bucket fails so callers can
    /// gate on it.
    async fn run_flush(&self) -> PgWireResult<Vec<Response>> {
        let layer = require_available(self.db.as_ref().and_then(|d| d.buffered_layer()), "FLUSH")?;
        // Misuse guard: FLUSH commits the open window per table (tiny parquet
        // files + tantivy builds), so a looping client mints file-count
        // explosion and contends flush_lock with routine flushes. Operator
        // cadence is "once before a deploy" — enforce a floor between runs.
        // Frozen-clock (test) harnesses are exempt: their cadence is
        // script-driven and the frozen epoch resets between environments.
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
        // Wake Walrus's safe reclaim worker and wait for its completion while
        // this instance is still serving, otherwise the replacement pays to
        // scan the outgoing instance's consumed WAL.
        // Frozen-clock test harnesses skip this operational handoff delay.
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

    /// Read recent Delta commit metadata without requiring direct object-store
    /// credentials on the operator's machine.
    async fn run_delta_history(&self, cmd: DeltaHistoryCmd) -> PgWireResult<Vec<Response>> {
        use datafusion_postgres::pgwire::api::Type;

        let db = require_available(self.db.as_ref(), "DELTA HISTORY")?;
        let table_ref = db.get_or_create_unified_table(&cmd.table).await.map_err(|e| admin_err(format!("DELTA HISTORY: open table '{}': {e}", cmd.table)))?;
        let table = table_ref.read().await;
        let commits: Vec<_> = table.history(Some(cmd.limit)).await.map_err(|e| admin_err(format!("DELTA HISTORY '{}': {e}", cmd.table)))?.collect();
        drop(table);

        let fields = Arc::new(
            ["version", "timestamp_utc", "operation", "read_version", "is_blind_append", "operation_parameters", "commit_info"]
                .into_iter()
                .map(|name| FieldInfo::new(name.to_string(), None, None, Type::VARCHAR, FieldFormat::Text))
                .collect::<Vec<_>>(),
        );
        let rows = commits.into_iter().map({
            let fields = fields.clone();
            move |commit| {
                let mut encoder = DataRowEncoder::new(fields.clone());
                let timestamp = commit.timestamp.and_then(chrono::DateTime::from_timestamp_millis).map(|v| v.to_rfc3339()).unwrap_or_default();
                let read_version = commit.read_version.map(|v| v.to_string()).unwrap_or_default();
                let version = commit.read_version.map(|v| (v + 1).to_string()).unwrap_or_default();
                let operation = commit.operation.clone().unwrap_or_default();
                let blind_append = commit.is_blind_append.map(|v| v.to_string()).unwrap_or_default();
                let parameters = serde_json::to_string(&commit.operation_parameters).unwrap_or_default();
                let info = serde_json::to_string(&commit).unwrap_or_default();
                for value in [&version, &timestamp, &operation, &read_version, &blind_append, &parameters, &info] {
                    encoder.encode_field(value)?;
                }
                Ok(encoder.take_row())
            }
        });
        Ok(vec![Response::Query(QueryResponse::new(fields, stream::iter(rows)))])
    }

    /// Return every raw action in one Delta commit. This is an audit primitive:
    /// it reads the transaction log only and never constructs a transaction.
    async fn run_delta_actions(&self, cmd: DeltaActionsCmd) -> PgWireResult<Vec<Response>> {
        use datafusion_postgres::pgwire::api::Type;

        let db = require_available(self.db.as_ref(), "DELTA ACTIONS")?;
        let table_ref = db.get_or_create_unified_table(&cmd.table).await.map_err(|e| admin_err(format!("DELTA ACTIONS: open table '{}': {e}", cmd.table)))?;
        let log_store = table_ref.read().await.log_store();
        let bytes = log_store
            .read_commit_entry(cmd.version)
            .await
            .map_err(|e| admin_err(format!("DELTA ACTIONS '{}' VERSION {}: {e}", cmd.table, cmd.version)))?
            .ok_or_else(|| admin_err(format!("DELTA ACTIONS '{}' VERSION {}: commit not found", cmd.table, cmd.version)))?;
        let actions = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<deltalake::kernel::Action>(line).map_err(|e| admin_err(format!("decode Delta action: {e}"))))
            .collect::<PgWireResult<Vec<_>>>()?;

        let fields = Arc::new(
            ["version", "action", "path", "size_bytes", "action_json"]
                .into_iter()
                .map(|name| FieldInfo::new(name.to_string(), None, None, Type::VARCHAR, FieldFormat::Text))
                .collect::<Vec<_>>(),
        );
        let rows = actions.into_iter().map({
            let fields = fields.clone();
            move |action| {
                let (kind, path, size) = match &action {
                    deltalake::kernel::Action::Add(add) => ("add", add.path.as_str(), add.size.to_string()),
                    deltalake::kernel::Action::Remove(remove) => ("remove", remove.path.as_str(), remove.size.map(|v| v.to_string()).unwrap_or_default()),
                    deltalake::kernel::Action::CommitInfo(_) => ("commitInfo", "", String::new()),
                    _ => ("other", "", String::new()),
                };
                let version = cmd.version.to_string();
                let json = serde_json::to_string(&action).map_err(|e| admin_err(format!("encode Delta action: {e}")))?;
                let mut encoder = DataRowEncoder::new(fields.clone());
                for value in [&version, kind, path, &size, &json] {
                    encoder.encode_field(&value)?;
                }
                Ok(encoder.take_row())
            }
        });
        Ok(vec![Response::Query(QueryResponse::new(fields, stream::iter(rows)))])
    }

    /// Reconstruct the full pre-commit Add actions for files removed by
    /// `version`. This is read-only and fails unless every removal has a source.
    async fn run_delta_recovery_audit(&self, cmd: DeltaRecoveryAuditCmd) -> PgWireResult<Vec<Response>> {
        use datafusion_postgres::pgwire::api::Type;

        let db = require_available(self.db.as_ref(), "DELTA RECOVERY AUDIT")?;
        let table_ref =
            db.get_or_create_unified_table(&cmd.table).await.map_err(|e| admin_err(format!("DELTA RECOVERY AUDIT: open table '{}': {e}", cmd.table)))?;
        let mut before = table_ref.read().await.clone();
        let bytes = before
            .log_store()
            .read_commit_entry(cmd.version)
            .await
            .map_err(|e| admin_err(format!("DELTA RECOVERY AUDIT '{}' VERSION {}: {e}", cmd.table, cmd.version)))?
            .ok_or_else(|| admin_err(format!("DELTA RECOVERY AUDIT '{}' VERSION {}: commit not found", cmd.table, cmd.version)))?;
        let removed = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<deltalake::kernel::Action>(line).map_err(|e| admin_err(format!("decode Delta action: {e}"))))
            .collect::<PgWireResult<Vec<_>>>()?
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
                // The replacement Arrow-table API is intended for analytics
                // and does not round-trip a complete Add action. Recovery must
                // preserve raw stats/tags byte-for-byte, which this delta-rs
                // compatibility method explicitly guarantees.
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

        let fields = Arc::new(
            ["removed_by_version", "path", "size_bytes", "source_add_json"]
                .into_iter()
                .map(|name| FieldInfo::new(name.to_string(), None, None, Type::VARCHAR, FieldFormat::Text))
                .collect::<Vec<_>>(),
        );
        let rows = sources.into_iter().map({
            let fields = fields.clone();
            move |add| {
                let version = cmd.version.to_string();
                let size = add.size.to_string();
                let json = serde_json::to_string(&deltalake::kernel::Action::Add(add.clone())).map_err(|e| admin_err(format!("encode source Add: {e}")))?;
                let mut encoder = DataRowEncoder::new(fields.clone());
                for value in [&version, &add.path, &size, &json] {
                    encoder.encode_field(value)?;
                }
                Ok(encoder.take_row())
            }
        });
        Ok(vec![Response::Query(QueryResponse::new(fields, stream::iter(rows)))])
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

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DeltaActionsCmd {
    pub table: String,
    pub version: u64,
}

pub(crate) fn parse_delta_actions(query: &str) -> Result<Option<DeltaActionsCmd>, String> {
    let Some(rest) = strip_command(query, "delta") else { return Ok(None) };
    let Some(rest) = strip_keyword(rest, "actions", char::is_whitespace) else { return Ok(None) };
    let mut parts = rest.split_whitespace();
    let table = parts.next().ok_or("DELTA ACTIONS requires: DELTA ACTIONS <table> VERSION <n>")?;
    let keyword = parts.next().ok_or("DELTA ACTIONS requires a VERSION")?;
    let value = parts.next().ok_or("DELTA ACTIONS requires a numeric VERSION")?;
    if !keyword.eq_ignore_ascii_case("version") || parts.next().is_some() {
        return Err("expected: DELTA ACTIONS <table> VERSION <n>".to_string());
    }
    let version = value.parse::<u64>().map_err(|_| format!("invalid Delta version '{value}'"))?;
    Ok(Some(DeltaActionsCmd { table: table.to_string(), version }))
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DeltaRecoveryAuditCmd {
    pub table: String,
    pub version: u64,
}

pub(crate) fn parse_delta_recovery_audit(query: &str) -> Result<Option<DeltaRecoveryAuditCmd>, String> {
    let Some(rest) = strip_command(query, "delta") else { return Ok(None) };
    let Some(rest) = strip_keyword(rest, "recovery", char::is_whitespace) else { return Ok(None) };
    let Some(rest) = strip_keyword(rest.trim(), "audit", char::is_whitespace) else {
        return Err("DELTA RECOVERY supports only: DELTA RECOVERY AUDIT <table> VERSION <n>".to_string());
    };
    let mut parts = rest.split_whitespace();
    let table = parts.next().ok_or("DELTA RECOVERY AUDIT requires a table")?;
    let keyword = parts.next().ok_or("DELTA RECOVERY AUDIT requires a VERSION")?;
    let value = parts.next().ok_or("DELTA RECOVERY AUDIT requires a numeric VERSION")?;
    if !keyword.eq_ignore_ascii_case("version") || parts.next().is_some() {
        return Err("expected: DELTA RECOVERY AUDIT <table> VERSION <n>".to_string());
    }
    let version = value.parse::<u64>().map_err(|_| format!("invalid Delta version '{value}'"))?;
    Ok(Some(DeltaRecoveryAuditCmd { table: table.to_string(), version }))
}

/// An intercepted `OPTIMIZE <table> WHERE date = 'YYYY-MM-DD'` admin command.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OptimizeCmd {
    pub table: String,
    pub date: chrono::NaiveDate,
    /// Restrict the compaction to one tenant's partition. A whole-date
    /// optimize spans every project's files for that date — tens of GB on a
    /// busy day, which doesn't fit in-process next to serving load
    /// (2026-07-27: two OOMs). One (project, date) partition is a few GB.
    pub project_id: Option<String>,
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
/// - `Err(msg)`: it *is* OPTIMIZE but malformed (no table, missing/non-`date`
///   filter, bad date). A bare `OPTIMIZE <table>` is rejected on purpose — an
///   unbounded in-process compaction can OOM the instance — and surfaced as a
///   clear error rather than a confusing DataFusion parser error.
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
    // `WHERE date = '...'` optionally AND-ed (either order) with
    // `project_id = '...'`. Values are simple quoted literals, so splitting on
    // a top-level ` AND ` needs no nesting awareness.
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
///   rejected on purpose — name the table explicitly. Unlike OPTIMIZE, VACUUM is
///   table-wide (all partitions) and takes no date filter; the optional
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

/// Rewrites Postgres synonyms that DataFusion's SQL parser doesn't accept.
///
/// `ABORT [ WORK | TRANSACTION ]` is a Postgres alias for `ROLLBACK`. Hasql's
/// connection pool emits `ABORT` defensively on session acquisition to clear
/// any leftover transaction state; without this rewrite, every Hasql client
/// (e.g. monoscope) sees its first statement on each connection fail with
/// `sql parser error: Expected: an SQL statement, found: ABORT`, which then
/// poisons the whole session.
fn rewrite_pg_synonyms(query: &str) -> Cow<'_, str> {
    let query = strip_keyword(query.trim_start(), "ABORT", |c| c.is_whitespace() || c == ';')
        .map_or(Cow::Borrowed(query), |rest| Cow::Owned(format!("ROLLBACK{rest}")));
    rewrite_row_to_json_record(&query).map_or(query, Cow::Owned)
}

/// `row_to_json(t)` over a derived-table alias, which DataFusion rejects while
/// planning the SQL — before any analyzer rule could see it. pgAdmin's dashboard
/// polls that shape every 5s.
///
/// This is an AST rewrite, not a text substitution: the statement is parsed,
/// visited, and unparsed only if something actually changed. Anything that fails
/// to parse, or that the visitor declines to touch, is returned unchanged and
/// reaches DataFusion byte-for-byte as before — a malformed or unusual statement
/// can never be corrupted into a different valid statement by this path.
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

/// (keyword, space-padded keyword, `query.type`, operation). First match wins,
/// so order is significant.
const QUERY_KINDS: [(&str, &str, &str, &str); 7] = [
    ("select", " select ", "SELECT", "SELECT"),
    ("update", " update ", "DML", "UPDATE"),
    ("delete", " delete ", "DML", "DELETE"),
    ("insert", " insert ", "DML", "INSERT"),
    ("create", " create ", "DDL", "CREATE"),
    ("drop", " drop ", "DDL", "DROP"),
    ("alter", " alter ", "DDL", "ALTER"),
];

fn classify_query(query: &str) -> (&'static str, &'static str) {
    let q = query.trim().to_lowercase();
    QUERY_KINDS
        .iter()
        .find(|(kw, padded, ..)| q.starts_with(kw) || q.contains(padded))
        .map_or(("OTHER", "UNKNOWN"), |&(.., query_type, operation)| (query_type, operation))
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

fn query_fingerprint(query: &str) -> String {
    format!("{:x}", Sha256::digest(normalized_query(query).as_bytes()))
}

/// Classify `query` and stamp the standard query/db tracing fields onto `span`.
fn record_query_span(span: &tracing::Span, query: &str) {
    let (query_type, operation) = classify_query(query);
    span.record("query.type", query_type);
    span.record("query.operation", operation);
    span.record("db.operation", operation);
    span.record("query.text", query_template(query));
}

/// Emit one bounded event for statements slow enough to affect the tail. Table
/// and project dimensions are extracted only for diagnosis; raw SQL is never
/// included in this event.
fn record_statement_latency(metrics: Option<&crate::database::ScanMetrics>, query: &str, protocol: &'static str, duration_us: u64, success: bool) {
    if let Some(metrics) = metrics {
        metrics.record_pgwire_query(duration_us);
    }
    const SLOW_QUERY_US: u64 = 1_000_000;
    if duration_us < SLOW_QUERY_US {
        return;
    }

    let (_, operation) = classify_query(query);
    let (tables, project_id) = query_dimensions(query);
    info!(
        event = "pgwire.slow_statement",
        query.class = operation,
        query.fingerprint = %query_fingerprint(query),
        query.template = %query_template(query),
        query.tables = %tables,
        project.id = %project_id,
        protocol,
        duration_us,
        success,
        "slow PostgreSQL statement"
    );
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

        // Admin commands, caught before DataFusion (whose parser rejects all
        // OPTIMIZE/VACUUM maintenance plus FLUSH and HANDOFF durability hooks.
        if let Some(cmd) = parse_optimize(query).map_err(admin_err)? {
            return self.run_optimize(cmd).await;
        }
        if let Some(cmd) = parse_vacuum(query).map_err(admin_err)? {
            return self.run_vacuum(cmd).await;
        }
        if let Some(cmd) = parse_delta_recovery_audit(query).map_err(admin_err)? {
            return self.run_delta_recovery_audit(cmd).await;
        }
        if let Some(cmd) = parse_delta_actions(query).map_err(admin_err)? {
            return self.run_delta_actions(cmd).await;
        }
        if let Some(cmd) = parse_delta_history(query).map_err(admin_err)? {
            return self.run_delta_history(cmd).await;
        }
        if parse_flush(query) {
            return self.run_flush().await;
        }
        if parse_handoff(query) {
            return self.run_handoff().await;
        }

        let span = tracing::Span::current();
        record_query_span(&span, query);

        let _giant = giant_stmt_permit(query.len()).await;
        let execute_span = tracing::trace_span!(parent: &span, "datafusion.execute");
        let t0 = std::time::Instant::now();
        let timeout = effective_statement_timeout(client_statement_timeout(client), self.max_statement_secs).filter(|_| statement_timeout_applies(query));
        let result =
            run_with_statement_timeout(timeout, <DfSessionService as SimpleQueryHandler>::do_query(&self.inner, client, query).instrument(execute_span))
                .await
                .map(|(responses, deadline)| with_response_deadlines(responses, deadline));
        record_statement_latency(self.scan_metrics.as_deref(), query, "simple", t0.elapsed().as_micros() as u64, result.is_ok());
        log_statement_failure("simple", &result);
        result
    }
}

/// Logs a failing statement from INSIDE its query span, so the span's
/// `query.text` lands on the same line as the error. `LoggingErrorHandler` runs
/// outside the span and can only report the message — which is why a pgAdmin
/// planning failure showed up in prod as a bare "Invalid function" with no way
/// to tell which statement produced it.
fn log_statement_failure(protocol: &str, result: &PgWireResult<impl Sized>) {
    if let Err(error) = result {
        warn!(protocol, error = %error, "statement failed");
    }
}

/// Extended query handler with tracing
pub struct LoggingExtendedQueryHandler {
    inner: DfSessionService,
    scan_metrics: Option<Arc<crate::database::ScanMetrics>>,
    max_statement_secs: u64,
    query_parser: Arc<RewritingQueryParser>,
}

impl LoggingExtendedQueryHandler {
    pub fn with_max_statement_secs(mut self, max_statement_secs: u64) -> Self {
        self.max_statement_secs = max_statement_secs;
        self
    }

    pub fn with_scan_metrics(mut self, m: Arc<crate::database::ScanMetrics>) -> Self {
        self.scan_metrics = Some(m);
        self
    }

    pub fn new_with_hooks(session_context: Arc<SessionContext>, hooks: Vec<Arc<dyn QueryHook>>) -> Self {
        let inner = DfSessionService::new_with_hooks(session_context, hooks);
        let query_parser = Arc::new(RewritingQueryParser { inner: ExtendedQueryHandler::query_parser(&inner) });
        Self { inner, scan_metrics: None, max_statement_secs: DEFAULT_MAX_STATEMENT_SECS, query_parser }
    }
}

/// Applies the same statement rewrites to the extended protocol that
/// `rewrite_pg_synonyms` applies to the simple one. pgAdmin's dashboard uses
/// simple queries, but a rewrite that fires on one protocol and not the other
/// would mean identical SQL succeeding or failing depending on how the client
/// sent it.
pub struct RewritingQueryParser {
    inner: Arc<<DfSessionService as ExtendedQueryHandler>::QueryParser>,
}

#[async_trait]
impl datafusion_postgres::pgwire::api::stmt::QueryParser for RewritingQueryParser {
    type Statement = <DfSessionService as ExtendedQueryHandler>::Statement;

    async fn parse_sql<C>(&self, client: &C, sql: &str, types: &[Option<datafusion_postgres::pgwire::api::Type>]) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let rewritten = rewrite_row_to_json_record(sql);
        self.inner.parse_sql(client, rewritten.as_deref().unwrap_or(sql), types).await
    }

    fn get_parameter_types(&self, statement: &Self::Statement) -> PgWireResult<Vec<datafusion_postgres::pgwire::api::Type>> {
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
        let span = tracing::Span::current();
        let query = &portal.statement.statement.0;
        record_query_span(&span, query);

        let _giant = giant_stmt_permit(query.len()).await;
        let execute_span = tracing::trace_span!(parent: &span, "datafusion.execute");
        let t0 = std::time::Instant::now();
        let timeout = effective_statement_timeout(client_statement_timeout(client), self.max_statement_secs).filter(|_| statement_timeout_applies(query));
        let result = run_with_statement_timeout(
            timeout,
            <DfSessionService as ExtendedQueryHandler>::do_query(&self.inner, client, portal, max_rows).instrument(execute_span),
        )
        .await
        .map(|(response, deadline)| with_response_deadline(response, deadline));
        record_statement_latency(self.scan_metrics.as_deref(), query, "extended", t0.elapsed().as_micros() as u64, result.is_ok());
        log_statement_failure("extended", &result);
        result
    }
}

fn handler_factory(
    session_context: Arc<SessionContext>, auth_config: AuthConfig, scan_metrics: Option<Arc<crate::database::ScanMetrics>>, db: Option<Arc<Database>>,
) -> Arc<LoggingHandlerFactory> {
    let max_statement_secs = db.as_ref().map_or(DEFAULT_MAX_STATEMENT_SECS, |db| db.config().core.timefusion_pgwire_max_statement_secs);
    let factory = LoggingHandlerFactory::new(session_context, auth_config).with_max_statement_secs(max_statement_secs);
    let factory = scan_metrics.into_iter().fold(factory, LoggingHandlerFactory::with_scan_metrics);
    Arc::new(db.into_iter().fold(factory, LoggingHandlerFactory::with_database))
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
        parse_delta_actions, parse_delta_history, parse_delta_recovery_audit, parse_flush, parse_handoff, parse_optimize, parse_vacuum, query_dimensions,
        query_fingerprint, query_template, rewrite_pg_synonyms, with_response_deadline,
    };
    use datafusion_postgres::pgwire::{
        api::results::{QueryResponse, Response},
        error::PgWireError,
        messages::data::DataRow,
    };
    use futures::StreamExt;
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn query_stream_observes_the_server_deadline() {
        let response = Response::Query(QueryResponse::new(
            Arc::new(vec![]),
            futures::stream::once(async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Err::<DataRow, _>(crate::server::pg_compat::statement_timeout_error())
            }),
        ));
        let Response::Query(mut response) = with_response_deadline(response, Some(tokio::time::Instant::now() + Duration::from_millis(1))) else {
            panic!("expected query response");
        };
        assert!(matches!(response.data_rows.next().await, Some(Err(PgWireError::UserError(_)))));
    }

    /// The deadline is enforced by dropping the in-flight future, and the DML
    /// path commits inside it — so a timed-out write is reported as failed while
    /// already partly durable. Reads only.
    #[test]
    fn the_statement_timeout_never_applies_to_a_write() {
        assert!(super::statement_timeout_applies("SELECT count(*) FROM otel_logs_and_spans"));
        assert!(super::statement_timeout_applies("SHOW server_version"));
        assert!(!super::statement_timeout_applies("INSERT INTO otel_logs_and_spans VALUES (1)"));
        assert!(!super::statement_timeout_applies("UPDATE otel_logs_and_spans SET name = 'x' WHERE id = '1'"));
        assert!(!super::statement_timeout_applies("DELETE FROM otel_logs_and_spans WHERE id = '1'"));
    }

    #[test]
    fn optimize_parses_table_and_date() {
        let cmd = parse_optimize("OPTIMIZE otel_logs_and_spans WHERE date = '2026-06-19'").unwrap().unwrap();
        assert_eq!(cmd.table, "otel_logs_and_spans");
        assert_eq!(cmd.date, "2026-06-19".parse().unwrap());
        // Case / spacing / quote / trailing-semicolon tolerance.
        assert_eq!(parse_optimize("optimize t where DATE='2026-01-02';").unwrap().unwrap().date, "2026-01-02".parse().unwrap());
        assert_eq!(parse_optimize("  OPTIMIZE  t  WHERE  date  =  \"2026-01-02\"  ").unwrap().unwrap().table, "t");
    }

    #[test]
    fn optimize_rejects_unbounded_and_malformed() {
        // Bare OPTIMIZE (no date) is rejected — would compact all history in-process.
        assert!(parse_optimize("OPTIMIZE otel_logs_and_spans").is_err());
        assert!(parse_optimize("OPTIMIZE").is_err());
        // project_id alone (no date bound), bad date, unknown column.
        assert!(parse_optimize("OPTIMIZE t WHERE project_id = 'x'").is_err());
        assert!(parse_optimize("OPTIMIZE t WHERE date = 'not-a-date'").is_err());
        assert!(parse_optimize("OPTIMIZE t WHERE date = '2026-01-02' AND name = 'x'").is_err());
    }

    /// Tenant-scoped compaction (2026-07-27: whole-date OPTIMIZE OOM'd twice
    /// in-process; one (project, date) partition is the safe unit).
    #[test]
    fn optimize_accepts_project_and_date_in_either_order() {
        let cmd = parse_optimize("OPTIMIZE t WHERE project_id = 'p-1' AND date = '2026-01-02'").unwrap().unwrap();
        assert_eq!(cmd.project_id.as_deref(), Some("p-1"));
        assert_eq!(cmd.date, "2026-01-02".parse().unwrap());
        let cmd = parse_optimize("optimize t where date='2026-01-02' and PROJECT_ID=\"p-1\"").unwrap().unwrap();
        assert_eq!(cmd.project_id.as_deref(), Some("p-1"));
        assert!(parse_optimize("OPTIMIZE t WHERE date = '2026-01-02'").unwrap().unwrap().project_id.is_none());
    }

    #[test]
    fn non_optimize_queries_fall_through() {
        assert_eq!(parse_optimize("SELECT 1"), Ok(None));
        assert_eq!(parse_optimize("INSERT INTO t VALUES (1)"), Ok(None));
        // Don't false-match an identifier that merely starts with "optimize".
        assert_eq!(parse_optimize("SELECT optimizer FROM t"), Ok(None));
        assert_eq!(parse_optimize("optimizer_stats"), Ok(None));
    }

    #[test]
    fn vacuum_parses_table_and_optional_retention() {
        let cmd = parse_vacuum("VACUUM otel_logs_and_spans").unwrap().unwrap();
        assert_eq!(cmd.table, "otel_logs_and_spans");
        assert_eq!(cmd.retention_hours, None);
        // RETAIN clause, case / plural / trailing-semicolon tolerance.
        assert_eq!(parse_vacuum("vacuum t RETAIN 48 HOURS;").unwrap().unwrap().retention_hours, Some(48));
        assert_eq!(parse_vacuum("  VACUUM  t  retain  1  hour  ").unwrap().unwrap().retention_hours, Some(1));
    }

    #[test]
    fn vacuum_rejects_bare_and_malformed() {
        // Bare VACUUM (no table) is rejected — must name the table.
        assert!(parse_vacuum("VACUUM").is_err());
        // Unknown trailing clause, non-numeric retention.
        assert!(parse_vacuum("VACUUM t WHERE date = '2026-01-01'").is_err());
        assert!(parse_vacuum("VACUUM t RETAIN abc HOURS").is_err());
    }

    #[test]
    fn non_vacuum_queries_fall_through() {
        assert_eq!(parse_vacuum("SELECT 1"), Ok(None));
        // Don't false-match an identifier that merely starts with "vacuum".
        assert_eq!(parse_vacuum("SELECT vacuumed FROM t"), Ok(None));
        assert_eq!(parse_vacuum("vacuum_log"), Ok(None));
    }

    #[test]
    fn flush_parses_bare_only() {
        assert!(parse_flush("FLUSH"));
        assert!(parse_flush("  flush ; "));
        // Anything with arguments or a mere prefix match falls through.
        assert!(!parse_flush("FLUSH t"));
        assert!(!parse_flush("SELECT flushed FROM t"));
        assert!(!parse_flush("flush_log"));
    }

    #[test]
    fn handoff_parses_bare_only() {
        assert!(parse_handoff("HANDOFF"));
        assert!(parse_handoff("  handoff ; "));
        assert!(!parse_handoff("HANDOFF now"));
        assert!(!parse_handoff("SELECT handoff FROM t"));
    }

    #[test]
    fn delta_history_parses_bounded_read_only_command() {
        let cmd = parse_delta_history("DELTA HISTORY otel_logs_and_spans LIMIT 250;").unwrap().unwrap();
        assert_eq!(cmd.table, "otel_logs_and_spans");
        assert_eq!(cmd.limit, 250);
        assert_eq!(parse_delta_history("delta history t").unwrap().unwrap().limit, 100);
        assert!(parse_delta_history("DELTA HISTORY t LIMIT 0").is_err());
        assert!(parse_delta_history("DELTA HISTORY t LIMIT 10001").is_err());
        assert!(parse_delta_history("DELTA RESTORE t").is_err());
        assert_eq!(parse_delta_history("SELECT delta FROM t"), Ok(None));
    }

    #[test]
    fn delta_actions_requires_one_exact_version() {
        let cmd = parse_delta_actions("DELTA ACTIONS otel_logs_and_spans VERSION 462919;").unwrap().unwrap();
        assert_eq!(cmd.table, "otel_logs_and_spans");
        assert_eq!(cmd.version, 462919);
        assert!(parse_delta_actions("DELTA ACTIONS t").is_err());
        assert!(parse_delta_actions("DELTA ACTIONS t VERSION nope").is_err());
        assert_eq!(parse_delta_actions("SELECT 1"), Ok(None));
    }

    #[test]
    fn delta_recovery_audit_is_explicit_and_version_bounded() {
        let cmd = parse_delta_recovery_audit("DELTA RECOVERY AUDIT otel_logs_and_spans VERSION 462921;").unwrap().unwrap();
        assert_eq!(cmd.table, "otel_logs_and_spans");
        assert_eq!(cmd.version, 462921);
        assert!(parse_delta_recovery_audit("DELTA RECOVERY otel_logs_and_spans VERSION 462921").is_err());
        assert!(parse_delta_recovery_audit("DELTA RECOVERY AUDIT t VERSION nope").is_err());
        assert_eq!(parse_delta_recovery_audit("SELECT 1"), Ok(None));
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

    #[test]
    fn abort_rewrites_to_rollback() {
        assert_eq!(rewrite_pg_synonyms("ABORT"), "ROLLBACK");
        assert_eq!(rewrite_pg_synonyms("ABORT;"), "ROLLBACK;");
        assert_eq!(rewrite_pg_synonyms("  abort  "), "ROLLBACK  ");
        assert_eq!(rewrite_pg_synonyms("Abort Work"), "ROLLBACK Work");
        assert_eq!(rewrite_pg_synonyms("ABORT TRANSACTION;"), "ROLLBACK TRANSACTION;");
    }

    #[test]
    fn non_abort_queries_are_borrowed_unchanged() {
        // Cow::Borrowed is the fast path; we just check the content is identical.
        assert_eq!(rewrite_pg_synonyms("SELECT 1"), "SELECT 1");
        assert_eq!(rewrite_pg_synonyms("BEGIN"), "BEGIN");
        assert_eq!(rewrite_pg_synonyms("ROLLBACK"), "ROLLBACK");
        // Don't false-match identifiers/columns that start with ABORT.
        assert_eq!(rewrite_pg_synonyms("SELECT aborted FROM t"), "SELECT aborted FROM t");
        assert_eq!(rewrite_pg_synonyms("ABORTED"), "ABORTED");
    }
}

// ===== pgwire_early_bind =====
// Early-bind responder that occupies `:5432` during the slow startup
// window (WAL replay, Delta-table open, foyer init), returning the
// Postgres SQLSTATE 57P03 "the database system is starting up" error
// to every connection until the real server takes over the listener.
//
// Without this, packets DNAT'd into the container during the multi-minute
// startup get RST'd back as ECONNREFUSED — clients like Hasql / pgjdbc /
// libpq treat 57P03 as transient and back off cleanly, ECONNREFUSED as a
// hard error. Hand the same `TcpListener` to `serve_with_listener` once
// ready: no rebind, no ECONNREFUSED window.

use std::{io, time::Duration};

use datafusion_postgres::pgwire::messages::startup::{GssEncRequest, SslRequest};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::debug;

const SSL_REQUEST_CODE: u32 = SslRequest::BODY_MAGIC_NUMBER as u32;
const GSS_REQUEST_CODE: u32 = GssEncRequest::BODY_MAGIC_NUMBER as u32;
/// Cap on the StartupMessage size we'll drain. Real pg clients send well
/// under 1 KiB; 64 KiB is comfortably above any legitimate payload and
/// bounds the work a malformed/hostile client can force on us.
const MAX_STARTUP_BYTES: u64 = 64 * 1024;
const STARTUP_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Budget for the over-cap fast path: response write only, no startup drain.
const CAP_RESPONSE_TIMEOUT: Duration = Duration::from_secs(1);
/// Hard cap on concurrent early-bind handlers. A thundering-herd reconnect
/// storm during startup could otherwise spawn unbounded tasks, each holding
/// a socket FD for up to STARTUP_READ_TIMEOUT.
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
                    // Over the cap (probable reconnect storm) we still send the canned
                    // 57P03 frame — dropping the socket unanswered would RST, which
                    // Hasql/libpq treat as ECONNREFUSED, the exact failure mode this
                    // responder exists to avoid — but skip the startup drain so the task
                    // is bounded by accept rate × write latency (~ms).
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
        let remaining = match sock.read_u32().await? {
            SSL_REQUEST_CODE | GSS_REQUEST_CODE => {
                sock.write_all(b"N").await?;
                (sock.read_u32().await? as u64).checked_sub(4)
            }
            _ => len.checked_sub(8),
        };
        drain_body(&mut sock, remaining).await?;
    }

    sock.write_all(response).await?;
    let _ = sock.shutdown().await;
    Ok(())
}

async fn drain_body(sock: &mut TcpStream, remaining: Option<u64>) -> io::Result<()> {
    let n = remaining.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "startup length below 8-byte header"))?;
    if n > MAX_STARTUP_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("startup body {n} exceeds {MAX_STARTUP_BYTES}-byte cap")));
    }
    tokio::io::copy(&mut sock.take(n), &mut tokio::io::sink()).await.map(drop)
}

/// Wire format: `Byte1('E') Int32(length) [Byte1(tag) String(value)]* Byte1(0)`
fn build_starting_up_response() -> Vec<u8> {
    let body: Vec<u8> = [(b'S', "FATAL"), (b'V', "FATAL"), (b'C', "57P03"), (b'M', "the database system is starting up")]
        .into_iter()
        .flat_map(|(tag, value)| [tag].into_iter().chain(value.bytes()).chain([0]))
        .chain([0])
        .collect();
    [b'E'].into_iter().chain(((body.len() + 4) as u32).to_be_bytes()).chain(body).collect()
}

#[cfg(test)]
mod pgwire_early_bind_tests {
    use super::*;

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

    async fn spawn_acceptor() -> (u16, CancellationToken, tokio::task::JoinHandle<()>) {
        spawn_acceptor_with(MAX_CONCURRENT_EARLY_HANDLERS).await
    }

    async fn spawn_acceptor_with(max_handlers: usize) -> (u16, CancellationToken, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        let task = tokio::spawn(async move { accept_loop(&listener, token, max_handlers).await });
        (port, shutdown, task)
    }

    async fn assert_closed(client: &mut TcpStream, why: &str) {
        let mut tail = [0u8; 1];
        assert_eq!(client.read(&mut tail).await.unwrap(), 0, "{why}");
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
        client.write_all(&PROTO_3_0.to_be_bytes()).await.unwrap();
        assert_57p03(&mut client).await;
        assert_closed(&mut client, "server must close after error").await;
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
        client.write_all(&PROTO_3_0.to_be_bytes()).await.unwrap();
        assert_57p03(&mut client).await;
        assert_closed(&mut client, "server must close after error").await;
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

    /// Exercises drain_body with n > 0 (the no-params test sends len=8, n=0).
    #[tokio::test]
    async fn drains_startup_params_then_responds() {
        let (port, shutdown, task) = spawn_acceptor().await;
        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let params = b"user\0foo\0database\0bar\0\0";
        let len = (4 + 4 + params.len()) as u32;
        client.write_all(&len.to_be_bytes()).await.unwrap();
        client.write_all(&PROTO_3_0.to_be_bytes()).await.unwrap();
        client.write_all(params).await.unwrap();
        assert_57p03(&mut client).await;
        shutdown.cancel();
        let _ = task.await;
    }

    /// A client that connects but never sends a startup message must be
    /// closed after STARTUP_READ_TIMEOUT instead of holding the slot forever.
    #[tokio::test(start_paused = true)]
    async fn silent_client_closed_after_timeout() {
        let (port, shutdown, task) = spawn_acceptor().await;
        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        // No bytes sent; advance virtual time past the timeout.
        tokio::time::advance(STARTUP_READ_TIMEOUT + Duration::from_secs(1)).await;
        assert_closed(&mut client, "server must close after timeout").await;
        shutdown.cancel();
        let _ = task.await;
    }

    /// At the handler cap, excess connections still receive 57P03 rather than
    /// RST — Hasql/libpq treat RST as ECONNREFUSED, which is the failure mode
    /// this responder exists to avoid.
    #[tokio::test]
    async fn cap_serves_57p03_without_waiting_for_startup() {
        let (port, shutdown, task) = spawn_acceptor_with(1).await;

        // First connection holds the only permit — never sends startup, so it
        // sits inside handle_one's read awaiting bytes.
        let _holder = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

        // Poll for cap behaviour rather than sleeping a fixed amount: open
        // probes until one comes back with the canned 57P03 frame, confirming
        // the holder has the permit and the cap fast-path is firing.
        // Robust on loaded CI runners.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut probe = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            if tokio::time::timeout(Duration::from_millis(200), assert_57p03(&mut probe)).await.is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "cap fast-path never observed");
            tokio::task::yield_now().await;
        }

        shutdown.cancel();
        let _ = task.await;
    }

    /// Oversized declared length must trip the MAX_STARTUP_BYTES guard;
    /// the server drops the connection without sending a response.
    #[tokio::test]
    async fn rejects_oversized_startup() {
        let (port, shutdown, task) = spawn_acceptor().await;
        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let oversized = (MAX_STARTUP_BYTES as u32) + 1024;
        client.write_all(&oversized.to_be_bytes()).await.unwrap();
        client.write_all(&PROTO_3_0.to_be_bytes()).await.unwrap();
        let mut tag = [0u8; 1];
        let n = client.read(&mut tag).await.unwrap();
        assert_eq!(n, 0, "server must drop connection on oversized startup");
        shutdown.cancel();
        let _ = task.await;
    }
}

// ===== grpc_handlers =====
// gRPC ingestion service. Bidi-streaming endpoint that accepts Arrow IPC
// payloads and forwards them to the BufferedWriteLayer via Database.
//
// Auth: optional static bearer token in `authorization: Bearer <token>` metadata,
// validated against `CoreConfig::grpc_token`. When unset, the endpoint is open
// (intended for trusted-network deployments / development).

use std::io::Cursor;

use anyhow::Context;
use arrow_ipc::reader::StreamReader;
use subtle::ConstantTimeEq;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
// `Response` is pgwire's in this module; the gRPC one is qualified at its two uses.
use tonic::{Request, Status, Streaming};

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

    async fn write(&self, req: Request<Streaming<WriteBatch>>) -> Result<tonic::Response<Self::WriteStream>, Status> {
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

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
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
