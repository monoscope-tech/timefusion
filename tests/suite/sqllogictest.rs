#[cfg(test)]
mod sqllogictest_tests {
    use std::{
        fmt,
        path::Path,
        sync::Arc,
        time::{Duration, Instant},
    };

    use anyhow::{Context, Result};
    use async_trait::async_trait;
    use datafusion_postgres::ServerOptions;
    use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
    use testcontainers::{ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner};
    use timefusion::database::Database;
    use tokio::{sync::Notify, time::sleep};
    use tokio_postgres::{NoTls, Row};
    use uuid::Uuid;

    // Custom error type that wraps both anyhow and tokio_postgres errors
    #[derive(Debug)]
    enum TestError {
        Postgres(tokio_postgres::Error),
        Other(String),
    }

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                TestError::Postgres(e) => write!(f, "Postgres error: {}", e),
                TestError::Other(s) => write!(f, "Error: {}", s),
            }
        }
    }

    impl std::error::Error for TestError {}

    impl From<tokio_postgres::Error> for TestError {
        fn from(e: tokio_postgres::Error) -> Self {
            TestError::Postgres(e)
        }
    }

    impl From<anyhow::Error> for TestError {
        fn from(e: anyhow::Error) -> Self {
            TestError::Other(e.to_string())
        }
    }

    struct TestDB {
        client: tokio_postgres::Client,
    }

    #[async_trait]
    impl AsyncDB for TestDB {
        type Error = TestError;
        type ColumnType = DefaultColumnType;

        async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
            let sql = sql.trim();
            // Only print SQL in verbose mode
            if std::env::var("SQLLOGICTEST_VERBOSE").is_ok() {
                println!("Executing SQL: {}", sql);
            }
            let is_query = sql.to_lowercase().starts_with("select");

            if !is_query {
                let affected = self.client.execute(sql, &[]).await?;
                if std::env::var("SQLLOGICTEST_VERBOSE").is_ok() {
                    println!("Statement executed, {} rows affected", affected);
                }
                return Ok(DBOutput::StatementComplete(affected));
            }

            let rows = self.client.query(sql, &[]).await?;
            if std::env::var("SQLLOGICTEST_VERBOSE").is_ok() {
                println!("Query returned {} rows", rows.len());
            }
            if rows.is_empty() {
                return Ok(DBOutput::Rows { types: vec![], rows: vec![] });
            }

            let types = rows[0]
                .columns()
                .iter()
                .map(|col| match col.type_().name() {
                    // UInt64 (from datafusion's array_length, json_length, etc.) is mapped to
                    // NUMERIC by datafusion-postgres (Postgres has no unsigned types). The
                    // values are always integral, so report Integer for sqllogictest's `I` checks.
                    "int2" | "int4" | "int8" | "numeric" => DefaultColumnType::Integer,
                    _ => DefaultColumnType::Text,
                })
                .collect();

            let result_rows = rows.iter().map(format_row).collect();

            Ok(DBOutput::Rows { types, rows: result_rows })
        }

        fn engine_name(&self) -> &str {
            "timefusion-postgres"
        }

        async fn shutdown(&mut self) {}
    }

    /// Wrapper that decodes Postgres binary NUMERIC into a plain decimal string.
    /// Format: ndigits(u16) weight(i16) sign(u16) dscale(u16) digits(u16 base-10000)...
    /// See postgres backend/utils/adt/numeric.c.
    struct PgNumeric(String);

    impl<'a> tokio_postgres::types::FromSql<'a> for PgNumeric {
        fn from_sql(_ty: &tokio_postgres::types::Type, buf: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
            if buf.len() < 8 {
                return Err("NUMERIC buffer too short".into());
            }
            let ndigits = u16::from_be_bytes([buf[0], buf[1]]) as usize;
            let weight = i16::from_be_bytes([buf[2], buf[3]]);
            let sign = u16::from_be_bytes([buf[4], buf[5]]);
            let dscale = u16::from_be_bytes([buf[6], buf[7]]) as usize;
            if buf.len() < 8 + ndigits * 2 {
                return Err("NUMERIC digits truncated".into());
            }
            let digits: Vec<u16> = (0..ndigits).map(|i| u16::from_be_bytes([buf[8 + i * 2], buf[9 + i * 2]])).collect();
            if sign == 0xC000 {
                return Ok(PgNumeric("NaN".into()));
            }
            if ndigits == 0 {
                return Ok(PgNumeric(if dscale == 0 { "0".into() } else { format!("0.{}", "0".repeat(dscale)) }));
            }
            // Integer part: digit group 0 is the most-significant; each subsequent group is 4 decimal digits.
            let mut int_part = String::new();
            for w in 0..=weight.max(0) as i32 {
                let idx = w as usize;
                let d = if idx < ndigits { digits[idx] } else { 0 };
                if w == 0 {
                    int_part.push_str(&d.to_string());
                } else {
                    int_part.push_str(&format!("{:04}", d));
                }
            }
            if int_part.is_empty() {
                int_part.push('0');
            }
            // Fractional part
            let mut frac_part = String::new();
            let frac_groups = (dscale as i32 + 3) / 4;
            for w in (weight as i32 + 1).max(0)..(weight as i32 + 1 + frac_groups) {
                let idx = w as usize;
                let d = if idx < ndigits { digits[idx] } else { 0 };
                frac_part.push_str(&format!("{:04}", d));
            }
            frac_part.truncate(dscale);
            let sign_prefix = if sign == 0x4000 { "-" } else { "" };
            Ok(PgNumeric(if dscale == 0 { format!("{sign_prefix}{int_part}") } else { format!("{sign_prefix}{int_part}.{frac_part}") }))
        }
        fn accepts(ty: &tokio_postgres::types::Type) -> bool {
            ty.name() == "numeric"
        }
    }

    fn format_row(row: &Row) -> Vec<String> {
        row.columns()
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let type_name = col.type_().name();

                match type_name {
                    "int2" => row
                        .try_get::<_, Option<i16>>(i)
                        .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| "error:int2".to_string()),
                    "int4" => row
                        .try_get::<_, Option<i32>>(i)
                        .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| "error:int4".to_string()),
                    "int8" => row
                        .try_get::<_, Option<i64>>(i)
                        .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| "error:int8".to_string()),
                    "float4" | "float8" => row
                        .try_get::<_, Option<f64>>(i)
                        .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| "error:float".to_string()),
                    // tokio-postgres has no built-in NUMERIC decoder (would require
                    // `with-rust_decimal-1`). Parse via a custom FromSql wrapper.
                    "numeric" => row
                        .try_get::<_, Option<PgNumeric>>(i)
                        .map(|v| v.map(|n| n.0).unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| "error:numeric".to_string()),
                    "bool" => row
                        .try_get::<_, Option<bool>>(i)
                        .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| "error:bool".to_string()),
                    "timestamp" => row
                        .try_get::<_, Option<chrono::NaiveDateTime>>(i)
                        .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| {
                            row.try_get::<_, Option<String>>(i).map(|v| v.unwrap_or_else(|| "NULL".to_string())).unwrap_or_else(|_| "[timestamp]".to_string())
                        }),
                    "json" | "jsonb" => row
                        .try_get::<_, Option<serde_json::Value>>(i)
                        .map(|v| v.map(|j| j.to_string()).unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| format!("error:{type_name}")),
                    _ => row.try_get::<_, Option<String>>(i).map(|v| v.unwrap_or_else(|| "NULL".to_string())).unwrap_or_else(|_| type_name.to_string()),
                }
            })
            .collect()
    }

    async fn connect_with_retry(port: u16, timeout: Duration) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), tokio_postgres::Error> {
        let start = Instant::now();
        let conn_string = format!("host=localhost port={} user=postgres password=postgres", port);

        while start.elapsed() < timeout {
            match tokio_postgres::connect(&conn_string, NoTls).await {
                Ok((client, connection)) => {
                    let handle = tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("Connection error: {}", e);
                        }
                    });
                    return Ok((client, handle));
                }
                Err(_) => sleep(Duration::from_millis(100)).await,
            }
        }

        // Final attempt
        let (client, connection) = tokio_postgres::connect(&conn_string, NoTls).await?;
        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok((client, handle))
    }

    /// Owns the MinIO instance for a test run. A locally-spawned `minio` binary
    /// is killed on drop; a Docker container is stopped by its own Drop; an
    /// externally-provided endpoint owns nothing.
    // The `Container` variant is inherently large (owns the testcontainers
    // handle); this guard is a single short-lived per-run value, so boxing
    // would add indirection for no benefit. (clippy 1.91 large_enum_variant.)
    #[allow(clippy::large_enum_variant)]
    enum MinioGuard {
        Process(std::process::Child),
        Container(#[allow(dead_code)] ContainerAsync<GenericImage>),
        External,
    }

    impl Drop for MinioGuard {
        fn drop(&mut self) {
            if let MinioGuard::Process(child) = self {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    async fn port_open(addr: &str) -> bool {
        tokio::net::TcpStream::connect(addr).await.is_ok()
    }

    /// Point the process at local MinIO so `Database::new()` never touches prod
    /// object storage. Resolution order (local-first, Docker last):
    ///   1. `TIMEFUSION_TEST_S3_ENDPOINT` if set (CI's MinIO, or any hand-run one).
    ///   2. An already-running MinIO on 127.0.0.1:9000 (e.g. `make minio-start`).
    ///   3. The local `minio` binary — spawned on :9000, killed when the test ends.
    ///   4. Docker (testcontainers) — only when no `minio` binary is on PATH.
    ///
    /// So `cargo test --test sqllogictest` needs zero setup, and Docker is a
    /// last resort rather than the default. Hitting non-local S3 is deliberately
    /// hard: it requires exporting the real AWS_* creds *and*
    /// `TIMEFUSION_TEST_S3_ENDPOINT` yourself.
    async fn ensure_local_minio() -> Result<(MinioGuard, String)> {
        const LOCAL: &str = "127.0.0.1:9000";
        let (guard, endpoint) = if let Ok(ep) = std::env::var("TIMEFUSION_TEST_S3_ENDPOINT") {
            (MinioGuard::External, ep)
        } else if port_open(LOCAL).await {
            (MinioGuard::External, format!("http://{LOCAL}"))
        } else if std::process::Command::new("minio").arg("--version").output().map(|o| o.status.success()).unwrap_or(false) {
            let child = spawn_local_minio()?;
            for _ in 0..100 {
                if port_open(LOCAL).await {
                    break;
                }
                sleep(Duration::from_millis(100)).await;
            }
            if !port_open(LOCAL).await {
                return Err(anyhow::anyhow!("local `minio` binary never came up on {LOCAL}"));
            }
            (MinioGuard::Process(child), format!("http://{LOCAL}"))
        } else {
            // Pinned like the e2e harness: the default image predates conditional
            // PUT, which makes Delta commit versions non-atomic (see MINIO_TAG).
            // GenericImage because the MinIO module waits for "API:" on stdout,
            // and modern images banner on stderr (see e2e::harness).
            let minio = GenericImage::new("minio/minio", "RELEASE.2025-09-07T16-13-09Z")
                .with_wait_for(WaitFor::message_on_stderr("API:"))
                .with_cmd(["server", "/data"])
                .with_env_var("MINIO_ROOT_USER", "minioadmin")
                .with_env_var("MINIO_ROOT_PASSWORD", "minioadmin")
                .start()
                .await
                .context("start MinIO container")?;
            let host = minio.get_host().await.context("get MinIO host")?.to_string();
            let port = minio.get_host_port_ipv4(9000).await.context("get MinIO port")?;
            (MinioGuard::Container(minio), format!("http://{host}:{port}"))
        };
        create_bucket(&endpoint, BUCKET).await?;
        Ok((guard, endpoint))
    }

    const BUCKET: &str = "timefusion-test";

    /// Spawn the local `minio` binary as a throwaway server on 127.0.0.1:9000.
    fn spawn_local_minio() -> Result<std::process::Child> {
        let data_dir = std::env::temp_dir().join("timefusion-slt-minio");
        std::fs::create_dir_all(&data_dir).ok();
        std::process::Command::new("minio")
            .arg("server")
            .arg(&data_dir)
            .arg("--address")
            .arg("127.0.0.1:9000")
            .env("MINIO_ROOT_USER", "minioadmin")
            .env("MINIO_ROOT_PASSWORD", "minioadmin")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .context("spawn local minio server")
    }

    /// Idempotent bucket create against MinIO (default creds minioadmin/minioadmin).
    async fn create_bucket(endpoint: &str, bucket: &str) -> Result<()> {
        use aws_sdk_s3::config::{Credentials, Region};
        let creds = Credentials::new("minioadmin", "minioadmin", None, None, "slt");
        let conf = aws_sdk_s3::config::Builder::new()
            .endpoint_url(endpoint)
            .credentials_provider(creds)
            .region(Region::new("us-east-1"))
            .force_path_style(true)
            .behavior_version(aws_config::BehaviorVersion::latest())
            .build();
        match aws_sdk_s3::Client::from_conf(conf).create_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = format!("{e:?}");
                if msg.contains("BucketAlreadyOwnedByYou") || msg.contains("BucketAlreadyExists") {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("create_bucket({bucket}) failed: {msg}"))
                }
            }
        }
    }

    async fn start_test_server() -> Result<(Arc<Notify>, u16, MinioGuard)> {
        let test_id = Uuid::new_v4().to_string();

        let (minio, endpoint) = ensure_local_minio().await?;

        // Free ephemeral port, asked of the kernel rather than derived from the
        // pid: with one process per .slt file these servers start concurrently,
        // and `5433 + pid % 100` collides often enough to wedge a whole run.
        let port = tokio::net::TcpListener::bind("127.0.0.1:0").await?.local_addr()?.port();

        // Explicit config rather than `Database::new()` + `set_var`: the storage
        // prefix and port are per-test, and writing them into the process env
        // makes concurrently-starting servers steal each other's values.
        let mut cfg = timefusion::config::AppConfig::default();
        cfg.aws.aws_s3_bucket = Some(BUCKET.to_string());
        cfg.aws.aws_s3_endpoint = endpoint;
        cfg.aws.aws_access_key_id = Some("minioadmin".into());
        cfg.aws.aws_secret_access_key = Some("minioadmin".into());
        cfg.aws.aws_default_region = Some("us-east-1".into());
        cfg.aws.aws_allow_http = Some("true".into());
        cfg.core.timefusion_table_prefix = format!("test-slt-{test_id}");
        cfg.core.timefusion_data_dir = std::env::temp_dir().join(format!("timefusion-slt-{test_id}"));
        cfg.cache.timefusion_foyer_disabled = true;

        // Use a shareable notification
        let shutdown_signal = Arc::new(Notify::new());
        let shutdown_signal_clone = shutdown_signal.clone();

        tokio::spawn(async move {
            let db = Database::with_config(Arc::new(cfg)).await.expect("Failed to create database");
            let db = Arc::new(db);
            let mut session_context = db.clone().create_session_context();
            db.setup_session_context(&mut session_context).expect("Failed to setup session context");

            let opts = ServerOptions::new().with_port(port).with_host("0.0.0.0".to_string());
            let auth_config = timefusion::server::AuthConfig { username: "postgres".into(), password: Some("postgres".into()) };

            tokio::select! {
                _ = shutdown_signal_clone.notified() => {},
                res = timefusion::server::serve_with_logging(Arc::new(session_context), &opts, auth_config, None, None, std::future::pending::<()>()) => {
                    if let Err(e) = res {
                        eprintln!("PGWire server error: {:?}", e);
                    }
                }
            }
        });

        // Generous: these servers boot concurrently with the rest of the suite,
        // and a debug-build `Database::new()` on a loaded box can take far longer
        // than it does in isolation. A too-tight budget here shows up as a
        // spurious .slt failure that only reproduces under full-suite load.
        let _ = connect_with_retry(port, Duration::from_secs(60)).await?;

        Ok((shutdown_signal, port, minio))
    }

    /// Run a single `tests/slt/<stem>.slt` against a private server.
    ///
    /// One server per file rather than one shared across all of them: under
    /// nextest each test is its own process, so the files run concurrently.
    /// They also share unqualified table names (`test_table`, `events`, `t`, …)
    /// and only stay isolated because each server gets its own storage prefix.
    async fn run_slt(stem: &str) -> Result<()> {
        // `_minio` keeps the MinIO instance alive for the whole test.
        let (shutdown_signal, port, _minio) = start_test_server().await?;
        let path = Path::new("tests/slt").join(format!("{stem}.slt"));

        let factory = || async move {
            let (client, _) = connect_with_retry(port, Duration::from_secs(30)).await?;
            Ok::<TestDB, TestError>(TestDB { client })
        };
        let result = sqllogictest::Runner::new(factory).run_file_async(&path).await;
        shutdown_signal.notify_one();
        result.map_err(|e| anyhow::anyhow!("{} failed: {e:?}", path.display()))
    }

    /// One `#[test]` per .slt file, so `cargo nextest run` fans them out across
    /// cores instead of walking ~2800 lines of SQL serially through one server.
    /// The test is named after the file, so `cargo nextest run variant_functions`
    /// runs just that one.
    macro_rules! slt_files {
        ($($stem:ident),* $(,)?) => {
            $(
                #[tokio::test(flavor = "multi_thread")]
                async fn $stem() -> Result<()> {
                    run_slt(stringify!($stem)).await
                }
            )*

            /// A new .slt file that nobody added to `slt_files!` would otherwise
            /// be silently never run.
            #[test]
            fn every_slt_file_has_a_test() {
                let declared = [$(stringify!($stem)),*];
                let missing: Vec<String> = std::fs::read_dir("tests/slt")
                    .expect("tests/slt")
                    .filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter(|p| p.extension().is_some_and(|x| x == "slt"))
                    .filter_map(|p| p.file_stem().and_then(|s| s.to_str()).map(str::to_owned))
                    .filter(|stem| !declared.contains(&stem.as_str()))
                    .collect();
                assert!(missing.is_empty(), "add these to slt_files!: {missing:?}");
            }
        };
    }

    slt_files!(
        aggregations,
        basic_operations,
        custom_functions,
        distinct_on_variant,
        edge_cases,
        filtering,
        function_availability_test,
        integration,
        json_functions,
        merge_on_read,
        monoscope_query_shapes,
        partition_pruning_test,
        pg_catalog,
        percentile_functions,
        variant_column,
        variant_functions,
    );
}
