#[cfg(test)]
mod sqllogictest_tests {
    use std::{fmt, path::Path, sync::Arc, time::Duration};

    use anyhow::{Context, Result};
    use async_trait::async_trait;
    use datafusion_postgres::ServerOptions;
    use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
    use testcontainers::{ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner};
    use timefusion::database::Database;
    use tokio::{sync::Notify, time::sleep};
    use tokio_postgres::Row;
    use uuid::Uuid;

    use crate::pg_client_compat::connect_with_retry;

    /// Render a Postgres error with its SQLSTATE and server message (Display alone is just "db error").
    fn pg_detail(e: &tokio_postgres::Error) -> String {
        match e.as_db_error() {
            Some(db) => format!("Postgres error [{}]: {}", db.code().code(), db.message()),
            None => format!("Postgres error: {e}"),
        }
    }

    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error("{}", pg_detail(.0))]
        Postgres(#[from] tokio_postgres::Error),
        #[error("Error: {0}")]
        Other(String),
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
            let trace = |msg: String| {
                if std::env::var("SQLLOGICTEST_VERBOSE").is_ok() {
                    println!("{msg}");
                }
            };
            trace(format!("Executing SQL: {sql}"));
            // Row-returning statements must go through `query()`; `execute()` yields only a count.
            let lowered = sql.to_lowercase();
            let is_query = ["select", "with", "show", "explain", "values", "table"].iter().any(|kw| lowered.starts_with(kw));

            if !is_query {
                let affected = self.client.execute(sql, &[]).await?;
                trace(format!("Statement executed, {affected} rows affected"));
                return Ok(DBOutput::StatementComplete(affected));
            }

            let rows = self.client.query(sql, &[]).await?;
            trace(format!("Query returned {} rows", rows.len()));
            if rows.is_empty() {
                return Ok(DBOutput::Rows { types: vec![], rows: vec![] });
            }

            let types = rows[0]
                .columns()
                .iter()
                .map(|col| match col.type_().name() {
                    // UInt64 arrives as NUMERIC (Postgres has no unsigned types) but is always
                    // integral, so report Integer for sqllogictest's `I` checks.
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
    /// Wire format: ndigits(u16) weight(i16) sign(u16) dscale(u16) digits(u16 base-10000)...
    #[derive(derive_more::Display)]
    #[display("{_0}")]
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
            let digit = |w: i32| digits.get(w as usize).copied().unwrap_or(0);
            // Digit group 0 is most-significant; every later group is 4 decimal digits.
            let int_part: String = (0..=weight.max(0) as i32).map(|w| if w == 0 { digit(w).to_string() } else { format!("{:04}", digit(w)) }).collect();
            let frac_groups = (dscale as i32 + 3) / 4;
            let mut frac_part: String = ((weight as i32 + 1).max(0)..(weight as i32 + 1 + frac_groups)).map(|w| format!("{:04}", digit(w))).collect();
            frac_part.truncate(dscale);
            let sign_prefix = if sign == 0x4000 { "-" } else { "" };
            Ok(PgNumeric(if dscale == 0 { format!("{sign_prefix}{int_part}") } else { format!("{sign_prefix}{int_part}.{frac_part}") }))
        }
        fn accepts(ty: &tokio_postgres::types::Type) -> bool {
            ty.name() == "numeric"
        }
    }

    /// Column `i` rendered as `T`. `None` means the decode failed; a SQL NULL renders "NULL".
    fn decode<'a, T>(row: &'a Row, i: usize) -> Option<String>
    where
        T: tokio_postgres::types::FromSql<'a> + fmt::Display,
    {
        row.try_get::<_, Option<T>>(i).ok().map(|v| v.map_or_else(|| "NULL".to_string(), |x| x.to_string()))
    }

    fn format_row(row: &Row) -> Vec<String> {
        row.columns()
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let type_name = col.type_().name();
                let text = || decode::<String>(row, i);
                match type_name {
                    "int2" => decode::<i16>(row, i).unwrap_or_else(|| "error:int2".to_string()),
                    "int4" => decode::<i32>(row, i).unwrap_or_else(|| "error:int4".to_string()),
                    "int8" => decode::<i64>(row, i).unwrap_or_else(|| "error:int8".to_string()),
                    // The f32 fallback is required: a float4 column is 4 wire bytes and fails an f64 decode.
                    "float4" | "float8" => decode::<f64>(row, i).or_else(|| decode::<f32>(row, i)).or_else(text).unwrap_or_else(|| "error:float".to_string()),
                    // tokio-postgres has no built-in NUMERIC decoder; use the wrapper above.
                    "numeric" => decode::<PgNumeric>(row, i).unwrap_or_else(|| "error:numeric".to_string()),
                    "bool" => decode::<bool>(row, i).unwrap_or_else(|| "error:bool".to_string()),
                    "timestamp" => decode::<chrono::NaiveDateTime>(row, i).or_else(text).unwrap_or_else(|| "[timestamp]".to_string()),
                    "json" | "jsonb" => decode::<serde_json::Value>(row, i).unwrap_or_else(|| format!("error:{type_name}")),
                    _ => text().unwrap_or_else(|| type_name.to_string()),
                }
            })
            .collect()
    }

    /// Owns the MinIO instance for a test run: a spawned `minio` binary is killed on
    /// drop, a container stops via its own Drop, an external endpoint owns nothing.
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

    /// Resolve a local MinIO endpoint, local-first so tests never touch remote storage:
    ///   1. `TIMEFUSION_TEST_S3_ENDPOINT` if set.
    ///   2. An already-running MinIO on 127.0.0.1:9000.
    ///   3. The local `minio` binary — spawned on :9000, killed when the test ends.
    ///   4. Docker (testcontainers) — only when no `minio` binary is on PATH.
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
            // Pinned to a tag with conditional PUT (Delta commits need it), pulled from
            // quay.io (the Docker Hub tag 404s). GenericImage because modern images
            // banner "API:" on stderr, which the testcontainers MinIO module does not expect.
            let minio = GenericImage::new("quay.io/minio/minio", "RELEASE.2025-09-07T16-13-09Z")
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

        // Ask the kernel for a free port; these servers start concurrently, so any
        // derived port scheme collides.
        let port = tokio::net::TcpListener::bind("127.0.0.1:0").await?.local_addr()?.port();

        // Per-test values must go in the config, never the process env: concurrent
        // servers would otherwise steal each other's prefix and port.
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

        // Deliberately generous: startup is much slower under full-suite load, and a
        // tight budget here surfaces as a spurious .slt failure.
        let _ = connect_with_retry(port, Duration::from_secs(60)).await?;

        Ok((shutdown_signal, port, minio))
    }

    /// Run a single `tests/slt/<stem>.slt` against a private server. One server per
    /// file: the files run concurrently and share unqualified table names, so
    /// isolation comes from each server's own storage prefix.
    async fn run_slt(stem: &str) -> Result<()> {
        // `_minio` keeps the MinIO instance alive for the whole test.
        let (shutdown_signal, port, _minio) = start_test_server().await?;
        let path = Path::new("tests/slt").join(format!("{stem}.slt"));

        let factory = || async move { Ok::<TestDB, TestError>(TestDB { client: connect_with_retry(port, Duration::from_secs(30)).await? }) };
        let result = sqllogictest::Runner::new(factory).run_file_async(&path).await;
        shutdown_signal.notify_one();
        result.map_err(|e| anyhow::anyhow!("{} failed: {e:?}", path.display()))
    }

    /// One `#[test]` per .slt file, named after the file, so nextest runs them in
    /// parallel and `cargo nextest run <stem>` runs just one.
    macro_rules! slt_files {
        ($($stem:ident),* $(,)?) => {
            $(
                #[tokio::test(flavor = "multi_thread")]
                async fn $stem() -> Result<()> {
                    run_slt(stringify!($stem)).await
                }
            )*

            /// Fails if a .slt file exists that `slt_files!` never declares.
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
