//! Shared pgwire test server: runs `serve_with_logging` over a MinIO-backed
//! `Database` on an OS-assigned port, shut down on drop.

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use datafusion_postgres::ServerOptions;
use timefusion::{database::Database, support::test_helpers::minio_test_config};
use tokio::sync::Notify;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

pub struct TestServer {
    port: u16,
    pub test_id: String,
    shutdown: Arc<Notify>,
}

impl TestServer {
    pub async fn start() -> Result<Self> {
        timefusion::support::init_test_logging();

        let test_id = Uuid::new_v4().to_string();
        // Kernel-assigned port: tests run as concurrent processes, so any fixed range collides.
        let port = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();

        let db = Arc::new(Database::with_config(minio_test_config(&test_id, &format!("/tmp/timefusion-{test_id}"))).await?);
        db.get_or_create_table("test_project", "otel_logs_and_spans").await?;

        let shutdown = Arc::new(Notify::new());
        let (db, sd) = (db.clone(), shutdown.clone());
        tokio::spawn(async move {
            let mut ctx = db.clone().create_session_context();
            db.setup_session_context(&mut ctx).expect("Failed to setup context");

            let opts = ServerOptions::new().with_port(port).with_host("0.0.0.0".to_string());
            let auth = timefusion::server::AuthConfig { username: "postgres".into(), password: Some("postgres".into()) };

            tokio::select! {
                _ = sd.notified() => {},
                res = timefusion::server::serve_with_logging(Arc::new(ctx), &opts, auth, None, None, std::future::pending::<()>()) => {
                    if let Err(e) = res {
                        eprintln!("Server error: {e:?}");
                    }
                }
            }
        });

        Self::connect(port).await?;
        Ok(Self { port, test_id, shutdown })
    }

    async fn connect(port: u16) -> Result<Client> {
        let conn_str = format!("host=localhost port={port} user=postgres password=postgres");
        for _ in 0..100 {
            if let Ok((client, conn)) = tokio_postgres::connect(&conn_str, NoTls).await {
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        eprintln!("Connection error: {e}");
                    }
                });
                return Ok(client);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(anyhow::anyhow!("Failed to connect after timeout"))
    }

    pub async fn client(&self) -> Result<Client> {
        Self::connect(self.port).await
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}
