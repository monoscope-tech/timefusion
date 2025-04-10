#[cfg(test)]
mod integration {
    use std::{
        collections::HashSet,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use anyhow::Result;
    use dotenv::dotenv;
    use rand::Rng;
    use scopeguard;
    use serial_test::serial;
    use timefusion::{config::Config, database::Database};
    use tokio::{sync::Notify, time::sleep};
    use tokio_postgres::{Client, NoTls};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    async fn connect_with_retry(port: u16, timeout: Duration) -> Result<(Client, tokio::task::JoinHandle<()>), tokio_postgres::Error> {
        let start = Instant::now();
        let conn_string = format!("host=localhost port={port} user=postgres password=postgres");

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

        let (client, connection) = tokio_postgres::connect(&conn_string, NoTls).await?;
        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok((client, handle))
    }

    async fn start_test_server() -> Result<(Arc<Notify>, String, u16)> {
        let test_id = Uuid::new_v4().to_string();
        let _ = env_logger::builder().is_test(true).try_init();
        dotenv().ok();

        let mut rng = rand::thread_rng();
        let port = 5433 + (rng.gen_range(1..100) as u16);

        unsafe {
            std::env::set_var("PGWIRE_PORT", &port.to_string());
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", test_id));
        }

        let shutdown_signal = Arc::new(Notify::new());
        let shutdown_signal_clone = shutdown_signal.clone();

        tokio::spawn(async move {
            let config = Config::from_env().expect("Failed to load config");
            let db = Database::new(&config).await.expect("Failed to create database");
            let session_context = db.create_session_context();
            db.setup_session_context(&session_context).expect("Failed to setup session context");

            let port = std::env::var("PGWIRE_PORT").expect("PGWIRE_PORT not set").parse::<u16>().expect("Invalid PGWIRE_PORT");

            let shutdown_token = CancellationToken::new();
            let pg_server = db.start_pgwire_server(session_context, port, shutdown_token.clone()).await.expect("Failed to start PGWire server");

            shutdown_signal_clone.notified().await;
            shutdown_token.cancel();
            let _ = pg_server.await;
        });

        let port = std::env::var("PGWIRE_PORT").expect("PGWIRE_PORT not set").parse::<u16>().expect("Invalid PGWIRE_PORT");
        let _ = connect_with_retry(port, Duration::from_secs(5)).await?;

        Ok((shutdown_signal, test_id, port))
    }

    // Rest of the file remains unchanged
    #[tokio::test]
    #[serial]
    async fn test_postgres_integration() -> Result<()> {
        let (shutdown_signal, test_id, port) = start_test_server().await?;
        let shutdown = || {
            shutdown_signal.notify_one();
        };

        let shutdown_guard = scopeguard::guard((), |_| shutdown());

        let (client, _) = connect_with_retry(port, Duration::from_secs(3))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {}", e))?;

        let timestamp_str = format!("'{}'", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"));
        let insert_query = format!(
            "INSERT INTO otel_logs_and_spans (project_id, timestamp, id, name, status_code, status_message, level) 
             VALUES ($1, {}, $2, $3, $4, $5, $6)",
            timestamp_str
        );

        let result = async {
            client
                .execute(
                    &insert_query,
                    &[&"test_project", &test_id, &"test_span_name", &"OK", &"Test integration", &"INFO"],
                )
                .await?;

            let rows = client.query("SELECT COUNT(*) FROM otel_logs_and_spans WHERE id = $1", &[&test_id]).await?;
            assert_eq!(rows[0].get::<_, i64>(0), 1, "Should have found exactly one row");

            let detail_rows = client.query("SELECT name, status_code FROM otel_logs_and_spans WHERE id = $1", &[&test_id]).await?;
            assert_eq!(detail_rows.len(), 1, "Should have found exactly one detailed row");
            assert_eq!(detail_rows[0].get::<_, String>(0), "test_span_name", "Name should match");
            assert_eq!(detail_rows[0].get::<_, String>(1), "OK", "Status code should match");

            for i in 0..5 {
                let span_id = Uuid::new_v4().to_string();
                client
                    .execute(
                        &insert_query,
                        &[&"test_project", &span_id, &format!("batch_span_{}", i), &"OK", &format!("Batch test {}", i), &"INFO"],
                    )
                    .await?;
            }

            let count_rows = client.query("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"test_project"]).await?;
            assert_eq!(count_rows[0].get::<_, i64>(0), 6, "Should have a total of 6 records");

            let count_rows = client.query("SELECT project_id FROM otel_logs_and_spans WHERE project_id = $1", &[&"test_project"]).await?;
            assert_eq!(count_rows[0].get::<_, String>(0), "test_project", "project_id should match");

            let count_rows = client.query("SELECT * FROM otel_logs_and_spans WHERE project_id = $1", &[&"test_project"]).await?;
            assert_eq!(count_rows[0].columns().len(), 80, "Should return all 80 columns");

            Ok::<_, tokio_postgres::Error>(())
        }
        .await;

        std::mem::drop(shutdown_guard);
        shutdown();

        result.map_err(|e| anyhow::anyhow!("Test failed: {}", e))
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrent_postgres_requests() -> Result<()> {
        let (shutdown_signal, test_id, port) = start_test_server().await?;
        let shutdown = || {
            shutdown_signal.notify_one();
        };

        let shutdown_guard = scopeguard::guard((), |_| shutdown());

        let num_clients = 5;
        let ops_per_client = 10;

        println!("Creating {} client connections", num_clients);

        let inserted_ids = Arc::new(Mutex::new(HashSet::new()));

        let timestamp_str = format!("'{}'", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"));
        let insert_query = format!(
            "INSERT INTO otel_logs_and_spans (project_id, timestamp, id, name, status_code, status_message, level) 
             VALUES ($1, {}, $2, $3, $4, $5, $6)",
            timestamp_str
        );

        let mut handles = Vec::with_capacity(num_clients);

        for i in 0..num_clients {
            let (client, _) = connect_with_retry(port, Duration::from_secs(3))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {}", e))?;

            let insert_query = insert_query.clone();
            let inserted_ids_clone = Arc::clone(&inserted_ids);
            let test_id_prefix = format!("{}-client-{}", test_id, i);

            let handle = tokio::spawn(async move {
                let mut client_ids = HashSet::new();

                for j in 0..ops_per_client {
                    let span_id = format!("{}-op-{}", test_id_prefix, j);
                    println!("Client {} executing operation {}", i, j);
                    let start = Instant::now();
                    client
                        .execute(
                            &insert_query,
                            &[
                                &"test_project",
                                &span_id,
                                &format!("concurrent_span_client_{}_op_{}", i, j),
                                &"OK",
                                &format!("Concurrent test client {} op {}", i, j),
                                &"INFO",
                            ],
                        )
                        .await
                        .expect("Insert should succeed");
                    println!("Client {} operation {} completed in {:?}", i, j, start.elapsed());

                    client_ids.insert(span_id);

                    if j % 3 == 0 {
                        let _query_result = client
                            .query("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"test_project"])
                            .await
                            .expect("Query should succeed");
                    }

                    if j % 5 == 0 {
                        let _detail_rows = client
                            .query(
                                &format!("SELECT name, status_code FROM otel_logs_and_spans WHERE id LIKE '{test_id_prefix}%'"),
                                &[],
                            )
                            .await
                            .expect("Query should succeed");
                    }
                }

                let mut ids = inserted_ids_clone.lock().unwrap();
                ids.extend(client_ids);
                ()
            });

            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.await.expect("Task should complete successfully");
        }

        let (client, _) = connect_with_retry(port, Duration::from_secs(3))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {}", e))?;

        let count_rows = client
            .query(&format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE id LIKE '{test_id}%'"), &[])
            .await
            .map_err(|e| anyhow::anyhow!("Query failed: {}", e))?;

        let count = count_rows[0].get::<_, i64>(0);
        let expected_count = (num_clients * ops_per_client) as i64;

        println!("Total records found: {} (expected {})", count, expected_count);
        assert_eq!(count, expected_count, "Should have inserted the expected number of records");

        let id_rows = client
            .query(&format!("SELECT id FROM otel_logs_and_spans WHERE id LIKE '{test_id}%'"), &[])
            .await
            .map_err(|e| anyhow::anyhow!("Query failed: {}", e))?;

        let mut db_ids = HashSet::new();
        for row in id_rows {
            db_ids.insert(row.get::<_, String>(0));
        }

        let ids = inserted_ids.lock().unwrap();
        let missing_ids: Vec<_> = ids.difference(&db_ids).collect();
        let unexpected_ids: Vec<_> = db_ids.difference(&ids).collect();

        assert!(missing_ids.is_empty(), "Expected all IDs to be found, missing: {:?}", missing_ids);
        assert!(unexpected_ids.is_empty(), "Found unexpected IDs: {:?}", unexpected_ids);

        let num_query_clients = 3;
        let queries_per_client = 5;

        let mut query_handles = Vec::with_capacity(num_query_clients);
        let query_times = Arc::new(Mutex::new(Vec::new()));

        for _i in 0..num_query_clients {
            let (client, _) = connect_with_retry(port, Duration::from_secs(3))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {}", e))?;

            let test_id = test_id.clone();
            let query_times = Arc::clone(&query_times);

            let handle = tokio::spawn(async move {
                let start = Instant::now();

                for j in 0..queries_per_client {
                    match j % 3 {
                        0 => {
                            let _ = client
                                .query("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"test_project"])
                                .await
                                .expect("Query should succeed");
                        }
                        1 => {
                            let _ = client
                                .query(
                                    &format!("SELECT name, status_code FROM otel_logs_and_spans WHERE id LIKE '{test_id}%' LIMIT 10"),
                                    &[],
                                )
                                .await
                                .expect("Query should succeed");
                        }
                        _ => {
                            let _ = client
                                .query("SELECT status_code, COUNT(*) FROM otel_logs_and_spans GROUP BY status_code", &[])
                                .await
                                .expect("Query should succeed");
                        }
                    }
                }

                let elapsed = start.elapsed();
                let mut times = query_times.lock().unwrap();
                times.push(elapsed);
                ()
            });

            query_handles.push(handle);
        }

        for handle in query_handles {
            let _ = handle.await.expect("Task should complete successfully");
        }

        let times = query_times.lock().unwrap();
        let total_time: Duration = times.iter().sum();
        let avg_time = if times.is_empty() { Duration::new(0, 0) } else { total_time / times.len() as u32 };
        println!("Average query execution time per client: {:?}", avg_time);

        std::mem::drop(shutdown_guard);
        shutdown();

        Ok(())
    }
}
