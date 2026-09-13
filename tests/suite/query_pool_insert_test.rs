//! Pins that a pgwire INSERT reserves nothing from the query memory pool, so the
//! pool's policy (`TIMEFUSION_MEMORY_POOL`) cannot bounce INSERTs. Both inbound routes
//! of `insert_records_batch` are measured; they diverge below the DataFusion sink.
//!
//! Requires MinIO on 127.0.0.1:9000 (`make minio-start`).

#[cfg(test)]
mod query_pool_insert {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use anyhow::Result;
    use serial_test::serial;
    use test_case::test_case;
    use timefusion::{config::AppConfig, database::Database, support::test_helpers::minio_test_config};

    const WRITERS: usize = 30;
    const ROWS_PER_INSERT: usize = 64;

    /// Peak `reserved()` observed on the session's memory pool until `stop` is set.
    /// The 100 µs interval must stay tight enough to catch a short-lived INSERT plan;
    /// `a_sort_does_reserve_from_the_query_pool` is the control that proves it is.
    fn spawn_pool_sampler(ctx: &datafusion::prelude::SessionContext) -> (Arc<AtomicUsize>, Arc<AtomicBool>, tokio::task::JoinHandle<()>) {
        let pool = ctx.runtime_env().memory_pool.clone();
        let (peak, stop) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicBool::new(false)));
        let (p, s) = (peak.clone(), stop.clone());
        let handle = tokio::spawn(async move {
            while !s.load(Ordering::Relaxed) {
                p.fetch_max(pool.reserved(), Ordering::Relaxed);
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
            p.fetch_max(pool.reserved(), Ordering::Relaxed);
        });
        (peak, stop, handle)
    }

    /// `buffered` selects which of `insert_records_batch`'s two inbound routes the
    /// INSERT takes: the buffered layer (prod) or a direct-to-Delta commit.
    async fn fair_spill_db(test_id: &str, buffered: bool) -> Result<Arc<Database>> {
        timefusion::support::init_test_logging();
        let cfg = minio_test_config(test_id, &format!("/tmp/timefusion-qpool-{test_id}"));
        let mut cfg = AppConfig::clone(&cfg);
        // The pool policy under test; everything else stays at test defaults.
        cfg.memory.timefusion_memory_pool = timefusion::config::MemoryPoolKind::FairSpill;
        let cfg = Arc::new(cfg);
        let db = Database::with_config(Arc::clone(&cfg)).await?;
        let db = Arc::new(if buffered {
            // A layer without a Delta writer errors on flush instead of exercising the path.
            let layer = Arc::new(timefusion::support::test_helpers::test_layer(cfg)?.with_delta_writer(timefusion::server::delta_write_callback(&db)));
            db.with_buffered_layer(layer)
        } else {
            db
        });
        db.get_or_create_table("test_project", "otel_logs_and_spans").await?;
        Ok(db)
    }

    /// `writer` distinguishes the rows so a concurrent run can't collapse to one key.
    fn insert_sql(writer: usize) -> String {
        let now = chrono::Utc::now();
        let values = (0..ROWS_PER_INSERT)
            .map(|i| {
                format!(
                    "('test_project', '{}', {}, 'w{writer}-r{i}', 'span-{writer}-{i}', 'OK', '', 'INFO', [], [])",
                    now.date_naive(),
                    now.timestamp_micros() + i as i64
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "INSERT INTO otel_logs_and_spans \
             (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) VALUES {values}"
        )
    }

    /// Concurrent INSERTs leave the query pool untouched, so the pool's policy
    /// cannot bounce them. Asserted on both routes.
    #[test_case(true ; "buffered")]
    #[test_case(false ; "direct")]
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_insert_does_not_reserve_from_the_query_pool(buffered: bool) -> Result<()> {
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let db = fair_spill_db(&test_id, buffered).await?;
        // `insert_records_batch` branches on exactly this, so pin the route.
        assert_eq!(db.buffered_layer().is_some(), buffered, "harness did not build the route it claims to measure");
        let mut probe = db.clone().create_session_context();
        db.setup_session_context(&mut probe)?;
        let (peak, stop, sampler) = spawn_pool_sampler(&probe);

        let writes = (0..WRITERS).map(|w| {
            let db = db.clone();
            tokio::spawn(async move {
                let mut ctx = db.clone().create_session_context();
                db.setup_session_context(&mut ctx)?;
                ctx.sql(&insert_sql(w)).await?.collect().await.map(|_| ()).map_err(anyhow::Error::from)
            })
        });
        for r in futures::future::join_all(writes).await {
            r?.map_err(|e| anyhow::anyhow!("concurrent INSERT failed under a FairSpill query pool: {e}"))?;
        }

        stop.store(true, Ordering::Relaxed);
        sampler.await?;
        assert_eq!(
            peak.load(Ordering::Relaxed),
            0,
            "INSERTs reserved from the query pool (buffered={buffered}); FairSpill would slice \
             that reservation and the 2026-05-28 ingest incident can recur"
        );
        Ok(())
    }

    /// Control: without it, the assertion above could pass merely because the
    /// sampler observes nothing at all.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_sort_does_reserve_from_the_query_pool() -> Result<()> {
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let db = fair_spill_db(&test_id, false).await?;
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let (peak, stop, sampler) = spawn_pool_sampler(&ctx);
        // Must stay large: a sort that finishes between two polls proves nothing
        // about the sampler. No table involved, so it cannot fail for storage reasons.
        ctx.sql("SELECT value FROM generate_series(1, 4000000) ORDER BY value DESC LIMIT 1").await?.collect().await?;
        stop.store(true, Ordering::Relaxed);
        sampler.await?;

        assert!(peak.load(Ordering::Relaxed) > 0, "sampler saw no reservation for a 4M-row sort — it cannot witness the INSERT claim either");
        Ok(())
    }
}
