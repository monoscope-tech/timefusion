//! Does a pgwire INSERT reserve from the **query** memory pool?
//!
//! This decides whether `TIMEFUSION_MEMORY_POOL=fair_spill` is safe to turn on
//! for the query pool. `81dcc1cd` (2026-05-28) made `Greedy` the default after
//! every monoscope INSERT bounced with `Memory limit exceeded … > 76MB hard
//! limit` — FairSpill had sliced the pool into `pool / num_spill` slots and ~30
//! concurrent writers collapsed each slot below one batch. FairSpill is also
//! what would bound the `ExternalSorterMerge` exhaustion that restarted prod on
//! 2026-09-02 (a spillable sorter took 7.3 GB of the 16 GB greedy pool, so the
//! unspillable merge half had nothing left).
//!
//! Both claims are about the same pool — `Database::shared_runtime_env`, which
//! `create_session_context` hands to every statement, INSERT included. The
//! write path has had its own FairSpill pool since 2026-08-20
//! (`flush_sort_runtime_env`), so the question is whether anything on the
//! INSERT plan still charges the query pool.
//!
//! Reading the code does not settle it: `ProjectRoutingTable::write_all`
//! registers no `MemoryConsumer`, but the reservations in a DataFusion write
//! come from operators, not from the sink. So measure it.
//!
//! Method: sample `memory_pool.reserved()` while the workload runs and keep the
//! peak. Sampling can miss a reservation shorter than the poll interval, which
//! is why `a_sort_does_reserve_from_the_query_pool` is in the same file as a
//! control — it uses the same sampler against a plan that provably reserves. A
//! green INSERT assertion means nothing without a green control.
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
    use timefusion::{config::AppConfig, database::Database, support::test_helpers::minio_test_config};

    /// Rows per INSERT, and INSERTs run concurrently — the ~30-writer shape of
    /// the 2026-05-28 incident, not a single-statement smoke test.
    const WRITERS: usize = 30;
    const ROWS_PER_INSERT: usize = 64;

    /// Peak `reserved()` observed on `pool` until `stop` is set.
    ///
    /// 100 µs, not a millisecond: an INSERT's plan is short-lived, and the
    /// control test is what proves this interval is tight enough to see one.
    fn spawn_pool_sampler(ctx: &datafusion::prelude::SessionContext) -> (Arc<AtomicUsize>, Arc<AtomicBool>, tokio::task::JoinHandle<()>) {
        // The session's own runtime env — the same `shared_runtime_env` every
        // statement gets, reached through the public API rather than the
        // crate-private accessor.
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

    async fn fair_spill_db(test_id: &str) -> Result<Arc<Database>> {
        timefusion::support::init_test_logging();
        let cfg = minio_test_config(test_id, &format!("/tmp/timefusion-qpool-{test_id}"));
        let mut cfg = AppConfig::clone(&cfg);
        // The pool policy under test. Everything else stays at test defaults so
        // a failure here is about the pool and nothing else.
        cfg.memory.timefusion_memory_pool = timefusion::config::MemoryPoolKind::FairSpill;
        let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
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

    /// The answer this file exists for: concurrent INSERTs leave the query pool
    /// untouched, so the pool's policy cannot bounce them.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn an_insert_does_not_reserve_from_the_query_pool() -> Result<()> {
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let db = fair_spill_db(&test_id).await?;
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
            // An INSERT that failed for pool reasons is exactly the 2026-05-28
            // symptom, so surface the message rather than a bare unwrap.
            r?.map_err(|e| anyhow::anyhow!("concurrent INSERT failed under a FairSpill query pool: {e}"))?;
        }

        stop.store(true, Ordering::Relaxed);
        sampler.await?;
        assert_eq!(
            peak.load(Ordering::Relaxed),
            0,
            "INSERTs reserved from the query pool; FairSpill would slice that reservation and \
             the 2026-05-28 ingest incident can recur"
        );
        Ok(())
    }

    /// Control. Without this, the assertion above could pass because the
    /// sampler observes nothing at all.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_sort_does_reserve_from_the_query_pool() -> Result<()> {
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let db = fair_spill_db(&test_id).await?;
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let (peak, stop, sampler) = spawn_pool_sampler(&ctx);
        // Millions of rows, not the handful this file inserts: a sort small
        // enough to finish between two polls proves nothing about the sampler.
        // No table involved, so the control cannot fail for storage reasons.
        ctx.sql("SELECT value FROM generate_series(1, 4000000) ORDER BY value DESC LIMIT 1").await?.collect().await?;
        stop.store(true, Ordering::Relaxed);
        sampler.await?;

        assert!(peak.load(Ordering::Relaxed) > 0, "sampler saw no reservation for a 4M-row sort — it cannot witness the INSERT claim either");
        Ok(())
    }
}
