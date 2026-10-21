//! `INSERT … SELECT … FROM unnest(<array-per-column>)` end-to-end, in MemBuffer
//! and after a flush to Delta. Unlike `INSERT … VALUES`, the SELECT path enforces
//! non-nullability, so every non-nullable column must be supplied; JSON columns
//! sent as `text[]` must coerce to Variant.

use timefusion::support;

use super::harness::{E2eEnv, FROZEN_START_MICROS};

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn insert_select_unnest_coerces_text_to_variant() -> anyhow::Result<()> {
    let (env, client) = E2eEnv::short_buckets().await?;
    let pid = "e2e_unnest";
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap();

    // All params bound as text[]: TF's pgwire rejects uuid[]/jsonb[].
    let n = 3usize;
    let pids = vec![pid.to_string(); n];
    let ids: Vec<String> = (0..n).map(|i| format!("id-{i}")).collect();
    let tss: Vec<String> = vec![dt.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string(); n];
    let dates: Vec<String> = vec![dt.format("%Y-%m-%d").to_string(); n];
    let bodies: Vec<String> = (0..n).map(|i| format!("{{\"msg\":\"hello-{i}\"}}")).collect();
    let attrs: Vec<String> = (0..n).map(|i| format!("{{\"k\":{i}}}")).collect();
    // summary elements joined with 0x1F; the comma in the second element pins delimiter safety.
    let summaries: Vec<String> = vec!["a\u{1f}b,c".to_string(); n];

    let sql = "INSERT INTO otel_logs_and_spans \
        (project_id, id, timestamp, date, summary, body, attributes) \
        SELECT u.pid, u.id, u.ts::timestamp, u.d::date, string_to_array(u.summary, chr(31)), u.body, u.attrs \
        FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[]) \
        AS u(pid, id, ts, d, summary, body, attrs)";
    // A bad final row must reject conversion before either project's first write/WAL append.
    let rejected_rows = 3;
    let mut rejected_pids = vec!["e2e_rejected_a".to_string(); rejected_rows];
    rejected_pids[rejected_rows - 1] = "e2e_rejected_b".into();
    let rejected_ids: Vec<String> = (0..rejected_rows).map(|i| format!("rejected-{i}")).collect();
    let rejected_tss = vec![tss[0].clone(); rejected_rows];
    let rejected_dates = vec![dates[0].clone(); rejected_rows];
    let rejected_summaries = vec![summaries[0].clone(); rejected_rows];
    let rejected_attrs = vec![attrs[0].clone(); rejected_rows];
    let before = env.snapshot_stats();
    for invalid in ["private-literal-canary invalid JSON".to_string(), format!(r#"{{"private-literal-canary":{}}}"#, "1".repeat(1_000))] {
        let mut rejected_bodies = vec![bodies[0].clone(); rejected_rows];
        rejected_bodies[rejected_rows - 1] = invalid;
        let error = client
            .execute(sql, &[&rejected_pids, &rejected_ids, &rejected_tss, &rejected_dates, &rejected_summaries, &rejected_bodies, &rejected_attrs])
            .await
            .expect_err("malformed JSON must reject the complete conversion stage");
        let message = error.as_db_error().expect("server error").message();
        assert!(!message.contains("private-literal-canary"), "wire errors must not disclose input: {message}");
        assert!(message.contains("Invalid JSON") && message.contains("body"), "safe error locates the invalid column: {message}");
        let after = env.snapshot_stats();
        assert_eq!(
            (after.rows_ingested_total, after.mem_total_rows, after.wal_known_topics, after.wal_disk_bytes),
            (before.rows_ingested_total, before.mem_total_rows, before.wal_known_topics, before.wal_disk_bytes),
            "conversion failure must not reach the write layer or WAL, including prior valid batches/projects"
        );
    }
    client.execute(sql, &[&pids, &ids, &tss, &dates, &summaries, &bodies, &attrs]).await?;

    // Exercise the two write entry points directly with full real-provider rows,
    // including a valid first batch followed by an invalid second project.
    use arrow::{
        array::{ArrayRef, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{datasource::sink::DataSink, physical_plan::stream::RecordBatchStreamAdapter};
    use std::sync::Arc;
    let mut ctx = env.db().clone().create_session_context();
    env.db().setup_session_context(&mut ctx)?;
    let provider = ctx.table_provider("otel_logs_and_spans").await?;
    let routing = provider.downcast_ref::<timefusion::database::ProjectRoutingTable>().expect("real routing provider");
    let stored = env.buffered_layer().query(pid, "otel_logs_and_spans", &[])?;
    let row = stored.iter().find(|batch| batch.num_rows() > 0).expect("valid inserted row").slice(0, 1);
    let input_batch = |project: &str, body: &str| -> anyhow::Result<RecordBatch> {
        let mut fields = row.schema().fields().to_vec();
        let mut columns = row.columns().to_vec();
        for (name, value) in [("project_id", project), ("body", body)] {
            let idx = row.schema().index_of(name)?;
            fields[idx] = Arc::new(Field::new(name, DataType::Utf8, false));
            columns[idx] = Arc::new(StringArray::from(vec![value])) as ArrayRef;
        }
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
    };
    let good = input_batch("e2e_rejected_a", r#"{"msg":"safe"}"#)?;
    let bad = input_batch("e2e_rejected_b", "private-literal-canary invalid JSON")?;
    let before = env.snapshot_stats();
    let mixed = arrow::compute::concat_batches(&good.schema(), [&good, &bad])?;
    assert!(routing.fast_insert_batch(mixed).await.is_err());
    let stream = RecordBatchStreamAdapter::new(good.schema(), futures::stream::iter([Ok(good), Ok(bad)]));
    assert!(routing.write_all(Box::pin(stream), &ctx.task_ctx()).await.is_err());
    let after = env.snapshot_stats();
    assert_eq!(
        (after.rows_ingested_total, after.mem_total_rows, after.wal_known_topics, after.wal_disk_bytes),
        (before.rows_ingested_total, before.mem_total_rows, before.wal_known_topics, before.wal_disk_bytes),
        "both write entry points must finish conversion across projects/batches before any WAL append"
    );

    // Keep the Variant extraction non-aggregate: an aggregate over it hits an
    // unrelated aggregate-schema path.
    let cnt: i64 = client.query_one("SELECT COUNT(*)::bigint FROM otel_logs_and_spans WHERE project_id=$1", &[&pid]).await?.get(0);
    assert_eq!(cnt, n as i64, "all rows inserted via unnest");
    let msg: String = client.query_one("SELECT body ->> 'msg' FROM otel_logs_and_spans WHERE project_id=$1 AND id='id-0'", &[&pid]).await?.get(0);
    assert_eq!(msg, "hello-0", "body coerced to queryable Variant in MemBuffer");

    let s: Vec<String> = client.query_one("SELECT summary FROM otel_logs_and_spans WHERE project_id=$1 AND id='id-0'", &[&pid]).await?.get(0);
    assert_eq!(s, vec!["a".to_string(), "b,c".to_string()], "string_to_array(chr(31)) is comma-safe");

    // Flush to Delta + evict MemBuffer: the Variant predicate must still hold from parquet.
    support::set_micros(FROZEN_START_MICROS + 10 * 60 * 1_000_000);
    env.flush_and_evict().await?;
    let n_hit: i64 =
        client.query_one("SELECT COUNT(*)::bigint FROM otel_logs_and_spans WHERE project_id=$1 AND body ->> 'msg' = 'hello-1'", &[&pid]).await?.get(0);
    assert_eq!(n_hit, 1, "Variant predicate holds from Delta after flush");
    Ok(())
}
