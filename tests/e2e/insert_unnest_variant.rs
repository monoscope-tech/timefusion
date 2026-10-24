//! `INSERT … SELECT … FROM unnest(<array-per-column>)` end-to-end, in MemBuffer
//! and after a flush to Delta. Unlike `INSERT … VALUES`, the SELECT path enforces
//! non-nullability, so every non-nullable column must be supplied; JSON columns
//! sent as `text[]` must coerce to Variant.

use timefusion::support;
use tokio_postgres::types::Type;

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
        let id = row.schema().index_of("id")?;
        fields[id] = Arc::new(fields[id].as_ref().clone().with_nullable(true));
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
    };
    let good = input_batch("e2e_rejected_a", r#"{"msg":"safe"}"#)?;
    let bad = input_batch("e2e_rejected_b", "private-literal-canary invalid JSON")?;
    let null_id = input_batch("e2e_rejected_b", r#"{"msg":"safe"}"#)?;
    let mut columns = null_id.columns().to_vec();
    let id = null_id.schema().index_of("id")?;
    columns[id] = arrow::array::new_null_array(columns[id].data_type(), 1);
    let null_id = RecordBatch::try_new(null_id.schema(), columns)?;
    let before = env.snapshot_stats();
    for bad in [bad, null_id] {
        let mixed = arrow::compute::concat_batches(&good.schema(), [&good, &bad])?;
        assert!(routing.fast_insert_batch(mixed).await.is_err());
        let stream = RecordBatchStreamAdapter::new(good.schema(), futures::stream::iter([Ok(good.clone()), Ok(bad)]));
        assert!(routing.write_all(Box::pin(stream), &ctx.task_ctx()).await.is_err());
        let after = env.snapshot_stats();
        assert_eq!(
            (after.rows_ingested_total, after.mem_total_rows, after.wal_known_topics, after.wal_disk_bytes),
            (before.rows_ingested_total, before.mem_total_rows, before.wal_known_topics, before.wal_disk_bytes),
            "both write entry points must validate all batches/projects before any WAL append"
        );
    }

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

/// JSON construction must decode stored Variant columns across every storage leg.
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn nested_variant_json_survives_pgwire_and_flush() -> anyhow::Result<()> {
    use serde_json::{Value, json};

    let (env, client) = E2eEnv::short_buckets().await?;
    env.db().cancel_maintenance();
    let sql = "SELECT id, row_to_json(named_struct('attributes', attributes)) AS record, jsonb_build_array(attributes) AS items \
               FROM otel_metrics WHERE project_id='e2e_nested_json' ORDER BY id";
    let insert = "INSERT INTO otel_metrics (project_id, timestamp, date, ingested_at, id, series_id, metric_name, metric_unit, metric_type, flags, dropped_attributes_count, message_size_bytes, attributes) \
                  VALUES ('e2e_nested_json', $1, $2, $1, $3, $3, 'requests', '1', 'GAUGE', 0, 0, 10, $4)";
    let attributes = json!({"failure":{"class":"timeout"},"count":2,"active":true,"items":[1,null,"text"]});
    let mut expected = std::collections::BTreeMap::from([("null", Value::Null), ("populated", attributes.clone())]);
    for phase in 0..3 {
        if phase == 1 {
            env.advance(std::time::Duration::from_secs(10 * 60));
            env.flush_and_evict().await?;
        } else {
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(support::now_micros()).unwrap();
            let rows =
                if phase == 0 { vec![("populated", Some(attributes.to_string())), ("null", None)] } else { vec![("new", Some(r#""{\"a\":1}""#.into()))] };
            for (id, value) in rows {
                client.execute(insert, &[&dt, &dt.date_naive(), &id, &value]).await?;
            }
            if phase == 2 {
                expected.insert("new", json!(r#"{"a":1}"#));
            }
        }
        assert_eq!(env.snapshot_stats().mem_total_rows, [2, 0, 1][phase]);
        let simple = client.simple_query(sql).await?;
        let actual = simple
            .iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .map(|row| -> anyhow::Result<_> {
                Ok((row.get(0).unwrap().to_string(), serde_json::from_str::<Value>(row.get(1).unwrap())?, serde_json::from_str::<Value>(row.get(2).unwrap())?))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let expected: Vec<_> = expected.iter().map(|(id, value)| (id.to_string(), json!({"attributes":value}), json!([value]))).collect();
        assert_eq!(actual, expected, "phase {phase}: simple JSON values");
        let statement = client.prepare(sql).await?;
        assert_eq!(statement.columns()[1].type_(), &Type::TEXT);
        assert_eq!(statement.columns()[2].type_(), &Type::JSONB);
        for _ in 0..2 {
            let rows = client.query(&statement, &[]).await?;
            let actual = rows
                .iter()
                .map(|row| -> anyhow::Result<_> {
                    Ok((row.try_get::<_, String>(0)?, serde_json::from_str::<Value>(&row.try_get::<_, String>(1)?)?, row.try_get::<_, Value>(2)?))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            assert_eq!(actual, expected, "phase {phase}: prepared and cached JSON values");
        }
    }
    Ok(())
}

/// Real metrics scans must retain inherited projection names after Variant rewrites.
#[test_case::test_case("SELECT coalesce(attributes, 'null') FROM otel_metrics WHERE project_id='e2e_projection'", "coalesce(otel_metrics.attributes,Utf8(\"null\"))", Type::TEXT, Some(serde_json::Value::Null); "direct coalesce")]
#[test_case::test_case("SELECT * FROM (SELECT coalesce(attributes, 'null') FROM otel_metrics WHERE project_id='e2e_projection') q", "coalesce(otel_metrics.attributes,Utf8(\"null\"))", Type::TEXT, Some(serde_json::Value::Null); "inherited coalesce")]
#[test_case::test_case("WITH q AS (SELECT coalesce(attributes, 'null') AS value FROM otel_metrics WHERE project_id='e2e_projection') SELECT value FROM q", "value", Type::TEXT, Some(serde_json::Value::Null); "explicit alias")]
#[test_case::test_case("SELECT * FROM (SELECT attributes FROM otel_metrics WHERE project_id='e2e_projection') q", "attributes", Type::JSONB, None; "nested column")]
#[test_case::test_case("SELECT coalesce(attributes, attributes) FROM otel_metrics WHERE project_id='e2e_projection'", "coalesce(otel_metrics.attributes,otel_metrics.attributes)", Type::JSONB, None; "binary coalesce")]
#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn variant_projection_names_survive_pgwire(sql: &str, name: &str, wire_type: Type, null_value: Option<serde_json::Value>) -> anyhow::Result<()> {
    let (env, client) = E2eEnv::short_buckets().await?;
    env.db().cancel_maintenance();
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap();
    client.execute(
        "INSERT INTO otel_metrics (project_id, timestamp, date, ingested_at, id, series_id, metric_name, metric_unit, metric_type, flags, dropped_attributes_count, message_size_bytes, attributes) \
         VALUES ('e2e_projection', $1, $2, $1, 'populated', 's1', 'requests', '1', 'GAUGE', 0, 0, 10, '{\"label\":\"a\"}'), \
                ('e2e_projection', $1, $2, $1, 'null', 's2', 'requests', '1', 'GAUGE', 0, 0, 10, NULL)",
        &[&dt, &dt.date_naive()],
    ).await?;
    let cache = timefusion::read::plan_cache::global().expect("production plan cache");
    let before = cache.shape_counters();
    let mut expected = vec![Some(r#"{"label":"a"}"#.to_string()), null_value.map(|v| v.to_string())];
    for phase in 0..3 {
        if phase == 1 {
            env.advance(std::time::Duration::from_secs(10 * 60));
            env.flush_and_evict().await?;
        }
        if phase == 2 {
            let now = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(support::now_micros()).unwrap();
            client.execute(
                "INSERT INTO otel_metrics (project_id, timestamp, date, ingested_at, id, series_id, metric_name, metric_unit, metric_type, flags, dropped_attributes_count, message_size_bytes, attributes) \
                 VALUES ('e2e_projection', $1, $2, $1, 'new', 's3', 'requests', '1', 'GAUGE', 0, 0, 10, '{\"label\":\"b\"}')",
                &[&now, &now.date_naive()],
            ).await?;
            expected.push(Some(r#"{"label":"b"}"#.to_string()));
        }
        assert_eq!(env.snapshot_stats().mem_total_rows, [2, 0, 1][phase], "fixture must exercise MemBuffer, Delta, then mixed storage");
        expected.sort();
        let simple = client.simple_query(sql).await?;
        let simple: Vec<_> = simple
            .iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .collect();
        assert!(
            simple.iter().all(|row| row.columns()[0].name() == name),
            "phase {phase}: simple output name must survive rewriting: {:?}",
            simple.iter().map(|row| row.columns()[0].name()).collect::<Vec<_>>()
        );
        let mut values = simple
            .iter()
            .map(|row| row.get(0).map(serde_json::from_str::<serde_json::Value>).transpose().map(|v| v.map(|v| v.to_string())))
            .collect::<Result<Vec<_>, _>>()?;
        values.sort();
        assert_eq!(values, expected, "phase {phase}: simple values and NULL semantics");
        let statement = client.prepare(sql).await?;
        assert_eq!(statement.columns()[0].name(), name);
        assert_eq!(statement.columns()[0].type_(), &wire_type);
        for _ in 0..2 {
            let rows = client.query(&statement, &[]).await?;
            let mut values = rows
                .iter()
                .map(|row| -> anyhow::Result<Option<String>> {
                    let value: Option<serde_json::Value> = if wire_type == Type::JSONB {
                        row.try_get(0)?
                    } else {
                        row.try_get::<_, Option<String>>(0)?.as_deref().map(serde_json::from_str).transpose()?
                    };
                    Ok(value.map(|v| v.to_string()))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            values.sort();
            assert_eq!(values, expected, "phase {phase}: prepared and cached values and NULL semantics");
        }
    }
    let after = cache.shape_counters();
    assert!(after.0 > before.0, "the queries must still use the shape cache");
    assert_eq!(after.1, before.1, "the fix must not add cache refusals");
    Ok(())
}
