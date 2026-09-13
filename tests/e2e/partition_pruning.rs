use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};
use super::ordering_pushdown::explain;

fn date_of(micros: i64) -> chrono::NaiveDate {
    chrono::DateTime::<chrono::Utc>::from_timestamp_micros(micros).unwrap().date_naive()
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn timestamp_between_prunes_to_its_date_partition() -> anyhow::Result<()> {
    let env = E2eEnv::builder().with_bucket_duration(Duration::from_secs(60)).start().await?;
    let client = env.pg_client().await?;
    let day = 86_400_000_000i64;
    let timestamps = [FROZEN_START_MICROS, FROZEN_START_MICROS + day, FROZEN_START_MICROS + 2 * day];

    for (i, timestamp) in timestamps.into_iter().enumerate() {
        insert_at(&client, &format!("day-{i}"), timestamp).await?;
    }
    env.advance(Duration::from_secs(3 * 86_400));
    env.force_flush().await?;

    let middle = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(timestamps[1]).unwrap();
    let query =
        format!("SELECT id FROM otel_logs_and_spans WHERE project_id = 'e2e_project' AND timestamp BETWEEN TIMESTAMP '{middle}' AND TIMESTAMP '{middle}'");
    let explain_plan = explain(&client, &query).await?;

    assert!(explain_plan.contains(&format!("date={}", middle.date_naive())), "BETWEEN must select its date partition; plan:\n{explain_plan}");
    for timestamp in [timestamps[0], timestamps[2]] {
        assert!(!explain_plan.contains(&format!("date={}", date_of(timestamp))), "BETWEEN scanned an unrelated date partition; plan:\n{explain_plan}");
    }

    let broad_date_explain = explain(&client, &format!("{query} AND date >= DATE '{}'", date_of(timestamps[0]))).await?;
    assert!(
        !broad_date_explain.contains(&format!("date={}", date_of(timestamps[2]))),
        "an existing date filter must not suppress tighter timestamp-derived bounds; plan:\n{broad_date_explain}"
    );

    Ok(())
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn write_derives_date_partition_from_timestamp() -> anyhow::Result<()> {
    let env = E2eEnv::builder().start().await?;
    let client = env.pg_client().await?;
    let timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap();

    client
        .execute(
            &format!(
                "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, hashes, name, level, status_code, summary) \
                 VALUES ('e2e_project', DATE '2000-01-01', TIMESTAMP '{timestamp}', 'mismatched-date', ARRAY[]::text[], 'span', 'INFO', 'OK', ARRAY['s'])"
            ),
            &[],
        )
        .await?;

    let date: String = client
        .query_one("SELECT CAST(date AS VARCHAR) FROM otel_logs_and_spans WHERE project_id = 'e2e_project' AND id = 'mismatched-date'", &[])
        .await?
        .get(0);
    assert_eq!(date, timestamp.date_naive().to_string());

    Ok(())
}
