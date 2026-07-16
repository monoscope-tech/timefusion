use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

fn plan(rows: &[tokio_postgres::Row]) -> String {
    rows.iter().map(|r| (0..r.len()).map(|c| r.try_get::<_, String>(c).unwrap_or_default()).collect::<Vec<_>>().join(" | ")).collect::<Vec<_>>().join("\n")
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
    let middle_date = middle.date_naive();
    let query =
        format!("SELECT id FROM otel_logs_and_spans WHERE project_id = 'e2e_project' AND timestamp BETWEEN TIMESTAMP '{middle}' AND TIMESTAMP '{middle}'");
    let explain = plan(&client.query(&format!("EXPLAIN {query}"), &[]).await?);

    assert!(explain.contains(&format!("date={middle_date}")), "BETWEEN must select its date partition; plan:\n{explain}");
    for timestamp in [timestamps[0], timestamps[2]] {
        let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(timestamp).unwrap().date_naive();
        assert!(!explain.contains(&format!("date={date}")), "BETWEEN scanned an unrelated date partition; plan:\n{explain}");
    }

    let broad_date_query =
        format!("{query} AND date >= DATE '{}'", chrono::DateTime::<chrono::Utc>::from_timestamp_micros(timestamps[0]).unwrap().date_naive());
    let broad_date_explain = plan(&client.query(&format!("EXPLAIN {broad_date_query}"), &[]).await?);
    assert!(
        !broad_date_explain.contains(&format!("date={}", chrono::DateTime::<chrono::Utc>::from_timestamp_micros(timestamps[2]).unwrap().date_naive())),
        "an existing date filter must not suppress tighter timestamp-derived bounds; plan:\n{broad_date_explain}"
    );

    Ok(())
}
