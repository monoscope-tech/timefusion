use anyhow::Result;
use timefusion::read::DeltaStatisticsExtractor;

#[tokio::test]
async fn test_statistics_extractor_cache() -> Result<()> {
    let extractor = DeltaStatisticsExtractor::new(10, 300, 20_000);
    assert_eq!(extractor.get_cache_stats().await, (0, 10));
    extractor.invalidate("test_project", "test_table").await;
    assert_eq!(extractor.get_cache_stats().await, (0, 10));
    extractor.clear_cache().await;
    assert_eq!(extractor.get_cache_stats().await, (0, 10));
    Ok(())
}
