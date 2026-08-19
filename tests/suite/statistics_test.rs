use anyhow::Result;
use timefusion::read::DeltaStatisticsExtractor;

#[tokio::test]
async fn test_statistics_extractor_cache() -> Result<()> {
    // Test basic cache functionality
    let extractor = DeltaStatisticsExtractor::new(10, 300, 20_000);

    // Initially cache should be empty
    assert_eq!(extractor.get_cache_stats().await.0, 0);

    // Test cache stats method
    let (used, capacity) = extractor.get_cache_stats().await;
    assert_eq!(used, 0);
    assert_eq!(capacity, 10);

    // Test invalidation
    extractor.invalidate("test_project", "test_table").await;
    assert_eq!(extractor.get_cache_stats().await.0, 0);

    // Test clear cache
    extractor.clear_cache().await;
    assert_eq!(extractor.get_cache_stats().await.0, 0);

    Ok(())
}
