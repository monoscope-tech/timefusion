# TimeFusion Delta Lake Configuration Guide

This document describes the Delta Lake configuration options available in TimeFusion.

## Environment Variables

### Performance Tuning

- **TIMEFUSION_BLOOM_FILTER_NDV** (default: 1000000)
  - Number of distinct values hint for bloom filters
  - Increase for high-cardinality data (e.g., 10000000 for billions of unique values)
  - Affects bloom filter memory usage and accuracy

- **TIMEFUSION_PAGE_ROW_COUNT_LIMIT** (default: 20000)
  - Maximum number of rows per data page
  - Lower values improve query latency but increase file size
  - Consider reducing to 10000 for wide schemas with many attributes

- **TIMEFUSION_OPTIMIZE_TARGET_SIZE** (default: 536870912 - 512MB)
  - Target file size for optimize operations in bytes
  - Larger files (1GB+) reduce metadata overhead for high-volume systems
  - Smaller files improve query concurrency

- **TIMEFUSION_CHECKPOINT_INTERVAL** (default: 20)
  - Number of Delta versions between checkpoints
  - Higher values reduce checkpoint frequency but increase startup time
  - Consider 50-100 for high-write scenarios

### Maintenance Operations

- **TIMEFUSION_VACUUM_RETENTION_HOURS** (default: 336 - 2 weeks)
  - How long to retain old file versions before vacuum cleanup
  - Shorter retention saves storage but limits time travel capabilities
  - Must be longer than your longest-running queries

### Storage Configuration

- **AWS_S3_BUCKET** (required)
  - S3 bucket for Delta table storage

- **AWS_S3_ENDPOINT** (default: https://s3.amazonaws.com)
  - S3 endpoint URL for custom S3-compatible storage

- **TIMEFUSION_TABLE_PREFIX** (default: timefusion)
  - Prefix for Delta table paths in S3

### Batch Processing

- **ENABLE_BATCH_QUEUE** (default: false)
  - Enable batch queue for write buffering
  - Set to "true" to enable asynchronous batch writes

## Key Configuration Changes

### 1. Parquet 2.0
Now enabled by default for better encoding efficiency and compression.

### 2. Enhanced Bloom Filters
Added bloom filters for:
- `attributes___http___request___method`
- `attributes___error___type`
- `level`
- `status_code`

### 3. Improved Z-Ordering
Z-ordering now includes both `timestamp` and `resource___service___name` for better multi-tenant query performance.

### 4. Consistent Timestamp Precision
All timestamp fields now use microsecond precision for consistency.

## Optimization Tips

1. **For High Volume (>1TB/day)**:
   ```bash
   export TIMEFUSION_OPTIMIZE_TARGET_SIZE=1073741824  # 1GB
   export TIMEFUSION_BLOOM_FILTER_NDV=10000000       # 10M
   export TIMEFUSION_CHECKPOINT_INTERVAL=100
   ```

2. **For Low Latency Queries**:
   ```bash
   export TIMEFUSION_PAGE_ROW_COUNT_LIMIT=10000
   export TIMEFUSION_OPTIMIZE_TARGET_SIZE=268435456  # 256MB
   ```

3. **For Cost Optimization**:
   ```bash
   export TIMEFUSION_VACUUM_RETENTION_HOURS=168      # 1 week
   export ENABLE_BATCH_QUEUE=true
   ```

## Monitoring

Monitor these metrics to tune configuration:
- Optimize operation duration and files processed
- Query latencies by service/time range
- Storage growth rate
- Checkpoint creation frequency