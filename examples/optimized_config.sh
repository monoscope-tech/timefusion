#!/bin/bash
# Example configuration for high-volume production deployment

# Storage configuration
export AWS_S3_BUCKET="your-bucket-name"
export AWS_S3_ENDPOINT="https://s3.amazonaws.com"
export TIMEFUSION_TABLE_PREFIX="production"

# High-volume optimizations (>1TB/day)
export TIMEFUSION_OPTIMIZE_TARGET_SIZE=1073741824  # 1GB files
export TIMEFUSION_BLOOM_FILTER_NDV=10000000        # 10M distinct values
export TIMEFUSION_CHECKPOINT_INTERVAL=100          # Less frequent checkpoints
export TIMEFUSION_PAGE_ROW_COUNT_LIMIT=15000       # Balanced page size

# Maintenance settings
export TIMEFUSION_VACUUM_RETENTION_HOURS=168       # 1 week retention
export ENABLE_BATCH_QUEUE=true                     # Enable write buffering

# Optional: Enable debug logging for monitoring
export RUST_LOG=timefusion=info,deltalake=info

echo "TimeFusion optimized configuration loaded:"
echo "- Target file size: 1GB"
echo "- Bloom filter NDV: 10M"
echo "- Checkpoint interval: 100 versions"
echo "- Vacuum retention: 1 week"
echo "- Batch queue: enabled"