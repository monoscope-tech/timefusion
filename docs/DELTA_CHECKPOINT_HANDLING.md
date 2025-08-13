# Delta Lake Checkpoint Handling in TimeFusion

## Overview

TimeFusion's object store cache includes special handling for Delta Lake checkpoint files to ensure consistency while maintaining performance.

## The Problem

Delta Lake uses a `_last_checkpoint` file to track the latest checkpoint version. When multiple writers are updating a table:

1. Writer A creates checkpoint at version 20
2. Writer A updates `_last_checkpoint` to point to version 20
3. Writer B reads cached (stale) `_last_checkpoint` showing version 10
4. Writer B creates unnecessary work or encounters consistency issues

## The Solution

TimeFusion uses a "stale-while-revalidate" approach for `_last_checkpoint` files:

### 1. Stale-While-Revalidate Pattern

`_last_checkpoint` files are always cached but with special handling:
- If the cached entry is older than 5 seconds, a background refresh is triggered
- The stale cached value is returned immediately while the refresh happens
- This provides low latency while ensuring eventual consistency

### 2. Explicit Cache Invalidation

Applications can explicitly invalidate the checkpoint cache when they know a table has been updated:

```rust
// After updating a Delta table
cache.invalidate_checkpoint_cache("s3://bucket/table");
```

### 3. Unified TTL Configuration

All files now use the same TTL configuration for simplicity:

```bash
# TTL for all cache entries (default: 7 days)
export TIMEFUSION_FOYER_TTL_SECONDS=604800
```

The special handling for `_last_checkpoint` files happens automatically regardless of the TTL setting.

## Configuration Recommendations

### Single Writer Scenario
```bash
# Can safely cache checkpoints for better performance
export TIMEFUSION_FOYER_CACHE_DELTA_CHECKPOINTS=true
export TIMEFUSION_FOYER_DELTA_METADATA_TTL_SECONDS=60
```

### Multiple Writers (Default)
```bash
# Don't cache checkpoints, use short TTL for metadata
export TIMEFUSION_FOYER_CACHE_DELTA_CHECKPOINTS=false
export TIMEFUSION_FOYER_DELTA_METADATA_TTL_SECONDS=5
```

### Write-Heavy Workloads
```bash
# Very short or no caching for Delta metadata
export TIMEFUSION_FOYER_CACHE_DELTA_CHECKPOINTS=false
export TIMEFUSION_FOYER_DELTA_METADATA_TTL_SECONDS=1
```

### Read-Heavy Workloads with Infrequent Writes
```bash
# Longer TTL acceptable if writes are rare
export TIMEFUSION_FOYER_CACHE_DELTA_CHECKPOINTS=false
export TIMEFUSION_FOYER_DELTA_METADATA_TTL_SECONDS=30
```

## Implementation Details

The cache implementation in `src/object_store_cache.rs` includes:

1. **Path Detection**: Methods to identify Delta metadata and checkpoint files
2. **Conditional Caching**: Based on file type and configuration
3. **Invalidation Logic**: Removes checkpoint cache when commits are written
4. **TTL Management**: Different TTLs for different file types

## Limitations

1. **Pattern-Based Invalidation**: Foyer doesn't support wildcard cache removal, so we can't invalidate all checkpoint files at once
2. **Network Latency**: Even with cache disabled, network latency to S3 may still cause brief inconsistencies
3. **DynamoDB Locking**: For strong consistency, use DynamoDB locking as described in DELTA_CONFIG.md

## Testing

The implementation includes comprehensive tests in `tests/delta_checkpoint_cache_test.rs`:

- Test checkpoint files are not cached when disabled
- Test cache invalidation when commits are written  
- Test separate TTL for Delta metadata files
- Test configuration options work correctly

## Future Improvements

1. **Smarter Invalidation**: Track checkpoint versions and invalidate selectively
2. **Distributed Cache Coordination**: Share invalidation events across nodes
3. **Checkpoint Versioning**: Cache multiple checkpoint versions with version-aware lookups