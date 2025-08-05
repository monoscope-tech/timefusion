# Delta Lake Checkpoint Handling in TimeFusion

## Overview

TimeFusion's object store cache now includes special handling for Delta Lake checkpoint files to prevent race conditions when multiple writers are updating Delta tables concurrently.

## The Problem

Delta Lake uses a `_last_checkpoint` file to track the latest checkpoint version. When multiple writers are updating a table:

1. Writer A creates checkpoint at version 20
2. Writer A updates `_last_checkpoint` to point to version 20
3. Writer B reads cached (stale) `_last_checkpoint` showing version 10
4. Writer B creates unnecessary work or encounters consistency issues

## The Solution

TimeFusion addresses this through several mechanisms:

### 1. Configurable Checkpoint Caching

By default, `_last_checkpoint` files are NOT cached to ensure readers always get the latest checkpoint information:

```bash
# Disable checkpoint caching (default)
export TIMEFUSION_FOYER_CACHE_DELTA_CHECKPOINTS=false

# Enable checkpoint caching (only for single-writer scenarios)
export TIMEFUSION_FOYER_CACHE_DELTA_CHECKPOINTS=true
```

### 2. Separate TTL for Delta Metadata

Delta metadata files (all files in `_delta_log/`) use a shorter TTL to reduce staleness:

```bash
# Short TTL for Delta metadata (default: 5 seconds)
export TIMEFUSION_FOYER_DELTA_METADATA_TTL_SECONDS=5

# Disable separate TTL (use regular TTL for all files)
export TIMEFUSION_FOYER_DELTA_METADATA_TTL_SECONDS=0
```

### 3. Automatic Cache Invalidation

When writing commit files (`*.json`) to the Delta log, the cache automatically invalidates the corresponding `_last_checkpoint` file to ensure subsequent reads get fresh data.

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