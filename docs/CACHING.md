# TimeFusion Caching Layer

TimeFusion includes an object store caching layer powered by Foyer to optimize performance by caching Parquet files and Delta Lake metadata at the storage level.

## Object Store Cache (Foyer)

### Overview

The object store cache uses [Foyer](https://foyer.rs), a high-performance hybrid cache library, to cache Parquet files accessed from S3. This reduces S3 API calls, network latency, and improves query performance.

### Architecture

- **Hybrid Caching**: Two-tier architecture with memory (L1) and disk (L2) caches
- **Write-Through**: Writes go directly to S3, then invalidate cache entries
- **TTL-Based Expiration**: Configurable time-to-live for cache entries
- **Sharded Design**: Better concurrency through sharding
- **Zero-Copy Operations**: Optimized for high throughput

### Configuration

Configure the object store cache via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `TIMEFUSION_FOYER_MEMORY_MB` | `256` | Memory cache size in MB |
| `TIMEFUSION_FOYER_DISK_GB` | `10` | Disk cache size in GB |
| `TIMEFUSION_FOYER_TTL_SECONDS` | `300` | TTL for cache entries (seconds) |
| `TIMEFUSION_FOYER_CACHE_DIR` | `/tmp/timefusion_cache` | Directory for disk cache |
| `TIMEFUSION_FOYER_SHARDS` | `8` | Number of shards for concurrency |
| `TIMEFUSION_FOYER_FILE_SIZE_MB` | `16` | File size for disk cache segments |
| `TIMEFUSION_FOYER_STATS` | `true` | Enable statistics logging |

### Cache Operations

- **GET**: Check cache first, fetch from S3 on miss, populate cache asynchronously
- **PUT**: Write to S3, then invalidate cache entry
- **DELETE**: Delete from S3, then remove from cache
- **LIST**: Pass-through to S3 (no caching)

### Performance Benefits

1. **Reduced S3 Costs**: Fewer API calls and data transfers
2. **Lower Latency**: Serve frequently accessed files from memory/disk
3. **Better Throughput**: Lock-free data structures and sharding
4. **Automatic Tiering**: Hot data in memory, warm data on disk

### Cache Statistics

The cache automatically logs statistics every 5 minutes:

```
Foyer hybrid cache stats - Hit rate: 85.2%, Hits: 1523, Misses: 265, TTL expirations: 12, Inner gets: 265, Inner puts: 145
```

The statistics show:
- **Hit rate**: Percentage of requests served from cache
- **Hits/Misses**: Cache hit and miss counts
- **TTL expirations**: Entries that expired due to age
- **Inner gets/puts**: Actual S3 operations (lower is better)

## Best Practices

### Memory Allocation

**Object Store Cache**: 
- Allocate based on working set size
- Typical: 256MB-2GB memory, 10GB-100GB disk
- Monitor hit rates to tune sizes
- Larger memory reduces S3 calls for hot data
- Disk tier handles warm data efficiently

### TTL Configuration

- **Real-time dashboards**: 60-300 seconds
- **Analytics reports**: 300-1800 seconds  
- **Historical data**: 1800-3600 seconds
- **Static reference data**: 3600+ seconds

### Cache Warming

For predictable workloads:
1. Pre-execute common queries on startup
2. Schedule periodic refresh of critical queries
3. Use longer TTLs for stable data

## Monitoring

Monitor cache effectiveness through:
- Log output showing hit rates and statistics
- Memory/disk usage metrics
- Query latency improvements
- S3 API call reduction

## Architecture Details

### Foyer Cache Implementation

The `FoyerObjectStoreCache` (`src/object_store_cache.rs`) provides:
- Implements `ObjectStore` trait for transparent integration
- Serializable cache entries with metadata
- Automatic TTL checking on access
- Graceful shutdown with cache persistence

### Cache Effectiveness

The cache is most effective for:
- Frequently accessed Parquet files
- Delta Lake metadata (_delta_log files)
- Repeated scans of the same partitions
- Dashboard queries accessing recent data

## Future Improvements

1. **Cache Invalidation**: Smarter invalidation on data writes
2. **Distributed Caching**: Multi-node cache coordination
3. **Predictive Prefetching**: ML-based prefetch for access patterns
4. **Compression**: Compress cached data to increase effective capacity
5. **Cache Metrics**: Prometheus/Grafana integration

## References

- [Foyer Documentation](https://foyer.rs)
- [Foyer GitHub](https://github.com/foyer-rs/foyer)
- [DataFusion Documentation](https://arrow.apache.org/datafusion/)