# Local hot tier: sub-500ms recent-window reads over R2 Delta Lake

Status: research/design (2026-07-31). Motivated by the latency matrix: every 1h/3h
dashboard query is 0.6–9s; warm plans show `time_elapsed_scanning_until_data` ≈ 2–7s
opening 50–325 hot-tail parquet files from R2 while scanning only ~3MB. The 3h query
on the largest tenant cannot be run raw at all (contributed to the 02:40Z wedge/restart).

## What the industry does (research summary)

Closest-stack precedent is **InfluxDB 3** (Rust + DataFusion + Parquet + object store):

- Queryable in-memory Arrow buffer for the last ~5–15 min, unioned with parquet — same
  shape as our MemBuffer ∪ Delta.
- **`MemCachedObjectStore` + `ParquetCacheOracle`** (influxdb PR #25377): at persist
  time, the just-written parquet bytes are inserted into the object-store byte cache,
  and the memory buffer is **not evicted until the cache confirms population** — so a
  recent-window query never pays an object-store round trip. Enterprise adds a
  **recency admission policy**: only cache files whose data is in the last N hours
  (default 5h).
- Their "72h limit" is literally a 432-parquet-file query cap — file count, not bytes,
  is the recognized wall. Compaction is what removes it (Enterprise).
- Purpose-built write-path caches (Last Value / Distinct Value) serve the two dominant
  dashboard shapes as SQL table functions, bypassing scan entirely.

Other systems, one line each:

- **Druid**: historicals pull whole encoded segments to NVMe and mmap them (page cache
  = the implicit LRU); tier membership by age rules; realtime task keeps serving until
  the historical confirms download (no handoff cliff). Restart is warm: rescan the
  cache dir, no re-download.
- **ClickHouse**: NVMe file-range cache over S3 MergeTree, opt-in
  `cache_on_write_operations` (write-through for inserts + merge outputs), SLRU +
  big-scan bypass so wide historical reads can't flush the hot window. Cache survives
  restart.
- **QuestDB**: the hot tier IS the database — raw uncompressed mmap'd columns locally;
  age-based demotion of whole partitions to Parquet/S3 (`STORAGE POLICY (TO PARQUET
  3d, DROP LOCAL 1M)`).
- **Mimir/Thanos**: explicit time-boundary routing (`query-ingesters-within` /
  `query-store-after`) with deliberate ~3h overlap; store-gateway mmaps tiny persisted
  index-headers, caches 16KB encoded chunk subranges; Mimir 2.10 snapshots the
  working-set list and eagerly reloads it on restart.

Cross-cutting patterns:

1. **Write-through at flush beats read-miss** — Druid handoff, ClickHouse
   cache_on_write, Influx oracle. The handoff cliff (fresh files never in cache) is
   exactly our measured problem: warm reruns stayed slow because hot-tail files are
   new every 10 min.
2. **Cache encoded immutable bytes** at scale; keep decoded structures only in small
   purpose-built caches. Exception that matches us: QuestDB's hot tier is decoded raw
   columns — mmap makes decoded data page-cache-evictable ("RAM when free, disk when
   pressured").
3. **Tier membership by age; within-cache eviction by scan-resistant LRU + bypass.**
4. **Restart warmth**: persistent on-disk cache + rescan on boot (Druid/ClickHouse),
   or persist the working-set list (Mimir).

## Design for TimeFusion (layered, independently shippable)

### P0 — Foyer write-through at flush (Influx oracle pattern)

At `flush_bucket`/commit time we hold the parquet bytes we just uploaded. Insert them
into Foyer keyed by their object-store path **before** the bucket drains (the drain
already happens post-commit via `settle_flushed_group`, so ordering is natural).
Also write-through hot-tail compaction outputs (light_optimize rewrites today's files
— those are the files dashboards read next).

Add: recency admission (only admit files whose data ∈ last N hours, default 6h) and a
large-scan bypass so a 14d scan can't evict the hot tail. Foyer's admission/weighter
hooks cover both.

Expected effect: removes the R2 first-byte × file-count term (~2–7s) from warm-path
queries. Does NOT fix decode heap, single-threaded dedup, or file-count plan overhead.

### P1 — mmap Arrow IPC third leg ("demote, don't drop")

Eviction currently drops sealed buckets at `retention_mins` (70 min). Instead, demote:
write each drained bucket as an **uncompressed Arrow IPC file** on NVMe
(`{data_dir}/hot_tier/{project}/{table}/{bucket_id}.arrow`, tmp+fsync+rename), and
serve a third plan leg from these via zero-copy mmap (`memmap2` → `Bytes::from_owner`
→ `Buffer` → `arrow_ipc::FileDecoder`, arrow-rs PR #6986 pattern; 64B alignment
default holds; `with_require_alignment(true)` in tests). Uncompressed is mandatory —
IPC buffer compression breaks zero-copy.

Read path: extend the `database.rs` scan union (mem_plan ∪ delta_plan, range exclusion
via `get_bucket_ranges`) to mem ∪ hot_tier ∪ delta. Coverage contract: each window
served by exactly one tier (MemBuffer → IPC → Delta), reusing the existing timestamp
range-exclusion mechanism — no new dedup logic; DedupExec still runs above the union.

Lifecycle: age-based unlink past the hot window (start 6h); open mmaps keep in-flight
queries safe. Restart: rescan the dir, validate footers (`ARROW1` magic — torn file =
treat as absent, fall through to Delta), instantly warm — kills the post-restart
latency cliff that made today's measurements worst-case. Own root dir, name-filtered
GC (lesson of ba8820e: never a generic recursive deleter).

Why decoded-IPC here despite the industry preferring encoded bytes: (a) parquet decode
heap is a proven OOM vector on this box (145MB/batch peaks, GatedScan exists because
of it) — mmap'd IPC converts decode heap into reclaimable page cache and near-zero
CPU; (b) we write the data ourselves, units are immutable sealed buckets, so coherence
is trivial; (c) QuestDB precedent: uncompressed IPC ≈ their native columns.

Sizing: uncompressed IPC ≈ MemBuffer's `get_array_memory_size` numbers. 6h across all
tenants ≈ a few × MemBuffer's steady state — NVMe-cheap. cgroup v2 gotcha: mapped file
pages count toward `memory.max` but are clean/reclaimable — pressure becomes refaults,
not OOM kills. Track via `memory.stat` (`file_mapped`, `workingset_refault`); the RSS
governor won't see this tier.

### P2 — parallelize DedupExec

Hash-partition by dedup key (or partition-local dedup over piecewise-sorted runs)
instead of `CoalescePartitionsExec` → single-threaded dedup. The pure-MemBuffer 1h
query on the big tenant was 750ms; this is its only fix.

### Non-goals here

Rollups/sketch pre-aggregation (separate plan — it's the only thing that makes the
big tenant's 3h+ windows safe at any tier) and hot-tail compaction reliability
(maintenance OOM work).

## Sources

InfluxDB 3: influxdata/influxdb PR #25377 (MemCachedObjectStore + oracle), issue
#25395, durability internals docs, 3.0 system architecture blog. arrow-rs: PR #6986 +
`arrow/examples/zero_copy_ipc.rs`, `FileDecoder` docs. Druid historical/segment-cache
docs, PR #6988 (lazyLoadOnStart). ClickHouse storing-data docs, PR #75072 (SLRU),
cache_on_write_operations. QuestDB storage-engine docs + Enterprise 3.3.1 storage
policies. Mimir store-gateway/ingester docs, 2.10 eager-loading. Feather V2 benchmarks
(ursalabs.org/blog/2020-feather-v2). cgroup v2 page-cache accounting:
biriukov.dev/docs/page-cache/6-cgroup-v2-and-page-cache.
