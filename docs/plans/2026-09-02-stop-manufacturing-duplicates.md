# Stop manufacturing our own duplicates — landed-batch identity at flush time

**Status:** design + implementation, 2026-09-02.
**Problem:** [`approaches-and-decisions.md` § WE MANUFACTURE THE DUPLICATES](approaches-and-decisions.md).
58% of duplicate groups in a real prod bin are byte-identical rows TimeFusion
re-inserted into itself. Dedup is ~96% of maintenance worker time, so a majority
of the fleet's maintenance budget is spent removing our own output.

## The mechanism, restated precisely

`prepare_flush` deliberately records a **conservative** WAL watermark in the
Delta commit — floored at every live hold, *including this flush's own*:

> *"An as-if-landed watermark was wrong when the commit went gen-dirty — a crash
> before the re-flush let derive skip inserts whose post-DML state only lived
> behind the cursor, silently reverting acked DML. The cost is re-replay + dedup
> of this commit's rows on a crash-mid-flush boot, which is the safe direction."*

That is correct and must not change. Combined with `settle_flushed_group`'s
benign failed-advance, the consequence is: **after an unclean exit, replay
re-inserts rows that are already in Delta.** The rows are byte-identical, which
is exactly what the prod evidence shows.

The chain that links the memory work to the maintenance work:

```
OOM kill -> unclean exit -> cursor not advanced -> replay re-inserts flushed rows
         -> duplicates -> dedup eats ~96% of maintenance -> backlog never drains
```

## The decision: skip at FLUSH time by content identity

Replay stays exactly as it is — every entry is replayed into MemBuffer, DML
replays against complete state, cursor semantics untouched. Instead, **the flush
declines to write a batch that is provably already committed.**

The property that makes this the right branch: **its failure direction is always
a duplicate, never a loss.** A hash miss means we flush rows that were already
there — today's behaviour exactly. There is no input for which this design drops
an acked row, which is the bar for anything on this path.

### Two designs considered and REJECTED

Both are recorded because they are the obvious "simplifications" a later session
would drift back into, and both are unsound.

**1. Replay-time skip by per-shard position threshold — UNSOUND, loses acked writes.**
Buckets interleave on a shard. Bucket A (10:00) holds entries at positions
100–200; open bucket B (10:10) holds 150–250. A's commit records max=200.
Entry 150 belongs to B, is *not* in Delta, and is ≤ 200 — a threshold skip drops
it. That is the 2026-07-08 acked-write-loss class. **A range is not an identity.**

**2. Replay-time skip by exact position SET — sound for rows, but trips the DML hazard.**
Skip a landed INSERT, then replay a later un-landed DELETE/UPDATE for the same
topic: the DML mutates a MemBuffer that is missing the skipped rows, and its
post-state never re-flushes. That is precisely the "silently reverting acked DML"
the gen-dirty comment warns about, re-entered through a new door. It is also
expensive — thousands of `(shard, block_id, offset)` triples per commit.

Content identity has neither failure mode: a DML changes the content, so the
hash changes, so the skip declines. **Sound by construction rather than by an
invariant someone must remember to maintain.**

## Prior art

- **Delta Lake's own `txn` action** (`appId`, `version`) is the protocol-blessed
  idempotent-write mechanism, and Spark's Delta sink uses exactly this shape:
  record a monotonic batch id with the commit, skip on replay when the committed
  version ≥ the batch being written. We do **not** use it: it wants one monotonic
  i64 per appId, which does not express per-topic `(shard, block_id, offset)`
  state, and it drags in checkpoint / `setTransactionRetentionDuration` surface.
  Our `commitInfo` watermark mechanism *is* this pattern, already built and
  already read at boot.
- **ClickHouse** dedups INSERTs by **block hash** over a window bounded by both
  count (`replicated_deduplication_window`) and time (`..._seconds`), with an
  explicit `insert_deduplication_token` override. The lesson is **granularity:
  one hash per BLOCK, not per row.** Our duplicates are whole-batch re-inserts,
  so batch identity is the cheap equivalent and needs no per-row filter.
  Our bounded window falls out for free: the boot history scan reads
  `delta_scan_depth` commits, which *is* `replicated_deduplication_window`.

Sources: <https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries>,
<https://github.com/delta-io/delta/blob/master/PROTOCOL.md>,
<https://www.vldb.org/pvldb/vol13/p3411-armbrust.pdf>

## Design

1. **Digest.** `landed_digest(&[RecordBatch]) -> [u8; 32]`: per-batch SHA-256 over
   the batch's Arrow IPC bytes, combined by **wrapping 256-bit addition**.
   - Commutative, so it is **immune to batch ORDER** — which matters because
     replay reads shard-by-shard while the original arrived interleaved across
     concurrent connections. Order genuinely differs; content does not.
   - Addition, not XOR: XOR cancels in pairs, so two identical batches would
     hash the same as zero batches.
   - SHA-256, not a 64-bit hash: a collision here writes nothing where it should
     have written rows, so the failure is silent loss. 64 bits is not enough.
2. **Record.** At commit, alongside `timefusion.wal_watermark`, write
   `timefusion.landed_digests`: the digests of the batch sets this commit
   contains, scoped per topic exactly like the watermark (same multi-topic shape,
   same backward-compatible parse).
3. **Load.** The boot history scan that already derives cursors also collects the
   landed digests into an in-memory set on the layer. Naturally window-bounded.
4. **Skip.** `flush_bucket` / `flush_groups_coalesced` hash `prepare_flush`'s
   output; on a hit, skip the Delta write **and the tantivy sidecar** (the
   original commit built both) and take `settle_flushed_group`'s success path
   unchanged — drain, release holds, advance the cursor. Count it.
5. **Self-insert.** A commit's own digests enter the live set when it lands, so
   an in-process re-flush of an identical batch set is also declined.

### The invariant, stated so it can be tested

> **The landed-digest set MUST NEVER feed `derive_wal_cursors_from_delta`.**

Cursor advance stays governed solely by the conservative watermark. The digest
set may only *decline a write whose rows are already durable*. If a future change
lets a digest advance a cursor, the interleaving counterexample above becomes
reachable and acked writes are lost. Pinned by
`landed_digests_never_advance_a_cursor`.

### What the skip does NOT cover, and why that is fine

The bucket that was mid-flush when the process died can contain the committed
entries *plus* new arrivals; its content differs, so the hash misses and it
flushes normally (duplicates, as today). The dominant case is unaffected:
10-minute buckets mean already-flushed buckets are *closed*, new data lands in a
different bucket, so a replayed old bucket reproduces exactly the committed batch
set and the skip fires.

## The load-bearing assumption, verified not assumed

**Is a replayed bucket's `prepare_flush` output byte-identical to the original's?**
This is the whole feature, so it is the failing test written first:

- `a_replayed_bucket_reflushes_to_the_same_digest` — flush, capture digest;
  replay the same WAL into a fresh layer; re-flush; assert the digest matches.

Determinism inputs checked while designing:
- `dedup_batches` preserves input batch order and boundaries (fast path returns
  the batches untouched; the slow path filters each batch in place). It does not
  concat or re-slice, so it cannot introduce a layout difference.
- Batch order across the bucket DOES differ between original and replay. Handled
  by commutativity, above.
- Ties in `dedup_batches` are order-sensitive (last-occurrence-wins). If two rows
  share key+tiebreak but differ in payload, the winner can differ, the digest
  differs, the skip declines. Safe direction.

## Tests

1. `a_replayed_bucket_reflushes_to_the_same_digest` — the feature's failing test.
2. `an_already_landed_batch_set_is_not_written_twice` — commit lands, cursor
   advance suppressed, replay, re-flush → zero new physical rows, all acked rows
   present. Row counts via the witness method, not `COUNT(*)` (count pushdown
   cannot probe this — `tf_count_star_is_wrong_not_flaky`).
3. `a_dml_mutated_bucket_still_flushes` — DML replays after refill, digest
   misses, flush proceeds, post-DML state is durable. Proves the skip **declines**
   when it must; this is the DML-hazard regression guard.
4. `landed_digests_never_advance_a_cursor` — the invariant above.
5. Digest unit tests: order-independence, and that duplicate batches do not
   cancel (the XOR trap).

## Rollout

One change, behind `TIMEFUSION_LANDED_SKIP_ENABLED` (default **false** until
staging proves it). The skip only fires after an **unclean** restart, which
cannot be induced in prod (the host is read-only) — so it is proven in staging,
where restarts are free, and prod confirms passively by
`flush_skipped_landed_total` rising with `wal.replay_rows`.

**Do not deploy into the open measurement window.** The stage-timeout concern
from earlier tonight (`840fd945`) is still unresolved and a deploy resets the
only process that can answer it.
