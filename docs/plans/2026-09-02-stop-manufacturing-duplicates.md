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

## The load-bearing assumption — verified, and it FAILED

**Is a replayed bucket's `prepare_flush` output byte-identical to the original's?**
This is the whole feature, so it was the failing test written first —
`a_replayed_bucket_reflushes_to_the_same_digest`, which flushes, captures the
digest, replays the same WAL into a fresh layer, re-flushes, and compares.

**It failed on the first run.** The naive digest — SHA-256 over each batch's
current IPC encoding — did not match. Had this been assumed rather than tested,
the feature would have shipped completely inert: every check a miss, every
duplicate still written, and nothing in the metrics to say why.

The cause, measured on a one-row otel span:

| | IPC bytes |
| --- | --- |
| client-supplied batch | 28,168 |
| after one WAL round-trip | 26,888 |
| after a second round-trip | 26,888 |

The client's arrays carry slack (view buffers, over-allocated offsets) that the
round-trip drops, and **the round-trip is a fixed point after one pass.**
Replayed batches are already at that fixed point; client batches are not.

So `landed_digest` hashes the **round-trip fixed point**, not the current
encoding, which puts both paths on the same footing. The cost is one extra
IPC round-trip per flush, paid only when the feature is enabled.

An incidental finding worth its own look later: buffered batches are ~4.5%
larger than they need to be. Canonicalising at insert would reclaim that
MemBuffer memory *and* make the digest a single serialize — but it touches the
hot ingest path, so it is not part of this change.

Determinism inputs checked while designing:
- `dedup_batches` preserves input batch order and boundaries (fast path returns
  the batches untouched; the slow path filters each batch in place). It does not
  concat or re-slice, so it cannot introduce a layout difference.
- Batch order across the bucket DOES differ between original and replay. Handled
  by commutativity, above.
- Ties in `dedup_batches` are order-sensitive (last-occurrence-wins). If two rows
  share key+tiebreak but differ in payload, the winner can differ, the digest
  differs, the skip declines. Safe direction.

## Tests (all written, all passing)

| test | what it pins |
| --- | --- |
| `a_replayed_bucket_reflushes_to_the_same_digest` | the load-bearing assumption; caught the round-trip bug |
| `an_already_landed_batch_set_is_declined_but_a_changed_one_still_flushes` | both directions: the skip fires on identical content, and **declines** for a DML-changed bucket — the DML-hazard guard |
| `landed_digests_never_advance_a_cursor` | the invariant: a commit carrying only digests advances nothing |
| `landed_digests_roundtrip_and_stay_scoped_to_their_topic` | on-disk format, per-tenant scoping, and the `with_metadata` clobber trap |
| `landed_digest_is_order_independent_and_never_cancels` | commutativity, and the XOR trap |
| `replayed_rows_that_delta_already_holds_are_not_written_again` (e2e) | **the seam** — real Delta, real object storage, the whole chain |

The DML case is worth stating as a property rather than an example: the skip is
**content**-keyed, so anything that changes what the bucket would write —
a DML, a late arrival, a different dedup winner — changes the identity and the
skip declines. There is no state to keep in sync for that to hold.

## Rollout

One change, behind `TIMEFUSION_LANDED_SKIP_ENABLED` (default **false** until
staging proves it). The skip only fires after an **unclean** restart, which
cannot be induced in prod (the host is read-only) — so it is proven in staging,
where restarts are free, and prod confirms passively by
`flush_skipped_landed_total` rising with `wal.replay_rows`.

**Do not deploy into the open measurement window.** The stage-timeout concern
from earlier tonight (`840fd945`) is still unresolved and a deploy resets the
only process that can answer it.

## What the end-to-end test found that nothing else could

Three defects, each of which alone made the feature completely inert while
every unit test still passed. All three lived in seams between components that
were individually correct.

1. **The identity was hashed in the wrong encoding.** A client batch and the
   same batch rebuilt from the WAL serialize to different bytes; the round-trip
   is a fixed point after one pass. Caught by the determinism test, before any
   wiring existed. See above.
2. **The install found no layer.** `bootstrap()` attaches the buffered layer to
   `Database` with `with_buffered_layer` *after* it calls
   `derive_wal_cursors_from_delta`, so `self.buffered_layer()` inside the derive
   was always `None`. The layer is now passed in explicitly rather than reached
   for through `self`.
3. **The scan that loads identities does not run on a clean boot.**
   `skip_delta_scan` short-circuits the whole Delta history scan when the cursor
   snapshot is clean or the local WAL is fully consumed. This is correct — a
   boot with nothing to replay manufactures no duplicates and needs no
   identities — but it means the feature is, by construction, **only ever active
   on a dirty boot.** Worth stating plainly, because it also bounds the cost: a
   clean deploy pays nothing.

## A property that fell out, and it is a good one

A client **cannot spoof a landed identity.** `otel_logs_and_spans` is
`version_append`, so `stamp_version` overwrites `updated_at` on every *inbound*
write — a client re-sending byte-identical rows gets a new stamp, so different
content, so no match, so its write proceeds. Only WAL replay preserves the
durable stamp (`observe_stamp`). The skip therefore fires on replay duplicates
and is structurally unable to fire on a client retry, which is the exact
targeting we wanted and did not have to build.

The corollary shaped the test: the duplicate cannot be produced over pgwire at
all. `set_drop_cursor_advance_for_test` produces it the way prod does — a Delta
commit that lands while the cursor advance that should follow it is lost, the
case `settle_flushed_group` documents as benign.

## Status

Implemented, tested, pushed; **the flag is off**, so prod behaviour is
unchanged (`TIMEFUSION_LANDED_SKIP_ENABLED=false`).

**The staging step in the rollout above is not available:** `docker service ls`
on the CapRover host shows no `timefusion-staging` service, so the ladder's
step 3 does not exist yet (as `approaches-and-decisions.md` anticipated —
building it is Phase 0 of the architecture plan).

That leaves the e2e seam test as the validation of record, which is stronger
than it sounds: real Delta, real object storage, and the duplicate produced the
way prod produces it (a commit that lands while its cursor advance is lost).
Turning the flag on in prod is a judgement call for a human, and the failure
direction if it is wrong is a duplicate, not a loss.

## The cost, measured — and the claim that had to be withdrawn first

The original design note asserted the digest's cost was "irrelevant next to the
parquet encode it guards." That was an assertion, not a measurement, and it was
wrong. The measured sequence:

| | digest | parquet encode | ratio |
| --- | --- | --- | --- |
| debug build (first look) | 39.6 ms | 7.6 ms | **5.2x** |
| release, SHA-256, 200k rows | 62.1 ms (369 MiB/s) | 20.5 ms (1.09 GiB/s) | **3.0x** |
| release, blake3, 200k rows | **17.3 ms (1.29 GiB/s)** | 20.5 ms | **0.84x** |

Three things worth keeping:

1. **The debug ratio was meaningless and nearly caused the wrong fix.** SHA-256
   runs far under its release speed unoptimized, so the debug number (5.2x)
   overstated a real 3.0x. Decomposing rather than trusting the total is what
   showed the hash was **83%** of the digest and the three Arrow passes only 17%.
2. **blake3 cut the whole digest by 72%** — same 256 bits, SIMD, and already in
   `Cargo.lock` as a transitive dependency, so it costs nothing to adopt.
3. **The digest is now cheaper than the work it avoids** (0.84x), which is what
   the original claim asserted without evidence. It is now a bench
   (`landed_digest` in `dedup_benchmarks`), so a future change that makes it
   expensive again shows up as a regression rather than as a surprise in prod.

Steady state is cheaper than any of these: the check short-circuits on an empty
identity set, which without a dirty boot it always is.

**A digest-format change needs no version gate.** A stale digest simply never
matches, and a non-match flushes — the safe direction — so old records age out
of the window harmlessly.
