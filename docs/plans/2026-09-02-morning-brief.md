# Morning brief — 2026-09-02 night

Everything below is pushed and green (suite **1323/1323**, e2e **61/61**).
Prod is running my build and healthy. **One decision is left for you.**

## The one decision

**Turn on `TIMEFUSION_LANDED_SKIP_ENABLED`?** It is shipped, tested, and OFF.

- **What it does:** stops TimeFusion re-writing rows it already has. 58% of the
  duplicate groups in a real prod bin are byte-identical rows our own WAL replay
  re-inserted after an unclean exit, and dedup is ~96% of maintenance time — so
  a majority of our maintenance budget is spent removing our own output.
- **Failure direction:** a wrong answer produces a duplicate, never a lost row.
  Every uncertainty returns "flush it"; the only path to a skip is a full
  128-bit match against an identity a landed commit recorded.
- **Validated by:** an end-to-end test on real Delta + object storage that
  reproduces the duplicate the way prod makes it (a commit that lands while its
  cursor advance is lost). Staging would have been the place to prove it, but
  there is no `timefusion-staging` service on the host.
- **Why I did not flip it:** it is the durability path, the area with the worst
  incident history in the repo, and I said in the plan that this call is yours.
  Nothing I learned since changed that.
- **Cost when on:** the identity digest is ~0.37x the parquet encode it avoids,
  and in steady state (no dirty boot) it is a map lookup.

Detail: `docs/plans/2026-09-02-stop-manufacturing-duplicates.md`.

## Scale readiness — measured, not asserted

`timefusion sim synth:whale`, 24h virtual, prod's pinned 10 workers, 4 seeds:

| load | result |
| --- | --- |
| **10x** | **keeps up** — queue fully drains, zero timeouts, lag 0 → 1h |
| 30x | keeps up, straining (138 timeouts) |
| 50x | **does not drain** at 10 workers; drains at 20 |
| 100x | does not drain at ANY worker count tried (up to 160) |

**The constraint changes character with scale, and that is the finding:**

- **50x is concurrency-bound** — doubling workers fixes it outright (4/4 seeds).
  But `HEAVY_REWRITE_PERMITS` is pinned at 10 *specifically* to bound rewrite
  OOM, and peak heap ≈ `block_size × permits`. **So 50x is a memory problem, not
  a scheduler problem** — and it is the same ceiling whose OOM restarts
  manufacture the duplicates above. The memory work and the throughput work are
  one problem; this is the measurement that shows it.
- **100x is deadline-bound** — 8x the workers moves pending only ~210 → ~60 and
  never reaches zero, while timeouts *rise* (2036 → 2758). Units must be **sized
  up front** to fit their budget rather than split after they fail. I verified
  this is not the split guard: turning the guard off doubles the residue.

Detail: `docs/plans/2026-09-02-scale-readiness-10x-100x.md`.

## What shipped tonight

| area | change |
| --- | --- |
| duplicates | the landed-batch skip (off by default) + `wal.landed_skips` counters |
| hashing | XXH3-128 for the digest (**17x** the hash vs blake3); XXH3 for in-process hashing incl. `query_fingerprint`, which was running SHA-256 on **every query** |
| hashing safety | six persisted hashes marked `FROZEN HASH` — a blanket sweep would have orphaned rollup history and reset certification coverage |
| observability | split `dedup_probe_timeouts_total` out of `dedup_bin_stage_timeouts_total` |

## Three things I got wrong, and corrected

Recorded because the corrections are more useful than the conclusions.

1. **"80% of dedup work is timing out."** Wrong. One counter was incremented
   from two sites meaning opposite things. Prod logs: **114 cheap probe
   timeouts, ZERO real staging timeouts.** Dedup's expensive path is healthy,
   and its probe backlog is draining (61 → 7 groups per phase over an hour).
2. **"The digest cost is irrelevant next to the parquet encode."** Asserted, not
   measured; it was 3x the encode. Decomposing showed the hash was 83% of it —
   which is what led to blake3 and then to XXH3-128 at your suggestion, and the
   claim is now true by measurement rather than by assertion.
3. **"The canary fails on baseline."** It does not — a fully green 1323/1323 run
   settled it. It is load-flaky, not broken. Three different tests failed once
   each across runs and passed in isolation; one SIGSEGV I chased was a release
   bench compile starving the machine, not a code fault.

## Where I would go next

1. **Your call on the flag.** Everything else about duplicates is inert until it.
2. **50x = memory headroom per rewrite.** The lever is not the scheduler. The
   question to answer is how much `block_size × permits` headroom exists at the
   current cgroup limit, and whether the landed skip's reduction in work moves it.
3. **100x = predictive unit sizing.** A byte preflight already exists (the sim
   exercises it); the design question is whether it can *size* a unit rather
   than only refuse one.
4. **Get a real prod journal into the sim.** Tonight's runs are rollup-shaped
   (`synth:whale`), so they measure the rollup path only. The journal lives in
   object storage, not on the host's disk, so it needs credentials I did not use
   under the read-only constraint.
