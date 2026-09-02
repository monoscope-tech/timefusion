# Morning brief — night of 2026-09-02

Everything is pushed to master and green (suite **1323/1323**, e2e **61/61**).
Prod is healthy on my build. **Nothing risky was shipped.** Three decisions are
waiting for you, ranked by risk.

The full evidence chain, including the wrong turns, is in
`2026-09-02-scale-readiness-10x-100x.md` and
`2026-09-02-stop-manufacturing-duplicates.md`.

---

## Decision 1 — Repair verifies ONE file per attempt (lowest risk, biggest waste)

**197 repair units, bulk-enqueued on 2026-08-16, have been claimed ~3,409 times
a day for 17 days and have never completed.**

Repair picks a file on the *absence* of a sort tag — only a suspicion. The unit
then reads the footer, finds the file already sorted, records it, and requeues.
Candidate selection is:

```rust
candidates.filter(|add| !self.repair_verified_sorted.contains(&add.path)).take(1)
```

`.take(1)` — **one file verified per claim.**

Three independent quantities agree, which is why I trust the mechanism:

| | |
| --- | --- |
| measured claim rate (two journal snapshots, 4.4h apart) | **142/hour ≈ 3,409/day**; 100% of the cohort gained attempts |
| elapsed x rate | 17 days x 3,409 = **57,953 files** |
| `repair_verified_sorted.txt` on the prod host | **12.76 MB ≈ 58,000 paths** |

These units are *small* (median 0.25 GiB), so no memory constant affects them —
they are admitted, they run, they simply yield one file each. **80% belong to
the whale**, so this grows with exactly the tenant profile the 100x customer
question is about.

The code comment predicts the outcome in as many words: *"could take days to
reach the file that actually poisons its scans — while looking busy the whole
time."*

- **Fix shape:** verify the whole candidate set per attempt instead of `take(1)`.
  The footer reads are independent and already cached.
- **Risk:** touches no memory constant and no durability path.
- **Status:** not implemented. It wants a reproducing test first — a repair task
  whose candidates are all pre-verified never completes.

---

## Decision 2 — Admission ceiling and the per-sort slice (must move together)

**Confirmed on live code**, not just a snapshot: re-fetching the journal 4.4
hours later, of 1,168 tasks stuck >48h, **1,144 (98%) were still queued**, only
24 drained, and 359 had attempts incremented.

Admission's ceiling is `MAX_DECODED_BYTES x (available/capacity)` clamped to
**[32 MiB, 512 MiB]**, against a **median unit of 316 MiB**:

| `MAX_DECODED_BYTES` | 100% free | 50% free | **25% free** | never-admissible |
| --- | --- | --- | --- | --- |
| **512 MiB (today)** | 86% | **33%** | **7%** | 14.1% |
| 2 GiB | 90% | 88% | **86%** | 10.5% |

At half occupancy only a third of the queue can be admitted. It is
*over*-guarded: admission capacity is 60 GiB, so today one unit may use **under
1%** of the pool it enters, and all 10 permits at maximum size use **8%** of it.

**But it cannot be raised alone.** Of the 338 sort-OOM failures (41 in the
*fresh* cohort, i.e. current behaviour), **every sized one is at or below today's
512 MiB ceiling, median 256 MiB** — a quarter-GiB unit already exhausts the
510 MB per-sort slice, because a sort's working set is a multiple of what it
decodes. Raising admission alone converts starved units into sort-failing units
on the OOM path.

- **Fix shape:** raise the admission ceiling **and** the per-sort slice together.
- **Risk:** two constants, OOM path, real interaction. Wants a canary.

---

## Decision 3 — Turn on `TIMEFUSION_LANDED_SKIP_ENABLED`?

Shipped, tested, **off**. It stops TimeFusion re-writing rows it already has:
58% of duplicate groups in a real prod bin are byte-identical rows our own WAL
replay re-inserted, and dedup is ~96% of maintenance time.

- **Failure direction:** a wrong answer produces a duplicate, never a lost row.
  The only path to a skip is a full 128-bit match against an identity a landed
  commit recorded.
- **Validated by:** an end-to-end test on real Delta + object storage that
  reproduces the duplicate the way prod makes it (a commit that lands while its
  cursor advance is lost). There is no staging service to prove it on.
- **Cost when on:** the digest is ~0.37x the parquet encode it avoids; in steady
  state (no dirty boot) it is a map lookup.

It interacts with Decision 2: with the flag off, any OOM-driven unclean restart
manufactures exactly these duplicates.

---

## Scale readiness

| load | result |
| --- | --- |
| **10x data per customer** | rollup backlog still drains, zero timeouts |
| 30x | drains, straining |
| 50x | needs ~2x the workers — a pool re-slice, not more memory |
| 100x | **more workers stop helping** — the timeout rate is a flat ~51% across an 8x worker range, because units do not fit their budget |

**More customers is a different axis from more data.** Maintenance arrival is
linear in *active* stream count and independent of data volume: 21 of our 124
streams are active, generating ~5,782 tasks/day. 10x customers means ~10x that,
regardless of how much each sends.

**The whale already dominates**: one project is 83.4% of queued bytes on 33% of
tasks, and 83.8% of all oversized units. We are running the 100x-customer
experiment in miniature today.

---

## What shipped

| area | change |
| --- | --- |
| duplicates | landed-batch skip (off by default) + `wal.landed_skips` counters |
| hashing | XXH3-128 for the digest (**17x** the hash vs blake3); XXH3 for in-process hashing incl. `query_fingerprint`, which ran SHA-256 on **every query** |
| hashing safety | six persisted hashes marked `FROZEN HASH` — a blanket sweep would have orphaned rollup history and reset certification coverage |
| observability | split `dedup_probe_timeouts_total` out of `dedup_bin_stage_timeouts_total` |

## Ruled out by measurement — do not re-litigate

- **Raise `MAX_DECODED_BYTES` for fusion**: buys **0.7%**. Its *admission* effect
  is separate and real (Decision 2); the sim cannot test admission — *"Memory
  admission … outside the model."*
- **Coarsen mint granularity**: real arrival is 6x lower than the sim modelled.
- **Restart cadence**: within noise.
- **The "climbing dedup timeouts" alarm**: those were the cheap probe. Zero real
  staging timeouts in an hour of logs.

## Method notes worth keeping

- **`synth:whale` models ZERO ingest arrivals** and flattered us; the real-journal
  default models **6x too many** and alarmed us. Validate any keep-up claim's
  arrival rate against `created_unix_ms` first.
- **`resource_admission` / `admission_busy` are never logged** — they are retry
  reasons stored on the task. Grepping logs for them proves nothing; diff the
  journal by task key instead.
- **`attempts` is reset, not cumulative.** Reading it as cumulative made a
  plentiful lane look starved.
- The journal is **not** on the host disk or in S3; it is inside the container at
  `/app/data/timefusion/.timefusion_meta/maintenance_tasks.json`.
