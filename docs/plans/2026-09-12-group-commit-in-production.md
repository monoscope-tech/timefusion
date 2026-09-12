# Group commit in production: what it bought, and what it exposed

2026-09-12. Reviewed against a 2,640 s (44 min) process running `1b391890` +
`16e925af` + PR #263, sampled from `timefusion_stats`.

## Verdict: it works, and the remaining cost is now visible

| metric | before (comparable age) | now |
|---|---:|---:|
| `journal_hold.avg_us` | 5,452 | **3,093** |
| `journal_hold` duty cycle | 18.5% | **14.5%** |
| `journal_lock_wait.avg_us` | 1,907 | 4,017 |
| `journal_stats_publishes_total` | n/a | **0.80/s** (target ~1/s) |

Two separate wins are mixed in here and should not be conflated. PR #263 removed
the full-journal rescan from every checkpoint — `journal_stats_publishes` at
0.80/s against a design target of ~1/s says that is working exactly as intended.
Group commit moved the `fsync` **out from under the global mutex**, which is what
the duty-cycle drop measures.

The comparison uses the 18.5%-at-34-min reading, not the 29.6% from the 19 h
process in the disk-flood doc. A mature process is not the comparator for a
44-minute one.

## The cost did not disappear; it became measurable

Group commit's own instrumentation is where the remaining cost now shows:

| metric | value |
|---|---:|
| `journal_commits` | 30,075 (**11.4/s**) |
| `journal_commits_coalesced` | 30,506 |
| `block.journal_commit_wait.count` | 60,581 |
| `block.journal_commit_wait.avg_us` | **361,837 (362 ms)** |
| `block.journal_commit_wait.max_ms` | **10,687** |

`performed + coalesced = 60,581` exactly, so every waiter is accounted for.

**The coalescing ratio of ~2x is optimal, not disappointing.** 11.4 fsync cycles
per second is one per 88 ms; at 23 arrivals/s that is ~2 arrivals per cycle, and
a group can only contain what arrived before its leader read `covered`. Batching
harder buys nothing — the batch is already the whole arrival window.

**The pipeline is at ~100% duty.** 11.4 commits/s × 88 ms = the entire second.
Every arrival therefore queues, which is what a 362 ms average with a 10.7 s
maximum means — and it is paid on the **pre-ack path**, so it is client latency.

This is the correct diagnosis to carry forward: the lever is **cost per commit**,
not batch size.

## Three fsyncs per commit, two of them optional

A commit runs `TaskJournal::checkpoint` — one `sync_all` on the task WAL — and
then `persist_rollup_journal`, which calls `write_atomic_with(durable = true)`:
`sync_all` on the temp file, then `sync_all` on the parent directory after the
rename. **Two of the three are the rollup journal.**

Only the task journal belongs in the pre-ack barrier:

- `rollup_journal`'s module doc: *"scheduling state, not the read-side
  correctness boundary … an absent dirty entry already means 'full rebuild
  required' to the builder."*
- `maintenance_tasks` is documented as *"the finer-grained source of truth
  coordinator workers consume"*, and is fsynced in the **same** commit.
- `rollup_dirty` has exactly one reader: requeueing partitions when bootstrap
  tasks were *discarded*.

So a lost second of the rollup journal weakens a backup whose primary is durable,
in the conservative direction. PR #265 throttles its write to once per second and
forces one at shutdown.

**A content hash alone would have shipped inert.** `apply_rollup_hours`
increments the source epoch on *every* call, so the encoded journal genuinely
changes on every ingest invalidation. The hash covers the idle case only; the
time window is what covers the busy one. This was caught by the test failing
(13 writes where 1 was expected), not by reading the code.

## Client latency, with the caveat stated

| pgwire | 19 h process, pre-group-commit | 44 min, post |
|---|---:|---:|
| p50 | 1.0 s | **0.21 s** |
| p95 | 9.0 s | **3.5 s** |
| p99 | 47.9 s | **7.8 s** |
| p999 | 184.8 s | **16.8 s** |

Directionally large and consistent with the fix, but **this is a 44-minute
process against a 19-hour one** and the journal, the queue and the caches all
differ. Treat as encouraging, not as the measurement. The number to re-read on a
mature process is `journal_commit_wait.avg_us`.

## New findings

**1. The deploy pipeline strands merged code — confirmed live, not historical.**
#262 merged 09:48Z, its `Build and Deploy` went green at 10:08:55Z having built
`be9722a5`, and the log ends `A newer master commit superseded this rollout;
production is unchanged.` The superseding commits were two **docs-only** pushes
at 09:56Z and 10:01Z, which start no rollout of their own. `docker service
inspect` reported `UpdatedAt=09:39:25Z` — merged code stranded for over an hour,
with a green job and nothing reporting the gap. This is the second observed
instance (the first was #251 on 09-10). **PR #258 fixes it and had been sitting
open since 09-10; merged today.**

**2. Journal history grows without bound.** `tasks_complete` read 71,399 on
09-11 and 78,907 today — nothing prunes Complete tasks. `compact()` rewrites the
whole snapshot (48 MB on 09-11) under the global mutex at a size that tracks
history rather than load, which is where `journal_hold.max_ms` spikes of 3.5 s
come from. Bounded retention for Complete history is the fix; the audit value of
a two-week-old completed task is low against a cost paid on every commit.

**3. The no-op rollup skip is being defeated by restart frequency.**
`rollup_noop_rebuild_skipped_total` read **0** on a 31-minute process and 38 on a
44-minute one, because boot-recovered coverage carries `content_fp: None`.
Production restarted **three times in two and a half hours** today. The
content-fingerprint tag follow-up in
`2026-09-11-noop-skip-production-result.md` is worth more than it looked when
written: it converts the skip from "effective after ~35 min of uptime" to
"effective immediately", and uptime is the scarce resource.

## Not re-derived here

The write-volume levers — tantivy `verify_blob` unpacking each index a second
time, and the unattributed DataFusion spill — already have a fix order in
`2026-09-11-write-latency-self-inflicted-disk-flood.md`. They remain the reason
an `fsync` costs 88 ms in the first place, and PR #265 only reduces how many of
them a client waits for.
