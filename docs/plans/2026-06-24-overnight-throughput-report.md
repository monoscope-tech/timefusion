# Overnight session report — compaction + throughput toward 10000/s (2026-06-24)

Autonomous run while you slept. **Honest headline: 10000/s is NOT reachable by safe
unattended TimeFusion-only changes** — the two dominant levers are a CapRover **config**
change and a **monoscope** (different repo) change, both needing your decision. I made the
safe progress I could, stopped one approach that was actively harming prod, and benchmarked
so this is data-driven. Details below.

## What I did

1. **Shipped on-demand compaction tooling** (committed to `master`, deployed as `54b7752`):
   - pgwire `OPTIMIZE <table> WHERE date = '...'` (server-side, reliable in-region I/O).
   - CLI `timefusion optimize [--date|--older-than-hours|--all|--dry-run]`.
   - Deploy was clean — prod booted in **75s**, healthy.
2. **Proved the laptop CLI can't compact prod** — reads stall and large parquet **uploads to
   R2 fail** (`error sending request`) from a home connection. Server-side pgwire is the only
   reliable path.
3. **Tried a full date sweep, then killed it** — see "What didn't work".
4. **Committed an OCC-retry fix** to branch `perf/compact-occ-retry` (NOT master, so no
   auto-deploy/restart while prod is stable). Ready for you to review + deploy.
5. **Benchmarked** ingest + drain (numbers below).

## What didn't work (and why it matters)

**Compaction of the file-heavy dates is not viable while the backlog is active.** Every
recent-old date (`06-18/19/20/21`) failed with a Serializable OCC conflict:
`"a concurrent transaction deleted data this operation read."` Cause: the 7.6M DLQ backlog
carries **old event-time** data, which lands in those same recent-old `date=` partitions, so
a concurrent flush/dedup deletes files mid-merge (each merge is ~4–5 min). Worse, the
in-process compaction **contended with the drain on `delta_commit_lock` and pushed prod
pressure to 100%**, causing insert rejects. I killed it — it compacted **nothing** and hurt
ingest. The OCC-retry fix (branch) helps *intermittently*-written dates but won't save a
*continuously*-written one. **Compaction only pays off on sealed older dates, or after the
backlog drains.**

## Benchmark (prod `54b7752`, steady state)

| Metric | Value | Note |
|---|---|---|
| Ingest arrival rate | **~615–1080 rows/s** | varies with upstream load; clean rising-segment measure |
| Drain ceiling (burst) | **~3000–3300 rows/s** | a flush cleared 294k rows in 90s earlier |
| Pressure at moderate load | sawtooths 20–35% | drain keeps up |
| Pressure under spike / compaction | → 100% → rejects | drain can't keep up at peak |
| Uptime since deploy | 40+ min, **no OOM restart** | healthier than `00c1136` (OOM ~hourly) — but load is low (night) |
| `backpressure_rejected_total` | 56 (mostly during the sweep) | |

**The ceiling is the DRAIN rate (~3000/s), not ingest accept.** Sustained throughput can't
exceed drain; when arrival spikes past it, the buffer hits 100% and TF rejects. (`force_flush`
correctly self-gates while completed buckets remain — that's a WAL-ordering invariant, not a
bug. Rejects mean completed buckets aren't draining fast enough, i.e. drain-bound.)

## Path to 10000/s — ranked, with owner

| # | Lever | Owner | Impact | Notes |
|---|---|---|---|---|
| 1 | **Container mem limit 66→32 GB** + explicit `BUFFER_MAX_MEMORY_MB=8000`, `FOYER_DISK_GB=60`, `MEMORY_LIMIT_GB=8` | **You (CapRover)** | Stops OOM restart loop → the DLQ-flood source | autotune sizes pools to ~48 GB off the 66 GB cgroup while the shared host backs only ~42 GB → host `global_oom`. Code can't see host-available RAM; this is the real fix. ~2-min config change. |
| 2 | **monoscope TF batch: 512 → 4–8k rows** (inlined-literal encoder, escape the libpq 65535-param cap) | **You/me (monoscope repo)** | Biggest *ingest* lever — fewer parses/WAL-appends/commits, higher drain rows/commit | The 512 cap is a PostgreSQL client artifact, not a TF limit. Different repo + Haskell — needs review, not an unattended change. |
| 3 | **TF drain throughput** — larger coalescing + parallel staged commits (parquet upload parallel, one short serialized commit) | TF code | Raises the ~3000/s ceiling | Deeper hot-path change; wants careful testing, not unattended. |
| 4 | **Compaction of sealed dates** (branch `perf/compact-occ-retry`) | TF code (ready) | File count ↓ → query memory ↓ → fewer OOMs | Only after backlog drains, or on dates >1 week old. |

**Realistic read:** #1 (config) + #2 (monoscope batches) together are what get you toward
10000/s. #1 stops the crashloop so the drain runs continuously; #2 raises both ingest and
drain rows/commit. TF-only changes (#3) can lift the ~3000/s ceiling but not to 10000/s alone.

## Ready to deploy (your call)

- **`master` is at `54b7752`** (deployed) — OPTIMIZE tooling. Stable.
- **Branch `perf/compact-occ-retry`** — OCC-retry for compaction. Safe, additive, tested
  (compiles). Merge when you want to compact sealed dates. Not urgent.
- **NOT done (needs your decision):** the autotune memory-fraction reduction. I chose not to
  deploy a memory-tuning change unattended because (a) prod isn't OOMing right now so I
  couldn't validate it, and (b) the real fix is the container limit (config). If you'd rather
  fix it in code than CapRover, I can lower the autotune fractions so the pool sum fits under
  host RAM — say the word.

## ⚠️ URGENT — overnight development (03:28 CEST / 01:28 UTC)

**Prod re-entered the OOM/wedge loop and is actively dropping inserts to the DLQ.**

- One OOM restart ~01:30 UTC (task `hmsa4ktqerxy` → `6xae9b0wdy3x`, `queries_total` reset).
- Now **wedged at 100% pressure**, `backpressure_rejected_total` climbing fast
  (158 → 560+ and rising). Logs: `Write backpressure exhausted after 60s: used=9254MB still
  over hard limit — Delta flush is not freeing memory; rejecting (data remains in WAL)`.
- The drain **is** committing (18 commits/6min) but **can't free memory**: `total_rows` is
  flat at ~1.217M and inserts are mostly rejected, yet the buffer stays pinned at the
  9254 MB hard limit. → memory is held by **buckets that never drain** — the lingering
  ~99h-old event-time buckets (`oldest_bucket_age_secs=356692`). This is the "lingering
  buckets / stuck drain" bug (2026-06-22 root-cause #3), now causing a hard wedge.
- **Not data loss / not a full outage:** rejected inserts remain in the WAL and replay
  later. It's degraded — DLQ growing, insert latency bad.

**Why I did not act autonomously:** the lingering-bucket drain bug needs real investigation
(unsafe to fix blind at 3am); a forced restart might clear the wedge *or* re-wedge on replay
(50/50) and the host is restart-locked. Deploying the memory fix wouldn't unstick *this*
wedge (it's stuck buckets, not pool sizing) and would add another replay cycle.

**Morning actions (in order):**
1. **Apply the container-limit + caps config** (table row #1) — gives headroom so the wedge
   is rarer, and bounds the OOM.
2. **Clear the current wedge:** a controlled restart *after* confirming the lingering buckets
   are committed to Delta (check WAL cursor vs Delta watermark) — else it re-wedges.
3. **Fix the lingering-bucket drain** (why a 99h completed bucket never flushes) — this is
   the real bug behind both the wedge and the OOM. Needs a debug build / the stuck-bucket
   stat from the 2026-06-22 P2 observability list.

## What I'm doing until you wake

Tightened to ~20-min health checks to build a complete timeline of the wedge (OOM restarts,
pressure, reject rate) for the morning. **No more prod-mutating changes** — the situation
needs your decisions, not an unattended gamble. Everything above is the action list.
