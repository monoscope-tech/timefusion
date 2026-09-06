# 10x readiness, re-measured on the FIXED system

2026-09-06 ~09:20Z. The standing "no headroom" result — *"across 10x load,
completions move UNDER 6% while backlog grows 11.6x"* — is from 2026-09-04 and
predates EVERY fix of the last 48h (probe admission, per-table EMA, contiguity
rank, spill cap, repair pool, landed-skip). This re-runs the question against
today's code.

## Pre-registered (written BEFORE results)

Arms: merged prod journal, 24h horizon, seed 7, release build —
(a) 1x = 17 ingesting streams (journal truth), (b) 10x = `--streams 170`.

- **Primary**: backlog trajectory at 10x — bounded band vs unbounded growth.
  Numbers: `pending_end`, completions by op, `frontier_lag_secs_max`.
- **Comparator**: the 09-04 signature (completions ~flat, backlog ~11.6x).
- **Mixed cells enumerated**: (i) backlog grows but completions ALSO scale ≥5x
  → capacity improved but still short — measure the multiple; (ii) bounded at
  10x with frontier lag blown → keeping up on volume, losing on freshness;
  (iii) bounded and lag fine → 10x-ready in the sim's IO-free model, and the
  remaining question is real-IO (staging/envelope), stated as such.
- The sim is IO-FREE: a pass here is necessary, not sufficient, for prod 10x —
  per-unit cost there is object-store round trips.

## CPU profile of the expensive dedup shape (partial, method noted)

`run-unit --op dedup` on the whale's freshly sealed day ran **258 s** of real
work (then correctly SPLIT itself — byte preflight working), sampled for 45 s
mid-phase via macOS `sample`. Two run-unit traps hit and documented on the way:
a quiet date's dedup completes in 4 s (probe-clean — nothing to profile), and
the `maintenance-cli` profile gives dedup a **0-byte pool** (`fair(pool_size:
0.0 B)`) because dedup draws the coordinator share that profile zeroes — repair
only escapes it now because repair has its own pool.

Result is PARTIAL: 65% of frames unsymbolicated (release built with
`debuginfo=0` — my own build-speed flag). Through the stripping, the named
frames are dominated by ALLOCATOR traffic (macOS xzone malloc/realloc/free +
memmove/memset/memcmp ≈ 9% at ~35% visibility → plausibly ~25% of CPU),
plus `read` syscalls and condvar waits — consistent with Arrow batch churn as
the CPU story, and with the jemalloc dirty-decay finding from 08-18 (~15% CPU
in allocator behavior) on the prod side. **Method fix for the real rerun:**
build release with `debug = "line-tables-only"`, re-sample, and aggregate by
Rust frame; do it when the cores are not running the 10x arms.

## VERDICT (pre-registered cells) — the fixes bought 1x-cycling, NOT 10x headroom

| metric (24 h, seed 7) | 1x (17 streams) | 10x (170 streams) |
|---|---:|---:|
| executions | 4,373 | **4,320 — FLAT** |
| pending_end | 3,503 | **50,980 (+14x)** |
| frontier_lag_secs_max | 83,739 | 83,739 (identical) |
| completions.DerivedRollup | 208 | **48 — collapses** |
| completions (other ops) | — | within ±5% |

Cell verdict: the 2026-09-04 signature (completions flat, backlog ~11.6x)
REPRODUCES EXACTLY on the fully-fixed system — none of the week's six fixes
moved the 10x ceiling, because they fixed CORRECTNESS and WASTE, and the
ceiling is CAPACITY: per-unit cost x slots. Derived rollups are the first
casualty (the freshness product), while the frontier lane itself keeps up.
A `--workers 32` arm is running to separate "scales with slots" from "capped
elsewhere"; its answer prices the envelope raise vs the DV writer.
