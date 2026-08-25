# Prod read-only follow-up — 2026-08-25 (P0-2 lag, P0-3 ledger over-claim)

## Provenance stamp (applies to EVERY number below unless stated)

| fact | value | how obtained |
|---|---|---|
| running image | `ghcr.io/monoscope-tech/timefusion:e67a149` | `docker service ps --no-trunc` @14:49:34Z |
| container id | `99fb08940a05` | `docker ps` |
| swarm task id | `t0zcrji80ez0` | log line prefix |
| container start | **2026-08-25T11:32:54Z** | `docker ps` CreatedAt; corroborated by `runtime.uptime_seconds`=12480 @15:00:56Z → 11:32:56Z |
| deploy during measurement | **NONE** — prior task `6a401dc` shut down 3 h ago, `d3b44f7` 4 h ago | `docker service ps` |
| host load1/5/15 @14:49:34Z | **38.42 / 39.37 / 38.82** | `uptime` on captain host |

Pre-fix comparator: image `d3b44f7` (WITHOUT `45711db`), container 08:01:21→09:18Z,
readings from `docs/plans/2026-08-25-prod-quiet-window-baseline.md` §4c.
**No load average was recorded for that window** (grepped the doc — absent).

---

## TASK 1 (P0-2) — lag warns on `e67a149`

### Fetch integrity

One `docker service logs --timestamps --since 2026-08-25T11:25:00Z`, parsed
server-side with awk over ALL lines:

```
total_lines 75643  first 2026-08-25T11:32:55.877Z  last 2026-08-25T14:50:03.304Z  lagwarns 36
```

First log line is **1.0 s after container start** and last is **now** ⇒ this is the
**complete container life**, timestamp-verified, not a `--since` promise.
Trap 2 did not fire on this fetch.

**Counter reconciliation (the one that matters):**
`runtime.scheduling_lag_max_ms = 550` @15:00:56Z, and the max `lag_ms` across all
36 parsed log lines is **550** (13:40:15.235Z). Exact agreement ⇒ no lag event
above 550 ms was missed by the fetch. (It cannot prove no 251–550 ms line was
dropped — there is no lag-warn *count* counter.)

### The 36 warns, all of them (UTC)

```
11:33:12(365) 11:33:24(363) 11:33:59(300) 11:39:05(356)
12:23:17(272) 12:42:51(273) 12:45:29(276)
13:01:00(427) 13:04:10(341) 13:12:05(305) 13:17:45(267) 13:20:51(364)
13:24:53(342) 13:25:30(282) 13:39:20(251) 13:39:23(320) 13:40:15(550)
13:42:11(274) 13:45:10(271) 13:56:13(320) 13:59:06(372)
14:05:26(256) 14:05:44(314) 14:05:50(372) 14:05:55(352) 14:05:57(497)
14:06:00(401) 14:06:02(498) 14:06:04(325) 14:09:09(267) 14:09:16(500)
14:09:19(348) 14:09:24(492) 14:09:35(277) 14:25:09(294) 14:32:52(461)
```

### `lag_ms` distribution — THE headline

| | pre-fix `d3b44f7` (n=12, boot window only) | now `e67a149` (n=36, whole life) |
|---|---|---|
| min | 263 | **251** |
| p50 | **999** | **341** |
| p90 | **1936** | **497** |
| max | **6029** | **550** |
| lifetime `scheduling_lag_max_ms` | **6029** (@83 min) | **550** (@208 min) |

**The multi-second tail is gone.** Not one of 36 events on `e67a149` exceeds
550 ms; the pre-fix build's *boot window alone* had p50 ≈ 1 s and a 6.0 s event.
Caveat: pre-fix per-event `lag_ms` values were recorded only for the 12 boot
warns, so this is a **lifetime-max / boot-distribution** comparison, not
steady-vs-steady. The lifetime `_max_ms` comparison is valid *as a worst-case
bound* — this is not the forbidden "correlate a `_max` counter with an episodic
stall".

### Rates — multiple windows, boot separated exactly as before

Boot boundary set to +6.65 min (same offset the pre-fix reading used: 08:01:21→08:08:00).

| window (UTC) | real length | container age | warns | rate /min |
|---|---|---|---|---|
| 11:32:55 → 11:39:34 (boot) | 6m39s | 0–6.6 min | 4 | 0.60 |
| **11:32:55 → 14:50:03 WHOLE LIFE** | **197m08s** | 0–197 min | **36** | **0.183** |
| **11:39:34 → 14:50:03 steady state** | **190m29s** | 6.6–197 min | **32** | **0.168** |
| 11:39:34 → 12:48:15 (AGE-MATCHED to pre-fix steady) | 68m41s | 6.6–75 min | 3 | **0.044** |
| 12:48 → 13:48 | 60m | 75–135 min | 12 | 0.20 |
| 13:48 → 14:48 | 60m | 135–195 min | 17 | 0.283 |
| 13:39:20 → 14:39:20 (the window that produced "0.35/min") | 60m | — | 22 | 0.367 |

(My recount of the alarm window gives **22**, the earlier session **21** — the
extra event is 13:39:20, a boundary inclusion. Immaterial; noted so nobody flags
the discrepancy later.)

Pre-fix `d3b44f7`: whole-life **0.25**/min (19/75m22s), steady-state **0.10**/min
(7/68m43s), boot 0.59/min (12/20m22s but all 12 inside the first 6.5 min).

### Verdicts

**A. The "3.5x worse" alarm is REFUTED — window-selection artifact. STRONG.**
The 21-warn/60-min window is 13:39–14:39, which contains the 14:05–14:09 burst
of 9. The same container's full life is **0.183/min**, *below* the pre-fix
whole-life 0.25/min. The earlier reading and this one are the **same process** —
so this is not a different sample, it is the same sample measured completely.

**B. The tail collapsed. STRONG, and window-free.** 6029 → 550 ms lifetime max,
at *higher* journal throughput (below). This does not depend on rate arithmetic
or on load matching at all, which is why it is the finding to keep.

**C. The steady-state RATE comparison is NOT SOUND. Say so.**
- 0.168 vs 0.10 /min looks 1.68x worse. It is not significant: exact Poisson 95%
  CIs are **0.041–0.210** (7 events / 68.7 min) and **0.115–0.237** (32 / 190.5
  min) — they overlap over 0.115–0.210.
- **Container ages differ 2.6x** (197 vs 75 min) and the current build's warns are
  strongly back-loaded: 0.044/min in life-minutes 6.6–75 vs 0.27/min in minutes
  135–195. Matched on age, the direction **flips** (0.044 vs 0.10) — but on n=3,
  so indicative only.
- **No `load1` exists for the pre-fix window.** It was never recorded.
- Load proxies that DO exist on both processes are in the table below and say the
  current process is doing **more** work per second, which cuts against the
  regression reading.

**What would make it comparable:** two windows on the same host at matched
`load1` (recorded at both ends), matched container age band (use the 6.6–75 min
band, it exists on both), and per-event `lag_ms` captured in both — i.e. record
`uptime` + a full-life awk parse on every future baseline. That is cheap and it
is the only thing missing.

### Load proxies (both processes, same host)

| metric | `d3b44f7` @83 min | `e67a149` @208 min |
|---|---|---|
| `journal_hold.count` / uptime | 124 691 / 4 971 s = **25.1/s** (12-min Δ gave 19.3/s) | 346 282 / 12 480 s = **27.7/s** |
| `journal_hold.max_ms` | **1105** | **685** |
| `journal_hold.avg_us` | 1 709 | 1 794 |
| `journal_lock_wait.max_ms` | **1074** | **678** |
| `journal_lock_wait.avg_us` | 217 | **163** |
| `journal_lock_wait.total_ms`/uptime | 27 090 / 4 971 = 0.55 % | 56 639 / 12 480 = **0.45 %** |

Every journal *max* and the lock-wait *average* fell, at higher ops/s. This is the
mechanism the WAL-fsync fix targets, and it moved in the right direction.

### Attribution of the residual — still the journal, but the mix flipped

Window **13:30:00.110 → 15:24:56.095Z** (parsed min/max of ALL 47 203 returned
lines, so the window is real), container age 117–232 min. 154 `blocking section
held a runtime worker` warns:

| section | count | share |
|---|---|---|
| `journal_hold` | **89** | 58 % |
| `journal_lock_wait` | **64** | 42 % |
| `pgwire_simple_handler_build` | 1 | 0.6 % |

`elapsed_ms` over the 154: min 258, p50 457, p90 542, **max 1278**.

Pre-fix `d3b44f7` mature window: 11 such warns, **all** `journal_lock_wait`,
284–672 ms. So on `e67a149` the mature residual is **no longer lock-wait
dominated** — `journal_hold` (the section itself, not contention for it) is now
the majority. That is the expected shape if fsync contention fell but the hold
itself did not: `journal_lock_wait.avg_us` fell 217 → 163 while
`journal_hold.avg_us` rose 1 709 → 1 794.

**`BlockWatch` should now point at `journal_hold`, not the lock.** MEDIUM
strength — 154 events vs 11 on the comparator, but the comparator is tiny.

**The unfavourable number, stated so it is not buried.** The blocking-warn
*frequency* went the other way: **154 / 114.9 min = 1.34/min** on `e67a149`
against the pre-fix mature window's **11 / 11m55s = 0.92/min** (and its boot
window's 10 / 20m22s = 0.49/min). That is ~1.5x MORE blocking warns per minute.
It is **not comparable** for exactly the reasons in Claim C — age band 117–232 min
vs ≤77 min, the current build's warns are back-loaded, the comparator is 11
events, and no `load1` exists for it. But it is the ratio that would have gone
unmentioned if the attribution section only reported maxima and averages, so:
**the per-minute blocking rate is not shown to have improved, and may be worse.**
Only the *tail* is unambiguously better (lag max 550 vs 6029; block max 1278 vs
counter maxima 1105/1074), and Claim B rests solely on the tail.

### Did the hourly maintenance pass cause the lag bursts? **NO — refuted.**

The coverage passes on this container fire at **:33** (see Task 2). The two lag
bursts are 13:39–13:45 and 14:05–14:09; the :33 minutes of 12:33/13:33/14:33
contain **zero** lag warns. Only the **boot** pass (11:33) coincides with warns.
The burst driver is something else — do not chase the hourly pass.

---

## TASK 2 (P0-3) — ledger `proved < held` — **RESOLVED: BENIGN. STRONG.**

### Passes captured: FOUR consecutive, all inside ONE container life

All 141 lines carry swarm task `t0zcrji80ez0` (container started 11:32:54Z, no
restart). Passes fire at **:33**, not :06 — the cadence is anchored to process
start (11:32:54 + ~17 s), which is why prior sessions saw :06 on other containers.

| pass (UTC) | disagreement lines | `held>proved` | `held<proved` | `held==proved` |
|---|---|---|---|---|
| 11:33 (boot replay) | 32 | **1** | 19 | 12 |
| 12:33 | 36 | **1** | 19 | 16 |
| 13:33 | 36 | **1** | 18 | 17 |
| 14:33 | 37 | **0** | 13 | 24 |
| **total** | **141** | 3 | 69 | 69 |

**Counter reconciliation — EXACT.** `maintenance.coverage_ledger_disagreements`
= **141** at 15:00:56Z and again at 15:15:57Z. 32+36+36+37 = **141**. Fifth such
reconciliation to close exactly. (The first whole-life fetch truncated the 14:33
pass at 24 lines mid-line; a re-fetch over a verified 14:20:00→15:24:09 window
returned the true 37. **Trap 2 fired and was caught by the counter** — this is
exactly the failure the reconciliation rule exists for.)

### The target cell, every appearance

`otel_logs_and_spans_rollup_dashboard_1h_v2 / 6297304f… / 2026-08-25`:

| pass | held | proved |
|---|---|---|
| 11:33 | 1 | 2 |
| 12:33 | **2** | **1** ← the over-claim |
| 13:33 | 1 | 1 ← **resolved, NO restart** |
| 14:33 | 1 | 2 |

**`held` fell to match `proved` at the very next pass, inside one container's
life.** The confound the task named (resolution only across a restart) is removed.

### Why it is benign — the mechanism, proven, not inferred

`src/database/maintain.rs:3336` `record_readable_coverage` builds
`batch.push((cell, proved))` for **every** cell it read and then
`self.coverage_ledger.replace_many(batch)` (line 3371). The ledger is
**overwritten with `proved` on every pass**. Therefore
**`held` at pass N ≡ `proved` at pass N−1**, by construction, and the ledger can
never be more than ONE pass stale.

Verified empirically across every cell that disagreed in two consecutive passes:

```
chain identity held(N) == proved(N-1):  98 confirmed,  0 violations
```

So the two directions are one phenomenon, not two failure classes:
- `held > proved` = coverage **shrank** between passes (a rewrite retired tags)
- `held < proved` = coverage **grew** between passes (new slices landed)

Both are one-pass write-ordering lag. There is no drift channel; a drifting
ledger would need `held` to stay pinned while `proved` walked away, and
`replace_many` makes that impossible.

### New `held > proved` lines — yes, and they are transient

Three in 141, one per pass, **a different cell each time**, and every one gone by
the next pass:

```
11:33  otel_logs_and_spans_rollup_dashboard_1m_v3  6297304f…  2026-08-25  held=3 proved=2
12:33  otel_logs_and_spans_rollup_dashboard_1h_v2  6297304f…  2026-08-25  held=2 proved=1
13:33  otel_metrics_rollup_metrics_1h_v2           87576849…  2026-08-25  held=2 proved=1
14:33  (none)
```

All are on **`date=2026-08-25`** — today, the actively-written partition. **Zero**
over-claims on any sealed date across all four passes. Population trend 1→1→1→0.
Reported per instruction as "new lines appeared", but the verdict is the same:
one-pass lag on the live frontier, not an over-claiming authority.

### Blast radius today: ZERO — nothing reads the ledger

`JsonCoverageLedger::routing_view` (`src/storage.rs:3832`) is the only method
that could route a query off the ledger, and it has **no production caller** —
`grep -rn routing_view src/` returns the definition and **one test**
(`src/database/mod.rs:12559`). The read path does not consult the ledger, exactly
as the design comment at `maintain.rs:3334` requires ("it must read zero before
any read path trusts the ledger"). So even a genuine over-claim could not have
served a read from files that are gone. STRONG (grep is exhaustive).

### FIFTH pass (15:33) — caught live, and it settles the question

Polled the counter every 60 s from 15:25:59 to 15:35:06; `uptime_seconds`
monotone 13 985 → 14 530 (still one container), `coverage_ledger_disagreements`
flat at 141 through 15:33:05 then **141 → 174 (+33)** at 15:34:06.
Re-fetched the window **15:25:00.038 → 15:38:08.119Z** (parsed, 5 376 lines):
**33** disagreement lines, all in minute 15:33. **+33 counter = 33 lines — sixth
exact reconciliation.**

Target cell over **five** consecutive passes in one life:

| pass | held | proved | chain: held(N) == proved(N−1)? |
|---|---|---|---|
| 11:33 | 1 | 2 | — |
| 12:33 | **2** | **1** | 2 == 2 ✓ |
| 13:33 | 1 | 1 | 1 == 1 ✓ |
| 14:33 | 1 | 2 | 1 == 1 ✓ |
| 15:33 | **2** | **1** | 2 == 2 ✓ |

The cell **oscillates 1↔2**: the live partition gains one slice, then a rewrite
retires it, on a roughly hourly beat. `held` is the previous pass's `proved`
every single time. The over-claim recurs and always resolves; it never grows.
Over-claim population per pass: **1, 1, 1, 0, 1** — bounded at one cell,
always on `date=2026-08-25`, never on a sealed date.

Bonus sixth lag window from the same fetch: 2 lag warns in 13m08s = **0.15/min**,
consistent with the 0.168/min steady state. `scheduling_lag_max_ms` held at
**550** across the extra 45 min (14:50 → 15:38), so the tail did not reappear.

### Against the task's literal decision rule — no contradiction

The rule reads: "`held` stays above `proved` or the gap widens ⇒ over-claim,
report loudly." At 15:33 the cell reads held=2 proved=1 again, so state plainly
why that is not a recurrence of the failure mode:

1. The rule's benign branch **was satisfied at 13:33** — `held` fell to 1 and met
   `proved`=1, inside one container life, no restart. The confound is gone.
2. 15:33 is a **fresh, independent one-pass lag instance**, not the same
   disagreement persisting: chain identity holds (held@15:33 = 2 ≡ proved@14:33 = 2).
3. **The gap never widens.** It is exactly 1 in every instance, across 5 passes
   and 174 disagreements. A drifting ledger widens; this oscillates 1↔2.
4. Every over-claim is on `date=2026-08-25`, the actively-written partition.
   **Zero on any sealed date**, across all five passes.

"Over-claim recurs hourly" and "benign" are consistent because the ledger is
rewritten to `proved` every pass and so is bounded at one pass of staleness.

**Standing condition before reads move onto the ledger:** the alarm is specified
to read zero, and it reads 141. Given the mechanism above, "zero" is unreachable
while writes land between passes — the gate as written can never open. That is a
**spec problem to resolve before item 07**, not a correctness bug today.
