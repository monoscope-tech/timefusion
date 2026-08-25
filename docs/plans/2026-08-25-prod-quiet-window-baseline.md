# Prod read-only baseline — 2026-08-25, image `d3b44f7`

Strictly read-only. No writes to prod tables, no host mutation. Census is a Delta
`get_add_actions` read against R2 (list/GET of the Delta log only).

Raw artifacts in this directory:

- **`stats-full.txt`** — the good dump: all **409** rows of `timefusion_stats` at 09:24:12Z (uptime 83 min). Reproduced verbatim in §5.
- `stats-0912.txt` — an earlier dump at 09:12:58Z, **TRUNCATED** (I piped it through `head -400`). Missing `jemalloc`, `section` and most `block` rows. Kept only because §5's deltas use two of its values. **Do not read it as the table.**
- `logs-raw.txt` / `logs.txt` — `docker service logs --since 90m`, ANSI-stripped
- `logs-tail-raw.txt` / `logs-tail.txt` — `docker service logs --since 12m`, ANSI-stripped
- `gap2.txt` — server-side-grepped `--since 08:21:00Z`, the fetch that closed the log gap
- `final-pass.txt` — the ~2 h census + stamp (§7)

---

## 1. Container stamp (from `docker service ps`, NOT `service ls`)

| | value |
|---|---|
| running image | `ghcr.io/monoscope-tech/timefusion:d3b44f7` |
| task id | `cbc2auo2h1px…` (`srv-captain--timefusion.1`) |
| container `StartedAt` | **2026-08-25T08:01:21Z** |
| previous images | `4762c2b` (shutdown ~1h ago), `85f2302` (~2h), `44d07e1` (~2h) |

`runtime.uptime_seconds` = 4296 at 09:12 and 4652 at 09:19. Container age at those
moments = 4297 s and 4653 s. **Process age == container age**, so there was no
in-container restart — every process-scoped counter below covers the whole life
of this container and nothing older.

| read | wall clock (UTC) | uptime |
|---|---|---|
| first (stats dump + census) | 09:12:58 | **71 min** |
| second (targeted stats) | 09:18:53 | **77 min** |
| final (census + ps stamp) | see §7 | |

---

## 2. Small-file census / drain series

`bench/local/small_file_census.py` **exists** and was used. `as_of=2026-08-25T09:12:59Z`,
`delta_version=502656`, `total_files=3523`.

| UTC | image | uptime | `out_of_policy_cells` | `small_files_in_them` | flagship `87576849/2026-08-19` |
|---|---|---|---|---|---|
| 06:01 | `18bcf8a` | ~5h49m | 51 | 447 | 53 |
| 07:03 | `44d07e1` | 18 min | 51 | 447 | 53 |
| 08:05 | `d3b44f7` | 3 min | 51 | 447 | 53 |
| **09:12** | **`d3b44f7`** | **71 min** | **51** | **447** | **53** |

`sealed_cells = 1081` (unchanged from 00:12 and 23:31 yesterday).

**FOURTH consecutive identical reading — now flat for 3h11m.** This is the first
of the four taken on a container older than ~20 minutes.

**Explicitly two readings, per the brief:**

(a) **Deploy-churn thesis (task 16).** The overnight series produced non-zero
windows only on a container that lived 7 hours (−66/−11/−91/−6/−107 per ~41 min
window). Three deploys since 07:03 reset in-flight units and each pays 300 s of
preload; the drain never gets going.

(b) **Rotation gap.** The 08-25 02:58 sample was a −6 window that the next sample
proved was a gap between bursts, not a stop. Four flat points at ~1 h spacing are
inside the demonstrated burst period.

**This sample does NOT discriminate them.** The container reaches 2 h at 10:01Z;
the census at that point is the discriminating sample, because every non-zero
window last night came from a container ≥ 2 h old. See §7.

Top of the out-of-policy list at 09:12 (files == small in every case except the
last):

```
87576849…a1a8   2026-08-19    53   53    2.1 GB
6297304f…f54e   2026-08-20    50   50    0.4 GB
6297304f…f54e   2026-08-17    49   49    0.9 GB
6297304f…f54e   2026-08-18    46   46    0.5 GB
6297304f…f54e   2026-08-21    32   32    0.2 GB
28f62f01…05b5b  2026-08-18    28   28    1.5 GB
87576849…a1a8   2026-08-22    28   28    1.2 GB
6297304f…f54e   2026-08-14    15   15    0.5 GB
87576849…a1a8   2026-07-31    82    6   36.4 GB   <- 82 files, only 6 small
```

---

## 3. §4 gates — readable or confounded?

### The `98e72d5` serde trap: **CLEARED**, and independently refuted by the data

`git merge-base --is-ancestor 98e72d5 d3b44f7` → **true**. It is also an ancestor
of `4762c2b`, `85f2302`, `44d07e1`, `0bcd14c`, `18bcf8a`, `62f2385` — i.e. of every
image deployed since `62f2385` (committed 2026-08-24 20:14Z; it was the live image
at 20:36Z per tasks/02). It is *not* in `1e42237` / `5e7934b`. So **`98e72d5` has
been continuously deployed for ~12.7 h across ≥ 6 containers**, and the ledger is
durable in `.timefusion_meta/`, so many hourly recovery passes have run on a build
carrying it.

Reading **(a)**: the artifact has cleared; a non-zero count now is not explained by
`CoverageEntry.files` defaulting to `[]`.

Two further pieces of evidence make this stronger than an argument from elapsed
time:

1. `record_readable_coverage` (`src/database/maintain.rs:3316-3335`) explicitly
   skips `held.is_empty()` — "the FIRST replay for that cell, not a disagreement".
2. **Every one of the 40 disagreements logged in the mature window is on
   `date=2026-08-25` — today, the live frontier — and on rollup tables only.**
   Zero on any sealed date. The serde artifact would strike *historical* cells,
   which are exactly the ones not firing.

| gate | value | uptime | verdict |
|---|---|---|---|
| `coverage_ledger_disagreements` | **69** | 71 & 77 min | **READABLE** (artifact cleared). See breakdown below. |
| `coverage_ledger_persist_failures` | **0** | 71 & 77 min | **READABLE** — not subject to the artifact on any build. Zero on a 77-min process; the standing alarm is quiet but has only 77 min of evidence. |
| `immutable_column_disagreement_total` | **0** | 71 & 77 min | **READABLE but NOT DECIDABLE.** The audit ships on in this build, so 0 means clean-so-far rather than unmeasured — but the decision gate (task 08) wants ≥ 24 h and this counter is process-scoped on a 77-min container. |

**What the 69 is made of.** All 40 disagreements in the 09:06–09:18 window fired in
a single burst at **09:06:34–09:06:41** (an hourly recovery pass), split:

| shape | n | reading |
|---|---|---|
| `proved > held` (`7→8` ×16, `2→3` ×4, `1→2` ×3, `6→7` ×1) | **24** | the ledger is missing an entry the Delta tags prove |
| `held == proved` (`1=1` ×8, `2=2` ×7, `7=7` ×1... ) | **16** | same entry count, differing content |

by table: `…dashboard_1m_v3` 11, `…dashboard_1h_v2` 11, `…metrics_1m_v2` 9,
`…metrics_1h_v2` 9.

**The per-cell breakdown makes this much less ambiguous than the counter alone.**
The 40 cells are **10 project_ids × 4 rollup tables** — i.e. essentially the
*entire* frontier population, not a subset — and within a table the shape is
near-uniform (`burst-0906-signature.txt`):

```
otel_metrics_rollup_metrics_1m_v2          9 projects, ALL held=7 proved=8
otel_logs_and_spans_rollup_dashboard_1m_v3 10 projects, 8× held=7 proved=8, rest 6/6, 6/7, 2/2, 2/3
otel_logs_and_spans_rollup_dashboard_1h_v2 10 projects, held/proved in {1,2} ± 1
otel_metrics_rollup_metrics_1h_v2          9 projects, held/proved in {1,2} ± 1
```

**A uniform off-by-one across every project of a table is a lag signature, not
drift.** Per-cell corruption would be scattered and unequal; this is "the ledger
holds N, the tags prove N+1" for the whole population at once, on the partition
being actively written.

**Ambiguity, honestly stated:** this is consistent with **(i)** a write-ordering
lag — the tag lands before the ledger persist, so a pass reading between them sees
one missing entry fleet-wide — and, less well, with **(ii)** genuine drift that
happens to be uniform. The date concentration (100 % on the frontier, 0 % on 1080
sealed cells) and the population-wide uniformity both point at (i). The
discriminator is whether the **same** cells re-disagree on the next pass with the
same gap; see §8.

**Counter-vs-log reconciliation (nearly a false anomaly):** the counter read 69 at
both 09:12 and 09:18 while the logs showed 40 warns in the 09:06–09:18 window.
That is not a contradiction — all 40 were at 09:06:3x, i.e. **before** the 09:12
read. The 09:12→09:18 delta of 0 simply means no pass ran in between.

A server-side grep over **08:21:00 → 09:16:43** (55 min, no gaps) found **exactly
one burst — the 09:06 one, 40 lines, nothing else.** So over the container's
75-minute life the checker fired roughly twice, producing 69 disagreements total
(40 at 09:06, ~29 earlier). The cadence is bursty and infrequent, which is why a
counter delta taken minutes apart reads 0 and means nothing.

### The ambiguity is RESOLVED from data already in hand: it is lag, reading (i)

The counter reconciles **exactly**: **29** disagreement lines in the boot burst
(08:06:23–08:06:33) **+ 40** in the 09:06 burst (09:06:34–09:06:41) = **69**, the
counter value. Two passes, one hour apart, fully accounted for. Nothing is
un-logged.

Diffing the two bursts by table (`burst-boot-signature.txt` vs
`burst-0906-signature.txt`):

| table | 08:06 pass | 09:06 pass |
|---|---|---|
| `otel_metrics_rollup_metrics_1m_v2` | 9 × **held=7 proved=7** | 9 × **held=7 proved=8** |
| `otel_logs_and_spans_rollup_dashboard_1m_v3` | 6 × **held=6 proved=7**, 1 × 7/7, 1 × 5/6, 1 × 1/2 | 8 × **held=7 proved=8**, 1 × 6/6, 1 × 6/7, … |

**`held` moved 6 → 7 between the two passes on the same table and the same
projects.** The ledger *absorbed* the entry it was missing at 08:06; by 09:06 the
tags had moved on to 8 and the ledger was one behind again. That is the
write-ordering lag, observed advancing, not drift — drift would leave `held`
pinned while `proved` walked away.

**Verdict for §4/task 17: `coverage_ledger_disagreements = 69` is READABLE and is
benign frontier lag.** It is not the `98e72d5` serde artifact and it is not
evidence of a broken ledger. What would change that: a burst where `held` does
*not* advance between passes, or any disagreement on a sealed date.

`coverage_ledger_persist_failures = 0` is the alarm that would show real trouble,
and it is quiet.

---

## 4b. `rollup_coverage_recovered` / `stale_generation` — the generation-orphan re-measure

Asked mid-session: is the 2026-08-22 "~92 % of the rollup tier is
generation-orphaned" figure still true after the orphan-repair migration
(`ORPHAN_REPAIR_MIGRATION`, shipped in `18bcf8a` / `2278e49`)?

The line appears **4 times in this container's life — two passes, one per source**,
and the value is the same in all four:

```
2026-08-25T08:06:25.002476Z INFO maintenance-worker ThreadId(73)
  timefusion::database::maintain: source="otel_metrics"
  recovered=4719 unverifiable=2037 stale_generation=0 event="rollup_coverage_recovered"

2026-08-25T08:06:33.532784Z INFO maintenance-worker ThreadId(73)
  timefusion::database::maintain: source="otel_logs_and_spans"
  recovered=20345 unverifiable=2349 stale_generation=0 event="rollup_coverage_recovered"

2026-08-25T09:06:34.371848Z INFO maintenance-worker ThreadId(39227)
  timefusion::database::maintain: source="otel_metrics"
  recovered=4787 unverifiable=2037 stale_generation=0 event="rollup_coverage_recovered"

2026-08-25T09:06:41.540669Z INFO maintenance-worker ThreadId(39227)
  timefusion::database::maintain: source="otel_logs_and_spans"
  recovered=20455 unverifiable=2349 stale_generation=0 event="rollup_coverage_recovered"
```

**`stale_generation = 0` on both sources, on both passes.** Not "absent from the
window" — the line is present and the field reads zero. **The ~92 % figure is
falsified on the live build.**

As a series (the ask), per source across the two passes an hour apart:

| source | 08:06 | 09:06 | Δ |
|---|---|---|---|
| `otel_metrics` — `recovered` | 4 719 | **4 787** | **+68** |
| `otel_metrics` — `unverifiable` | 2 037 | 2 037 | **0** |
| `otel_metrics` — `stale_generation` | 0 | 0 | 0 |
| `otel_logs_and_spans` — `recovered` | 20 345 | **20 455** | **+110** |
| `otel_logs_and_spans` — `unverifiable` | 2 349 | 2 349 | **0** |
| `otel_logs_and_spans` — `stale_generation` | 0 | 0 | 0 |

Two things worth carrying forward:

1. `stale_generation` is 0 and **flat**, so there is no downward trend left to
   capture — it has already bottomed out. Two points on one container cannot
   prove it stays 0, but they refute "92 % dark".
2. **`unverifiable` is byte-identical across both passes** — 2 037 and 2 349 —
   while `recovered` grew by 68 and 110. That is a **frozen** population of
   ~4 386 slices that recovery cannot verify, and it is *not* the generation
   problem. It is the residual worth naming next; the generation-orphan reason
   for deferring tag-aware rollup-tier compaction is gone, but this one is new.

**Window caveat, trap 2, and it bit again:** a server-side-grepped fetch with
`--since 2026-08-25T08:00:00Z` returned **only the 08:06 pair** and exited 0 —
i.e. it silently truncated and would have supported "the line fired once". The
four occurrences above are assembled from three fetches with independently
verified timestamp windows (`logs.txt` 07:45:47–08:21:43, `logs-tail.txt`
09:06:07–09:18:02, `gap2.txt` 08:21–09:16:43, which contains zero occurrences —
correctly, since no pass ran then). **Do not quote an occurrence count from a
single `docker service logs` fetch.**

---

## 4c. §6 residual — the lag-warn rate

Warn text: `tokio runtime scheduling lag — a task that asked for 500ms woke this
late; the pgwire handshake shares this runtime lag_ms=<n>`.

**Trap 2 fired again.** `--since 90m` returned **07:45:47 → 08:21:43** (35m56s),
and the fetch was additionally cut by my own 90 s SSH timeout. The window also
straddles the restart, so it contains `4762c2b` lines. All rates below are
computed on lines carrying the running task id `cbc2auo2h1px` and on the min/max
timestamps of **those** lines.

Three fetches, the third with the grep done **server-side** so the SSH does not
time out mid-stream — that one covers the hole the first two left:

| window (UTC) | real length | container age | lag warns | **rate** |
|---|---|---|---|---|
| 08:01:21 → 08:21:43 (boot) | 20m22s | 0–20 min | 12 | 0.59 /min |
| 08:21:00 → 09:16:43 (rest of life) | 55m43s | 20–75 min | 7 | 0.126 /min |
| 09:06:07 → 09:18:02 (mature sub-window) | 11m55s | 65–77 min | 1 | 0.084 /min |
| **08:01:21 → 09:16:43 — WHOLE CONTAINER LIFE** | **75m22s** | 0–75 min | **19** | **0.25 /min** |
| **08:08:00 → 09:16:43 — steady state (boot excluded)** | **68m43s** | 8–75 min | **7** | **0.10 /min** |

Prior series for comparison: **5.7 → 1.75 → 0.45** warns/min. This is the fourth
point: **0.25 /min over the whole container life, 0.10 /min excluding boot** — a
further ~1.8× (whole-life) to ~4.5× (steady-state) below the 0.45 residual, and
~23× to ~57× below the 5.7 baseline.

Individual steady-state warn times (all of them, so the distribution is visible,
not a mean): 08:28, 08:51, 08:56, 09:03, 09:04, 09:05, 09:16 — note the 09:03–09:05
cluster of three, i.e. the residual is still episodic rather than uniform.

**The structure matters more than the rate.** All 12 boot-window warns land in
**08:01–08:07**, the container's first 6.5 minutes:

```
4 @ 08:01   4 @ 08:02   1 @ 08:03   1 @ 08:05   1 @ 08:06   1 @ 08:07
```

and **zero from 08:08 to 08:21**. The residual on this build is concentrated in
boot, not distributed through steady state. `lag_ms` over the 12: n=12, min=263,
p50=999, p90=1936, **max=6029** (a mean would have hidden the 6 s tail — trap 8).

**Caveats, so this is not over-read:**

- The 0.084/min mature sub-window rests on **one event** and should not be quoted
  alone; the 0.10/min steady-state figure over 68m43s (7 events) is the usable one.
- The whole-life figure is **complete, not extrapolated** — but the coverage of
  **08:21:43 → 08:28:06** rests on `docker service logs --since 2026-08-25T08:21:00Z`
  being honoured, not on parsed timestamps, because that third fetch grepped
  server-side and so returned only matching lines (first at 08:28:06). If `--since`
  under-delivered there, up to 6m23s is unobserved and 0.25/min is a lower bound.
  Everything from 08:01:21→08:21:43 and 09:06:07→09:18:02 is timestamp-verified.
  What is *not* covered at all is 09:16:43 → now.
- Seven events is still a small sample; 0.10/min carries a wide interval.
- `runtime.scheduling_lag_max_ms = 6029` is the same 6 s event, and per the 08-24
  finding a `_max_ms` counter cannot be correlated with episodic stalls. The
  log-derived rate is the §6 number; the counter is not.

### `jemalloc.frag_pct` and `retained_mb` — **MEASURED** (after I nearly reported them missing)

**Self-inflicted measurement error, caught and corrected — record it.** My first
dump piped psql through `head -400`. The table is 409 rows and the **`jemalloc`
component is the LAST block emitted**, so it was cut off along with 15 `block`
rows and all 4 `section` rows. I had already written "no jemalloc rows exist" and
started reasoning about `#[cfg(feature = "profiling")]`. `Dockerfile:92,99` at
`d3b44f7` do build `--features profiling`, which is what prompted the re-dump.
The real dump (`stats-full.txt`, 09:24:12Z, uptime 4971 s / **83 min**):

| key | value |
|---|---|
| `jemalloc.allocated_mb` | 50 686 |
| `jemalloc.active_mb` | 55 159 |
| `jemalloc.resident_mb` | 105 670 |
| `jemalloc.mapped_mb` | 108 470 |
| **`jemalloc.retained_mb`** | **1 376 819** (≈ 1.34 TB) |
| **`jemalloc.frag_pct`** | **52.0** |

**`frag_pct = 52.0` — over half of resident memory backs no live allocation.**
`resident` (103 GB) is **2.08×** `allocated` (49.5 GB). This is the §6 metric that
was "armed and unread"; it is now read, at one point, on an 83-minute process.

Read it carefully:

- **One sample is not the prediction.** §6 predicted frag_pct *climbs into* a slow
  episode and falls out of it; a single 52.0 is consistent with both "high and
  flat" (fragmentation ruled out as the episode driver) and "currently mid-climb".
  Two more samples an hour apart are what tests it.
- `retained_mb` (1.34 TB) is **virtual address space jemalloc has madvised away**,
  not RSS. It is not 1.34 TB of memory in use. It is worth flagging only because
  it is 13× `mapped_mb`, which indicates very heavy allocate/free churn — the
  shape `maintain.rs:4658` already comments on.
- **`resident_mb` (105 670 MB = 103 GB) EXCEEDS the 80 GB cgroup limit**
  (`memory.limit_bytes` = 85 899 345 920). If it were cgroup-charged memory the
  container would already have been OOM-killed. So jemalloc `resident` and
  `buffered_layer.process_rss_mb` / `memory.charged_bytes` (44 GB / 45.4 GB)
  **measure different things** — jemalloc counts pages it has madvised away but
  not unmapped, which the cgroup does not charge. Not a contradiction, but it
  means **`frag_pct` must not be read as "52 % of our memory budget is waste"**;
  the wasted share of the *charged* footprint is a different, smaller number this
  instrument does not give.

Same-process movement over the two earlier reads:

| | 09:12 | 09:18 |
|---|---|---|
| `buffered_layer.process_rss_mb` | 44 084.5 | **35 281.0** (−8.8 GB in 6 min) |
| `memory.charged_pct` | 52 | **38** |

### `journal_hold` counters (§6 asks for these)

| key | 09:12 (71 min) | 09:18 (77 min) | 09:24 (83 min) | Δ over the 12 min |
|---|---|---|---|---|
| `journal_hold.count` | 110 814 | 118 172 | 124 691 | **+13 877** (≈ 19.3/s) |
| `journal_hold.total_ms` | 185 088 | 200 446 | 213 211 | **+28 123 ms** |
| `journal_hold.max_ms` | 1 105 | 1 105 | 1 105 | **flat** |
| `journal_hold.avg_us` | 1 670 | — | 1 709 | +39 |
| `journal_lock_wait.count` | 110 814 | 118 172 | 124 691 | identical to hold, every read |
| `journal_lock_wait.total_ms` | — | 26 311 | 27 090 | — |
| `journal_lock_wait.max_ms` | — | 1 074 | 1 074 | flat |
| `journal_lock_wait.avg_us` | — | — | 217 | — |

**28.1 s of worker-occupancy in 720 s = 3.9 % of one worker**, across 48 workers.
`journal_hold.avg_us` climbing 1 670 → 1 709 over 12 min is the process-age drift
the 08-24 session recorded for `delta_snapshot_refresh`; too small a move to call.

Other blocking sections, only visible after the untruncated re-dump:

```
block   pgwire_extended_handler_build   count 1736  total_ms 1269  max_ms 2  avg_us 731
block   pgwire_simple_handler_build     count 1736  total_ms 1504  max_ms 4  avg_us 866
block   pgwire_startup_handler_build    count 1736  total_ms    6  max_ms 0  avg_us   3
section delta_snapshot_refresh          count  807  total_ms 62425 max_ms 1300 avg_us 77354
```

`pgwire_*_handler_build` are trivial (max 4 ms) — **the pgwire-handshake blocking
path found on 08-24 is quiet on this build.** `delta_snapshot_refresh` averages
**77.4 ms/call** at 83 min uptime, inside the 53→121 ms process-age band recorded
on 08-24; it is a `section` (wall time, awaits included), not worker occupancy.

In aggregate the journal cost is now small, but the journal is still the named
section in **every** `blocking section held a runtime worker` warn:

- boot window: 10 such warns, 9 × `journal_hold` (435–542 ms), 1 × `journal_lock_wait` (404 ms)
- mature window: 11 such warns, **all `journal_lock_wait`**, 284–672 ms

So the residual worker-blocker on this build is **the journal lock**, and its
character changes with process age (hold at boot, lock-wait when mature). That is
where `BlockWatch` should point next.

---

## 5. Full `timefusion_stats` — raw

Untruncated dump, **409 rows**, taken at `2026-08-25T09:24:12Z`, uptime 4971 s
(83 min), image `d3b44f7`. Also saved as `stats-full.txt` in this directory.
(An earlier 09:12:58Z dump in `stats-0912.txt` is TRUNCATED at 400 lines by a
`head -400` I piped it through — do not use it; it is missing the `jemalloc`
and `section` blocks and most of `block`. See §4c.)

```
2026-08-25T09:24:12Z
budget | committed_mb | 61440
budget | warn_at_mb | 61440
budget | slack_mb | 0
budget | query_pool_mb | 16384
budget | mem_buffer_hard_mb | 21484
budget | maintenance_pool_mb | 16964
budget | foyer_mb | 4560
budget | tantivy_peak_mb | 1536
budget | df_metadata_cache_mb | 512
budget | oversubscribed | false
mem_buffer | project_count | 11
mem_buffer | total_buckets | 46
mem_buffer | total_rows | 285271
mem_buffer | total_batches | 338
mem_buffer | replay_dml_noops_total | 0
mem_buffer | estimated_bytes_approx | 972161244
mem_buffer | estimated_mb_approx | 927.1
mem_buffer | bucket_duration_micros | 300000000
mem_buffer | oldest_bucket_age_secs | 773
buffered_layer | reserved_bytes | 0
buffered_layer | max_memory_bytes | 18773704704
buffered_layer | max_memory_mb | 17904.0
buffered_layer | pressure_pct | 5
buffered_layer | backpressure_engaged_total | 0
buffered_layer | backpressure_rejected_total | 0
buffered_layer | backpressure_force_flush_total | 0
buffered_layer | flush_completed_total | 492
buffered_layer | flush_failed_total | 0
buffered_layer | rows_ingested_total | 6577717
buffered_layer | rows_flushed_total | 6292446
buffered_layer | rows_in_buffer_lag | 285271
buffered_layer | flush_freed_bytes_total | 20870803319
buffered_layer | process_rss_bytes | 46059753472
buffered_layer | process_rss_mb | 43926.0
buffered_layer | orphaned_topics | 0
buffered_layer | orphan_pin_age_secs | null
buffered_layer | drained | false
buffered_layer | boot_micros | 1787644883102315
wal | recovery_complete | true
wal | recovery_duration_ms | 0
tantivy | recovery_pending_files | 0
wal | files | 17
wal | disk_bytes | 16777219772
wal | disk_mb | 16000.0
wal | quarantine_files | 0
wal | quarantine_mb | 0.0
wal | shards_per_topic | 4
wal | known_topics | 23
dml | occ_conflicts_total | 0
dml | retry_successes_total | 0
dml | retry_exhausted_total | 0
read_dedup | ordering_violations_total | 35327
read_dedup | ordering_violations_mem | 0
read_dedup | ordering_violations_delta | 35327
maintenance | checkpoints_created | 33
maintenance | checkpoint_failed | 0
maintenance | checkpoint_corrupt | 0
maintenance | log_files_cleaned | 580
maintenance | log_cleanup_failed | 0
maintenance | checkpoint_lag_versions | 12
maintenance | dangling_removed | 0
maintenance | reconcile_failed | 0
maintenance | dedup_timed_out_total | 0
maintenance | dedup_failed_total | 0
maintenance | light_optimize_timed_out_total | 0
maintenance | light_optimize_failed_total | 0
maintenance | light_optimize_tick_truncated_total | 0
maintenance | light_optimize_projects_planned_total | 0
maintenance | light_optimize_projects_completed_total | 0
maintenance | light_optimize_bins_committed_total | 35
maintenance | light_optimize_waves_committed_total | 35
maintenance | repair_bins_in_flight | 0
maintenance | dedup_bins_committed_total | 4
maintenance | dedup_waves_committed_total | 4
maintenance | light_optimize_wal_yields_total | 0
maintenance | light_optimize_flush_debt_yields_total | 0
maintenance | light_optimize_memory_brakes_total | 0
maintenance | mor_delta_leg_sorts_total | 0
maintenance | flush_sort_unsorted_fallbacks_total | 0
maintenance | light_optimize_ticks_degraded_total | 0
maintenance | rollup_hits_full_total | 0
maintenance | rollup_hits_hybrid_total | 3
maintenance | rollup_rebuilds_incremental_total | 31
maintenance | rollup_rebuilds_full_total | 111
maintenance | rollup_dirty_partitions | 241
maintenance | rollup_skipped_covered_by_wider | 0
maintenance | split_declined_at_floor | 16
maintenance | immutable_column_disagreement_total | 0
maintenance | coverage_ledger_disagreements | 69
maintenance | coverage_ledger_persist_failures | 0
maintenance | rollup_resumed_total | 0
maintenance | rollup_resume_declined_total | 0
maintenance | rollup_untagged_inputs | 0
maintenance | rollup_tier_untagged_found | 4
maintenance | rollup_witnessless_slices | 2349
maintenance | rollup_tier_untagged_retired_total | 0
maintenance | rollup_min_contiguous_days | 30
maintenance | rollup_median_contiguous_days | 30
maintenance | rollup_oldest_invalidation_age_seconds | 846936
maintenance | rollup_scan_cohorts_total | 0
maintenance | rollup_scan_projects_total | 0
maintenance | rollup_scan_estimated_bytes_total | 0
maintenance | rollup_cohort_splits_total | 0
maintenance | rollup_singleton_failures_total | 0
maintenance | rollup_staged_projects_total | 142
maintenance | rollup_shared_commits_total | 0
maintenance | rollup_commit_actions_total | 132
maintenance | rollup_occ_retries_total | 0
maintenance | rollup_ambiguous_landings_total | 0
maintenance | rollup_scan_duration_ms_total | 4791966
maintenance | rollup_staging_duration_ms_total | 31402
maintenance | rollup_commit_duration_ms_total | 63470
maintenance | rollup_end_to_end_duration_ms_total | 4886978
maintenance | rollup_output_rows_total | 98341
maintenance | rollup_output_files_total | 126
maintenance | tantivy_uncovered_files | 1409
maintenance | pending_dedup | 3717
maintenance | pending_base_rollup | 2820
maintenance | pending_derived_rollup | 37
maintenance | pending_hot_packing | 22
maintenance | pending_sealed_consolidation | 78
maintenance | pending_repair | 551
maintenance | eligible_base_rollup | 2654
maintenance | eligible_sealed_total | 3132
maintenance | tantivy_oversized_skipped | 0
maintenance | rollup_full_hours_rebuilt_total | 0
maintenance | rollup_incremental_hours_rebuilt_total | 0
maintenance | tasks_pending | 6833
maintenance | tasks_running | 16
maintenance | tasks_retry | 376
maintenance | tasks_complete | 39023
maintenance | backlog_bytes | 3882632794277
maintenance | oldest_task_age_seconds | 7377852
maintenance | eligible_watermark_lag_seconds | 0
maintenance | processed_bytes_total | 176841837946
maintenance | processed_bytes_per_second | 0
maintenance | raw_tail_duration_seconds | 900
maintenance | sealed_compaction_debt_bytes | 456976992060
maintenance | cpu_tokens_used | 16
maintenance | decoded_bytes_used | 8583531911
maintenance | object_read_tokens_used | 16
maintenance | object_write_tokens_used | 16
maintenance | rollup_misses_total | 58
maintenance | rollup_miss_not_built_total | 21
maintenance | rollup_miss_stale_coverage_total | 4
maintenance | rollup_miss_tiny_interior_total | 17
maintenance | rollup_miss_too_many_branches_total | 0
maintenance | rollup_miss_unsupported_total | 0
maintenance | rollup_miss_incomplete_coverage_total | 0
maintenance | rollup_miss_unknown_filter_total | 3
maintenance | rollup_miss_filter_not_eligible_total | 11
maintenance | rollup_miss_missing_measure_total | 0
maintenance | rollup_miss_unaligned_bucket_total | 0
maintenance | rollup_miss_unknown_group_by_total | 0
maintenance | rollup_miss_missing_project_total | 0
maintenance | rollup_miss_unbounded_time_total | 2
maintenance | rollup_miss_non_decomposable_total | 0
maintenance | rollup_miss_rewrite_schema_mismatch_total | 0
maintenance | dirty_bin_queue_depth | 24633
maintenance | dirty_bin_enqueued_total | 142
maintenance | dirty_bin_eligible_total | 0
maintenance | dirty_bin_processed_total | 0
maintenance | dirty_bin_requeued_total | 0
maintenance | dirty_bin_batch_probe_clean_total | 0
maintenance | dirty_bin_dropped_rows_total | 0
maintenance | dirty_bin_rewrite_duration_ms_total | 0
maintenance | dedup_bins_deferred_cold_total | 0
maintenance | dedup_passes_flush_yields_total | 0
maintenance | dedup_bin_stage_timeouts_total | 0
maintenance | wave_commits_yielded_to_flush_total | 0
maintenance | repair_resumed_total | 0
maintenance | repair_resume_declined_stale_total | 0
maintenance | repair_resume_declined_incomplete_total | 0
maintenance | repair_resume_row_mismatch_total | 0
maintenance | cron_long_running_total | 4
maintenance | cron_ticks_fired | 110
maintenance | cron_ticks_skipped | 4
maintenance | retry_reason | compaction_debt_remaining
plan_cache | hits | 56136
plan_cache | misses | 19
plan_cache | hit_pct | 100.0
plan_cache | shape_hits | 109
plan_cache | shape_skips | 4
memory | charged_bytes | 41325608960
memory | limit_bytes | 85899345920
memory | charged_pct | 48
memory | query_pool_used_bytes | 0
memory | query_pool_pct | 0
scan_decode | bytes_total | 53533049742
scan_decode | peak_batch_bytes | 4570808
scan_decode | polls_inflight | 2
scan_decode | polls_inflight_peak | 28
scan_decode | pressure_throttled_total | 100023
scan_decode | worst_case_heap_mb | 122.1
scan | total | 11633
scan | skipped_delta | 8495
scan | skipped_delta_pct | 73.0
scan | mem_only | 8472
scan | delta_only | 1911
scan | mem_plus_delta | 1250
scan | dedup_eligible | 3161
scan | dedup_skipped | 1
scan | dedup_skipped_pct | 0.0
scan | dedup_skipped_per_date | 0
scan | dedup_skipped_per_file | 0
scan | dedup_denied_uncertified | 3160
scan | dedup_denied_by_leg | 0
scan | tantivy_scan_calls | 254
scan | tantivy_scan_us_total | 112589395
scan | tantivy_uris_us_total | 2069680
scan | tantivy_fastpath | 0
scan | tantivy_split_raw | 254
scan | tantivy_split_bloom | 0
scan | tantivy_split_date | 0
scan | tantivy_live_files_total | 887818
scan | tantivy_raw_files_total | 60026
scan | tantivy_backfill_built | 3
scan | tantivy_carried_forward | 26
scan | prefilter_attempts | 510
scan | prefilter_used | 254
scan | prefilter_skipped | 256
scan | prefilter_skipped_empty_index | 0
scan | prefilter_skipped_low_selectivity | 121
scan | prefilter_skipped_field_coverage_gap | 0
scan | prefilter_skipped_no_index_or_cap | 0
scan | prefilter_skipped_no_hits_returned | 0
scan | prefilter_skipped_delta_error | 0
scan | rollup_stale_no_witness | 0
scan | rollup_stale_moved | 973
scan | rollup_stale_grew | 808
scan | rollup_stale_shrank | 165
scan | rollup_stale_no_source_rows | 0
scan | pruned_calls | 1329
scan | pruned_files_total | 2907587
scan | pruned_select_us_total | 11832583
scan | pruned_build_us_total | 143745
scan | pruned_scan_us_total | 163073761
scan | tantivy_manifest_commits | 3
scan | tantivy_manifest_commit_us_total | 1812588
scan | dedup_denied_never_certified | 3121
scan | dedup_denied_fp_moved | 0
scan | dedup_denied_never_certified_pct | 100.0
scan | dedup_denied_no_window | 7
scan | dedup_denied_unresolved | 32
scan | dedup_denied_disabled | 0
scan | cert_granted_total | 0
scan | cert_slice_outside_day | 0
scan | cert_slice_dirty | 4
scan | cert_slice_partial | 14
scan | cert_slice_day_covered | 0
scan | cert_refused_dropped | 4
scan | cert_refused_incomplete | 0
scan | cert_refused_empty | 0
scan | cert_refused_fp_moved | 0
scan | cert_dwell_total | 0
scan | cert_dwell_secs_avg | 0
scan | cert_dwell_p50_secs | 0
scan | cert_dwell_p90_secs | 0
scan | fast_resolve_hits | 1249
scan | fast_resolve_misses | 1
scan | fast_resolve_hit_pct | 99.9
scan | provider_cache_hits | 1729
scan | provider_cache_misses | 357
scan | provider_cache_evictions | 221
scan | provider_cache_hit_pct | 82.9
scan | provider_build_abandoned | 0
scan | provider_build_us_avg | 148
scan | provider_build_total | 357
scan | provider_scan_us_avg | 97944
scan | provider_scan_total | 3415
scan | bounded_otel_scan_candidates | 0
scan | bounded_otel_scan_rejections | 0
scan | wide_scan_oversize_total | 23
scan | dedup_bounded_total | 6314
scan | dedup_full_set_total | 70
scan | dedup_full_set_pct | 1.1
scan | wide_scan_selected_mb_p50 | 3554
scan | wide_scan_selected_mb_p90 | 8480
scan | wide_scan_selected_mb_p99 | 44218
scan | mem_plan_us_avg | 7560
scan | mem_plan_total | 11633
scan | lat_p50_us_approx | 40946
scan | lat_p95_us_approx | 178083
scan | lat_p99_us_approx | 776234
scan | lat_p999_us_approx | 1507571
pgwire | queries_total | 57011
pgwire | lat_p50_us_approx | 52998
pgwire | lat_p95_us_approx | 237520
pgwire | lat_p99_us_approx | 767743
pgwire | lat_p999_us_approx | 3064547
foyer | hits | 212712
foyer | misses | 5905
foyer | range_hits | 211666
foyer | range_misses | 2457
foyer | bytes_served | 190589764111
foyer | inner_bytes_read | 943635211026
foyer | range_bytes_read | 32061660248
foyer | ttl_expirations | 0
foyer | inner_gets | 5905
foyer_metadata | hits | 9557
foyer_metadata | misses | 63
foyer_metadata | range_hits | 0
foyer_metadata | range_misses | 0
foyer_metadata | bytes_served | 3232838984
foyer_metadata | inner_bytes_read | 33554680
foyer_metadata | range_bytes_read | 0
foyer_metadata | ttl_expirations | 0
foyer_metadata | inner_gets | 63
foyer | memory_mb | 4048
foyer | disk_gb | 600
foyer | ttl_seconds | 3024000
foyer | l1_max_entry_mb | 16
foyer | block_size_mb | 2048
foyer | cache_recent_days | 35
foyer | cache_dir | /app/data/timefusion/cache
foyer | metadata_memory_mb | 512
foyer | metadata_disk_gb | 5
foyer | l1_used_bytes | 4227374790
foyer | l2_used_bytes | 490030899200
foyer | entry_count | 1495
foyer | evictions | 43083
logical_count | resident_partitions | 0
logical_count | resident_bytes_estimated | 0
logical_count | resident_mb_estimated | 0
logical_count | resident_limit_bytes | 4294967296
logical_count | resident_limit_mb | 4096
logical_count | active_builds | 0
tantivy | queries | 498
tantivy | indexes_searched_total | 1271
tantivy | indexes_per_query | 2
tantivy | searches | 1031
tantivy | search_us_avg | 3826
tantivy | hits_materialized | 297656
tantivy | manifest_loads | 325
tantivy | manifest_hits | 185
tantivy | manifest_hit_pct | 36.3
tantivy | manifest_load_us_avg | 271402
tantivy | blob_fetches | 2
tantivy | blob_fetch_us_avg | 22326573
tantivy | index_opens | 144
tantivy | index_open_us_avg | 74412
tantivy | reader_hits | 887
tantivy | reader_hit_pct | 86.0
tantivy | reader_cache_capacity | 2048
tantivy | search_concurrency | 32
tantivy | cache_seeded | 316
tantivy | cache_seed_failures | 0
tantivy | manifest_load_us_total | 88205968
tantivy | blob_fetch_us_total | 44653147
tantivy | index_open_us_total | 10715355
tantivy | search_us_total | 3945539
tantivy | plan_us_total | 210220
tantivy | prepare_us_total | 56105492
tantivy | prepares | 1031
tantivy | fanout_us_total | 60142936
bloom_prune | queries_pruned | 4912
bloom_prune | files_probed | 106158
bloom_prune | files_rejected | 98442
bloom_prune | registry_hits | 4916
bloom_prune | registry_misses | 13
bloom_prune | loads | 53
bloom_prune | load_errors | 0
bloom_prune | build_files | 556
bloom_prune | build_errors | 0
bloom_prune | resident_bytes | 132181902
parquet | metadata_cache_hits | 85767
parquet | metadata_cache_misses | 6244
parquet | bytes_read | 160070085491
parquet | read_time_us | 1781758851
parquet | scans | 5580
parquet | files_planned | 27573
parquet | bytes_planned | 7252891397314
parquet | selected_row_groups | 5
scan | fast_resolve_cache_entries | 70
scan | provider_cache_entries | 23
runtime | uptime_seconds | 4971
runtime | scheduling_lag_ms | 51
runtime | scheduling_lag_max_ms | 6029
runtime | worker_threads | 48
block | journal_hold.count | 124691
block | journal_hold.total_ms | 213211
block | journal_hold.max_ms | 1105
block | journal_hold.avg_us | 1709
block | journal_lock_wait.count | 124691
block | journal_lock_wait.total_ms | 27090
block | journal_lock_wait.max_ms | 1074
block | journal_lock_wait.avg_us | 217
block | pgwire_extended_handler_build.count | 1736
block | pgwire_extended_handler_build.total_ms | 1269
block | pgwire_extended_handler_build.max_ms | 2
block | pgwire_extended_handler_build.avg_us | 731
block | pgwire_simple_handler_build.count | 1736
block | pgwire_simple_handler_build.total_ms | 1504
block | pgwire_simple_handler_build.max_ms | 4
block | pgwire_simple_handler_build.avg_us | 866
block | pgwire_startup_handler_build.count | 1736
block | pgwire_startup_handler_build.total_ms | 6
block | pgwire_startup_handler_build.max_ms | 0
block | pgwire_startup_handler_build.avg_us | 3
section | delta_snapshot_refresh.count | 807
section | delta_snapshot_refresh.total_ms | 62425
section | delta_snapshot_refresh.max_ms | 1300
section | delta_snapshot_refresh.avg_us | 77354
jemalloc | allocated_mb | 50686
jemalloc | active_mb | 55159
jemalloc | resident_mb | 105670
jemalloc | mapped_mb | 108470
jemalloc | retained_mb | 1376819
jemalloc | frag_pct | 52.0
```

### Same-process deltas — 09:12 (4296 s) vs 09:18 (4652 s), 355 s apart

| key | 09:12 | 09:18 | Δ / 355 s |
|---|---|---|---|
| `tasks_complete` | 38 997 | 39 017 | **+20** (≈ 203/hr) |
| `tasks_pending` | 6 823 | 6 893 | **+70** |
| `pending_base_rollup` | 2 812 | 2 844 | **+32** |
| `pending_dedup` | 3 708 | 3 750 | **+42** |
| `pending_sealed_consolidation` | 76 | 77 | +1 |
| `pending_repair` | 550 | 551 | +1 |
| `tasks_retry` | 371 | 373 | +2 |
| `oldest_task_age_seconds` | 7 377 178 | 7 377 533 | **+355 = exactly the elapsed wall clock** |
| `process_rss_mb` | 44 084.5 | 35 281.0 | **−8 803** |
| `charged_pct` | 52 | 38 | −14 |
| `scheduling_lag_ms` (instantaneous) | 3 | 1 | (51 at 09:24) |

## 6. Anomalies

1. ~~**The queue is growing 3.5× faster than it drains.**~~ **RETRACTED — see §7.**
   I measured `+20 complete / +70 pending` over a **355-second** window on a
   71-minute process and wrote that the queue was growing. Over the **2 776-second**
   window on the same container it is **+96 complete / −67 pending — the queue is
   shrinking.** A 6-minute delta is not a throughput measurement, and this is the
   fifth time in this repo's history that a too-short window produced a confident
   wrong direction. The usable figure is **~124 completions/hr with pending
   falling ~87/hr** on a mature container. (The 08-24 `+40 rebuilds / +83 pending`
   reading was taken over 45.6 min and is not overturned by this; it may simply
   describe a different load period.)

2. **`oldest_task_age_seconds` grew by exactly the elapsed time again** —
   7 377 178 → 7 377 533, +355 s over 355 s. **85.4 days and counting, on a
   container that never restarted.** Nothing old is being worked at all. This is
   the task-16 signature, unchanged.

3. **Dedup is failing on a DataFusion sort memory limit, and it is the standing
   `retry_reason`:**

   ```
   dedup: Not enough memory to continue external sort. Consider increasing the
   memory limit config 'datafusion.runtime.memory_limit', or decreasing
   'datafusion.execution.sort_spill_reservation_bytes'.
   Failed to allocate additional 551.4 MB for ExternalSorter[1] with 0.0 B already
   allocated — 73.2 MB remain available for the total memory pool:
   fair(pool_size: 5.0 GB)
   ```

   Alongside it: `pending_dedup = 3708→3750` (the largest pending class),
   `tasks_retry = 371`, and **13 `operation=Dedup timeout_seconds=300` warns in
   the 12-minute mature window** (15 in the 20-minute boot window). Dedup is the
   dominant retry/timeout consumer on this build.

4. **`budget.slack_mb = 0`** with `committed_mb == warn_at_mb == 61440` and
   `oversubscribed = false`. The budget is committed exactly to its warn line with
   zero slack — a self-consistent config, but it means any overshoot warns
   immediately.

5. **`process_rss_mb` fell 44 084 → 35 281 (−8.8 GB) in 6 minutes** while
   `charged_pct` went 52 → 38. Two readings: a large maintenance unit finished and
   released, or a scan-pressure-valve shed. The valve *is* firing — 10 valve warns
   in the two windows, e.g. `61% -> 60% of cgroup limit, growth 1919 MB/s` and
   `48% -> 51%, growth 4591 MB/s`. Not resolved from two points.

6. **18 × `starting maintenance before the cache preload finished`** in the boot
   window — the 300 s preload gate from task 16 §3, firing as described.

7. **`87576849/2026-07-31` holds 82 files / 36.4 GB but only 6 are small.** It is
   out of policy on the ≥2-small-files rule while being by far the largest cell by
   bytes. Noted, not diagnosed.

8. **`checkpoint_lag_versions = 12`** and `tantivy recovery_pending_files = 0` —
   both healthy on this container.

9. **One `tantivy build produced 56 segments (> 32); merging inline`** in the
   mature window. Inline merge on a maintenance path; noted only.

10. **Certification grants ZERO, and it denies essentially every dedup skip.**
    From the untruncated dump — these rows were below the `head -400` cut and
    have not been read this session before:

    ```
    scan  dedup_eligible                  3161
    scan  dedup_skipped                      1     dedup_skipped_pct  0.0
    scan  dedup_denied_uncertified        3160
    scan  dedup_denied_never_certified    3121     ..._pct  100.0
    scan  dedup_denied_fp_moved              0
    scan  cert_granted_total                 0
    scan  cert_slice_day_covered             0
    scan  cert_slice_partial                14     cert_slice_dirty  4
    scan  cert_refused_dropped               4     cert_refused_incomplete/empty/fp_moved  0
    ```

    **3 160 of 3 161 dedup-eligible scans were denied, 100 % of them for
    "never certified", and `cert_granted_total` is 0.** So `DedupExec` is in
    effectively every plan on this container. Per the 08-22 finding
    (`tf_cert_works_contiguity_blocks`) `cert_granted_total` is process-scoped and
    a young process reads 0 for benign reasons — but this is a **77–83 minute**
    process that has run 3 161 eligible scans and granted nothing, and the
    refusal reasons are all zero except `cert_refused_dropped = 4`. That pattern
    (nothing granted, nothing explicitly refused) is what task 13 describes. Not
    diagnosed here; flagged as the largest read-path finding in this dump.

11. **`rollup_stale_no_witness = 0` — the 08-22 "95.2 % unverifiable" is GONE.**

    ```
    scan  rollup_stale_no_witness      0
    scan  rollup_stale_moved         973   (grew 808 / shrank 165)
    scan  rollup_stale_no_source_rows  0
    ```

    Staleness is now attributed as genuine movement, not as unverifiable. This
    corroborates task 12's closure ("real churn beats benign churn 122:1") from a
    different instrument and on a different build.

12. **`tantivy_fastpath = 0` out of `tantivy_split_raw = 254` — unchanged since
    08-22.** Every tantivy scan still pays the 2-leg split; the fastpath has never
    fired on this container either. `tantivy_scan_us_total = 112.6 s` over 254
    calls (443 ms/call).

13. **Prefilter skip labels still do not sum to the total — task 11's readout gap,
    confirmed live.** `prefilter_attempts 510`, `used 254`, `skipped 256`, but the
    labelled reasons total only **121** (`low_selectivity`; `empty_index`,
    `field_coverage_gap`, `no_index_or_cap`, `no_hits_returned`, `delta_error` all
    0). **135 of 256 skips (53 %) are unattributable**, exactly as
    `00-STATUS` §1 predicted from `pg_compat.rs:1233` being unmerged.

14. **Healthy, worth recording as the baseline they are:**
    - `plan_cache`: 56 136 hits / 19 misses = **100.0 % hit rate** (`shape_hits` 109, `shape_skips` 4).
    - `bloom_prune`: 98 442 of 106 158 files probed were **rejected (92.7 %)**; `load_errors` 0, `build_errors` 0.
    - `scan.skipped_delta_pct = 73.0` (8 495 of 11 633 scans never touched Delta).
    - `wal`: `recovery_complete true`, `recovery_duration_ms 0`, `quarantine_files 0`, 17 files / **16.0 GB** on disk, 23 topics.
    - `dml`: `occ_conflicts_total` / `retry_successes_total` / `retry_exhausted_total` all **0**.
    - `tantivy`: `reader_hit_pct 86.0`, `cache_seed_failures 0`.
    - `parquet`: metadata cache 85 767 hits / 6 244 misses (93.2 %).

15. **`read_dedup.ordering_violations_delta = 35 327`** (and `..._mem = 0`) in
    83 minutes. All ordering violations come from the Delta side. Recorded as a
    baseline rate (~426/min); no interpretation offered.

16. **Serving-latency baseline for this quiet window** (approximate quantiles the
    process keeps itself, over 57 011 pgwire queries in 83 min — a distribution,
    not a mean, per trap 8):

    | | p50 | p95 | p99 | p999 |
    |---|---|---|---|---|
    | `pgwire.lat_*_us_approx` | **53.0 ms** | **237.5 ms** | **767.7 ms** | **3 064.5 ms** |
    | `scan.lat_*_us_approx` | 40.9 ms | 178.1 ms | 776.2 ms | 1 507.6 ms |

    `foyer` hit rate **97.3 %** (212 712 / 218 617), `ttl_expirations 0`,
    190.6 GB served. `foyer_metadata` 99.3 %.

17. **`wide_scan_selected_mb_p99 = 44 218` (43 GB selected by a single scan)**,
    p90 8 480 MB, p50 3 554 MB, with `wide_scan_oversize_total = 23` refusals in
    83 min. The refusal valve from the 08-23 work is firing 23 times an hour, and
    the p99 selected set is over half the 80 GB cgroup limit. `dedup_full_set_pct`
    is 1.1 % (70 of 6 384), so the bounded path is carrying almost everything.

---

## 7. THE DISCRIMINATING CENSUS — 09:59Z, container age 1h58m. **The streak is broken; the drain resumed.**

Container stamp taken in the same command, so it qualifies the numbers directly:

```
2026-08-25T09:59:05Z
cbc2auo2h1px…  srv-captain--timefusion.1  …:d3b44f7  Running 2 hours ago
StartedAt 2026-08-25T08:01:21.053034036Z   image d3b44f7
runtime.uptime_seconds = 7072  (1h57m52s)
```

**Same container, no deploy, uptime 1h58m — the ≥2h regime that has been
unobtainable for days.**

| UTC | image | uptime | `out_of_policy_cells` | `small_files_in_them` | flagship `87576849/2026-08-19` |
|---|---|---|---|---|---|
| 06:01 | `18bcf8a` | ~5h49m | 51 | 447 | 53 |
| 07:03 | `44d07e1` | 18 min | 51 | 447 | 53 |
| 08:05 | `d3b44f7` | 3 min | 51 | 447 | 53 |
| 09:12 | `d3b44f7` | 71 min | 51 | 447 | 53 |
| **09:59** | **`d3b44f7`** | **118 min** | **51** | **425 (−22)** | **53** |

`delta_version` 502656 → 502706, `total_files` 3523 → **3556**, `sealed_cells` 1081.

### What moved, and it is a single cell

The −22 is **entirely one cell**: `28f62f01-46a1-400e-8195-da7bc3505b5b /
2026-08-18` went **28 → 6 small files (−22)**. Every other cell in the visible top
list is unchanged file-for-file. The flagship `87576849/2026-08-19` sat at 53 for
the fifth consecutive census.

### Which thesis this supports

**It breaks the flat streak, and it breaks it exactly where the rotation/maturity
reading predicted and the pure-deploy-churn reading did not.** The series was flat
at 447 for **3h11m across three containers and two deploys** (06:01, 07:03, 08:05,
09:12), then moved **−22 within the same container, with no deploy, on the sample
that crossed ~2 h**.

- **Deploy churn alone is not sufficient.** The 08:05→09:12 window had no deploy
  either and was flat, so "a deploy happened" is not what distinguishes the moving
  window from the still one. What distinguishes them is **container age**: the
  drain appeared only after ~1 h 15 m of maintenance time past the 300 s preload.
- **Rotation is confirmed, not refuted.** One cell drained 28 → 6 while the
  flagship and everything else sat still — the per-cell bursty rotation documented
  in tasks/02's ninth census, seen again.
- **The two combine into one statement:** hygiene needs a long-lived container to
  reach any given cell, *and* it rotates rather than finishing cells. A ~1 h deploy
  cadence therefore lands almost entirely inside the dead zone. That is task 16's
  thesis with the missing evidence supplied, and it is now an observation on one
  container rather than an association across containers.

**Stated limits, so this is not over-read:** one moving window after four flat
ones. It does not prove the drain would have continued at any rate, and it cannot
separate "age unlocks the drain" from "this particular cell came up in the
rotation now". A second spaced point on the same container is running (§8) and is
what tightens it.

### And still: `out_of_policy_cells = 51` for the TENTH consecutive census

`28f62f01/2026-08-18` fell 28 → 6 and **did not retire** — 6 small files is still
≥ 2, so it stays out of policy. Files leave; cells do not. This is the exact
practical shape of defect 0 (`benefit` inert because `InputFootprint.files` is
written only at claim time): hygiene samples cells instead of finishing them.
**Signal 2 read alone would report "nothing happened" through a window that
retired 22 files.**

### Counters over 09:12:58 → 09:59:14 — same process, so these ARE rates

| key | 09:12 (71 min) | 09:59 (118 min) | Δ over 2 776 s |
|---|---|---|---|
| `tasks_complete` | 38 997 | 39 093 | **+96** (≈ 124/hr) |
| `tasks_pending` | 6 823 | **6 756** | **−67 — the queue SHRANK** |
| `pending_base_rollup` | 2 812 | 2 773 | **−39** |
| `pending_dedup` | 3 708 | 3 693 | **−15** |
| `pending_sealed_consolidation` | 76 | **83** | +7 |
| `tasks_retry` | 371 | 376 | +5 |
| `oldest_task_age_seconds` | 7 377 178 | 7 379 954 | **+2 776 = exactly the elapsed wall clock** |
| `process_rss_mb` | 44 084.5 | 35 245.8 | −8 839 |
| `charged_pct` | 52 | 46 | −6 |
| `journal_hold.count` | 110 814 | 168 738 | +57 924 (≈ 20.9/s) |
| `journal_hold.total_ms` | 185 088 | 303 747 | +118 659 ms (**4.3 % of one worker**) |

**This retracts anomaly 1 as I first wrote it.** On the 355-second young-process
window I measured `+20 complete / +70 pending` and wrote "the queue is growing 3.5×
faster than it drains". Over the 2 776-second mature window it is
**+96 complete / −67 pending** — the queue is **shrinking**. The 355 s sample was
too short and too young to carry that claim; a 6-minute delta on a 71-minute
process is not a throughput measurement. The corrected statement is in anomaly 1.

`oldest_task_age_seconds` still advances at exactly wall-clock rate (**85.4 days**),
now measured over 2 776 s as well as 355 s. **Nothing old is being worked, on a
container that has never restarted.** That signature is untouched by the drain
resuming, and it is the one finding here that no reading softens.

### `jemalloc` second point — `frag_pct` is FLAT, not climbing

| key | 09:24 (83 min) | 09:59 (118 min) | Δ |
|---|---|---|---|
| `allocated_mb` | 50 686 | 57 767 | +7 081 (+14 %) |
| `active_mb` | 55 159 | 62 993 | +7 834 |
| `resident_mb` | 105 670 | 115 229 | +9 559 |
| `mapped_mb` | 108 470 | 117 894 | +9 424 |
| `retained_mb` | 1 376 819 | 1 469 693 | +92 874 |
| **`frag_pct`** | **52.0** | **49.9** | **−2.1** |

§6 predicted `frag_pct` "climbs into a slow episode and falls out of it — flat
would rule fragmentation out too". Over 35 minutes in which allocation grew 14 %,
**frag_pct did not climb; it fell slightly.** Two points, both taken in a stretch
with no observed slow episode, so this **weakly** supports the flat/ruled-out arm
and proves nothing about behaviour during an episode. It is the first time the
metric has been read at all.

---

## 8. LEDGER BURST #3 (10:06Z, uptime 2h05m) — **the lag reading is CONFIRMED on three passes**

Counter re-read at 10:13:23Z, uptime **7 922 s (2h12m)**:
`coverage_ledger_disagreements = 107`, `persist_failures = 0`,
`immutable_column_disagreement_total = 0`.

**Exact reconciliation, three for three:** 29 (08:06) + 40 (09:06) + **38 (10:06)**
= **107**, the counter value. Every disagreement the counter has ever recorded on
this container is accounted for by a logged line. Nothing is un-logged, and the
cadence is confirmed **hourly** (08:06:23, 09:06:34, 10:06).

### `held` advances every pass — this is a lag, not drift

The discriminator I named in §3 was: do the same cells re-disagree, and does
`held` advance? Same nine projects, same table, three passes:

| pass | `otel_metrics_rollup_metrics_1m_v2` (9 projects) | `…dashboard_1m_v3` |
|---|---|---|
| 08:06 | `held=7 proved=7` | 6× `held=6 proved=7` |
| 09:06 | `held=7 proved=8` | 8× `held=7 proved=8` |
| **10:06** | **`held=8 proved=9`** | **8× `held=8 proved=9`** |

**`held` walks 7 → 7 → 8 while `proved` walks 7 → 8 → 9.** The ledger keeps
catching up and the tags keep moving one ahead — one new entry per hour on the
live frontier, which is exactly the rollup cadence. Drift would pin `held` while
`proved` ran away; instead the gap stays constant at one and both advance.

**Verdict: `coverage_ledger_disagreements` is benign frontier write-ordering lag,
established on three hourly passes with a monotone `held`.** It is not the
`98e72d5` serde artifact, and it is not ledger corruption. `persist_failures`
remains 0 across all three.

### Two new shapes in this burst, neither fatal, both worth naming

1. **First disagreement on a date other than today:**
   `…dashboard_1h_v2 project_id=87576849… date=2026-08-24 held=6 proved=6`.
   Across 107 disagreements this is the **only** non-`2026-08-25` cell, and it is
   yesterday — the day that sealed at midnight, i.e. still the most recently
   written partition. Equal counts, so it is a content difference, not a missing
   entry. Consistent with the same lag draining out of the previous day. **Still
   zero disagreements on any of the 1 080 genuinely sealed cells.**
2. **First `proved < held`:** `…dashboard_1h_v2 project_id=6297304f… date=2026-08-25
   held=3 proved=1`. The ledger holds **more** than the tags prove — the opposite
   direction from every other line, and **not** a lag signature. One occurrence in
   107. Two readings, not resolved: a compaction/rewrite retired tags the ledger
   still remembers (benign, and the next pass should show `held` falling to 1), or
   a genuine over-claim by the ledger. **This is the one line that would matter if
   it recurs**, because an over-claiming ledger can serve a read from files that
   are gone. Cheapest check: the 11:06 pass — does that cell's `held` fall to
   match `proved`?

---

## 8b. `unverifiable` is FROZEN — third byte-identical reading

Third pass triple (log window verified, `--since 09:30:00Z`, lines at ~10:06):

```
source="otel_metrics"          recovered=4857  unverifiable=2037  stale_generation=0
source="otel_logs_and_spans"   recovered=20570 unverifiable=2349  stale_generation=0
```

Full series, three passes on one container, 08:06 → 10:06:

| source | metric | 08:06 | 09:06 | 10:06 | trend |
|---|---|---|---|---|---|
| `otel_metrics` | `recovered` | 4 719 | 4 787 | **4 857** | +68, **+70** |
| `otel_metrics` | `unverifiable` | 2 037 | 2 037 | **2 037** | **frozen** |
| `otel_metrics` | `stale_generation` | 0 | 0 | **0** | **flat zero** |
| `otel_logs_and_spans` | `recovered` | 20 345 | 20 455 | **20 570** | +110, **+115** |
| `otel_logs_and_spans` | `unverifiable` | 2 349 | 2 349 | **2 349** | **frozen** |
| `otel_logs_and_spans` | `stale_generation` | 0 | 0 | **0** | **flat zero** |

Two conclusions, at different strengths:

1. **`stale_generation = 0` on three passes, both sources.** The generation-orphan
   population is empty on this build. Still one container, still under 2h at the
   first two passes — the third is at 2h05m, so **one** of the trigger's two axes
   is now met; "two separate containers" is not.
2. **`unverifiable` is byte-identical three times — 2 037 and 2 349 — while
   `recovered` grew by 138 and 225 respectively.** Recovery is demonstrably doing
   work each pass and this population never changes by even one slice. A frozen
   **~4 386-slice** set that coverage recovery cannot verify. Three identical
   readings while a sibling counter moves is much stronger than two: it is not a
   sampling artifact and it is not "nothing ran". **This is the successor problem
   to the generation orphans** — same blast radius, different cause, and it has no
   instrument beyond this log line.

**Trap discipline applied:** the count of 38 comes from a fetch whose burst is
fully contained in the window (all 38 at `T10:06`, i.e. not clipped at either
edge), and it is independently corroborated by the counter arithmetic
(29+40+38 = 107 exactly). Earlier in this session a single server-side-grepped
fetch silently truncated and would have supported a wrong occurrence count; the
counter reconciliation is what makes this one safe to quote.

---

## 8c. Still running at hand-off

**Census #2 on the mature container** (`census2.txt`) — fires ~10:45Z, container
age ~2h44m. Two spaced points on one long-lived container is what turns §7's
single −22 window into a rate. Read-only (Delta log listing + one
`timefusion_stats` select).

---

## 9. Final stamp
