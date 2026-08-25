# `SELECT 1` costs 1.4–3.0 s on an old process and 46 ms on a fresh one

2026-08-24, prod, read-only. Started as "opening a connection costs 2.3 s"; the
connection turned out to be innocent, and so did the host. What is left is a
process that gets ~20× slower over hours of uptime.

## 0. Where this stands (read this first)

Everything below §5 is a chronology, including three of my own verdicts that
later data overturned. The net result, as of 2026-08-24 22:00:

**There are two phenomena, and the stalls are EPISODIC, not a function of age.**

1. **Deploy churn** — a new container replaying WAL while the old one still
   serves. Explains every stall on a young or mid-life process, replicated four
   times, and gone once the churn stopped. **My own pushes to the benchmark
   script were causing it**: `deploy.yml` exempted `docs/**` and `**/*.md` but
   not `bench/**`. Fixed (`9ab6b00`).
2. **A server-side stall in the pgwire startup exchange** — **localized**: the
   container's own healthcheck, over *loopback*, sees `auth_ms` of 470–1,423 ms
   while `connect_ms` and `write_ms` are 0 and handler construction maxes at
   4 ms. Not the network, not the haproxy, not auth logic, not handler build.
   Frequent (2 of 3 sampling windows), and it explains the external
   connect-only stalls.

**The system-level finding, which is bigger than either:** this process **fails
to wake a 500 ms timer on time by 0.3–2.9 s, several times a minute,
continuously**, on 48 workers with the host 40 % idle. The auth stalls sit inside
that distribution. Causation is unproven precisely *because* the lag never stops
— an always-present cause cannot be correlated with an intermittent effect — so
the next step is per-connection accept-to-startup timing inside the server, not
more outside correlation.

**Eliminated, each with the instrument built for it:** host load (44.0 → 178 ms
versus 32.9 → 4,460 ms — both directions); the maintenance journal mutex (held a
worker 2,380 ms with *zero* effect on query cost); Delta snapshot refresh (hit
403 ms/call while `connect` sat at 305 ms); worker starvation (`scheduling_lag_ms`
0–1 through every stall); swap (100 % full at all times, so it differentiates
nothing).

**The title premise is FALSE.** A 7.15-hour process — longer-lived than the 5 h
one that opened this plan — measured `connect` **107 ms** and warm `SELECT 1`
**23.5 ms**, the fastest samples of the whole investigation, with 221 GB
retained and 606,953 journal fsyncs behind it. Cost is *episodic*: flat at
~177 ms for hours, with occasional multi-minute slow stretches that recover on
their own. Do not read "age" as the cause.

**Armed and pending:** `jemalloc.frag_pct` (prediction: climbs into an episode,
falls out of it — if it stays flat, fragmentation is out too) and per-connection
`block.pgwire_*_handler_build` timers.

**Do not** ship a scheduled restart (restarts *cause* the churn stalls) or touch
allocator decay config (mechanism unconfirmed; the last decay change cost ~15 %
CPU).

> Reading the CSVs: gaps of tens of minutes are **the sampling laptop asleep**,
> not prod outages. The sampler now runs under `caffeinate -i`, which still does
> not cover a closed lid.

## 1. The measurement that started it

Noticed while proving that a 440 GB scan does not starve the box
(`2026-08-23-a-440gb-scan-no-longer-starves-the-box.md`): the *idle* baseline for
opening a connection was 2.3 s, and 5.5 s in an earlier sample. Decomposed on
prod, process uptime ~5 h, host load average 39:

| stage | median | note |
|---|---|---|
| DNS | 2.5 ms | negligible |
| TCP connect | 128 ms | network RTT to the host |
| full connect (startup + auth) | **4,748 ms** | ~4.6 s of per-connection setup |
| `SELECT 1` on an **open** connection | **2,969 ms** | |
| second `SELECT 1`, same connection | **1,437 ms** | |

A no-op query on an already-established connection taking 1.4–3.0 s rules the
connection path out as the cause: whatever this is, it is not handshake, auth, or
`pg_catalog` setup, because a warm connection pays it too. Pooling would not fix
it — it would only stop paying the 4.6 s setup repeatedly.

## 2. The re-measurement that reframed it

Same script (`bench/pass-condition/conncost.py`), same client, same host, **~4
minutes** after a deploy:

| stage | 5 h uptime, load 39 | 4 min uptime, load 22 | ratio |
|---|---|---|---|
| DNS | 2.5 ms | 1.5 ms | — |
| TCP connect | 128 ms | 45 ms | 2.8× |
| full connect | 4,748 ms | **219 ms** | **21.7×** |
| `SELECT 1` (open conn) | 2,969 ms | **103 ms** | **28.8×** |
| second `SELECT 1` | 1,437 ms | **46 ms** | **31.2×** |

So there is no structural per-connection cost. 219 ms to connect and 46 ms for a
warm no-op are healthy. **The cost appears with process age and/or load, and both
moved together in these two samples — they are confounded and deconfounding them
is step 1 of the plan below.**

## 3. What is ruled out, with the measurement

**The host is not CPU-saturated.** 48 cores; `%Cpu(s): 39.2 us, 5.5 sy, 52.1 id`
— **half idle**. Load average 22–39 on 48 cores is not saturation.

**TimeFusion is not CPU-limited and is not the top consumer.**
`docker inspect` reports `NanoCpus=0 CpuQuota=0 CpuPeriod=0` — **no CPU limit at
all**. It was using 554 % (≈5.5 of 48 cores) while three monoscope processes used
692 %, 554 % and 338 % (≈15.8 cores). So "the box is CPU-saturated" — which this
session asserted more than once — was wrong as a whole-host claim.

**Memory is not the constraint**: 16.7 GiB of a 120 GiB limit on the fresh
process, 41.5 GiB on the old one.

**It is not the connection path**: a warm connection pays the same cost.

That leaves something internal to the process that degrades with uptime, while
tokio has 48 workers and the machine has cores to spare. The
`timefusion.runtime.scheduling_lag_ms` gauge exists precisely for this shape — "a
task that asked for 500 ms woke late" — and prod logged **117 such warnings in 15
minutes** on the old process. Workers were starved while the host was half idle,
which means they were **blocked**, not busy.

## 4. Candidate causes, and how each is distinguished

None of these is established. They are listed with the observation that would
confirm or kill each, because the tempting move — "add connection pooling" —
addresses none of them.

| # | Hypothesis | Confirm / kill it by |
|---|---|---|
| 1 | **Blocking work on async workers.** Object-store IO, Parquet decode or a `std::sync::Mutex` held across an await starves workers while cores idle. Fits the evidence best: lag with idle CPU is the signature. | Correlate `scheduling_lag_ms` with concurrent operation counts; add a `tokio` blocking-duration probe, or run one deploy with `--cfg tokio_unstable` + runtime metrics to read the injection queue depth. |
| 2 | **The maintenance journal mutex.** `db.maintenance_tasks.lock()` is taken on every claim/complete/checkpoint, and the journal held ~5,500 pending tasks with 16 workers cycling. If a query path touches the same lock, every query queues behind maintenance. | **Half-refuted 2026-08-24, in code**: no `journal()` / `maintenance_tasks.lock()` call exists under `read/`, `database/scan.rs` or `server/` — the query path never takes it, so no query can queue *on the lock*. What survives is the indirect path: it is a `std::sync::Mutex`, so a long hold occupies a runtime **worker**, and a query scheduled onto that worker waits anyway. Both the wait and the hold are now timed — `block.journal_lock_wait.*` / `block.journal_hold.*` in `timefusion_stats`. |
| 3 | **State that grows with uptime.** Mem buffer (`rows_in_buffer_lag` read 286,438), plan cache, foyer index, Delta snapshot file lists. Planning cost is already known to scale with the window rather than the data, so a growing snapshot would hit *every* query including `SELECT 1`'s catalog work. | Sample `conncost.py` every 30 min against uptime on a process nobody redeploys; plot alongside `mem_buffer` and snapshot file counts. |
| 4 | **Swap.** Host swap was **2047.8 of 2048 MB used** — full. A 2 GB swap on a 192 GB host is small enough to be vestigial, but a fully-used swap plus 122 GB in `buff/cache` can stall on page faults. | `vmstat` si/so during a slow window; compare against a fresh process. |
| 5 | **jemalloc fragmentation.** Known-live concern in this codebase (`dirty_decay_ms` cost ~15 % CPU in TLB misses once before). | `jemalloc` stats over uptime; `prof_active` is off, so this needs the stats endpoint, not a heap dump. |

## 5. The plan

**Phase 0 — deconfound age from load. Nothing else is worth doing first.**
The two samples differ in *both* process age (5 h vs 4 min) and host load (39 vs
22), so neither is attributable yet. Run `conncost.py` every 30 minutes for a full
day, recording process uptime, host load, container CPU and RSS with every
sample. Two clean outcomes:

- Cost tracks **uptime** and not load → state that grows in-process (3, 5), and
  the immediate mitigation is a scheduled restart while the real cause is found.
- Cost tracks **load** and not uptime → contention (1, 2), and a restart buys
  nothing.

This needs a process nobody redeploys for a day. During this session prod ran at
least eight images in a working day, restarting every 10–20 minutes at times,
which is why this has not been separated already.

The sampler exists: `bench/pass-condition/conncost_watch.py` (2026-08-24). It
runs the `conncost.py` decomposition every 30 min and writes one CSV row per
sample carrying uptime, host load, scheduling lag, blocking-section holds, and
the uptime-growing state from hypothesis 3 (mem buffer, plan cache, provider
caches, charged bytes). `--analyze` correlates each cost stage against uptime
and against load *separately* — that pair of numbers is the whole deliverable of
Phase 0 — and refuses to read a series across a restart, because uptime resetting
mid-run means the samples either side are different processes.

    cd bench/pass-condition && python3 conncost_watch.py            # 30-min loop
    python3 conncost_watch.py --analyze                             # the verdict

**The quiet window, and who it binds.** Phase 0 and "push to master often" are
in direct conflict: master deploys, a deploy restarts prod, and a restart voids
the series. So the instrumentation above was front-loaded into one push
(`5e7934b`, 2026-08-24). **Any push to master that touches code resets the
clock** (`deploy.yml` `paths-ignore` already exempts docs-only pushes, for this
exact reason) — the
sampler keeps running and `--analyze` will simply report a shorter longest-run.
Nobody has to coordinate; they only have to know that a deploy is not free here.

**An autofmt child does NOT cost a second restart** — checked, because the
opposite is the intuitive guess and this file asserted it for an hour. The
autofmt workflow runs on every master push and commits its `cargo fmt` /
`clippy --fix` output as a new *code* commit (`5e7934b` produced `83423ee`, two
cosmetic reformattings of the new instrumentation). But that commit is pushed
with the workflow's `GITHUB_TOKEN`, and **GitHub does not trigger workflows from
`GITHUB_TOKEN` pushes** — `gh run list --workflow=deploy.yml` shows no run for
`83423ee`, and prod has stayed on `5e7934b` since. So a code push costs exactly
one restart, and the running image can sit one commit behind master's tip
indefinitely. Read `docker service ls` for what is *running*; master's tip is
not it.

The 2026-08-24 baseline against the *pre*-instrumentation binary, for reference:
`connect=2147 ms`, `query=1269 ms`, `reuse=749 ms` at host load 27.5 — the
symptom reproduces, and `runtime.uptime_seconds` came back **empty**, which is
the gap this instrumentation closes.

**Phase 1 — attribute the block, not the CPU.** With no CPU profiler in prod
(`TIMEFUSION_CPU_PROFILE=false` since the 2026-08-11 SIGSEGV crashloop, newest
flamegraph is Aug 12), the usual tool is unavailable, and the signal to chase is
*blocking*, not CPU anyway. Cheapest first:

1. Read `scheduling_lag_ms` / `scheduling_lag_max_ms` from `timefusion_stats` over
   the Phase 0 day. ~~It already exists and nothing has plotted it.~~ **It did
   not exist there.** The gauge was OTel-only, and `timefusion_stats` had no
   `runtime` component at all — no lag, and no process uptime either, so the
   view could not qualify a single one of its own process-scoped counters.
   Added (2026-08-24) as `runtime.{uptime_seconds, scheduling_lag_ms,
   scheduling_lag_max_ms, worker_threads}`.
2. Instrument suspected blocking sections with a "held for N ms" warn, starting
   with the journal mutex and the Delta snapshot refresh. **Done for the
   journal** (`observability::BlockWatch` / `Watched<T>`, exposed as the `block`
   component: `<section>.{count,total_ms,max_ms,avg_us}`, plus a warn over
   250 ms). **The Delta snapshot refresh is wrapped too, but under a different
   claim**: `refresh_table_snapshot` is `async` and deliberately does not hold
   the write lock across `update_state()`, so timing it as "blocking" would be
   a lie — awaits give the worker back. It reports under `section`
   (wall time only), the journal under `block` (worker occupancy). Every query
   on the resolve path pays the refresh and its cost scales with the active
   file list rather than with what the query reads, which is exactly the shape
   hypothesis 3 predicts: read `section.delta_snapshot_refresh.avg_us` against
   uptime.
3. Only if 1–2 are inconclusive, consider re-enabling the CPU profiler — noting
   that it took prod down once and that the signal wanted here is a blocked
   worker, which a CPU sampler shows as *idle*.

### First readings from the instrumented build (2026-08-24, uptime 198 s)

Deployed `5e7934b` and read it back. On a **fresh** process, before any of the
Phase-0 day has accumulated:

| row | value | what it says |
|---|---|---|
| `runtime.uptime_seconds` | 198 | the qualifier every other row needed |
| `runtime.scheduling_lag_ms` | 1 | fine right now |
| `runtime.scheduling_lag_max_ms` | **2,764** | **2.8 s of lag already, on a 3-minute-old process** |
| `block.journal_lock_wait.max_ms` | **0** (4,017 acquisitions) | nobody ever waits for this mutex |
| `block.journal_hold.max_ms` | 22 (avg 1.5 ms) | it is not hogging a worker either |
| `section.delta_snapshot_refresh.avg_us` | **53,510** (max 253 ms, 14 calls) | ~53 ms per resolve, paid by every query |

Two things follow immediately, and both narrow the plan:

- ~~**Hypothesis 2 is effectively dead.** Zero wait across 4,017 acquisitions and
  a 22 ms worst hold.~~ **RETRACTED 29 minutes later — see below.** That verdict
  was read off a 198-second-old process, which is the single trap this repo
  keeps falling into ([[tf_young_process_reads_as_fixed_2026-08-23]]): a
  young process reads as healthy. The half that survives is structural and
  does not depend on age — no `journal()` caller exists in the read path, so
  nothing queues *on the lock*.
- **Worker starvation is not exclusive to old processes.** 2.8 s of max lag
  inside the first 198 seconds means whatever blocks workers also blocks them
  when the process is new — so lag alone will not separate age from load either.
  Boot (WAL replay, snapshot materialization) is the obvious confound for a
  *max* this early; the Phase-0 day is what distinguishes "boot spike" from
  "steady-state starvation", by whether `scheduling_lag_max_ms` keeps climbing
  after the first hour.

**The first two samples of the window, same process, both drivers moving:**

| uptime | host load | connect | reuse |
|---|---|---|---|
| 198 s | 26.6 | 469 ms | 207 ms |
| 722 s | 37.2 | 1,433 ms | 354 ms |

Cost roughly tripled in nine minutes of process age — and load rose 40 % over
the same interval, so this pair is exactly as confounded as the pair that opened
this plan. It is two points; the day of samples is what separates them. Worth
recording only because it says the effect shows up within *minutes*, not hours,
which makes the Phase-0 day much more likely to succeed than "wait 5 h for the
slow state to build".

`delta_snapshot_refresh` is now the most interesting number in the set — 53 ms
of every query on a process three minutes old, and hypothesis 3 predicts it
grows. It is a wall-time row, so it is not a starvation claim; the point of
watching it is the slope.

### The same process 29 minutes later (uptime 1,917 s) — everything grew

One process, no restart, nothing redeployed. Cumulative averages understate the
change, so these are **marginals over the interval** (delta total ÷ delta count):

| | 198 s | 1,917 s | |
|---|---|---|---|
| `delta_snapshot_refresh` per call | 53.5 ms | **121.5 ms** | 2.3× |
| `delta_snapshot_refresh` max | 253 ms | **1,344 ms** | 5.3× |
| `journal_hold` per call | 1.54 ms | 2.24 ms | 1.5× |
| `journal_hold` max | 22 ms | **1,580 ms** | 72× |
| `journal_lock_wait` per call | **0.00 ms** | 0.21 ms | from nothing |
| `journal_lock_wait` max | **0 ms** | **1,426 ms** | from nothing |
| `scheduling_lag_max_ms` | 2,764 | 6,579 | 2.4× |

**Hypothesis 2 is back, and hypothesis 1 with it.** A `std::sync::Mutex` held
1.58 s occupies a runtime worker for 1.58 s, and somebody waited 1.43 s to get
it — on a process half an hour old, where 29 minutes earlier nobody ever waited
at all. The read path still never takes this lock, so the damage is indirect
(the worker, not the lock), which is exactly the surviving half of the
refutation above and exactly the shape `scheduling_lag_ms` reports.

**Hypothesis 3 is the cleanest signal so far.** `delta_snapshot_refresh` more
than doubled per call, on the resolve path every query pays, at ~12 calls/min.
Nothing about a `SELECT 1` should care, and the refresh cost scales with the
active file list rather than with the query.

Neither is proven — both moved while host load also rose, which is the
confound Phase 0 exists to break. What *is* established is that all three
suspects move on the timescale of **minutes**, so this does not need a 24-hour
vigil to resolve.

**Which is fortunate, because the day-long quiet window is not obtainable.**
Prod restarted twice in the first 50 minutes of it — not from this work
(`1e42237` was another worker's deploy), and the plan's own note already said
prod ran eight images in a working day. A protocol that requires everyone else
to stop deploying is not a protocol.

**So Phase 0 is re-scoped to the within-process slope, and the sampler now runs
every 5 minutes instead of 30.** At the 30-minute cadence a process that lives
~40 minutes yields *one* sample, and `--analyze` needs three in a run — the
series would have been all breaks and no runs. At 5 minutes an ordinary
inter-deploy life yields 6–10, which is enough to fit uptime against cost
*inside* one process, where the load confound still applies but the age axis is
clean. Each restart then becomes another replicate of the same short experiment
rather than the thing that ruins it.

### First `--analyze`: within one process, cost goes DOWN with age (n=3)

The longest uninterrupted run available so far is three samples spanning
184 s → 982 s of one process:

    connect    uptime_seconds: r=-0.94   load1: r=+0.00
    query      uptime_seconds: r=-0.94   load1: r=+0.03
    reuse      uptime_seconds: r=-0.93   load1: r=-0.01

`connect` went 1,420 ms → 298 ms → 263 ms as that process aged. **Negative**, and
load explains none of it. Over the first ~15 minutes a TimeFusion process gets
*faster*, which is what a cold Foyer/plan cache warming up looks like and is
consistent with the standing note that "idle == 4-min-old container == cold
cache".

**Extended to n=5 across 117 s → 1,357 s (22.6 min) of one process, sign holds:**
`connect` r=-0.77, `reuse` r=-0.69, against load r=-0.29 / -0.04. The series is
409 → 506 → 298 → 299 → 273 ms: it falls off the cold start and then **plateaus
at ~270–300 ms**. Through 22 minutes of uptime there is no degradation at all.

Three samples is not a result — the sign could flip with the fourth, and this
run covers 3–16 minutes of uptime while the observation that opened this plan
was at **five hours**. But it does say something already: the plan's framing —
"fresh is fast, old is slow" — is not what a single process does over its first
quarter hour. Whatever costs 1.4 s at 5 h either turns around later, or was
never about age. Runs long enough to cross an hour are what settle it, and those
need the deploy traffic to quiet down on its own.

Meanwhile the in-process counters keep climbing within every run
(`journal_hold.max_ms` 42 → 702, `charged_bytes` 13.7 GB → 47.6 GB in nine
minutes), so "state grows" and "queries get slower" are, so far, *not* the same
statement.

### The spike caught in-flight — three samples, one process, drivers separated

Correlations over a plateau hide the thing that matters, so `--analyze` now
prints the worst `connect` sample beside its neighbours. All three below are the
**same process**, five minutes apart:

| uptime | load | connect | reuse | `scheduling_lag_ms` | `refresh.avg_us` | `journal_hold.max_ms` |
|---|---|---|---|---|---|---|
| 170 s | 20.7 | 270 ms | 74 ms | 0 | 107,062 | 6 |
| **505 s** | **41.3** | **2,960 ms** | **680 ms** | **1** | **175,138** | 615 |
| 810 s | 33.8 | 252 ms | 70 ms | 0 | 144,271 | 1,186 |

Four things fall out of one table:

- **Age is not the driver.** Uptime rose monotonically while cost spiked 11× and
  came back. Whatever this is, it is not accumulated state — the process was
  *older* when it was fast again.
- **Workers were not starved.** `scheduling_lag_ms` was 0 → 1 → 0 straight
  through the spike. The "blocked workers while cores idle" story — hypothesis 1,
  the best fit on paper — is **not** what is happening in this event.
- **The journal mutex is eliminated for the spike.** `journal_hold.max_ms`
  climbs monotonically (6 → 615 → 1,186) and is *highest* on the fast sample. It
  grows with age; it does not track cost.
- **What does track it: host load, and the Delta snapshot refresh.**
  `refresh.avg_us` is cumulative, so a jump from 107 ms to 175 ms *average* means
  the marginal refreshes during that interval were far more expensive. Query cost
  and snapshot-refresh cost moved together, at peak host load, with no worker
  starvation — which points at IO/CPU contention with the co-tenant processes
  (the plan's own §3: monoscope was using ~15.8 of 48 cores) or at object-store
  latency, not at anything TimeFusion accumulates.

**The second spike replicates it, 16 minutes later and in a different process:**
uptime 194 s → 544 s → 850 s, connect 280 ms → **3,675 ms** → 258 ms, at load
29.2 → **49.5** → 39.2, with `scheduling_lag_ms` 1 ms throughout. Same shape,
same eliminations. Across all 17 instrumented samples the four slow ones
(`connect` > 800 ms) sit at **median load 39.2** against **29.9** for the
thirteen fast ones, and *every* slow sample had a scheduling lag of 0–1 ms.

**One ambiguity is still open, and `load1` cannot close it.** A host load of 49
is equally consistent with "the box got busy" and with "TimeFusion got busy" —
a flush or a maintenance burst *raises host load itself*, so the correlation
with load is not yet evidence of an external neighbour. The sampler now also
records `buffered_layer.{flush_completed_total, flush_failed_total,
pressure_pct}`; a flush counter that advances across exactly the slow samples
says it is our own work, and one that sits still says it is the neighbours. Both
spikes landing at ~8–9 minutes of uptime is a hint toward the former (the flush
interval is 600 s), and it is only a hint — the other two slow samples sat at
184 s and 722 s.

**First four samples with the flush counters — and load alone is already not
enough.** No spike landed in the window, but the quiet samples are informative:

| uptime | load | connect | flushes | pressure_pct | refresh avg_us |
|---|---|---|---|---|---|
| 280 s | 25.5 | 397 ms | 0 | 9 | 76,765 |
| 152 s | 23.4 | 356 ms | 0 | 8 | 150,398 |
| 462 s | 33.2 | 344 ms | 30 | 5 | 86,115 |
| 770 s | 39.4 | 412 ms | 61 | 5 | 86,074 |

Thirty-one flushes completed between the last two samples — roughly six a
minute, continuously — with `connect` flat at ~350–410 ms throughout. **Flushes
are not by themselves a stall**, which weakens "our own flush burst" as the spike
mechanism before any spike has even been caught with the counter attached.

And `load1` alone is not sufficient either: **load 39.4 here cost 412 ms**, while
slow samples sat at load 37–49. A monotone load→cost story would have predicted a
stall at 39.4. So the live lead narrows again — to a *threshold* or a burst that
`load1` averages away over its minute, not to load as a continuous driver.
Sampling moved to 180 s to resolve how long a spike actually lasts, which is the
next thing worth knowing: a stall that spans two adjacent samples is a different
animal from one that fits inside a single 6-rep probe.

This reorders the hypothesis table: **1 and 2 are eliminated for the spikes**
(directly, with the instrument built for them), 3 survives only as "state grows"
without yet explaining cost, and the live lead is contention — which is the
branch the plan predicted would mean *a restart buys nothing*.

### The measurement was destroying its own precondition

Ten samples across 30 minutes of one process, all fast (281–428 ms) at loads
from 23 to **44.7** — and load 44.7 cost 386 ms. That is higher than the load at
three of the four slow samples, so **`load1` is finished as an explanation**; the
earlier "slow median 39.2 vs fast 29.9" was an association that this run breaks.

What made this run possible is the actual finding. `docker service ps` shows
prod cycling through `c8860f5`, `86132a3`, `4aa69aa`, `5b38240` — **all of them
mine**. `paths-ignore` exempts `docs/**` and `**/*.md`; it does not exempt
`bench/**`, so every push to the sampler script deployed and restarted prod.
Five restarts in an hour, caused by the tool that exists to measure what happens
when a process is left alone. I had already written in this file that another
worker's deploy broke the window — that was true once and self-inflicted the
rest of the time.

`bench/**` is now in `paths-ignore` (it is python/shell tooling, not cargo, and
cannot change the running image).

**This also supplies a mechanism for the spikes that survives the evidence.**
Every slow sample sat inside a deploy: a *new* container replaying its WAL and
materializing snapshots while the *old* one still served traffic. That explains
what nothing else did — why the old process spiked at mid-life uptimes (722 s,
505 s, 544 s) rather than at any consistent age; why host load peaked without
TimeFusion's own scheduling lag moving (the load was the other container); and
why 30 quiet minutes at load 44.7 cost nothing. It is a hypothesis, not a
result: the test is whether spikes disappear now that the sampler no longer
triggers deploys, and that test runs itself from here.

### The quiet window: 0 slow samples of 15, and hypothesis 3 decoupled too

Since the sampler stopped triggering deploys: **15 samples, none above 800 ms**,
against 4 of 17 before. That includes a continuous 36-minute run at loads up to
44.7. The spike-free period is exactly the deploy-free period.

One sample in it retires the remaining suspect. At uptime 524 s,
`delta_snapshot_refresh.avg_us` read **403,842** — the refresh got roughly five
times more expensive than its usual ~80 ms — and `connect` was **305 ms**,
completely unmoved. So the earlier co-movement of refresh cost and query cost was
not causal; both were downstream of the deploy churn. Hypothesis 3 survives only
as "some state grows" and no longer as an explanation of query cost.

**What this does not explain, and I am not going to pretend otherwise:** the
observation that opened this plan was a **5-hour-old** process at 4,748 ms with
no deploy in progress. Nothing in this window got past 36 minutes of uptime, so
that regime was never reproduced. The honest state is two separate claims:

1. **Established here** — spikes of 1.4–3.7 s on a *young or mid-life* process
   are deploy churn: a new container replaying WAL while the old one serves.
   Instrumented, replicated four times, and gone once the churn stopped. This is
   also, retroactively, what most of the plan's own §1/§2 samples were: that
   session ran eight images in a working day.
2. **Untested** — whether a genuinely long-lived process degrades. It needs prod
   to stay up for hours, which is now more likely (bench pushes no longer
   deploy) but is not something this work can force.

The mitigation the plan hypothesized for a "cost tracks uptime" outcome — a
scheduled restart — would have been actively harmful: **restarts are the thing
that produced every stall measured here.**

### A second, smaller phenomenon survives the quiet window — and it IS the connection path

One slow sample appeared with no deploy anywhere near it (26 samples, 1 slow).
Lined up against the four deploy-churn stalls, it does not belong to them:

| uptime | load | dns | tcp | **connect** | **reuse** | |
|---|---|---|---|---|---|---|
| 722 s | 37.2 | 1.7 | 78.5 | 1,433 | **353** | deploy churn |
| 184 s | 28.4 | 1.4 | 123.5 | 1,420 | **198** | deploy churn |
| 505 s | 41.3 | 1.6 | 86.4 | 2,960 | **680** | deploy churn |
| 544 s | 49.5 | 4.3 | 103.2 | 3,675 | **1,249** | deploy churn |
| 2,057 s | **30.6** | 2.0 | 62.5 | **1,450** | **77.5** | quiet window |

In the churn stalls the *whole process* is slow — a warm `SELECT 1` costs
198–1,249 ms. In the quiet-window stall the warm query is **77.5 ms**, DNS and
TCP are normal, and only the phase between "socket open" and "ready for query"
— pgwire startup, auth, session setup — takes 1.4 s. Everything else about that
sample is calm: load 30.6, mid-life uptime, no restart on either side of it.

**This narrows the connection path back in, on better evidence than the plan
ruled it out with.** §1 discharged it because a warm connection paid the cost
too — but every sample §1 had was a churn sample, where everything pays. With
churn removed, what is left is connection-establishment-only, at ~4 % of samples.

The instrument for it already half-exists: the healthcheck probe times its own
`auth_ms` stage (see `spawn_runtime_lag_sampler`'s doc), and per-connection
session setup is the obvious suspect to time next. That is the natural Phase 2
if the rate holds — but at 1 in 26, it needs more samples before anyone
instruments a specific stage.

### One hour of uptime, and the process is at its fastest

The first undisturbed process to survive an hour reached **3,529 s** with:

    up=2794s load=25.5 connect=180ms reuse=45.0ms
    up=3162s load=24.7 connect=209ms reuse=43.8ms
    up=3529s load=39.0 connect=179ms reuse=42.6ms

`reuse` at 42–45 ms is **the plan's own healthy baseline** (46 ms, measured on a
4-minute-old process), and `connect` at ~180 ms beats the 219 ms that §2 called
healthy. At an hour of uptime, at load 39, with `journal_hold.max_ms` past 1,000
and gigabytes of allocation churn behind it.

So the age hypothesis is now falsified across the entire range this window can
reach: 3 minutes to 1 hour, cost is flat-to-improving. Five hours remains
unreached, but the burden has shifted — an hour of uptime produces the best
numbers in the dataset, not the worst.

**Still flat at 102 minutes** (`connect` 182–340 ms, `reuse` 42–68 ms, 1 slow
sample of 45). And in that stretch `block.journal_hold.max_ms` reached **2,380**
— a `std::sync::Mutex` occupied a runtime worker for 2.4 seconds — with
`scheduling_lag_max_ms` at 2,356 and **no effect on query cost whatsoever**.
That is the strongest disposal of hypothesis 2 available: the event it predicts
happened, at the magnitude it predicts, and queries did not notice. The 48
workers absorb it.

### At ~108 minutes the slow regime arrived on its own — and hypothesis 4 is the live lead

The flat stretch ended without a deploy. From uptime 6,494 s onward, five of six
samples were slow, with **`reuse` slow too** (191–627 ms) — the whole-process
signature, not the connection-only one:

| uptime | load | connect | reuse | lag_ms | hold_max | refresh avg_us | charged |
|---|---|---|---|---|---|---|---|
| 4,449 s | 44.0 | **178 ms** | 45 ms | 0 | 1,064 | 179,821 | — |
| 6,494 s | 43.0 | **852 ms** | 302 ms | 1 | 2,380 | 120,082 | 25.5 GB |
| 6,704 s | 38.4 | 1,074 ms | 192 ms | 12 | 2,380 | 118,520 | 13.1 GB |
| 6,993 s | — | 3,049 ms | 525 ms | 1 | 2,380 | 117,750 | 26.5 GB |
| 7,399 s | 47.1 | 705 ms | 627 ms | 0 | 2,380 | 115,225 | 17.4 GB |
| 7,611 s | 49.4 | 1,025 ms | 151 ms | 1 | 2,380 | 113,838 | 15.6 GB |

Load 43.0 costs 852 ms here; load 44.0 cost 178 ms an hour earlier. So it is
**not load**, and it is not the counters this plan built either — `hold_max` is
frozen at 2,380, `refresh_avg_us` is *falling*, `pressure_pct` is 0–7, and
`scheduling_lag_ms` is 0–1 through all of it. **TimeFusion's own runtime is not
starved while its queries take 600 ms.**

`top` on the host during the slow window says what the in-process counters
cannot:

    %Cpu(s): 37.7 us, 17.4 sy, 40.2 id, 4.3 wa
    3740410 timefusion   861.5% CPU   18.9g RES
    3617008 monoscope    430.8%       3940135 monoscope 407.7%
    318     kcompactd    100.0%  (kernel memory compaction, pinned)
    3623661 postgres     100.0%       3667422 postgres  100.0%
    MiB Swap: 2048.0 total, 0.2 free, 2047.8 used

Three facts together: **TimeFusion is now the top consumer at 861 %** (8.6 of 48
cores, up from the 554 % §3 recorded), **`kcompactd` is pinned at 100 %**, and
**swap is 100 % full** — while the box is still 40 % idle. Meanwhile
`charged_bytes` oscillates between 13 GB and 26 GB every few minutes: TimeFusion
is allocating and releasing ~13 GB repeatedly, which is precisely what drives
kernel compaction pressure.

**That makes hypothesis 4 the live lead, not the dead one.** Not "swap thrashing"
as originally framed, but its neighbour: sustained multi-gigabyte allocation
churn keeping `kcompactd` saturated, so allocation latency rises box-wide. It
fits every observation the others failed — cost that ignores load, workers that
are never starved, a `SELECT 1` that costs 600 ms, and an onset ~2 hours in
rather than at a fixed age. The counters that would confirm it are jemalloc's
own (fragmentation, dirty page decay), which this codebase has tangled with
before.

### CORRECTION: the plan's original premise is CONFIRMED. Age wins.

At uptime **8,052 s (2.24 h)**, load **32.9**: `connect` = **4,460 ms**,
`reuse` = **807 ms**. That is the plan's opening measurement reproduced
(4,748 ms at 5 h) on a process nobody touched, at a load *lower* than samples
that cost 178 ms.

So the earlier heading in this file — "the age hypothesis is now falsified" —
was **wrong, and wrong in the way this repo keeps being wrong**: it generalized
from a range that did not include the phenomenon. Everything up to ~1.2 h was
genuinely flat; the onset is later. Corrected picture:

| process age | behaviour | evidence |
|---|---|---|
| 0–3 min | slow (cold caches) | connect 400–1,400 ms, falling |
| 3 min – ~1.2 h | **flat, and the best of the day** | connect ~177 ms at loads 24–44, 40+ samples |
| ~1.8 h – 2.2 h | **degrading badly** | connect 705 → 852 → 1,025 → 3,049 → **4,460 ms** |

And load is now decisively eliminated in *both* directions: **44.0 → 178 ms**
(1.2 h) versus **32.9 → 4,460 ms** (2.2 h). Higher load, twenty-five times
faster. What is left standing is the thing the plan named first and I twice
talked myself out of: **something in-process grows, and past roughly 1.5 hours it
starts costing every query, including `SELECT 1`.**

The deploy-churn finding survives intact and is still worth having — it explains
the *young-process* stalls, it was destroying the measurement, and the fix is
merged. It was simply not the whole thing. The honest sequence is: churn masked
the real signal, removing the churn made a clean window possible, and the clean
window then took two hours to show what it was always going to show.

`kcompactd` was at 0 % in this sample after being pinned at 100 % twenty minutes
earlier, so the allocation-pressure lead is *not* confirmed either — one
instantaneous `top` is not a time series, which is why the sampler now records
`kcompactd_cpu`, `swap_free_mb` and `iowait_pct` on every row.

### Qualifier, one hour later: the degradation is EPISODIC, not monotone

At 2.5–2.9 h of uptime — the same process, no restart — `connect` is back to
**280–420 ms** across eight samples, with `kcompactd` at 0 % throughout, swap
100 % full the whole time (so swap is constant and cannot be a differentiator),
and `iowait` between 0.2 % and 18.1 % on *fast* samples alike.

So the 1.8–2.24 h slow stretch was an **episode that ended on its own**. Cost is
not a monotone function of age; a process at 2.9 h is fine. Both of my previous
headings were too strong in opposite directions, and the shape that fits all of
it is: **occasional multi-minute episodes where everything gets slow, which
resolve without intervention** — deploy churn is one reliable trigger for them,
and there is at least one other trigger that is not a deploy.

That reframes what to look for. Not "which quantity grows with age" but "which
quantity spikes during an episode and recovers". Anything monotone — uptime,
`journal_hold`, cache entry counts — is the wrong shape by construction.

### First jemalloc reading (deployed `c3fb011`, uptime 4,873 s)

    jemalloc.allocated_mb   10,943      jemalloc.resident_mb   13,838
    jemalloc.active_mb      11,420      jemalloc.mapped_mb     15,436
    jemalloc.frag_pct         20.9      jemalloc.retained_mb  147,247

**`retained` is 147 GB** — address space jemalloc has decommitted but kept
mapped, on a box with 192 GB of RAM and a 120 GB cgroup limit. It is not
resident, so it costs no RSS, but it is the accumulated record of exactly the
churn `kcompactd` was seen burning a core on. `frag_pct` at 20.9 % (2.9 GB
resident behind no live allocation) is the number to watch across the next
episode: the prediction is that it climbs into one and falls out of it. If it
stays flat through a slow window, fragmentation is out too and the remaining
suspect is the connection-setup path itself.

Note the deploy that carried this **failed its own post-deploy gate** —
"replacement returned 57P03/not-ready continuously for 12,011 ms (budget
10,000 ms)" — with `WAL recovery 0ms`. The image is live and serving; the gate
measures handoff readiness, and 12 s against a 10 s budget on a box at load ~35
is its own small signal about startup under contention. Not chased here.

### Handler construction is NOT the connection stall — and that points outside the process

`62f2385` deployed and reporting, over 135 connections:

    block.pgwire_simple_handler_build     avg 876us   max 2ms
    block.pgwire_extended_handler_build   avg 768us   max 4ms
    block.pgwire_startup_handler_build    avg   4us   max 0ms

Per-connection setup inside TimeFusion costs **under 5 ms, worst case**. It
cannot be the 1,450 ms `connect` sample. Combined with auth being a string
compare, essentially nothing in this process's connection path is expensive.

**So the search moves outside the process — and there is an obvious candidate
this plan never mentioned: `srv-timefusion-pgwire-proxy`, the haproxy in front
of pgwire.** Every measured connection traverses it, and its config is built to
produce exactly this signature: `timeout connect 1s` with `fall 2` on a 250 ms
`tcp-check`, so a backend marked DOWN makes *new* connections pay a connect
timeout plus retries while *established* sessions are untouched.

**Refuted within the hour, by its own logs.** Over 8 hours haproxy logged 17
`is DOWN` transitions, every one of them `Layer4 connection problem, Connection
refused` — the signature of a container that is gone, i.e. a deploy restart, not
a health-check timeout. Lining the transitions up against every slow sample
recorded all day:

| slow sample | connect | nearest DOWN/UP |
|---|---|---|
| 12:00Z | 1,433 ms | 87.8 min away |
| 13:20Z | 2,960 ms | 7.5 min away |
| 15:02Z | 1,450 ms | 33.7 min away |
| 16:23Z | 3,049 ms | 97.6 min away |
| 16:42Z | 4,460 ms | 78.8 min away |

Not one stall is within 7 minutes of a proxy transition; most are an hour or two
away. The proxy is not flapping during the stalls, and its DOWN events are the
deploy churn already accounted for. **Hypothesis raised and killed in the same
session — worth recording precisely so nobody re-raises it from the config
alone, which reads guilty.**

What that leaves inside connection establishment, having eliminated DNS, TCP,
auth, handler construction and the proxy: the pgwire **startup exchange itself**
and the **accept path**. A slow accept fits the signature exactly — the kernel
completes the TCP handshake from the listen backlog (so `tcp` stays fast) while
the startup message waits for a task that has not been polled yet. That is the
next thing to time, and it needs a timer around accept-to-first-message rather
than around anything measured so far.

**Second finding, from the same read:** `jemalloc.retained_mb` is **21,127** on a
184-second-old process, against **147,247** on an ~80-minute-old one. Retained
address space grows roughly 7× over an hour of uptime. That is the first
quantity found that genuinely accumulates with age — which is what "state grows
in-process" was always looking for, though it is address space rather than
resident memory, so it costs no RSS and is not yet tied to query cost.

### VERDICT on the connection stall: server-side, entirely in the startup exchange

The container's own healthcheck runs the same pgwire startup+auth exchange every
5 seconds **over loopback**, and reports it split by stage. Sampling the worst of
its last five probes alongside every external sample:

| my `connect` (internet) | in-container `total` | `connect_ms` | `write_ms` | **`auth_ms`** |
|---|---|---|---|---|
| 149 ms | **1,423 ms** | 0 | 0 | **1,423** |
| 160 ms | **470 ms** | 0 | 0 | **470** |
| 172 ms | 1 ms | 0 | 0 | 1 |

**The stall is real, server-side, and 100 % of it is `auth_ms`** — the window
between "startup packet written" and "auth response read". Over loopback, so
the network, the haproxy and my laptop's path are all excluded by construction.
`connect_ms = 0` excludes the accept itself. And `block.pgwire_*_handler_build`
maxes at 2–4 ms, so it is not handler construction; auth is a string compare, so
it is not the credential check.

Note the first two rows: my external `connect` was **fast** (149–160 ms) while
the server was stalling. The two are near-independent, which is why 26 external
samples found this once — I was sampling a frequent server-side event through a
narrow, badly-timed window. **The healthcheck was a better instrument than the
one I built, and it was already running.**

What remains inside that window is the connection's task being polled at all,
plus pgwire's own startup processing (parameter status, ready-for-query). The
scheduling reading supports the former: `scheduling_lag_max_ms` sat at
2,764–8,177 ms in these same processes while the instantaneous
`scheduling_lag_ms` read 0–1 ms — multi-second polling gaps that a 500 ms timer
sampler almost always misses. That is a *hypothesis*, not a result: the sampler
now records `inproc_auth_ms` and `scheduling_lag_max_ms` on the same row, and
their correlation is the next thing to read.

### The lag test: inconclusive by saturation, because the runtime is late CONTINUOUSLY

`spawn_runtime_lag_sampler` has been writing a timestamped `warn!` per event at
≥250 ms all along, and nothing had read it. Lining those up against the three
healthcheck windows above:

| window (UTC) | `auth_ms` | lag warns inside it |
|---|---|---|
| 20:49:05–20:49:30 | **470** | 518, 593, 500 ms |
| 20:51:09–20:51:34 | **1** | 290, 345, 545, 313 ms |

**The hypothesis cannot be tested this way: the warns never stop.** Over twelve
minutes there are ~40 of them — several a minute, 275 ms to 2,850 ms — in stall
windows *and* in the window where `auth_ms` was 1 ms. A cause that is always
present cannot be correlated with an effect that is intermittent.

But the negative result is smaller than what it uncovers. **This process fails
to wake a 500 ms timer on time, by 0.3–2.9 seconds, several times a minute,
continuously, on a 48-worker runtime and a host that is 40 % idle.** That is not
a connection problem or a query problem; it is the runtime being persistently
unable to poll a trivial task. The auth stalls (470–1,423 ms) sit squarely
inside that distribution, which is consistent with causation without proving it.

Proving it needs the timing *inside* the server — accept-to-startup-handled per
connection — not more correlation of two things measured from outside. That is
a code change and it is the first move for whoever picks this up. Note the
instrument shape trap while doing it: `scheduling_lag_max_ms` is a monotone
high-water mark (already 2.8–8.2 s, so a 1.4 s event cannot move it) and
`scheduling_lag_ms` is an instantaneous read that misses sub-second events —
**neither column can ever correlate with an episodic stall.** The per-event
`warn!` is the only record that can, which is why the logs answered what the
counters could not.

**What to do next, in order:**
1. Let the process keep ageing with the host columns recording. The question is
   now narrow: which in-process quantity is monotone across the 1.2 h → 2.2 h
   transition? `journal_hold` froze, `refresh_avg_us` *fell*, `pressure_pct` is
   low — none of the current instruments explain it, so the next one has to come
   from jemalloc's own stats (fragmentation, dirty decay) or from a per-stage
   breakdown of connection setup.
2. Do **not** ship a scheduled restart yet. It would help this, and it would
   reintroduce the churn stalls that dominated the first half of this
   investigation. Measure which is worse before trading one for the other.

### THE FIX (2026-08-25, `f7378af`): fsync was freezing workers, and their queues with them

Phase 1 named "workers are blocked, not busy". This is what was blocking them.

`TaskJournal::checkpoint` does `write_all` + **`sync_all`** while holding the
journal mutex, and it is called from **46 sites** — every claim, every
completion, every retry, with 16 maintenance workers cycling. `compact`
additionally serializes every live task (thousands) before its own fsync. All
synchronous, all on tasks running on the shared runtime. `block.journal_hold.max_ms
= 2,380` was measuring precisely this.

The damage is not to the caller. A worker inside a blocking syscall cannot poll
**any** of the tasks queued on it, so an fsync freezes unrelated work — a
health probe, a timer, a client's connection setup. That is the whole distance
between "maintenance does IO" and "`SELECT 1` costs seconds".

`support::without_blocking_the_worker` runs such work through
`tokio::task::block_in_place`, which hands the worker's remaining tasks to
another thread *first*. Applied at three places: the journal fsync, the compact
serialize+fsync, and inside `write_atomic_with` — the shared helper behind the
storage sidecars, the rollup journal and WAL metadata, where `durable` costs two
fsyncs. Outside a multi-thread runtime it calls straight through, so CLI
subcommands and tests are unaffected. The mutex is still held across the IO:
durability ordering is unchanged. `store_snapshot` was already on
`spawn_blocking` and was left alone.

**The test measures the right thing, which took two attempts.** One worker, a
blocking task, a neighbour queued behind it: naive, the neighbour waits the full
400 ms; through the helper it runs immediately (verified failing without the
fix at 405 ms). The first version measured *timer lag* and passed even unfixed —
tokio's time driver can be driven by the idle `block_on` thread, so timers keep
firing while the worker is held. A probe that cannot see the defect is worse
than no probe.

**Verification, prod — confirmed at matched load.**

| | pre-fix (`18bcf8a`) | post-fix, 1st read | post-fix, **load-matched** (`44d07e1`) |
|---|---|---|---|
| lag warns / 20 min | **114** (5.7/min) | 45 (2.25/min) | **35 (1.75/min)** |
| host load (15-min avg) | ~30 | 23.45 | **30.47** |
| `journal_hold.avg_us` | 2,815 | **1,610** | |

The first post-fix read was taken at load 23.45 and was therefore confounded —
a 22 % lighter box could explain a lower rate on its own. The scheduled window
landed at a **15-minute load average of 30.47, matching the baseline**, with the
1-minute average at 38.84 — i.e. *equal or busier* — and measured **1.75/min
against the baseline's 5.7/min**. Same load, **3.3× fewer stalls**, and below
the pre-fix variance band (3.3–5.7/min). The reduction is attributable.

The first 10 minutes after the deploy were discarded before measuring — a fresh
process throws a lag burst during WAL replay (`inproc auth_ms` read 1,252 ms at
41 s of uptime), and counting that would have read *worse* than reality.

**And warns did not go to zero, so other blockers remain.** 1.75/min means
workers are still being held by something this fix does not cover — sync
object-store metadata reads, zstd paths outside `write_atomic_with`, or
CPU-bound work that never yields. `BlockWatch` is the instrument for finding
them: wrap a suspect, read `block.<name>.max_ms`. That is the next session's
work, not a reason to discount this one.

Note on comparing the two runs: use `avg_us` and rates, **never `max_ms`**. The
maxes are monotone high-water marks, so the 19-minute process reads lower than
the 7-hour one for reasons that have nothing to do with the fix — the same
instrument-shape trap recorded above.

### The premise, settled at 7.15 hours — the age the plan was written about

A process finally survived long enough (nobody deployed for seven hours), and it
is the fastest sample in the entire investigation:

| | plan's §1 (5 h uptime) | this process (**7.15 h**) |
|---|---|---|
| `connect` | 4,748 ms | **107 ms** |
| warm `SELECT 1` | 2,969 ms | **23.5 ms** |
| in-container `auth_ms` | — | 8 ms |
| host load | 39 | 30 |

**"A trivial query costs seconds after hours of uptime" is false.** The title
premise is refuted at a *longer* uptime than the one that produced it, and the
§1 numbers were a process being crushed by a concurrent deploy, as this
investigation found by accident when it caught itself doing the same thing.

And the state that "grows with uptime" grew exactly as predicted while cost did
not follow it: `retained_mb` **221,291** (221 GB of decommitted address space),
`frag_pct` 19.2 %, `journal_hold.count` **606,953**. That last one is the
justification for the fix on its own terms — **606 thousand blocking fsyncs in
7 hours (~24/second), totalling 1,708,649 ms of frozen-worker time, or 28
minutes of a worker doing nothing but waiting on the disk.**

**Phase 2 — fix what Phase 1 names.** Deliberately unspecified. The failure mode
to avoid is the one this session already hit twice: proposing a mechanism
(a size limit; a witness rewrite) before measuring, then discovering the premise
was wrong.

## 6. What NOT to do

- **Connection pooling as a fix.** A warm connection pays the same cost; pooling
  hides the 4.6 s setup and leaves the 1.4 s `SELECT 1` untouched. Worth doing for
  its own sake, not as a fix for this.
- **Adding CPU or raising a CPU limit.** There is no limit, and the host is half
  idle.
- **Quoting either sample as "the" number.** 4,748 ms and 219 ms are the same
  system 4 minutes apart. Any single-sample connection number from this
  deployment is a statement about process age and load, not about connecting.

## 7. Reproduce

```bash
cd bench/pass-condition && python3 conncost.py     # needs ./tfurl, read-only
```

Records DNS / TCP / connect / query / reuse medians over 6 reps. Pair every run
with `docker service ps srv-captain--timefusion` (uptime, and the **running**
image — `ls` shows the configured one) and `uptime` on the host.
