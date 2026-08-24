# `SELECT 1` costs 1.4–3.0 s on an old process and 46 ms on a fresh one

2026-08-24, prod, read-only. Started as "opening a connection costs 2.3 s"; the
connection turned out to be innocent, and so did the host. What is left is a
process that gets ~20× slower over hours of uptime.

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
