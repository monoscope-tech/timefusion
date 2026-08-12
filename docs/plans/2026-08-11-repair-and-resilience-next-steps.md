# Repair & resilience: next steps (2026-08-11)

Written after a day that shipped three repair fixes, unlocked the whale's 10-day
window, and produced a 13-minute total outage. State at writing: prod healthy on
`6ef51a8`, whale tier **36** (07-31=36, 08-01=0).

The ordering below is deliberate: **availability before throughput.** Today's
outage was triggered by a bug we had classified as a maintenance nuisance.

---

## Where we actually are

| Thing | State |
|---|---|
| Whale poison, 07-31 | 36 files / ~12 GB, none over the 1 GiB admission cap |
| Whale poison, 08-01 | **0 — clean** |
| 10-day query | `mode=bounded`, 8.1s (was an error) |
| 11d+ | `full-set` — day 11 *is* 07-31 |
| Repair throughput | ~13 min/bin, stage+commit in one pass |
| Prod | healthy, `6ef51a8` |

Shipped today: streaming per-bin commits (`03dccf0`), packing stand-down
(`03dccf0`), process-wide repair permit (`60740ec`), CI smoke-step fix
(`fecd23f`).

---

## P0 — availability

### 1. The `exit 137` health-kill (root cause unknown)
The trigger for today's outage: SIGKILL → restart → SIGSEGV crashloop → 13 min
down. Previously rated "kills in-flight repair"; it can also leave the process
unable to boot.

What we know: probe is `--timeout=5s --retries=5` with `PROBE_OP_TIMEOUT` 1500ms,
so ~25+ s unresponsive. Measured during a repair pass: **CPU 805% of 4800%,
memory 17.8 of 96 GiB** — so neither CPU saturation nor OOM.

Next: instrument the probe path rather than widen it. Log a timestamp at accept,
at auth, and at first response, so a slow probe says *which stage* was slow.
Suspicion worth testing first: the probe shares the single global tokio runtime
(`main.rs:96`) with CPU-bound maintenance sorts that don't yield. If so, the fix
is a dedicated runtime for the health listener, not a longer deadline.

**Do not widen the probe before this is understood** — it would mask the only
signal we have.

### 2. The unexplained SIGSEGV
> **The evidence is GONE (checked 2026-08-12).** There is no `wal.broken-*` anywhere on the
> host — `/home/ubuntu/timefusion-data` holds only live directories. So the offline
> reproduction below is no longer available, and this item cannot be worked as written.
>
> What is left: the CPU-profiler hypothesis is now testable by env var
> (`TIMEFUSION_CPU_PROFILE`) without needing the corrupt WAL at all, and that is the cheapest
> remaining move. If the segfault recurs, **copy the WAL off the host before restarting** —
> that is what was missed here, and it cost the only reproduction.

Between 12:14 and 12:23 every boot segfaulted (exit 139, no Rust panic). Evidence
was preserved at `/home/ubuntu/timefusion-data/wal.broken-1786451053`.

Reproduce offline: copy that dir, point a local binary at it, run under
`gdb`/`catchsegv`. No panic text ⇒ unsafe/FFI, so `RUST_BACKTRACE` will not help.
Two known-good images crashed identically, so it is **state**, not code.

**CORRECTION (16:30):** I listed the `walrus-rust` `StorageImpl::read`
`debug_assert!` as a missing bounds check. It is not — the line under it is
`&mmap[offset..offset + dest.len()]`, a Rust slice index, which is bounds-checked
and PANICS on overrun in release too. The `debug_assert!` only improves the
message in debug builds. No UB, no fix needed, and it is not a SIGSEGV candidate.
Truncation behind a live mmap would also give SIGBUS (135), not 139.

That leaves the **CPU profiler** as the only unsafe-adjacent thing at that point
in boot (signal handlers + libunwind). Gated behind `TIMEFUSION_CPU_PROFILE` as
of this plan's follow-up, so the hypothesis can be tested by env var instead of
by shipping an image into an outage.

> **THE TEST HAS NEVER BEEN RUN (checked 2026-08-12).** The gate is
> **default-ON** — `profiling.rs:47` only disables when the var is explicitly
> `false`/`0`. `TIMEFUSION_CPU_PROFILE` is **not set** on the prod service, and
> prod logged `profiling: enabled (jemalloc heap auto-dump + rolling CPU
> flamegraph)` at 14:43 UTC today. So the prime suspect has been live in
> production continuously since the incident, and writing this gate did not
> neutralise anything — it only made the test *possible*.
>
> To actually run it: set `TIMEFUSION_CPU_PROFILE=false` on the service. Heap
> profiling is unaffected (jemalloc's own, via the baked `malloc_conf`), so OOM
> attribution survives; what is lost is the rolling CPU flamegraph.
>
> **APPLIED 2026-08-12 14:51 UTC.** `TIMEFUSION_CPU_PROFILE=false` is set on the
> service (via the CapRover API, full-replace with every other field preserved —
> today's `stop-first` included). Prod now logs `profiling: jemalloc heap
> auto-dump only — CPU sampler disabled by TIMEFUSION_CPU_PROFILE`, replicas
> `1/1`, heap dumps still landing.
>
> The restart cost is bounded by design and that is why this was worth doing now
> rather than saving for a quiet moment: resumable footer repair is on by default
> (`49c5adf`, "turn resume on by default so a rewrite survives a restart"), so a
> restart costs the pass its current bin, not its progress. The certification
> counters do reset — but that measurement no longer needs a clean window, since
> the coverage finding was derived from the code (see the coverage plan).
>
> **The clock starts here.** The hypothesis is that the CPU sampler (signal
> handlers + libunwind) caused the 2026-08-11 exit-139 crashloop. The test is
> simply: does a SIGSEGV recur with the sampler off? A recurrence falsifies it and
> the search moves on; a long quiet period is weak evidence for it, since the
> original was a burst between 12:14 and 12:23 and not a steady failure. What is
> lost meanwhile is the rolling CPU flamegraph — if perf work needs it back, flip
> the var and note the gap in coverage.

### 3. CapRover `serviceUpdateOverride` — one word
> **DONE 2026-08-12.** `UpdateConfig.Order` is now `stop-first`, applied through the CapRover
> API with every other field of the app definition preserved (StopGracePeriod 90s, the memory
> limit/reservation, the seccomp override). Takes effect on the next deploy; nothing was
> restarted to apply it. `"FailureAction": "pause"` is untouched and still explains the wedge
> shape — a separate call to make deliberately.

The timefusion app config literally contains `"Order": "start-first"`, which is
the WAL-lock deadlock. This is *why* every manual `stop-first` gets reverted; it
was never a CapRover quirk. Change to `"stop-first"` in the UI. Needs the
CapRover password.
Also there: `"FailureAction": "pause"` explains the wedge shape (failed rollouts
pause and accumulate not-ready tasks rather than rolling back).

---

## P1 — finish the whale

### 4. Drain 07-31 (36 files)
> **Measured 2026-08-12. The whale is `87576849-4941-49d3-a15d-680fef88a1a8` — project
> "past3", i.e. monoscope-self.** Recording the id here because every verification step needs
> it and it was written down nowhere.
>
> **Read the probe methodology note first — it is cache-sensitive.** Sweeping the 30
> most-recently-active projects at 30d:
>
> | | bounded | full-set | no DedupExec |
> |---|---|---|---|
> | first (COLD) sweep | 11 | **19** | 0 |
> | re-run (WARM), twice, identical | 11 | **1** | 18 |
>
> Same query, same projects, minutes apart. Warm, 18 projects plan with no `DedupExec` at all;
> cold, those same 18 plan `full-set`. So **an EXPLAIN probe does not measure repair progress
> unless cache warmth is held constant** — "11d flipped to bounded" can be warmth, not repair.
> This matches the known `dedup_skip` behaviour that needs a warm fast-resolve cache. Any
> future use of this probe must state whether it was cold or warm; a cold sweep is the
> conservative one.
>
> Substantively, warm state: **29 of 30 active projects are healthy at 30 days.** The one that
> is not is past3, and it is worse than this plan describes — `full-set` at **every** width
> probed, 1d / 3d / 7d / 9d / 11d / 14d / 30d. The 2026-08-09 profile had it bounded through
> 8d and full from 9d, which localised the problem to `date=2026-07-31`. Full-set at **1 day**
> cannot be a 07-31 partition: something is voiding ordering in past3's *recent* data too, so
> draining 07-31 alone will not fix it.
>
> Not the flush escalation path: `flush_sort_unsorted_fallbacks_total` is **0**.
>
> **MEASURED 2026-08-12 from the Delta log — this section's numbers are wrong by ~30x.**
> Counting **active** add files (checkpoint v478202, `numOfAddFiles=2494` table-wide) for
> past3, rather than inferring from EXPLAIN:
>
> | date | active files | size |
> |---|---|---|
> | …up to 07-19 | 1–8 each | small |
> | 07-20 | 30 | 24.8 GB |
> | 07-21 | 70 | 55.6 GB |
> | 07-22 | 116 | 91.3 GB |
> | 07-23 | 113 | 86.0 GB |
> | 07-24 | 121 | 93.4 GB |
> | 07-25 | 33 | 33.0 GB |
> | 07-26 | 76 | 57.3 GB |
> | 07-27 | 70 | 54.4 GB |
> | 07-28 | 99 | 85.3 GB |
> | 07-29 | 53 | 38.8 GB |
> | 07-30 | 63 | 46.3 GB |
> | **07-31** | **82** | **36.3 GB** |
> | **08-01** | **80** | **35.2 GB** |
> | 08-02 | 32 | 13.5 GB |
> | 08-03 … 08-11 | 1–11 each | small |
>
> Three corrections, each load-bearing:
>
> 1. **07-31 is 82 active files / 36 GB, not "36 files".**
> 2. **08-01 is NOT clean.** This plan records "08-01=0"; it has **80 active files / 35 GB**.
>    The whale-tier counter that said zero was measuring something else.
> 3. **It is a 13-day band, not one partition: 07-20 … 08-02 = 1038 active files, ~751 GB.**
>    Every date outside that band is compacted to 1–6 files, so compaction works normally —
>    something specific to that window left ~750 GB uncompacted and it never caught up.
>
> So "drain 07-31's 36 files, ~8h" was never the shape of this job. At the plan's own ~13
> min/bin it is weeks, not hours, and it should be planned as a backlog-burndown with a
> throughput target — not a one-shot repair. **Do not start it as written; size it first.**
>
> Method note, because it matters for the next person: every EXPLAIN-derived number in this
> section (mine and the original) is unreliable — the same probe returned no-`DedupExec`,
> `bounded` and `full-set` for one query inside an hour. The Delta checkpoint is ground truth
> and costs one object read. Physical object listing is NOT ground truth either: 07-31 lists
> 124 parquet objects against 82 active, the rest being retention-protected tombstones.

> **Superseded correction (kept for the reasoning): the "do NOT drain" conclusion below is
> NOT supported — I over-read an unstable instrument.** Probing single dates directly gave `full-set` for
> BOTH `2026-07-31` and `2026-07-30` (a date this plan never mentions) while `2026-08-01` was
> bounded — i.e. the legacy poisoning looks real and possibly wider than 36 files. Minutes
> later the same single-date probes returned **no `DedupExec` at all**. Same query, same
> project, three different answers within an hour: no-DedupExec / bounded / full-set.
>
> **Treat every EXPLAIN-derived claim in this section — including this plan's original
> "10d bounded, 11d full-set" — as unreliable.** The sliding-window table below is real as far
> as it goes, but it only ever covered *recent* dates, so it cannot support any conclusion
> about 07-31. Mine did, and that was wrong.
>
> Ground truth instead of plan shapes: `repair_verified_sorted.txt` holds, for past3,
> **one** verified-sorted file each for 07-17..08-02 (with 07-24 and 07-30 absent entirely),
> against 21 / 67 / 52 / 239 for 08-08..08-11. Whether "1 verified file" means *clean because
> compacted down to one file* or *1 of 36 done* decides the whole question, and it needs the
> partition's actual file count — which is the one cheap measurement still missing. Get that
> before deciding to drain or not; do not decide from EXPLAIN.
>
> The rolling fresh-flush effect described below is a separate, additional problem. Both can
> be true.

> **Follow-up on the recent-window behaviour.** past3 has
> **399 files in `repair_verified_sorted.txt`**, so repair has not been ignoring it. Sliding
> the window instead of widening it locates the real thing (2-day window, varying only where
> it *ends*):
>
> | 2d window ending | mode |
> |---|---|
> | now | full-set |
> | 2h ago | full-set |
> | 6h ago | **bounded** |
> | 12h / 18h / 24h ago | **bounded** |
>
> Ordering is voided by the **most recent few hours of flush output**, not by anything in
> 07-31. Any window that excludes the last ~2–6h is bounded; any window that includes it is
> full-set. Freshly flushed files are unsorted until compaction reaches them, and every
> dashboard that covers "now" — which is most of them — pays full-set until it does.
>
> It also **rotates between tenants**. Minutes after the table above, past3's own last-2h had
> gone bounded while `edb04135` (Blockradar), bounded at 30d in the earlier sweep, had gone
> full-set at last-2h. So "the whale" is not a property of one project; it is whichever tenant
> compaction is currently behind on, and past3 shows it most because it ingests most.
>
> This retires the model this section was written around (one poisoned partition, one whale,
> ~8h of repair to drain it). The open question is now a throughput one: **how long flush
> output stays unsorted before compaction sorts it, and whether that lag is bounded for a
> high-ingest tenant.** Measure that before spending repair time.
>
> Caveat that applies to every number above: the probe is both cache- and time-sensitive (see
> the cold/warm table). Single readings mislead; slide the window and repeat.

No blockers: every file is under the admission cap, largest 631 MB. At ~13
min/bin that is ~8 hours of uninterrupted repair; the real rate is set by how
often the process restarts.

Verify with the EXPLAIN probe as it drains: 11d should flip to `bounded` when
07-31 is clean, then chase 128 files → 14-day and 663 → 30-day.

### 5. Enrichment writes are failing for the whale
> **The described failure is GONE (measured 2026-08-12, 6h of prod logs).** The plan said to
> confirm rather than assume; confirmed:
>
> | signature | count in 6h |
> |---|---|
> | `SortPreservingMergeExec` | **0** |
> | `full-set` / `mode=full` dedup fallback | **0** |
> | `Resources exhausted` | 11 — but **all** `ExternalSorter[N]`, none MOR dedup |
>
> Zero full-set fallbacks anywhere means the read-side ordering problem this item was about
> has cleared. What is left is a *different* failure the plan anticipated ("if it survives the
> repair, it is a separate bug") and it is not the read path at all: `ExternalSorter`
> allocation failures on the **compaction** side. Track that as its own item; it does not
> belong to the whale.
>
> Not verified: the 07-31 file count and the 11d `bounded` flip. The EXPLAIN probe needs the
> monoscope-self project id, which is not written down anywhere in these docs — **record it
> here next time**, since every whale verification step needs it.

monoscope's `UPDATE` dies against the **query** pool with
`SortPreservingMergeExec` at 15.0 GB of 16.4 GB — unbounded MOR dedup over
poisoned partitions. Roughly one failure per 90s at peak. Should clear with
07-31, but confirm rather than assume; if it survives the repair, it is a
separate read-path bug.

---

## P2 — refactors the day's failures actually justify

### 6. Per-table guards vs process-wide resources
The bug behind `60740ec`: `round_robin_bins` caps concurrency per table, the
light pool is shared by all tables, and `REPAIR_SORT_PARTITIONS`' doc asserted
"repair runs exactly ONE bin at a time" — true per table, false globally.

**Audit the same class elsewhere.** Every knob sized against a shared pool needs
to state its scope. Start with `light_optimize_k`,
`TIMEFUSION_MAINTENANCE_REWRITE_CONCURRENCY`, and the flush-sort path.

### 7. `PER_SORT_BUDGET_BYTES` is a fiction
`light_optimize_k` = `light_share / PER_SORT_BUDGET_BYTES` with the constant at
**4 GiB**, while a measured whale repair sort holds **14.4 GB** (sorter 7.4 +
unspillable merge 7.0). k has therefore always been ~3.6x optimistic, which is
what let packing co-exist with repair until `03dccf0` stopped it.

Either derive the per-sort estimate from the bin's input bytes, or split the
pool so packing and repair cannot see each other's memory at all. The permit and
the stand-down are both workarounds for a wrong number.

### 8. Observability gaps that cost real time today
- `wave_commit_enter` logs `wave_ids`, not `project_id` — impossible to attribute
  a commit to a tenant. My monitor's `whale_commits` counter could never fire.
- Repair events omit the date/file being repaired in some paths.
- ~~The CPU profiler is **ungated**~~ — DONE: `TIMEFUSION_CPU_PROFILE`.

### 9. e2e cannot manufacture a footer-less file
`with_sort_skip_bytes(0)` no longer produces one — the flush path escalates to a
pooled sort instead of skipping, so every output declares `sorting_columns`. That
means **no test exercises the actual repair rewrite**, only the suspect walk.

Fix: a fixture that writes a parquet without `sorting_columns` and registers it
via a Delta add action. This is the missing coverage under all of today's repair
work.

---

## P3 — process

### 10. The shared checkout
Fired both directions in two days: my edit was swept *into* another session's
commit, and later my WIP was *wiped* by one. Only reliable protection is to
commit to a branch immediately and verify with `git cat-file blob HEAD:<file>`.
Never `git add -A`, never `stash`/`reset --hard`.

### 11. Measurement discipline
Three wrong calls today, all from the same root: believing a signal without
checking what produced it.
- `grep -o "replay"` matched a substring inside the `wal_gc` message → "the crash
  is in WAL replay" (there was no replay event at all). **Grep whole lines, or
  anchor on `event="..."`.**
- Named the operator in a pool error as the culprit; it is the *starved*
  consumer, not the holder. **Read the whole consumer list.**
- Declared "no root, cannot remediate" after `sudo` and a volume `ls` failed.
  **`ubuntu` is in the docker group, so `docker run -v /host/path:/d alpine` IS
  root.** That one cost most of the outage.

---

## Suggested order

1. ~~Gate the profiler~~ DONE (`TIMEFUSION_CPU_PROFILE`); the walrus `debug_assert` item was withdrawn — see the correction in #2
2. Instrument the health probe (#1) — the outage trigger
3. CapRover `stop-first` (#3) — needs you, one word
4. Let 07-31 drain; verify the ladder (#4)
5. Footer-less e2e fixture (#9) — the coverage gap under everything shipped today
6. `PER_SORT_BUDGET_BYTES` / pool split (#7)
7. Scope audit of per-table guards (#6)
