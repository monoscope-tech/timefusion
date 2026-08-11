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
Between 12:14 and 12:23 every boot segfaulted (exit 139, no Rust panic). Evidence
preserved at `/home/ubuntu/timefusion-data/wal.broken-1786451053`.

Reproduce offline: copy that dir, point a local binary at it, run under
`gdb`/`catchsegv`. No panic text ⇒ unsafe/FFI, so `RUST_BACKTRACE` will not help.
Two known-good images crashed identically, so it is **state**, not code.

Related hardening regardless of outcome: `walrus-rust`
`StorageImpl::read` bounds-checks with `debug_assert!`, compiled out in release.
Make it a real check that returns an error.

### 3. CapRover `serviceUpdateOverride` — one word
The timefusion app config literally contains `"Order": "start-first"`, which is
the WAL-lock deadlock. This is *why* every manual `stop-first` gets reverted; it
was never a CapRover quirk. Change to `"stop-first"` in the UI. Needs the
CapRover password.
Also there: `"FailureAction": "pause"` explains the wedge shape (failed rollouts
pause and accumulate not-ready tasks rather than rolling back).

---

## P1 — finish the whale

### 4. Drain 07-31 (36 files)
No blockers: every file is under the admission cap, largest 631 MB. At ~13
min/bin that is ~8 hours of uninterrupted repair; the real rate is set by how
often the process restarts.

Verify with the EXPLAIN probe as it drains: 11d should flip to `bounded` when
07-31 is clean, then chase 128 files → 14-day and 663 → 30-day.

### 5. Enrichment writes are failing for the whale
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
- The CPU profiler is **ungated** (`profiling.rs::start()` takes no env switch),
  so it cannot be disabled without a rebuild. Gate it.

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

1. Gate the profiler + fix the walrus `debug_assert` (small, unblocks #2)
2. Instrument the health probe (#1) — the outage trigger
3. CapRover `stop-first` (#3) — needs you, one word
4. Let 07-31 drain; verify the ladder (#4)
5. Footer-less e2e fixture (#9) — the coverage gap under everything shipped today
6. `PER_SORT_BUDGET_BYTES` / pool split (#7)
7. Scope audit of per-table guards (#6)
