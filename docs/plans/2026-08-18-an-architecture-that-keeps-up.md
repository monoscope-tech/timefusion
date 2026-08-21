
### Part XVI resolved — on one image, over seven hours, growth is BOUNDED

The question left open by the retraction ("do the lumps release or accumulate?")
now has an answer, from the first uninterrupted multi-hour window obtained.
`a1bf953`, hourly `process_rss_mb`:

| uptime | 1h | 2h | 3h | 4h | 5h | 6h | 7h |
| --- | --- | --- | --- | --- | --- | --- | --- |
| MB | 9,979 | 12,661 | 13,435 | 11,356 | 12,419 | **17,020** | 12,796 |

**The lumps release.** Every excursion returns to an 11-13 GB floor, including the
17 GB peak in hour 6 which I flagged at the time as possibly a step change — it
was not. Net across seven hours is **+470 MB/h**, implying ~240 h to the 124.6 GB
wall, against an 8-20 h kill cadence. No kill in **14h45m**, roughly double the
historical minimum gap.

So on this build a kill cannot come from the growth visible here. It requires an
excursion far outside this envelope — an event, not a trend. Which is where the
first (retracted) reading pointed, but that conclusion was drawn from two samples
in one quiet window and did not deserve to be believed at the time; it does now,
on seven samples spanning both quiet and lumpy hours.

**What this does not establish.** It is ONE image. `d5688fd` accumulated
13.8 → 22.2 GB in ~30 minutes with no release, so builds genuinely differ, and the
five kills earlier in the day were real. The claim is bounded to `a1bf953`.

**Methodological point worth keeping.** Four separate times this session a rate
computed from a short window was wrong, twice badly enough to publish and retract.
The only readings that held up came from a series long enough to contain both
regimes. On a lumpy signal, *the minimum useful sample is one that spans a release*
— anything shorter measures a regime and calls it a rate.

---

## Part XVIII — 2026-08-21 09:34 onward: sustained crashloop on `41ca4c6`

**Not self-recovering.** Failures at 09:34:17, 09:37:41, ~09:44, ~09:50, ~10:00,
across ~30 minutes. Kernel OOM kills went 8 → 10; the others were
`dockerexec: unhealthy container`, i.e. the health check killing the container
during startup work. pgwire intermittently refused connections
(`the database system is starting up`).

Traffic was NORMAL throughout — 500 SELECT / 180 UPDATE per 4 minutes — so this
is not load-driven. I nearly reported an "UPDATE storm" from a 12-line log tail;
counting the operations refuted it.

### The replay hypothesis, and why it is only half right

`rows_in_buffer_lag` across the episode:

```
1,254,723 -> 867,115 -> 1,598,997 -> 1,194,632 -> 428,879 -> (killed again)
```

I escalated this as a monotone death spiral on the first three points. **It is
not monotone** — it oscillates, and the cycle that drained furthest (down to
429 k, five minutes of uptime, the best of the episode) still died. So "each
restart leaves strictly more to replay" is refuted; replay pressure is real but
it is not a ratchet.

### The evidence worth acting on

From the logs immediately before a kill:

```
light_optimize_memory_brake  limit=68719476736        (64 GiB)
dedup_drain_flush_yield      on 4+ tables
tantivy cache reap: before=65808MB  budget=65536MB    (64 GiB, AT budget)
tokio runtime scheduling lag — a task that asked for 500ms woke this late
PgWire error 57014: canceling statement due to statement timeout
WAL GC: deleted 4 stale files, freed 4194304000 bytes
```

**The tantivy cache is budgeted at 64 GiB on a 120 GB container and is sitting at
its budget.** That is more than half the cgroup limit committed to one cache, and
it would explain the striking regularity of the kill band (124.5-125.4 GB across
six kills, five images, two days): a component pinned near 65 GB plus an ordinary
working set lands on the wall at almost the same number every time.

**Caveat, because this is one inference from one log line.** The reap message
scans files, and every kill shows `file-rss ≈ 161 MB`, so if that cache is on
disk or mmap'd-but-not-resident it is NOT the anon memory that kills the process.
Someone should check whether `tantivy` cache accounting is anon or disk before
acting on this. If it is anon, it is the single largest term on the box and the
first thing to cap.

The scheduling lag and 57014 timeouts say the process is saturated, not merely
memory-hungry — CPU and memory pressure are arriving together.

### What I did not do

No intervention. The host is read-only by standing instruction, and a rollback
redeploys a different image against the same WAL and the same cache budget, so it
does not address any candidate cause. Two push notifications were sent; the
second was partially wrong (it called the lag monotone) and that is corrected
above.

### Part XVIII resolved — the episode ran 09:34 to ~10:01, ~27 minutes

Recovery confirmed at 10:20: `5559ac8` running 10 minutes, the preceding task
entry a clean `Shutdown` (deploy) rather than `Failed`, last failure 19 minutes
earlier, RSS 14.4 GB, `rows_in_buffer_lag` back to a normal 339 k, coverage 30,
kernel kill count static at 10.

**What ended it.** The last `41ca4c6` instance survived seven minutes — the
longest of the episode — and drove `rows_in_buffer_lag` to **0** for the first
time. Once the replay backlog was gone, the next start had almost nothing to do
and the loop stopped. A new image (`5559ac8`) landed at about the same moment, so
the two are confounded: I cannot say whether the build helped or simply arrived
after the backlog cleared.

**What this episode is worth remembering for.**

1. **Replay pressure is real but self-limiting.** The lag oscillated
   (1.25M / 867k / 1.60M / 1.19M / 429k / 0) rather than ratcheting, and it
   eventually drained under exactly the conditions I had claimed would prevent it
   from draining. The "death spiral" framing was wrong and is retracted above.
2. **A health check that kills during startup work extends an outage.** Two of
   the five failures were `dockerexec: unhealthy container`, not OOM. Each one
   destroyed partially-completed replay. A startup grace period long enough to
   cover replay would likely have ended this in one cycle instead of five.
3. **The tantivy cache budget remains the open question** — 64 GiB reserved on a
   120 GB container, observed sitting at budget, against a kill band of
   124.5-125.4 GB. Still unresolved whether that memory is anon (in which case it
   is the dominant term) or disk-backed (in which case it is a red herring).

Item 2 is the cheapest available mitigation and does not require knowing the
answer to item 3.
