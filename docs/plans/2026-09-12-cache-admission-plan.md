# Plan: prove and fix foyer write-capture admission

2026-09-12. Execution plan for the leading candidate in
[`2026-09-12-surviving-10x-on-the-write-path.md`](2026-09-12-surviving-10x-on-the-write-path.md).
Decided: **single node**, so the local disk budget is the whole game.

## The question this plan answers

Write-capture is not obviously waste. It exists so a file we just uploaded is not
re-downloaded from R2 on the next query (`storage.rs:1700-1707`). Removing it
trades local disk writes for remote GETs and latency.

So the decision rests on one number, and it is the S3-FIFO "one-hit wonder"
question:

> **Of the entries admitted to L2 by write-capture, what fraction are read at
> least once before they are evicted?**

- mostly **unread** → admission is near-pure waste, skip it
- mostly **read** → capture is earning its keep, and the flood is elsewhere

Nothing is changed in prod until that number is in hand. Four attributions have
already died by measurement in two days; this one is not shipping on arithmetic.

## What we already know (measured)

| fact | value |
| --- | --- |
| device writes, cgroup `wbytes` over 50 s | **507 MB/s** |
| `l2_used_bytes` across >1 h | **638,659,358,720 — byte-identical** (cap 600 GB) |
| `inner_gets` delta over 60 s | **0** — no read-driven admissions |
| foyer read-side population | 10.8 MB/s |
| logical write ceiling (4-way RAID1) | **~0.8 GB/s** — we are at ~63% |

L2 is permanently full, so **every admission forces an eviction**. With zero
read-driven admissions, essentially all admissions must be write-capture — which
is what makes it the leading candidate. What the numbers cannot yet say is
whether those entries are useful before they die.

## Stage 1 — one deploy: counters + the fix, defaulted OFF

Behaviour is unchanged on deploy. Only the flag flip changes behaviour, and the
flip needs no rebuild.

### 1a. Admission-source counters

`insert_main` is called from several places; the write-capture call is
`storage.rs:1639` inside `CachingMultipartUpload::complete`. Tag each admission
with its source and count bytes:

```
foyer.admit_bytes.write_capture
foyer.admit_bytes.read_miss
foyer.admit_bytes.warm
```

If `write_capture` lands near 500 MB/s, attribution is settled.

> **Note a real defect while we are here.** `admit()` calls itself "Single funnel
> for every cache population, so `scan_bypass_scope` can suppress all of them in
> one place" (`storage.rs:1476`) — but write-capture calls `insert_main`
> **directly** and never enters that funnel, so neither `bypass_active()` nor
> `repeat_sighting` has ever applied to it. Routing write-capture through
> `admit()` is the principled fix; it needs `CachingMultipartUpload` to reach
> the cache wrapper rather than the bare `FoyerCache`, so it is deliberately
> **not** in Stage 1.

### 1b. Hit-before-evict, for write-captured entries only

`EvictionCounter::on_leave` already receives the value, so eviction is
observable. Hit counts are the hard part:

**Do NOT add a hit counter to `CacheValue`.** It is `Serialize`/`Deserialize`
and is persisted into L2 — 638 GB of it. Any field change risks failing to
deserialize every existing entry on deploy, and a counter would reset on every
L2 round-trip anyway. If a field is ever added there it must carry
`#[serde(default)]`.

Instead, a side map keyed by cache key, populated only for write-captured
admissions — ~2,300 live entries, so the memory cost is negligible:

```
DashMap<String, AtomicBool>   // key -> "was read at least once"
```

- write-capture admission → insert `false`
- cache hit → set `true`
- `on_leave(Evict)` → read, count into one of two counters, remove

```
foyer.write_capture.evicted_unread
foyer.write_capture.evicted_after_hit
```

The ratio is the decision.

### 1c. The gated change

```
TIMEFUSION_WRITE_CAPTURE_L2   (default: true = today's behaviour)
```

When `false`, `complete()` skips admission **only** for values above
`l1_max_entry_bytes` (16 MB) — exactly the ones `insert_main` would steer to
`Location::OnDisk`. Small files keep their L1 (memory) admission, which never
touches disk. Compaction outputs are 128 MB–1.5 GB, so for them it is a clean
binary and the blast radius is limited to the entries that cost disk bandwidth.

Follows the existing kill-switch convention
(`TIMEFUSION_REPAIR_RESUME_ENABLED`, `TIMEFUSION_LANDED_SKIP_ENABLED`).

## FINAL VERDICT (2026-09-12): DO NOT FLIP — write-capture earns its keep

With the accounting corrected (`7c8f0e4e`, both the multipart tee and the
single-part PUT warm tagged as write capture), prod answers the question this
plan was built to ask:

| counter | value |
| --- | --- |
| `write_capture_admitted` | **137** (0 before the retag — the fix now reaches the traffic) |
| `write_capture_evicted_after_hit` | **75** |
| `write_capture_evicted_unread` | **30** |
| `admit_write_capture_bytes` | 234 MB |

**71% of write-captured entries are read before they are evicted.** They are not
one-hit wonders; capture is doing exactly the job it was written for — a file we
just uploaded is usually read again shortly after, and caching it saves an R2
round trip. Per this plan's own decision table, that is the **"do not flip"**
branch, and `TIMEFUSION_WRITE_CAPTURE_L2` stays `true`.

The volume is also negligible: **234 MB** of write-capture admissions against
616 MB/s of device writes. Flipping the flag would forfeit a 71%-effective cache
to save nothing measurable.

The flag and counters stay: the flag costs nothing defaulted on, and the counters
are what turned a plausible story into a measured refusal. That is the whole
return on the instrument-first ordering — the alternative was shipping a change
that degraded reads to fix a problem it did not touch.

## Stage 2 first reading: hypothesis refuted by its own instrument

Shipped as `2e96eec7`, read on prod at ~1 h uptime.

| counter | value |
| --- | --- |
| `admit_write_capture_bytes` | **0** |
| `write_capture_admitted` | **0** |
| `admit_read_miss_bytes` | 290 GB (≈ `inner_bytes_read`, so genuine R2 reads) |
| read-miss admissions, 90 s delta | **1.4 MB/s** |
| device writes, same window | **616 MB/s** |

**Write-capture admits nothing at all**, and total foyer admissions are ~1.4 MB/s
against 616 MB/s of device writes. Per this plan's own third branch: **do not
flip the flag.** The arithmetic that motivated it — 4.2 evictions/s × ~120 MB —
was a coincidence, the fifth such in this investigation.

A 90 s delta with the device at 616 MB/s shows `processed_bytes_total` **0**,
WAL `disk_bytes` **0**, flush 53 KB/s. **Nothing in TimeFusion's accounting
explains the writes.**

### Two measurement traps this stage paid for

**A stats reading of exactly 0 may be a lock artifact, not data.**
`runtime_stats` calls `try_get_stats()` — a non-blocking `try_read` that returns
`CacheStats::default()` when contended. An intermediate reading showed
`hits`/`misses`/`inner_bytes_read`/`bytes_served` all at exactly 0 while
admissions stood at 290 GB, and that was read as "nothing was fetched from R2".
It was four zeros from one failed lock. **Four counters reading exactly zero at
once is the tell.** Re-read until a delta is non-trivial before concluding.

**The first gate reached none of the traffic.** It covered `put_multipart` only,
while the bulk of write-side warming goes through single-part `put_cached`
(`storage.rs:1563`, "Warm the cache directly from the just-written bytes"). Real
code, real test, zero effect — the ingest-dedup failure mode exactly. Fixed by
tagging and gating that path too, with a guard that fails when reverted. It
turned out not to be the flood either, but the accounting was actively
misleading: a write was being counted as a read miss.

### What is still unattributed, and the one candidate left

616 MB/s with every application-level channel idle. The remaining hypothesis is
**foyer's block-engine internal reclamation**: `l2_used_bytes` is pinned
byte-identical at 638.66 GB against a 600 GB cap, evictions tick at ~8.5/s with
essentially no admissions to drive them, and the engine uses **2 GB blocks**. A
cache structurally over capacity may be in a permanent relocate-to-reclaim loop
— which would be invisible to every counter added here, because it is below the
admission layer.

Testing that needs file-level attribution on the host (which region files are
being rewritten), i.e. root access this investigation does not have. It is the
next instrument, and it is an ops task rather than a code one.

## Stage 2 — original decision table (kept for the record)

Wait **≥2 h after deploy** before quoting anything: a restart re-inflates the
maintenance queue for ~25 min and resets every counter, and a young process
reads as fixed.

| reading | action |
| --- | --- |
| `evicted_unread` dominant (say >80%) | flip the flag; this is the fix |
| mixed | flip anyway, then reconsider admit-on-first-read (write-around) so hot files keep their entry |
| `evicted_after_hit` dominant | **do not flip.** Capture is earning its keep; the flood is something else and Stage 1's counters have narrowed it |

## Stage 3 — validate the flip

Judge on three numbers, ≥3 samples each, ≥2 h of quiet:

1. cgroup `wbytes` rate — the direct target
2. md3 `w_await` and util — should leave the 95-100% band
3. `backlog_bytes` — **the one that matters most.** Maintenance processes only
   ~17 MB/s while its own cache churn consumes ~500 MB/s of the disk. If the
   flood is self-inflicted, the backlog should start draining rather than
   growing. That is the 10x-relevant outcome; p99 is a side effect.

Watch for the expected regression: R2 GETs on freshly-written files
(`inner_gets`, `inner_bytes_read`) should rise. That is the trade being made. It
is only a problem if read latency degrades materially.

**Rollback:** set the flag back to `true`. No rebuild.

## Tests

Per the bug-fix workflow, the guards must assert **cost**, not just correctness,
and must be shown to fail with the change reverted:

- an upload above `l1_max_entry_bytes` with the flag off performs **no L2
  admission** (assert on the admission counter, not on a hit afterwards — a
  correctness-only assertion passes either way)
- an upload below the threshold still lands in L1 with the flag off
- with the flag on, behaviour is byte-identical to today
- the side map does not leak: an evicted key is removed

## Appendix A — RAID split (DRAFTED, NOT TO BE EXECUTED)

Recorded now so it is ready; nobody runs this yet. It is live surgery on the
**root** filesystem of the production host.

Today: `md3 : active raid1 nvme0n1p3 nvme1n1p3 nvme2n1p3 nvme3n1p3` — 1.8 TB,
4 copies of every byte, `/` mounted on it.

Target: md3 as a **2-way** mirror (same 1.8 TB, no resize, no data movement) plus
a new **RAID0** of ~3.4 TB for reconstructible data.

```bash
# 0. Confirm fully healthy first — [UUUU], no resync in progress
cat /proc/mdstat

# 1. Drop two legs. The array stays REDUNDANT throughout (never below 2 copies).
mdadm /dev/md3 --fail /dev/nvme1n1p3 --remove /dev/nvme1n1p3
mdadm /dev/md3 --fail /dev/nvme3n1p3 --remove /dev/nvme3n1p3

# 2. Tell mdadm 2 legs is the intended count, or it reports degraded forever.
mdadm --grow /dev/md3 --raid-devices=2

# 3. Reclaim the freed partitions.
mdadm --zero-superblock /dev/nvme1n1p3 /dev/nvme3n1p3
mdadm --create /dev/md4 --level=0 --raid-devices=2 /dev/nvme1n1p3 /dev/nvme3n1p3
mkfs.xfs /dev/md4 && mkdir -p /mnt/ephemeral && mount /dev/md4 /mnt/ephemeral

# 4. Persist. `nofail` matters: md4 has NO redundancy and must never block boot.
mdadm --detail --scan >> /etc/mdadm/mdadm.conf
update-initramfs -u
# /etc/fstab:  /dev/md4  /mnt/ephemeral  xfs  defaults,nofail  0 0
```

Use LVM on md4 rather than a bare filesystem, so swap and cache are independently
resizable:

```bash
pvcreate /dev/md4 && vgcreate ephemeral /dev/md4
lvcreate -L 64G     -n swap  ephemeral      # see Appendix B
lvcreate -l 100%FREE -n cache ephemeral
mkswap /dev/ephemeral/swap
mkfs.xfs /dev/ephemeral/cache
```

Then move **only reconstructible** data — foyer cache, `tantivy_scratch`, the
`*_spill` dirs — by adding a second bind mount to the CapRover service
definition (`deploy/`). **WAL and journals stay on the mirror.** Zero code
change; TF derives these paths from `timefusion_data_dir`, so the mount does the
work.

**What this buys — and what it does NOT.** In a RAID1 *every leg receives every
write*, so going 4-way → 2-way halves TOTAL device traffic but leaves each
surviving drive at the same ~500 MB/s. **Step 1 alone buys no write headroom**;
it is a prerequisite that frees two drives, not a fix. Do it only as part of
committing to step 2.

The win is entirely in step 2, and it is two things:

- **~2x ephemeral bandwidth** — 490 MB/s striped over two drives is ~245 MB/s
  each, against 500 MB/s hitting every drive today.
- **WAL isolation, the bigger prize** — the mirror pair drops to ~10 MB/s and
  carries only WAL and journals, so client `fsync`s stop queueing behind cache
  churn. That is the mechanism behind the 13.4 s worst-case barrier wait, and
  separation attacks it in a way no scheduler can.

Per-drive write load, before and after:

| | nvme0 | nvme1 | nvme2 | nvme3 |
| --- | --- | --- | --- | --- |
| now (all on md3 ×4) | 500 MB/s | 500 MB/s | 500 MB/s | 500 MB/s |
| after step 1 | 500 MB/s | idle | 500 MB/s | idle |
| after step 2 | **~10 MB/s** | ~245 MB/s | **~10 MB/s** | ~245 MB/s |

Capacity: usable space from the `p3` region goes **1.918 TB → 5.754 TB (3.0x)**,
redundancy overhead 75% → 25%.

### Drive endurance — check this regardless of any RAID decision

At ~500 MB/s per drive the array absorbs **43 TB/day/drive ≈ 22 DWPD** on 1.92 TB
devices. Read-intensive NVMe is rated 0.3-1 DWPD, mixed-use 3, write-intensive
5-10 — so this is **2-75x rated endurance** depending on drive class, and a
3 DWPD drive's entire five-year budget is consumed in roughly eight months.

**Run `nvme smart-log /dev/nvme0 | grep -i percentage_used` on all four** (needs
root; the OVH panel also shows disk health). If those are climbing, wear-out is
the dominant risk and the software fix stops being a performance optimisation
and becomes hardware preservation. The RAID split halves total device writes;
removing the 500 MB/s removes the problem.

**What it costs, stated plainly:**
- durable-data fault tolerance drops from surviving 3 drive failures to 1
- md4 has **no** redundancy — a single drive loss destroys the whole ephemeral
  volume. Acceptable *only* because every byte on it is reconstructible, and
  **only if TF is verified to boot cleanly with an empty cache dir** (test this
  before, not after)
- it is live `mdadm` work on the root array of a production ingest path

**Do the software stages first.** If Stage 2 removes most of the 500 MB/s, the
pressure that motivates this largely disappears, and it can be done calmly at a
maintenance window instead of under duress.

## Appendix B — swap on the ephemeral volume (TASK)

**Current state, measured.** 188 GB RAM, 122 GB available,
`/proc/pressure/memory` **0.00 across all windows** — there is no memory
pressure today. Swap is **active but negligible**: ~1-2 GB total and **100%
used**, from the four 537 MB `p4` partitions OVH created. TF's cgroup is capped
at 120 GiB with `memory.swap.max` set to the same value, i.e. effectively
unbounded.

So this is **insurance, not a fix.** The case for it is TF-specific: this
process has a history of OOM kills, and each restart costs ~25 min of
maintenance-queue re-inflation plus every in-flight unit. Swap converts a hard
kill into degraded performance, which for this workload is the better failure.

### Task

1. `lvcreate -L 64G -n swap ephemeral` on md4 (see Appendix A), `mkswap`,
   `swapon`, add to `/etc/fstab` with **`nofail`**.
2. **`vm.swappiness = 10`** (currently **60** — far too eager for a database).
   Swap must be an emergency reserve, not a routine tier.
3. Bound the container: set `memory.swap.max` to a deliberate figure
   (e.g. 32 GiB) rather than leaving it equal to `memory.max`. This is the knob
   that decides *degrade* vs *thrash*.
4. Leave the four tiny `p4` partitions alone — 2 GB is not worth the write
   traffic on the mirror pair.

### Sizing rationale

64 GB, not more. Enough to absorb a spike; **not** enough to thrash for hours.
Swapping a 120 GiB working set through any device leaves the box unresponsive,
which is worse than a fast restart — the goal is to survive a transient, not to
run from disk.

### THE TRADE-OFF THIS MAKES — read before agreeing

Appendix A states that losing md4 costs a cold cache, not data loss. **Putting
swap on md4 makes that false.** If a RAID0 member dies while pages are swapped
out, every process holding those pages is killed — TF goes down, not just cold.

Accepted here because TF is built to survive restarts (WAL replay, clean-shutdown
path), and an md4 failure is already a severe event: 638 GB of cache gone and
every read falling through to R2. A restart on top of that is small marginal
harm.

**The alternative, if that coupling is unacceptable:** a smaller swapfile on md3
instead. It survives a single drive failure and keeps md4-loss benign, at the
cost of 2x write amplification and scarce root space — `/` is at **83% with
291 GB free** and shrinking. Emergency-only swap is rarely written, so the
amplification barely matters; the space does.

### Verification

- `swapon --show` reports the new device (needs root — a non-root
  `swapon --show` returns empty and reads as "no swap", which is how the earlier
  reading in this investigation was wrong)
- under induced pressure, TF degrades instead of being OOM-killed
- `/proc/pressure/memory` stays near zero in normal operation; if swap is being
  touched routinely, `swappiness` is still too high
