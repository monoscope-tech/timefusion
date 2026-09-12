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

## Stage 2 — read the counters, then decide

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

Then move **only reconstructible** data — foyer cache, `tantivy_scratch`, the
`*_spill` dirs — by adding a second bind mount to the CapRover service
definition (`deploy/`). **WAL and journals stay on the mirror.** Zero code
change; TF derives these paths from `timefusion_data_dir`, so the mount does the
work.

**What this buys:** ephemeral traffic stops being written 4x and moves off the
contended mirror entirely; WAL fsyncs halve their device cost and stop competing
with cache churn; foyer gets room to stop running 38 GB over its cap; +3.4 TB.

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
