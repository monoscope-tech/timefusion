# Writes take up to 90s because TimeFusion floods its own disk

2026-09-11. Measured against prod (`srv-captain--timefusion`, 19 h uptime at
time of sampling — a mature process, not a cold one).

## The symptom is real and worse than reported

`timefusion_stats` component `pgwire`:

| metric | value |
| --- | --- |
| `lat_p50_us_approx` | 1,015,011 (1.0 s) |
| `lat_p95_us_approx` | 9,009,684 (9.0 s) |
| `lat_p99_us_approx` | 47,889,678 (**47.9 s**) |
| `lat_p999_us_approx` | 184,804,777 (**184.8 s**) |

"Up to 90 seconds" understates it: p99 is 48 s and p999 is 185 s.

## The disk is saturated continuously, not in bursts

`iostat -x 5 12` on the host, md3 (`/`, a 4-way RAID over all four NVMes):

- util **99.26–100.02%** on every one of the 11 real samples
- `w_await` **63–620 ms**
- queue depth (`aqu-sz`) **212–620**
- writes **327–584 MB/s**, `wareq-sz` 212–435 KB (large writes, i.e. whole-file
  rewrites, not WAL appends)

The first `iostat` line is the since-boot average (46 MB/s, 0.05% util) and is
NOT the current state — reading it as such is how this looks like a burst.

There is no second device to move anything to: md2 is a 988 MB `/boot`, and
md3 spans all four NVMes. `/` is 89% full.

## TimeFusion is the flood — it is self-inflicted

Attribution via `/sys/fs/cgroup/system.slice/docker-<cid>.scope/io.stat`
(world-readable, no root), device `9:3` = md3, 45 s deltas:

| container | md3 write rate |
| --- | --- |
| timescaledb | ~725 KB/s |
| redpanda-0 | ~73 KB/s |
| **timefusion** | **33.1 TB / 19 h = ~484 MB/s** |

484 MB/s matches the observed 330–584 MB/s. TF's cgroup `wios` on md3
(71,591,210 over 19 h = **1,047 writes/s**) matches iostat's md3 `w/s` of
985–3,355. Two independent counters agree.

Co-tenants are exonerated by measurement, not assumption.

### A bound that was wrong by 32x — do not repeat it

An earlier pass in this session bounded TF's writes at ~15 MB/s from
`foyer.inner_bytes_read` (446 GB/19 h = 6.5 MB/s) plus
`flush_freed_bytes_total` (283 GB/19 h = 4.1 MB/s), and concluded TF was a
victim of a co-tenant. **That bound counted only two of TF's write channels
and omitted the dominant one.** `docker stats` showed TF at 167 TB written and
that was dismissed as implausible; it is in fact correct once you divide by the
~5 devices each bio is counted against (md3 + 4 NVMe legs). A partial channel
census is not a bound.

## The mechanism: a whole-file rewrite + 3 fsyncs per INSERT, under two global locks

`src/database/write.rs:607` → `invalidate_rollup_batches` →
`invalidate_rollup_hours` (`src/database/maintain.rs:4181`), per
`(project, date)`, **before the client is acked**:

1. take the global `rollup_journal_lock`
2. full `rollup_slice_coverage.retain` scan
3. `enqueue_maintenance_hours` → the global `journal()` mutex →
   `TaskJournal::checkpoint()` (`src/maintenance_coordinator.rs:3284`) —
   appends dirty records, `sync_all()`, then `publish_statistics()` full-scans
   every task
4. `persist_rollup_journal()` (`src/database/maintain.rs:892`) →
   `rollup_journal::store` (`src/rollup_journal.rs:70`) — serializes **the
   entire entry set** to JSON and `write_atomic_with(path, durable=true)`,
   which is temp-write + **fsync + rename + parent-dir fsync**

Step 4 is O(all entries) per invalidation — a genuine design defect, and it is
on the pre-ack path. **But it is NOT the 400+ MB/s flood.** See "What the flood
is not" below; the file is 105 KB, not the ~15 MB that story needed.

### The amplifier: a global mutex held across an fsync on the disk it saturates

`block` component, same 19 h:

| metric | value |
| --- | --- |
| `journal_hold.count` | 2,284,754 (33.4/s) |
| `journal_hold.avg_us` | 8,859 |
| `journal_hold.max_ms` | **7,210** |
| `journal_hold.total_ms` | 20,242,118 → **29.6% of wall-clock duty cycle** |
| `journal_lock_wait.total_ms` | 29,000,170 (aggregate across waiters) |
| `journal_lock_wait.max_ms` | **13,756** |

A single global `std::sync::Mutex` is held 29.6% of wall-clock, across an fsync,
on a disk whose write latency TF's own journal writes have inflated to
63–620 ms. Arrivals at 33/s queue behind holds that reach 7.2 s.

This is a feedback loop, not a linear cost: journal writes saturate md3 → every
fsync (including walrus's own `sync_each` WAL append, which every INSERT pays
pre-ack) inflates → holds lengthen → the queue deepens → p99 48 s.

### Why the earlier "journal is only 0.5% of insert latency" reading is not a contradiction

That measurement (this morning) counted time inside the *lock section* of
INSERT spans. It did not and could not see the disk saturation the journal's
own writes cause everywhere else, which is paid by every other fsync in the
process. Both numbers are correct; they measure different things.

## What the flood is NOT — three hypotheses killed by measurement

Each of these was plausible from a code read and each is refuted by a counter
delta. Recorded so they are not re-proposed.

**Not `persist_rollup_journal`.** A `docker cp` listing of
`/app/data/timefusion/.timefusion_meta/` gives the actual file sizes:
`rollup_invalidations.json` is **105 KB**, not the multi-MB the arithmetic
needed. At ~33/s that is ~3.5 MB/s. The task journal is the same story:
`maintenance_tasks.wal` 47 MB + `maintenance_tasks.json` 48 MB, compacting on a
~10 min cycle ≈ 160 KB/s.

**Not foyer cache traffic.** Two samples 75 s apart on the live process:

| channel | delta | rate |
| --- | --- | --- |
| md3 `wbytes` | 21.7 GB | **289 MB/s** |
| foyer `inner_bytes_read` | 93 MB | 1.2 MB/s |
| foyer `bytes_served` | 5.2 GB | 70 MB/s |
| foyer `l2_used_bytes` | 0 | flat |

Foyer fetches are **240x too small** to explain the writes. An earlier
`bytes_served ≈ 431 MB/s ≈ observed writes` correlation on the 19 h process was
a coincidence and does not survive a delta on the live one.

**Not a co-tenant.** cgroup `io.stat` deltas: timescaledb ~725 KB/s,
redpanda-0 ~73 KB/s.

## FOUND: Tantivy index builds write to the container's overlay filesystem

`docker diff <cid>` (read-only) on a 25-minute-old container lists **42,780
changed paths**. Every one of them is a Tantivy segment file under `/tmp`:

| extension | count |
| --- | --- |
| `.term` / `.store` / `.pos` / `.idx` / `.fieldnorm` / `.fast` | 7,403 each |

That is **7,403 segments across 5 temp directories** — ~1,480 segments per
build, against a `MAX_DEFERRED_SEGMENTS` cap of **32**
(`src/tantivy/mod.rs:51`). The container's writable layer measures **19.1 GB**
(`docker ps -s`).

Three `tempfile::tempdir()` calls put this on `std::env::temp_dir()` — `/tmp`,
which is the container's **overlay2 writable layer**, not the data volume:

- `src/tantivy/mod.rs:1030` — `build_parquet_and_pack`, the streaming
  per-parquet-file index build
- `src/tantivy/mod.rs:1058` — `build_and_pack`
- `src/tantivy/mod.rs:1095` — `verify_blob`, which **unpacks the entire index a
  second time** to prove the archive is not corrupt

So one index build writes its data to disk about twice: the raw segments, then
`pack_dir` tars it (read) and `verify_blob` unpacks the whole thing again
(write). All on overlayfs, all on md3, and accounted for by **no counter
anywhere**.

**The rate arithmetic (this is what makes it a flood, not the snapshot).**
`docker service logs | grep -c tantivy_index_built` gives **54 builds in 10
minutes** = 5.4/min. The 19.1 GB writable layer spread over 5 live build dirs
puts an index at **~3.8 GB**. 5.4/min × 3.8 GB × ~2 writes ≈ **680 MB/s** —
the same order as the 289–614 MB/s measured at the device. Tantivy indexing is
big enough to be the dominant term.

**Corrected:** an earlier draft of this doc claimed `finish_writer`'s inline
merge fires on every build, adding a third rewrite. It does not —
`grep -c "segments (>"` over 30 minutes of logs returns **0**, so builds stay
under the 32-segment cap and never merge inline. The 7,403 segments are spread
across many builds' directories, not produced by one.

`src/tantivy/search.rs:786` already does this correctly with
`TempDir::new_in(parent)` — the build path simply never got the same treatment.

## Also untracked: DataFusion spill

No `timefusion_stats` counter accounts for 289–614 MB/s. The one large TF disk
writer with no counter at all is DataFusion spill:

- maintenance spills to `timefusion_data_dir/<spill_subdir>` — i.e. md3 — capped
  by `timefusion_maintenance_spill_max_gb` (default **220**)
- the general `RuntimeEnvBuilder` at `src/database/mod.rs:6722` does **not** call
  `spill_disk_builder`, so it uses DataFusion's default DiskManager →
  `std::env::temp_dir()` → the container's writable layer, measured at
  **19.1 GB** twenty minutes after boot

**The spill cap's own sizing argument is now stale.** `src/config.rs:2179` reads
"the prod host has ~390 GB free"; `df -h /` now reports **200 GB free (89%
full)**. A single 220 GB repair spill no longer fits.

This is the open attribution question, and it is why the first change to ship is
per-channel byte counters — without them, post-deploy validation is blind.

## The loop has started closing on itself

During sampling the container restarted:
`task: non-zero exit (-1): dockerexec: unhealthy container`. Slow writes time
the healthcheck out → restart → WAL replay and maintenance-queue re-inflation →
more disk load → slower writes. That is **one** observed restart on this build,
not yet a confirmed loop — but the mechanism is present and the restart cost is
real (the queue re-inflates for ~25 min and every counter resets).

## Shipped here: index scratch off the container overlay

Branch `tantivy-tmpdir-off-overlay`. `scratch_tempdir(root)` replaces the three
bare `tempfile::tempdir()` calls, and `TantivyIndexService::new` takes an
explicit `scratch_root` (no defaulted `/tmp`, so a future call site cannot
silently reintroduce this). Guarded by
`index_builds_keep_scratch_off_the_process_temp_dir`, which points `TMPDIR` at
an empty directory and asserts **nothing lands there** — a cost assertion, since
a build that writes to `/tmp` still passes every correctness test.

This moves the bytes onto the volume the operator sized and can observe. It does
**not** by itself reduce the byte count — both paths are md3. The volume levers,
in order of expected size, are below.

## Fix order

1. **Instrument the untracked channels** — tantivy build bytes, spill bytes,
   journal bytes in `timefusion_stats`. Attribution above rests on arithmetic
   from resident sizes and log rates, not a counter; without these, post-deploy
   validation is blind. Ship with (2) so it costs no extra restart.
2. **Stop client INSERTs paying synchronous fsyncs under a global mutex** —
   branch `journal-group-commit` (`1b391890`, `16e925af`, pushed) amortizes the
   fsync across concurrent writers; PR #263 (open) stops the full-journal rescan
   per checkpoint. This is what converts disk latency into *client* latency, and
   it works regardless of which channel wins the attribution above.
3. **Cut tantivy's write volume**, the likely dominant term at ~680 MB/s:
   - `verify_blob` unpacks the full index a second time purely to prove the
     archive opens — roughly half the scratch volume, for a guard that could be
     satisfied more cheaply.
   - ~3.8 GB per index at 5.4 builds/min is the real question: index-per-parquet
     against maintenance's 36x rewrite churn means maintenance keeps triggering
     full reindexes. Check for a skip-unchanged path before adding one.
4. **DataFusion spill**, still unmeasured: the `mod.rs:6722` runtime uses
   DataFusion's default DiskManager (no `spill_disk_builder`), and the
   maintenance cap's sizing comment assumes 390 GB free where 200 GB remain.

Do NOT simply defer the checkpoint off the pre-ack path: WAL replay never
re-seeds a lost checkpoint, so the barrier is load-bearing. The cost must be
amortized, not dropped.

Independently worth fixing: **foyer L2 holds 638 GB against a 600 GB configured
cap** on a disk with 200 GB free. Proven above not to be the flood, but it is a
misconfiguration on a disk that is 89% full.
