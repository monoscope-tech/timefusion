# Write/backfill slowness + restart loop — final diagnosis & plan (2026-06-22)

Investigation done live against prod (`timefusion_stats` over pgwire + read-only SSH to
`captain.s.past3.tech`). Several first-pass theories were disproved by measurement; the
"ruled out" section is kept on purpose so we don't re-litigate them.

## Environment (confirmed on the host)

- **CPU:** 48 cores, **no container CPU limit** (`NanoCPUs=0`).
- **RAM:** host 188 GB but only **~42 GB actually available** (shared with `timescaledb` + others).
  Container mem **limit = 66 GB**, reservation 16 GB.
- **Disk:** 1.8 TB **at 91 %** — only **166 GB free**. Data volume `/home/ubuntu/timefusion-data`:
  WAL **32 GB**, foyer cache **50 GB**.
- **Deployed image:** `6f66d4e` (latest; the 2 GB-dedup-overflow fix is already live).
- **Config (all defaults unless noted):** `MEMORY_FRACTION=0.5` → MemBuffer cap ~6.8 GB
  (derived, nondeterministic); `FOYER_DISK_GB=120`; `RUST_LOG=info`; `flush_parallelism=4`;
  `flush_interval=300s`; `pressure_flush_pct=75`; `wal_max_file_count=200`; `retention=70min`.

## Confirmed root causes (ranked)

1. **OOM restart loop is the dominant pain.** Operator-confirmed; caught live (`queries_total`
   476K→4.6K). The container limit (66 GB) is **larger than the host can back (~42 GB free)**, so
   the kernel OOM-killer takes TF (the biggest cgroup) on *host* pressure — well before TF's own
   6.8 GB MemBuffer is a factor. Each kill is a `SIGKILL`, so boot pays a slow recovery (below).
   **The exact RSS driver that reaches the host ceiling is still unconfirmed** (foyer? query
   working set? boot-replay transient? leak?) — see Open Questions.
2. **WAL bloat → slow restarts.** 32 GB = *today's* backfill volume in 1 GB segments, **not** an old
   backlog. Truncation is gated on the slowest read-cursor, and `another batch write already in
   progress` (walrus per-topic append collisions, ~23/hr) stalls the post-commit cursor advance.
   `wal_max_file_count=200` × 1 GB = **200 GB before the emergency-flush valve trips — on a disk with
   166 GB free**, i.e. the WAL can fill the disk first. A dirty boot then spends ~**38 s scanning** the
   bloated segments to replay only ~1 GB / 3,149 entries (+ ~21.7 s `delta_cursor_reconcile`).
3. **Lingering buckets** (9 older than retention; oldest ~67 h *event-age*). **Not** a flush error
   (zero `Failed to flush coalesced commit` in 12 h) — old-event-time backfill data that isn't
   draining. Exact mechanism not yet pinned (needs a stat/debug build).
4. **Backfill is the trigger.** e.g. project `6297304f` dumped ~800 K rows in one minute. Backfills
   outrun flush-advance → WAL + MemBuffer grow → host RAM pressure → OOM.

## Ruled OUT (with evidence — do not revisit)

- **Drain serialization (`delta_commit_lock`) is the bottleneck** → NO. Measured a flush clearing
  ~2.5 GB / 550 K rows in ~2 min, pressure sawtoothing 13–50 %. The serial commit path keeps up at
  steady state. **The Mutex→RwLock change is dropped** (it would add OCC retry storms + a racy
  `added_files` diff on the single shared unified table, and more concurrent encodes = *worse* for the
  OOM). If drain ever becomes the limit, do **staged commit** (parallel upload, one small serialized
  commit) — not a blanket RwLock.
- **Tantivy halves the buffer** → NO. `WRITER_HEAP_BYTES=64 MB` → reservation is 64 MB × 8 = ~512 MB.
  Tantivy build is already detached off the commit path.
- **MemBuffer hard-limit > container RAM** → NO. MemBuffer self-caps ~6.8 GB vs a 66 GB container.
- **`clean_shutdown=false` means a dirty exit** → NO (normal for a *running* instance: the flush path
  writes `false` continuously; only graceful shutdown writes `true`). The latest restart (18:42) was
  **clean** (`clean_shutdown=true`, fast boot). `false` is only a problem when it appears in a **boot**
  log — which means the prior exit was a `SIGKILL`/OOM.

## Plan

### P0 — Config/ops (no code; biggest risk reduction; apply together, once, in a low-traffic window)

Set via CapRover App Configs (these all trigger one restart = one WAL replay):

| Change | From | To | Why |
|---|---|---|---|
| `TIMEFUSION_WAL_MAX_FILE_COUNT` | 200 | **50** | Emergency-flush valve trips at ~50 GB, not 200 GB — stops the WAL filling the 91 %-full disk. |
| `TIMEFUSION_FOYER_DISK_GB` | 120 | **60** | 50 GB WAL + 60 GB cache = 110 GB, safe under 166 GB free. |
| `TIMEFUSION_BUFFER_MAX_MEMORY_MB` | unset (~6.8 GB, derived) | **10000** | Make the budget deterministic; today it's `MEMORY_FRACTION` × a boot-time memory read. |
| `TIMEFUSION_MEMORY_FRACTION` | 0.5 | **remove** | Redundant once MB is explicit; verify `max_memory_mb`≈9–10 GB in `timefusion_stats` after. |
| Container mem **limit** (Service Update Override) | 66 GB | **~32 GB** | Make the OOM boundary real (host can't back 66 GB). Protects the host while leaving transient headroom over TF's ~10–15 GB working set. |
| `TIMEFUSION_FLUSH_PARALLELISM` | 4 | 8 | Harmless on 48 cores, matches intent — but **low impact** (commits serialize). |

Also: **free disk** (drain/trim WAL, prune old foyer/delta files) — 91 % is itself an outage risk.

### P0 — Diagnose the OOM (needed to close root cause #1)

- Confirm `OOMKilled` vs host-pressure: `docker inspect` the dead task's `State.OOMKilled`; check
  `journalctl -k | grep -i oom` for which cgroup the kernel killed.
- Capture peak RSS before a kill (or a heap profile) to find what actually grows toward the host
  ceiling — foyer memory tier, query working sets, the boot-replay transient, or a leak.

### P1 — Code fixes (durable; none require the dropped RwLock)

1. **Per-topic WAL append queue.** Serialize/queue walrus ops per topic instead of surfacing
   `another batch write already in progress` as an insert error. Kills the insert rejections *and*
   un-stalls cursor advance → WAL truncates. Highest-value code change.
2. **Bounded-chunk flush.** Cap how much one `CoalescedGroup` combines (by rows/bytes) so a backfill
   can't build a multi-GB combined bucket that doubles during dedup-concat + parquet-encode. Lower
   memory transient (helps the OOM) and smaller/faster commits.
3. **Self-healing flush.** Split-on-failure, then **quarantine after K cycles** (reuse the existing
   WAL-replay `quarantine_entry`, `buffered_write_layer.rs:73`), so one bucket can't pin a topic's
   cursor + memory indefinitely. Bounds blast radius.
4. **Per-topic WAL truncation + smaller segments.** So one lagging topic doesn't pin shared 1 GB
   segments, and a dirty boot scans less → fast restarts (the 250 ms empty-WAL case).
5. **Backfill shaping.** Producer-side rate-limit to drain rate + **contiguous** timestamps (wide
   event-time ranges scatter rows across buckets); expose the existing `skip_queue=true` Delta-direct
   path (`database.rs:2714`) as a gRPC bulk-load mode so huge historical loads never sit in WAL+MemBuffer.

### P2 — Observability & silent failures

- Add to `timefusion_stats`: per-commit latency, commits/sec, drained-bytes/sec, per-topic **WAL
  cursor lag**, oldest stuck bucket `(project, table, age)`, and `flush_failed_total`. Today flush
  health is invisible over SQL — we had to SSH for all of the above.
- Fix the silent degradations: `persist delta snapshot … No such file or directory` (every commit,
  bad local path) and tantivy `tar append` (~10/hr, search index silently not built). Alert when
  either exceeds a rate instead of logging at warn forever.

## Suggested order

1. P0 config/ops (WAL+disk+container-limit) — immediate, no code.
2. P0 OOM diagnosis (`OOMKilled` + RSS/heap).
3. P1 #1 (per-topic WAL queue) + #2 (bounded-chunk flush) + #3 (self-healing) — failing-test-first.
4. P1 #4 (WAL truncation) + #5 (backfill bulk path).
5. P2 observability + silent-failure fixes.

## Open questions / verification

- **What drives RSS to the host ceiling?** (root cause #1 — unconfirmed). Until known, keep the
  MemBuffer cap modest and don't raise the container limit.
- **Why don't the 9 old buckets drain** with no flush error? Needs the stuck-bucket stat (P2) or a
  temporary `RUST_LOG=debug,timefusion::buffered_write_layer=debug` to capture the
  `MemBuffer flushable buckets: count=…, cutoff=…` / `Flushing N → M commits` lines.
- **Restart frequency:** still ~2 in 90 min observed (latest were graceful/fast). Watch
  `queries_total` resets + boot `clean_shutdown=` lines after the P0 changes land.
