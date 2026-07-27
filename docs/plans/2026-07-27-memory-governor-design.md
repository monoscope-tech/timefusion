# Unified memory governor — implementation design

Date: 2026-07-27. Companion to `2026-07-27-long-term-architecture.md` (class B).
**Status: designed, then adversarially reviewed against the code. Parts 1-2 and
4-5 survive with corrections; Part 3's WAL inversion is REFUTED as originally
argued and is now scoped down.** Review verdicts are inline; see §0 first.

## §0 — What the review overturned (read this before implementing)

Five corrections, each material:

1. **There is no false ack today.** `insert()` rejects at `bwl.rs:735-749`
   *before* the WAL append, and that error propagates through pgwire/gRPC to the
   client. So "a write is rejected after being acked" — the headline invariant
   the whole inversion was meant to establish — **already holds**. The real
   seam is narrower: *drop-instead-of-durable* after
   `write_backpressure_timeout` of failed relief, i.e. only when Delta is
   already down, and the producer DLQ covers it. This changes the priority of
   Part 3 from "highest-value" to "optional, after everything else."
2. **The WAL hard cap bounds DISK, not RAM.** 192GB of WAL headroom is *hours*
   of ingest; RAM headroom above the 120% limit is *minutes*. Making the WAL cap
   "the single ingest gate" would convert a bounded, client-visible rejection
   into an unbounded RAM ramp — on a box already OOM-killing 4×/9h. The
   inversion's core safety argument is false as written.
3. **The WAL cursor is position/hold-based, not count-based** — the watermark
   rework landed 2026-07-03 (`bwl.rs:797-800`). The design's invariant-(5)
   reasoning came from a *stale comment* (`bwl.rs:872-874`), not the code. A
   genuine adjacent seam does exist and went unexamined: `with_wal_pin` removes
   the pending hold even when `apply` fails (`bwl.rs:2620-2624`), so a
   durable-but-unapplied entry can become unpinned.
4. **`process_rss_bytes` reads TOTAL RSS** from `/proc/self/statm` — including
   file-backed mmap pages. Walrus WAL segments are mmapped, so a multi-GB WAL
   backlog inflates "drift" with pages the kernel can drop for free. It is also
   Linux-only (returns None on macOS, so the governor silently no-ops in local
   tests). Drift-bias as specified is refuted; it needs `smaps_rollup` or memcg
   `anon`, plus a floor/ceiling.
5. **The biggest untracked consumer was missed entirely.** Parquet decode is
   explicitly outside every pool (`config.rs:1284`) and is what actually OOM'd
   prod on 2026-07-20. It must be the *first* ledger entry, not one of six.

Also corrected: the proposed fractions don't sum to 72% (0.66 without foyer,
0.83 with), the "query 0.30→0.15 is a no-op" claim is false on the actual 188GB
box (32GB→28GB), the light-optimize slice *is* carved out (so don't
double-count) but the claimed underflow risk is impossible (`.min(pool × 3/4)`
guards it), and `memory_fraction` (0.9) makes the existing check *overstate* the
query pool — a conservative bias, not an OOM cause.

**Revised priority order:** the ledger for untracked consumers (Part 1) and the
boot audit (Part 2) are the real wins. Pre-emption (Part 4) must shed
*untracked* consumers first — deferring maintenance is self-defeating.

## The diagnosis

There is no single meter. There are **six independent, non-communicating
budgets**: the `reserved_bytes` CAS in `buffered_write_layer.rs` (charged
against a `max_memory_bytes()` that subtracts foyer+tantivy from the *MemBuffer*
knob only), the Greedy query pool (`database.rs:3372`), the maintenance
FairSpill pool, the light-optimize slice carved from it (:3420-3450), foyer's
internal capacities — and **nothing at all** for parquet decode, maintenance
`collect()`, flush-encode transients, tantivy writer heap, the DML coalescer
queue, and DML mem-legs.

Consequence: every budget can read "within limits" while RSS is 89GB. That is
not a tuning problem, it is a missing invariant.

## Part 1 — The meter (`src/memgov.rs`, new)

One `Arc<MemGovernor>`, built in `Database::new` before the runtime env and the
buffered layer. State is atomics only — no locks on the hot path: a fixed
`limit_bytes`, one class record per
`Consumer{MemBuffer, Query, Maintenance, LightOptimize, Foyer, Transient}`
(`budget`, `charged`, `peak`, `denials`), a 1s-sampled `rss_bytes`, and a
signed `drift_bias`.

Three primitives:

- `try_charge(Consumer, bytes) -> Option<Charge>` — RAII guard, `#[must_use]`,
  `Drop` decrements, with `grow`/`shrink` so an estimate can be corrected in
  place. **This is the only way to add to the ledger.** RAII matters here
  specifically: today `release_reservation` is a manual `fetch_sub`
  (`bwl.rs:679`) and `mem_buffer.rs:865` documents a real prod incident where a
  `fetch_sub` underflow wrapped to `USIZE_MAX` and rejected every write
  forever. A guard makes that unrepresentable.
- `charge_forced(Consumer, bytes) -> Charge` — never fails, counts
  `forced_total`. Used only where durability already happened (Part 3).
- `pressure(Consumer) -> {Normal, Soft, Hard}` — from both the class ledger and
  global RSS.

**Composing with DataFusion:** a `GovernedPool` newtype implementing
`MemoryPool`, wrapping the existing Greedy/FairSpill pools rather than replacing
them. `try_grow` charges the governor first; on governor denial it returns
`ResourcesExhausted` *without* touching the inner pool. Keeping the pool's own
error type is what lets DataFusion's spill machinery engage exactly as it does
today — and preserves the FairSpill→Greedy lesson from 2026-05-28. Wrap all
three pools at construction; the wrapper is additive.

**REVIEW VERDICT: SOUND-WITH-CAVEATS.** DataFusion is pinned at 54.0.0 and a
delegating `MemoryPool` wrapper is mechanically fine, but "spill machinery
engages unchanged" oversells it: **spilling is per-operator opt-in** — sorts and
aggregations catch `try_grow` failure and spill; joins, Parquet decode, and most
other operators propagate it as a **query failure**. Budget accordingly. Two
further caveats: `grow()` is infallible and bypasses the governor's veto
entirely (so untracked-but-charged paths need care), and double-accounting is
only a problem if the ledger *also* carries the static pool budget as reserved —
**charge live bytes in exactly one place** or you deny at ~half budget. TF runs
Greedy by default and has a FairSpill-starved-ingest history (2026-05-28), so do
not let the wrapper quietly re-import FairSpill semantics.

### MEASURED: parquet decode heap in prod (2026-07-27, `component='scan_decode'`)

Step 3 of the rollout (accounting-only) shipped in `fc8007d` and answered the
sizing question the rest of this document was guessing at. Read from
`timefusion_stats` after ~40 minutes of live traffic:

| metric | 09:52 (quiet) | 10:20 (busy) |
|---|---|---|
| `bytes_total` | 5.2 GB | **254.5 GB** |
| `peak_batch_bytes` | 1.26 MB | **145.8 MB** |
| `polls_inflight_peak` | 1 | **6** (of 16 permits) |
| `worst_case_heap_mb` | 1.2 | **834** |

Conclusions, which change the plan:

- **A single decoded batch reached 145.8 MB** — 115× the quiet-period figure. So
  decode memory is driven by batch *size*, not just concurrency, and any
  per-stream estimate calibrated in a quiet window is ~100× low.
- **Observed worst case is ~834 MB**, and the ceiling at full gate saturation is
  `145.8 MB × 16 ≈ 2.3 GB` — not the tens of GB feared. **A Transient budget of
  ~3 GB covers decode at the current `MAX_CONCURRENT_SCAN_READERS=16`.**
- That reconciles with the 2026-07-20 OOM: at the 48-way concurrency of the time,
  `145.8 MB × 48 ≈ 7 GB`, which alone exceeds this box's ~8.7 GB of slack. So
  `GatedScanExec`'s cap of 16 is what currently keeps decode survivable, and the
  gate — not a ledger — is doing the real work today.
- Throughput is ~6.7 GB/min of decoded Arrow, so the *cumulative* counter is a
  churn signal, not a memory signal; only the peak product bounds heap.

**Implication for Part 1:** decode no longer needs to lead the ledger for
*safety* — it is already bounded by the semaphore. It should still be charged so
the budget is honest, but the unbounded consumers (DML coalescer queue, DML
mem-legs, maintenance `collect()`) are now the higher-risk entries, since nothing
caps them at all.

**The `Transient` ledger is the single highest-value piece of this whole
document** — and per review it must lead with the consumer that actually killed
prod. Ordered by demonstrated blast radius:

1. **Parquet scan decode** — explicitly outside every pool
   (`config.rs:1279-1287`) and the direct cause of the 2026-07-20 OOM. Charge at
   the existing `GatedScanExec` choke point, replacing a bare semaphore permit
   with permit+charge. *If only one thing from this document ships, ship this.*
2. **Walrus WAL mmap and tantivy index memory** — absent from every budget
   today, and a large part of why RSS − ledger is big. (Also the reason the
   drift signal in §1 must exclude mapped-file pages.)
3. **Maintenance `collect()`** — the `dedup_max_decoded_bytes` estimator already
   exists (`config.rs:347`); it simply isn't charged anywhere.
4. **Flush encode** — per group inside `buffer_unordered`.
5. **Tantivy writer heap** — charged when the permit is acquired, replacing the
   *static* worst-case subtraction at `bwl.rs:598-604`, which frees budget
   whenever no index build is running.
6. **DML coalescer queue** and **DML mem-legs** (`update_with_source` needs ×2 —
   it clones *and* IPC-serializes).

**Reading the limit:** `memgov::detect_limit()` — cgroup v2 `memory.max` (plus
`memory.high` if lower), v1 `memory.limit_in_bytes` (treat ≥ `i64::MAX/4096` as
unlimited), else `sysinfo::total_memory()`. Refactor `autotune.rs:56-57` to call
it so boot-time and runtime agree by construction. Also sample **PSI**
(`memory.pressure`) and `memory.events`: a nonzero `max` delta is the only
signal that reliably precedes an OOM kill, and it is free to read.

### Drift reconciliation — the load-bearing idea

Both existing estimators are documented as wrong in *opposite* directions:
`estimate_reservation` is `get_array_memory_size() × 1.15` (a lower bound that
ignores 48-arena jemalloc fragmentation), while `mem_buffer.rs:174-198`'s
counter drifts *high* on coalesce (the 948GB-on-a-44GB-budget incident).

So don't try to make estimates exact. Each 1s tick:

```
untracked  = measured_unreclaimable_RSS − Σ charged
drift_bias = ewma(untracked)            // fast-attack, slow-decay, clamped ≥ 0
available  = limit − Σ charged − drift_bias − slack
```

The governor never has to be right about any individual estimate, because the
aggregate error is measured every second and charged to everyone. A 15%-low
estimator just grows `drift_bias` by 15% of the ledger; an over-counting one
yields negative `untracked`, clamps to 0, and wastes a little headroom — the
safe direction. If `drift_bias > 25% of limit`, log `error!` with the per-class
ledger: something large is allocating entirely outside the ledger.

**⚠ REVIEW VERDICT: REFUTED as specified.** Three defects, all confirmed
against the code:

- `process_rss_bytes` (`bwl.rs:224-230`) reads `/proc/self/statm` field 2 —
  **total** RSS, including file-backed mmap. Walrus WAL segments are mmapped, so
  a large WAL backlog is counted as "drift" even though the kernel can drop
  those pages for free. No "unreclaimable RSS" is derivable from statm at all.
- **The ratchet is real.** Fast-attack EWMA charges drift on every burst, while
  jemalloc dirty-page decay makes `free()` show up in RSS seconds late — so each
  deny-then-free cycle *raises* measured drift before slow-decay releases it,
  trending toward zero headroom exactly when the governor is active.
- It is **Linux-only** (None elsewhere), so the governor silently no-ops in
  local tests — the worst possible failure mode for a safety mechanism.

**Required before this ships:** measure unreclaimable memory properly
(`/proc/self/smaps_rollup`, or memcg `anon` + `kernel` from `memory.stat`, never
statm), subtract mapped-file pages explicitly, put a hard floor *and ceiling* on
`drift_bias`, symmetrize the EWMA (or gate attack on a PSI signal rather than on
raw RSS deltas), and give the non-Linux path an explicit loud degradation rather
than a silent zero. Also correct in place where a real number is available
(post-encode parquet size → `charge.shrink()`) and tune jemalloc
(`background_thread`, `dirty_decay_ms`) instead of letting the bias absorb
allocator lag.

## Part 2 — Boot-time admission

**Recommendation: auto-clamp proportionally with a loud `error!` and a stats
row — do not refuse to start.** TF is the durability layer for an ingest
pipeline; refusing to boot converts a misconfiguration into an outage with a
growing WAL backlog, and boot recovery is itself long and fragile. Clamping is
better provided it is loud, visible in `timefusion_stats`, and switchable:
`TIMEFUSION_MEMORY_ADMISSION=clamp|warn|refuse` (default `clamp`; `refuse` for
CI and fail-fast operators).

Replace `memory_oversubscription` (`autotune.rs:182-189`) with a
`budget_audit()` that fixes **four** independent accounting bugs, each alone
sufficient to have caused an OOM:

1. **The maintenance pool is omitted entirely** — with the prod-recommended
   `TIMEFUSION_MAINTENANCE_POOL_GB=24`, prod is silently 24GB over its own
   declared invariant.
2. **MemBuffer is counted at nominal, not at its 120% hard ceiling**
   (`HARD_LIMIT_HEADROOM_DIVISOR`) — a 20% systematic under-count of the
   largest consumer. This is why a 19GB budget shows a ~23GB observed limit.
3. **`memory_fraction` is applied at allocation (`database.rs:3376`) but not in
   the check** (`autotune.rs:183` uses raw `memory_limit_gb × 1024`) — the
   check and the allocation disagree.
4. **The DataFusion metadata cache is omitted.**

Plus: the light-optimize slice is carved *out of* the maintenance pool, so it
must **not** be double-counted — and nothing currently tests that
`light_optimize_pool_bytes() <= maintenance_pool_bytes()`, whose violation
would underflow the subtraction at `database.rs:3446`.

**REVIEW VERDICT: (i), (ii), (iv) CONFIRMED as real bugs. (iii) is real but
harmless — `memory_fraction` defaults to 0.9, so the check *overstates* the query
pool by 10%, a conservative bias, not an OOM cause; fix it for correctness, don't
sell it as a cause.** The carve-out claim is confirmed (`database.rs:3440`
subtracts the light slice, so count maintenance once), but the claimed underflow
risk is **impossible**: `light = (pool/3 × N).min(pool × 3/4)`
(`database.rs:3431-3434`) always leaves heavy ≥ pool/4 — the `.min` guard is the
whole point of that line. Keep the invariant test anyway; drop the alarm.

**The audit's real gap:** stopping at config knobs misses the consumers that
actually killed prod — Parquet decode (`config.rs:1279-1287`, explicitly
unpooled), walrus mmaps, and tantivy. An audit of four knobs would have passed
on 2026-07-20 while the box died. The `Transient` ledger, not the audit, is the
load-bearing piece.

`SLACK_FRACTION = 0.20` for thread stacks (48 × 2MB Tokio + Rayon), walrus WAL
mmaps, jemalloc per-arena dirty pages, and Arrow scratch outside any pool —
empirically the gap between the declared ~72% invariant and the actual kills.

**The originally proposed fractions are withdrawn — REVIEW REFUTED the
arithmetic.** query 0.15 + buffer 0.10 + maintenance 0.13 + transient 0.08 +
slack 0.20 = **0.66**, and 0.83 once the existing foyer 0.15/0.02 is included —
no reading gives the claimed 72%. The "query 0.30→0.15 is a no-op above 213GB"
claim is also false where it matters: on the actual 188GB box the cap
(`clamp(1,32)`, `autotune.rs:38`) binds at 0.30 giving 32GB, while 0.15 gives
28GB — a real reduction on precisely the host this is for.

Worse, **buffer 0.25→0.10 combined with Part 3 is actively dangerous**: it
shrinks the reservoir 2.5× *while* removing the overflow valve, which makes the
ungoverned force-admit path the steady state under any ingest spike. Do not do
both.

Correct approach instead: derive the transient budget from *measured* data
(rollout step 3 below is pure measurement for exactly this reason), keep the
MemBuffer fraction where it is until measurement says otherwise, and only then
re-derive a set of fractions that provably sums under the limit with a
property test over host sizes. Small hosts are fine either way — the existing
floors (`MIN_QUERY_POOL_GB=1`, `MIN_BUFFER_MB=256`, maintenance 1GiB) yield a
runnable 4GB config.

The clamp must scale only the *elastic* classes and **never** shrink MemBuffer:
shrinking the ingest buffer under a fixed inbound rate converts a memory problem
into a rejection problem.

## Part 3 — Universal write admission, and the WAL inversion

### REFUTED: do not invert the WAL append

The original proposal was to reorder `insert()` to WAL-append before admission
so that "a write can never be rejected after being acked." **The review
established that this invariant already holds** — the rejection at
`bwl.rs:735-749` propagates as an error through pgwire/gRPC to the client, so no
ack precedes it. The inversion would therefore buy no durability, while its
safety argument (the WAL hard cap becomes the single gate) is false: that cap
bounds *disk* at 192GB ≈ hours of ingest, whereas RAM headroom above the 120%
limit is minutes. With a wedged `flush_lock`, force-admit is an unbounded RAM
ramp on a box already OOM-killing four times in nine hours.

The restart story is also worse than assumed: if MemBuffer was 3× budget at OOM,
all of it is unflushed WAL; on boot, replay refills to hard pressure, parks
(`bwl.rs:621-628`), and waits on the same flush path that was wedging. Live
force-admitted ingest is *not* pausable the way replay is, so the combination is
a replay-park + force-admit loop — the 2026-07-09 6h-boot outage lineage.

**What to do instead**, in priority order:

1. **Fix the wedge** (companion doc, Part 1). The rejection only fires after
   `write_backpressure_timeout` of *failed relief flushes* — i.e. when flush is
   already broken. Unwedging flush removes the trigger, which is strictly better
   than removing the brake.
2. **Examine the genuine seam the design missed:** `with_wal_pin` removes the
   pending hold even when `apply` fails (`bwl.rs:2620-2624`), so a
   durable-but-unapplied entry can become unpinned. That is where
   "durable but not admitted" actually intersects cursor advance, and it is a
   real correctness question — unlike the invented one.
3. **Keep `TIMEFUSION_WAL_ADMIT_DECOUPLE` off** unless and until a soak proves
   otherwise; its own config doc already demands that soak. Note that
   `force_reserve` exceeds even the 120% ceiling, so enabling it also
   invalidates the boot audit's MemBuffer term.

The remaining items in this Part are still worth doing, because they close
*real* gaps rather than a hypothetical one.

### Closing the other unreserved paths

- **pgwire has no soft gate at all** while gRPC soft-rejects at 85%. Add a
  Postgres `NoticeResponse` (SQLSTATE 53200) alongside the success tag under
  soft pressure — protocol-legal, every client either surfaces or safely
  ignores it. Under hard pressure, a bounded *delay* is the backpressure signal
  a synchronous protocol understands. This is the cheapest real win in the Part.
- **gRPC's 85% reject stays a reject** (the original proposal wanted it softened
  to a hint; with the inversion refuted, that would just remove a working brake).
  It is a pre-durability rejection surfaced to a client that retries — which is
  correct behavior, not a seam.
- **DML mem-legs** charge `Transient` before `with_wal_pin`. They already
  WAL-append before mutating, so they are already durable-then-apply; they just
  need metering.
- **Coalescer enqueue** charges, and on denial **drains synchronously** rather
  than growing (the caller already WAL-appended, so delay is the only correct
  response — never drop). Add a byte trigger alongside the row trigger: at the
  config's own 4KB/row estimate, 1M rows is ~4GB of retained batch clones with
  no byte bound.

## Part 4 — Runtime governance

Priority: **ingest > query > light-optimize > maintenance.**

**Tier ordering CORRECTED by review — shed UNTRACKED consumers first.** The
original design put maintenance deferral at tier 1. That is self-defeating and
re-runs a root-caused incident: deferring compaction produces file-count blowup
(sealed days at 300-4800 files, 2026-07-20/21) whose read-side cost is
*untracked* Parquet decode memory. Meanwhile maintenance rewrites run in a
FairSpill pool (`database.rs:3408-3427`) that spills to disk and **errors rather
than OOMs**. So tier 1 would trade a bounded, pooled, spillable consumer for an
unbounded untracked one — the wrong direction.

Corrected order:

1. **Wide scans (untracked, the actual 2026-07-20 OOM source)** — shrink
   `heavy_scan_sem` from 16 toward a floor of 2 via `forget_permits`, restore
   with `add_permits`. Confirmed available in the pinned Tokio 1.48, and it
   removes only *available* permits (never revokes held ones), so in-flight
   decoders keep their memory and no query is killed — new ones just serialize.
2. **Maintenance** — only after scans are throttled, and with the
   guaranteed-progress escape below. Gate the five `maintenance_rewrite_sem`
   sites to *defer* (not block — they run under table timeouts, so blocking just
   becomes a timeout) and cancel in-flight rewrites via a `CancellationToken`.
   The existing dedup skip path is the precedent: abandon loudly, read-side dedup
   keeps queries correct.
3. **MemBuffer relief** — existing `pressure_flush_pct`/`MAX_RELIEF_ROUNDS`
   machinery, but triggered by *global* pressure. This is the key behavioral
   win: today MemBuffer flushes at 75% *of its own budget*, blind to a 24GB
   maintenance sort next door.
4. **Delay ingest.** Never reject.

**Hysteresis:** high 80% / low 65% of `limit − slack`, **minimum 30s dwell per
tier** (60s for re-enabling maintenance) — the anti-oscillation term matters
most because maintenance jobs run for minutes and a 5s flap would cancel work
repeatedly and prevent all forward progress. De-escalate one tier at a time.
**PSI override:** a nonzero `memory.events:max` delta or `full.avg10 > 10`
forces immediate top-tier escalation regardless of dwell — that is the
"about to be killed" signal and dwell must not gate it.

**Mandatory:** alert on `maintenance_deferred_total`, and give maintenance a
guaranteed-progress escape — one rewrite per hour runs regardless of tier.
Without it, pre-emption becomes the file-count blowup it was meant to prevent.

## Part 5 — Observability

A `memgov` component in `timefusion_stats` (the table is `(component, key,
value)` triples, so this is additive). One query must answer "why are we about
to die":

- **Limit & measurement:** `limit_bytes`, `rss_bytes`, `rss_pct_of_limit`,
  `unreclaimable_bytes`, `psi_some_avg10`, `psi_full_avg10`,
  `cgroup_memory_events_max` — that last one alone would have flagged all four
  kills.
- **Per class:** `budget`, `charged`, `peak`, `pct`, `denials_total`.
- **The reconciliation triple** (most diagnostic): `ledger_total_bytes`,
  `drift_bias_bytes`, `untracked_pct`. Large untracked ⇒ "the ledger is lying";
  large class pct with small drift ⇒ "we know exactly who".
- **Boot audit** (so a misconfig is queryable, not just in a rotated log line):
  `audit_committed_bytes`, `audit_slack_bytes`, `audit_verdict`,
  `audit_clamped_knobs`.
- **Governance:** `tier`, `tier_since_secs`, `escalations_total`,
  `maintenance_deferred_total`, `maintenance_cancelled_total`,
  `scan_permits_available`, `forced_admits_total`, `ingest_delayed_total`.
- **Transient sub-ledger** (which untracked consumer woke up): scan decode,
  maintenance collect, flush encode, tantivy, coalescer, DML legs.
- Plus `mem_buffer.estimate_drift_bytes` = the drifting counter minus the
  authoritative walk, making that documented drift visible instead of a comment.

Mirror class gauges into `metrics.rs` as OTel gauges so alerting needs no SQL
polling.

## Rollout order (each step independently revertible)

1. **Observability only** + `budget_audit` in `warn` mode. Zero behavior
   change, and it immediately answers whether the box is oversubscribed today
   (likely by >24GB) and gives a `drift_bias` baseline.
2. **Boot admission** `warn` → `clamp`.
3. **Meter + Transient ledger, accounting-only** (`try_charge` always
   succeeds). One release of pure measurement tells you whether the six
   estimates are sane before they can deny anything — this is where you learn
   if `drift_bias` is 5GB or 60GB.
4. **Pool wrapping** as a pass-through observer (budgets = inner pool sizes),
   then tighten.
5. **Governance tiers 1-2.** Both degrade throughput, not correctness.
6. **Transient enforcement** (deny, not just record) — last of the ledger work,
   since it can newly fail scans and maintenance.

The WAL inversion is **removed from the rollout** (see Part 3). The flush unwedge
in the companion doc replaces it: it eliminates the condition that triggers
rejection instead of removing the brake that prevents OOM.

## Risks

- **`drift_bias` swamps the budgets** — if untracked allocation is 40GB+,
  effective budgets shrink hard and throughput drops. Step 3 measures this
  before anything can deny; the answer would be jemalloc tuning and a larger
  slack, not a bigger ledger.
- **Maintenance starvation** — pre-emption that defers compaction recreates the
  2026-07-20/21 file-count blowup; hence scans-before-maintenance ordering plus
  the hourly-progress escape, both mandatory.
- **Silent no-op off Linux** — the RSS/PSI signals don't exist on macOS, so the
  governor must degrade loudly rather than reading zero and appearing healthy in
  local tests.
- **`MemoryPool` wrapper divergence** if DataFusion changes the trait — avoid
  default method impls so trait changes become compile errors.
- **Missed charge sites** — `drift_bias` is the honest confession of what was
  missed, which is why the bias term is a feature rather than a workaround.

## Kill switches

`TIMEFUSION_MEMGOV_ENABLED` (master; false = record only),
`TIMEFUSION_MEMORY_ADMISSION` (clamp|warn|refuse),
`TIMEFUSION_WRITE_REJECT_OVER_BUDGET` (default false = never reject; true
restores today's `bwl.rs:750` behavior — the inverted alias of the existing
`WAL_ADMIT_DECOUPLE`), `TIMEFUSION_MEMGOV_TRANSIENT_ENFORCE`,
`TIMEFUSION_MEMGOV_MAINTENANCE_GATE`, `TIMEFUSION_MEMGOV_SCAN_SHRINK`,
`TIMEFUSION_MEMGOV_HIGH_PCT`/`_LOW_PCT`, `TIMEFUSION_MEMGOV_SLACK_PCT`,
`TIMEFUSION_TRANSIENT_POOL_GB`.
