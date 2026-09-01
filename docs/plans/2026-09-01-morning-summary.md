# Morning summary — the night of 2026-08-31/09-01

Read this first; the detail lives in `2026-08-31-maintenance-throughput-night.md`,
`2026-09-01-certification-cannot-converge.md`, `2026-09-01-slice-certification-plan.md`,
`2026-08-31-how-other-systems-schedule-maintenance.md` and
`2026-09-01-what-100x-actually-requires.md`.

## The one-line state

Maintenance now runs on all lanes and is measurable for the first time; **dedup
consumes ~76-87% of the fleet and produces no certifications at all**, which is
why multi-day query latency has not moved. 10x is a tuning problem with known
levers; 100x is not, and needs a capacity decision rather than a patch.

## What shipped and is live

| # | change | evidence it earned |
|---|---|---|
| 1-6 | repair one-pass rewrites, idleness-not-budget deadlines, loop-timeout derivation, byte-based batch sizing, coordinator pool `pool*3/5`, repair-attempt reset | repair 0 → ~30/h complete, 10.11 GB legacy debt retired; BaseRollup 121 → ~480/h |
| 7-11 | watched dedup probe, `light_optimize_k` repriced, repair rewrite semaphore, dedup bisection floor, fusion partition pricing | `pending_dedup` 5,049 → 2,808 |
| 12 | dedup gets the fleet idle window (300 s → 900 s) | units killed at deadline 36 → 4 per 25 min |
| 13 | **work-done counters** (`work.<Op>.{worker_secs,killed_secs,progress_rows}`, `work.Dedup.rows_dropped`) | made every number below possible |
| 14 | `admission_busy` is transient, not a capacity failure | `resource_admission` refusals no longer split units |
| — | `maintenance_scan_pruning` (`bytes_scanned` per maintenance query) | found the probe re-reads a partition once per hash shard (item 3 below) |
| 16 | **slice certification** — a clean slice certifies the files whose whole span it covered | the first thing that can make `cert_granted_total` non-zero; producer only, read flag OFF |

**Reverted:** deploy 15 (`d8d9d357`). See the incident below.

## What the instruments now say

**Where the fleet's time goes** (25 min, clean process):

| operation | worker_secs | share | queue move |
|---|---|---|---|
| **Dedup** | 9,084 | **76%** | −2.44/min |
| Repair | 1,059 | 9% | **flat** |
| HotPacking | 961 | 8% | +0.6/min |
| BaseRollup | 776 | 7% | −3.64/min |

Confirmed again post-revert at ~50 min uptime: Dedup 16,510 s of 18,986 (**87%**),
14,349 rows dropped, `pending_dedup` 2,819 → 2,683.

**Dedup's two products, both measured:**

- rows removed: 0.39-0.87 per worker-second;
- certifications: **zero**. `cert_granted_total = 0`, `dedup_skipped = 0` of
  **9,296** eligible scans, `dedup_denied_never_certified = 100.0%`.

**Query latency** (one busy project, chart shape):

| window | cold | warm |
|---|---|---|
| 1 h | 0.33 s | 0.40 s |
| 6 h | 20.46 s | 0.40 s |
| 24 h | 7.04 s | 1.00 s |
| 7 d | >60 s | 6.5-34.8 s |
| 14 d | 47.24 s | **42.83 s** |

Warm ≤24 h is fine. **14 d barely improves warm, so the multi-day cliff is
structural, not cache** — and the structure is `DedupExec` in every plan, which
is exactly what certification would remove. In-process percentiles: p50 82 ms,
p95 577 ms, p99 7.1 s, p99.9 24.6 s.

**Local fleet bench** at prod's 10 GB coordinator share (10 cores): peak 94 MB/s
at 6 concurrent rewrites, dips to 58 at 8 with **zero failures** — where an 8 GB
pool failed 4 of 8. **Memory decides whether concurrency fails; cores decide
where throughput peaks.**

## The finding that matters most

**Certification cannot converge, so dedup's read-path product is unreachable.**
`record_clean_slice` needs zero dropped rows AND an unchanged partition
fingerprint AND full-day interval coverage. Success trips the first, every other
maintenance lane trips the second, and the scheduler never probes a whole day for
the third. Both consumers — per-date and per-file — read the day-level grant, so
the finer one cannot rescue an empty producer.

**Shipped as deploy 16** (`8a9b0fe6`): a clean slice now certifies the files
whose whole span it covered, as `stale` per-file evidence. Producer only, read
flag stays OFF, so no query result can move. Watch `cert_slice_files_proved`
against `cert_slice_files_unproven`.

## The incident, in full

Deploy 15 reclassified the rollup admission refusal and wrote
`retry_task(.., admission_backoff(self.journal().attempts(&key)))`. A temporary in
argument position lives to the end of the statement, so that mutex guard was
still held when `retry_task` locked the same non-reentrant journal mutex. **Every
rollup worker that hit an admission refusal parked forever.**

The symptom chain is worth remembering because none of it says "deadlock":
`exit 137: dockerexec: unhealthy container` (reads as OOM; it is the healthcheck
killer) every ~6 minutes → then the flapping **stops by itself**, because a
parked fleet is quiet enough to pass its health check → the only true signal was
that **zero `work.*` counters existed after 35 minutes of uptime**, i.e. not one
unit had completed.

Reverted as `106da7ea`; prod confirmed healthy. Structural fix in the working
tree: `retry_admission_busy(key)` takes no backoff argument, so no call site can
hold a guard — `retry_task` already reads `attempts` under the lock it owns.

**Process lessons, both mine:** I stacked deploy 15 on deploy 14 before deploy 14
had reported, and I shipped a change to a *locking* path on a targeted run
because the full suite was green — but no test exercises admission refusal under
contention, so green meant nothing there.

## Held, not pushed

- **Rollup admission re-land** — the same reclassification via
  `retry_admission_busy`, to go out ALONE, after the above is verified in prod.

## What I would do next, in order

1. **DONE — deploy 16 verified in prod.** `cert_slice_files_proved = 526`,
   non-zero for the first time in the system's history, with `dedup_skipped`
   still 0 (read flag off, so nothing about query results moved). **But
   `cert_slice_files_unproven = 20,423`, so only 2.5% of files are being
   certified** — the containment test rejects the rest because a ten-minute
   slice cannot contain a file that spans longer.

   **That ratio is the next lever, now quantified: the slice width must exceed
   the file span, or evidence accrues at 2.5% of what the same scan could
   prove.** Hot-packed files span hours, so the clean passes that would certify
   them are the coarsened ones. Cheap experiment before any code: raise the
   dedup coarsening floor for CLEAN partitions only, and watch the proved:unproven
   ratio move. Expect a modest number: a ten-minute slice can only certify files whose whole
   span fits in ten minutes. The ratio against `cert_slice_files_unproven` says
   which slice widths would pay.
2. **Then flip `timefusion_read_dedup_skip_per_file`** after diffing `count(*)`
   with it on and off over a churning partition — the flag's own doc comment
   demands exactly that, and its failure mode is a silent over-count on every
   dashboard tile. This is the step that should move 14 d latency.
3. **Cut the probe's repeated whole-partition scans — this is the biggest
   measured lever, and it reverses my earlier reading.** A 15-minute
   post-restart sample said scans were small (0.43 GB over 29 scans) and I called
   the probe-cost hypothesis refuted. On a **warm** fleet, 45 minutes says the
   opposite:

   ```
   462 scans   58.88 GB scanned   11,821 s of scan time
   93 scans >= 5s account for 99% of that time and 51.5 GB
   ```

   The top of the distribution is the finding:

   | scanned | time | note |
   |---|---|---|
   | 9,408 MB | 1,454 s | and the SAME 9,408 MB again at 1,410 s |
   | 3,434 MB | 90-118 s | the same 3,434 MB **six times** — 20.6 GB, ~640 s |

   **The repetition is the shard re-read.** `probe_hash_shards` runs the probe
   once per shard and each pass re-reads the selected files — the code says so
   ("Each pass may reread the selected files, but no pass can accumulate the
   whale's full key cardinality in memory"). So a 3.4 GB partition is read six
   times to answer one question, and 11,821 s of scan time in 45 minutes is
   ~27% of a 16-worker fleet's capacity spent inside scans alone, against dedup's
   87% share.

   **The local bench refutes the shard-re-read reading, and reattributes the
   cost.** `TF_BENCH_PROBE=1` on the same 1,148 MB prod file, 1 GB pool:

   | passes | secs |
   |---|---|
   | 1 shard | 0.2 |
   | 2 shards | 0.3 |
   | 4 shards | 0.4 |
   | 6 shards | 0.6 |

   Six passes cost 0.6 s, not minutes — because **the probe reads only its dedup
   key columns** (`timestamp`, `id`), and parquet prunes the rest. The same file
   costs ~43 s to scan in full (the `scan only` variant). So a key-only probe is
   ~200x cheaper than a full read, sharding it is nearly free, and **the GB-scale
   90-1,450 s scans in prod are almost certainly NOT probes** — they are
   all-column reads: the dedup chunk REWRITES (which re-read their files per
   chunk) or rollup source scans.

   **So the next step is attribution, not optimisation.** `maintenance_scan_pruning`
   carries no operation label — the gap I flagged when I published those totals,
   and the reason I twice drew the wrong conclusion from them (first "probes are
   cheap" on a cold sample, then "probes are the cost" on a warm one). Add
   `operation` and `phase` to that log line, then re-read. Changing the shard
   count on this evidence would be optimising something that costs 0.6 s.

4. **Repair is permit-starved, and the permit is sized against a pool that has
   since doubled.** DIAGNOSED. 1,059 worker-seconds for zero queue movement and
   `compaction_incomplete` 23 → 126 is not failure — it is **173
   `repair_rewrite_permit_busy` events in 40 minutes**: units claim, cannot get
   the single `repair_rewrite_sem` permit, hand the worker back (correctly — that
   is the 2026-09-01 anti-parking fix) and retry every 30 s.

   One permit x ~20-50 min per whole-file rewrite is **~2 units/hour**, against
   `pending_repair = 358`. That is ~179 hours to drain, i.e. the queue is flat by
   arithmetic, not by fault.

   **Do NOT simply raise the permit — I nearly recommended that and the code's
   own history refutes it.** My first reading was that
   `benches/rewrite_throughput.rs` runs six concurrent whole-file sorts of a
   1.1 GB file at a 10 GB pool with zero failures, so 1 permit is conservative.
   But the permit's comment records why it is 1: **prod's worst repair bin is
   2.3 GB compressed, ~28 GB decoded**, and two overlapping repair rewrites
   produced `Not enough memory to continue external sort` on 2026-09-01 — the
   moment the liveness clock let them live long enough to overlap. Two worst-case
   bins is ~56 GB decoded against a ~10 GB pool. My bench file is ~13 GB decoded,
   less than half one worst-case bin, so it does not measure this case at all.

   **The right fix is to size the permit in BYTES, not in units** — the rule
   every system in the prior-art survey follows, and the one this repo already
   applies in `maintenance_admission`. A byte budget lets several small repair
   bins overlap (which is most of the 358) while a 28 GB bin still runs alone.
   A count of 1 is the same mistake as a deadline-as-budget: it prices the
   worst case onto every unit.

   Ship it ALONE, after deploy 17 is verified, and size the budget from the
   decoded-bytes estimate the admission path already computes.
5. **Do not reallocate dedup's share** until 1-3 land: if certification starts
   working, the same worker-seconds buy a read-path win instead of nothing.

## Corrections I made to myself tonight, recorded so they are not re-made

- `rows_dropped/worker_secs` is right for comparing deploys on one work mix, and
  wrong for allocating capacity — certification, not row removal, is dedup's main
  product.
- `rollup_miss_unaligned_bucket_total` (1,798, the largest miss reason) is **not**
  about window alignment: `PartialBucket` fires on `width < grain || width % grain
  != 0`, i.e. the query's bucket width against the tier's grain. Mostly benign
  per-candidate noise. The actionable misses are `filter_not_eligible` (994),
  `stale_coverage` (309), `not_built` (203) against 199 hits.
- The probe-reads-the-whole-day hypothesis: I called it **refuted** on a
  15-minute post-restart sample (29 scans, 0.43 GB), then a warm 45-minute window
  (462 scans, 58.9 GB, 11,821 s) **supported it** — with the added finding that
  the same partition is re-read once per hash shard. The lesson is the one this
  night keeps teaching: a sample taken from a just-restarted process measures the
  restart, not the system. Wait for warm.
