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
| — | `maintenance_scan_pruning` (`bytes_scanned` per maintenance query) | refuted my own probe-cost hypothesis |

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

The fix is written and tested but **not pushed** (see below): let a clean slice
certify the files whose whole span it covered, as `stale` per-file evidence.
Producer only, read flag stays OFF, so query results cannot move.

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

- **Slice certification** — implemented, 3 green full-suite runs, plus two new
  soundness tests (boundary-straddling file not certified; slice evidence stays
  stale across a restart). Held because the suite has shown 1-2 parallelism
  flakes per run and I will not push a certification change on an ambiguous
  signal after tonight's incident.
- **Rollup admission re-land** — the same reclassification via
  `retry_admission_busy`, to go out ALONE, after the above is verified in prod.

## What I would do next, in order

1. **Ship slice certification (producer only), watch `cert_slice_files_proved`.**
   Expect a modest number: a ten-minute slice can only certify files whose whole
   span fits in ten minutes. The ratio against `cert_slice_files_unproven` says
   which slice widths would pay.
2. **Then flip `timefusion_read_dedup_skip_per_file`** after diffing `count(*)`
   with it on and off over a churning partition — the flag's own doc comment
   demands exactly that, and its failure mode is a silent over-count on every
   dashboard tile. This is the step that should move 14 d latency.
3. **Find where dedup's seconds actually go.** `maintenance_scan_pruning` says
   probes scan little (0.43 GB across 29 scans, max 103 MB), so it is NOT an
   unpruned whole-day scan. The instrument does not cover the rewrite path, and
   `dedup_probe_ctx` builds a provider over every file of the partition BEFORE
   any query runs — invisible to any scan metric. Attribute it from logs first:
   unit `ran_secs` vs summed probe `elapsed_ms` vs `wave_bin_staged.staging_ms`.
4. **Repair is running and retiring nothing** — 1,059 worker-seconds for zero
   queue movement, `compaction_incomplete` 23 → 126. Not diagnosed.
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
- The probe-reads-the-whole-day hypothesis is **refuted for probes**,
  provisionally — thin sample, and the instrument cannot see rewrite scans.
