# Work count, not code speed — the levers after the CPU campaign

2026-09-15. The CPU-per-byte campaign (`2026-09-15-cpu-per-byte-campaign.md`)
closed at −26–31% CPU/row and its own honest floor: no function above ~2%,
the last profiled bucket is required column-mapping work. What remains between
here and 10x is the number of rows the system writes and re-reads per row
ingested. This plan decomposes that number — measured today, not quoted from
memory — and ranks the levers.

## The decomposition, measured from the Delta log (2026-09-14T21:00 → 09-15T22:00 UTC)

Method: every commit in `_delta_log/` carries `commitInfo.operation`, and every
`add` carries `stats.numRecords` and `partitionValues.date` (verified populated
across sampled WRITE and OPTIMIZE commits). Summing adds per operation over a
25-hour window gives the write-amplification split with **no deploy and no new
counters** — durable history, immune to process restarts.

| table | operation | commits | rows written / day | bytes / day |
| --- | --- | --- | --- | --- |
| otel_logs_and_spans | OPTIMIZE (packing) | 1,897 | 364.6M | 53.7 GB |
| otel_logs_and_spans | WRITE w/ removes (1:1 file rewrites) | 359 | 247.8M | 30.4 GB |
| otel_logs_and_spans | WRITE, no removes (flush = ingest) | 297 | **12.3M** | 1.5 GB |
| otel_metrics | OPTIMIZE | 994 | 263.9M | 9.5 GB |
| otel_metrics | WRITE (flush) | 83 | 16.3M | 0.7 GB |
| all rollup tables combined | build/rewrite | ~1,050 | ~5.7M | ~0.2 GB |
| **total** | | ~4,700 | **~910M** | **~96 GB** |

**Row write amplification: ~32x** (910M written / 28.6M ingested-to-Delta).
Byte amplification: ~42x. The old "36 rows per ingested row" figure is not
stale — it is current, and now it has a shape:

- **69% is OPTIMIZE** — packing/compaction re-writing rows.
- **28% is 1:1 whole-file rewrites** (each commit adds exactly what it
  removes; ~410/day on spans, avg 604k rows/file).
- Flush is 3%. Rollup builds are 0.6% — the rollup lane, after the 09-11/09-15
  no-op-skip work, is no longer a write-volume problem at all.

### The age split is the decisive fact

Splitting the same window's adds by partition date:

| table | op | today | day 1–2 | older |
| --- | --- | --- | --- | --- |
| spans | OPTIMIZE | 356.1M | 3.0M | ~0 |
| spans | WRITE (both kinds) | 260.1M | ~0 | ~0 |
| metrics | OPTIMIZE | 263.9M | 0 | 0 |

**~99% of all maintenance churn is today's partition.** This is not a backlog
draining (that debt was paid — 1.23 TB → 16 GB); it is the steady-state cost
of keeping the hot day compact. Today's 12.3M ingested spans rows get
rewritten **~49 times on the day they arrive** (604M today-churn / 12.3M);
metrics rows ~16 times. A geometric merge policy pays O(log₂ N) rewrite rounds
per row — call it 5–7 for the file counts involved — so roughly **7x of the
32x is structurally necessary and the rest is policy**.

This also closes a loop with the read path: `dedup_denied_fp_moved` showed
today's partition can never certify because its fingerprint moves every few
minutes. The mover is this same churn. Cutting it improves write CPU, cert
stability, and scan-dedup skip *together*.

## Levers, ranked

### L0 — attribution tags on every maintenance commit (BLOCKING, behaviour-neutral)

Two gaps the decomposition could not close from history alone:

1. The 1:1 rewrite lane is unattributed. 410 rewrites/day vs ~17/h; candidate
   authors are Dedup rewrites (dedup drops rows though — these commits don't),
   sort-laundering, dirty-bin rewrites. Log greps over one hour showed 198
   Dedup completions, 71 Packing, 3 Repair — completions ≠ commits.
2. `timefusion_stats` resets per deploy, so trends need OTel.

Ship: (a) a custom `commitInfo` field naming the authoring op + unit on every
maintenance commit (delta-rs supports commit metadata), making every future
decomposition exact from durable history; (b) per-op `rows_written` /
`bytes_written` OTel counters so `monoscope chart` can answer "did it
improve?" across restarts. Both are small and change no behaviour.

### L1 — THE lever: bound intraday packing to geometric rounds (~69% of churn)

Target invariant: **a row is rewritten by OPTIMIZE at most ~log₂(files/day)
times**, not ~30–49. Candidate mechanisms, in increasing order of change:

- **Fan-in floors that actually bind.** The value guard and `min_files` exist,
  but 1,897 OPTIMIZE commits/day on spans (~79/h) re-merging an average of
  192k rows each says the hot-tail pass is repeatedly folding a large
  converged file with a trickle of small newcomers — the classic
  one-big-file+trickle pathology that `bin_breaks_size_ratio` was built to
  refuse. First step is a census, not a patch: per (project, day), count
  OPTIMIZE commits and the size distribution of their inputs (all derivable
  from the log with L0 tags, mostly derivable today). If the converged output
  appears as an input in the next round, that is the bug.
- **Tiered intraday, consolidate at seal.** Accept K small sorted files
  intraday (queries already union membuffer + many files; post-SPM the Delta
  leg streams), merge to target size ONCE when the day seals. This is
  LSM-tiering for the hot level — the shape RocksDB/ClickHouse use precisely
  because leveling the hot level is quadratic.
- Success metric, stated before the change per house rule:
  `OPTIMIZE rows/day ÷ flush rows/day` on spans falls from ~30 toward ≤8,
  while p95 file-count per (project_id, today) stays bounded and read
  latency does not regress. `dedup_denied_fp_moved` should fall as a side
  effect.

Constraint compliance: today's partition holds no certifications (its
fingerprint moves constantly — that is the point), so packing-policy changes
there cannot void proofs. No new backlog is manufactured; work is *removed*.

### L2 — name and shrink the 1:1 rewrite lane (~28% of churn)

Blocked on L0's attribution. Then, by author:

- If Dedup-by-rewrite: deletion vectors shipped 09-06 — rewrites of 604k-row
  files to drop a handful of dupes should be DVs (`work.Dedup.rows_dropped`
  was 46.7k in 25 min against 248M/day of rewrite volume — a ~5000:1
  write-to-drop ratio if dedup is the author).
- If sort-laundering (un-lying footers): sort-at-write is already prod
  (`repair_sorted_at_write_total`), so new files should be born honest and
  this lane should be a finite backlog — bound it per day and let it retire.
- If dirty-bin: the counters read 0 this boot; unlikely.

### L3 — otel_metrics packing policy (piggybacks on L1)

16x churn to support 0.7 GB/day of ingest (994 commits, 9.5 GB/day rewritten).
Same code path as L1; verify the fix reaches it, don't assume.

### L4 — read-side work count (mostly the team's arc; two open items)

Cert durability and rollup-coverage destruction are RESOLVED (#296–#300 — do
not re-open). Remaining, from the five-levers close-out:

- Teach the rollup matcher `coalesce(dim, const)` → stored dim (712
  `unknown_filter`/hr, the status-code timeseries every dashboard runs; uses
  existing dims, zero backfill). Precondition: first verify the plan-cache's
  parameterized `time_bucket(?, …)` is not what produces `unaligned_bucket`
  (551/hr) — if alignment is unprovable for every cached plan, fix that first
  or the matcher work under-delivers.

### L5 — decisions that are the user's, not this plan's

1. **`TIMEFUSION_LANDED_SKIP_ENABLED=true`** — validated end-to-end, one
   CapRover env flip, restarts prod; batch with the next deploy. Removes
   manufactured duplicates (~8–14% of rows in sampled windows) after unclean
   exits, which is upstream *input* to dedup work.
2. **Sessions/RUM rollup tier** — only with an accepted backfill cost.
3. **Tier D maintenance offload** — the only lever that removes the single-box
   ceiling; conflicts with the 2026-08-03 single-process directive. Not needed
   for 10x if L1/L2 land; becomes the conversation at ~20x.
4. **x86-64-v4 (AVX-512) canary** — untested `--build-arg TARGET_CPU` flip,
   host supports it; maybe a few percent.

## The 10x arithmetic, revised (estimates marked)

Today: 910M maintenance-written rows/day for 28.6M ingested. If L1 lands at
≤8 rounds and L2 halves its lane (both *estimates* until their censuses run):
~250M/day — **32x → ~9x**. At 10x ingest that is ~2.5B rows/day of
maintenance write work, ~2.7x today's absolute volume — inside what the box
already does with the maintenance pool at 29% of ~12–16 cores. Combined with
the campaign's −26–31% coefficient, the naive "217 cores" becomes an
*estimated* ~50–70 — still tight against the 32-core cgroup, which is why L4's
read-side multipliers (rollup hits, dedup skip) and the L5 decisions stay on
the table. The claim this plan stands behind is narrower and testable: **the
write-amplification ratio is the metric, 32x is today's measured value, and
L1+L2 name the two lanes that own 97% of it.**

## Rules this plan inherits (all previously paid for)

- Counter first, read it, then patch (four dead attributions in two days once).
- One change per deploy; deploys kill in-flight units and reset coverage; ≥2h
  quiet before trusting prod numbers; never measure a young process.
- No work that voids certifications/rollups or manufactures backlog; file
  rewrites outside today's partition are census-only until cert impact is
  understood.
- Every experiment names the metric it moves before it ships.
