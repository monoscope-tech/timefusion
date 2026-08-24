# Handoff: what is still open on 14d/30d, with the context to continue

2026-08-24. Written so someone who was not in the session can pick any item up
cold. Each item states what is known, what is NOT known, the exact commands, and
the trap that will mislead you if you skip them.

Background pages, in reading order:

- `2026-08-22-seven-window-six-project-matrix.md` — the baseline, six projects x
  seven windows, monoscope's real SQL
- `2026-08-22-make-14d-30d-complete.md` — the four failure modes and the Tier-1
  checklist

Four pages this document used to point at were deleted on 2026-08-24 under the
`README.md` policy (complete or superseded). Their conclusions, which are all
this handoff needs:

- **The coordinator was disabled on every container** — that is why `not_built`
  never cleared; fixed. (`2026-08-23-nothing-is-being-built.md`)
- **Lever 1, coarsening: the queue was 24x inflated** — a pending day minted one
  unit per slice; shipped, sealed rollup units 11,174 -> 637.
  (`2026-08-23-the-queue-is-24x-inflated.md`)
- **Lever 2, compaction throughput: three defects, all verified fixed 2026-08-24**
  — cells retire now, 275 -> 49 on the flagship cell, -28% fleet-wide.
  (`2026-08-23-lever-2-compaction-throughput.md`)
- **Sealing is slow because each sealed unit re-reads a whole partition to emit a
  dozen rows** — the cost is the commit, not the sort.
  (`2026-08-23-why-sealing-is-slow.md`)

Read any of them with `git show <sha>:docs/plans/<name>`; `git log
--diff-filter=D -- docs/plans` lists every deletion with its SHA.

## Read this first: four traps that produced wrong readings

Every one of these cost real time in-session. They are not hypothetical.

1. **`timefusion_stats` counters are PROCESS-SCOPED.** Prod restarts every 20-40
   minutes. Zeros on a young container mean nothing. Always print uptime:
   `ssh ubuntu@captain.s.past3.tech 'docker service ps srv-captain--timefusion --no-trunc | head -2'`
2. **Read `rollup_min_contiguous_days` BEFORE quoting any routing number.** It
   resets to 0 on restart and needs ~25 min to rebuild. At 0 the router may not
   attempt, so a shape records neither hit nor miss and it reads exactly like
   "the fix did nothing".
3. **`maintenance_tasks.json` is a CHECKPOINT, not live state.** It was 3h15m
   stale once and two reads 30 min apart returned byte-identical numbers, which
   looked like "nothing is draining". `stat` it first; the `.wal` beside it is
   the live one.
4. **Query latency depends on what MAINTENANCE is doing.** The 2026-08-22
   baseline was taken on a box where the coordinator had never started, so it had
   48 cores to itself. Comparing against it without controlling for
   `tasks_running` measures the background, not the change.

A fifth, more general: **a mean is not a distribution.** "p1 averages 318 MB per
file, so it is not fragmented" was wrong — p10 was 0 MB and 44% were under
target. Take percentiles.

---

## 1. Does the sealed backlog actually drain? (highest value)

**State.** After lever 1, sealed rollup units went 11,174 -> 637 and every one is
day-wide. `pending_base_rollup` 12,460 -> ~1,500-1,900. The coordinator runs at
14-16 workers and `rollup_rebuilds_full_total` reached 1,504 on one container.

**Not known.** Whether the remaining ~637 sealed units clear, and how long it
takes. Every measurement in-session was truncated by a restart. The best rate
observed was ~490 rebuilds/hr overall but only **~41/hr of sealed work** — the
rest is today's frontier, which ingest replenishes.

**How to check.**

```bash
psql "$TF" -c "SELECT key,value FROM timefusion_stats
  WHERE key IN ('pending_base_rollup','tasks_running','rollup_rebuilds_full_total',
                'rollup_min_contiguous_days','rollup_hits_hybrid_total')"
```

Sealed-only detail needs the journal (check `stat` first, see trap 3):

```bash
ssh ubuntu@captain.s.past3.tech 'sudo -n python3 -c "
import json,datetime,collections
d=json.load(open(\"/home/ubuntu/timefusion-data/.timefusion_meta/maintenance_tasks.json\"))
ts=d if isinstance(d,list) else d.get(\"tasks\",[])
p=[t for t in ts if t[\"state\"]==\"pending\" and t[\"key\"][\"operation\"]==\"base_rollup\"]
today=datetime.datetime.now(datetime.UTC).date()
day=lambda t: datetime.datetime.fromtimestamp(t[\"key\"][\"slice\"][\"start_micros\"]/1e6,datetime.UTC).date()
s=[t for t in p if (today-day(t)).days>=2]
c=len({(t[\"key\"][\"project_id\"],day(t)) for t in s})
print(\"sealed=%d cells=%d inflation=%.1fx\"%(len(s),c,len(s)/max(c,1)))"'
```

**Done when** sealed units approach 0 AND `rollup_hits_hybrid_total` climbs while
`rollup_min_contiguous_days` holds at 30. The second half matters: units
completing without routing improving means the tier is built but unreachable, a
different bug.

**The blocker is organisational.** This needs a few hours without a deploy.
Coverage takes ~25 min to rebuild and prod restarted ~15 times in one session.
Consider pausing the deploy train for one evening; nothing else will settle it.

---

## 2. Does routing offset the maintenance load?

**State.** Routing works — `rollup_hits_hybrid_total` reached 79. All routing
RULES are fixed: `filter_not_eligible` 0 (null guard), `stale_coverage` 0
(witness/bound fix). Remaining misses are `not_built`.

**The open question.** Queries got SLOWER this session (see §3), and the
hypothesis is that maintenance now competes for the box. If routing takes over
the wide windows, scan load falls and both should improve together. That has
never been observed, because coverage has never held at 30 long enough.

**How to check.** With coverage at 30 and stable, run the matrix and compare
`rollup_hits_hybrid_total` before/after:

```bash
python3 bench/local/query_matrix.py
python3 bench/local/render_matrix.py bench/local/matrix3.csv
```

**Done when** wide-window cells complete AND the hit counter moves for them.

---

## 3. Why did query latency regress? (settle the causal claim)

**State.** p1 `87576849` regressed hard against the baseline:

| shape | baseline | after |
|---|---|---|
| throughput 1h | 199 ms | 1,734 ms |
| throughput 3d | 3,297 ms | 8,217 ms |
| log_list 1h | 459 ms | 4,002 ms |

Meanwhile p4 `dcad860a` IMPROVED at narrow windows (1h 1,542 -> 246 ms). And the
throughput pass condition at 14d+30d went **9/12 -> 6/12**.

**The claim, which is NOT yet proven:** the regression is maintenance contention,
because the baseline ran on a box whose coordinator had never started.

**Why it matters.** If it is not contention, something shipped this session made
queries slower and the summary mis-attributes it. That is the single most
important open question here.

**PARTIALLY REFUTED already, 2026-08-24.** p1 and p4 were measured in the SAME
sweep, on the same box, under the same maintenance load:

| project | 1h throughput before | after | direction |
|---|---|---|---|
| p4 `dcad860a` | 1,542 ms | 246 ms | 6x FASTER |
| p1 `87576849` | 199 ms | 1,734 ms | 8.7x SLOWER |

Contention slows everything. It cannot produce opposite directions in one sweep,
so "maintenance load" is NOT a sufficient explanation for p1 and the summary
should not lean on it.

What differs about p1 specifically, and the next hypothesis to test: p1 is the
most fragmented project (588 of 1,339 sealed files under target) and therefore
the one compaction is actively rewriting. A rewritten file is a new object, so
p1's foyer cache is being invalidated continuously while its own partitions are
compacted — cold reads on every sweep. That predicts p1's latency RECOVERS once
its compaction settles, while p4 (already compact) never paid the cost.

Test it by re-running the matrix for p1 alone after its out-of-policy cells reach
zero, and by watching `foyer.hits`/`misses` around a p1 query before and after.

Two busy-arm readings taken minutes apart were 2.43 s and 7.32 s for the same
p1 1h query, so single samples are worthless here — take several, report the
minimum, and note that even the fast one is 12x the 199 ms baseline.

**How to test.** The box hands you a free control: after every restart,
`wait_for_preload` holds the coordinator for 300 s, so `tasks_running = 0`. Probe
p1 inside that window, then again once it reaches 16.

```bash
bench/local/idle_probe.sh     # waits for tasks_running=0, probes, records
```

Compare against the busy arm. A large gap confirms contention; a small one means
look at the shipped diffs — start with `2f7754a` (scan ceiling) and `6a5975a`
(cooperative cancellation), the two that touch the read path.

---

## 4. p1 30-day `log_list` OOM (failure mode B)

**State.**

```
Resources exhausted: unordered merge-on-read dedup exceeded its 2048 MiB per-query budget
```

Only p1; p4 answers the same shape at 30d in 879 ms, so it tracks duplicate
density, not window width.

**Why it was not fixed.** Unordered keep-greatest is blocking by nature: it
cannot emit a key until it has seen every version, so a downstream `LIMIT` cannot
bound it. A bounded top-N IS sound here — `dedup_keys` is `[timestamp, id]` and
an UPDATE re-appends the row's ORIGINAL timestamp, so all versions of a key share
a timestamp, and any key below the n-th best timestamp can be evicted.

**Three things that make it non-trivial**, all verified in-session:

1. `Greatest` retains whole `RecordBatch`es plus winner masks, so evicting a KEY
   frees nothing without also compacting retained batches and redoing the pool
   accounting.
2. Eviction must be **tie-inclusive** — equal timestamps at the boundary, or a
   key loses a version to the cut. `OrderedUnionForTopK` already refuses to push
   a fetch through `DedupExec` for exactly this reason.
3. `DedupExec` has no `fetch`; DataFusion pushes fetch into the Sort, not below
   it, so it needs a new rule matching `SortExec(fetch, on a dedup-key column)`
   above a `DedupExec`.

**Do not rush this.** It is merge-on-read version resolution, with two logged
prod incidents in its history. It wants review, and a test for the
stale-version-across-a-tie case specifically.

**Note it may be masked:** the scan ceiling now refuses that cell at 2.5 s having
selected 460 GB, so mode B does not currently reproduce in prod. It will return
if `TIMEFUSION_WIDE_SCAN_REJECT_MB` is raised.

---

## 5. Compaction: the three fixes DO retire cells — CLOSED 2026-08-24

**State.** Three defects found via the funnel log (`680acac`) and fixed:
smallest-first packing (`8844064`) and the debt policy matching the packer
(`e16f157`). All three are live as of 2026-08-24.

**Answered (`565da6e`): compaction is verified retiring files** — 275 -> 49 on
the flagship cell and -28% fleet-wide, against the 48 cells / ~1,772 small files
baseline below. The checks are kept because they are the right way to re-verify
after any packer change, not because the question is still open.

**How to check.** The funnel log answers directly — if a unit still selects
nothing, it now says which filter stage emptied the list:

```bash
ssh ubuntu@captain.s.past3.tech \
  'docker service logs srv-captain--timefusion --since 15m 2>&1 | grep -a compaction_unit_selected_nothing | tail'
```

Ground truth is object storage, not counters:

```bash
set -a; source .env.prod; set +a
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
# count cells with >=2 files under 256 MB on sealed dates; see the lever-2 page
# for the full snippet. AWS_REGION must be "de", not "auto" (OVH rejects auto).
```

Baseline to beat: **48 cells / ~1,772 small files**, unchanged across 45 minutes
before these fixes.

---

## 6. Known-red tests

`config::tests::tantivy_defaults_are_the_deserialized_ones_not_the_derived_ones`
was red on master; it **passes as of 2026-08-24 afternoon** (whole suite
1117/1117 locally). Consider this one closed.

Replacing it, and more serious because it is intermittent: **E2E
`recent_window_pruning::text_match_conjunct_does_not_poison_parquet_pushdown`**
fails in CI on its SECOND assertion (`DedupExec fell to full-set`; the primary
`input_rows = 0` passes). It failed on `7f15de5`, passed on `fb8dd70`, failed on
`3700730`, and passes locally. It guards a path with two logged prod incidents,
so a guard that flickers is worth a look on its own account — a flaky assert
trains people to ignore a red E2E job.

---

## 7. Two smaller things worth a look

- **`SealedConsolidation` is a DERIVED operation**, so every deploy discards the
  queued units and the planner re-mints them from the file list. With restarts
  every 20-40 min a unit has a narrow window to be claimed. Making these survive
  a restart, or deploying less often, is the cheapest throughput win available.
  Note the design deliberately does NOT persist them — a persisted queue was 20x
  inflated with work already done (2026-08-19) — so this is a trade, not a bug.
- **Interactive `OPTIMIZE` cannot compact a live box.** It sorts the whole
  partition inside the shared 5 GB FAIR query pool, so it gets a small share
  beside dashboard traffic — five attempts saw 2.6 to 264 MB free and failed. One
  small cell did succeed (15 files -> 1). Use the consolidation path instead.

---

## 8. Transplanted from `2026-08-22-per-day-tantivy-index.md` (closed 2026-08-24)

That plan is closed as superseded — the per-day index was never built, and the
three defects that actually explained the divergence were serial-loop starvation,
a cap sized on the wrong population, and an accidental `tantivy` inheritance.
Read it at `git show b6acc2a:docs/plans/2026-08-22-per-day-tantivy-index.md`.
What survives it:

- **Re-measure `tantivy_uncovered_files` against the NEW population before
  quoting any convergence number.** Rollup tiers left `indexed_set()` on
  2026-08-24 (`8698271`), so `indexed_set()` is now the single table
  `otel_logs_and_spans`. Every historical figure in that plan — and in the
  memory files — is against a population 37% larger. The count will collapse for
  bookkeeping reasons; that is not the drain working.
- **A consequence worth knowing before tuning anything:** with one indexed table,
  the 08-23 rotation and `buffer_unordered(3)` work is inert, and so is the
  concern that a slow spans pass starves the others. One table cannot starve
  another. `spawn_cron_job` still skips an overlapping tick for the WHOLE job, so
  a >15-minute spans pass still costs the next tick — but that now only delays
  itself.
- **Reconcile should sweep tables that have manifests but are no longer indexed.**
  Three manual cleanups now: `otel_metrics` manifests (08-23), then on 08-24 the
  six rollup prefixes (84 manifests + 6,398 blobs) and `indexes/otel_metrics/`
  (981 blobs, 3.67 GB) that the first cleanup left behind. Nothing can read them
  and nothing can collect them.
- **The read-side pair that plan deferred**, both still unattributed:
  `search_us_avg` 0.34 -> 2.1 ms and `reader_hit_pct` 96.9% -> 85.2%. Its two
  hypotheses were "working set > cache" and "IO contention from a permanently
  running backfill"; removing 37% of the build population should have moved the
  second, so this is now a cheap re-measure rather than an investigation.

## 9. Transplanted from `2026-08-20-dedup-and-sort-strategy.md` (deleted 2026-08-24)

That plan was self-declared SUPERSEDED (the hot tier it half-addressed no longer
exists) and substantially shipped. Two of its "STILL OPEN" bullets have since
closed on their own — the cert-grant watch (certification was diagnosed healthy
on 08-22; the counter is process-scoped) and resumable dirty-bin staging (shipped
behind `TIMEFUSION_REPAIR_RESUME_ENABLED`). These three did not:

- **§4b, the unordered-input degraded path.** `DedupNeedsOrderedInput` restores
  the SPM when ordering was merely coalesced away; genuinely unordered legs still
  have no policy. Pick one: spillable/hash-partitioned dedup, reject-before-decode
  with an actionable error plus repair scheduling, or a tightly bounded fallback.
  Footer repair reduces the frequency and is not a memory-safety invariant — this
  is the same family as item 4's OOM above.
- **§5, duplication accounting.** Record per profiled query: physical rows, unique
  dedup keys, versions per key, predicate-rejected rows, files and object-store
  requests. The much-quoted "32.6x duplicates" conflates versions at rest with
  tier copies, and nothing can size the dedup problem honestly until they are
  separated.
- **§6, overlap-scoped dedup** — the strategic shape, and explicitly last.
  Compaction first emits a "unique within dedup key" property on its outputs,
  invalidated by any overlapping append or rewrite; plan-time overlap analysis
  over file `[min_ts, max_ts]` then lets proven-unique non-overlapping files
  bypass `DedupExec` entirely, with overlap groups paying it per group. Binary
  certification retires once bypass coverage exceeds it.

---

## The benchmark

`bench/local/` (gitignored) holds `query_matrix.py`, `render_matrix.py`,
`idle_probe.sh` and a README. It reproduces the six-project x seven-window sweep
with the SQL monoscope actually emits, and it is resumable, so a mid-run deploy
costs nothing. Create `bench/local/tfurl` first:

```bash
grep -m1 '^TIMEFUSION_PG_URL=' ../monoscope/.env | cut -d= -f2- > bench/local/tfurl
```
