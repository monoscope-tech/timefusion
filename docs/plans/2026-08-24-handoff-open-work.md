# Handoff: what is still open on 14d/30d, with the context to continue

2026-08-24. Written so someone who was not in the session can pick any item up
cold. Each item states what is known, what is NOT known, the exact commands, and
the trap that will mislead you if you skip them.

Background pages, in reading order:

- `2026-08-22-seven-window-six-project-matrix.md` — the baseline, six projects x
  seven windows, monoscope's real SQL
- `2026-08-22-make-14d-30d-complete.md` — the four failure modes and the Tier-1
  checklist
- `2026-08-23-nothing-is-being-built.md` — the coordinator was disabled on every
  container
- `2026-08-23-the-queue-is-24x-inflated.md` — coarsening; lever 1
- `2026-08-23-lever-2-compaction-throughput.md` — compaction; lever 2

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

## 5. Compaction: confirm the three fixes actually retire cells

**State.** Three defects found via the funnel log (`680acac`) and fixed:
smallest-first packing (`8844064`) and the debt policy matching the packer
(`e16f157`). All three are live as of 2026-08-24.

**Not known.** Whether cells now retire. Every prior check was taken minutes
after a deploy, before consolidation had run.

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

## 6. Known-red test, not ours

`config::tests::tantivy_defaults_are_the_deserialized_ones_not_the_derived_ones`
fails on master. Verified by stashing that it fails WITHOUT any of this session's
changes — it belongs to the concurrent tantivy work.

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

## The benchmark

`bench/local/` (gitignored) holds `query_matrix.py`, `render_matrix.py`,
`idle_probe.sh` and a README. It reproduces the six-project x seven-window sweep
with the SQL monoscope actually emits, and it is resumable, so a mid-run deploy
costs nothing. Create `bench/local/tfurl` first:

```bash
grep -m1 '^TIMEFUSION_PG_URL=' ../monoscope/.env | cut -d= -f2- > bench/local/tfurl
```
