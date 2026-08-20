# Duplicates, merge-on-read, and sorting: what is actually slow and what to do

2026-08-20. Investigated from a prod slow-query report (trace
`9b4b64dad7af955fbad42fa8d43c90da`, project `6297304f`), measured live against
`timefusion.s.past3.tech` on deployed `37c48f2`.

## Prod baseline matrix (2026-08-20 night, deployed 493bb1b)

Full shapes x windows matrix, serial with load controls (raw in scratchpad
bench_baseline/). Targets: 30d<10s, 24h<1s. P1=busiest tenant
(`dcad860a-9a98-4c9e-9e69-20d52dcf90e2`, 24.7k rows/10min), P2=incident
project (`6297304f-89c0-48a9-9b5c-20bcac61f54e`). Only ONE cell of 40
completes at 30d.

Method (so a rerun is comparable): one query at a time; `SELECT 1` control
between every cell (controls 20–211ms, median ~26ms — no load skew);
statement_timeout 12s on 1h/24h, 30s on 7d/30d; TIMEOUT = one attempt, never
retried; 30d skipped when the shape's 7d timed out. Point-lookup shapes are
`SELECT timestamp,id,name,level … LIMIT 100`; counts are
`SELECT count(1) FROM (SELECT id …) t` to dodge the count-pushdown
over-report. Trace ids: P1 `4eef98285d8ce7f931724244dfe07f66`, P2
`8776383c42b77d8043c4196968fafae1`.

| Shape | Proj | 1h | 24h | 7d | 30d |
|---|---|---|---|---|---|
| A trace lookup | P1 | 4,696 | TIMEOUT | TIMEOUT | skip |
| A trace lookup | P2 | 1,323 | TIMEOUT | TIMEOUT | skip |
| B email miss | P1 | 1,050 | 8,808 | 5,180 | 4,467 |
| B email miss | P2 | 203 | TIMEOUT | TIMEOUT | skip |
| C level=ERROR LIMIT 100 | P1 | TIMEOUT | TIMEOUT | TIMEOUT | skip |
| C level=ERROR LIMIT 100 | P2 | 2,574 | TIMEOUT | TIMEOUT | skip |
| D kind=SPAN count | P1 | 5,553 | TIMEOUT | TIMEOUT | skip |
| D kind=SPAN count | P2 | 728 | TIMEOUT | TIMEOUT | skip |
| E plain count | P1 | 1,248 | 12,235 | TIMEOUT | skip |
| E plain count | P2 | 169 | TIMEOUT | TIMEOUT | skip |

**GOAL: zero TIMEOUT cells in this matrix.** Every one of these is a
log-explorer shape a user actually runs; today 21 of 40 cells time out
(12s/30s budgets), including a ONE-HOUR LIMIT-100 query (C/P1/1h). Success
for this plan is re-running this exact matrix and every cell completing —
then tighten toward 24h<1s / 30d<10s.

Plan shape per cell (EXPLAIN, 24h, P1):

| Shape | DedupExec | predicate= on scans | EmptyExec | file groups |
|---|---|---|---|---|
| A trace | bounded[timestamp]/greatest | 0 of 4 | 0 | 427 + 48 + 3 |
| B email | bounded/greatest | 2 (row + stats) | 0 | 427 + 3 |
| C level | bounded/greatest | 0 of 4 | 0 | 427 + 48 + 2 |
| D kind | **full-set** + CoalescePartitions | 0 of 2 | 1 | 427 + 48 |
| E plain | bounded/greatest | 2 | 0 | 427 + 3 |

B's non-monotonic column (24h 8.8s > 7d 5.2s > 30d 4.5s) is the pushdown
working: all B cells return 0 rows, the stats predicate prunes nearly every
row group, so wider windows add pruned files, not scan work. Stats delta over
the run: wide_scan_oversize_total 25→32, scan lat_p999 29.7→31.6s,
cert_granted_total stayed 0.

Attribution (EXPLAIN per shape, 24h): A/C carry the selective filter only in
FilterExec bundled with the text_match hint — zero parquet predicate on any
scan (the P0 in this doc; B is the positive control: its email predicate DOES
push, and it is the only shape whose 30d completes, 4.5s, wider windows
nearly free). D is the only full-set/Coalesce plan and has exactly one
EmptyExec leg (the §4 EmptyExec-veto bug). Every plan pays a 427-file-group
hot-tier leg (fragmentation, the fixed cost). cert_granted_total=0 /
dedup_denied 100% (the §2 certification gap). The night's four fixes map 1:1
onto these four causes.

## The measurement ladder (one project, otel_logs_and_spans)

| query | result |
| --- | --- |
| trace_id point lookup, 2h window | OK, 0 rows — hot leg filtered below dedup; delta legs empty via time/partition pruning (NOT predicate filtering, see P0) |
| count(*), 3h window | 15.7s wall, ~1.5s total compute — 1.32M rows merged → 632K after dedup |
| trace_id point lookup, 6h window | timeout at 55s |
| trace_id point lookup, delta-only 4h window (yesterday) | delta scan emits **2.11M rows unfiltered**, dedup full-set buffers 2.04M, filter above dedup → 0 |
| trace_id point lookup, 24h, SELECT * | **hard error**: `unordered merge-on-read dedup exceeded its 2048 MiB per-query limit` after 9.3s |
| 30d dashboard (08-20 profile, other project) | 7.24M rows scanned → 613 emitted; 32.6x is duplicate versions |

Read this ladder carefully — it separates **three different problems** that look
like one "dedup is slow" symptom:

1. **P0 bug**: the delta leg does not apply pushable predicates (below).
2. **Duplication at rest**: 32.6x on sealed history because the drain is dead.
3. **File-open latency**: 3h costs 15.7s wall on ~1.5s of compute — the gap
   is plausibly object-store round trips (682 hot-tier file groups in one
   plan), but wall-minus-compute also contains footer reads at planning,
   decode-admission waits, provider construction, metadata-cache misses, and
   SPM startup across many streams — and 682 file groups is not itself a
   request count. Record request count/latency, provider-build, footer-fetch
   and admission-wait before calling the attribution settled (Foyer's 96%
   DATA hit rate does not exonerate metadata/footer IO). Either way dedup
   fixes do not touch this; compaction remains the likely remedy.

Also measured: in the 3h window the hot leg (689K rows) and delta leg (629K
rows) each return a **full copy** of the same data — 2x read amplification on
every recent query before any dedup even starts, and a non-empty hot leg is
also precisely what forbids the certification skip (`skip_dedup` is set only in
`scan_delta_only`).

## P0: delta leg ignores partial_filters (incomplete fix, visible on 37c48f2)

`37a87e2`/`0396714` made columns immutable-by-default so point-lookup predicates
are safe below `DedupExec`. That works on the **hot leg** (EXPLAIN shows
`FilterExec: project_id AND context___trace_id AND timestamp` under the hot
`DataSourceExec`). The **delta leg was never covered** (not a regression —
no evidence it ever applied these): the logical `DeltaScan` carries the
predicate in `partial_filters=[...]`, but the physical scan emits every row in
the window — measured 2.11M rows out of a 4h delta-only window for a
`context___trace_id =` equality that matches 0. Reproduced identically with
`context___span_id =` and `status_code =` equalities: 2.11M rows out every
time, filter above dedup → 0.

**DISCRIMINATED (codex, same day): it is the conjunction-conversion failure,
not a general DeltaScanExec/GatedScanExec problem.** The control experiment a
string equality cannot run (tantivy injects `text_match` on every string
equality) works with a numeric one:

- `duration = -1234567` (no text_match injected) → physical parquet
  `predicate=`, `required_guarantees=[duration in (-1234567)]`, 14 row groups
  considered / 0 matched, `bytes_scanned=0`, `DeltaScanExec output_rows=0`.
  **Non-partition pushdown works.**
- `context___trace_id = <nonexistent>` (gains `text_match`) → NO `predicate=`
  on the scan, `pushdown_rows_matched=0 / pruned=0`, every window row emitted,
  only the final FilterExec rejects.

Leading diagnosis at the time — conjunction-conversion failure — was
**REFINED 2026-08-20 evening after local reproduction**: the dominant
mechanism is upstream of conversion. The tantivy split path
(`scan_delta_table`'s file-selection branch) builds its provider WITHOUT
`.with_pushdown_with_deletion_vectors(true)` — only the cached-provider path
(mod.rs:8020/8022) sets it. On a DeletionVectors-feature table (all prod
tables) that makes `parquet_pushdown_enabled=false`, so `process_filters`
produces **predicate=None**: no conjunct is pushed at all, convertible or
not. This is why only text_match-bearing queries lose pushdown — they are the
only ones routed through the tantivy split. Verified by a debug probe:
`get_read_plan` received `predicate=None` on the failing leg. Fix: one line —
add the DV opt-in to the file-selection builder (actual DV-bearing FILES still
disable the predicate per-file inside the fork, so correctness is unchanged).

The conjunction-split fork fix remains as defense-in-depth: once a predicate
exists, one non-bindable conjunct must not discard the rest (unit-tested in
the fork: `test_predicate_pushdown_keeps_bindable_conjuncts`, OR subtrees stay
atomic).

Corollary: the hot/delta range-exclusion conjuncts DO appear in the physical
numeric plan (`timestamp outside hot range OR updated_at > gate`), so
exclusion is not dropped wholesale — but any query whose filter set includes a
non-convertible conjunct loses them along with everything else, and even when
present the OR-weakened form may not prune at parquet level. Investigate the
residual overlap, don't rebuild exclusion.

Consequence: any point lookup whose window reaches Delta decodes the entire
window, and with `mode=full-set` (see below) buffers it → the 2 GiB abort.
Bloom filters and the tantivy prefilter are both defeated because rows are
materialized before the predicate runs.

Fix (bug-fix workflow: failing test first): the control experiment ruled out
`DeltaScanExec`/`GatedScanExec` stripping predicates wholesale — go directly
to the conjunction-conversion site. Regression guard: string equality AND
`text_match(...)` must still yield a physical parquet predicate containing the
equality and timestamp bounds, and delta-leg `output_rows=0` for a nonexistent
value.

## Why full-set instead of bounded (the sorting half)

`DedupExec` streams (bounded[timestamp], O(one timestamp's keys) memory) when
a `SortPreservingMergeExec` sits above the union and each leg declares
`output_ordering`. The failing prod plan has `CoalescePartitionsExec` and
`mode=full-set`.

**STALE-CLAIM CORRECTION (third review, verified against the pinned fork
rev 6472090):** "one footer-less file voids ordering for the whole partition"
describes replaced code. The fork already: picks the ordering supported by the
largest file set, isolates non-conforming files into an **unordered sibling
union** (`next/scan/mod.rs:801-868`), derives footer min/max stats, regroups
overlapping files via `split_groups_by_statistics_with_target_partitions`
(`:1032`), and caps ordered groups at `MAX_ORDERED_FILE_GROUPS = 512`
(`:1012`) with fallback when grouping degenerates.

So the open question is DIAGNOSIS: why did the observed plan still lose
ordering? Candidates: the unordered sibling existed and something above merged
it; regrouping exceeded the 512 cap (682 hot file groups in the same plan!);
sort-column stats unavailable; a later optimizer pass replaced the SPM with
Coalesce (the `DedupNeedsOrderedInput` re-insertion may not cover this shape);
or the deployed rev differs. This is diagnosis work, not implementation work.

Still true and worth keeping: bounded dedup does **not** require time-disjoint
files — per-stream ordering + a merge suffices; overlapping files are just
extra merge streams. And the 08-02 memory's "no global ordering can exist"
over-claimed accordingly.

Hard constraints (do not relitigate):
- No `version_append: false` — tombstones only exist via DedupExec; disabling
  resurrects deleted rows.
- No read-time `SortExec` on the delta leg — OOMed the pool twice (08-02,
  08-07), both reverted.
- Caching is fine (Foyer 96%+ hit rates) — untouched.

## Duplication: three classes, three fixes

| class | source | fix |
| --- | --- | --- |
| version churn | UPDATE = append (`dml.rs:939`); monoscope hash enrichment | bounded upstream by ingest-stamping (steady-state ~1 version/row); physical removal = the dedup drain |
| replay/abandoned-commit copies | WAL cursor behind, watchdog-dropped commits that land anyway — all deliberate "duplicate, never lose" | drain + certification |
| different-id retries | monoscope acks before TF write; ids random per attempt | **no TF-side dedup can fix this** — deterministic ids upstream (sketched in 2026-07-27-long-term-architecture.md) |

State of the machinery: the drain (`dirty_bin_processed_total`) has been **0
since 08-16** (the rollup `continue` retired it; re-enable attempt `954d516`
wedged the tier and was reverted `d5688fd`). Certification fires on 0.2–0.5% of
Delta-reading scans; currently 0% skip (370 eligible / 0 skipped).

(The original ordered plan that stood here was superseded by the review — see
the checklist below, the single source of truth.)

## Review corrections (2026-08-20, post-review)

A code review against the plan found the roadmap mixed confirmed problems,
hypotheses, and already-implemented work. Verified against source:

- **Coordinator day-wide certification EXISTS** (`maintain.rs` ~1070:
  `covers_day` → `record_certification` after a clean day-wide unit, unmoved
  file set) and is in the deployed build — yet prod shows
  `cert_granted_total=0`. The task is *why coverage is zero* (units splitting
  via `maintenance_dedup_task_split`? OOM retries — see the live
  `retry_reason`? fp churn from packing? units never day-wide?), not
  implementation.
- **Hot/Delta range exclusion EXISTS** (`mod.rs:9731`: Delta excludes merged
  mem∪hot ranges, weakened `OR stamp > gate` for MOR). The measured 2x
  double-read means the exclusion was ineffective at scan time. **Unifying
  hypothesis**: the exclusion conjuncts are appended to `delta_filters` and
  ride the same partial_filters path the P0 shows being dropped — one root
  cause may explain both the unfiltered point lookup and the double-read.
  Verify before building anything new here.
- **The P0 evidence proves the physical delta scan emitted the full window**,
  not yet *where* the predicate is lost (ProjectRoutingTable handoff, provider
  construction, conjunction conversion, or parquet execution). Parquet
  pushdown/pruning/page-index/reorder are all enabled (`mod.rs:4373`), even
  under deletion vectors (`mod.rs:8015`). Run the discriminating matrix below
  before sizing the fix.
- **The 32.6x figure conflates two things**: DedupExec in/out (7.23M→221.7K)
  counts at-rest version duplicates AND hot/delta tier copies together. Proper
  accounting must record separately: physical rows, unique dedup keys,
  versions/key, predicate-rejected rows, tier-overlap rows, files and
  object-store requests.
- **"Full-set unreachable" is not achievable operationally** — crashes, old
  files, fallback writes reintroduce unordered input. A degraded-path policy
  is required, not just footer repair (which reduces frequency, not
  reachability).

## Checklist (reordered per review — diagnose before build)

### 1. Fix conjunction conversion — DIAGNOSED (first)
Codex's control experiment settled the matrix: numeric equality (no
text_match) pushes down fully (`predicate=`, `required_guarantees`,
`bytes_scanned=0`); string equality + injected `text_match` loses the ENTIRE
conjunction (no `predicate=`, `pushdown_rows_matched/pruned=0`). The bug is
whole-conjunction failure on one non-convertible conjunct.
- [ ] Regression test: string equality AND `text_match(...)` → assert the
      physical parquet predicate still contains the equality and timestamp
      bounds, and `DeltaScanExec output_rows=0` for a nonexistent value.
- [ ] Fix IN THE FORK (primary): split top-level AND terms only, preserve OR
      subtrees atomically, independently select parquet-convertible terms,
      keep the complete original expression above the scan. Conversion sites:
      `process_predicate` fallback stuffs non-convertible exprs into the
      parquet predicate (`next/scan/plan.rs:715-721`); one failed bind of the
      combined expr discards everything (`next/scan/mod.rs:734-770`); and
      `gather_filters_for_pushdown` marks ALL parent filters unsupported on
      one Err (`next/scan/exec.rs:493-497`). Unit tests live in the fork;
      cover projection-excluded predicate columns, column mapping,
      Utf8/Utf8View coercion, and actual DV files.
- [ ] TF-side e2e regression proves the dependency stays wired (extend
      `tests/e2e/recent_window_pruning.rs`: string equality AND
      `text_match(...)` → `pushdown_rows_pruned > 0`, scan output_rows ≈ 0
      for a nonexistent value). Optional fast mitigation while the fork bump
      lands: strip text_match-bearing conjuncts at the `provider.scan`
      handoff (`src/database/mod.rs:8164`) — sound because pushdown is
      Inexact and text_match is served by the tantivy prefilter.
- [ ] Deletion-vector matrix (CORRECTED: DV-bearing files disable the parquet
      predicate ON PURPOSE — filtering before the row-position mask shifts
      ordinals and resurrects deleted rows; fork `next/scan/mod.rs:733-734`).
      Cells: (a) DV feature on, selected files carry no DV → pushdown allowed;
      (b) files carry DV masks → predicate correctly absent; (c) evaluate a
      post-DV / pre-DedupExec residual filter so DV windows stop feeding the
      whole window into dedup — without it, conjunction splitting alone leaves
      DV files degraded. Note `has_selection_vectors` is `files.iter().any()`:
      ONE DV file kills the predicate for the whole set — the same
      all-or-nothing genus as the conjunction bug; consider per-file scoping.
- [ ] Post-deploy verify: 24h `SELECT *` trace_id lookup completes; string
      point lookups show `predicate=` on the delta scan.
- [ ] Then re-profile the 2x hot/delta overlap BY ADMISSION REASON before
      attributing it to conjunction loss: rows admitted because outside the
      hot range vs specifically by `updated_at > gate` vs exact duplicates of
      hot rows vs genuine newer Delta versions. Distinguishes a bug from an
      overly conservative gate derivation from expected MOR behavior.

### 1c. Hot tier must absorb updates instead of forfeiting coverage (NEW)
Diagnosed from the 2x hot/delta double-read + the enrichment pattern (every
row updated ~twice). Two compounding leaks in `hot_tier.rs`:
- `version_gate` = the demoted file's `max_stamp`; enrichment versions are
  always stamped later, so `OR stamp > gate` re-admits ~2 Delta versions per
  row on every query over the window, forever.
- A late flush re-demotes the bucket as a SECOND file, and `read_leg`'s
  covers_window rule ("exactly one file per bucket") then retracts the whole
  window — it falls through to Delta wholesale (`unproven_windows_total=863`).
  Updates arriving VOIDS coverage instead of EXTENDING it.
Fix shapes (compose; A first):
- [ ] **A. Stacked base+delta files**: base file (complete at stamp <= s1) +
      late-drain file (all rows with stamp in (s1, s2]) = coverage stands,
      gate advances to s2. Read leg serves the stack, DedupExec collapses.
      Metadata/read-leg change only.
- [ ] **B. Merge-on-demote**: late version for a demoted bucket rewrites the
      bucket's .arrow (old + new, keep-greatest, one file, advanced
      max_stamp, covers_window=true; write-new+rename keeps mmap safety).
      Local-disk cost only; the hot-tier analog of dedup-as-you-compact.
Success criterion: recent windows (tier depth ~20h at the 600GB cap) read
ZERO Delta rows for fully-enriched buckets — the only path to 24h<1s/300ms,
since even perfectly-pruned R2 reads pay per-file round trips.
Precondition check before building: post-deploy matrix + §1b admission-reason
profile to confirm how much of the residual amplification this owns.

### 2. Why is deployed certification coverage zero? — DIAGNOSED 2026-08-20
**Cause: certification requires a day-wide dedup unit, and prod never produces
one that survives.** Evidence (12h window): 545/560 dedup unit starts are
narrower than a day (294 under 10min; otel_logs_and_spans tops out at exactly
6h because `coarsen_to_width` refuses to fuse past MAX_DECODED_BYTES=256MB);
otel_logs_and_spans — the table with 3441/3495 denials — had ZERO day-wide
starts; the only day-wide units (3, all otel_metrics) died at the 300s Dedup
deadline every time (55 timeout events vs 4 OOMs — deadline dominates 14:1).
Eliminated: bookkeeping failures (0), fp churn (2 ever), fresh splits (0
events — narrowing is historical journal state).

Two structural gaps: (a) a day whose dedup cell is journal-Complete never gets
another dedup unit — days exit the certifiable set permanently as the queue
drains; (b) a day-wide pass that drops >0 rows completes UNCERTIFIED and is
never re-probed.

**Fix direction (not yet implemented): decouple certification from unit
shape** — accumulate per-(project,table,date) clean-slice coverage from the
narrow units that already complete, certify when the union of clean slices
covers the day over an unmoved fingerprint; companion re-probe pass for
certifiable-but-Complete days. Riskier alternative: hash-shard-aware
`split_time_task` + a probe-only mode exempt from the 300s rewrite deadline
(the deadline comment itself names the former; raising the deadline was tried
2026-08-18 and reverted same day after an OOM).

Original diagnosis checklist (retained):
- [ ] `run_coordinator_dedup_once` already certifies clean day-wide units
      (maintain.rs ~1070). Find which precondition fails in prod: units
      splitting (`maintenance_dedup_task_split`), OOM retries (live
      `retry_reason` shows an external-sort exhaustion), `covers_day=false`
      shapes, fp churn from concurrent packing, or persistence off
      (`timefusion_dedup_certification_persist`).
- [ ] Watch `cert_granted_total` / `dedup_skipped_pct` move after the fix.

### 3. Duplicate removal at rest — RESOLVED 2026-08-20 night (both directions)
Two independent results, reconciled:
- `feat/dedup-as-you-compact` @ ca4983c: sealed consolidation ALREADY collapses
  versions while merging (SortByDedup, since e69de6a); the flag
  `timefusion_compact_dedup_merge` (default OFF) extends it to `compact_date`.
- `work/dedup-parallel` df30961 proves the structural limit: two versions of a
  key stranded in separate CONVERGED target-sized sorted runs are never
  re-selected together, so compaction-time dedup can never certify
  convergence. It is a space/read-amplification optimization; **authoritative
  physical collapse stays with the dedup engine**, whose input scope contains
  every version of a rewritten key.
Consequence: the drain (or slice-covered coordinator dedup units, see §2) is
NOT retirable. Certification comes from the dedup engine; compaction reduces
how much it has to chew. The original retirement criterion is refuted.

Original framing (superseded):
- [ ] Test whether compaction can be the primary drain: collapse versions
      keep-greatest while merging overlap groups; outputs become unique-within
      by construction; certification follows clean compaction.
- [ ] Measure convergence on a fragmented prod-shaped partition (sim /
      run-unit ladder first — no deploy-per-hypothesis).
- [ ] Only if convergence is insufficient: resumable dirty-bin staging with
      its own budget (the `954d516` wedge cause — admission deadline vs 3600s
      stage deadline are the same dial at opposite ends). Do not build both
      engines up front.

### 4. Ordering loss — DIAGNOSED 2026-08-20: an EmptyExec leg vetoes the merge
**Cause (prod-verified with a discriminating pair):** TF's tantivy split
(`scan_delta_with_tantivy`) yields two delta legs when the snapshot has raw
index debt; a leg whose file selection prunes to zero comes back as a bare
`EmptyExec`, which declares no output ordering. Delta legs are unsortable by
design, so `ordered_children` bails, `merge_req` stays None, no SPM is built,
and DedupExec falls to full-set over a CoalescePartitionsExec. Verified:
`level='INFO'` 24h (delta legs non-empty) plans bounded/SPM; `level='ERROR'`
and the trace_id incident shape (one EmptyExec leg) plan full-set/Coalesce —
even when the sibling delta leg DECLARES `output_ordering=[timestamp DESC]`.
Rejected: 512-group cap (the 682 groups are the TF hot tier, which declares
ordering), missing stats, DedupNeedsOrderedInput gap, mem/hot undeclared.

**Severity inversion: the better tantivy prunes, the worse the plan** — the
most selective point lookups are exactly the ones that lose bounded mode and
inherit the 2 GiB full-set ceiling.

**Fix: drop provably-empty legs before building the union** (walk the
single-child chain to EmptyExec) in `wrap_result` / `scan_via_provider`
(`src/database/mod.rs`), keeping one leg if all are empty. An empty leg
contributes nothing; removing it fixes detect_bound, top-K, and statistics at
once.

### 4b. Unordered-input degraded path (before making unordered rare)
- [ ] `DedupNeedsOrderedInput` already restores the SPM when ordering was
      coalesced away (optimizers.rs:3018); the remaining case is genuinely
      unordered legs. Pick an explicit policy: spillable/hash-partitioned
      dedup, reject-before-decode with an actionable error + repair
      scheduling, or a tightly bounded fallback. Footer repair reduces
      frequency; it is not a memory-safety invariant.
- [ ] Footer coverage as an operational goal: drive footer-repair until no
      live partition holds a footer-less file; keep
      `flush_sort_unsorted_fallbacks_total > 0` paging.
- [ ] Diagnose why prod plans still lose ordering DESPITE the fork's
      isolation/regrouping (see corrected sorting section): unordered sibling
      merged above? 512-group cap exceeded? stats-backed prefix empty? SPM
      replaced by a later pass? deployed rev mismatch?

### 5. Duplication accounting (instrument before quoting ratios)
- [ ] Record separately per profiled query: physical rows, unique dedup keys,
      versions per key, predicate-rejected rows, hot/delta tier-overlap rows,
      files + object-store requests. (The 32.6x figure conflates versions at
      rest with tier copies.)

### 6. Overlap-scoped dedup — the strategic shape (last)
- [ ] FIRST: make compaction emit an explicit "unique within dedup key"
      property on its outputs, invalidated by any append/rewrite that can
      overlap the file. Overlap analysis then consumes that PROOF rather than
      inferring uniqueness indirectly.
- [ ] Bypass requires ALL of: file unique-within; no tombstone or unresolved
      version chain inside it; no mem/hot leg overlapping its key/time
      domain; marker still valid.
- [ ] Plan-time overlap analysis over file `[min_ts, max_ts]` including
      mem/hot leg bucket ranges; non-overlapping proven-unique files bypass
      DedupExec, overlap groups pay it per-group.
- [ ] Retire binary certification once bypass coverage exceeds it.

### 7. Upstream (monoscope) — independent of all the above
- [ ] Deterministic ids per event so client retries stop minting different-id
      duplicates (`ON CONFLICT DO NOTHING` on the PG leg; TF-side dedup can
      never fix this class).

### Not doing (settled, don't relitigate)
- `version_append: false` — resurrects tombstoned rows.
- Read-time SortExec on the delta leg — OOMed twice, reverted.
- Caching changes — Foyer hit rates are 96%+; not the problem.

References: InfluxDB 3.0 querier dedups only overlapped files
(https://www.influxdata.com/blog/influxdb-3-0-system-architecture/,
https://docs.influxdata.com/influxdb3/clustered/reference/internals/storage-engine/);
compactor merges into non-overlapping files continuously
(https://www.influxdata.com/blog/compactor-hidden-engine-database-performance/).
