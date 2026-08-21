# Hot-leg pruning: per-file statistics so the Arrow tier can skip files

2026-08-21. Successor to `2026-08-20-dedup-and-sort-strategy.md` — that plan's
fixes are deployed (image `5559ac8`); this is the next constraint the
per-project profile isolated.

## 1. Problem statement, with the measurement that isolates it

Per-project profile (2026-08-21 ~11:00 UTC, 100 cells, raw in scratchpad
`bench_per_project/`):

- **Shape A (trace_id lookup) TIMEOUTs at 24h on ALL five projects** — the
  only shape that fails universally — despite returning a handful of rows and
  despite the delta leg now pruning to 4-5 file groups.
- Mid-volume projects meet targets on other shapes (P2 kind-count:
  733ms / 1.0s / 1.1s / 1.1s across 1h/24h/7d/30d) — so the engine is capable;
  the failure is specific.
- 1h windows work everywhere (1-4s); 24h windows die. The hot tier is ~20h
  deep at the 600 GB cap. **A 24h window sweeps the entire tier.**

Mechanism: the hot leg is uncompressed Arrow IPC served via DataFusion's
`ArrowSource` (`hot_tier.rs::leg_exec`, ~:800-840). Arrow IPC files carry
**no per-file column statistics** — no min/max, no blooms, nothing a
predicate can prune on. `buckets_in_range` prunes by TIMESTAMP range only
(the `[min_ts, end_ts)` in the filename). Every other predicate — trace_id,
id, email, session, level — is evaluated only AFTER decode, by the
`FilterExec` that `compile_filter_conjunction` builds above the scan. So a
point lookup over 24h mmaps and filters ~567 file groups (~600 GB of page
cache churn + row-wise filter CPU) to emit 5 rows.

This is the exact mirror of the parquet bug fixed yesterday: the delta leg
lost its pruning to a predicate-conversion failure; the hot leg never had
pruning to lose. Same query shape, same two-orders-of-magnitude waste, one
tier over.

Why fixing it is the only path to the targets: with ~200-400ms/file opens on
R2, delta can never serve 24h<1s — the hot tier is the ONLY tier fast enough,
and today it is the bottleneck precisely on the windows it exists to serve.

## 2. Current write/read anatomy (what we can hook)

Write (`hot_tier.rs::write_bucket`, ~:558-603): a sealed MemBuffer bucket's
batches are sorted (`sort_partition`, by declared sorting_columns), then
written as ONE Arrow IPC file via `write_atomic_with` (tmp + fsync + rename).
The filename already encodes derived metadata:
`{bucket_id}_{min_ts}_{end_ts}_{seq}{s}{c}_{max_stamp}.arrow` — sort marker,
covers marker, version stamp — recovered by `rescan()` on boot without
opening files. **The batches are in memory at write time** — computing any
per-column statistic here is a pass over data we are already holding, off the
query path, inside demotion's own spawn_blocking permit.

Read (`scan` → `buckets_in_range` → `plan_leg` → `leg_exec`): metas filtered
by timestamp overlap; `plan_leg` decides served files/coverage/gate;
`leg_exec` builds `FileScanConfigBuilder` + `ArrowSource` with projection and
puts the compiled filter conjunction above it. **The hook point for pruning
is between `buckets_in_range` and `plan_leg`**: drop files that provably
cannot match the query's equality predicates, BEFORE they enter `served`.

Merge-on-demote (shipped 2026-08-20) rewrites stacks into one file — any
sidecar must be (re)generated there too, which falls out naturally if
generation lives inside `write_bucket`.

## 3. Design

### 3.1 What statistic for which column

One size does not fit all; pick per column class, computed from the declared
schema at demote time:

| column class | examples | statistic | why |
|---|---|---|---|
| high-cardinality point-lookup keys | `id`, `context___trace_id`, `context___span_id`, `attributes___session___id`, `attributes___user___email`, `attributes___user___id` | **split-block bloom filter** (same construction parquet uses; `fastbloom` or hand-rolled SBBF, ~1% FPP) | equality-only predicates; min/max useless on random hex |
| low-cardinality enums | `level`, `kind`, `status_code`, `resource___service___name` | **distinct-value set** (dedup at write; cap at 64 values, overflow = "no claim") | exact membership beats blooms at this cardinality; also serves IN-lists |
| already covered | `timestamp` | filename `[min_ts, end_ts)` (exists) | — |

The column list is NOT hardcoded: it derives from the table's YAML schema —
blooms for every `Utf8` field named in a new optional `prune_columns` schema
key, defaulting to the dedup keys + the tantivy-indexed text fields that are
plain identifiers. First iteration MAY hardcode the otel set behind that
default to avoid a schema migration; the YAML key is the end state.

### 3.2 Where the statistics live: one sidecar per .arrow file

`{same_basename}.prune` beside the .arrow file. Format: bincode of

```rust
struct PruneSidecar {
  version: u8,                       // format evolution; unknown version = no claim
  rows: u64,
  blooms: Vec<(String, Sbbf)>,       // column name → bloom bytes
  enums: Vec<(String, Vec<String>)>, // column name → distinct values (≤64)
}
```

Why a sidecar and not the filename (too big), not an Arrow custom-metadata
footer block (rewriting the IPC footer breaks the existing
`footer_is_readable`/zero-copy assumptions and the write path's single
streaming pass), and not a global index (per-file lifecycle must atomically
follow the file through GC/merge/retire — pairing by basename makes that
free).

Lifecycle invariants (mirror the tier's own):
- **Best-effort, absent = serve.** A missing/torn/unreadable/old-version
  sidecar means "no claim": the file is served exactly as today. A sidecar
  can only REMOVE work, never rows. (Same posture as `covers_window`.)
- **Written BEFORE the .arrow rename.** Sidecar first, then the data file's
  atomic rename — so a visible .arrow either has its sidecar or (crash
  window) lacks it and serves unpruned. Never the reverse order: a sidecar
  without its file is garbage, cleaned by the same GC sweep.
- **Immutable**, like the data files. Merge-on-demote writes a NEW pair and
  retires the old pair together (`retired` list gains the sidecar path).
- **GC unlinks pairs**: `unlink()` extends to `path.with_extension("prune")`.
- `rescan()` ignores sidecars entirely (metas still come from filenames);
  sidecars are opened lazily at scan time and **memoized** like
  `validated` (a `DashMap<PathBuf, Arc<Option<PruneSidecar>>>` — decoded once
  per file per process; they are small, ~1-4 KB/file, ~2-8 MB resident for
  the whole tier).

### 3.3 Read-side pruning

In `scan()` (or a helper it calls between `buckets_in_range` and `plan_leg`):

1. Extract prunable predicates from `filters`: top-level conjuncts of the
   forms `col = literal`, `col IN (literals)`, and `text_match(col, literal)`
   where the literal is a plain term (the tantivy hint mirrors an equality —
   treat it as one; a multi-term/fuzzy query is not prunable, skip). Reuse
   the conjunct-walking approach of `compile_filter_conjunction` /
   `collect_text_match_tree` — do NOT invent a third expression walker if one
   can be shared.
2. For each candidate file: load (memoized) sidecar. For every prunable
   predicate on a column the sidecar covers: bloom says absent, or enum set
   excludes the value → **the file cannot contribute → drop it**. Any
   predicate on an uncovered column contributes nothing (conservative).
3. CRITICAL coverage interaction: a pruned file must NOT break `plan_leg`'s
   whole-bucket accounting the wrong way. Dropping a file from `served`
   while it still counts in `files_per_bucket` would retract the bucket's
   range claim and send the whole window to Delta — WORSE than not pruning.
   Two sound options:
   - (a) prune AFTER `plan_leg`: remove pruned files from the exec's file
     groups but KEEP their contribution to ranges/gate. Sound because a
     pruned file provably contributes zero rows to THIS query, so serving
     the window without it loses nothing, and the exclusion claim is about
     rows Delta would otherwise supply — which the predicate equally rules
     out there. **This is the chosen shape**: `plan_leg` unchanged,
     `leg_exec` receives a `pruned: HashSet<usize>` and skips those files
     when building groups. If ALL served files prune away, the leg still
     claims its ranges with an empty exec — mirror of the delta-leg
     "excluded outright" case that made shape D fast. (Note: an
     empty-but-claiming hot leg must NOT be dropped by wrap_result's
     provably_empty filter when it carries ranges — represent as an
     EmptyExec + keep ranges on the HotLeg, which wrap_result already
     handles since ranges travel separately from the plan.)
   - (b) prune before plan_leg and patch the accounting — rejected: touches
     the soundness-critical whole() logic for no additional win.
4. Metrics: `hot_tier.prune_files_skipped_total`,
   `prune_files_considered_total`, `prune_sidecar_missing_total`,
   `prune_bloom_fp_detected` (optional: a pruned-in file whose FilterExec
   emitted 0 rows is a bloom FP or a stats miss — cheap signal via existing
   operator metrics, skip if not trivial).

### 3.4 Backfill for existing files

The tier cycles in ~20h at the disk cap, so backfill is OPTIONAL: full
coverage arrives organically within a day. Still, a cheap accelerator:
`gc()`-adjacent idle task that walks files lacking sidecars (oldest-last),
decodes (streaming, one batch at a time), computes, writes sidecar. Budget:
N files per tick. If this is more than ~40 lines, skip it — one day of
organic coverage is acceptable. Decision gate at implementation time.

### 3.5 Alternative considered: extend the tantivy prefilter to the hot leg

The delta leg already narrows by tantivy hits (`id IN (hits)` + file
selection). Extending coverage to hot files would give text-search pruning
too. Rejected as the PRIMARY mechanism because: tantivy indexing lags the
hot tail by design (uncovered files fall back), the index doesn't cover all
prune columns (email/session), and it couples read-path availability to the
indexing service's health. It composes later: tantivy row-selections for hot
files would slot into the same `leg_exec` hook. Not in this iteration.

## 4. Invariants that must survive (checklist for review)

- A sidecar can only remove FILES the predicate already excludes — never
  affect rows of files that are served. FPP=false-positive means serve (pay
  decode), never false-negative (lose rows). Bloom parameters sized so FPP
  ~1% at the largest bucket row counts (~1-2M rows: ~1.2 MB bloom at 1%;
  cap bloom bytes at 2 MB/column, above that write "no claim").
- Absent/torn/old-version sidecar = today's behavior, bit for bit.
- Coverage claims (`ranges`, `version_gate`) are computed EXACTLY as today
  (`plan_leg` untouched); pruning only shrinks the physical file list.
- Pair lifecycle: no path may unlink a .arrow without its .prune (GC, merge
  retirement, disabled-tier cleanup in `finish_open`).
- Demotion stays best-effort: sidecar computation failure = log + write the
  .arrow without it (never fail the demote).
- No new process-global state; memoization keyed by path like `validated`.

## 5. Test plan (failing-first, per repo workflow)

1. **Unit — pruning decision**: files with sidecars (bloom containing "a",
   enum {INFO}), query `id='b'` → file dropped from exec groups while
   `ranges`/`version_gate` unchanged; query `id='a'` → served; no sidecar →
   served. Pure `plan`-level test beside the `plan_leg` tests.
2. **Unit — lifecycle**: demote writes pair; GC unlinks pair; merge-on-demote
   retires old pair, new pair present; disabled-tier sweep removes both.
3. **Integration (suite)**: HotTier end-to-end — demote buckets with known
   ids, query for absent id over the full window, assert
   `prune_files_skipped_total == files` and result correctness; query for a
   present id, assert exactly the holding file survives pruning and rows
   return.
4. **E2E regression (the money test)**: extend
   `recent_window_pruning.rs` — flush + demote several buckets, point lookup
   by trace_id on a value in exactly one bucket: assert hot-leg
   DataSourceExec file count == 1 (or skipped == N-1 via stats), rows
   correct. Red on current code (all files served), green with sidecars.
5. **Property (cheap, if fastbloom lacks one)**: bloom membership never
   false-negative over random insert/query sets.

## 6. Rollout

- Flag: `timefusion_hot_tier_prune` default ON (kill switch; OFF = never
  read sidecars; writes continue so coverage accrues for re-enable).
- Deploy; within ~20h the tier is fully sidecar-covered organically.
- Verify: shape-A cells in the profile matrix (`bench_per_project/
  run_bench.sh`) — success = **A completes at 24h on every project**, target
  <1s for the point lookup itself; watch `prune_files_skipped_total` /
  `considered` ratio (expect >0.95 for point lookups).
- Risk watch: demotion latency (sidecar adds a hash pass per row over 2-6
  prune columns — expect single-digit % of the existing sort cost; measure
  via existing demote timing) and RSS (memoized sidecars ~MBs).

## 7. Expected impact, quantified against the profile

Shape A at 24h today: sweep ~567 files. With blooms at 1% FPP: open ~1 true
file + ~5 false positives → **~6 files** → decode cost drops ~100x; the
remaining wall is DedupExec over the survivors + plan overhead, comfortably
sub-second (the 1h cells, which sweep ~30 files today, run 1-4s dominated by
exactly this decode). B (email) and session lookups inherit the same win.
C (level='ERROR') gains from enum sets only when a bucket contains no errors
— common for healthy services, so real but smaller. D/E (counts) gain
nothing here — their lever is certification (dedup skip) + count pushdown,
already in flight.

## 8. Out of scope (explicitly)

- Parquet-ifying the hot tier (loses zero-copy page-cache property).
- Tantivy-for-hot (composes later, §3.5).
- Cross-file zone maps / global indexes.
- Delta-leg changes of any kind.
