# Merge-policy prior art, and the similar-size rule TimeFusion lacks

2026-09-06, overnight research block. Question: the packer's write amplification
was measured at ~36 rows written per row ingested (2026-09-04), the value floor
(`timefusion_pack_max_rows_per_file_eliminated`) cut Pack write volume 66% —
what does prior art say the NEXT structural rule is, and what is the 100x lever?

## 1. ClickHouse: SimpleMergeSelector — the size-RATIO heuristic

ClickHouse's merge selector explicitly optimises write amplification, with two
heuristics that map directly onto our measured pathology:

- **Similar-size preference**: merge parts of comparable size first,
  specifically to avoid re-rewriting a large part to absorb small ones — the
  exact shape our 09-04 measurement flagged (2-file merges = 82.5% of packing
  write volume for 9.9% of the file reduction; a big+small pair rewrites the
  big file for one file of benefit).
- **Age escalation**: older parts gain merge priority, bounding how long the
  ratio guard can defer work (our `starved` machinery is the same idea on the
  claim side).
- Tunable via `merge_selector_base` — the exponent of the "reduction curve":
  larger base = bigger merges = LOWER amplification at the cost of latency.

RocksDB's *universal compaction* encodes the same rule as `size_ratio`: a run
joins a merge only if its size is within (1 + size_ratio/100) of the
accumulated candidate — a hard similarity gate, not a preference.

**Transfer**: our value floor prices FAN-IN (rows per file eliminated) but not
SIMILARITY. A 10-file bin of 9 tiny files + 1 huge one passes the floor easily
(good fan-in) while still paying the huge rewrite — the precise shape both
ClickHouse and RocksDB refuse. The missing rule is a per-bin size-ratio guard.

## 2. RocksDB leveled compaction — the contrast that names our gap

Leveled compaction keeps L1+ key-range partitioned so a merge touches only
overlapping ranges. The 09-04 root-cause note ("TimeFusion has L0 and nothing
else") stands: every packer output goes back into the same undifferentiated
pool. A ratio guard is the cheap approximation of levelling — it naturally
stratifies the pool into size generations (tiny→small→target) because only
similar sizes merge, which is why universal compaction behaves like implicit
tiering. Full levelling remains the deep fix; the guard buys its main benefit
(no repeated large-file rewrites) for ~30 lines.

## 3. Deletion vectors — the 100x lever, scoped

The prod table ALREADY carries `delta.enableDeletionVectors: true` (observed in
the run-unit table-properties log 2026-09-05) — readers must handle DVs, but
nothing writes them. Upstream delta-rs: **writing to DV-enabled tables is not
supported** (delta-io/delta-rs#4079 open as of Jan 2026; #1094 is the tracking
issue; the kernel crate reads DVs). Databricks-side Delta supports DVs in
MERGE/UPDATE/DELETE since 3.1.

**Why it is the 100x lever**: dedup currently REWRITES whole files to drop
0.0008% of rows (454.6M rows scanned to drop 3,782, 96.5% of worker time,
2026-09-01). With DV writes, a dedup unit emits a bitmap per file instead of a
rewrite — write amplification for dedup collapses from ~file-size to ~bytes-of-
bitmap. The read path already tolerates DVs by spec (and our fork controls both
sides).

**Scope estimate**: fork-level work in our delta-rs branch — DV serialization
(RoaringBitmap format), the `remove`+`add`-with-DV commit shape, reader-path
verification (kernel already reads), conflict semantics with concurrent
OPTIMIZE. Weeks, not days. Prerequisite: none — the property is already on.
This is the first structural item for the 10x/100x roadmap, ahead of levelled
compaction.

## 4. Academic anchor

Sarkar & Athanassoulis, "Constructing and Analyzing the LSM Compaction Design
Space" (VLDB 2021): compaction policies decompose into trigger, granularity,
data movement, and picking policy — and picking policy (WHICH runs merge) is
the dominant term for write amplification under skewed size distributions.
Our packer had trigger (targets/budgets) and granularity (bins) but until the
value floor had NO picking policy beyond count/bytes; the ratio guard completes
the picking-policy axis. Their taxonomy also names our end state: partial,
similarity-gated compaction ≈ "tiered with size-ratio picking" — the
lowest-write-amplification family short of full levelling.

## Tonight's action

Implement the size-ratio guard in the packer selectors, default-ON behind a
kill switch, gated exactly like the contiguity term: pre-registered primary
(steady-state write amplification in the 400-round `select_tail_bin` test +
replay of real prod units) and guards (live file count, starvation metrics,
sim A/B on the merged prod journal). Ship/no-ship decided in the morning WITH
the midnight-hump drain data, not at 4am.

Sources: [ClickHouse merge docs](https://clickhouse.com/docs/merges) ·
[merge_selector settings](https://clickhouse.com/docs/es/reference/settings/merge-tree-settings/merge-selector) ·
[ClickHouse PR #70645](https://github.com/ClickHouse/ClickHouse/pull/70645) ·
[delta-rs #4079](https://github.com/delta-io/delta-rs/issues/4079) ·
[delta-rs #1094](https://github.com/delta-io/delta-rs/issues/1094) ·
[Delta state of the project](https://delta.io/blog/state-of-the-project-pt2/)

## Outcome (03:20Z) — implemented, measured, and REFUTED-for-now by composition

The guard went in behind `timefusion_pack_max_size_ratio` (shared predicate
`bin_breaks_size_ratio`, wired into BOTH selectors — the floor's one-lane
lesson). The 400-round harness, all four arms:

| arm | amplification | live files |
|---|---:|---:|
| unguarded | 6.73x | 31 |
| floor 1M (shipped default) | **2.06x** | 32 |
| ratio 4 alone | 3.96x | 34 |
| floor + ratio | **WEDGE** at ~round 13 (reads as 0.03x on a corpse) |

Two findings, both kept as pinned tests:

1. **The ratio works but loses to the floor alone** on uniform arrivals — the
   floor's fan-in escape already channels merging into 5+-file bins, which is
   most of what similarity buys there. Real (non-uniform) shapes may differ;
   that evidence would come from the sim A/B, which the wedge makes moot for now.
2. **Composition livelock, the real discovery**: a guard that refuses a
   selected bin WITHOUT resuming the walk manufactures a livelock whenever its
   refused bin is the walker's fixed first choice. The floor has this latent
   hazard today (refusal returns empty); the ratio guard merely makes the
   hazardous shape (`[output, output]` first) reachable. Same family as the
   row-cap livelock (2026-09-03), caught in a fixture instead of prod this time.

**Ships OFF (0).** Unblock path, in order: (a) make bin selection RESUME past a
floor/ratio refusal instead of returning empty — this de-fangs the latent floor
hazard independently of the ratio; (b) re-run the pinned composition arm (its
assertion message says what to flip); (c) then the sim A/B on real shapes
decides the default. The morning gets a small, well-lit decision instead of a
4 am default flip.
