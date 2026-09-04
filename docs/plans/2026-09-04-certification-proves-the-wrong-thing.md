# Certification proves the wrong thing

Prior art check on the certification subsystem, prompted by tonight's finding
that `cert_granted_total` sits at 0 and the whole customer-facing `hashes`
timeout chain hangs off it. The question: how do the systems that solved this
already make "you may skip deduplication" a durable property?

## What TimeFusion does

`record_certification` (`src/database/maintain.rs:4984`) grants only when

```
dropped == 0 && complete && !post.is_empty() && fp(pre) == fp(post)
```

i.e. a pass over the **whole partition** found **no duplicates** and **nothing
committed while it ran**. The proof is anchored to `partition_file_fp` — a hash
of the partition's **file list** — so any subsequent write voids it.

Two consequences, both measured tonight:

- A pass that *removes* duplicates can never certify (`dropped != 0`). Prod:
  `cert_refused_dropped = 4`, exactly matching `dedup_bins_committed_total = 4` —
  every completed dedup was refused a grant for having done work.
- Only a separate zero-drop **probe** can mint a proof, and probes were queued
  behind probes that cannot certify (fixed in `c627b356`).

## What IOx does

InfluxDB 3 / IOx does not certify "I removed the duplicates". It maintains a
**structural invariant** and records it as the file's **compaction level**:

- **L0** — freshly ingested, may overlap arbitrarily.
- **L1+** — compacted, and *"no files should overlap with other files in the same
  level"*.

The querier then reads the level and plans accordingly:

> "Because File 2 and File 3 overlap, they need to go through the Deduplicate &
> Merge operator. File 1 does not overlap with any file and only needs to be
> unioned with the output of the deduplication."

Three differences matter, and the third is the deep one.

**1. Granularity — per FILE, not per PARTITION.** IOx's proof is an attribute of
each file. TimeFusion's is a fingerprint over the partition's entire file list,
so one new file invalidates the proof for every file in the partition. This is
the difference between a proof that survives ingest and one that cannot. TF
already has the right shape in `certified_files_in_partition` /
`skippable_certified_files` — the additive, per-file half — and it is the part
that should carry the weight.

**2. Who mints it — the compactor, at commit.** IOx's compaction *produces* the
level as part of writing the output. TimeFusion re-derives cleanliness afterwards
with a probe, which is a second full pass and a second chance to be pre-empted.

**3. What it asserts — a structural property, not a content one.**
IOx asserts **non-overlap**, which is checkable from min/max statistics without
reading a single row. TimeFusion asserts **duplicate-freedom**, which is a
statement about content and needs a scan.

This is the reframing: **non-overlap in the dedup-key space *implies* what
certification is trying to prove.** Two versions of one key are by definition the
same key; if two files' key ranges are disjoint, no duplicate group can straddle
them. So a cheap, statistics-only check subsumes an expensive content scan.

TimeFusion's own `version_append` is what breaks time-disjointness in the first
place (`2026-09-03-why-the-frozen-mass-is-a-read-path-bug.md`), and compaction is
the only thing that restores it. The lever has been named in these docs before —
"union the non-overlapping, merge only the overlapping" — and it is still unbuilt.

## What this says about tonight's decision

I considered relaxing `record_certification` to grant when a *rewrite* leaves the
partition clean, since a keep-greatest dedup output is duplicate-free by
construction. I did not, and the prior art supports that:

**IOx never certifies "I did the work correctly."** It maintains an invariant that
is independently checkable from file statistics. Certifying from the rewrite would
make the read path trust the rewrite's correctness — so a dedup bug would become
invisible rather than merely expensive, and the failure mode here is a silent
over-count on every dashboard tile. The two-pass design's independence is a
feature; the defect was that the second pass was starved, which is now fixed.

## The direction, if this is picked up

Not "make certification cheaper" but "certify the cheaper property":

1. Record, per output file at commit, the **dedup-key range** the rewrite
   produced (min/max of the sort prefix — already the sort order, already in the
   footer). This is `Add.stats` work, not a new subsystem.
2. At plan time, partition the scan's files into non-overlapping and overlapping
   sets on that range, union the first above `DedupExec` and merge only the
   second. This is exactly what `skippable_certified_files` already does with
   *timestamp* spans — the change is the key it compares on and the fact that the
   evidence comes from the file's own statistics rather than a stored
   certification.
3. Certification-by-probe then becomes a fallback for legacy files that carry no
   such range, rather than the only path.

The attraction is that step 2's soundness argument is the one already written and
reviewed in `read::skippable_certified_files`; what changes is where the evidence
comes from — and statistics survive restarts, deploys and new writes, which a
process-local `dedup_clean_fp` does not.

Sources:
- [Compactor: A Hidden Engine of Database Performance — InfluxData](https://www.influxdata.com/blog/compactor-hidden-engine-database-performance/)
- [Working with the ReplacingMergeTree engine — ClickHouse](https://clickhouse.com/docs/guides/replacing-merge-tree)
- [ReplacingMergeTree Explained — Altinity](https://altinity.com/blog/clickhouse-replacingmergetree-explained-the-good-the-bad-and-the-ugly)

## Footnote: ClickHouse is the cautionary case, not the model

ReplacingMergeTree deduplicates only *within* a merged part, asynchronously, with
no guarantee of when — and offers no part-level "fully deduplicated" flag to
query. The standard advice is `FINAL`, or `ORDER BY key, version DESC LIMIT 1 BY
key` at query time: i.e. **always pay the merge-on-read cost**. That is
TimeFusion's current behaviour (`dedup_skipped = 0 of 2,051`), reached by
accident rather than by design. It is a working system at scale, so this is not
fatal — but it is the state to move away from, and IOx is the one that shows how.

## CORRECTION — the cheap proof does not exist yet, because the layout does not

The section above recommends certifying **non-overlap from file statistics**
rather than duplicate-freedom from a content scan, on the grounds that
non-overlap in the dedup-key space implies what certification is trying to prove.
The implication is sound. **The premise is not, on today's data.**

Measured straight from `Add.stats` on the live Delta log — no data read, which is
the whole point of the proposal (`scratchpad/disjointness.py`):

```
otel_logs_and_spans: 8,122 live files, 1,217 cells, 0 files lacking timestamp stats

files that overlap NOTHING else in their cell : 1,100 / 8,122 = 13.5%
cells where EVERY file is disjoint            :   968 / 1,217 = 79.5%

cell size    files   disjoint    pct
1              946        946  100.0%
2-4            313         42   13.4%
5-16           320         69   21.6%
17-64        3,114         33    1.1%
65+          3,429         10    0.3%
```

The 79.5 % of "fully disjoint cells" is **an artefact**: 946 of the 1,217 cells
hold exactly one file, and a lone file is trivially disjoint. Those cells were
never the problem.

**In the cells that matter, non-overlap essentially does not exist.** Cells of
17–64 files are **1.1 %** disjoint; cells of 65+ are **0.3 %** — and those two
buckets hold **6,543 of 8,122 files, 81 % of the table.**

So certifying non-overlap today would let ~13.5 % of files skip the dedup, almost
all of it from single-file cells that already cost nothing. **It would not move
the customer's queries.**

### What this actually means — the recommendation was half right

IOx's non-overlap proof is cheap *because its compactor maintains non-overlap as
an invariant*: L1+ files are non-overlapping **by construction**. Ours are not,
and the reason is documented in this repo already — `version_append` writes a
row's ORIGINAL timestamp into a NEW file, so every update re-overlaps the
partition (`2026-09-03-why-the-frozen-mass-is-a-read-path-bug.md`).

Non-overlap is therefore **not a cheaper way to prove what we have. It is a
different physical layout we would have to build**, and the proof only becomes
cheap once the layout exists. Stated as a sequence:

1. compaction **establishes** disjointness in the dedup-key space (this is work,
   and it is the same work the frozen-mass docs already argue for);
2. **then** statistics prove it for free, per file, surviving restarts and new
   writes;
3. **then** the querier unions the disjoint files above the dedup.

Step 2 is the part I described as the fix. It is really the payoff, and step 1 is
the cost — which is exactly the cost the maintenance backlog is currently failing
to pay.

**This does not rescue the coverage arithmetic in the morning brief**, and it
should not be read as doing so. It says the escape route I proposed needs the
layout work first, so the honest position is: **there is no cheap way out of the
coverage problem that today's file layout supports.**

I am glad this was measured before it was recommended further; it took one query
against statistics that were already there.

## REFINEMENT — the layout IS reachable, and by the work already queued

The correction above says non-overlap is "a different physical layout we would
have to build". That is too pessimistic, and the statistics say why. Measuring
each file's span as a fraction of its date partition (`scratchpad/span_widths.py`,
`Add.stats` only):

```
cells with >=17 files (81% of the data): file span as a fraction of its DAY
   p10 0.001   p50 0.010   p90 0.208   p99 0.880
   spanning >50% of the day:  4.0%
   spanning >90% of the day:  0.9%
```

**Files are NARROW. The median file in a big cell covers 1 % of its day — about
14 minutes — and only 4 % span more than half a day.** So the earlier suspicion
that wide, day-spanning files cause the overlap is **wrong for the cells that
hold 81 % of the data.**

Which leaves only one explanation, and it is the encouraging one: **many narrow
files cover the SAME narrow windows.** Sixty-five files averaging fourteen
minutes could tile a day comfortably and be almost entirely disjoint; instead
0.3 % of them are. They are stacked, not spread — which is exactly what a flush
path that writes one file per 10-minute bucket per writer produces.

**Stacked files are merged by ordinary compaction.** Merging every file covering
one window into one file makes that window disjoint from the others, because the
windows themselves are already narrow. No splitting, no new layout, no re-sorting
by a different key — this is precisely what hot-tail packing and sealed
consolidation already do.

So the honest sequence is better than the correction implied:

1. **Drain the compaction backlog** — the work already queued, and the thing the
   four fixes tonight were aimed at.
2. Disjointness follows **as a by-product**, because the spans are already narrow
   and merging removes the stacking.
3. Statistics then prove non-overlap for free, per file, surviving restarts.
4. The querier unions disjoint files above the dedup, and certification-by-probe
   becomes a fallback rather than the only path.

**This does not change the coverage arithmetic's conclusion** — coverage is still
not reachable by waiting at today's throughput. What it changes is the *target*:
the escape route is not a new storage design, it is **finishing the compaction
that is already the plan**, after which the cheap proof becomes available.

Two corrections in one night on the same recommendation, in opposite directions.
The first was right that the premise was unmeasured; the second is what the
measurement actually says.

## The overlap components — where merging works, and where it cannot

Merging every mutually-overlapping group makes the groups disjoint by
construction, so the achievable layout is exactly the **connected components of
the overlap graph**. Computed by sweep-line over `Add.stats`
(`scratchpad/components.py`), on the 107 cells holding 17+ files:

```
6,544 files  ->  206 overlap components        (96.9% fewer files)

component BYTE size (206 components):
   p50      9.8 MiB    p90  1,168.9 MiB    p99  82,609 MiB    max  89,084 MiB
   fit the 256 MiB target: 70.9%
   over 1 GiB: 25

component FILE count: p50 16   p90 73   max 328
```

**The distribution is violently bimodal, and that is the whole story.**

- **71 % of components are ≤256 MiB, median 9.8 MiB.** These merge inside one
  ordinary compaction unit, and merging them yields disjointness immediately.
  This is the part where "no splitting, no new layout" is exactly right.
- **25 components exceed 1 GiB, and the largest is 89 GiB across 328 files.**
  These cannot be merged as a unit under any deadline. They are the day-wide
  units this repo has already found unsatisfiable, and the heavy tail recorded as
  "6.4 % of units carry 67.1 % of bytes".

So the refinement above needs one more qualification, and it is the important one:

> **Merging establishes disjointness for the ~71 % of components that fit a bin.
> For the giant components it cannot, because you cannot merge 89 GiB — those
> need SPLITTING BY TIME, which is precisely what IOx's L1 does and TimeFusion's
> compaction does not.**

That reconciles the whole thread. IOx's compactor both merges *and* splits, so
non-overlap is an invariant it can maintain at any scale. Ours only merges, which
is sufficient until a component grows past a bin — and then the component becomes
permanently unmergeable and permanently overlapping, which is exactly the frozen
mass.

### What this makes concrete for the 10x question

A component grows by chain-overlap: one file spanning two windows welds them
together, and thereafter every file in either window joins the same component.
**At 10x ingest the welding happens faster**, so more components cross the bin
threshold and become permanently stuck. That is a mechanism by which the frozen
mass grows super-linearly with volume — consistent with the night's opening
theme, that unit size scales with tenant size and a bigger tenant puts a larger
*share* of its work outside the budgets.

**The cheapest high-value change this suggests is a splitting compaction for
oversized components** — not a general re-layout, just the ability to cut one
component into bin-sized time ranges. 71 % of components need nothing; 25 need
this; and the 25 are where the bytes are.

## The splitting primitive already exists — what is missing is unit SELECTION

Before proposing new machinery, I checked what repair already does. It slices:
`coordinator_slice_target` / `repair_slice_want` / `repair_slice_cuts` /
`repair_slice_bounds` (`maintain.rs:7070-7120`) cut a bin into event-time slices
along the lead sort column, and `repair_bin_sliced` logs how many. The bench's
`PROD: b256 p1 x13 slices` row prices exactly this.

But the staging loop says (`maintain.rs:7157`):

> "One pass when not sliced; otherwise one pass per slice, **in sort order, all
> feeding the SAME writer**."

So slices are a *memory* device — they bound what one sort must hold — not a
layout device. Output files are cut by `timefusion_writer_max_file_bytes`, not at
slice boundaries. **That is fine, and in fact already gives the property we
want**: rows arrive in timestamp order, so each output file covers a contiguous
timestamp range and consecutive files are disjoint apart from ties.

**Which means a single compaction unit already produces near-disjoint output.**
The overlap does not come from the writer. It comes from **which files a unit
selects**: `select_coordinator_compaction_candidates` bin-packs **smallest-first
by size**, with no reference to time at all. Two units on the same cell therefore
take arbitrary, interleaved subsets, and each emits files spanning the other's
range. For a component too big for one unit — the 25 over 1 GiB — this is
guaranteed, because no single unit can ever cover it.

### So the change is smaller and better-targeted than "build splitting"

Not a new layout, not a new writer, not even a new slicing primitive:

> **For a component larger than one bin, select the unit by TIME RANGE rather
> than by file size** — give each unit a disjoint slice of the component's span
> and let it take every file overlapping that slice. Successive units then tile
> the component instead of interleaving, and their outputs are disjoint by
> construction.

The pieces this needs already exist: the cut points (`repair_slice_cuts`), the
time-bounded predicate (`repair_slice_bounds`), the sort-ordered writer, and the
component boundaries themselves — computable from `Add.stats` with no IO, as
`scratchpad/components.py` does in a few seconds for the whole fleet.

The bin-packer's smallest-first rule stays exactly as it is for the 71 % of
components that fit a bin; it is only the oversized ones that need the
time-ranged variant. That also respects the reason smallest-first exists
(prod 2026-08-23, one large file eating the budget and retiring nothing).

**Caveat I cannot close from statistics:** files carry a timestamp min/max but a
unit must also not split a *dedup key group* across two units, or two versions of
one row land in different outputs and the dedup can never see them together.
Since `timestamp` leads the dedup key, a cut strictly between two distinct
timestamps is safe; a cut *inside* a run of equal timestamps is not.
`repair_slice_bounds` already reasons about slice boundaries in the output's sort
direction, so this is a question to settle against that code rather than a new
problem — but it is the thing to get right, and it is exactly the class of bug
that is invisible until someone trusts a wrong count.

### The cut-point caveat, measured — and it is not a blocker

The open question was whether a time-ranged unit can always cut *between*
distinct timestamps, since a cut inside a run of equal timestamps would put two
versions of one row in different units and the dedup would never see them
together.

**Logs** — measured on the real prod file `875ea2a1` produced, locally, no prod
load:

```
rows 2,023,604   distinct timestamps 1,765,665   (1.15 rows per timestamp)
rows sharing ONE timestamp: p50 1  p90 2  p99 2  max 36
rows in runs > 1000: 0
possible cut points: 1,765,664
```

The largest indivisible unit is **36 rows — 0.0018 % of the file.** Cuts are
available at essentially any granularity.

**Metrics** — the shape most likely to break this, because scrape-aligned
timestamps put many series on the same instant. Prod, 10-minute window:

```
total rows 65,246   distinct timestamps 1,830   (35.7 rows per timestamp)
max rows sharing one timestamp: 768
```

**Metrics is 31x more clustered than logs, exactly as expected — and still fine.**
768 rows is a trivial granularity floor for a unit sized in the millions. The
concern was real enough to check and is not a blocker for either table.

What this does confirm is that the two tables have **very different timestamp
structures**, so a cut-point strategy must be derived from the data (pick the
next distinct value) rather than from a fixed row or time stride. A fixed stride
tuned on logs would be 31x off on metrics — the same family of mistake as pricing
a bin in rows when the deadline is spent on IO.

## What time-ranged compaction would cost, and what it would buy

Computed from `Add.stats` alone (`scratchpad/split_cost.py`): tile every overlap
component once — merge the ones that fit a bin, cut the oversized ones into
256 MiB time ranges.

```
components 1,318   over 8,106 files
  fit a bin :  1,012  ->  1,012 merge units
  oversized :    306  ->  3,845 time-ranged units   (919.7 GiB, 5,008 files)

TOTAL UNITS to tile the fleet once: 4,857
files after: ~4,857   (from 8,106 — 40.1% fewer)
non-overlap: 13.5%  ->  ~100%   (every output disjoint BY CONSTRUCTION)
```

**The buy is the whole certification problem.** Once outputs are time-disjoint,
non-overlap is provable from statistics per file, for free, surviving restarts —
and the 218:1 decline ratio, the plateau at 52 grants, and `dedup_skipped = 1`
all stop being about scheduling.

**The cost is 4,857 units of debt paydown, one time.** Per-unit wall time is the
open variable and it is NOT the sort: tonight's bench put a 211 MB / 2 M-row sort
at **1.1–2.7 s**, while prod units run ~21 min, so the cost is object-store IO and
the commit. That difference is the number to establish before scheduling this
work — it is the difference between roughly one day and roughly one week of
continuous maintenance.

### The mass is far more concentrated than the fleet numbers suggest

```
oversized components, largest first        GiB   files  units   cell
   87.0   121   348   87576849/2026-07-24
   85.0   116   341   87576849/2026-07-22
   80.7   104   323   87576849/2026-07-28
   80.0   114   321   87576849/2026-07-23
   53.4    76   214   87576849/2026-07-26
   51.8    70   208   87576849/2026-07-21
   50.6    70   203   87576849/2026-07-27
   49.3   101   198   87576849/2026-07-30
```

**Every one of the eight largest components is the same tenant, on consecutive
days in late July** — about 538 GiB of the 919.7 GiB oversized total, from ten
days of one project. This is the whale, and its shape is now exact rather than
anecdotal.

**That changes the sequencing.** This does not have to be a fleet-wide programme
before anything improves: **~58 % of the oversized mass is ten cells.** They can
be tiled independently, in priority order, and the fleet's remaining 298
oversized components are comparatively small. A targeted paydown of one tenant's
July is a far easier thing to schedule — and to measure — than "drain the
backlog".

### The one number needed to size this cannot be measured today

The paydown estimate above turns on per-unit wall time, and specifically on where
it goes. Two attempts, both worth recording:

**1. Measuring object-store throughput from here is meaningless.** Reading three
~300 MiB prod files end to end gave **3.9 MiB/s aggregate**, which would imply
~66 s of pure read per 256 MiB unit. **That number must not be used.** It is this
laptop's link to OVH over the public internet, not the datacenter-local path prod
reads on. I include it only so nobody re-derives it and believes it.

**2. Prod does not instrument the decomposition.** The only unit timing emitted
is `maintenance_unit_slow` (`maintain.rs:3679`), which carries
`elapsed_secs` / `deadline_secs` / `headroom_pct` and fires **only** when a unit
uses more than a quarter of its deadline. There is no read/sort/write/commit
split anywhere on the staging path.

So the decisive question — *of the ~21 minutes a prod unit takes, how much is
object-store read, how much is the sort, how much is the Delta commit?* — **has
no answer available today**, and tonight's bench only rules the sort out
(1.1–2.7 s for 211 MB / 2 M rows at prod's per-worker pool).

**That makes phase timers the gate on this whole programme.** Not a nice-to-have:
4,857 units is roughly a day of continuous maintenance if a unit is ~30 s of IO
and commit, and roughly a week if it is ~21 minutes. Those are different
decisions, and nothing currently distinguishes them.

It is also, once again, the coverage-matrix pattern: compaction has a rich funnel
for **which files a unit selects** — good enough that it exposed tonight's
livelock — and **nothing at all for what a unit costs.** Adding four timers around
the existing staging loop is a smaller change than any fix shipped tonight, and
it is what converts "drain the backlog" from an aspiration into a schedule.

## The drain is real, and it does NOT produce disjointness — measured 90 minutes apart

Two runs of the same statistics-only measurement, ~90 minutes apart, on the same
long-lived process:

| | earlier | +90 min | change |
|---|---:|---:|---|
| live files | 8,122 | **7,781** | **-341 files** |
| disjoint (all cells) | 13.5 % | 14.1 % | +0.6 pt |
| cells 17-64: files | 3,114 | 2,983 | -131 |
| cells 17-64: **disjoint** | 33 | **23** | **-10** |
| cells 65+: files | 3,429 | 3,168 | -261 |
| cells 65+: **disjoint** | 10 | **10** | **0** |

**Compaction is unambiguously working: 341 files retired in ninety minutes, about
3.8 files/minute, concentrated in exactly the big cells.** That settles any doubt
that the lane is productive once a process is left alone.

**And disjointness did not move.** Fleet-wide it went 13.5 % → 14.1 %, which is
the single-file cells drifting. In the cells that hold 81 % of the data it is
**flat at 0.3 %** for 65+ and actually **fell** for 17-64. Hundreds of files were
merged away and the overlap property is exactly where it started.

**This is the prediction of merge-but-never-split, confirmed in real time.**
Merging files inside a cell reduces the count; it does not reduce overlap,
because units select files by SIZE and each unit's output therefore spans the
others' ranges. The property the read path needs is untouched by the work the
maintenance lane is doing.

### It separates the night's two competing explanations, and both are partly right

- *"The queue drains linearly, so the deploy cadence is the whole problem."*
  **True of file count** — 3.8 files/min, and it only happens on an unrestarted
  process.
- *"It asymptotes at the frozen mass, so the split defect is the whole problem."*
  **True of disjointness** — 0.3 % before, 0.3 % after, 261 files later.

So they are not rivals. **Draining fixes fragmentation. Only splitting fixes
overlap.** And it is overlap, not fragmentation, that keeps `dedup_skipped` at 1,
certification declining 218:1, and the customer's `hashes` predicate stranded
above `DedupExec`.

**The practical consequence is uncomfortable and worth stating plainly: letting
the maintenance lane run to completion — even for many uninterrupted hours —
would not fix the customer's queries.** It would leave a tidier table with the
same overlap. That is the strongest argument yet for the time-ranged unit
selection, and it is now an observation rather than an inference.

## SIMULATION REFUTES the time-ranged proposal — and finds the real rule

Before recommending time-ranged unit selection further, I simulated it against
today's size-ordered rule on the largest real component
(`87576849/2026-07-24`, 121 files, 87 GiB):

```
TIME-RANGED : 120 units   overlapping unit-range pairs: 1,405 of 7,140
SIZE-ORDERED: 121 units   overlapping unit-range pairs: 1,407 of 7,260
```

**Essentially identical. The proposal does not work on the shape it was designed
for.** One unit per file, and their ranges still overlap.

The reason is in the cell's own statistics, which I had not looked at:

```
whale cell 87576849/2026-07-24, 121 files
  file SIZE  : p10 668 MiB   p50 1017 MiB   p90 814 MiB
  files over the 256 MiB target: 99%
  file SPAN  : p50 6.3% of a day (~90 min)   >50% of a day: 1%
```

**Ninety-nine per cent of this cell's files are ALREADY over the compaction
target** — a full GiB each — while overlapping one another across ~90-minute
spans. So this is not "many small stacked files" at all. My earlier
"narrow and stacked" characterisation was a fleet average that does not
describe the mass.

### The actual rule that freezes it

`select_coordinator_compaction_candidates` opens with:

```rust
// A file at or above target is converged and never packing's work.
if add.size >= target { continue; }
```

**So 99 % of this cell is skipped before any selection rule runs.** Time-ranged
or size-ordered makes no difference — the packer never considers these files at
all. The cell is, by compaction's own definition, *finished*.

**And that definition is the defect.** "Converged" means *large enough*, which is
a statement about FRAGMENTATION. The read path does not care about file size; it
cares about OVERLAP. A cell of 121 mutually-overlapping 1 GiB files satisfies
compaction completely and defeats the reader completely, and nothing in the
system currently notices the difference.

That is why disjointness sat at 0.3 % across ninety minutes while 341 files were
retired elsewhere: **the work was happening in the cells that were already fine,
and the frozen cells were skipped by design.**

### What this changes

The lever is not how units select among *eligible* files. It is that
**over-target files are permanently ineligible, so an overlapping cell can never
be repaired.** Any fix has to make overlap a first-class reason to rewrite a
file, independent of its size — and rewriting 121 GiB-sized files to disentangle
them is genuinely expensive, which is presumably why the rule exists.

I am deliberately stopping the design here rather than proposing a replacement
rule at 04:00 after the last one was refuted by its own simulation. What is
established, and worth the morning:

1. the frozen mass is **over-target, mutually-overlapping files**, not small ones;
2. compaction **skips them by design** and always will;
3. "converged" is defined on size and the read path needs it defined on overlap;
4. the simulation harness (`scratchpad/sim_timeranged.py`) is written, so the
   next candidate rule can be tested against real cells before anyone builds it.

## One more layer: dedup IS bin-scoped, and the whale's files straddle ~9 bins

Checking whether *any* lane can touch over-target files: `dedup_partition_paths`
takes **every** file of the partition with **no size filter**, so the dedup path
is not subject to the packer's `size >= target` skip. Dedup can read these files.

And dedup is scoped to **bins** — the 10-minute time buckets tracked in
`dedup_dirty_bins` and probed by `probe_dup_bins`. A bin-scoped rewrite emits
that bin's rows in sort order, so **its output is time-bounded by construction**.
That is the time-ranged mechanism I proposed building. **It already exists, in
the dedup lane.**

So why is the whale still frozen? Put the two measurements together:

- the whale's files have **p50 span 6.3 % of a day ≈ 90 minutes**;
- a bin is **10 minutes**.

**Each file straddles roughly nine bins.** So a bin-scoped rewrite of one 10-minute
bin must read every file overlapping it — on this cell, several GiB of input to
emit one bin's output — and it cannot retire those inputs, because they still
hold eight other bins' rows. That is the shape behind the budget refusals this
repo has recorded before (92.9 % refused on budget).

**I am stating this as an interaction to verify, not as a fourth mechanism.** I
have now revised this diagnosis three times tonight, each revision correct
against its own measurement and each superseded by the next; the honest position
at 04:00 is to record what is established and stop.

**Established, and independently checkable:**

1. The packer skips every file at or above target (`mod.rs:7944`), so
   HotPacking and SealedConsolidation can never touch 99 % of the whale.
2. The dedup path applies **no size filter**, so it can.
3. Dedup is **bin-scoped**, and a bin-scoped rewrite's output is time-bounded —
   the disjointness mechanism exists there and nowhere else.
4. The whale's files span **~9 bins each**, so bin-scoped work over them reads far
   more than it writes and cannot retire its inputs.

**The question that decides the fix — and the one to open with in daylight:**
*is the frozen mass frozen because dedup refuses it on budget, or because nothing
ever enqueues those bins?* Facts 1–4 make both plausible, they imply completely
different fixes, and the funnel to tell them apart does not exist — which is the
same instrumentation gap that `prep/unit-phase-timers` opens.

**Bin figure verified** (I checked because `mem_buffer.rs` uses 5 minutes and I
did not want to quote the wrong constant). There are two distinct "bucket"
concepts and they differ:

- **MemBuffer flush bucket** — `DEFAULT_BUCKET_DURATION_MICROS = 5 * 60 * 1e6`
  (`write/mem_buffer.rs:37`), i.e. **5 minutes**.
- **Dedup bin** — `BIN_MICROS = 10 * 60 * 1e6`, defined identically in three
  places (`database/compact.rs:1150`, `database/write.rs:502`,
  `database/maintain.rs:5726`), i.e. **10 minutes**.

The dedup bin is the 10-minute one, so "the whale's files straddle ~9 bins"
stands. **Note for `CLAUDE.md`: it documents the MemBuffer as using "10-minute
time buckets", which the code contradicts — that constant is 5 minutes.** A small
thing, but it is exactly the sort of stale figure that produces a confidently
wrong calculation, and `BIN_MICROS` being copy-pasted in three files is a second
one waiting to happen.
