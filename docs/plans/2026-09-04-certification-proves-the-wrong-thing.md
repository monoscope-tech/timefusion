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

## The open question, answered: neither. The lanes are not blocked, they are slow.

I proposed two possibilities — dedup refuses these bins on budget, or nothing
enqueues them. **The counters say neither.** On the long-lived process:

```
dedup_bins_committed_total        20      dedup_failed_total              0
dedup_waves_committed_total       15      dedup_timed_out_total           0
                                          dedup_bin_stage_timeouts_total  0
                                          dedup_bins_deferred_cold_total  0
retry.Dedup.dedup                 22      retry.Repair.compaction_incomplete   68
retry.Dedup.dedup_incomplete       5      retry.HotPacking.compaction_debt_remaining 24
retry.Dedup.source_not_flushed    16      retry.BaseRollup.source_not_flushed  20
retry.Dedup.worker_error           6
```

**Every hard-refusal counter is zero.** No budget deferrals, no stage timeouts,
no failures, no cold deferrals. Dedup is being claimed, is running, and has
committed **20 bins** — up from 4 earlier in the night. The lanes are working.

**So the constraint is throughput, not blockage** — and that is a materially
better answer than either hypothesis, because it means nothing needs unblocking.
It also puts a number on the scale of the problem: at 10-minute bins a single
day-cell holds **~144 bins**, the ten whale cells hold **~1,440**, and each of
those bins must read the several GiB of overlapping GiB-sized files that straddle
it. **Twenty bins committed in roughly two hours** is the rate against that.

**This makes `prep/unit-phase-timers` unambiguously the first thing to merge.**
The question is no longer "what is blocked" — nothing is. It is "what does a bin
cost, and which phase owns the cost", and that is precisely the number the
codebase does not currently emit. Everything downstream — whether to change the
convergence rule, whether to split, whether more uptime helps — turns on it.

**And it retires my own framing.** I spent the small hours looking for the thing
that was stuck. The counters say nothing is stuck; the work is simply far larger
than the rate, and the rate has never been decomposed.

## The rate, decomposed as far as today's counters allow — and it is brutal

On the long-lived process at **7,148 s** uptime:

```
work.Dedup.worker_secs        69,752        dedup_bins_committed_total   20
work.Dedup.progress_rows  19,186,817,032    dedup_waves_committed_total  15
work.Dedup.rows_dropped          105,053    pending_dedup             2,134
```

Three ratios, each worth stating on its own.

**1. Dedup is consuming essentially the whole heavy maintenance pool.**
69,752 worker-seconds in 7,148 wall-seconds is **9.76 workers running
continuously**, against `HEAVY_REWRITE_PERMITS = 10`. **Dedup is ~98 % of heavy
maintenance capacity.** Every other lane — packing, consolidation, repair,
rollups — divides the remaining 2 %.

**2. Each committed bin costs ~58 worker-MINUTES.**
69,752 / 20 = **3,488 worker-seconds per bin.** At ~1,440 bins across the ten
whale cells alone, that is ~1,400 worker-hours, or roughly **six days at the
full ten-permit pool** — for one tenant's ten days.

**3. It processes ~182,000 rows for every row it removes.**
19.2 **billion** rows through the dedup path to drop **105,053**. That is the
cost of files straddling ~9 bins each: a bin rewrite must read every overlapping
file in full, and almost everything it reads is not a duplicate.

**Caveat on ratio 3:** `progress_rows` on this path is fed by BOTH
`note_unit_progress` (rows written) and `PlanProgress` (plan `output_rows`), so
19.2 B is an upper bound and may double-count. Halve it and the figure is still
~91,000 rows read per row removed. Ratio 1 does not depend on it at all —
`worker_secs` against uptime and permits is arithmetic.

### What this changes

**"Sorting, hotpacking, rollups, dedup should not be breaking a sweat" — dedup is
using 98 % of the pool to remove a hundred thousand rows.** That is the answer to
the 10x question in one line, and it is not about scheduling, budgets, livelocks
or certification: **the work per unit of benefit is four to five orders of
magnitude too high**, because the physical layout forces every bin to read files
that overlap it nine deep.

It also reframes everything shipped tonight. The four fixes were real and each
removed a genuine defect — but they were tuning the allocation of a pool that is
98 % consumed by an operation doing 182,000x more reading than removing. **The
layout is the cost. Nothing scheduled on top of it can be cheap.**

## The pruning lead, closed: there is no timestamp predicate, and there cannot be

The dedup rewrite's row filter is built at `compact.rs:1569`:

```rust
let rows_filter = format!("{partition_filter} AND \"{DEDUP_FILE_COL}\" IN ({in_list}){shard_pred}");
```

Partition, a **file-ID list**, and a shard predicate. **No timestamp range.** So
the scan is never given a reason to prune row groups, and the 20-row-group
statistics measured above go unused.

**But that is not a bug, and adding a predicate would be wrong.** The rewrite
REPLACES the files it selects. To retire a file you must rewrite **every row in
it**, not just the rows of the bin that made it dirty — otherwise the rest of the
file is lost. File-granular replacement therefore *requires* reading selected
files in full, and a timestamp predicate would silently drop rows.

**So the amplification is structural, and it is the same fact from a third
angle:** a bin becomes dirty, the bin maps to the ~9 files straddling it, and
those files must be rewritten whole — dragging their other eight bins along. The
read amplification is not a missing optimisation; it is the cost of using **whole
files as the unit of replacement** when files span many bins.

That closes the last cheap lead I had. **Ways out are all structural**, and each
one is a real project: narrower files at write time so a file spans one bin;
partial-file replacement (deletion vectors, so a rewrite can supersede part of a
file); or a compaction that re-cuts files onto bin boundaries once so later
dedups are bin-local.

**I am stopping the analysis here.** Tonight has produced eight corrections, and
the last three were each triggered by reading a definition I had assumed. The
established facts are recorded above; the next person should start from
`prep/unit-phase-timers` and a decomposition of that ~58 worker-minutes, because
every option in the paragraph above trades differently against read, sort and
commit, and nobody currently knows which one dominates.

## Prior art on the three structural options — one is eliminated by our stack

The three ways out named above are not novel; the field has an answer, and it is
worth knowing which before choosing.

**The industry answer to exactly our problem is DELETION VECTORS.** Delta Lake's
own framing of the motivation is our situation almost verbatim:

> "if we had a Delta table with one massive Parquet file containing 1,000,000,000
> rows and we delete or update one row, copy-on-write would result in
> 999,999,999 rows of data being written to a new Parquet file, even though only
> 1 row is being updated"

That is precisely what a dedup bin does here: rewrite ~9 bins of file content to
remove a handful of duplicate rows. Deletion vectors mark rows as removed
**without rewriting the file**, and defer the real rewrite to a later `OPTIMIZE`
— which is exactly the read-amplification fix option 2 describes.

**But delta-rs cannot write them.** It *reads* tables with deletion vectors;
writing is an **open feature request** (delta-io/delta-rs issue #4079, opened
2026-01-14), and `delta-kernel-rs` does not support writing at all yet.

**So option 2 is blocked on upstream or fork work.** We do carry a delta-rs fork,
so it is not impossible — but it is a protocol-level feature with correctness
consequences across every reader, not a maintenance tweak, and it would be the
largest thing in this document by a wide margin.

**That elevates the two options we can actually build today:**

1. **Narrower files at write time** — if a file spans one bin, a dirty bin
   rewrites one file and the amplification disappears at the source. This is a
   flush/compaction output-sizing change, entirely within our control, and it
   only helps data written *after* it ships.
3. **A one-time re-cut of existing files onto bin boundaries** — pays the
   amplification once, deliberately, to make every later dedup bin-local. This is
   what would drain the existing whale mass, and it is schedulable per cell.

**They are complements, not alternatives:** (1) stops the problem being created,
(3) clears what already exists. Neither needs a protocol change, and both are
sized by the same unknown — the read/sort/commit split that
`prep/unit-phase-timers` would emit.

Sources:
- [Delta Lake Deletion Vectors](https://delta.io/blog/2023-07-05-deletion-vectors/)
- [What are deletion vectors? — Delta Lake docs](https://docs.delta.io/delta-deletion-vectors/)
- [delta-rs #4079: Support writing to tables with Deletion Vectors enabled](https://github.com/delta-io/delta-rs/issues/4079)

## Option 1 sized: the mismatch is BIN vs FILE SPAN, and either side can move

`timefusion_writer_max_file_bytes` is **512 MiB**, and its own comment already
records the property we want:

> "each cut lands on a contiguous slice of an already-sorted stream, so every
> piece keeps a sorted footer and stays **event-time disjoint**"

**So the writer already emits event-time-disjoint pieces.** The problem is not the
cut, it is the *size* it cuts at relative to the bin:

| | span |
|---|---|
| dedup bin (`BIN_MICROS`) | **10 min** |
| a 512 MiB file at whale density | **~45 min** |
| the whale's actual files (p50 1017 MiB) | **~90 min** |

**A file straddles ~4.5 bins at today's setting and ~9 at the sizes actually on
disk.** That single mismatch is the read amplification.

It can be closed from either end, and the trade is different:

- **Shrink the file to the bin.** Cutting at a bin boundary means ~114 MiB files
  at whale density — **4.5x more files**, which is precisely the fragmentation
  compaction exists to remove. This buys bin-locality at the cost of file count,
  and file count is what the packer and the query planner both pay for.
- **Widen the bin to the file.** `BIN_MICROS` is a constant (10 min, duplicated
  in three files). A ~45-minute bin would make a 512 MiB file span roughly one
  bin with **no change to file sizes at all** — the cheapest possible version of
  option 1. The cost is dedup granularity: a dirty bin drags 4.5x more rows into
  each unit, so units get bigger even as they get fewer.

**I have not tested either and I am not recommending one.** What is worth having
written down is that the defect is a *ratio*, not a property of files or of bins
alone, and that one side of it is a constant. That is a much smaller design space
than "change the storage layout", and it is the first thing I would put in front
of whoever picks this up — along with the caution that `BIN_MICROS` is
copy-pasted in three places (`compact.rs:1150`, `write.rs:502`,
`maintain.rs:5726`) and would need to move in all of them.

## Bin widening, SIZED — 4.5-9x less read amplification from one constant

`bins/file` is the read amplification of a bin-scoped rewrite: a dirty bin must
rewrite every file covering it, and each such file carries `(bins/file - 1)`
other bins' rows that are read and rewritten for nothing. Computed over 7,702
live files from `Add.stats`:

| `BIN_MICROS` | all files, mean bins/file | cells with 17+ files, mean |
|---:|---:|---:|
| **10 min (today)** | **27.77** | **11.35** |
| 20 min | 14.20 | 6.02 |
| 30 min | 9.69 | 4.26 |
| 45 min | 6.68 | 3.09 |
| 60 min | 5.20 | 2.52 |
| 120 min | 2.97 | 1.68 |

**Widening the bin from 10 to 60 minutes cuts mean amplification 27.8 → 5.2
(5.3x) fleet-wide and 11.35 → 2.52 (4.5x) where the mass is. At 120 minutes it is
9.4x and 6.8x.** From changing one constant, against an operation consuming ~98 %
of the maintenance pool.

**The tension I cannot resolve from statistics, and will not hand-wave:** a wider
bin means each unit covers more time, so units get *bigger*. The offsetting
argument is that a unit's cost is bounded by **the files it must read**, not by
the bin's width — at 10-minute bins, six separate units each read the same ~45
minute file; at 60 minutes, one unit reads it once for six times the benefit. If
that holds, total work falls ~6x and unit size barely moves. If it does not — if
a wider bin pulls in proportionally more files — units grow toward the 900 s
deadline and the change makes things worse.

**Which of those happens is exactly the read/sort/commit split that
`prep/unit-phase-timers` emits, and it is why that branch is first.** The table
above says the prize is 4.5–9x on the dominant consumer of the entire pool; the
timers say whether it is collectable.

**One caution if anyone acts on this:** `BIN_MICROS` is not a config value, it is
a `const` copy-pasted into three files (`compact.rs:1150`, `write.rs:502`,
`maintain.rs:5726`). Changing it also re-keys `dedup_dirty_bins`, so in-flight
bin state from the old width would need to be discarded rather than
reinterpreted.

## The tension RESOLVED from statistics — wider bins barely grow the unit

I said the risk of widening `BIN_MICROS` was that units grow, and that only the
phase timers could settle it. **That was wrong: it is answerable from `Add.stats`
alone.** A unit must rewrite every file overlapping its bin, so unit size is
"bytes of files overlapping the bin" — computable per bin width, no code, no IO.
Over the 95 cells holding 17+ files (5,966 files):

| `BIN_MICROS` | files/bin | **bytes/bin (unit size)** | bins with data | **total read to sweep once** |
|---:|---:|---:|---:|---:|
| **10 min (today)** | 5.17 | **1,469 MiB** | 13,610 | **19,530 GiB** |
| 20 min | 5.60 | 1,522 MiB | 6,808 | 10,120 GiB |
| 30 min | 6.01 | 1,577 MiB | 4,540 | 6,990 GiB |
| 45 min | 6.66 | 1,653 MiB | 3,028 | 4,887 GiB |
| **60 min** | 7.26 | **1,734 MiB** | 2,271 | **3,847 GiB** |
| 120 min | 9.76 | 2,053 MiB | 1,137 | 2,280 GiB |

**Going from 10 to 60 minutes: unit size grows 1,469 → 1,734 MiB — just +18 % —
while the total read volume to sweep every cell once falls 19,530 → 3,847 GiB, a
5.1x reduction. At 120 minutes it is +40 % unit size for an 8.6x reduction.**

**The feared trade barely exists**, and the reason is the same fact as everything
else: **files already span 45–90 minutes**, so a 60-minute bin overlaps almost the
same file set a 10-minute bin does. Today's narrow bins do not read less per
unit — they read *the same files, over and over, once per bin*. 13,610 bins each
pulling 1.4 GiB is the 19.5 TiB.

### Why this is the strongest lever found tonight

- It attacks the **dominant** consumer: dedup at ~98 % of the maintenance pool.
- It is **one constant**, not a layout change, not a protocol change, not a new
  compaction mode.
- The measured prize is **~5x less total read volume** for **+18 % unit size**.
- It needs **no new data**: this table is the whole analysis, and anyone can
  re-run `scratchpad/bin_unit_size.py` against a fresh checkpoint.

**What still argues for the phase timers first** — but no longer as a blocker:
they give the *absolute* cost (is a unit read-bound or commit-bound?), which sets
expectations for how much of that 5x actually shows up as wall-clock. The
*relative* question, which I wrongly said needed them, is settled above.

**Caveats I can see, stated rather than buried:** a 1,734 MiB unit is not small,
and the deadline and memory budgets were tuned around today's shape — `+18 %` on
an average hides a tail. And `bins with data` falling from 13,610 to 2,271 means
far fewer, larger units, which interacts with the claim/lease machinery
(starvation ordering, retries, the 900 s deadline) in ways this table cannot see.

## It generalizes: `otel_metrics` gives 5.5x for +8 %

The bin-width table above was `otel_logs_and_spans` only. Re-run against
`otel_metrics` (95 cells with 17+ files, 5,364 files):

| width | files/bin | unit size | total read to sweep once |
|---:|---:|---:|---:|
| **10 min (today)** | 4.96 | **277 MiB** | **3,666 GiB** |
| 30 min | 5.73 | 285 MiB | 1,263 GiB |
| **60 min** | 6.87 | **299 MiB (+8 %)** | **661 GiB (5.5x less)** |
| 120 min | 9.16 | 324 MiB (+17 %) | 359 GiB (10.2x less) |

**Better than logs on both axes: +8 % unit size instead of +18 %, for 5.5x
instead of 5.1x.** And metrics units are far smaller in absolute terms — 277 MiB
against logs' 1,469 MiB — so the deadline risk that argues for a soak is much
lower on this table.

**Across both tables the sweep cost falls 23,196 GiB → 4,508 GiB, 5.1x.**

Two things follow.

**The lever is not tenant- or table-specific.** It comes from the same fact in
both: files span far more than a bin, so narrow bins re-read them. That it lands
within half a point on two tables with completely different row widths
(logs ~104 B/row, metrics ~12 B/row) and different sort keys is the strongest
evidence available that it is structural rather than an artefact of one shape.

**`otel_metrics` is the better place to try it first.** Its units are 5x smaller,
its improvement is larger, and it is already the table with the worst dedup path
(`2026-09-04-otel-metrics-never-got-the-collapse.md` — it takes the window form
on every rewrite). A soak there risks less and would show more.

## Per-cell: SIX cells are half the fleet's read cost, and they gain the most

Fleet averages hide where the cost is. Total read to sweep one cell once, by bin
width, for the six largest:

| cell | size | files | 10m | 30m | 60m | 120m | 240m |
|---|---:|---:|---:|---:|---:|---:|---:|
| `87576849/2026-07-24` | 87.0 GiB | 121 | 1,342 G | 509 G | 297 G | 195 G | 141 G |
| `87576849/2026-07-22` | 85.0 GiB | 116 | **3,867 G** | 1,343 G | 713 G | 397 G | 236 G |
| `87576849/2026-07-28` | 80.7 GiB | 104 | 1,248 G | 466 G | 273 G | 172 G | 126 G |
| `87576849/2026-07-23` | 80.0 GiB | 114 | 1,083 G | 411 G | 247 G | 167 G | 117 G |
| `87576849/2026-07-26` | 53.4 GiB | 76 | 1,267 G | 455 G | 253 G | 152 G | 99 G |
| `87576849/2026-07-21` | 51.8 GiB | 70 | 1,341 G | 482 G | 266 G | 163 G | 110 G |

**Read the first two columns against the third.** An 85 GiB cell costs
**3,867 GiB** to sweep once at today's bin width — a **45x read amplification**.
The others run 15–25x. These are not averages; they are what the dedup lane
actually pays on the cells that matter.

**Two conclusions, both sharper than anything from the fleet aggregate.**

**1. Six cells are ~52 % of the entire fleet's sweep cost.** Their 10-minute
total is **10,148 GiB** against the fleet's 19,530 GiB. Six cells, one tenant,
six consecutive days in July. Everything else in `otel_logs_and_spans` — 1,200+
cells — is the other half.

**2. They gain the most from widening.** The largest cell's own curve: **2.6x at
30 min, 4.5x at 60 min, 6.9x at 120 min, 9.5x at 240 min.** Those six cells fall
from 10,148 GiB to ~2,048 GiB at 60 minutes.

**So the targeting is unusually favourable.** The change is one constant, its
benefit is largest exactly where the cost is concentrated, and the cells that
gain are a single tenant's contiguous date range — which is also the smallest
possible blast radius for a soak. If the widening were applied to nothing else,
these six cells alone would return ~8 TiB of read per sweep.

**Caveat, and it is the same one as everywhere:** this is read *volume*, not wall
clock. Whether 8 TiB of avoided reads is hours or days depends on the read/commit
split that `prep/unit-phase-timers` measures and nothing currently emits.

## The 45x outlier explained — a handful of WIDE files dominate everything

Why does one cell cost 45x while its neighbour costs 15x, at the same size and
file count? The two cells differ in one respect:

| | `2026-07-22` (45x) | `2026-07-24` (15x) |
|---|---|---|
| files / size | 116 / 85.0 GiB | 121 / 87.0 GiB |
| **files spanning >50 % of the day** | **11 (8.5 GiB)** | **1 (0.8 GiB)** |
| widest file | 75.0 % of day, 1,011 MiB | 78.6 % of day, 850 MiB |

**And the cost of a single wide file is enormous.** A 1,011 MiB file spanning
75 % of a day covers ~108 ten-minute bins, and **every one of those bins must
read it in full**:

```
108 bins x 1,011 MiB = ~107 GiB of sweep read, from ONE file
```

**One gigabyte of data costing a hundred gigabytes of reading.** Eleven such
files is most of that cell's 3,867 GiB.

### This reconciles my earlier contradiction, and it is the better lever

Earlier I measured "files are narrow — p50 spans 1 % of a day" and used it to
argue the wide-file hypothesis was wrong. **Both are true, and I drew the wrong
conclusion from the first.** The median file is narrow; the COST is
`span x size`, so it is dominated entirely by the tail. **1 % of files being wide
is irrelevant to the median and decisive for the total.**

**And it gives a cheaper, better-targeted fix than widening the bin:**

- **It is a handful of files, not a global constant.** Eleven in the worst cell;
  ~1 % fleet-wide.
- **Splitting one is bounded work.** That 1,011 MiB / 75 %-of-a-day file, re-cut
  into ~8 pieces of ~126 MiB each spanning ~9 %, drops from ~107 GiB of sweep
  read to ~13 GiB. **One file rewrite, ~8x return, repeatable per file.**
- **It needs no soak.** Splitting a file into time-contiguous pieces is what the
  writer already does at `max_file_bytes`; there is no change to bins, claims,
  leases or deadlines, and no interaction with the machinery that made bin
  widening need a soak.
- **It is measurable in advance.** `span x size` ranks every file in the fleet by
  exactly how much sweep read it causes, from `Add.stats`, with no IO.

**This should be evaluated before the bin widening.** Both attack the same
ratio — bin widening raises the denominator globally, splitting wide files lowers
the numerator where it is concentrated — but splitting is targeted, incremental,
reversible in effect, and carries none of the claim/lease/deadline risk.

**What I have not done:** ranked the fleet by `span x size` to see how few files
carry the total, or checked why these files are wide in the first place (a
compaction that merged across a wide range, most likely, since the writer cuts at
512 MiB and these are ~1 GiB). Both are cheap and both are the obvious next step.

## Ranked: 6.5 % of files cause 60 % of all maintenance read

Every live file scored by the sweep read it causes — `bins_spanned x size`, since
a file overlapping N bins is read N times per sweep. All 7,632 live files of
`otel_logs_and_spans` (note: a wider denominator than the 5,966 used earlier,
which counted only cells with 17+ files — hence the larger total):

```
total sweep read at 10-min bins: 44,130 GiB

 top N files   % of files   GiB of sweep read   % of total
           1        0.01%                213G         0.5%
          10        0.13%              1,555G         3.5%
          50        0.66%              5,574G        12.6%
         100        1.31%              8,922G        20.2%
         250        3.28%             17,018G        38.6%
         500        6.55%             26,603G        60.3%
```

**500 files — 6.5 % of the table — cause 60 % of all maintenance read.**

The worst offenders individually:

```
  bins      MiB  % of day   GiB of sweep read
   119     1830     82.6%               213G
   141     1237     97.7%               170G
   144     1008    100.0%               142G   <- spans the ENTIRE day
   144     1004    100.0%               141G
   144      936    100.0%               132G
```

**Several files span 100 % of their day.** A 1 GiB file covering all 144 bins is
read 144 times per sweep: **142 GiB of reading for 1 GiB of data.**

### This is the fix, and it is small

Splitting the top 500 files into time-contiguous pieces — which is what the
writer already does at `max_file_bytes`, on an already-sorted stream — would cut
maintenance read volume by ~60 %. The work is **rewriting ~500 files once**,
against a saving of **~26,600 GiB per sweep**. The return is roughly two orders
of magnitude on the first sweep alone.

And unlike every other option in this document it needs **no new mechanism, no
constant change, no protocol feature and no soak**:

- the writer already emits time-contiguous, event-time-disjoint pieces;
- the candidates are rankable exactly, from `Add.stats`, with no IO;
- it is per-file and incremental — do ten, measure, do ninety more;
- and it strictly *reduces* the quantity every other lever is fighting.

**Ranked list of what to do, revised one final time:**

1. **Split the widest files, worst-first.** ~500 files, ~60 % of maintenance read,
   no new machinery. `scratchpad/wide_rank.py` produces the list.
2. **`prep/unit-phase-timers`** — still worth it, now to measure the improvement
   rather than to decide anything.
3. **Bin widening** — the global version of the same fix; keep it in reserve, and
   re-measure after (1), because (1) removes most of what it was going to buy.

**Why this was not obvious earlier, and the lesson:** I measured file spans on
the median (p50 = 1 % of a day) and concluded files were narrow. They are. But
this cost function is `span x size`, and **a median tells you nothing about a
tail-dominated total.** The right question was never "how wide is a typical
file" but "which files cause the most reading" — and that one is answerable
directly, in seconds, from statistics that were there all along.

## COMPACTION CREATES THE WIDE FILES. The two lanes work against each other.

Is the wide-file problem legacy, or still being made? Wide files (>50 % of a day)
by partition age:

```
 age         date  files  wide  wide%  sweep GiB
   0d   2026-09-04     70     0   0.0%          2
   3d   2026-09-01   1657    12   0.7%         75
   6d   2026-08-29    231    34  14.7%        239
   8d   2026-08-27    317    42  13.2%        246
  12d   2026-08-23     22    16  72.7%        339
  13d   2026-08-22     22    12  54.5%        244

last 7 days : 115 wide of 3,645 files ( 3.2%)
older       : 1,158 wide of 3,979 files (29.1%)
```

**Look at the file counts against the wide percentages.** `2026-09-01` has
**1,657 files and 0.7 % wide**. `2026-08-23` has **22 files and 72.7 % wide** —
and costs **339 GiB of sweep read**, more than any recent date, from 22 files.

**The correlation is inverse and near-perfect: the more compacted a partition,
the wider its files, and the more the dedup lane pays for it.**

That is not a coincidence, it is arithmetic. **Merging files unions their time
ranges.** Compaction's whole job is to produce fewer, larger files; larger files
made from merged inputs span wider; and a wider file is read once per bin it
touches. **Compaction is manufacturing exactly the property that makes dedup
expensive.**

### This is the same defect as "converged means large enough", seen from the cost side

`select_coordinator_compaction_candidates` optimises file COUNT and calls a file
done at 256 MiB. Nothing anywhere scores a file's SPAN. So the compaction lane
improves its own metric while degrading the dedup lane's, and neither can see the
other. A partition compacted down to 22 files is, by compaction's definition, in
excellent shape — and is the single most expensive thing in the fleet for dedup
to touch.

**Consequences, in order of how much they change the plan:**

1. **Splitting wide files is NOT a one-time cleanup.** It is a treadmill *fed by
   compaction*, not by ingest. Splitting the top 500 today would help enormously
   and then slowly refill, at whatever rate compaction merges across wide ranges.
2. **So the source fix belongs in the packer**: bound the output's SPAN, not only
   its size. "Do not merge files whose combined range exceeds X" is a
   candidate-selection rule of the same shape as the byte and row budgets already
   there — and this document has now measured what X costs at several values.
3. **Recent data is mostly healthy** (3.2 % wide in 7 days vs 29.1 % older), so
   the damage is concentrated in the compacted past, which is also where the
   frozen mass lives. Those are the same cells for the same reason.

**And it reframes tonight's four fixes one last time.** They made compaction work
better — the packer livelock, the planner budget, the probe ordering, the rollup
liveness. **Compaction working better means files merged wider, faster.** None of
them is wrong, but the lane they accelerate is the one manufacturing the dominant
cost, and that interaction is not visible from inside any of them.

## Can the existing CLI do it? Probably not, and the reason is the same defect

The top recommendation — split the ~500 widest files — would be an *operation*
rather than a project if existing tooling could do it. It nearly can:

```
timefusion optimize --project X --date D --consolidate --target-size-mb N --dry-run
```

`--target-size-mb` exists (`main.rs:881`) and is validated as **"only applies to
`--consolidate`"**. So the obvious move is to consolidate a wide-file cell with a
small target and let the writer's cut produce narrow, time-contiguous pieces.

**I do not think it will work, and the reason is worth stating because it is the
same defect a third time.** Setting `--target-size-mb 128` LOWERS the target — and
the packer's first act is `if add.size >= target { continue; }`. A 1 GiB wide file
is *further* above a 128 MiB target than above a 256 MiB one, so lowering the
target makes it **more** skipped, not less. **The knob that looks like "make files
smaller" actually means "merge files smaller than this", and a wide file is never
on the input side of that.**

That is now the third distinct symptom of one root: **over-target files are
invisible to compaction**, so nothing — not the packer, not the debt planner, not
the CLI — can be pointed at them.

**So this needs verifying, not assuming.** `--dry-run` answers it in one command
against one cell, and it is the first thing I would run in the morning. If it does
skip them, the smallest possible fix is a CLI path that treats over-target files
as *inputs* when explicitly named — far narrower than a new compaction mode, and
it would make the 500-file cleanup an operation after all.

**I am flagging this rather than asserting either way.** I have been wrong nine
times tonight and every one was reasoning past something I had not read. The
`size >= target` skip is read; the CLI's behaviour under a lowered target is not.

### ...and reading further, the operation DOES exist: `--recompress`

My prediction about `--consolidate --target-size-mb` was right, and the code says
so in the same file (`main.rs:956`):

> "**`--recompress` is the ONLY force-rewrite.** Bin-packing (`Compact`/`SortBy`,
> and `consolidate`'s leveled variant) **skips files already at target** AND drops
> single-file bins, so a lone file can never be rewritten by them"

So `--consolidate` cannot touch wide over-target files — as predicted. **But
`--recompress` can, and it is documented as doing exactly what is needed:**

> "`recompress_partition` rewrites the partition through `replace_where` with the
> **schema ORDER BY**, **regardless of file count or size** … `--project`
> narrows the overwrite predicate … which is what makes the job small enough to
> run on an ordinary runner."

**That is the whole fix, and it ships today:**

```
timefusion optimize --project 87576849-… --date 2026-07-22 --recompress
```

It rewrites the cell through a sorted `ORDER BY`, and the writer cuts that sorted
stream at `max_file_bytes` (512 MiB) into **time-contiguous, event-time-disjoint**
pieces. On an 85 GiB cell that is ~170 outputs, each spanning ~1/170 of the day —
**about 8.5 minutes, narrower than the 10-minute dedup bin.**

**A cell recompressed this way would go from 45x read amplification to roughly
1x.** Six cells at ~85 GiB each is ~440 GiB of rewriting, against ~8 TiB of sweep
read saved per pass.

**So the top recommendation is an operation, not a project:** run `--recompress`,
worst cell first, ranked by `scratchpad/wide_rank.py`. It needs no code, no
constant change, no soak, and it is the documented force-rewrite path built for
precisely the "one bad file poisons a partition" shape — which is what a wide
file is, measured in read cost rather than footers.

**What to check before running it**, since I have not: `--recompress` was built
for footer repair on partitions of ordinary size, and its cost on an 85 GiB whale
cell is unmeasured. Start with `--dry-run`, then the smallest of the six, and
watch that the outputs really are ~512 MiB and narrow — `compaction_unit_span`
(on `prep/bin-width`) would report exactly that, which is another reason to merge
it first.

### Verified: `--recompress` will not skip these files, and `--dry-run` is safe

Two checks that remove the last uncertainty from the recommendation above.

**1. `recompress_partition` has exactly ONE skip condition**, and it is
`"no files in partition"` (`compact.rs:691`). There is no size test, no file-count
test, no sorted-run test. **It will rewrite a whale cell.** That is the difference
between it and every other path in this document, all of which begin by skipping
over-target files.

**2. `--dry-run` is genuinely read-only on this path.** I briefly thought it was
not — the recompress block (`main.rs:969`) contains no `dry_run` check — but the
dry-run branch at `main.rs:940` **returns at line 953**, before recompress is
reached. It prints candidate partitions and file counts and mutates nothing.
(Caught by reading rather than assuming; noted because the recompress block in
isolation genuinely does look unguarded.)

**And the reason this works when the coordinator cannot: the CLI has no 900 s
deadline.** Every failure in this document traces to work that does not fit a
fixed budget — day-wide units unsatisfiable against 900 s, components too large
for a bin, sorts that exceed a pool slice. `optimize --recompress` is an
operator-run, unbounded job. **It can do exactly the day-wide work the coordinator
is structurally unable to do**, which is why the escape hatch exists and why it is
the right tool for a backlog that accumulated precisely because nothing bounded
could touch it.

**Expect it to be slow, and size it before running.** An 85 GiB sorted rewrite is
roughly 400x the 211 MB / 2 M-row bin benchmarked at 1.1–2.7 s, so order tens of
minutes of sort plus object-store IO for the whole cell. That is fine for an
operator job and impossible for a 900 s unit — which is the entire point.

## What this means for a 100x customer, specifically

The brief's question named a prospect at ~100x current total volume. Everything
above is about *today's* cost; this is the scaling shape, and it follows from the
same arithmetic rather than from any new assumption.

**Today, maintenance cost is `data x bins_per_file`.** A file is read once per bin
it overlaps, so the total sweep read is not proportional to the data — it is
proportional to the data times how many bins each file straddles. Measured
fleet-wide, `bins_per_file` is **27.8 mean** at a 10-minute bin.

**And `bins_per_file` is not a constant — it GROWS with volume**, for two
measured reasons:

1. **Compaction widens files as it merges**, and more data means more merging.
   The correlation is already visible: 1,657-file partitions are 0.7 % wide,
   22-file partitions are 72.7 % wide.
2. **Components grow by chain-overlap.** One file spanning two windows welds them,
   and everything in either window then joins. Higher ingest welds faster.

**So maintenance cost today is SUPER-LINEAR in volume.** That is the precise
statement of "it will not hold at 10x", and it is why the frozen mass grows
faster than the fleet does.

**With span-bounded output, it becomes linear.** If a file never spans more than
one bin, `bins_per_file = 1` by construction and the cost collapses to `data` —
one read per byte per sweep. Nothing else in this document changes that exponent;
the pool size does not, the scheduler does not, the certification design does
not. **Only the file-to-bin ratio sits in the exponent.**

**That is the honest answer to the 100x question:** not "we need N times more
machines", but **"the cost currently scales worse than the data, and one bounded
change moves it to scaling with the data."** A 100x tenant on today's layout does
not need 100x the maintenance capacity — it needs considerably more than that,
and the multiplier is not knowable in advance because it depends on how the
tenant's writes weld. On a span-bounded layout, 100x the data is 100x the
maintenance, which is a capacity question rather than an architectural one.

**Caveat, and it is the same one throughout:** `bins_per_file = 1` is the ideal.
Real output will not be perfect, and the residual depends on how tightly the
writer's byte cut aligns with bin boundaries — the tension quantified in the
bin-widening section. The claim here is about the exponent, not the constant.

### Operational notes for running `--recompress` against prod

Checked, because "run this command against production" deserves more than a
recommendation.

**It does NOT start maintenance workers.** `run_optimize_cli` builds its Database
with `Database::with_config`, and `start_maintenance_schedulers()` is called
separately from `bootstrap` (`server/mod.rs:126`). So the CLI will not begin
competing for claims, and will not run the coordinator.

**It DOES load and rewrite a task journal** (`database/mod.rs:3431-3435`:
`TaskJournal::load` then `requeue_running` then `checkpoint`). That journal lives
at `cfg.core.timefusion_data_dir`, which is **local**, so from a laptop or a
runner it touches a local, empty journal and cannot disturb prod's. **Do not run
this with `TIMEFUSION_DATA_DIR` pointed at the prod volume.**

**The real hazard is Delta commit contention.** `recompress_partition` performs a
`replace_where` over the partition, and prod's live maintenance may be compacting
or deduping the same cell at the same time. One of the two will lose its commit.
That is not corruption — Delta's conditional put is what makes it safe — but it
means a long recompress can be wasted work if the coordinator lands first, and
the whale cells are exactly the ones the coordinator keeps claiming.

**So the sequencing that avoids the waste:** pick a cell the coordinator is not
currently working (the funnel logs which cells are claimed), or accept the risk
on the first attempt and watch for a commit failure. The night's own evidence
says the odds are reasonable — those cells are claimed often but complete rarely,
which is the whole problem.

**And prefer to run it where the bytes are.** The read is object-store-bound and
an 85 GiB cell is a lot of transfer; a runner in the same region as the bucket
will finish in a fraction of the time a laptop would. My own measurement of
3.9 MiB/s from here — explicitly not a prod number — is what that difference
looks like.

## Three selection designs refuted. The lever is provably NOT in selection.

The packer sorts candidates by SIZE. A natural idea is to sort by event time
instead, so a unit picks a contiguous run and its output spans only that run —
same bytes, same file count, narrower output, no threshold to choose. Simulated
over the 119 cells that actually have selectable files:

| ordering | mean bins spanned | p50 | p90 |
|---|---:|---:|---:|
| smallest-first (today) | **128.2** | 145.0 | 145.0 |
| time-adjacent | **128.1** | 145.0 | 145.0 |

**Identical.** 9 % of cells improve by >10 %, 6 % get worse. Noise.

**And the p50 explains why: 145 bins is the whole day** (a day is 144
ten-minute bins). **The under-target files are individually scattered across the
entire day**, so *any* subset of them spans it. Ordering cannot narrow an output
whose inputs are spread end to end.

That is now three selection designs refuted by measurement in one night:

| design | result |
|---|---|
| time-RANGED unit selection | 1,405 vs 1,407 overlapping pairs — identical |
| splitting compaction for oversized components | those files are never selected at all (`size >= target` skip) |
| time-ADJACENT ordering | 128.1 vs 128.2 bins — identical |

**So the conclusion is a negative one, and it is worth as much as a positive:
the output-span problem cannot be fixed by choosing WHICH files a unit merges.**
It has to come from one of exactly two places:

1. **The WRITER'S CUT** — cut output at bin boundaries, not only at
   `max_file_bytes`. A 200 MiB merge of day-scattered inputs currently produces
   ONE file spanning the day; cutting on bin boundaries would produce several
   narrow ones. **Cost: more files** — the fragmentation the packer exists to
   remove, and the same tension as bin widening, but now located precisely.
2. **The INPUTS being narrow already** — which is what
   `optimize --recompress` achieves in one pass, and why it remains the top
   recommendation.

**This is why I stopped proposing selection changes.** Three attempts, three
refutations, each cheap because the harnesses were already written — and together
they localise the defect to the writer, which no amount of scheduling work would
have reached.

## The writer-side fix is refuted too — and that leaves exactly one lever

The three selection refutations localised the defect to the writer's cut. So:
what if the writer cut output at bin boundaries as well as at `max_file_bytes`?
Simulated over the 123 merges the packer would actually perform:

```
IF the writer cut at BIN boundaries, each merge would emit:
  output files: mean 126   p50 144   p90 144
  avg output size: mean 1.42 MiB   p50 1.44 MiB

today each merge emits ONE file of up to 256 MiB.
```

**~144 files of ~1.4 MiB each, per merge.** That is catastrophic fragmentation —
precisely what compaction exists to prevent, and it would multiply the file count
the query planner and the packer both pay for. **Refuted.**

### Four designs eliminated; one survives, and the reason is structural

| design | verdict |
|---|---|
| time-RANGED unit selection | identical (1,405 vs 1,407 overlapping pairs) |
| splitting compaction for oversized components | those files are never selected (`size >= target`) |
| time-ADJACENT ordering | identical (128.1 vs 128.2 bins) |
| **bin-boundary cutting in the writer** | **144 files of 1.4 MiB per merge** |
| **`optimize --recompress`** | **~170 files of ~8.5 min each — works** |

**Why the survivor is different, stated precisely:** the four failures are all
*partial* operations over a *scattered* subset. A merge of files spread across a
whole day produces a day-spanning output no matter how you choose or cut it —
narrow output requires narrow input, and the input is scattered by construction.

**`--recompress` is the only one that rewrites the ENTIRE partition, sorted.**
That is what breaks the circularity: with all the data present and re-sorted by
timestamp, the writer's ordinary 512 MiB cut lands on a contiguous stream and
every output is narrow automatically. The scatter of the inputs stops mattering
because nothing is left out.

**So the recommendation is not a preference among options — it is the only option
that survived measurement**, and the four that did not are recorded above so
nobody spends a day rediscovering them. Each took minutes to refute once the
harness existed; the harnesses are in `scratchpad/` and take a partition path.

## CORRECTION: `--recompress --project` is DISABLED. Drop the flag.

The command I recommended would have failed on first use. `recompress_partition`
begins (`compact.rs:678-681`):

```rust
if project.is_some() {
    // Scoped `replace_where` can deadlock; reject it before reading the table.
    anyhow::bail!("recompress --project is disabled: scoped replace_where deadlocks; re-run without --project");
}
```

**And the `main.rs` comment I quoted to justify the flag is STALE.** It says
"`--project` narrows the overwrite predicate … which is what makes the job small
enough to run on an ordinary runner" — describing a capability that was later
disabled for deadlocking. **That is the second time tonight a comment in this
codebase overstated what the code does** (the first being `mergeable`'s "the same
test the packer applies"). Checking rather than quoting is the reason this was
caught before it reached an operator.

**The correct command drops the flag:**

```
timefusion optimize --date 2026-07-22 --recompress --dry-run
```

**And the scope penalty is negligible, because the whale IS those dates:**

| date | whole date | whale cell alone | other 13 projects |
|---|---:|---:|---:|
| 2026-07-22 | 86.0 GiB | 85.0 GiB | ~1.0 GiB |
| 2026-07-24 | 87.9 GiB | 87.0 GiB | ~0.9 GiB |

Six whole dates are **445.7 GiB** against **438 GiB** for the six whale cells —
a **1.8 %** increase in bytes rewritten. The tenant that caused the problem is
~99 % of the data on those dates.

**What it does change is blast radius, not cost.** Dropping `--project` means the
rewrite touches 13 other tenants' data for those dates — a small number of bytes,
but their files are replaced too, so their Delta history gains a version and any
concurrent maintenance on them can lose a commit. That is a fact to state before
running, not a reason not to.

**So the recommendation survives, with the flag removed and the blast radius
named.** The one-line lesson is the same as everywhere in this document: the
comment described intent, the code described behaviour, and only one of them was
checkable.

## LIVE CONFIRMATION: `compaction_unit_span` in prod, within minutes

The accidental deploy put `compaction_unit_span` into production hours earlier
than planned. It confirms the entire theory immediately, with real units:

```
operation             table                 date        files  span      bins
HotPacking            otel_logs_and_spans   2026-09-04     10  4,638s      8
SealedConsolidation   otel_metrics          2026-08-24      9  15,895s    27
HotPacking            otel_logs_and_spans   2026-09-04      2  9,075s     16
SealedConsolidation   otel_metrics          2026-08-16      2  62,911s   105
SealedConsolidation   otel_logs_and_spans   2026-08-24      3  50,102s    84
SealedConsolidation   otel_logs_and_spans   2026-08-24      2  70,915s   119
HotPacking            otel_logs_and_spans   2026-09-04      3  455s        1
```

**Merging TWO files routinely produces an output spanning 105-119 bins — 17 to
20 hours.** Not a pathological case; those are ordinary `SealedConsolidation`
units, on both tables, minutes apart.

**Every prediction holds:**

- **Tiny merges produce day-spanning outputs.** Two files → 119 bins. The unit
  retires one file and creates an object that 119 future dedup bins must each
  read in full.
- **The count says nothing about the span.** 10 files → 8 bins; 2 files → 119.
  The packer's only measures are count and bytes, and neither is even correlated
  with the cost the output imposes.
- **Sealed work is far worse than hot work**, exactly as the age analysis said:
  today's `HotPacking` units span 1-16 bins; `SealedConsolidation` on August
  dates spans 84-119.
- **And the instrument is not merely printing large numbers** — `3 files → 455s
  → 1 bin` is in the same window. When the inputs happen to be adjacent, the
  output is local. That single line is what makes the others trustworthy.

**This is the measurement that was missing.** It cost four lines around an
existing loop, and it makes the defect visible continuously rather than through a
night of statistics archaeology. A span budget in candidate selection can now be
chosen from a histogram instead of a guess — and the histogram says a bound
anywhere near a handful of bins would reject most of what compaction currently
does.

**Worth stating plainly:** these units are running right now, and each one is
making the dedup lane's next pass more expensive. The `--recompress` cleanup
addresses the accumulated damage; only a span bound stops it accumulating.

## The span histogram, from live prod — and a candidate policy

**24 samples over 60 minutes** (updated from an earlier n=14, which said the same
thing less sharply):

```
bins spanned: p10 3   p50 16   p90 105   max 144   mean 37

                     n     p50   p90   max    <= 16 bins
HotPacking          13      16    16    16       100 %
SealedConsolidation 11      74   119   144         9 %

otel_logs_and_spans 20      16     -   144
otel_metrics         4      92     -   105

a 16-bin bound would reject 10 of 24 units — essentially exactly the
SealedConsolidation ones.
```

**`max = 144` is a full day** (144 ten-minute bins): one sealed unit produced an
output spanning its entire partition.

**The two lanes separate almost perfectly at 16 bins, and more data sharpened it
rather than blurring it.** **100 % of `HotPacking` units are ≤16 bins (13 of 13);
91 % of `SealedConsolidation` units are above it (10 of 11).**
So a **16-bin span bound would leave hot packing entirely untouched and reject
essentially all sealed consolidation.**

**That is a much sharper policy than "bound the span at X" in the abstract, and
its implication is uncomfortable and worth facing:** such a bound would
effectively **disable `SealedConsolidation`**, because — per the three selection
refutations — that lane cannot choose narrower inputs. Its files are scattered
across the day, so any merge it makes spans the day.

**And that may well be correct.** `SealedConsolidation` exists to reduce file
count on sealed dates. Measured tonight, its outputs span 92-119 bins, and each
such output is then read by 92-119 dedup bins in full. **The lane is trading a
small file-count win for a very large read-amplification loss**, and nothing in
the system was accounting for the second half of that trade.

**What would settle it** — and what I am explicitly not concluding from n=6:
1. more samples still, over several processes — n=24 in one hour on one process
   is enough to see the separation but not to set a production constant;
2. the per-unit cost decomposition from `prep/unit-phase-timers`, to price the
   file-count win against the read-amplification loss in the same units;
3. whether sealed dates still *need* consolidation once `--recompress` has
   rewritten them, since a recompressed cell is already at target sizes.

**Note the third point may dissolve the question entirely.** If the cleanup pass
leaves cells with correctly-sized, narrow files, `SealedConsolidation` would have
nothing to do on them — and the lane's cost would fall to zero without any policy
change at all.

## THREE HOURS OF COMPACTION REDUCED THE COST BY 0.007 %

The same statistics-only measurement, taken **three hours apart**, across a period
in which the maintenance lanes ran continuously:

| | 3 hours ago | now | change |
|---|---:|---:|---|
| live files | 8,122 | **7,485** | **−637 (−7.8 %)** |
| wide files (>50 % of a day) | 1,273 | **1,275** | **+2** |
| wide as a share | 15.7 % | **17.0 %** | **worse** |
| **total sweep read at 10-min bins** | **44,130 GiB** | **44,127 GiB** | **−3 GiB (−0.007 %)** |

**Compaction retired 637 files in three hours and reduced the cost that actually
matters by three gigabytes out of forty-four thousand.**

This is the whole investigation in one table:

- **The lane is working.** 7.8 % of the table's files are gone. By its own
  metric — file count — it had a good three hours.
- **The cost it exists to reduce did not move.** Sweep read is flat to four
  significant figures.
- **The wide-file population grew slightly**, and its *share* rose from 15.7 % to
  17.0 %, because the files being retired are the narrow ones. **Compaction is
  concentrating the table toward exactly the shape that costs the most.**

**Every earlier finding predicted this**, and it is worth noting that they were
derived independently: `span x size` concentration said the cost lives in a few
wide files, so retiring 637 narrow ones could not move it; the live
`compaction_unit_span` histogram said sealed units emit 74–144-bin outputs, so the
lane replaces narrow files with wide ones; and the age analysis said heavily
compacted partitions are the most expensive. **This table is those three
predictions arriving together in a single before/after.**

**And it is the clearest possible statement of the 10x problem.** More volume
means more compaction, and more compaction means more of this: a lane that
improves its own metric by 7.8 % per three hours while the maintenance cost it
drives stays exactly where it was. **Scaling the fleet scales the wrong number.**

## Projected payoff: rewrite 85 GiB once, cut fleet maintenance read 8.5 %

Modelling `--recompress` on the worst cells — the whole cell sorted and cut at
512 MiB, so each output is contiguous and sub-bin — against today's 44,127 GiB
fleet sweep read:

| recompress top N cells | GiB rewritten (one-off) | fleet sweep after | reduction |
|---:|---:|---:|---:|
| **1** | **85 G** | 40,379 G | **8.5 %** |
| 3 | 208 G | 37,198 G | 15.7 % |
| **6** | **364 G** | 33,654 G | **23.7 %** |
| 10 | 605 G | 29,605 G | 32.9 % |
| 20 | 732 G | 26,309 G | **40.4 %** |
| 50 | 768 G | 24,008 G | 45.6 % |

**One cell — one command, 85 GiB rewritten once — takes 8.5 % off the fleet's
entire maintenance read cost, permanently.** Six cells take off 23.7 %. Twenty
cells take off 40.4 %.

**And note the cost column flattens: 20 cells cost 732 GiB, 50 cells cost 768 GiB
— thirty more cells for thirty-six more gigabytes**, because the tail is small.
So the natural stopping point is around 20 cells: past that, effort rises and
return does not.

**Compare against the measured alternative.** Three hours of the maintenance
lanes running flat out moved this number by **0.007 %**. One `--recompress` of a
single cell moves it by **8.5 %** — roughly **1,200x the effect of everything
compaction did in three hours**, for a bounded, one-off cost.

**Caveats, all stated earlier and none of them dissolved:**
- the model assumes recompress output is contiguous and sub-bin, which follows
  from the sorted rewrite plus the 512 MiB cut but has not been observed;
- `--recompress` cost on an 85 GiB cell is unmeasured (built for footer repair on
  ordinary partitions);
- it will contend with live maintenance on the same partition;
- and without a span bound on `SealedConsolidation`, the gain re-accumulates.

**But the first move is now unambiguous, small, and measurable**: one command,
one cell, and the same statistics script re-run afterwards to confirm 8.5 %.

### The payoff model is robust to bursty data — the obvious objection does not bite

The projection assumes a recompressed cell's 512 MiB pieces each span roughly
`cell_span / N`, which for the whale is 24 h / 170 ≈ **8.5 minutes — under one
bin**. The obvious objection is that ingest is bursty, so pieces covering quiet
periods would span far longer than the average and cross many bins.

**That objection does not bite, and the reason is worth stating because it also
explains why the current layout is so bad.** A file's sweep cost is the number of
bins it overlaps **that contain data** — a bin with nothing in it is never dirty
and never rewritten. So a piece spanning a quiet gap costs nothing for the gap.

Concretely: a 512 MiB piece contains 512 MiB of rows *whatever* the wall-clock
gap between them. Because pieces are cut on a **sorted** stream, consecutive
pieces are time-disjoint, and each covers exactly the bins its own rows occupy.
Burstiness changes *which* bins a piece covers, not *how many have data in it*.

**And that is precisely the property today's files lack.** A wide file is not
wide because its rows are spread thinly — it is wide because it was *merged from
inputs drawn from across the day*, so it genuinely holds rows in 100+ separate
bins. The cost is real there and illusory in the bursty-data objection, and the
difference is exactly "sorted contiguous output" versus "union of scattered
inputs".

So the sensitivity that remains is not burstiness. It is the two already stated:
whether `--recompress` really emits contiguous sub-bin pieces (follows from the
code, unobserved in prod), and its wall-clock cost on an 85 GiB cell.

### Verified locally on real data — and the sub-bin claim depends on DENSITY

Tested the "sorted rewrite + byte cut = narrow pieces" assumption on the real
201.5 MiB / 2,023,604-row file `875ea2a1` produced, with whatever burstiness prod
actually has:

```
file spans 405 min = 41 bins

pieces   size each   bins/piece (min/p50/max)   sweep cost
     1     202 MiB          41 / 41 / 41         8,282 MiB
     2     101 MiB          19 / 23 / 23         4,242 MiB
     3      67 MiB          12 / 14 / 16         2,814 MiB
```

**Two findings, and the second corrects my projection's wording.**

**1. The mechanism works: cost falls roughly as 1/N.** Cutting the sorted stream
into N pieces cuts sweep cost by about N (8,282 → 4,242 → 2,814 MiB). Each piece
is read only by the bins its own rows occupy. That is the whole thesis, verified
on real data.

**2. But the pieces here are NOT sub-bin, and my payoff model said they would
be.** Three 67 MiB pieces still span 12–16 bins each. **The sub-bin property is a
function of DENSITY, not of the 512 MiB cut**: this file holds ~30 MiB per hour,
so 512 MiB of it covers ~17 hours.

**Why the payoff table is nevertheless sound: it targets only the densest cells.**
The whale cell is 85 GiB in a day — about **3.5 GiB/hour, ~118x denser than this
test file** — so 512 MiB of it is **~8.7 minutes**, genuinely sub-bin. The
projection's assumption holds for exactly the cells it recompresses and would
*not* hold for a sparse cell.

**So the correct general statement is:** `--recompress` reduces sweep cost by
roughly `pieces_produced`, i.e. by `cell_bytes / 512 MiB` — which is large
precisely where the cell is large. On the whale that is ~170x and the sub-bin
floor caps it at the cell's byte size; on a sparse cell it is a handful of x.
**The lever scales with the problem, which is the property you want, but the
"~1x amplification after" claim is specific to dense cells.**

**Checked: the payoff MODEL was already density-aware; only my prose was loose.**
`sweep_after` computes `span = (hi-lo)/N` per piece and floors the bin count at 1
(`(span//BIN)+1`) — so a sparse cell's pieces are correctly charged for the bins
they really span, and only a dense cell's collapse to one. **The 8.5 % / 23.7 % /
40.4 % figures therefore stand as computed**; what needed correcting was my
description of *why* ("pieces are sub-bin", true only for dense cells), not the
arithmetic. Worth stating because the distinction decides whether the table needs
re-running: it does not.

One residual approximation remains, and it is small: the model divides a cell's
span evenly across its pieces, whereas equal-BYTE pieces of a bursty cell span
unequal time. The estimate is therefore an average. Since empty bins cost
nothing (above), the error is bounded and does not favour the recommendation.

## The in-region blocker has a small fix: expose recompress the way OPTIMIZE is

`recompress_partition` has exactly one caller — `main.rs:973`, the CLI. pgwire's
intercepted `OPTIMIZE <table> WHERE date = '…'` (`server/mod.rs:465`) routes to
`compact_date`, the bin-packing path, which **skips over-target files** and
therefore cannot touch the wide ones. **So there is no in-region way to run the
night's top recommendation today.**

That is the entire blocker, and it is smaller than it looks. **The admin-command
interception already exists** — `OPTIMIZE <table> WHERE date = '…'` is parsed
(`server/mod.rs:810`), routed, and returns a tag. A `RECOMPRESS <table> WHERE
date = '…'` alongside it would:

- run **inside the prod process**, which is already in-region, so the object-store
  read is at datacenter speed rather than the **3.9 MiB/s** a laptop sees;
- reuse the maintenance-rewrite permit `recompress_partition` already acquires,
  so it queues against the existing lanes instead of competing blindly;
- need no new machinery — the operation, its permit, and the command-interception
  pattern all exist.

**The trade to be explicit about:** it does the 85 GiB rewrite *inside* the
serving process, on the same memory pool and the same permits as live
maintenance. That is a real cost, and it is why this belongs behind an explicit
admin command rather than the coordinator: an operator chooses when to pay it,
which is exactly the property `--recompress` already has and the coordinator's
900 s units cannot.

**Recommended order, then:**
1. Add `RECOMPRESS … WHERE date = …` as an intercepted admin command (small; the
   pattern is `server/mod.rs:465` and `:810`).
2. Run it on the worst cell, watch `compaction_unit_span` and re-run
   `wide_rank.py` — the 8.5 % is checkable within one pass.
3. Only then decide on the span budget, with the cost of a rewrite now known.

**This also removes the last dependency on an in-region runner**, which was the
only thing standing between the analysis and an experiment.

### RETRACTED, one paragraph later: in-process RECOMPRESS is the shape that OOM'd prod

The `OptimizeCmd` struct carries the reason my proposal will not work
(`server/mod.rs:772-778`):

> "Restrict the compaction to one tenant's partition. **A whole-date optimize
> spans every project's files for that date — tens of GB on a busy day, which
> doesn't fit in-process next to serving load (2026-07-27: two OOMs).** One
> (project, date) partition is a few GB."

**So `OPTIMIZE` requires a project scope precisely because whole-date in-process
work killed the instance twice.** And `--recompress` **cannot be project-scoped**
— `compact.rs:678` rejects it, because scoped `replace_where` deadlocks.

**Those two constraints are mutually exclusive.** An in-process `RECOMPRESS`
would be whole-date by construction, at **85 GiB** — several times the "tens of
GB" that already caused two OOMs. **The proposal is dead, and it is the fifth
design refuted tonight, this time by an incident recorded in a struct comment.**

**So the in-region runner requirement stands after all.** The work must happen
outside the serving process:

- **a same-region runner or container** executing `timefusion optimize --date … --recompress`
  against prod storage, with `TIMEFUSION_DATA_DIR` local — this is the only route
  that exists today, and the infrastructure question is real rather than
  avoidable;
- or, if someone wants it in-process later, **first fix the reason recompress
  cannot be project-scoped** (the `replace_where` deadlock), which would bring an
  85 GiB job down to the few-GB partitions `OPTIMIZE` already tolerates. That is
  a delta-rs/`replace_where` investigation, not a compaction one.

**Two paragraphs, one proposal, one retraction — and the retraction came from
reading eight lines further into the same file.** That is the ninth or tenth time
tonight that the answer was already written down next to the thing I was
proposing to change.

### What the `replace_where` deadlock would take to fix (not attempted)

Since that deadlock is now the gate on ever running this in-process, here is what
is established about it, and what is not.

**The write is (`compact.rs:826-838`):**

```rust
let input_plan = ctx.sql("SELECT * FROM recompress_src WHERE date = '…' ORDER BY …")…;
let replace_pred = format!("date = '{date_str}'");
table.write(vec![])
    .with_input_plan(input_plan)
    .with_save_mode(SaveMode::Overwrite)
    .with_replace_where(replace_pred)…
```

**Project-scoping changes only the two predicates** — both would gain
`AND project_id = '…'`. Nothing else about the call differs.

**The plausible mechanism, which I have NOT verified:** the input plan reads the
same files `replace_where` is overwriting, on a runtime with a bounded thread
pool and a maintenance memory pool. A narrower predicate changes which files
delta-rs must open to evaluate the overwrite, and self-contention between the
scan and the overwrite on a bounded pool is the classic shape of this hang.
**That is a hypothesis from the call shape, nothing more** — the comment says
"can deadlock", which is someone's hard-won experience, not a diagnosis I can
reconstruct.

**The experiment that would settle it costs nothing and needs no prod:** the
sqllogictest/e2e harness already stands up local MinIO. Write one test that
recompresses a two-project partition with a project-scoped predicate, run it
under a small `target_partitions`, and see whether it hangs. If it does, the
stack trace names the deadlock; if it does not, the restriction may be
obsolete — the comment carries no date, and several things in this file have
been rewritten since.

**Worth doing, because the payoff is disproportionate:** project-scoping turns an
85 GiB whole-date job into a few-GB partition job, which is the size `OPTIMIZE`
already runs in-process. That single fix would collapse the infrastructure
dependency, the OOM risk, and the blast radius all at once — the three objections
that killed the in-process route above.

### EXPERIMENT RUN: the scoped-recompress deadlock does not reproduce locally

I ran the experiment rather than leaving it as a suggestion. Branch
`scratch/replace-where-deadlock` (**DO NOT MERGE** — it lifts the guard) removes
the `project.is_some()` bail and scopes **both** predicates, the input plan and
`replace_where`, by project. Two tests:

| case | result |
|---|---|
| minimal: 2 projects, 80 rows, 2 files | `Rewritten { files: 2 }` in **1.2 s** |
| contention: 12 files/project + 12 concurrent write batches into the same partition, mid-overwrite | `Rewritten { files: 12 }` in **3.5 s** |

**No hang in either, and the second project's rows survived the scoped
overwrite** — the correctness property a wrong `replace_where` would silently
destroy.

**This does NOT prove the restriction is obsolete.** Local MinIO has none of the
real object-store latency, the held maintenance-rewrite permit, or the data scale
that a deadlock plausibly needs. **What it does establish is that the failure is
not unconditional**, which changes the daylight task from "investigate a
mysterious deadlock" into "try it on staging with a timeout and watch".

**One methodological note, because it nearly cost the whole experiment:** the
first version of the test passed in 0.9 s while proving *nothing* — it returned
`Skipped("already at target tier and every footer is sorted")`, so the
`replace_where` write never ran. A green tick on a test that never reaches the
code under test is the most expensive kind of false confidence. It is fixed by
passing a different zstd level, and the test now **asserts `Rewritten`** so it
cannot silently regress to proving nothing.

**Why this matters more than it looks:** if scoping is safe, an 85 GiB whole-date
rewrite becomes a few-GB partition rewrite — the size `OPTIMIZE` already runs
in-process. **That one change would collapse the in-region runner requirement,
the OOM risk and the blast radius simultaneously**, and it is the cheapest path
from tonight's analysis to something actually running.

**Why I could not take the deadlock experiment further tonight.** The one
condition local MinIO cannot supply is real object-store latency, and the
obvious place to get it is staging. But **there is no `.env.staging` in this
checkout** (`.env`, `.env.prod`, `.env.ovh`, `.env.minio`, `.env.cloudflare`,
`.env.test` — no staging), and `CLAUDE.md`'s description of it ("`timefusion-staging`,
same **R2**, scratch prefix") predates the R2→OVH migration, so it may be stale
as well as unconfigured. I was not going to guess at credentials for a write
test.

**So the remaining step needs someone with staging access or an in-region
runner** — the same dependency as the cleanup itself, which is convenient: one
environment unblocks both. The test to run there is exactly
`scratch/replace-where-deadlock-v2`'s second case, which already exercises
concurrent writes into the partition being overwritten.
