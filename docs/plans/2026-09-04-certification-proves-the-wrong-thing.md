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
