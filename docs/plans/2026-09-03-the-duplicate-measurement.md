# The duplicate measurement: the ingest-side lever is DEAD, and a number I nearly published was an artifact

**2026-09-03.** The gate on the proposed ingest-side dedup-key check was one
measurement: are duplicate groups **same-file** (both copies bucket-resident
together -> catchable at ingest) or **cross-file** (arrived after flush -> missed)?
Taken on the whale project's `date=2026-09-02` partition, complete — every file,
no sampling, so no pair can be missed.

## The answer: DEAD, twice over

On the **201 Delta-live files** (1,864,064 rows, 1,839,050 distinct dedup keys):

| | |
| --- | --- |
| physical rows / distinct keys | **1.014x** |
| duplicate groups | **25,011 = 1.36% of keys** |
| **SAME-FILE** (catchable at ingest) | 1,053 — **4.2%** |
| **CROSS-FILE** (missed at ingest) | 23,958 — **95.8%** |
| **EXACT duplicates (same `updated_at`)** | **0 — in BOTH categories** |

Two independent reasons the lever cannot work:

1. **95.8% are cross-file** — the second copy arrives after the bucket flushed, so
   an in-bucket check never sees it.
2. **There are no exact duplicates at all.** Every same-key group differs in
   `updated_at`, i.e. every one is a legitimate merge-on-read **version** produced
   by monoscope's enrichment. An exact-duplicate check would find nothing to
   drop, and a looser key-only check would **eat enrichment**.

**Do not build the ingest-side dedup check.** Not gated, not deferred — refuted.

## The artifact I nearly published

My first pass read all **625 parquet files present in the partition prefix** and
reported:

> 1,297,431 duplicate groups = **70.5% of keys**, 100% cross-file,
> 1,231,727 of them EXACT — i.e. the partition is ~3.2x physically duplicated.

**All of it was an artifact.** Only **201 of the 625 files are live**; the other
**424 are superseded files Delta has logically removed but VACUUM has not yet
deleted.** Reading them re-materialises every pre-compaction version of every row
and *manufactures* exactly the duplicates the number claimed to find.

The check that caught it: resolve the live set from the Delta checkpoint
(`_last_checkpoint` -> `NNNN.checkpoint.parquet`, `add` minus `remove`, then apply
post-checkpoint JSON commits) instead of trusting the object listing.

**Rule: a partition prefix is not a table.** Any measurement over raw S3 listings
must resolve the Delta live set first, or it measures history rather than state.

## Two things worth following up

1. **424 of 625 files (68%) in that prefix are dead.** That is retained garbage
   awaiting VACUUM, and it is not free: it is bucket bytes, and it inflates any
   tool that lists or scans the prefix (including, evidently, mine).
2. **Dedup's real job here is collapsing enrichment versions, not removing client
   duplicates** — 100% of live same-key groups are versions. That fits
   `tf_mor_breaks_time_disjoint_files_2026-08-02`: `version_append` is what makes
   `DedupExec` mandatory, and compaction is the only thing that retires the
   versions.

**Scope limit, stated plainly:** one project, one date, one point in time. The
58%-WAL-replay-manufactured duplicates measured on 2026-09-02 were from a
crash-era bin; exact duplicates clearly DO occur under unclean restarts. This
says they are absent from this partition **now** — which is precisely the regime
in which the ingest check was proposed to help, and it does not.
