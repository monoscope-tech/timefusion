# Plan: let a clean SLICE certify the files it proved

Implements the direction argued in `2026-09-01-certification-cannot-converge.md`.
Measured on prod deploy 13: `cert_granted_total = 0`, `dedup_skipped = 0` of
**9,296** eligible scans, `dedup_denied_never_certified = 100.0%`, while dedup
consumes **76%** of the maintenance fleet. `DedupExec` therefore survives in every
plan — described in-tree as "the single largest term left in 30d query latency",
and consistent with 14-day charts measuring 42.8 s warm vs 1.0 s at 24 h.

## The one-line diagnosis

Every consumer is built. **Only the producer is missing.**

`Certification { fp, since, files, stale }` already carries the per-file
evidence; `certified_files_in_partition` already intersects it with live files;
`read::skippable_certified_files` already enforces isolation; the read path
already runs certified files as a second leg unioned ABOVE `DedupExec`. All of it
is dead code because nothing ever inserts a `Certification`: the only insert site
requires whole-day coverage under an unchanged partition fingerprint, which a
live table never holds still for.

## What a clean slice actually proves

The dedup probe registers a provider over **every file of the `(project_id,
date)` partition** and filters it to the slice `[start, end)`. So when a pass
reports zero duplicate groups for that window, it has proved:

> every row in the partition whose `timestamp` lies in `[start, end)` is unique.

That is a statement about **rows in a time range**, not about files. It upgrades
to a statement about a file only when **the file's entire span lies inside the
slice** — then every row it holds was covered by the proof.

This is why the current code cannot simply keep its `post` file list: those files
generally extend beyond the slice. The containment test is the missing step, and
it is what makes slice-level certification sound.

## Soundness conditions (all required, fail closed)

A file `f` in partition `(project, table, date)` may be certified by a pass over
`[start, end)` only if:

1. `dropped == 0` — the pass removed nothing, so it found no duplicates;
2. `complete` — no chunk was skipped for being unsealed or over budget (an
   `Ok(0)` with skipped work proves nothing);
3. `fp(pre) == fp(post)` — no concurrent commit landed mid-pass, so the file set
   the probe read is the file set being vouched for;
4. **`f.span ⊆ [start, end)`** — the new condition. A file overlapping the
   boundary is NOT certified;
5. `f` has statistics from which a span can be derived — a missing-statistics
   file is excluded, never assumed contained.

6. **`timestamp` must be one of the schema's `dedup_keys`.** The upgrade from
   "rows in this window are unique" to "this file is clean" works only because a
   duplicate group shares one exact `timestamp`, so no group can straddle the
   slice boundary — which is true precisely when timestamp is a dedup key, the
   same predicate `stage_dedup_partition_range` already branches on. For a schema
   without it, a row inside the window could have a duplicate at a different
   timestamp in a file that does not overlap the certified one, so read-time
   isolation would not catch it. Fail closed for such schemas.

Read-time isolation (no uncertified file may overlap a certified one) is already
enforced by `read::skippable_certified_files` and is not weakened here.

### Three places the evidence can silently become a stronger claim than it is

These are the failure modes that make this change dangerous, and each needs code:

- **The persisted store restores `stale: false` unconditionally** (`mod.rs:3338`).
  Since prod restarts on every deploy, a slice-derived entry would come back
  indistinguishable from a whole-day grant and could then satisfy the
  whole-partition arm. **`stale` must round-trip through persistence.** Step 3's
  tightening is worthless without this.
- **The refusal path deletes the entry** (`record_certification` ends with
  `dedup_clean_fp.remove(&key)`), and `record_clean_slice`'s dirty path routes
  through it — so the first pass finding duplicates anywhere in the partition
  would erase every slice-proved file, recreating "success destroys the evidence"
  one level down, on the frontier where dirty passes are routine. It must
  **keep-and-mark-stale when the entry carries files**, the same semantics the
  flag-on arm of `dedup_window_certified` already uses. This is sound: a
  duplicate group touching a certified file forces that file's rewrite, and a
  path that is no longer live already drops out in
  `certified_files_in_partition`.
- **Merging must never downgrade.** Three cases, explicitly: entry absent →
  insert `stale: true`; entry already stale → union the file lists; entry
  **non-stale with a live fp → leave untouched**, or a slice pass would demote a
  real whole-day grant to a weak one.

## The change

1. **`record_clean_slice`**, on the clean-but-partial path (today: bump
   `CERT_SLICE_PARTIAL`, return `Ok(None)`) — additionally compute
   `partition_file_spans`, select files satisfying (4) and (5), and **merge**
   them into the entry's `files`. Merge, never replace: evidence from different
   slices is additive, and each file's proof stands on its own slice.
2. **Mark slice-derived entries `stale: true`.** `stale` already means exactly
   "cannot grant the whole-partition skip; the file list remains true of the
   files it names". This is what keeps the weaker proof from being read as the
   stronger one.
3. **Tighten the whole-partition arm to require `!cert.stale`** in
   `dedup_window_certified`. Today it matches on `cert.fp == partition_file_fp`
   alone, so a slice-derived entry whose fingerprint still matched would grant a
   day-wide skip it never proved. This is a correctness tightening independent of
   the rest.
4. **Count it.** `cert_slice_files_proved` (files added) and
   `cert_slice_files_unproven` (rejected for spanning the boundary or lacking
   statistics). Without the second, "no files certified" and "the containment
   test rejects everything" are the same observation — the
   `tf_prefilter_label_hid_four_refusals_2026-08-23` mistake.
5. **Persist** through the existing `persist_certifications` path, so a restart
   does not strand the evidence (the journal marks the slice Complete forever).

## What is deliberately NOT in this change

- **The read flag stays OFF.** `timefusion_read_dedup_skip_per_file` is default
  off with an explicit warning: *"the failure mode is a silent over-count on
  every dashboard tile"*. This change ships the PRODUCER only. Nothing about
  query results can move.
- **No change to the day rule.** Full-day coverage still grants the strong,
  non-stale certification exactly as today.
- **No capacity reallocation.** Dedup's 76% share is a separate decision that
  needs the where-do-the-seconds-go measurement, not this.

## Verification

1. **Unit, soundness:** a file overlapping the slice boundary must NOT be
   certified; a contained one must be; a file without statistics must not be.
2. **Unit, the tightening:** a `stale` certification whose `fp` matches must not
   grant the whole-partition skip.
3. **Integration:** the existing `dedup_compaction_test` family already asserts
   routed answers equal raw answers — the harness for step two, when the flag is
   eventually flipped.
4. **Prod, producer only:** `cert_slice_files_proved` climbs from zero while
   `dedup_skipped` stays 0 (flag off). That is the whole success criterion for
   this deploy: evidence accumulating where none could before.

   **Calibrate the expected magnitude before reading it.** A ten-minute slice can
   only certify files whose ENTIRE span fits in ten minutes — fresh flush files,
   mostly. Hot-packed multi-hour files need a coarsened clean pass wide enough to
   contain them, and sealed day-wide files still need the day rule. So a modest
   `cert_slice_files_proved` alongside a large `cert_slice_files_unproven` is the
   *expected* shape, not a failure; the ratio is the signal about which slice
   widths would pay.
5. **Only then**, a follow-up flips the flag after diffing `count(*)` with it on
   and off over a churning partition, as the flag's own doc comment demands.

## Why this ordering

The read path's failure mode is silent wrong answers on every dashboard tile.
Shipping the producer alone is unobservable to queries and makes the next step's
A/B possible; shipping both at once would mean debugging an over-count and an
empty evidence store at the same time.
