# Certification cannot converge, and that is why dedup buys nothing

Measured on prod 2026-09-01 (deploy 13), one process:

```
dedup_eligible                  9,296   scans that COULD have skipped DedupExec
dedup_skipped                       0   scans that did
dedup_skipped_per_date              0
dedup_skipped_per_file              0
dedup_denied_never_certified    8,767   = 100.0% of denials
cert_granted_total                  0
cert_slice_partial                106   slices banked toward a day
cert_slice_dirty                   11
cert_slice_day_covered              0   days ever completed
```

Dedup is **76% of the maintenance fleet** (`work.Dedup.worker_secs` 9,084 of
11,899 recorded in 25 min). Its two products are duplicates removed — 3,519 rows,
a 0.18% rate on the live frontier — and certifications, of which there are none.
`cert_granted_total` has been 0 since 2026-08-20 and this is not a throughput
problem: **the grant rule cannot be satisfied by a live table.**

## Why the rule cannot be satisfied

`record_clean_slice` (`maintain.rs:4722`) banks a slice toward a day only if
**all** of:

1. the pass dropped **zero** rows, and
2. the partition's file fingerprint is **unchanged** across the pass
   (`partition_file_fp(pre) == partition_file_fp(post)`), and
3. the accumulated intervals then span the **entire UTC day**
   (`s <= day_start && e >= day_end`).

Any failure of (1) or (2) **wipes the day's accumulated evidence**
(`dedup_slice_coverage.remove(&key)`), and a fingerprint change resets the
interval list to just the current slice.

Three independent reasons that never converges:

- **Success destroys the evidence.** A slice that finds duplicates — dedup doing
  its job — trips (1) and erases the day. On a partition with any duplicates at
  all, the counter resets exactly when work happens.
- **Every other lane moves the fingerprint.** Ingest flushes, hot-tail packing,
  sealed consolidation, repair and the rollup chain all commit files into
  `(project, date)`. Condition (2) requires that **no file changes anywhere in
  the partition** for as long as it takes to prove the day. The code comment at
  `certified_files_in_partition` already concedes this: *"Recent partitions gain
  files continuously … in prod that question is `false` essentially always."*
- **Nothing ever probes a whole day.** The coordinator enqueues dedup units where
  duplicates are *suspected*, not all 144 ten-minute slices of a day. Condition
  (3) asks for full-day interval coverage that the scheduler has no reason to
  produce. Measured: 106 partial slices banked, 0 days covered.

## The consumers are already built — and both are downstream of the grant

The read path has **two** skip mechanisms, and neither is the problem:

- **per-DATE** (`per_date_dates`) — shipped and prod-exposed.
- **per-FILE** (`certified_file_split`, `mod.rs:9350`) — a finer split that runs
  certified files as a second leg unioned ABOVE `DedupExec`. It exists precisely
  to survive a partition gaining files, and its doc comment names the 100%
  `never_certified` measurement as its motivation.

But `certified_files_in_partition` starts with
`let Some(cert) = self.dedup_clean_fp.get(&key) else { return HashSet::new() }`.
**The per-file path reads the day-level grant.** With `cert_granted_total = 0`,
`dedup_clean_fp` is empty, so turning on `timefusion_read_dedup_skip_per_file`
today would change nothing. The finer consumer cannot rescue an empty producer.

## The direction the evidence points

A clean slice over an unmoved fingerprint **already proves something durable**:
the files it read hold no duplicates within that slice. Delta files are
immutable — a rewrite produces new paths — so file-level evidence is monotone and
cannot be invalidated by unrelated activity in the partition. That is exactly the
input `certified_files_in_partition` wants, and it is what the producer currently
throws away in favour of an all-or-nothing day.

So: **grant at the granularity that is actually proven (files), instead of
accumulating toward a day that a live table will never hold still for.**

## Why this was not implemented tonight

The read path is the risk surface. `timefusion_read_dedup_skip_per_file` is
default OFF with an explicit warning — *"the failure mode is a silent over-count
on every dashboard tile"* — and the soundness argument
(`read::skippable_certified_files`) has to hold under a file that overlaps a
certified one, inclusive bounds, and missing statistics. Changing what
certification MEANS at 02:00, against a read path whose failure mode is silent
wrong answers, is how the 2026-05-28 dual-write incidents happened.

The correct sequence:

1. **Verify the claim cheaply first.** `cert_slice_day_covered = 0` with
   `cert_slice_partial = 106` is strong, but add a counter for *which* of the
   three conditions rejected each slice — `dropped != 0`, fingerprint moved, or
   still partial. That is one `match` in `record_clean_slice` and it turns this
   document's argument into a measurement. (Same lesson as
   `tf_prefilter_label_hid_four_refusals_2026-08-23`: one label hid four
   different refusals.)
2. **Then make the producer emit file-level evidence**, keeping the day rule as
   the degenerate case, behind the existing flag.
3. **Validate by diffing `count(*)` with the flag on and off over a CHURNING
   partition**, which is what the flag's own doc comment demands.
4. Only then consider whether dedup still deserves 76% of the fleet — with
   certification actually landing, the same worker-seconds buy a read-path win
   instead of nothing.

## What this does NOT say

It does not say dedup is wasted work. Removing duplicates is load-bearing for
correctness regardless of certification, and the 0.18% live-frontier rate is one
25-minute window on a population the code notes may be duplicate-dense in the
backlog. It says the *second* product — the one that removes `DedupExec` from
every query plan, described in-tree as "the single largest term left in 30d query
latency" — is currently unreachable by construction.
