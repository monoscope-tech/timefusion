# Maintenance voids the proof it earns

Immediate follow-on from
`2026-09-03-the-hashes-pushdown-is-not-safe-and-already-exists.md`, which ended
by identifying **certification coverage** as the safe, already-built mechanism
that would push the customer's `hashes` predicate below the dedup. That was too
coarse. The counters name a sharper failure, and it is one we cause ourselves.

## The reading

Prod, `timefusion_stats`, `component='scan'`:

```
dedup_eligible                    1869
dedup_skipped                        0      dedup_skipped_pct  0.0
dedup_denied_uncertified          1869      <- 100% of eligible scans
  dedup_denied_fp_moved           1767      <- 94.5%
  dedup_denied_no_window            88
  dedup_denied_never_certified      12      (0.7%)
  dedup_denied_unresolved            2
cert_granted_total                   0
cert_slice_files_proved             32   vs  cert_slice_files_unproven  470
```

**The dedup skip fires zero times out of 1,869 eligible scans.** The safe
pushdown is not partially available — it is entirely unavailable, so every
`hashes` query pays the full above-dedup materialisation described in the
previous doc.

## The distinction that matters

`NeverCertified` and `FpMoved` are different failures, and the code is explicit
about it (`src/database/mod.rs:148`):

> "did anything certify this at all?" — `NeverCertified` = yes, `FpMoved` = no
> (**written since certified**)

Only **0.7 %** of denials are "nobody ever proved this date clean". **94.5 % are
"somebody proved it, and then the partition changed."** So certification is *not*
failing to run, and coverage is *not* simply absent. Proofs are being produced
and then invalidated.

## Why they are invalidated — the fingerprint is the file set

`PartitionStats::fingerprint` (`src/database/mod.rs:2025-2032`) hashes the
partition's **file list**. Any change to that list moves it. That includes new
ingest, and it equally includes **every compaction, hot-tail pack, dedup rewrite
and sealed consolidation we run** — all of which exist precisely to rewrite files.

So the two halves of the system are in direct opposition:

- **Certification** proves "this partition's files contain no duplicates", and
  stakes that proof on the file set staying put.
- **Maintenance** improves the partition by *replacing the file set*.

Every rewrite that makes the data better simultaneously destroys the proof that
it is clean. The date is then re-queued for certification, and the next rewrite
voids it again. On a backlogged system — which is exactly the state the rest of
tonight's docs describe — rewrites never stop, so the proof never survives long
enough to be used by a read.

That is why `cert_granted_total` is 0 while certifications demonstrably exist:
they exist, they are stale, and the churn that staled them is ours.

## Why this is the same finding as the frozen mass

This closes the loop the previous doc opened, and tightens it:

- `2026-09-03-why-the-frozen-mass-is-a-read-path-bug.md` — un-deduped partitions
  re-plan queries into unspillable 16-partition sorts.
- `2026-09-03-the-frozen-mass-day-wide-dedup.md` — the sealed lane is starved, so
  those partitions stay un-deduped.
- **This doc** — and even where maintenance *does* run, its own churn voids the
  certification that would have let reads skip the dedup entirely.

The customer-visible symptom (`hashes` queries timing out on issues pages) is
downstream of all three. There is no separate read-path project to open.

## What to check next, and one caveat

**Caveat, stated first:** the process had **1,373 s (23 min) of uptime** at the
time of reading. `cert_granted_total = 0` is therefore weak on its own — 23
minutes may simply predate the first grant (a young process reads as fixed).
The `fp_moved` ratio is the robust part: it is a per-scan verdict, and reaching
it *requires* a stored certification whose fingerprint no longer matches. Re-read
both on a process with several quiet hours before quoting the grant rate.

Then, in order:

1. **Attribute the fingerprint movement.** Ingest into today's partition moving
   its fingerprint is expected and harmless — today is always dirty. The question
   is the **sealed** dates in a 7-day issues window, which no ingest should touch.
   If those are moving, the mover is maintenance, and that is the actionable half.
2. **Consider certifying against something a rewrite preserves.** A proof about
   *row content* survives a file-set change that only repacks the same rows; a
   proof about the *file list* cannot. A compaction that provably preserves the
   row multiset could carry the certification forward instead of invalidating it
   — the rewrite already knows it collapsed duplicates, so it is in the best
   possible position to re-issue the proof rather than destroy it.
3. **Re-certify as part of the commit, not as a later sweep.** The dedup rewrite
   has just proved the partition clean; making certification a separate job that
   must re-read the partition afterwards is what leaves the window for the next
   rewrite to void it.

Item 2/3 is the real design change and it is not small. Item 1 is a measurement
and should come first, because if sealed dates are *not* moving, then the 94.5 %
is all today's partition and the whole picture changes.
