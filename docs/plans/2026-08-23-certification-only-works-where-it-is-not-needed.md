# Certification succeeds only for the tenants that do not need it

2026-08-23. Closes the diagnosis half of "break the certification contiguity
blocker". The blocker is not contiguity any more, and the mechanism that was
supposed to fix it sits downstream of a gate it can never pass.

## What prod actually holds

`dedup_certifications.json` on the CapRover host, 106 entries. Read-only.

```
2026-08-18  7 entries    2026-08-21  5 entries
2026-08-19  3 entries    2026-08-22  5 entries
2026-08-20  2 entries
```

So certification **does** reach recent dates. The earlier reading — that only old,
quiet days can certify — is out of date, and "the longest consecutive run is 5"
is no longer the binding constraint.

The binding constraint is *which* tenants. Every recent entry belongs to a small
project:

```
2a39bd83  4f020cf8  5ce1c976  8100121c  94c5dc1f  d062e010  28f62f01
```

Not `dcad860a` (the whale). Not `87576849` (p1). Not `00000000` (the shared
unified-default table). **The three projects whose dashboards are slow hold zero
certifications on any date.** `dedup_denied_never_certified_pct` reads 100.0 and
`dedup_skipped_per_date` reads 0 — not because the per-date skip is broken (it is
default-ON and correct), but because there is nothing certified in the windows
anyone queries.

## Why the big tenants can never certify

`record_certification` grants only when

```rust
dropped == 0 && complete && !post.is_empty() && partition_file_fp(pre) == fp_post
```

`partition_file_fp` is over the WHOLE partition's live file list. A single write,
flush or compaction landing anywhere in the day while the sweep runs moves that
fingerprint and the grant is refused. A quiet tenant's day goes still and
certifies; a whale's day never does. `record_clean_slice` does not soften this —
it accumulates clean slices and only calls `record_certification` once the slices
cover the entire day, so partial proof accumulates and then expires.

## Why additive per-file certification does not rescue it yet

4b91f8c added the machinery to certify a *file set* and dedup only the
uncertified remainder — the right idea for exactly this case. But
`certified_file_split` resolves its file list from a `Certification`, and a
`Certification` only exists after the all-or-nothing grant above. So the additive
path is downstream of the gate that churning tenants cannot pass, and turning
`timefusion_read_dedup_skip_per_file` on today would grant nothing to the
projects it was built for.

The sidecar shows the same thing from the other side: most stored entries carry
`files = 0`, because they predate 4b91f8c. Only four entries (all 2026-08-22,
all small tenants) carry a real file list.

## The fix, stated but not built

Grant a certification over the **subset of files a sweep proved clean**, rather
than only over a partition whose fingerprint held still.

- The soundness rule is already established and tested (§3a,
  `2026-08-22-rollup-correctness-and-routing.md`): a certified file may skip
  `DedupExec` iff no UNCERTIFIED file's timestamp span overlaps its own — PER
  FILE, not per set. It holds because the dedup key is `(timestamp, id)` and
  merge-on-read re-appends preserve `timestamp`.
- What is missing is the producer: `record_certification` must be able to emit a
  partial `Certification { files }` for the files the sweep actually covered,
  instead of returning `None` the moment the partition fingerprint moves.
- The failure mode of getting this wrong is a **silent wrong answer** — a skipped
  file whose duplicate lived in an uncertified neighbour. It needs the same
  treatment §3a got: rule, proof, case table, and a prod parity diff of `count(*)`
  with the flag on and off over a churning partition, before it is enabled
  anywhere.

Not attempted here: this is the most correctness-sensitive surface in the read
path, and a half-finished version of it is worse than none.

## What this changes about the plan

"2.1 Get certification working" in `2026-08-22-make-14d-30d-complete.md` was
scoped as an enablement task — flip the flags, watch the counters. It is not.
Both flags are already correct and one is already on; the missing piece is a
partial-grant producer, which is a design task with a silent-wrong-answer failure
mode. It should be sequenced accordingly.
