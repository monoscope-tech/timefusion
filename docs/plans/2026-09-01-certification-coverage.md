# Certification coverage: the producer is pointed at the wrong data

2026-09-01, after deploy 19.

## What the split counters settled

Deploy 19 split the certified-skip refusal so the two causes could not hide in
one number. Prod, ~40 min after it went live:

| counter | value | reads as |
| --- | --- | --- |
| `cert_skip_blocked_no_stats` | 0 | statistics are present on every file |
| `cert_skip_blocked_overlap` | 128 | every certified file has an uncertified neighbour |
| `cert_skip_files` | 0 | nothing has ever skipped |
| `dedup_denied_never_certified` | 164 of 177 denials | **the partition had no certification at all** |
| `dedup_denied_by_leg` | 0 | no scan ever reached a whole-partition grant |

`no_stats = 0` kills the stats-poisoning hypothesis outright. `by_leg = 0` plus
`never_certified = 164/177` says the 128 overlap blocks all came from the
per-file path, on the few partitions holding any evidence at all.

**So neither contiguity nor the isolation rule is the first-order problem.
Coverage is.**

## Why coverage lands where queries do not

Certification has only ever been a side effect of dedup rewrites: ~21-minute
units, age-ordered, 2,573 pending. Evidence therefore accrues on old backlog
dates while queries read recent windows. Widening the containment test — the fix
I was about to build — is refuted by this data: a perfectly contained slice on a
date nothing queries is still worth nothing.

The code already says the other half of it, at `record_clean_slice`:

> whole-day coverage under an unchanged fingerprint never happens on a live
> table (`cert_granted_total` = 0 since 2026-08-20)

Today's date churns, so its fingerprint moves and it can never be granted whole.
A 14-day window is **13 sealed dates plus today** — and sealed dates take no
writes, so their fingerprint is stable and a grant sticks.

## The grant granularity that matters

Verified in `scan_delta_partitions`: certified files are returned as
`certified_plans` and unioned **above** `DedupExec`. A per-file skip therefore
routes those files *around* the operator — it does not remove it. The structural
term only disappears when the deduped leg is empty, i.e. when every file in the
scan is certified.

**Scattered per-file wins cannot move the 14-day latency; full-window coverage
can.** This is what discriminates between the candidate fixes.

## The lever: certification needs a proof, not a rewrite

`probe_dup_bins` already GROUP BYs the dedup keys over an entire
`(project, date)` and returns the bins whose group count exceeds one. **An empty
result is a whole-date cleanliness proof** — the same predicate a zero-drop
rewrite establishes, at key-only cost, writing nothing. A local bench put a
key-only probe at ~200x cheaper than a full read.

That proof is computed today and thrown away: `batch_probe_classify` keeps only
the per-bin classification.

### What dedup actually spends its time on

Prod, same window, from the `work.*` counters:

| operation | worker_secs | share | rows scanned | rows dropped |
| --- | --- | --- | --- | --- |
| Dedup | 7,843 | **96.5%** | 454,596,841 | 3,782 |
| SealedConsolidation | 49 | 0.6% | 555,789 | — |
| HotPacking | 25 | 0.3% | 440,120 | — |
| BaseRollup | 205 | 2.5% | 0 | — |

**A duplicate rate of 0.0008%.** Dedup scans 454 million rows to remove 3,782 —
it is overwhelmingly a cleanliness *proof*, not a removal. Paying full-rewrite
price for a proof a key-only aggregate can produce is the whole inefficiency,
and it is 96.5% of the maintenance fleet.

### Step 1 (built) — certify from the probe already running

Feed the empty-probe verdict through `record_certification` with
`(dropped = 0, complete = true)`, capturing the file list **before** the probe so
the fingerprint compare can still reject a grant if anything commits while it
runs. Reusing the existing recorder rather than a second rule is deliberate — the
soundness argument stays in one place.

The batch probe is already ordered recent-first, so its verdict lands where reads
are.

### Step 2 (next) — probe the sealed dates nothing enqueues

The batch probe only visits `(project, date)` groups with **queued dirty bins**.
Sealed dates that were fully processed have none, so they are never probed and
never certified — and they are exactly the 13/14 of a dashboard window. Extend
the candidate list with uncertified dates inside the query window, bounded per
tick and ordered newest-first.

### Not doing: flush-time certification

Sound only if a flushed file is internally duplicate-free, which is unestablished
— client retries inside the 10-minute bucket could put duplicate keys in one
file. Cross-file duplicates are already handled by the overlap rule. If pursued,
the flush has the batch in memory and a distinct-count over the dedup keys would
make the grant conditional rather than assumed. Left for after step 2, and it is
the churning-hot-date case, not the window case.

## Success criterion

After the sweep covers one busy project's full 14-day window: re-run the latency
matrix on that project, confirm the plan no longer contains `DedupExec`, and
confirm the 42–47 s point moves. That measurement is what the whole chain exists
for — anything short of it is a counter moving, not a query getting faster.
