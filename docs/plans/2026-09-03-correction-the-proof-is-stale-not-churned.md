# CORRECTION: the proof is STALE, not churned

This retracts the central mechanism claimed in
`2026-09-03-maintenance-voids-the-proof-it-earns.md`. That doc ended with the
right instruction and I am glad it did:

> "**Next is a measurement, not a change:** attribute fingerprint movement on
> SEALED dates specifically. … if sealed dates in a 7-day window are NOT moving,
> the 94.5 % is all today and the picture changes."

The measurement has been run. **Sealed dates are not moving.** The picture
changes.

## The measurement

`scratchpad/fp_movement.py` walks the last 300 Delta commits on
`otel_logs_and_spans` (a **252-minute** window, 2026-09-03 19:46 → 23:58) and
buckets every `add`/`remove` by how old the target partition's date is:

```
age bucket                       partitions   adds  removes   top operations
today                                    10   1053      954   add:WRITE=986, remove:OPTIMIZE=903, add:OPTIMIZE=67
2-7d  (sealed, in issues window)          0      0        0
8-31d (sealed)                            1      1        6    remove:OPTIMIZE=6
>31d  (sealed)                            4      4        4    add/remove:OPTIMIZE=4

distinct partitions touched, by operation:
  OPTIMIZE   partitions=15  of which SEALED=5
  WRITE      partitions=10  of which SEALED=0
```

**The 2-to-7-day band — the exact band an issues page queries — received zero
file-set changes in over four hours.** Sealed churn overall is trivial: 5 sealed
partitions touched, 5 adds and 10 removes, against 1,053 adds on today's.

## What that refutes, and what replaces it

**Refuted:** "every compaction, hot-tail pack and dedup rewrite voids the very
proof it earns, and on a backlogged system rewrites never stop, so the proof
never survives." That describes a race between certification and churn. On the
dates that matter **there is no churn to race**. The mechanism is real in
principle and it is not what is happening.

**What replaces it:** the certifications on those sealed dates were voided at
some earlier point — during the long compaction backlog, which did rewrite them —
and **nothing has re-issued them since**. `cert_granted_total` reads **0** across
two separate process lifetimes now, and `cert_slice_files_proved = 32` against
`cert_slice_files_unproven = 470` (**6.4 %**). The proof is not being repeatedly
destroyed; it is **stale, and the certifier is not producing new grants.**

That also explains the shape of the denial split that started this. Only 12
scans hit `never_certified` because these dates *were* certified once. 1,945 hit
`fp_moved` because that one certification is permanently out of date. A
never-refreshed proof and a constantly-invalidated proof produce the identical
counter, which is exactly why the counter alone could not distinguish them, and
why the attribution measurement was worth running before touching any code.

## The actual chain

With `cert_declined_dirty_bins` on the board and the dedup lane starved, the
chain reads:

```
dedup lane starved  ->  bins stay dirty  ->  certification DECLINES
    ->  no new grants (cert_granted_total = 0)
    ->  sealed dates keep a stale fingerprint  ->  dedup_skipped = 0 of 2,051
    ->  readmit_mutable_filters never fires
    ->  the customer's `hashes` predicate is stranded above DedupExec
    ->  the issues page times out
```

The **root is unchanged** — it is still the starved dedup lane, and the customer
timeout is still downstream of it, so the conclusion of the previous two docs
stands. What changes is the **middle link**, and with it the fix. A design that
carries certifications across rewrites (the "certify the row multiset, not the
file list" idea) would solve a problem these dates do not have. The thing to fix
is that **dirty bins block certification, and the dedup that would clean them is
starved** — one problem, not two.

## Caveat on the process, and a measurement hazard worth recording

Prod restarted **25 minutes** before this reading, and `docker service ps` shows
shutdowns at 25 min, 52 min, 2 h and 4 h ago with **no error and
`DesiredState=Shutdown`** — these are deploys, not crashes. Someone is
redeploying roughly every 25-30 minutes.

Two consequences:

1. **Maintenance throughput cannot be measured in this regime at all.** Units run
   ~21 minutes against a ~25-minute restart cadence, so most die to process exit
   before committing. `pending_dedup` sat at 2,123 and
   `dedup_bins_committed_total` at 4, unchanged across the window. Any
   convergence number quoted tonight is invalid — this is the known
   deploy-cadence trap, hit again.
2. **The certification finding survives it.** `dedup_skipped = 0` and
   `cert_granted_total = 0` were observed independently across *two* process
   lifetimes, and the fingerprint attribution above comes from the Delta log,
   which is durable and process-independent.

The correction itself is the lesson: the previous doc reasoned from a counter to
a mechanism, and two mechanisms fit that counter equally. Only the log
distinguished them.
