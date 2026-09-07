# Next measurable DB improvements (fable-investigated, 2026-09-06 night)

Ranked candidates from a senior-engineer pass over docs/plans + measured findings,
AFTER DV-dedup shipped. Do NOT deploy any of these while DV-dedup soaks (a quiet
process is the measurement instrument). Implement + local-test now; deploy after
DV-dedup is validated.

## Premise corrections (stale figures the briefing carried)
- `md5::compress` 5.71% CPU — ALREADY FIXED 2026-08-18 (compact.rs:1538). Not a candidate.
- `parse_json_impl 18.4% CPU` — appears in NO doc; do not build on it.
- dedup-lane contiguity rank term — ALREADY SHIPPED (maintenance_coordinator.rs:1848).

## Ranked candidates

1. **Ingest-time duplicate prevention** — dedup-key check inside the MemBuffer
   10-min bucket (`write/mem_buffer.rs::insert_batch`), scoped to the client-retry
   residual (landed-skip already covers the 58% WAL-replay class). THE only option
   that scales to 100x: preventing dupes ends the certification one-pass-delay that
   even DV-dedup concedes it doesn't fix. Metric: `scan.cert_declined_dirty_bins`
   (was 12,716) → ~0 for new dates. Moderate risk (hot write path; bench vs write
   p99 with tests/membuffer_concurrency_bench.rs). Deploy behind kill-switch.
   Complementary to DV-dedup.
   - **SCOPING REFINEMENT (2026-09-06, mid-deploy):** NOT a quick bucket check.
     Flush-time per-bucket dedup ALREADY catches same-bucket dupes (dedup_compaction_test
     header confirms). A cross-flush client-retry dupe shares the exact
     (timestamp,id) → same bucket_id, but arrives AFTER the original bucket
     flushed+evicted, so a live-bucket check can't see the original. Catching it
     at ingest needs a PERSISTENT recently-flushed-keys index (bloom/LRU with a
     memory budget), not a bucket scan — a real feature with hot-path + memory
     cost. landed-skip (shipped) already covers the WAL-replay class; this targets
     the genuine client-retry residual. Design properly (index sizing, false-positive
     rate, write-p99 bench) before building; do NOT hand-roll at 2am.

2. **Fix user-facing sort-memory burst failures** — the ONLY measured customer
   breakage: 17 pgwire failures/hour, 100% unspillable `ExternalSorterMerge`
   exhaustion (one 16-partition query ~24GB vs 16GB pool). `sort_spill_reservation_bytes`
   is 64MB (`mod.rs:5327`), ~20x below observed 754MB–2.2GB merges. Doc:
   2026-09-03-user-queries-are-failing-on-sort-memory.md. Step 1: arm thread-matched
   `query.text` attribution (whale shape still unattributed). Step 2: size the spill
   reservation / cap partitions for that shape (also raises the 100GB
   max_temp_directory_size relevance). Metric: zero sort-memory PgWire errors over
   days. Low-moderate risk. **HIGH user value.** No overlap with DV.

3. **Resume-past-refusal in packer bin selection + re-decide size-ratio guard** —
   Pack is 35.9x of the ~36x write amplification and WRITE-bound (the OTHER big
   writer besides dedup). The ratio guard (potential 2.06x→3.96x) shipped OFF
   because floor+ratio composition LIVELOCKS (refused bin returns empty instead of
   resuming the walk; the floor carries this latently in prod). Fix: resume past a
   refusal (~30 lines, pinned failing test exists), re-run composition arm, sim A/B
   the ratio default. Low risk (harness+sim verifiable pre-deploy). Complementary.

4. **Certify-on-completion** — close the one-pass-delay keeping the read-path dedup
   skip at ZERO (`scan.dedup_skipped` fired 0 of 13,088; the 32x-dup 30d wall). When
   a dedup unit commits with count guards satisfied (which the DV path already
   computes: losers+survivors==scanned, survivors==distinct-key), grant the
   interval's certification in the same pass. Moderate risk (proof-invalidation on
   later flushes — "maintenance voids its own proof" trap). Overlaps DV thematically
   but DV doc leaves it unsolved.

5. **Envelope raise** COORDINATOR_JOB_WORKERS 16→32 with HEAVY_REWRITE_PERMITS in
   step — STAGING TEST ONLY. Sim shows ~linear to 32 but has no permit semaphore
   (2.06x is an upper bound) and even 2x dents a 10x backlog only 4% — bounded
   drain-quality, NOT a 10x lever. Real OOM risk on the 120GiB box.

## Watch (inside DV workstream, being monitored now)
DV-bearing files disable per-file parquet predicate pushdown → dedup-scale
DV-marking could regress reads fleet-wide. OPTIMIZE consolidation policy (trigger
on DV PRESENCE, bound the DV'd-file fraction) is load-bearing for read latency.
Verify with a rows-scanned/rows-returned metric on the hot dashboard path.

## Thin evidence (re-profile before trusting)
09-06 dedup CPU profile suggested ~25% CPU in allocator/Arrow-batch churn, but 65%
of frames unsymbolicated. Re-profile with debug=line-tables-only first.
