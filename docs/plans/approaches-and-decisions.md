# Approaches and decisions — running log

Newest first. One entry per decision that changed direction, with what refuted it.
Detail lives in the dated plan files; this is the index.

## 2026-09-01/02 — maintenance capacity night

**Goal:** make dedup/sorting/hotpacking/rollups keep up, toward 10x and a
prospective 100x customer.

### Shipped to prod

| # | change | status |
| --- | --- | --- |
| 19 | split the certified-skip refusal (`no_stats` vs `overlap`) | live |
| 20 | rollup admission reclassified, deploy-15 footgun made unwritable | live, verified |
| 21 | certify from the batch probe | live, measured a NULL |
| 22 | certify sealed dates from the Delta snapshot | live — **first grants in system history** |
| 23 | project-major ordering | live |
| 24 | 16x probe rate + decline memo + `cert_probe_declined` | live |
| 25 | certification reaches `otel_logs_and_spans` at all | live |
| 26 | instrument HOW dirty a declined date is | live |
| — | window claim reservation (`5ed8c6b5`) | live, A/B'd at ~1% |

### The chain, and where it broke

1. Dedup is **~96% of maintenance worker time** and drops **0.0004%** of rows —
   it is a cleanliness PROOF, not a removal.
2. So certify cheaply instead of rewriting. Built it; `cert_granted_total` left a
   zero held since 2026-08-20, and prod queries were measured skipping
   `DedupExec`.
3. **Refuted as a leading strategy:** duplicates are sparse but SPREAD (~26-50 of
   144 bins per date), so every partition is dirty and certification cannot grant
   until removal happens. Removal is the constraint.
4. **Refuted again:** the removal queue's ordering is worth ~1%. A/B on the real
   77k-task prod journal — 22,162 pending, 27,175 executions/24h, backlog only
   halves either way. **The queue is CAPACITY-bound, not order-bound.**
5. **The cost, located:** the rewrite's sort **OOMs a 1 GB pool on one 204 MB
   production bin**; prod survives only by slicing 13 ways at 15 MB/s.

### Open, in priority order

1. **Align `sorting_columns` with the dedup keys** (`timestamp, id` leading;
   `service` after). Today `service` sits BETWEEN the dedup keys, so files are not
   prefix-ordered for the window's `PARTITION BY (timestamp, id)` and the sort is
   unavoidable. NOT DONE: changes physical layout for every future file and every
   query's read path, needs the latency matrix re-run, and does **not**
   retroactively fix existing files — so it cannot drain the current backlog on
   its own.
2. **Prevent duplicates at ingest** — dedup-key check inside the MemBuffer's
   10-minute bucket, so dates are born certifiable and need neither rewrite nor
   probe. The only item that scales to 100x. Design decision, not a patch.
3. Cheaper units (batch sizing already gave 39.4s -> 20.3s on one file).

### Decisions NOT taken, and why

- **Raising `STARVATION_MICROS` 3d -> 15d.** Refuted: `starved` is `u8::MAX` when
  NOT starved, so any starved task outranks any non-starved one — raising the
  threshold EVICTS the query window from the privileged lane. 9 test failures.
- **Declaring output ordering in `narrow_provider`.** Refuted before building: the
  file order and the window's partition key are misaligned (see 1 above), so it
  would have been a no-op shipped into the row-deleting path.
- **Bin-scoped dedup instead of whole-date.** Refuted: duplicates are spread over
  ~18-35% of bins and `stage_dedup_chunk` re-reads every file a chunk touches, so
  the whole-date unit is roughly right-sized.

### Method notes that cost real time

- **`COUNT(*)` cannot probe dedup coverage** — count pushdown answers it without a
  Delta scan. `GROUP BY … HAVING count(*)>1` also lies: it reads THROUGH
  `DedupExec` and sees duplicates already collapsed.
- **A counter stuck at an exact value means SCOPE, not throughput.** 437 grants
  frozen = the producer never ran on the main table (the dedup cron skips
  rollup-declaring tables).
- **`git stash` no-ops on an already-committed change**, which silently made an
  A/B compare a build against itself. Verify the arm with a marker
  (`grep -c window_turn` must be 0 in the baseline).
- **`synth:whale` cannot validate scheduling** — a fixed 813-task backlog that
  always drains, so no reservation ever binds. Use the real journal
  (`docker cp <ctr>:/app/data/timefusion/.timefusion_meta/maintenance_tasks.json`).
- **A young process reads as fixed.** Check uptime before quoting any counter.
