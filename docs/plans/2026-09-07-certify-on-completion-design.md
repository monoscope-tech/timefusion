# Certify-on-completion: let the DV pass that proved a slice clean say so

**Premise verdict: half-holds — lead with the decoupling.**
The one-pass-delay is real and **worse** than described: a row-dropping pass doesn't just
fail to certify, it **wipes the whole day's accumulated clean-slice coverage**
(`maintain.rs:5025`). But certification never dequeues a journal task — the coordinator
runs `journal.complete(&key)` regardless of the cert outcome (`maintain.rs:2127-2128`) —
so **certify-on-completion alone will not drive `pending_dedup` → ~0**. The floor is
sustained by re-mint physics (below); this change kills the delay, a companion change
kills the re-mint. The metric this design directly moves is `cert_refused_dropped → ~0`.

## Where certification happens today, and why it lags

- A completing coordinator Dedup unit calls `record_clean_slice` (`maintain.rs:2119`)
  with `pre` files captured before the pass (`maintain.rs:2111-2114`) and the
  `(dropped, complete)` verdict from `dedup_partition_range_limited`
  (`compact.rs:960-985`; landed-only drops, `complete && failed.is_empty()`).
- `record_clean_slice` treats the pass as DIRTY iff
  `dropped != 0 || post.is_empty() || fp(pre) != fp(post)` (`maintain.rs:5023`).
  Dirty ⇒ wipe the day's interval coverage (`:5025`) and route to
  `record_certification`, which refuses on `dropped != 0` (`CERT_REFUSED_DROPPED`,
  `maintain.rs:5199-5205`). Clean ⇒ merge the interval; whole-day union under one
  fingerprint grants (`:5047`, `:5067`), partial banks per-FILE evidence
  (`certify_files_within_slice`, `:5061`, entries `stale: true`).
- The fingerprint is a **sorted-URI hash, paths only, FROZEN** (`mod.rs:2051-2064`).
- Under copy-on-write the `dropped != 0` refusal was harmless conservatism: dropping rows
  rewrote files, so `fp` moved anyway. Under DV-dedup the commit is
  `Remove(old) + Add(same path, +DV)` (`compact.rs:2005-2011`, `:2197`, `:2234-2248`) —
  **the URI set does not move**, and the only failing conjunct is `dropped != 0`. The
  committed post-state is byte-for-byte the state the pass just proved duplicate-free,
  and we refuse to say so. That is the one-pass delay, plus the coverage wipe on top.
- The confirming zero-drop pass is scheduled only by a NEW invalidation: an ingest flush
  (`invalidate_rollup_hours` → `enqueue_maintenance_hours`, `maintain.rs:3915`) or —
  for sealed quiet days — the **boot-only** cursor reconcile walking the DV commit's own
  `data_change=true` actions (`maintain.rs:878-916`; the fork's `write_deletion_vectors`
  sets `data_change: true`, vendor `deletion_vectors.rs:176,500`). Prod's restart cadence
  is what re-pends everything DV'd since last boot (`invalidate` upserts Complete slices
  back to Pending, `maintenance_coordinator.rs:2450-2461`) — the ~500 floor is that
  standing population of confirm passes plus frontier mints in their 15-min quiet period
  (`FINALIZATION_DELAY_MICROS`, `maintenance_coordinator.rs:161`).

## The change

Thread one bit — `losers_masked` — from the DV staging path to the grant rule:

1. `stage_dedup_chunk_dv` marks its `StagedBin`/`DedupUnit` as masked-in-place (adds
   preserve target paths; `before=scanned, after=survivors`, `compact.rs:2242-2248`).
   The CoW path (`compact.rs:1990-2000`) stays unmasked.
2. `dedup_partition_range_limited` returns `losers_masked = all landed bins masked`
   alongside `(dropped, complete)`.
3. `record_clean_slice` dirty test becomes
   `(!losers_masked && dropped != 0) || post.is_empty() || fp(pre) != fp(post)`.
   A masked, complete, fp-stable pass merges its interval as CLEAN evidence, banks its
   per-file certifications, and — when it fills the day's last hole — grants via
   `record_certification(…, (0, true))`, which re-checks the live fp one final time
   (`maintain.rs:5172`). No new grant path; the existing rule, correctly fed.
4. **DV-visibility guard (load-bearing):** the URI fp is blind to a concurrent same-path
   DV commit (DML DELETE/UPDATE write DVs too, `dml.rs:1289`, `:1249`, `:1519`). For the
   masked arm only, compare the LIVE post `(path, dv_unique_id)` set against the
   **expected post** = `pre` with this pass's OWN committed DV attachments applied — the
   `StagedBin` adds carry exactly those descriptors, so the wave knows what it committed.
   NOT plain pre-vs-post: the pass's own commit changes the dv_ids of every file it
   touched, so a naive compare would decline every masked pass with losers > 0 and ship
   the feature dead. Computed live, never persisted — the FROZEN fp (`mod.rs:2054-2057`)
   is untouched. Anything in post beyond the expected set is a foreign interleaved DV
   commit ⇒ decline, exactly like `fp_moved` today. Defense-in-depth, not the primary argument:
   DV-only mutations are cleanliness-monotone (masking rows cannot create a duplicate),
   and UPDATE-via-DV pairs its mask with a new Add (new path ⇒ URI fp moves ⇒ declined).

Safety inventory: concurrent flush ⇒ new path ⇒ fp moves ⇒ decline (unchanged).
Concurrent CoW rewrite ⇒ paths change ⇒ decline; also caught by `commit_wave`'s
target-liveness check + OCC (`maintain.rs:7720-7730`). Partial wave ⇒ `complete=false`
⇒ handler retries, `record_clean_slice` never runs (`maintain.rs:2115-2132`). MemBuffer
leg: proof is Delta-only; the read side already accounts for it
(`read/mod.rs:196-198`, `skippable_certified_files` `:200`).

**Contiguity:** unchanged and still binding. A unit is capped ≈6h (`maintain.rs:2103-2110`),
so a single completion grants the whole day only when it fills the LAST hole; the win is
(a) dropping passes stop wiping sibling evidence — the bigger lever on days with dup bins
spread over ~50 of 144 bins — and (b) the last-hole unit grants same-pass instead of one
pass later. The Dedup contiguity rank term already drives units toward completing runs
(`maintenance_coordinator.rs:1848-1870`, default ON, `config.rs:2198`).

**Companion change (separate, sized separately):** tag DV-dedup commits in commitInfo and
have boot reconcile skip re-minting **Dedup** for self-authored DV commits — that, not
certification, is what lowers the `pending_dedup` floor. Open question to resolve first:
must the ROLLUP re-mint from dedup commits survive (does removing dup rows change built
rollup output, or does the rollup builder read dedup-applied)? Flag, don't assume.

## Success criteria & validation

Pre-deploy sizing (free, do first): read prod `cert_refused_dropped`, `cert_slice_dirty`,
`cert_slice_partial`, `cert_slice_day_covered` rates to decompose the ~500 floor into
confirm-passes vs frontier mints.

- `cert_refused_dropped` → ~0; `cert_slice_dirty` drops to genuine fp-moved cases.
- `cert_slice_day_covered` / `cert_granted_total` rise; `dedup_skipped / dedup_eligible`
  (`mod.rs:231`) rises from its measured 0.
- `pending_dedup` floor: expected to fall only with the companion change; do not gate
  this change on it.
- Read p95 (14/30d, ≥3 alternated runs, ≥10 min uptime) must NOT regress; correctness
  tripwire: `COUNT(*)` over a granted window identical with skip forced on/off.

Ladder: (1) unit tests on the new dirty test — masked+fp-stable grants, masked+dv-id-moved
declines, CoW dropped still declines; prove each guard can fail by reverting it.
(2) `timefusion sim` on a prod journal: sim models coverage GEOMETRY from completed slices
(`maintenance_sim.rs:975-996`) but not the dirty arm — it validates ordering/convergence
(`dedup_cells_day_covered`), not the grant rule; the grant needs (3) e2e: DV pass on a
seeded-dup day, assert same-pass cert + no `DedupExec` in the subsequent plan.
(4) staging with real S3, then prod behind the existing kill switches.

## Risks

- **Over-certification** (the worst bug): the fp URI-blindness to same-path DV commits is
  the one real hole; the `(path, dv_unique_id)` compare closes it fail-closed. The owed
  concurrent-DML race test (DV-dedup vs DML-DV on the same file) is an adjacent live risk
  of the DV path itself — write it as part of this work.
- **Inert-ship paths:** (i) DV dedup requires `timefusion_use_deletion_vectors` (default
  ON, `config.rs:2903`) AND the table property (`compact.rs:1367-1372`) — verify prod's
  table has `enable_deletion_vectors=true` or every masked pass silently takes CoW and
  this change never fires; (ii) the floor decoupling above — without the companion change
  the headline metric won't move and this reads as dead; (iii) frontier days churn fp on
  every flush regardless — this helps SEALED days, which is where the backlog lives.
- **Coverage-wipe removal changes sim baselines:** re-run the 2-seed pre-registered gate
  before trusting any throughput delta (deploy-cadence trap, 2026-09-05).
