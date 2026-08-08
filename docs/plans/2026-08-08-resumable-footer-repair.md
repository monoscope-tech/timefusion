# Resumable footer repair

**Status:** IMPLEMENTED, shipped OFF. `resumable_staged_bin` is called from the
per-project closure in `optimize_table_light`, right after the footer probe and
before staging; a hit returns `BinOutcome::Staged` so the normal `commit_wave`
lands it. Gated on `TIMEFUSION_REPAIR_RESUME_ENABLED` (default false).
Two deviations from the sketch below, both deliberate:
 - **not gated on `TailPass::Repair`.** A packing bin is equally
   data-preserving, and today's oversized unsorted file is repaired by the PACK
   pass. A packing bin's inputs change every tick so the lookup almost always
   misses; the cost of a miss is one small local file read.
 - `plan_tail_pass` / `tail_pass_policy` were split out of `optimize_table_light`
   so the e2e can stage the EXACT bin the next pass re-selects — resume keys on
   input-set equality, so a hand-picked file list would prove nothing.

Tests: `database::tests::resume_*` (the pure verdict table) and
`tests/e2e/repair_resume.rs` (stage → abandon → restart → commit-not-restage,
plus the stale-twin decline + reclaim, and the kill-switch no-op).
Remaining: the prod rollout below.
**Problem:** a footer-repair rewrite takes 40+ minutes. Any process restart —
a deploy, or the healthcheck replacing the swarm task — throws that work away
and the next pass starts the same file from scratch. Prod 2026-08-08 lost two
passes this way in one morning (an 08:02 deploy, and an unexplained same-image
replacement at 06:45), each discarding ~2 GB of already-written parquet.

**Goal:** a repair pass commits work that was already staged instead of redoing
it. Loss becomes "the bytes not yet written", not "the whole rewrite".

**Shape in one sentence:** before staging a bin, look for a staged-intent whose
input set is exactly this bin's, and if it is still valid, return it as an
already-staged bin so the normal commit path lands it.

---

## What already exists (do not rebuild)

| piece | where | what it does |
|---|---|---|
| `StagedIntent` | `database.rs` (`struct StagedIntent`) | `{wave_id, table_name, project_id, recorded_at, paths}`, appended to `<data_dir>/staged_intent.jsonl` before a bin's commit. Survives restarts. |
| `record_staged_intent` / `clear_staged_intent` | `database.rs` | append / rewrite-without. Serialized by `staged_intent_manifest_lock`. |
| `reconcile_staged_intents` | `database.rs` | at boot: deletes staged parquet whose entry is old and whose files are unreferenced by the snapshot. **Deletion only — leave it alone; resume runs earlier, at bin selection, and reconcile then cleans up whatever resume declined.** |
| `parse_staged_intents` | `database.rs` | lenient JSONL parse; a torn tail degrades to fewer entries, never a boot failure. Keep that property. |
| `StagedBin` | `database.rs` (`struct StagedBin`) | `{project_id, wave_id, target_paths, removes, adds, stage_store, dedup}`. `target_paths` is the INPUT file list; `adds`/`removes` are ready Delta actions. |
| `split_live_bins` | `database.rs` | partitions bins on "every `target_paths` entry still live in the snapshot". Exactly the staleness guard a resume needs. |
| `bin_adds_live` | `database.rs` | distinguishes "someone rewrote my targets" from "my own commit already landed". |
| `staged_actions(&targets, adds, data_change)` | `database.rs` | builds `(removes, adds)`; repair passes `data_change = false`. |
| per-bin commit | `4f2484a` | repair commits each bin as it finishes, so loss is already bounded to one bin. |

**The manifest today records OUTPUTS only.** That is enough to delete orphans and
not enough to commit them: you cannot tell which input files a staged output was
meant to replace, so you cannot tell whether it is still valid.

## The two invariants that make this safe

Both are already relied on elsewhere, which is why this is tractable rather than
a new correctness surface.

1. **Staleness.** A staged output is valid only if *every* input it replaces is
   still live in the current Delta snapshot. If any input was rewritten
   underneath (by dedup, consolidate, another repair), the output is garbage and
   committing it would resurrect removed rows or drop new ones.
   `split_live_bins` is this check, verbatim.
2. **Row preservation.** A repair is data-preserving: `StagedBin::dedup == None`
   ⇒ `data_change() == false`. So
   `sum(output numRecords) == sum(input numRecords)` must hold. This is what
   catches a *truncated* staging — a process killed halfway through writing its
   outputs. Without it, a partial rewrite would silently drop rows.

Note what is NOT at risk: staged parquet is invisible to readers until the
atomic commit, so a bad resume cannot serve wrong data. It can only produce a bad
*commit*, and the two checks above are what stand in front of that.

---

## Implementation

### 1. Widen the manifest record

Add to `StagedIntent`, both `#[serde(default)]` so old lines still parse:

```rust
/// Input files this staged output replaces. Empty on pre-resume entries,
/// which are then cleanup-only (the old behaviour).
#[serde(default)]
target_paths: Vec<String>,
/// The staged Add actions, verbatim, so a resume rebuilds the bin without
/// re-reading footers. `deltalake::kernel::Add` is serde-serializable.
#[serde(default)]
adds: Vec<deltalake::kernel::Add>,
```

Populate at both `record_staged_intent` call sites. **Only the hot-bin/repair
site needs to resume**; the dedup site may keep empty vectors (dedup is
row-dropping, so invariant 2 does not apply to it — see "Explicitly out of
scope").

Size check: `Add.stats` carries per-column min/max for ~96 columns, so expect
tens of KB per entry. Fine for a JSONL file with a handful of live entries; if it
ever isn't, drop `stats` from the manifest and re-read it from the staged file's
footer on resume.

### 2. Resume at bin selection, not at boot

Hook it where the pass is **about to stage a bin**, in the per-project closure in
`optimize_table_light` — the same place the footer probe
(`repair_bin_already_sorted`) already short-circuits work:

```rust
if pass == TailPass::Repair
    && let Some(bin) = self.resumable_staged_bin(table_ref, table_name, &project_id, &files).await
{
    return (project_id, Ok(BinOutcome::Staged(bin)));   // commit it, don't re-stage
}
```

This is deliberately NOT a boot-time sweep. Keying on "an intent whose
`target_paths` set equals the bin we were about to stage" means:

- the boot case falls out for free — the first pass after a restart selects the
  same file (selection is deterministic given the snapshot) and finds its own
  abandoned output;
- it also works *within* a session, e.g. a bin whose commit failed on OCC after
  a successful stage;
- the lookup is direct. A boot sweep has to walk the manifest and reverse-map
  each entry back onto a snapshot, which is the same checks in a more awkward
  direction, and it duplicates the selection logic that decides what is worth
  repairing at all.

`resumable_staged_bin` returns `Some` only if ALL of the following hold. For each
manifest entry matching `(table_name, project_id)` whose `target_paths` set
equals `files`, with `adds` non-empty:

1. **Age gate.** Skip entries younger than the existing max-wave-age threshold.
   Same reason `reconcile` has one: on an overlapping rolling deploy a
   still-running instance may be mid-staging, and adopting its entry would commit
   a half-written output. Reuse the constant, do not invent a second one. Note
   this is the one cost of the bin-selection hook over a boot sweep — the first
   pass after a fast restart may be inside the window and decline; it resumes on
   the next tick. Prefer that over racing a live writer.
2. **Already landed?** If `bin_adds_live(...)`, the commit succeeded before the
   crash. Clear the intent, count it, move on. (Do not re-commit.)
3. **Staleness.** Every `target_paths` entry must be live in the snapshot. Else
   decline — leave it for reconcile to delete.
4. **Outputs present.** `head()` every `adds[].path` on the stage store; sizes
   must equal the recorded `Add.size`. Any miss ⇒ decline.
5. **Row preservation.** Sum `numRecords` from the snapshot's live Add stats for
   `target_paths`, and from the recorded `adds`. Must be equal. Else decline and
   log loudly — this is the case that would have dropped rows.
6. **Return a `StagedBin`.** Rebuild it and let the normal path commit it:
   ```rust
   let (removes, adds) = staged_actions(&targets, adds, false);
   StagedBin { project_id, wave_id, target_paths: files, removes, adds, stage_store, dedup: None }
   ```
   Returning `BinOutcome::Staged` means there is **no second commit path at all**
   — it goes through the same `commit_wave` a freshly-staged bin does, which
   holds the commit lock, honours flush priority, re-verifies liveness under the
   lock, handles the OCC ladder, and clears the intent on success. That reuse is
   most of the safety argument; resist any temptation to commit inline.

### 3. Metrics and logging

Add counters next to the existing maintenance stats:
`repair_resumed_total`, `repair_resume_declined_stale_total`,
`repair_resume_declined_incomplete_total`, `repair_resume_row_mismatch_total`.

The row-mismatch counter must be **loud** (`error!` with the two sums and the
paths). It should be zero forever; a non-zero value means either a bug here or a
row-preserving assumption that stopped holding.

---

## Tests

Unit (`#[cfg(test)]` in `database.rs`, alongside `staged_chunk_tests`):

- old JSONL lines without the new fields still parse and are treated as
  cleanup-only
- resume declines when one input is missing from the live set
- resume declines when an output object is absent, or its size differs
- resume declines on a row-count mismatch, and increments the loud counter
- resume clears (does not re-commit) an entry whose adds are already live

E2E (`tests/e2e/`) — this is the one that actually proves it:

1. write and flush data so a partition has a footer-less file
2. run a repair pass but abort it *after* staging and *before* commit (the
   cleanest seam is a test hook on the commit step, mirroring how
   `hot_tail_sorted_footer.rs` drives `optimize_table_light` directly)
3. rebuild the `Database` from the same data dir (as a restart would)
4. run the resume path
5. assert: the commit lands, the partition's footers are sorted, **row count is
   unchanged**, and no orphan parquet remains

Add a negative case in the same file: mutate one input file between stage and
resume, assert resume declines and reconcile deletes the staged output.

---

## Rollout

- Gate on `TIMEFUSION_REPAIR_RESUME_ENABLED`, default **false** for the first
  deploy. Turn it on after one clean boot with the counters at zero, so the
  manifest-widening ships and gets exercised before anything commits from it.
- Verify in prod: kill the container mid-repair (`docker kill`, not a graceful
  stop) and confirm the next boot logs `repair_resumed_total=1` and the
  partition's `explain` flips to `mode=bounded` without a fresh 40-minute
  rewrite.
- Kill switch: setting the flag false reverts to today's behaviour exactly
  (reconcile deletes, next pass restages).

## Explicitly out of scope

- **Dedup bins.** They are row-dropping (`data_change = true`), so invariant 2
  does not hold and a different validity argument is needed. Leave them
  cleanup-only.
- **A boot-time sweep.** Superseded: the bin-selection hook covers the restart
  case (the first pass after a restart re-selects the same file and finds its own
  output) and needs no second code path. Only add a sweep if a real case appears
  where a staged output is worth committing even though no pass would select its
  inputs — none is known.
- **Making the rewrite itself faster or smaller.** Time-slicing a large file's
  rewrite is a separate plan; resume makes the current unit survivable, not
  cheaper.

---

## Other open work, for whoever picks this up

Ranked, with enough context to start. Details and measurements are in the
session memory notes referenced in each line.

1. **Healthcheck kills long passes** — service-level, not code. `pgwire_ready_at`
   has its own 750 ms handshake deadline; measured failing at 0.896 s under load
   with no deploy in flight, and 3 consecutive failures replace the task. Docker's
   `Timeout` is 2 s and the probe's own worst case already overruns it, so there is
   no headroom to win in Rust — widen the container's `Timeout` (→5 s) and
   `Retries` (→5). **This is the highest-value item on the list and it is not a
   code change.**
2. **Wave barrier** — a wave commits only when every project's bin finishes.
   `4f2484a` exempts repair via per-bin commit; packing still batches, which is
   correct, but the general shape is worth revisiting if another pass ever grows
   long bins.
3. **`ORDER BY <ordinal>` over `CAST(COUNT(*))` is rejected** — Postgres accepts
   it; breaks monoscope's service graph ~5500×/hour. One-line client-side fix
   (alias the aggregate). Do NOT text-rewrite `count(*)` in `rewrite_pg_synonyms`
   — it runs on every statement including INSERTs and would corrupt row data
   containing that substring. A correct TF-side fix is an AST rewrite in
   `plan_cache.rs`, but that hook only fires for statements the cache accepts, so
   it would be partial.
4. **`otel_metrics` packing times out at 240 s every tick** — same
   "unit larger than the budget" class. Its two largest projects are 360 MB and
   310 MB over 37 files against a 256 MB target; needs a per-table target size.
5. **Wide aggregates are slow even when healthy** — the dedup key columns are 45%
   of an otel parquet file's bytes and `id` alone is 43% (19.4 B/row, UUID text).
   A 7-day `count(*)` reads ~5.8 GB for 4 of 96 columns. The fix is a per-date
   dedup split (skip DedupExec for dates the sweep certified clean and the mem/hot
   legs do not cover); three specific blockers are recorded. Note this is a
   *different* problem from the footer poison — a LIMIT query is already free at
   any width (29 days = 490 ms), so do not benchmark the log explorer with
   `count(*)`.
6. **39 files / 55.2 GB exceed even the budget-derived repair reach** and need
   `optimize --recompress`.
