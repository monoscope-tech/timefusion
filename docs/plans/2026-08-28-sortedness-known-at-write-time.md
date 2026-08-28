# Sortedness is known at write time — stop reading it back

2026-08-28. Branch `next/sortmark`. Shipped as three commits.

## The premise I started with was half wrong, and that matters

The task was framed as *"persist sortedness at write time, which stops the
repair backlog regenerating on every restart."* The restart half was **already
fixed**. `persist_verified_sorted` / `load_verified_sorted` exist, write
`repair_verified_sorted.txt` beside the WAL dir, boot-load at
`database/mod.rs`, and carry a restart test. That landed by the 08-20 module
split. The field doc still said "excluded after one footer read per process",
which is what made the stale framing survive — that doc is now corrected.

**What was actually missing is the probe, not the re-probe.** Admission was
O(every file ever written), and here is the arithmetic that makes that bite:

- `plan_compaction_debt` calls a file a Repair suspect when it is not in
  `repair_verified_sorted`.
- The only thing that put a file in that set was `repair_bin_already_sorted` —
  i.e. reading its footer back.
- Flush is the dominant producer of files and does **not** tag its output:
  **1,593 of 1,648** prod add actions carry no tags at all (measured 08-19).

So every flushed file became a suspect and bought its exoneration with a ranged
read, one `.take(1)` per plan, behind repair's rewrite budget. But the write
path already knew the answer — it *chose* the footer. Recording it there makes
the suspect set O(files actually written unsorted), which is the set repair
exists to fix.

## What shipped

Marking at the three points where a commit is known to have landed: the staged
flush commit, the coalesced group commit, and `record_wave_landed` (packing,
dedup, repair output). The coalesced path marks **per unit**, not per group — a
group is one commit but many prepared writes, and one unit degrading to an
unsorted write must neither exonerate nor be exonerated by its neighbours.

Plus a **seeding sweep** for files that already exist, because neither marking
nor `load_verified_sorted` reaches the fleet that was on storage the day this
ships. Bounded three ways (5,000 files per sweep, 16 in flight, unknowns only),
detached from boot, repeated hourly.

## Two things that would have failed silently

**1. Encoded vs decoded paths.** Delta stores `Add.path` URL-**encoded**;
`LogicalFile::path()` — which admission and the probe both key on —
**decodes** it. Marking the raw form inserts strings nothing ever looks up: the
counter climbs, every file stays a suspect, and the feature looks finished. Any
tenant id containing a character Delta encodes is enough. Verified in the
dependency's source, not assumed, and pinned by a test that asserts the encoded
form is *absent*.

**2. Declared order ≠ stamped footer.** `build_writer_properties` stamps the
footer only when `declare_sorted` **and** `schema.sorting_columns()` — the
*conversion* — is non-empty. The conversion maps names to physical parquet leaf
indices and drops what it cannot place, so a schema can declare an order that is
never stamped. Prod 2026-08-07 held a 924 MB file in exactly that state.
Marking on the declaration would have made that file permanently invisible to
the repair that exists to fix it. The predicate tests the conversion.

`false` is always the safe value: it costs a footer read, never a wrong
exoneration. That is why a resumed staged bin takes it — its intent manifest
does not record what an earlier *process* declared, and guessing about someone
else's write is not worth one avoided read.

## A leak this work would have introduced

The persisted list was compacted **at boot only**. Correct when the footer probe
was its sole feed — a few paths per repair tick — and prod restarting every
20-40 minutes hid the rest. Every commit now appends, orders of magnitude
faster, and **the process that would grow it without bound is precisely the
long-lived one this work is meant to produce.** Bounding it at boot only would
have made the leak a reward for fixing the restarts. Runtime compaction now
amortizes to one rewrite per `REPAIR_VERIFY_PERSIST_CAP` appends, trimming the
in-memory set to match.

## How to tell whether it worked

`repair_sorted_at_write_total` is the **falsifier**, not the success metric. A
silently-empty path set looks exactly like a working feature with nothing to do,
so **0 on a busy process means the marking is not reaching the commit path.**
Read it against `flush_sort_unsorted_fallbacks_total`: this counts files whose
footer we stamped, that one counts files we could not.

Then the real question, on a container with ≥1h uptime:

- `footer_repair_seed_swept` logs `candidates` / `verified` / `unsorted`. Both
  numbers always, because `verified` alone cannot distinguish "the fleet is
  healthy" from "the sweep found nothing to look at" — and the difference
  between those is the entire size of the repair backlog.
- `pending_repair` was **589** before this. If admission is now
  O(actually-unsorted), it should fall toward the genuine unsorted population
  and stop being re-derived at full width every scan.

**Trap, per the standing rules:** every counter here is process-scoped and prod
restarts every 20-40 minutes. Stamp `docker service ps` uptime and the running
image on any number before quoting it.

## Verification

`cargo lint` exit 0. Five targeted tests pass, and the two that matter were
**witnessed failing first** by making `mark_written_sorted` a no-op — the
falsifier assert fires with "the marking is not reaching the commit path", which
is exactly what a wiring bug would produce. Without that assert an empty marked
set would satisfy `marked == live` on an empty table and the test would be
vacuous.

The end-to-end test asserts the marked set equals the snapshot's
`LogicalFile::path()` set across **both** commit paths. That comparison is
against real delta-rs output rather than our idea of it, because the convention
at risk lives in the dependency. It does **not** cover encoding — the project
ids in it contain nothing delta-rs encodes — and the unit test covers that
instead. Stated because a test that looks like it covers something it does not
is worse than no test.

## Not done, deliberately

The other ranked sorting items are untouched and remain separate: derive the
write cap from the sort budget in decoded bytes; enqueue repair immediately on a
flush fallback; sort the DML rewrite path (`dml_writer_properties` passes
`declare_sorted=false` at 2 of 3 sites); fold repair into dedup/compaction;
order repair by query impact.
