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
detached from boot.

### The sweep's cadence is load-bearing, and my first version was a no-op

I wrote it as an hourly loop. Two facts already recorded in this repo kill that,
and only kill it when you put them side by side:

- the sweep is spawned inside `start_maintenance_schedulers`, so its **first**
  pass runs while tables are still loading — maintenance workers wait ~300s for
  that preload for precisely this reason — and reads nothing;
- **prod containers live 20-40 minutes**, so an hourly retry never fires a
  second time.

Every boot would have swept nothing, forever, while looking like it had run. And
the empty path took an early return that **skipped the log line**, so the
evidence would have been silence — indistinguishable from a fully-seeded fleet.

Fixed both: retry every 60s until a pass actually READS a table, then hourly for
the trickle from paths that do not mark; and return + log `tables_read`,
including on the empty path. `tables_read` is not a statistic. It is the only
thing that separates "nothing to do" from "ran too early".

The general lesson: **a periodic task's interval has to be checked against the
process's lifetime, not just against the work's natural period.** On a box that
restarts every 20-40 minutes, anything hourly runs exactly once — at the worst
possible moment, during startup.

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

## Blocking IO, again

Runtime compaction (below) read and rewrote a multi-MB file with bare
`std::fs` while holding the mutex — reachable from the **flush commit path**.
The append beside it was already wrapped in `without_blocking_the_worker`; the
new code was not. Inline blocking IO on a tokio worker is this codebase's
recurring incident shape: a journal fsync on the ingest path was worth 5.7 lag
warns per minute until it moved. Wrapped now.

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

## The change broke two e2e tests, and the one that PASSED was the problem

Both fixtures build repair work by flushing sorted rows. Marking now records
those at write time, so the fixtures have no repair work at all.

- `a_second_table_skips_its_repair_tick_rather_than_sharing_the_light_pool`
  failed loudly on `repair_ticks_yielded = 0`. **Not a broken permit — an empty
  queue.** The permit is taken *before* planning, so the counter does not depend
  on there being work; it depends on the two passes OVERLAPPING. With zero
  suspects a pass completes in a single poll, and `tokio::join!` polls
  sequentially, so the first releases the permit before the second is ever
  polled. The test's real precondition was "the pass does enough IO to yield",
  which was incidental and is now gone.

- `one_repair_pass_clears_every_sorted_suspect_not_one_per_pass` **passed, and
  should not have.** It counts lines in `repair_verified_sorted.txt` and asserts
  `cleared >= before` — but the flush now appends to that same file, so the
  assertion was satisfied by the writes themselves. The repair pass could have
  cleared nothing. This was found only by asking what a green test was proving,
  which is the habit worth keeping: **the failing test cost an hour; the passing
  one would have cost a regression.**

Both now disable the mechanism for their fixture via a real config flag,
`timefusion_repair_mark_sorted_at_write` (default on). Not a test seam — a
mechanism that decides a file will never be offered to repair deserves an off
switch on the same reasoning as `timefusion_repair_resume_enabled`. Documented
limit: it stops NEW marks and does not un-mark anything, so recovery from a
wrong exoneration is deleting the file and restarting. It gates the seeding
sweep too, because gating only the marking leaves the sweep re-deriving the same
answer an hour later — a kill switch that does not kill.

The better fix, genuinely-unsorted fixtures, is **not reachable**:
`flush_sort_pool_bytes()` floors at 64 MB so the pool cannot be starved into the
unsorted fallback, and the sibling test's own docstring already records that
`with_sort_skip_bytes(0)` stopped producing footer-less files once flush began
escalating to a pooled sort. Manufacturing one needs a hand-written parquet.

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

## PROD VERIFICATION — image `e50e50d`, deployed 2026-08-28 16:01Z

Three log lines, and they vindicate the two cadence/logging fixes exactly:

```
16:01:43  footer_repair_verified_loaded  loaded=22545 dropped=0
16:01:43  footer_repair_seed_swept       tables_read=0 candidates=0 verified=0
16:02:55  footer_repair_seed_swept       tables_read=6 candidates=5000 verified=5000 unsorted=0
```

- **The first sweep read nothing** — the predicted boot race, running ahead of the
  table load. It is visible ONLY because the empty path was made to log; with the
  original early return this was silence, indistinguishable from a healthy fleet.
- **The 60s retry did the real work**: 6 tables, 5,000 files probed, **5,000
  verified sorted, 0 unsorted**. With the original hourly retry this second pass
  would never have fired — the container dies in 20-40 minutes — and the seeding
  would have been a permanent no-op that logged like a success.
- **`unsorted = 0` of 5,000** is the substantive result: every file probed was
  already correctly sorted. That is 5,000 pure suspects, each of which was
  costing footer repair a ranged read, now permanently exonerated.
- `loaded=22545` is the pre-existing persisted set, reloaded at boot.

**The falsifier does not fire:** `repair_sorted_at_write_total` = **2**, non-zero,
so the marking reaches the commit path. Stated honestly: 2 is small, and
consistent with a ~12-minute-old container against a 600 s flush interval — about
one flush cycle. **It must be re-read at higher uptime; a value that does not GROW
is the same alarm as a zero.**

`pending_repair` = **578** (589 before). Barely moved, and that is expected:
`candidates` hit the 5,000 per-sweep cap, so most files remain unknown and a cell
keeps its Repair task while ANY of its files is a suspect. The cap plus
restart-driven re-seeding means roughly 5,000 files per boot; the number to watch
is whether `pending_repair` trends down across boots, not within one.

## The e2e suite is red on master, and it is not a flake

`smoke::count_star_returns_correct_value` failed during this work. It is filed as
a flake (`tasks/01-NEXT` item 10, "1 failure in 2 full runs"). It is not one, and
it is not caused by this change. Both claims were established by control rather
than by argument:

1. **Cheap control:** a full e2e run with
   `timefusion_repair_mark_sorted_at_write = false` for every test — which makes
   marking AND seeding inert — failed **identically**, same test, same position.
2. **Expensive control:** pristine `origin/master` (`07185b46`), its own
   worktree and its own `CARGO_TARGET_DIR` so it could not be served this
   branch's binary, failed the **same test at the same position**, 58/59.

**Why it is a real bug.** Seven INSERTs, each awaited and therefore acked,
then `COUNT(*)`. It returned **4**, then **5**, then **4** — a *variable*
shortfall, so a race, not an off-by-N and not a test miscounting.

**My first hypothesis was count pushdown, and it is REFUTED.** The sibling smoke
test counts with `... AND id = $2` and always passes; the failing one filters
only by `project_id`, which looked like the count-pushdown shape. It is not:
`finalize_window` does `let lo = lo?`, so `match_count_plan` requires a lower
timestamp bound, and the failing query has no `timestamp` predicate — pushdown
never engages for it. Recorded because I asserted it before reading that
function. The two tests differ in TWO ways (query shape *and* seven rows versus
one), and I attributed to the interesting one without checking that its
suspected path was reachable at all.

**What is left:** the seven rows share an identical timestamp (the test formats
`Utc::now()` once, to second precision, before the loop) with distinct ids, under
`dedup_keys = [timestamp, id]`. The live suspects are the read-side dedup /
merge-on-read path and the MemBuffer+Delta union's time-range exclusion. Start by
establishing which leg loses them.

An earlier hypothesis of mine — that the seeding sweep's per-file
`table_ref.read()` was delaying `refresh_table_snapshot` into a stale snapshot —
was **refuted**: the failure survived that fix. The fix is kept regardless,
because background hygiene should not be able to contend with the foreground.

Worth stating plainly: **rate-matching a known flake is not evidence of the same
cause.** "1 in 2, same as recorded" is what made this look settled for three
days. It took a control to separate the questions, and it took reading *which*
assertion failed to see the pre-existing thing was never a flake at all.

## Not done, deliberately

The other ranked sorting items are untouched and remain separate: derive the
write cap from the sort budget in decoded bytes; enqueue repair immediately on a
flush fallback; sort the DML rewrite path (`dml_writer_properties` passes
`declare_sorted=false` at 2 of 3 sites); fold repair into dedup/compaction;
order repair by query impact.
