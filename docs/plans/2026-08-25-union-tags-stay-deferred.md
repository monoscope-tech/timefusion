# Tag-aware rollup-tier compaction stays deferred — and the reason is not the ledger

**Owner:** unassigned. **Status:** DEFERRED, with an explicit revisit trigger.
**Last reviewed:** 2026-08-25. **Supersedes:** `tasks/07`'s deferral reasoning.

## Verdict

Still deferred, but the two premises it was deferred on are both wrong and the
real reason is much better. **Premise (a) — "generation orphaning makes ~92% of
the tier dark" — is stale, not false:** the 92% was measured on 2026-08-22
*before* the orphan repair migration shipped and fired (`18bcf8a`, per-source
keying `2278e49`, now `ORPHAN_REPAIR_MIGRATION` at
`src/maintenance_coordinator.rs:683`), and nobody has re-measured since; the live
number is the `stale_generation` field of the `rollup_coverage_recovered` log
line (`src/database/maintain.rs:3652`). **Premise (b) — "the ledger will replace
tags, so don't invest in tags" — is wrong today:** the ledger landed as a
*boot-time seed and a verified shadow*, not as an authority. It has **no
publish-time writer** (its only production caller is the hourly Delta-tag replay,
`maintain.rs:3311`/`:3597`), its `routing_view` bridge **has no production
caller** (`storage.rs:3815`, used only by a test at `mod.rs:12305`), and the
hourly replay **overwrites** whatever the ledger seeded. The tags are still the
authority (`storage.rs:3663-3665` says so explicitly). So "wait for the ledger"
is not a reason — the ledger is not coming to rescue this soon.

The real reason to defer is that **writing a union slice tag breaks three
specific mechanisms today, one of them a correctness bug**, and the ledger as
built does not shield any of them. It is still the wrong week, for better
reasons, with a trigger condition that is now checkable rather than atmospheric.

---

## 1. What a union tag would break, concretely

The change is "merge files whose slices are contiguous and share a
partition/generation, write the union slice as the output tag". Three mechanisms
read those tags and all three assume the tag matches a slice the journal knows.

### 1a. `rollup_slice_complete` is exact equality — coverage would evaporate

`src/maintenance_coordinator.rs:2431-2440`: a tagged slice is considered
complete only when `task.key.slice == slice` for a task in state `Complete`.
Exact equality on the `TimeSlice`, not containment, not overlap.

A file re-tagged `(a, c)` from journal-complete slices `(a, b)` and `(b, c)`
matches **no** task. The hourly replay's filter (`maintain.rs:3572`,
`if !complete { continue }`) then drops it, and the merged file contributes
nothing to `rollup_slice_coverage`.

Worse, this is not merely a lost read. `record_readable_coverage`'s orphan sweep
(`maintain.rs:3341-3351`) pushes `(cell, vec![])` for cells it did not see this
pass, and `replace_many` treats an empty vector as **retire the cell**
(`storage.rs:3868-3875`). So the ledger entry is deleted too. **A successful
compaction would silently delete the coverage of what it compacted**, on both
the tag path and the ledger path, one hour later.

### 1b. `slice_retires` uses containment — duplicate data would stack

`src/rollup.rs:748-759`: a tagged file is retired by a republished slice only
when `file_start >= start && file_end <= end`. The "file wider than the slice"
case is explicitly asserted false at `rollup.rs:2110`.

A union-tagged file is by construction wider than any of the slices that built
it. When one of those slices is rebuilt — which happens routinely, on any
invalidation — the new output cannot retire the union file. Both live. The rows
are counted twice.

**This is a correctness bug, not a fragmentation bug**, and it is the same shape
as the tier-double-counting incidents already on record. It is the single
strongest argument for deferring.

### 1c. `carried_coverage_tags` deliberately refuses unions

`src/database/mod.rs:7176-7195` carries the six coverage tags forward **only when
every input agrees on every one**; any disagreement, or any untagged input,
yields an empty map. Tests at `mod.rs:11427-11433` pin "differing slices must NOT
be merged into a span".

That refusal is the fix for a real incident — `tag_sorted` previously copied only
the sorted-run tag and silently erased rollup tier coverage (`mod.rs:7170-7180`).
So the union-tag work does not merely *add* a capability; it **removes a guard
that was installed after an incident**, and would have to argue that the incident
cannot recur. It also has to be threaded past the fact that rollup tiers are
currently excluded from packing altogether (`maintain.rs:322-328`, skip at
`:360`, with a 25-line rationale at `:300-321`).

## 2. Why the ledger landing does not change the answer

Five commits landed the ledger and all five are real work:

| sha | what it added |
|---|---|
| `3ec8003` | `CoverageEntry`, `CoverageCell`, `trait CoverageLedger`, `merge_coverage`, `JsonCoverageLedger`, the sidecar const |
| `f6b50e5` | the Delta-tag replay populates the ledger; ledger field on `Database` |
| `b9572cf` | the verifier (`held != proved` → `coverage_ledger_disagreements`), `replace()`, `retire_before` |
| `98e72d5` | `CoverageEntry.files` — entries name the files that serve them |
| `4bbd0fe` | the ledger records only what the READ PATH would serve (writing moved into the filtered loop, past `rollup_slice_complete` and the generation match) |

And `24f96ea` deleted the `TIMEFUSION_COVERAGE_LEDGER_READS` flag, so the seeding
path ships on.

But read what that adds up to. The ledger is **derived from the tags, once an
hour, by replaying them**. Its single production writer is
`record_readable_coverage`, called from `recover_rollup_coverage`. `routing_view`
— the documented bridge for moving reads off the tags — has **no production
caller**. The query read path never consults the ledger; it reads the in-memory
`rollup_slice_coverage` map (`mod.rs:3967-4022`), which the replay rebuilds from
tags and overwrites every hour. `storage.rs:3663-3665` states the design position
plainly: the tags remain the authority.

`4bbd0fe` in particular makes the entanglement **tighter**, not looser: by
recording only what passes `rollup_slice_complete` and the generation match, the
ledger now inherits §1a exactly. A union-tagged file fails
`rollup_slice_complete`, so it is excluded from the ledger by design.

**Conclusion:** the ledger has not yet earned the right to be the reason for
anything. It is a good shadow and a good boot seed. Task 17 (durability and scale
before the tags are dropped) is still the gating work, and note the scale problem
`98e72d5` created: `CoverageEntry.files` makes the sidecar grow with **file
count** (~13,800 live tier files), and every change re-serializes every cell
(`storage.rs:3659-3666`) — which is the sidecar getting *larger* precisely as the
compaction under discussion would make it smaller. That interaction is an
argument for doing task 17 first, not for doing them together.

## 3. The premise that needs re-measuring before any of this matters

**Do not compact a tier that is dark.** If a large fraction of tier files carry a
generation the reader rejects, compacting them buys a file-count number and moves
no query — which is exactly the half-win `tasks/07`'s own done-when clause
forbids.

The 92% figure (`docs/plans/2026-08-22-query-latency-matrix.md:1610-1638`: 37
usable of 461 `(project_id, date)` cells in a 35-day window, while
`contiguous_days` reported 30) predates the orphan repair. The repair shipped,
fired, and is keyed per source. Nothing in the repo states the current fraction.

The measurement is one log line, read against uptime:

```bash
ssh ubuntu@captain.s.past3.tech \
  'docker service logs srv-captain--timefusion --since 2h 2>&1 | grep rollup_coverage_recovered | tail -3'
# read `stale_generation` against `recovered` on the SAME line
```

Traps that apply: the counters are process-scoped and prod restarts every 20-40
minutes, so read `docker service ps` uptime first; and `--since 2h` has returned
five minutes of logs before — parse the timestamps out of the fetch before
quoting a duration.

Note also the root cause behind the orphaning is still live in the planner:
`partitions_of` (`maintain.rs:679`) builds the planner's `covered` set from file
paths and non-emptiness only, never consulting generation. So orphaned dates
still *look* covered to the planner and are not re-enqueued except by the repair
migration. A future spec change re-orphans everything again — `generation_id`
(`rollup.rs:69-75`) is an FNV over `format!("{spec:?}")`, so adding one measure
invalidates every previously built slice. That is a standing hazard independent
of this task and worth its own item.

## 4. Decision

**Defer.** Do not write union tags this week.

Ranked reasons:

1. §1b is a correctness bug that produces double-counted rows, and this codebase
   already has that incident.
2. §1a would make a successful compaction delete its own coverage an hour later,
   through both the tag path and the ledger path.
3. §1c means the work starts by removing a post-incident guard.
4. The benefit is unquantified until §3 is re-measured — the tier may be largely
   unreadable, in which case compacting it moves no query.

Note what is *not* on that list: "the ledger will replace tags soon". It will
not, on current evidence, and that reason should stop being cited.

## 5. Revisit trigger — check all four, in order

Revisit when **all four** hold. Each is checkable; none is a judgement call.

> **MEASURED the same day, 2026-08-25 — trigger 1 is all but satisfied, and the
> 92% is FALSIFIED on the live build.** Read from `rollup_coverage_recovered` on
> image `d3b44f7`, two hourly passes (08:06:2xZ and 09:06:3xZ) on one container
> started 08:01:21Z:
>
> | source | 08:06 | 09:06 | Δ |
> |---|---|---|---|
> | `otel_metrics` — `recovered` | 4,719 | 4,787 | +68 |
> | `otel_metrics` — `stale_generation` | **0** | **0** | 0 |
> | `otel_logs_and_spans` — `recovered` | 20,345 | 20,455 | +110 |
> | `otel_logs_and_spans` — `stale_generation` | **0** | **0** | 0 |
>
> Not "absent from the window" — the line is present on both sources and the
> field reads zero, so the ratio is 0%, not ~92%. **What is still missing for
> trigger 1 as written, and it fails on BOTH axes:** all four lines come from the
> SAME container (started 08:01:21Z), and the two passes fired at **5 min and
> 65 min uptime** — so neither "≥2h uptime" nor "two separate containers" is met.
> Take the confirming read on the next long-lived container rather than treating
> this as closed.
>
> **A new residual surfaced in the same read, and it is NOT the generation
> problem:** `unverifiable` is byte-identical across both passes — 2,037 and
> 2,349, ~4,386 slices — while `recovered` grew by 68 and 110. That is a FROZEN
> population recovery cannot verify. The generation-orphan reason for deferring
> this work is gone; this one takes its place and is unowned.
>
> Trap that bit during this measurement: a server-side-grepped fetch with
> `--since 2026-08-25T08:00:00Z` returned only the 08:06 pair and exited 0, which
> would have supported "the line fired once". The four occurrences were assembled
> from three fetches with independently verified windows. **Never quote an
> occurrence count from a single `docker service logs` fetch.**

1. **The tier is readable.** `stale_generation / recovered` from
   `rollup_coverage_recovered` is below ~10% on a container with ≥2h uptime,
   measured on two separate containers. If it is still high, fix the orphaning
   first — that is a bigger query win than the file count and it is a different
   piece of work.
2. **Retirement handles a wider file.** `slice_retires` (`rollup.rs:748`) accepts
   overlap-with-containment-of-the-republished-range, or the merged file carries
   its constituent slices rather than their union, with the
   `rollup.rs:2110` assertion updated deliberately and a test that a rebuild of
   one constituent retires the right thing. **This is the hard part and it must
   land first, on its own, with its own test.**
3. **Completeness handles a wider file.** `rollup_slice_complete`
   (`maintenance_coordinator.rs:2431`) recognises a slice covered by a *set* of
   complete tasks, not only by an exactly-matching one — otherwise §1a stands.
4. **The ledger is the authority, or is explicitly declared never to be.**
   Either `routing_view` (`storage.rs:3815`) has a production caller and the read
   path consults the ledger rather than the tag-derived map, and task 17's
   durability/scale work has landed; **or** a decision is recorded that tags stay
   authoritative permanently, in which case 2 and 3 alone are enough and the
   ledger stops being cited in this discussion at all.

If 1 fails, do the orphan work. If 2 or 3 fail, do those — they are prerequisites
with independent value, since both are latent hazards for any future rewrite of
tier files, not just for union tags.

## 6. Constraints that survive unchanged, for whoever picks this up

Carried from `tasks/07`, all still verified against the code:

- **Contiguity is the whole precondition.** A union tag over non-contiguous
  slices claims coverage of the gap — a tag claiming coverage that is not there
  gives wrong query results.
- **Generation must match.** Do not assume generations within a partition agree;
  §3 is the reason.
- **`source_rows` is physical, not logical** (`TAG_SOURCE_ROWS`,
  `maintenance_coordinator.rs:123-137`, written with a `-1` sentinel when
  unknown at `maintain.rs:1924-1939`). A union tag must sum something; name the
  population before summing. Note task 12 is closed — real churn beats benign
  churn 122:1 — so the witness is not being fixed, which makes getting the union
  sum right entirely this work's problem.
- **delta-rs `OPTIMIZE` strips custom tags** (`src/database/compact.rs:119-125`;
  no tag carry-forward anywhere in that file). Route the merge through the
  consolidation path with an explicit tag write, never through a plain
  `OPTIMIZE`.
- **Interactive `OPTIMIZE` cannot compact a live box** — it sorts the whole
  partition inside the shared query pool and gets a small share beside dashboard
  traffic.
- **The done-when has two halves.** Live-file count down ≥10x on one project
  **and** the 30d whale dashboard query re-measured against
  `docs/plans/2026-08-22-query-latency-matrix.md`. A file-count win that does not
  move the query is not the win being claimed.
