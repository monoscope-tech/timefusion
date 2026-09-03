# The `hashes` pushdown: measured unsafe, and already built

Follow-up to `2026-09-03-why-hashes-queries-time-out.md`, which ended on a gate:
measure `err:` selectivity, then decide whether to push the predicate below
`DedupExec` by declaring `hashes` monotone. Both halves of that gate have now
been measured. **The selectivity gate passes decisively. The safety gate FAILS.**
And the safe version of the optimisation already exists in the codebase.

## 1. Selectivity — the fix would be worth a great deal, for issues pages

Prod, 30-minute windows, read-only, per project:

| project | rows/30m | most common `err:` tag | selectivity |
|---|---|---|---|
| `dcad860a` | 136,094 | 43 | **0.032 %** |
| `87576849` | 86,140 | 135 | **0.157 %** |
| `28f62f01` | 79,970 | 435 | **0.544 %** |

The *most common* error tag in the busiest project matches about one row in 184.
A predicate this selective evaluated after a full-window materialisation is the
whole timeout.

**But the same is emphatically NOT true of the dashboard panels.** The
endpoint-stats panels filter `hashes @> ARRAY['{{var-endpointHash}}']`, and the
top endpoint hash in `28f62f01` matches **54,038 of 59,808 rows — 90 %**. No
prefilter helps there; those panels are scan-bound by nature and their fix is the
window or the projection. Do not quote a single "hashes queries" number: the
issues-page shape and the dashboard shape are opposite.

## 2. The tombstone hazard — checked, and it does not apply

Before pushing anything below the dedup, the DELETE path had to be checked: if a
DELETE appended a *minimal* tombstone with NULL `hashes`, a pushed
`array_has_all` would drop the tombstone at the leg, leave the older live version
as the only one dedup sees, and **resurrect a deleted row** past the
`deleted IS DISTINCT FROM true` filter.

It does not. `src/dml.rs:946` and the projection at `:1037-1051` build a
**full-row** tombstone — every column carried forward, only the marker
substituted. `hashes` travels with it. This hazard is closed.

## 3. The monotonicity gate — FAILED on real data

The optimisation's premise is that `hashes` only ever grows, so
`array_has_all` is monotone: if any version matches, the winning version matches.
Today's three monoscope writers (`BackgroundJobs.hs:2519/2537/2867`) all produce
supersets — append, append, and a `DISTINCT`-over-`unnest` set-union. So the
premise looks true from the source.

**The premise is about the data, not about today's source.** Prod data spans
months of older client code, and TF's own DML permits a full replacement
(`SET hashes = make_array(...)` — the existing test
`a_filter_on_an_updated_column_never_matches_the_superseded_version` does exactly
that). So the check was run against the data:
`scratchpad/monotone_audit.py`, over the **Delta-live** files of one full prod
partition-day (dead files are not the table — see
`2026-09-03-the-duplicate-measurement.md`).

```
project_id=87576849…/date=2026-09-02
  LIVE files 201 (err 0)   rows 1,864,064   keys 1,839,050   multi-version keys 25,011
  version pairs compared: 25,014
  SUBSET VIOLATIONS: 1
    'monoscope-ui' 77474d8a-…  older ['e583c276'] -> newest ['f0131962']
    stamps 2026-09-02 14:49:17 -> 17:51:34
```

**One version pair in 25,014 replaces its tag outright** — a single-element set
swapped for a different single-element set three hours later, on an endpoint hash
(no `err:` prefix). No appending writer can produce that, so it is either a
re-ingest of the same key with a recomputed endpoint hash, or the endpoint
remapping in `Endpoints.hs:544` ("remap … to their canonical hashes").

0.004 % is not a rounding error here, because **the failure is silent and
wrong-way**. Pushed down, a query for the retired tag admits the stale version,
drops the current one at the leg, and the stale version wins keep-greatest and
passes the filter above — a **ghost row**, presented as current, on the page a
customer uses to decide what is broken. A slow issues page is a complaint; a
confidently wrong one is worse.

**Verdict: do not declare `hashes` monotone. Not built, and it should not be.**

## 4. The safe version of this optimisation is already in the codebase

This is the part worth carrying forward. When `DedupExec` is SKIPPED for a leg,
`scan_delta_only`'s `readmit_mutable_filters` (`src/database/mod.rs:9512`) puts
the mutable-column predicates **back into the leg** — because with no dedup there
is no stale version to resurrect. Both skips are **default ON**:
`timefusion_read_dedup_skip_per_date` and `..._per_file`.

So on a certified date, `array_has_all(hashes, …)` is *already* pushed to the
scan, with no monotonicity assumption and no new code. The mechanism I was about
to build unsafely exists safely, and is gated on one thing:

> **Certification coverage.**

That closes a loop with the whole night's maintenance work. The `hashes` timeout
is not an isolated read-path defect to be patched around — it is another
consumer of the certification coverage that the dedup backlog is starving, the
same backlog behind `2026-09-03-the-frozen-mass-day-wide-dedup.md` and
`2026-09-03-why-the-frozen-mass-is-a-read-path-bug.md`. Draining that lane speeds
these queries **by the same mechanism**, and it is the reason to keep pushing
there rather than to open a second front.

## 5. What is left, ranked

1. **Certification coverage** — the safe pushdown, already built, waiting on the
   dedup lane. Highest value; already the night's main line of work.
2. **A tantivy index on `hashes`** — strictly safe with no client invariant: the
   prefilter's id-set half is sound on mutable columns (`id` is a dedup key, so
   `id IN (hits)` admits whole key groups atomically and the above-dedup filter
   rejects stale-only matches). `List(Utf8)` is already supported
   (`src/tantivy/mod.rs:207,230`). The cost is real: adding a column changes the
   table's index schema, so every existing index rebuilds, at a measured ~16/hour.
   Weeks to deliver, and the 29x rollup-index cost history argues for care.
3. **An `err:`-only path** — deliberately rejected. Restricting the pushdown to
   `err:`-prefixed tags would sidestep the one measured violation class, but it
   encodes a client's tag-naming convention in the query engine. No.

## 6. For the morning

- **The append-only assumption is false in the data, by a hair.** If monoscope
  *intends* `hashes` to be append-only, the endpoint-remap path is a bug worth
  finding on the client side; if it intends remapping, then this pushdown is
  permanently unavailable and route 2 is the way. That is Anthony's call, and it
  is the one decision here that TimeFusion cannot make for itself.
- **A one-in-25,014 measurement is why this was worth running.** Reading the
  three current writers said "append-only, ship it". The data said otherwise, and
  the source review would have shipped a silent correctness bug on the exact page
  a customer had already complained about.
