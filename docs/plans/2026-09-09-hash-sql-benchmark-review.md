# Hash SQL benchmark review

The harness uses the merged dd5647f implementation with real Delta files,
local MinIO, physical Tantivy sidecars, and the ordinary SQL planner.
It checks each hourly bucket against an independent arithmetic oracle.
Repeated array terms count once; overlapping tag predicates form a union.
An unindexed newer version removes the old matching row from the result.
The harness requires the expected histogram and uniqueness counters.
A timeout, wrong bucket, or wrong route fails the run.

First rs-distill pass: reuse the existing database, MinIO configuration,
record conversion, physical index builder, and SQL session helpers.
Predicate, coverage, and route are enums. Query setup and expected results
remain separate from measured execution. The nested oracle loops remain
because explicit row-by-row counting is easy to compare with generated data.
Benchmark JSON output is the artifact, not application logging.

First rs-evasion pass: no hit cap, fake visibility mask, forced route,
ignored error, unsafe code, or assertion suppression is introduced.
The partial-coverage case performs a real newer-version insert. Its index
is deliberately absent to measure the fallback, not to manufacture a pass.
Review found that workers should stop before temporary storage is dropped.
The harness now awaits Database::shutdown and propagates its failure.
Shutdown occurs after measured queries and before publishing the result.

Second scoped review: warm uniqueness proofs are an explicit setup condition,
not evidence of cold-reader performance. The ordinary route uses
count(timestamp), which is equivalent on this non-null fixture, and the
native route uses count(*). Arm order reverses each repetition. Timing
includes SQL planning and collection, but excludes network wire overhead.
No remaining scoped finding. Optimized execution, cold-reader measurements,
and concurrent-ingestion measurements are still pending.

The production probe lives in bench/hash_histogram_production.py. It checks
running task images before and after sequential queries. All sixteen pairs
must complete and agree, and every native query must finish within three
seconds, before its acceptance result can pass. Global counter deltas do
not establish per-query attribution. Python compilation passed. An actual
SSH check with an intentionally wrong image exited 1 before reading the DSN
or running SQL and created no result artifact. Production execution awaits
the deployment; no performance improvement is claimed from this check.

Cold-reader extension: reuse the real restart fixture's configuration clone
and separate data/index directory. Every cold arm gets a new Database and
TantivySearchService. It must recover existing proofs from storage and pass
the same exact bucket and route assertions. ReaderState labels each sample.
The shared constructor keeps writer/reader wiring identical in both modes.
Each case has four warm repetitions and one cold sample, for 240 queries.
A single cold sample is not a latency distribution. MinIO and OS page caches
remain warm; this measures fresh application caches, not cold storage media.
Database construction is outside the timer; SQL planning and collection
remain inside. Each cold database shuts down before its directory is dropped.

Follow-up rs-distill review consolidated database/index construction instead
of duplicating the setup. Follow-up rs-evasion review checked that the
set-once search service is attached to a newly constructed database, not a
clone of the warm database that would silently keep its old reader. No cache
reset API or production configuration knob was added. Execution is pending
the current optimized build; source review alone does not establish results.

Release-scope review: rename the production script result to probe_passed.
Two repetitions over this saved error and an absent hash cannot establish
the plan's proposed p95 < 1 second and p99 < 2 seconds release targets.
The three-second bound is only an initial smoke gate. Full validation also
requires common endpoints, log patterns, overlapping hashes, 3/7/30-day
windows, non-hourly buckets, recent data, chart API timing, concurrent
ingestion, and maintenance progress. No smoke result can close that scope.

Optimized execution completed successfully on 300,000 events (10,000/day).
All 240 bucket and route assertions passed, including cold persisted proofs
and unindexed replacements. Setup took 21.762 seconds; proof warmup took
3.628 seconds. Compressed raw results and a summary with source/benchmark
hashes are saved under evidence/2026-09-08-hashes/local-histogram-sql-optimized-300k*.

At 30 days, complete warm native medians were 5.78–18.72 ms; ordinary
medians were 41.63–46.26 ms. Complete cold native samples were 217.88–244.47 ms
versus ordinary 100.24–138.81 ms. Partial warm native medians were
302.82–428.96 ms versus ordinary 57.20–69.78 ms. Partial cold native samples
were 524.05–584.75 ms versus ordinary 122.33–157.02 ms. These are local
fixture results, not production latency forecasts or p95/p99 estimates.

The partial-coverage and cold-reader costs are material gaps, even though
this small workload finishes below one second. The unchanged optimized
binary is now running 100,000 rows/day (three million total), session 25216,
with /tmp/timefusion-hash-sql-optimized-100000.json and .log outputs.
Use that scale result and production observations before choosing a fix.
Do not present complete warm performance as evidence that the recent or
partially covered production tail is fast.

The three-million-event baseline passed all 240 cases. Complete warm 30-day
medians were 8.47–142.81 ms, versus ordinary 122.97–156.40 ms. Partial warm
medians were 730.08–980.74 ms, versus ordinary 152.90–175.20 ms. Partial cold
samples were 1,416.89–1,801.51 ms, versus ordinary 271.10–295.33 ms.
Raw and summarized scale evidence is saved as local-histogram-sql-optimized-3m*.

A diagnostic build now retains symbols and adds existing SearchStats phase
counters to each measurement. It passed all 240 cases too. Complete cold
30-day queries recorded 30 blob fetches: fetch-plus-unpack accounted for
644–752 ms, with reader opens about 21–22 ms, within 709–840 ms total.
Warm partial cases recorded zero fetches and opens but still took about
736–1,012 ms. Fetch counters include unpacking, not just network time.
This separates a cold-cache cost from the remaining visibility/read work.

The diagnostic binary is hash_histogram_sql-b0f1217e96d29072, built with
`cargo rustc --release --bench hash_histogram_sql --locked -- -C strip=none`.
Its symbol table is present; the original 92a1b55615e170c8 binary is stripped.
The first sample attempt found the completed process and took no profile.
A repeat now triggers sample automatically at the first partial-coverage
fallback warning. Treat that repeat as profiled diagnostic data, not an
unprofiled latency baseline.

Review: Measurement and IndexIo keep metrics in named typed fields and reuse
the existing counters. Cold measurements retain their own search service.
No application behavior, runtime setting, or route assertion changed.
