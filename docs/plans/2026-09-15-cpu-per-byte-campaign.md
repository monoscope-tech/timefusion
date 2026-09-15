# The CPU-per-byte campaign: four deploys, ~30% less CPU per row, and where the rest lives

2026-09-15. Goal: "10x traffic needs ~217 cores — unacceptable." Method: root
`perf` on the live process → bucket stacks by mechanism → patch our own forks →
deploy → re-run the IDENTICAL capture. Every number below is from prod.

## Results

| | campaign start | close |
| --- | --- | --- |
| whole-process CPU | 21.7 cores @ 1,650 rows/s | **12.3 cores @ 1,345 rows/s** (4-min avg) |
| CPU per row | 13.2 mcore·s | **9.1 mcore·s (−31%)** |
| pgwire p50 / p99 / p999 | 1,015 ms / 47.9 s / 184.8 s (09-11) | **40 ms / 0.54 s / 1.27 s** |
| RSS | — | 12.8 GiB of 120 (batch_size 4096 canary holds) |
| naive 10x requirement | 217 cores | **~123-135 cores** (band across builds: −26-31% CPU/row) |

## What shipped (all validated by the same instrument that found them)

1. **DataFusion `RecordOutput`** (`41162dd`): `output_bytes` per batch no longer
   rebuilds ArrayData per column + SipHash-dedupes every buffer. 7.9% → 0.2% of
   work samples. Cost: the metric over-counts sliced/shared buffers (display
   only; spill accounting untouched).
2. **Kernel `apply_schema` identity cache** (`de4c7f90` → `70463d0b` →
   `3718d059` → `a5898574`): batches whose schema the transform cannot change
   skip it. Took FOUR iterations because production profiles kept refuting the
   synchronization design: a global Mutex put 70% of the fast path into
   lock_contended; the RwLock replacement moved it to read_contended (6,300
   samples); **thread-local** finally eliminated the class (0.02%). At
   per-batch frequency across 30+ threads, ANY shared word is the bottleneck.
3. **`TIMEFUSION_QUERY_BATCH_SIZE`** knob, canaried 2048 → **4096** live: RSS
   flat, latency best-of-campaign. 8192 remains available; watch RSS and
   query-pool exhaustion per the config comment.

Also: tantivy fell from ~39% of CPU (09-13) to ~0.2% as a side effect of the
09-12 overlay/carry-forward work; SIMD baseline was already fixed (x86-64-v3);
`x86-64-v4` (AVX-512, host supports it) remains an untested canary via
`--build-arg TARGET_CPU`.

## Deploy 5 and what the last bucket turned out to be

`apply_schema` burned ~9% through its SECOND caller — the expression-evaluator
path, whose input arrays carry a fresh `Fields` allocation per batch. Deploy 5
(`e035e5db`) shipped evaluator-owned memoization: the first batch's identity
verdict transfers to later batches from the same input allocation, sabotage-
verified (forcing the fast path breaks a real checkpoint test).

**The post-deploy profile then settled what the bucket IS: required work, not
waste.** With deploy 5 live the transform still runs (~8% of work samples),
which means the verdict in production is non-identity — `apply_schema` here
genuinely changes fields (Delta column-mapping metadata). A cache cannot elide
work whose output differs from its input. The only further lever is a
plan-replay construction (precompute the field/metadata plan once, rebuild
arrays per batch without re-deriving it) — worth perhaps half the bucket for a
substantially larger fork patch. That is where micro-optimization honestly
ends: every VALIDATED-waste bucket is gone; what remains is required
transformation or architecture.

## The honest 10x arithmetic

Micro-optimization delivered ~1.75x of CPU headroom and is near its floor: the
profile shows no function above ~2%. The remaining gap to 10x (~123 needed vs
32-core cgroup / 48-core box) is WORK COUNT, not code speed: maintenance writes
~36 rows per ingested row and scans re-dedup nearly everything. Those levers —
certification, dedup-skip, rollup coverage — change the exponent; everything in
this campaign changed the coefficient. They are mapped in
`2026-09-12-surviving-10x-on-the-write-path.md`.

## Method notes that will save the next campaign

- dwarf perf on AMD/IBS pollutes leaves with sched-in frames: filter by DSO,
  drop syscall-stub leaves, and treat the FLAT capture as the weights.
- A laptop cannot resolve <10% effects under ambient load; validate CPU
  mechanisms on prod with identical captures.
- `SET datafusion.execution.batch_size` works per pgwire session — free prod
  A/B for engine knobs.
- Demangled-name regexes lie: `apply_schema_to_struct`'s mangled symbols did
  not match a `transform_struct` filter and hid the evaluator path for two
  deploys. Grep the raw symbols too.
