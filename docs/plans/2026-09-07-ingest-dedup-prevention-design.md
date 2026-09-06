# Ingest-time client-retry duplicate prevention — DESIGN (fable, 2026-09-07)

The strategic 100x complement to DV-dedup: DV makes each dedup pass cheap but does
NOT prevent duplicates (certification one-pass-delay persists). Preventing the
client-retry residual at ingest is what makes new dates born certifiable.

## THE PIVOT (discovered before coding)
The obvious rule "drop when incoming tiebreak <= seen" is **inert on otel**:
`version_append: true` + `stamp_version` (write/mod.rs:6766, called at
database/write.rs:615) OVERWRITES `updated_at` with a fresh monotonic stamp per
batch, so a retry always has a GREATER tiebreak — the `<=` never fires.
=> Use **CONTENT IDENTITY** (per-row analogue of landed-skip): drop a row iff its
exact client-visible content (ALL columns EXCLUDING the TF-stamped tiebreak) was
already flushed to Delta. A legitimate new version differs in content -> passes ->
keep-greatest resolves it as today. Makes dropping safe by construction.

## STRUCTURE — exact two-stage hash map (NOT bloom/cuckoo: FP = unique-row data loss)
Per-table `IngestDedupIndex` on BufferedWriteLayer (sibling of landed_digests):
`DashMap<u128 key_hash, u128 content_hash>` x 2 epochs (current, previous).
- key_hash = xxh3_128 over canonical serialization of schema dedup_keys
  (otel: timestamp i64 LE, LEN-PREFIXED service bytes, LEN-PREFIXED id bytes).
  LENGTH-PREFIXING MANDATORY (else ("ab","c")==("a","bc") collides constructibly).
- content_hash = xxh3_128 over ALL columns in schema order, each length-prefixed,
  EXCLUDING dedup_tiebreak iff version_append (exactly what stamp_version overwrites).
- Two-stage probe: hash 3 key cols (cheap) -> on key hit, compute full-row content
  hash -> drop iff equal. Full-row hash only at key-hit rate (~retry rate, 1e-5).
- Soundness: drop only when identical client-visible bytes already committed. Under
  keep-greatest, removing one of two content-identical rows is query-invisible. Only
  unique-row-loss path = 128-bit double collision ~6e-25/window = same bar as shipped
  landed_digest (128-bit). Every MISS direction fails toward a duplicate (DV-dedup
  backstop). REDUCTION, not guaranteed prevention.

## MEMORY — own budget on BufferedWriteLayer, OUTSIDE mem_buffer's ~21GiB cap
Rate ~3.8M rows/h. ~50 B/entry. Default cap 1 GiB (timefusion_ingest_dedup_max_mb=1024)
= 20M entries = ~5.3h coverage. At 100x: 1GiB=~3min coverage (degrades coverage, NOT
correctness). Do NOT chase 29GiB. Eviction = EPOCH-PAIR rotation (2 probes/lookup,
O(1)): rotate when current epoch > cap/2 bytes OR window/2 age (window default 6h);
drop previous, current->previous, fresh current.

## INTEGRATION
- PROBE (drop): in insert_bounded (write/mod.rs:1249) after compact_batch+bound_event_time,
  BEFORE reserve_with_backpressure + WAL append (durability-clean: dropped row never
  enters WAL). Per-row Arrow boolean mask + filter_record_batch; zero-hit batch passes
  through untouched.
- GATE: only live inbound ingest. WAL REPLAY bypassed by construction (replay uses its
  own loop ~write/mod.rs:1631, never insert_bounded) — LOAD-BEARING: filtering replay
  re-enters the rejected 09-02 "replay-time skip by position" design (silently reverts
  acked DML). Comment the probe site citing 2026-09-02-stop-manufacturing-duplicates.md.
  DML re-appends (bound=false): gate probe on bound==true.
- POPULATE: in prepare_flush after flush-time dedup_batches (~write/mod.rs:2819), recorded
  POST-COMMIT (alongside landed-digest bookkeeping ~:2929) — crash before commit leaves
  keys unindexed -> duplicate, never loss. Also populate on landed-skip DECLINE (~:2843).
- No double work: flush-time dedup_batches (same-buffer) + landed-skip (whole-batch WAL
  replay) + this index (post-flush-drain retry residual) are disjoint classes.

## KILL SWITCH + METRICS
- timefusion_ingest_dedup_mode: off (default) | shadow | enforce
- timefusion_ingest_dedup_max_mb (1024), timefusion_ingest_dedup_window_secs (21600)
- ingest_dedup.* stats: key_hits, dropped_rows_total (shadow: would_drop_rows_total),
  index_entries, index_bytes, epoch_rotations, probe_nanos_total.
- ROLLOUT: ship SHADOW first. Flip to enforce only when would_drop_rows/rows_ingested
  lands in the known 0.0004-0.0008% band over >=24h. Higher -> misfiring (version traffic
  flagged), abort. ~zero -> inert (hash-point mismatch), abort. Post-enforce proof:
  cert_declined_dirty_bins trends toward ~0 for NEWLY-INGESTED dates.

## BENCH + TESTS
- Bench: extend tests/membuffer_concurrency_bench.rs, off/shadow arms ALTERNATED,
  PRELOAD ~20M entries (empty DashMap is artificially fast). p99 delta < 5%.
- Tests (failing-first): insert->force_flush->force_evict->re-send identical -> enforce:
  count unchanged & dropped>0; same-key-different-content passes; UPDATE-after-flush still
  applies (bound=false bypass); unclean restart -> replay re-inserts freely (empty index)
  + landed-skip declines; no-dedup-keys table untouched; shadow counts but never filters.

## RISKS
1. Residual dup windows (drain-commit gap, restarts index unpersisted, epoch evict, 100x
   shrink) — all fail toward dupes, DV backstop. REDUCTION lever, value is downstream.
2. Data-loss: ONE channel, 128-bit double collision ~6e-25/window = shipped landed_digest bar.
3. Silent inertness = likeliest failure (a plausible spec ships dead — see the pivot).
   Shadow-rate band check is the mandatory detector; measure from a >=10min process over a
   real day, deploys frozen.
4. Semantic delta: survivor keeps ORIGINAL updated_at (vs today's retry's fresher stamp).
   Content identical. Flag so a future updated_at investigation isn't surprised.

## STATUS: designed 2026-09-07. Implement SHADOW-mode first (safe, off by default),
deploy AFTER DV-dedup validates + the observability batch, validate shadow rate >=24h,
then enforce.
