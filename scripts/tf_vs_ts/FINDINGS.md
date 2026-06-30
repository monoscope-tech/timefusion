# TimeFusion vs TimescaleDB correctness audit — `otel_logs_and_spans`

Date: 2026-06-29. Source-of-truth = TimescaleDB (prod-eu). monoscope dual-writes
every row to both, so a **settled** window must match exactly. It does not.

## Verdict: NOT SAFE to cut over to TimeFusion yet.

Three independent defects make customer query results diverge from the
source-of-truth. The first two are correctness bugs; the third is by-design but
still breaks query semantics.

## Defect 1 — Episodic row LOSS in TF (TF distinct < TS distinct)

Per-window distinct-id comparison (10-min windows):

| window (UTC)        | project  | TS distinct | TF distinct | TF loss |
|---------------------|----------|------------:|------------:|--------:|
| 06-24 14:00         | dcad860a |      21,861 |      13,730 |  −37%   |
| 06-29 18:00         | dcad860a |      30,795 |      24,620 |  −20%   |
| 06-29 14:00         | dcad860a |      31,295 |      28,082 |  −10%   |
| 06-27/28, 06-29 06h | dcad860a |        =    |        =    |   0%    |
| 06-24 14:00         | 98fdd4f3 |       6,501 |       3,404 |  −48%   |
| 06-29 10:00 (flood) | 6297304f |  11,409,556 |      20,904 | **−99.8%** |

Loss is **load/incident-correlated**: ~0% on quiet windows, 10–50% on busy days,
catastrophic during a burst. TF is not lagging (max(timestamp) matches TS to the
millisecond), and every missing id is *entirely absent* from TF (not time-shifted).

**Mechanism:** the base dual-write (`bulkInsertOtelLogsAndSpansTF`, `WriteBoth`,
`Telemetry.hs:1136-1173`) is transactional — a TF failure/under-persist DLQs the
batch as `tf-failed` for later `WriteTfOnly` replay. But under memory
backpressure/OOM TF rejects writes, and the **DLQ does not drain** (known
commit-starvation, see memory `dlq_commit_starvation`), so those rows are
*never* replayed into TF while PG keeps them → permanent gap.

## Defect 2 — Duplication in TF (TF total > TF distinct), unique in TS

06-25 14:00 dcad860a: TF total **23,376** vs TF distinct **11,688** (exactly 2×);
id `90997eed-…` = 2 rows in TF, 1 in TS. So `COUNT(*)` over that window is ~2×
the truth on TF (and still missing ~30% of distinct ids).

**Mechanism:** TF has no idempotent conflict key (PG upserts on id). Any Kafka
redelivery or DLQ replay re-appends rows TF already had. The main trigger is the
`TuplesOk` 0-row-success swallow (`Telemetry.hs:1049-1059`): TF lands the rows
but reports the wrong wire status → cross-check declares a false failure → DLQ
`WriteTfOnly` replay inserts the same rows a second time. PG dedupes; TF doubles.

## Defect 3 — Enrichment gap (BY DESIGN): `hashes`, `http.route`, parameterized `url.path`

On rows present in BOTH (settled 06-28 14:00 window, 403 shared rows): every
scalar / timestamp / JSON-blob field matches exactly — EXCEPT:

- `hashes`: TS `{3dec0490,pat:86778d10}` vs TF `{}` (empty) — 346/403 rows.
- `attributes.url.path`: TS parameterized `/v1/customers/{param}`; TF raw
  `/v1/customers/cus_UUmH…`.
- `attributes.http.route`: present in TS, absent in TF.

**Mechanism:** base row is written un-enriched to both; enrichment is applied
*after* via background UPDATEs. UPDATE-1 (parameterized path + `http.route` +
hashes merge) is **PG-only by design** — DataFusion can't run the
`jsonb_build_object`/`now()` SQL (`BackgroundJobs.hs:1719-1748`). UPDATE-2
(`pat:` hash tag) is dual but best-effort/circuit-broken on TF
(`BackgroundJobs.hs:1565-1584`).

## Downstream query impact
- Volume / throughput / count-over-time charts: WRONG (loss + duplication).
- Endpoint/route breakdowns & anomaly detection: WRONG (no `http.route`; raw
  high-cardinality paths instead of `{param}` templates).
- Log/span pattern grouping & dedup (`hashes`): BROKEN (empty on TF).
- Latency percentiles (`duration`): values correct per-row, but skewed by missing rows.
- Exact field lookups on rows that exist: CORRECT (verified, apart from the 3 enrichment fields).

## Cutover gating criteria (verify with the harness here)
1. TF base-write durable end-to-end: fix DLQ drain + TF backpressure rejection.
   PASS = `TF distinct == TS distinct` for all settled windows across days.
2. TF idempotency: add a dedup/conflict key or fix the `TuplesOk` false-failure.
   PASS = `TF total == TF distinct` always.
3. Port enrichment (parameterized path, `http.route`, `hashes`) into the TF path.
   PASS = `row_diff.js` reports 0 field mismatches incl. enrichment columns.

## Tooling (this dir)
- `lib.sh` — TS/TF connection helpers.
- `counts_matrix.sh <pid> <mins> <ISO_start>...` — per-window total + distinct-id
  parity matrix (detects loss + duplication).
- `row_diff.js <pid> <start> <end>` — field-by-field parity over the id
  intersection (normalizes jsonb key-order, timestamp spelling, array repr).
