# Query latency and routing matrix harness

Date: 2026-09-17. Plan item: [20](2026-09-16-next-days-work-plan.md#20-refresh-the-latency-and-routing-matrix--p1-tf--monoscope).

## State

The bounded runner is ready. Its production ladder must wait until Monoscope PR #575 and TimeFusion PR #311 are deployed, the replacement has passed recovery and readiness soak, and one immutable TimeFusion boot is old enough for a comparable measurement.

[`bench/query_routing_matrix.py`](../../bench/query_routing_matrix.py) runs reviewed, read-only client query shapes across 1-hour, 1-day, 7-day, and 30-day windows. Each window has a current-time and sealed-time arm, and each arm has a naturally observed first execution followed by one immediate warm execution.

For every execution it records:

- expected immutable image identity, server version, boot identifier, and per-run process age; the boot and version are server-proven while the image value comes from the deployment receipt supplied by the operator;
- client round-trip latency, row count, and a canonical result digest;
- the physical `EXPLAIN` plan;
- a full, hybrid, raw, or unknown `EXPLAIN` source hint, explicitly separated from execution-route evidence;
- ambient miss reasons, an ambient physical-read upper bound, and all numeric `timefusion_stats` deltas; process-wide counters are not attributed to one production query;
- raw-oracle equality when a separate local oracle DSN is supplied.

The runner accepts only one `SELECT` or `WITH` statement and rejects mutating tokens. Reviewed manifests and read-only credentials remain required because this lexical check is defense in depth rather than a SQL parser. It applies a statement deadline and atomically checkpoints the report after every execution.

## Stop conditions

The ladder stops the current shape immediately on:

- a TimeFusion boot change;
- SQLSTATE `57P03` or the canonical deployment-drain response;
- any query error or statement deadline;
- a result mismatch against the local raw oracle;
- more than 1 GiB of ambient physical Parquet reads during one execution;
- any 7-day or 30-day route without a full/hybrid outcome attributable on an explicitly isolated process.

The byte threshold is a conservative post-execution ladder stop, not an active I/O cap. The 10-second statement deadline is the active execution bound. Ambient traffic can cause a safe false-positive stop; it cannot justify a query-specific byte or route claim. Never pass `--isolated` to a shared production process.

Production runs must not disable rollups, purge caches, or manufacture a raw control. Raw-versus-routed equality belongs in an isolated local pair of servers with identical seeded data. Production supplies bounded latency, an `EXPLAIN` hint, and ambient resource context only.

Plans can contain project identifiers or literal filter values. Treat result JSON as a private diagnostic artifact unless its plans and manifest values have been scrubbed.

## Invocation

Copy the example manifest and replace the seeded project identifier with the bounded test project. Add literal shapes from `tests/slt/monoscope_query_shapes.slt` and the current Monoscope query builders without simplifying their SQL.

```console
python3 bench/query_routing_matrix.py matrix.json \
  --dsn "$TIMEFUSION_PG_URL" \
  --image 'ghcr.io/monoscope-tech/timefusion@sha256:…' \
  --output matrix-result.json
```

For the local correctness arm:

```console
python3 bench/query_routing_matrix.py matrix.json \
  --dsn "$ROUTED_TIMEFUSION_PG_URL" \
  --oracle-dsn "$RAW_TIMEFUSION_PG_URL" \
  --image local-candidate \
  --output matrix-local-oracle.json
```

The first production corpus should cover the current top-tenant list, dashboard aggregate, RUM, service, container, issue-chart, and endpoint auto-ack shapes. Keep endpoint auto-ack versioned separately so the post-#575 result cannot be confused with the prior seven-day statement.

## First bounded production result

After Monoscope #575 deployed and the TimeFusion #311 workflow reconciled the already-running image without a restart, one top-tenant throughput shape ran on a stable two-hour-old boot. The query is the literal five-minute `count(*)` shape from the compatibility contract. No raw oracle was used in production.

| Window | Period | First | Warm | `EXPLAIN` source hint |
| --- | --- | ---: | ---: | --- |
| 1 hour | current | 93.5 ms | 87.1 ms | raw |
| 1 hour | sealed | 168.1 ms | 161.8 ms | raw |
| 1 day | current | 374.0 ms | 421.6 ms | raw |
| 1 day | sealed | 195.9 ms | 164.2 ms | raw |
| 7 days | current | 735.2 ms | not run | raw; ladder stopped |

`EXPLAIN` names `otel_logs_and_spans` and no rollup table. Execution windows coincided with full/hybrid counter increments for the sealed and one-day arms, but the process is shared, so those increments cannot be attributed to this query. The 7-day route therefore remained unproven and triggered the long-window stop; the sealed 7-day and all 30-day arms were not executed. Ambient Parquet-read deltas ranged from 2.9 MB to 72.0 MB and are not attributed to this query.

This result establishes bounded latency and an unproven production route; it does not prove a rollup miss. The runner now records separate ambient planner and execution deltas. Route correctness and raw-versus-routed equality must be established on the isolated local arm before proposing a matcher change.

A repeat after the evidence-semantics correction completed the four 1-hour arms, then reached the 10-second statement deadline while planning or starting the current 1-day arm. The runner stopped the shape and did not issue a 7-day query. The sealed 1-hour first/warm pair varied from 2.63 seconds to 680 ms on that repeat, compared with 168/162 ms above. Treat the table as one bounded observation rather than a stable percentile; the matrix needs matched repetitions on an aged boot.
