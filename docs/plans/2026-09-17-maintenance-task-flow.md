# Maintenance task-flow accounting

Date: 2026-09-17. Plan item: [19](2026-09-16-next-days-work-plan.md#19-explain-queue-estimate-oscillation-and-repeated-task-creation--p1-tf).

## Result

The large maintenance gauge moves because the planner creates and reclassifies estimated work while workers commit physical output. It is not a physical byte counter.

A paired 82-second production sample reconciled the live gauge exactly:

| Component | Start | End | Change |
| --- | ---: | ---: | ---: |
| Live estimated backlog | 1,360,809,010,853 B | 1,445,949,060,204 B | +85,140,049,351 B |
| Durable task-journal subset | 67,863,503,627 B | 189,571,176,504 B | +121,707,672,877 B |
| Work outside the durable journal | 1,292,945,507,226 B | 1,256,377,883,700 B | -36,567,623,526 B |

The durable subset added 521 active tasks carrying 123,058,426,529 estimated bytes, retired four tasks carrying 1,350,272,860 bytes, and repriced one task by -480,792 bytes. The journal arithmetic residual was zero. The durable change plus the outside-journal change equals the live gauge change with a zero residual.

Most durable growth came from `BaseRollup`. The largest groups included the self-monitoring `metrics_1m_v2` tier (+13.37 GB), one dashboard tier (+12.89 GB), and one sessions tier (+12.35 GB). Project identifiers are omitted here because the ranking, rather than customer identity, is the useful result.

The state comparison included 213 `superseded -> pending` and nine `complete -> pending` transitions. These transitions are supported lifecycle events:

- A superseded parent becomes fresh debt only after it has no live descendant. `enqueue_inner` then resets its attempts and makes it pending.
- A completed slice becomes pending when a later source mutation invalidates it.

The sample therefore shows task manufacture and reopened work. It does not by itself show duplicate or incorrect work. A policy change needs a longer trace that joins each reactivation to the invalidation or planning event that caused it.

## Physical progress during the trace

A separate five-minute event sample contained 200 task starts and 196 finishes. The live backlog increased by 66,220,397,947 estimated bytes. Identity-bearing task events alone accounted for a 97,012,705,497-byte decrease, leaving a 163,233,103,444-byte residual. Aggregate planner events reported 340 compaction-debt tasks and five coarsened slices without task identity, which explains why event logs alone cannot close the ledger.

The matching Delta-log ledger recorded 12 physical commits:

| Lane | Commits | Rows written | Parquet bytes written |
| --- | ---: | ---: | ---: |
| Flush | 5 | 97,024 | 14,236,053 B |
| Wave commit | 7 | 2,396,715 | 133,113,961 B |

Physical output continued while the estimated queue grew. Queue-byte movement must therefore stay separate from Delta rows and bytes when deciding whether a lane is progressing.

## Reproduction tool

[`bench/maintenance_task_flow.py`](../../bench/maintenance_task_flow.py) reads a bounded JSON or JSONL structured-event export. It can also compare two maintenance journal snapshots, replay their paired WAL tails, and add a Delta physical-write ledger for the same half-open interval.

```console
bench/maintenance_task_flow.py events.jsonl \
  --ledger delta-ledger.json \
  --journal-before before.json --journal-before-wal before.wal \
  --journal-after after.json --journal-after-wal after.wal
```

The report keeps three domains separate:

1. Observable task transitions and their decoded-byte estimates.
2. The durable journal subset. Derived hygiene operations are intentionally absent from this journal.
3. Physical Delta commits, rows, Parquet bytes, and deletion-vector bytes.

Missing identity, missing estimates, and aggregate-only planner events remain explicit coverage gaps. The tool does not infer physical work from queue estimates.

## Next measurement

Run the same paired capture over a healthy four-to-six-hour window. Join every `complete -> pending` and `superseded -> pending` transition to its source invalidation or planner pass. Rank creation and retirement rates by operation, tier, tenant, partition age, and slice width. Use that trace to decide whether wave-planner packing remains material after eager input retraction; do not tune planner policy from this short sample.
