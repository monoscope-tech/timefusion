# Dedup certification: coverage, not survival

**Status 2026-08-12.** Successor to
[2026-08-11-certification-survival.md](2026-08-11-certification-survival.md), which is closed.
That plan asked "how do we make certifications survive?"; the answer is that they are not
dying — they are never being issued for the partitions queries actually read.

**Owner:** unassigned. Prerequisite reading: `dedup_window_clean` and `record_certification`
in `src/database.rs`, and `docs/plans/2026-08-09-per-date-dedup.md`.

---

## The finding, in one paragraph

`record_certification` is only ever reached from the dedup sweep, and the sweep's date scope is

```rust
// src/database.rs:9037
let dates: Vec<chrono::NaiveDate> = (0..=lookback).rev().map(|d| today - chrono::Duration::days(d)).collect();
```

with `lookback = timefusion_dedup_lookback_days`, **default 1**. So the only partitions that
can ever hold a certification are **today and yesterday**. A 7-day dashboard spans seven date
partitions, five of which the sweep has never looked at; a 30-day query, twenty-eight. Those
are denied `NeverCertified` no matter how long the process has been up, how warm the cache is,
or whether verdicts persist across restarts.

`certify_partition` already says this out loud, in a comment written for the rollup backfill:

> The sweep never reaches past its lookback, so a sealed day has never been certified and the
> backfill has to do it before it may roll the day up.

The rollup path noticed and worked around it. The read path did not.

## Why the counters looked like a warm-up problem

Three reads (2026-08-11 ×2, 2026-08-12), every one on a young process because TF redeploys
several times a day:

| counter | all three reads |
|---|---|
| `dedup_denied_fp_moved` | **0** |
| `cert_dwell_total` | **0** |
| `dedup_denied_never_certified_pct` | **100.0** |
| `dedup_skipped_pct` | 0.0 – 0.5 |
| `cert_granted_total` | 0 – 4 |

Each read was individually dismissible as "20 minutes old, the confirming pass has not come
round yet". Three of them with an identical shape are not, and the code says why. **Do not
spend another day arranging a 24h window** — it would measure the same structural zero.

## What to do

### 1. Decide what certification is *for* before widening anything

Two coherent designs, and they want different things:

- **Sealed days are certifiable forever.** A partition with no writes since the last sweep is
  clean permanently — that is the whole premise. If so, certification should be issued once
  per sealed day and kept until its fingerprint moves, and the lookback is the wrong control
  entirely.
- **Certification is a hot-window optimisation.** Then 100% `never_certified` on a 7-day query
  is correct behaviour and the read-side skip should stop counting it as a denial, because the
  denominator is meaningless.

The counters cannot tell these apart. Pick one first; the rest of this list assumes the first.

### 2. Do NOT just raise `timefusion_dedup_lookback_days`

It is the obvious move and it is wrong. The lookback controls *the dedup sweep*, not
certification — raising it to 30 makes the sweep re-scan and re-dedup a month of sealed
partitions on every tick to obtain a verdict it could have derived from the fingerprint alone.
That is the OOM shape from 2026-07-04 and 2026-07-21 (see `tf_dedup_sweep_oom_crashloop`),
bought for a read-path optimisation.

### 3. The cheap version: certify sealed partitions without re-deduping them

A sealed partition's fingerprint is already computed (`partition_file_fp`), and
`certify_partition` already short-circuits when the fingerprint matches a live certification.
What is missing is the *first* grant for a day the sweep never visits. A separate low-rate pass
that walks sealed days, computes the fingerprint, and certifies only partitions it can prove
clean — never rewriting — costs a file listing per partition and no dedup work at all.

Bound it by the same budget machinery the sweep uses, and let it walk newest-first so the days
dashboards actually hit get covered first.

### 4. Measure coverage directly, not as a denial ratio

`never_certified_pct` conflates "the sweep failed to certify this" with "the sweep was never
allowed near it". Split it: a partition inside the sweep's lookback that is still uncertified
is a real failure worth alerting on; one outside it is just out of scope. Until that split
exists, the headline number cannot move for a good reason or a bad one distinguishably.

## Exit criteria

- `dedup_skipped_pct` materially above the current 0–0.5% on a 7-day dashboard query, **and**
- `cert_dwell_*` still near zero (confirming certifications are not being invalidated, i.e.
  the sealed-day premise holds), **and**
- no regression in the dedup sweep's tick cost or peak RSS — the failure mode this trades
  against is the sweep OOM, not slow queries.

Kill switches are unchanged and still in place: `timefusion_dedup_certification_persist=false`,
then `timefusion_read_dedup_skip_swept=false`. Doubt a `count(*)` and reach for the second.
