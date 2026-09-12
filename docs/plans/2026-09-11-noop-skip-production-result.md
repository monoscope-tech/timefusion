# The no-op rollup skip in production — what it actually bought

Deployed `6b533469` at **00:04Z**. Measured 00:28Z-02:29Z by sampling
`timefusion_stats` every 15 minutes. Eight windows, ~2.4 hours.

## Verdict

**It works, it is correct, and it is worth about a third — not the 72.6% the
pre-deploy byte share suggested.** The headline number was a byte share over a
3h window on an 11h process; the thing that matters is the worker-second rate,
and that fell less.

| | s/h |
|---|---:|
| baseline, 11h process (193,539 / 11h) | 17,594 |
| baseline, young process, first hour | 19,585 |
| **after, cumulative since deploy (28,273 / 2.40h)** | **11,780** |
| after, mean of 8 windows | 10,033 |
| after, median of 8 windows | 11,552 |

**≈33% lower against the 11h baseline, ≈40% against the like-for-like young
process.** Both baselines and the measurement cover a young process burning a
restart backlog, so the comparison is fair in that respect; it is *not* fair on
time-of-day, and 2.4 hours in one band is not a day.

## It is doing exactly what it was built to do

Per-window skip share swings from 0% to 84%, and the swing is the point:

```
00:43Z  +skips=  76  +pubs= 428  skip= 15.1%   BaseRollup  11696 s/h   pend=123
00:58Z  +skips=  21  +pubs=   4  skip= 84.0%   BaseRollup   1676 s/h   pend=124
01:13Z  +skips=   4  +pubs= 810  skip=  0.5%   BaseRollup  14196 s/h   pend=222
01:28Z  +skips=   0  +pubs= 120  skip=  0.0%   BaseRollup  11552 s/h   pend=133
01:43Z  +skips=  30  +pubs=  24  skip= 55.6%   BaseRollup   6316 s/h   pend=118
01:58Z  +skips=  29  +pubs=   9  skip= 76.3%   BaseRollup   2312 s/h   pend=112
02:13Z  +skips=   5  +pubs= 809  skip=  0.6%   BaseRollup  10420 s/h   pend=188
02:28Z  +skips=   4  +pubs= 119  skip=  3.3%   BaseRollup  22100 s/h   pend=143
```

The 810-publication waves at **01:13Z and 02:13Z are exactly one hour apart** —
an hourly cron minting genuine work. The skip correctly stays near zero through
those and takes over in between, which is the churn it was built for.

Overall it skipped **169 of 2,492 claimed units (6.8%)** while removing ~a third
of the worker-seconds, because the units it skips are the expensive
50-60-second republications and the ones it lets through are cheaper.

`pending_base_rollup` stayed between 112 and 222 across the whole window — flat,
not growing. The saving is not deferred work.

## Correctness

The traced slice — `dashboard_1m_v3` / 87576849 / 2026-08-17, the one that had
been rebuilding every 32 minutes — was checked against raw after skips had
fired on it:

```
raw     count(*)            over hour 00:00 = 264654
rollup  SUM(request_count)  over hour 00:00 = 264654
```

Exact. Routing health also looks much better (`rollup_miss_stale_coverage` is
13% of misses, against 57% on the pre-deploy process) but **that is not claimed
as an effect of this change**: a young process with freshly built coverage
flatters that metric, and it needs a mature reading.

## The limitation the data exposed, and the next lever

**Coverage recovered from tier tags at boot carries `content_fp: None`, and
`None` declines the skip.** So after every restart, no slice can be skipped
until it has published at least once *in that process* — and prod restarts on
every non-docs push plus healthcheck replacements.

That is most of the gap between the 6.8% skip share measured here and the 2.51x
repeat factor measured on the mature 11h process, which implies ~60% of
publications were repeats.

**This is the highest-value follow-up.** It is strictly additive — an absent tag
still yields `None`, which still declines — but it is not a one-liner, because
the proof has to survive in two places. Sites, all in `database/maintain.rs`
unless noted:

1. `TAG_CONTENT_FINGERPRINT` next to `TAG_SOURCE_FINGERPRINT`
   (`maintenance_coordinator.rs:206`).
2. Write it in the publish tag block (~`3151`), beside the `source_fp` tag.
   `content_fp` is already in scope there.
3. Collect `content_fp_by_identity` in the tag-scanning loop that already builds
   `measures_by_identity` and `paths_by_identity` (~`5009-5040`), keyed by the
   same `(project, slice_start, slice_end, generation, source_fp, source_rows)`
   identity.
4. Populate both fields at the tag-recovery construction (~`5278`):
   `content_fp` from that map, and `output_files` from
   `paths_by_identity`'s `paths.len()` — which is already fetched three lines
   above for the ledger entry.
5. For the skip to survive a restart that reloads from the ledger rather than
   from tags, `crate::storage::CoverageEntry` needs the same field with
   `#[serde(default)]`, written at `record_readable_coverage` and read at the
   ledger construction (~`4806`).

Steps 1-4 alone fix the common case (tags present). Step 5 covers the ledger
path. Do NOT skip the `output_files` half: `output_files: 0` declines the skip
just as `content_fp: None` does, so populating one without the other changes
nothing.

A test exists to extend rather than invent: give
`a_rollup_whose_input_has_not_moved_completes_without_rebuilding` a second
`Database` over the same data dir — the restart shape that
`a_certification_survives_a_restart_and_still_grants_the_skip` already uses —
and assert the skip still fires. It must be run red first: on today's code it
cannot fire after a restart at all, which is the whole point.

## Not this change

- 7d `count(*)` for one project: **23.8 s, 38.6 s, 24.4 s**. 30d: **90 s timeout,
  90 s timeout, 54.9 s**. Measured at 01:30Z on the mature process with headroom
  (35 GB/120 GB, CPU ~1,500-2,500%). The dominant rollup miss reason is
  `not_built` (2,710 of 5,559), not `stale_coverage`. Query latency is the
  biggest user-visible problem and this change does not address it.
- The derived tier is still wedged — `retry.DerivedRollup.base_generation_unverified`
  kept climbing. That is PR #262.
- The journal lock is the next throughput ceiling. That is PR #263 and
  `2026-09-11-the-journal-lock-is-the-next-ceiling.md`.

## Addendum, 2026-09-12

The journal landscape this document ends on has moved. PR #263 merged, and
`journal_stats_publishes_total` reads **2,102 over a 2,640 s process (0.80/s)**
against the ~1/s design target — the throttle works. Group commit shipped
(`1b391890`, `16e925af`), which moved the journal `fsync` out from under the
global mutex.

The no-op skip itself is unchanged and still limited exactly as described above:
`rollup_noop_rebuild_skipped_total` read **38** at 44 minutes of uptime, because
boot-recovered coverage still carries `content_fp: None`. The follow-up in "The
limitation the data exposed" is still the highest-value one for this lane.
