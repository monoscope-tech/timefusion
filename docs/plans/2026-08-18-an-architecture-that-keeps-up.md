
### Part XVI resolved — on one image, over seven hours, growth is BOUNDED

The question left open by the retraction ("do the lumps release or accumulate?")
now has an answer, from the first uninterrupted multi-hour window obtained.
`a1bf953`, hourly `process_rss_mb`:

| uptime | 1h | 2h | 3h | 4h | 5h | 6h | 7h |
| --- | --- | --- | --- | --- | --- | --- | --- |
| MB | 9,979 | 12,661 | 13,435 | 11,356 | 12,419 | **17,020** | 12,796 |

**The lumps release.** Every excursion returns to an 11-13 GB floor, including the
17 GB peak in hour 6 which I flagged at the time as possibly a step change — it
was not. Net across seven hours is **+470 MB/h**, implying ~240 h to the 124.6 GB
wall, against an 8-20 h kill cadence. No kill in **14h45m**, roughly double the
historical minimum gap.

So on this build a kill cannot come from the growth visible here. It requires an
excursion far outside this envelope — an event, not a trend. Which is where the
first (retracted) reading pointed, but that conclusion was drawn from two samples
in one quiet window and did not deserve to be believed at the time; it does now,
on seven samples spanning both quiet and lumpy hours.

**What this does not establish.** It is ONE image. `d5688fd` accumulated
13.8 → 22.2 GB in ~30 minutes with no release, so builds genuinely differ, and the
five kills earlier in the day were real. The claim is bounded to `a1bf953`.

**Methodological point worth keeping.** Four separate times this session a rate
computed from a short window was wrong, twice badly enough to publish and retract.
The only readings that held up came from a series long enough to contain both
regimes. On a lumpy signal, *the minimum useful sample is one that spans a release*
— anything shorter measures a regime and calls it a rate.
