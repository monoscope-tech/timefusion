---
name: compaction-chart
description: Prod status in one command (query latency, maintenance backlog, rollups, dedup/DVs, certification, light/major compaction, vacuum, memory, per-day Delta files) and refresh the compaction dashboard at its permanent artifact URL. Use for "how is prod", "look at prod and share the numbers", compaction status, file-count charts, or "update the compaction chart".
---

**Artifact URL (stable):** https://claude.ai/code/artifact/896a7eb9-2c29-4ca0-98ab-4ca02fb8d671 · favicon `🗜️`
(pass it as `url` to the Artifact tool from any session — never mint a new URL).

## 1. One command

```bash
bench/prod_report.py                     # ~10 s: text report, every section
bench/prod_report.py --window 120        # + rates over a 2-minute window (use on a young process)
bench/prod_report.py --json > snap.json  # everything, machine-readable (chart input)
bench/prod_report.py --no-delta --no-ssh # stats only, ~2 s
```

It is read-only and needs no setup beyond what is already on this laptop:
`psql` + `TIMEFUSION_PG_URL` (read from `../monoscope/.env`), key-based ssh to the
CapRover host, `gh` (names the deployed commit — the image carries only a
digest), and `.env.prod` (falls back to the main checkout's copy from a
worktree). Each source fails independently: a dead ssh or S3 prints
`host: unavailable (...)` and the rest of the report still renders.

What it reads:
- `timefusion_stats` → latency, ingest/flush, maintenance backlog, rollups,
  dedup/DV strip, certification, light/major compaction, checkpoints, memory.
- ssh → deployed image digest + state, container mem/CPU, recent task failures.
- Delta logs (checkpoint parquet + JSON replay, not `deltalake`, because the
  python API does not expose deletion vectors) → per table: version, files,
  bytes, DV files, masked rows, last-commit age; for `otel_logs_and_spans`: the
  commit lane mix over the retained log, retained tombstones (vacuum health),
  a per-date table, and `per_project_date` in the JSON.

Reading it — traps the numbers carry:
- **Counters reset at boot.** The header prints uptime; `/h since boot` on a
  <1h process is noise (the report flags it). Use `--window`.
  `tasks_complete` is journal-lifetime, not per boot.
- **DV strips land in bursts** when the fleet budget's 10-minute window reopens,
  so a `--window` under 10 min swings from 0 to ~400/h. To check the cap, count
  DV-retiring OPTIMIZE commits per 10 min in the Delta log (should be ≈12–14),
  and remember `dv_rewrites_landed_total` also counts ordinary packing bins that
  merely had a DV input.
- **Latency percentiles are since boot too** — a fresh deploy's p99 includes
  cold caches. Wait ≥10 min after a deploy before quoting them.
- **Rollup hit rate** is per query; don't compare it with stale-coverage or
  witness percentages (different denominators).
- **Today's partition** always shows hundreds of files and most DV files; judge
  compaction on sealed days (files ≈ `ideal` = ceil(GB) at the 1 GB target).
- **Vacuum** has no counter: the report shows its schedule/retention (config
  defaults, prod does not override) and retained tombstones; `past retention > 0`
  means vacuum is lagging.
- Dormant tiers (no commit in 2 days) are listed on one line — old spec
  versions, not a stuck builder, unless a current tier appears there.

The OVH quirks from the old snippet live in the script (`region=de`, checksum
env set to `when_required`); keep them if you touch it.

## 2. Refresh the dashboard — one command, then publish

```bash
bench/prod_report.py --html docs/dashboards/compaction-chart.html   # ~15 s warm, ~1 min cold
```

`--html` implies `--history 8` and regenerates **every data block** of the
page in place, between `<!--gen:NAME-->` / `/*gen:data*/` markers:

| block | source |
|---|---|
| `sub` (snapshot line), `tiles` (compaction) | Delta replay + deployed commit |
| project × day grid, then/now row (`gen:data`) | Delta replay (`per_project_date`, `per_date`) |
| `live` (maintenance now) | `timefusion_stats` |
| `hist` + hourly charts + daily table | monoscope OTel metrics, 8 days hourly |

Monoscope history is fetched **one UTC day per query** (a 7-day query times out
server-side after 3 min; a day returns in under a second) and closed days are
cached in `~/.cache/tf_prod_report/`. Counters are differenced per series with
restarts handled; latency is the worst one-minute p99 window in each hour.

Nothing outside the markers is generated: the two `.note` paragraphs are
number-free reading guides on purpose, so they never go stale. Put new numbers
in a generated tile (edit `html_compaction` / `html_live` / `html_history` in
the script), not in prose. If a source fails, its block keeps the previous
values and the text report names the failed source — say so when sharing.

Then republish: Artifact tool with `file_path: docs/dashboards/compaction-chart.html`,
`url: <the stable URL above>`. If the publish is refused because another
session published, read the artifact, confirm your file contains its content,
and publish again.

Rollup tables live at `timefusion/<table>`, **not** `timefusion/default/<table>`.

## Context that stays true

- Project IDs: 87576849… is the whale tenant (10-100× everyone else). Full IDs
  via `aws s3api list-objects-v2 --prefix "timefusion/otel_logs_and_spans/project_id=" --delimiter "/"`.
- Off-box compaction recipes and the 2026-07-30 backlog story: see
  memory `tf_cli_offbox_2026-07-30` and `tf_compaction_binfanin_leak_2026-07-30`.
