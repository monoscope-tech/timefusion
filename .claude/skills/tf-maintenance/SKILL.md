---
name: tf-maintenance
description: Refresh and republish the intra-day maintenance dashboard (ingest/flush, hot-tail compaction, dedup queue, crons, read path, OOM banner) at its permanent artifact URL. Use for "what's the server doing today", light-compaction/dedup status, or "update the maintenance page".
---

Refresh `docs/dashboards/hot-maintenance.html` with live data and republish to
the permanent artifact URL below (pass it as `url` to the Artifact tool from
any session — never mint a new URL).

**Artifact URL (stable):** https://claude.ai/code/artifact/b3b569fc-56e8-4527-8600-b3e5e80774ce · favicon `⚙️`

## 1. Data sources

**Counters** (reset at each boot — always pair numbers with boot time):
```bash
PGURL=$(grep -m1 '^TIMEFUSION_PG_URL=' ../monoscope/.env | cut -d= -f2-)
psql "$PGURL" -t -A -F'|' -c "SELECT component,key,value FROM timefusion_stats"
```
Key rows: buffered_layer (rows_ingested_total, flush_completed_total,
pressure_pct, process_rss_mb), maintenance (light_optimize_* waves/bins/ticks,
dirty_bin_queue_depth, dedup_*, checkpoints_created), wal (quarantine_files/mb,
disk_mb), pgwire/scan/plan_cache/foyer for the read path.

**Restart/OOM state** (read-only ssh):
```bash
ssh ubuntu@captain.s.past3.tech 'docker service ps srv-captain--timefusion --format "{{.Name}} {{.CurrentState}} {{.Error}}" | head -3'
# OOM confirmation: docker inspect <exited container> --format "OOM:{{.State.OOMKilled}} at {{.State.FinishedAt}}"
```
`pgwire.queries_total` doubling back to ~0 also betrays a restart.

## 2. Update the template

Edit `docs/dashboards/hot-maintenance.html`: the red `.banner` (OOM/restart
story — keep the history, e.g. "kills at 18:47 and 20:32"), every `.tile`
number, the `.sub` boot-time line, and the closing `.note`. Health pills:
`ok`/`warn`/`crit`.

Interpretation guide:
- MemBuffer pressure low + RSS tens of GB = heap churn / reservation-leak
  pattern (see memory `tf_compaction_binfanin_leak_2026-07-30`, OOM #3).
- `dirty_bin_queue_depth` in the thousands with `dedup_bins_deferred_cold`
  close behind is by-design cold backlog; read-side DedupExec keeps queries
  correct regardless.
- WAL `quarantine_files > 0` = acked data not in the store → investigate
  (`quarantine/dml/*` = parked enrichment merges; `timefusion redrive-dml`
  recovers them — see memory `tf_dml_quarantine_redrive_2026-07-30` if present,
  else the 800373f commit message).

## 3. Republish

Artifact tool with `file_path: docs/dashboards/hot-maintenance.html`,
`url: <the stable URL above>`, favicon `⚙️`.
