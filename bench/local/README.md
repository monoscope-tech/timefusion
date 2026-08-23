# Prod query matrix (local, gitignored)

Re-runs the SQL monoscope actually emits (`docs/monoscope-query-shapes.md`)
across 6 projects x 7 windows, so runs are comparable over time.

    grep -m1 '^TIMEFUSION_PG_URL=' ../monoscope/.env | cut -d= -f2- > bench/local/tfurl
    python3 bench/local/query_matrix.py            # appends to matrix3.csv, resumable
    python3 bench/local/render_matrix.py bench/local/matrix3.csv

Method, and why each part matters:

- **Serial.** One wide scan has selected 460 GB; two concurrent is a
  self-inflicted OOM.
- **Two reps, warm (rep2) quoted.** A cell over 15 s skips rep2 rather than
  paying twice — those print with `*` and are COLD.
- **Skip-wider-on-timeout.** Once a shape times out at some window, wider
  windows are skipped: the raw path is monotone in window.
- **Resumable.** Re-running skips cells already in the CSV, so a restart or a
  mid-run deploy costs nothing.

Reading the output:

- `fail` = the 60 s server cap (`DEFAULT_MAX_STATEMENT_SECS`; `min(client,
  server)` makes it un-raisable from the client). `fail'` = implied by the skip.
- `REFUSED` = the per-scan byte ceiling (`TIMEFUSION_WIDE_SCAN_REJECT_MB`).
- `OOM` = a per-query pool ceiling, usually merge-on-read dedup.

Traps that have produced wrong readings here, all of them more than once:

1. **Read `rollup_min_contiguous_days` before quoting any routing number.** It
   resets to 0 on restart and takes ~25 min to rebuild; at 0 the router may not
   attempt at all, so a shape records neither hit nor miss.
2. **Check container uptime.** Every `timefusion_stats` counter is
   process-scoped. Zeros on a 2-minute-old container mean nothing.
3. **A deploy mid-run splits the dataset.** Prod has restarted every 20-40 min
   for a whole session; stamp cells with the image they ran on.
4. **Query latency depends on what MAINTENANCE is doing.** A box where the
   coordinator is idle answers far faster than one where 16 workers are
   compacting. Compare like with like, or the sweep measures the background.
