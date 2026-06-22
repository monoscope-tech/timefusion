#!/usr/bin/env bash
# Monitor TimeFusion accept-rate health while the DLQ replay drains.
# Reads only the in-memory `timefusion_stats` table (cheap — no otel scans).
#
# Usage:
#   TF_URL='postgresql://postgres:...@timefusion.s.past3.tech:5432/postgres' \
#     ./bench/dlq_drain_monitor.sh [interval_secs] [max_iters]
# Falls back to monoscope/.env TIMEFUSION_PG_URL if TF_URL is unset.
set -uo pipefail

TF="${TF_URL:-$(grep -m1 '^TIMEFUSION_PG_URL=' ../monoscope/.env 2>/dev/null | cut -d= -f2-)}"
INT="${1:-15}"
MAX="${2:-240}"   # default ~1h at 15s
[ -z "$TF" ] && { echo "set TF_URL or provide ../monoscope/.env"; exit 1; }

q() {
  psql "$TF" -t -A -F'|' -c "SELECT
    (SELECT value FROM timefusion_stats WHERE component='buffered_layer' AND key='pressure_pct'),
    (SELECT value FROM timefusion_stats WHERE component='mem_buffer'     AND key='total_rows'),
    (SELECT value FROM timefusion_stats WHERE component='buffered_layer' AND key='backpressure_rejected_total'),
    (SELECT value FROM timefusion_stats WHERE component='buffered_layer' AND key='backpressure_engaged_total'),
    (SELECT value FROM timefusion_stats WHERE component='wal'            AND key='files'),
    (SELECT value FROM timefusion_stats WHERE component='wal'            AND key='disk_mb'),
    (SELECT value FROM timefusion_stats WHERE component='mem_buffer'     AND key='oldest_bucket_age_secs'),
    (SELECT value FROM timefusion_stats WHERE component='pgwire'         AND key='queries_total')" 2>/dev/null
}

printf '%-8s %6s %7s %8s %8s %7s %6s %7s %7s\n' TIME PRESS ROWS dROWS/s REJECTED dREJ WALf WAL_MB AGE_h
prev_rows=""; prev_rej=""; prev_q=""
for ((i=0; i<MAX; i++)); do
  IFS='|' read -r press rows rej eng files walmb age qtot <<<"$(q)"
  ts=$(date +%H:%M:%S)
  if [ -z "${press:-}" ]; then
    echo "$ts  TF UNREACHABLE (restart / boot warmup?)"
    prev_rows=""; prev_rej=""; prev_q=""   # reset deltas across a restart
    sleep "$INT"; continue
  fi
  if [ -n "$prev_rows" ]; then
    drows=$(( (rows - prev_rows) / INT ))
    drej=$(( rej - prev_rej ))
    dq=$(( qtot - prev_q ))
    [ "$qtot" -lt "$prev_q" ] && { echo "$ts  ⚠ RESTART detected (queries_total reset $prev_q→$qtot)"; }
  else
    drows=0; drej=0; dq=0
  fi
  ageh=$(awk "BEGIN{printf \"%.1f\", ${age:-0}/3600}")
  warn=""
  [ "${press:-0}" -ge 75 ] && warn="$warn  ⚠PRESSURE"
  [ "${drej:-0}" -gt 0 ]   && warn="$warn  ⚠REJECTS+$drej"
  [ "${files:-0}" -ge 45 ] && warn="$warn  ⚠WAL_NEAR_CAP"
  printf '%-8s %5s%% %6sk %7s %8s %5s %6s %7s %7s%s\n' \
    "$ts" "$press" "$((rows/1000))" "$drows" "$rej" "$drej" "$files" "$walmb" "$ageh" "$warn"
  prev_rows=$rows; prev_rej=$rej; prev_q=$qtot
  sleep "$INT"
done
