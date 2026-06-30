#!/usr/bin/env bash
# Count-parity matrix: for each (project, window) print total + distinct-id on
# both sides and the delta. A settled window with TS>TF means TF lost writes.
# Usage: counts_matrix.sh <project_id> <window_minutes> <ISO_start>...
source "$(dirname "$0")/lib.sh"
PID="$1"; MINS="$2"; shift 2
printf "%-20s | %12s | %12s | %12s | %10s | %s\n" "window_start(UTC)" "TS_total" "TF_total" "TS_distinct" "TF_distinct" "verdict"
for T0 in "$@"; do
  T1="$(date -u -j -v+${MINS}M -f '%Y-%m-%d %H:%M:%S' "$T0" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || \
        date -u -d "$T0 + $MINS minutes" '+%Y-%m-%d %H:%M:%S')"
  W="project_id='$PID' and timestamp >= '$T0+00' and timestamp < '$T1+00'"
  read TST TSD < <(ts "select count(*),count(distinct id) from otel_logs_and_spans where $W" | tr '|' ' ')
  read TFT TFD < <(tf "select count(*),count(distinct id) from otel_logs_and_spans where $W" | tr '|' ' ')
  TST=${TST:-ERR}; TFT=${TFT:-ERR}; TSD=${TSD:-ERR}; TFD=${TFD:-ERR}
  V=""
  if [ "$TFT" != ERR ] && [ "$TFD" != ERR ] && [ "$TFT" -gt "$TFD" ] 2>/dev/null; then V="TF_DUP(+$((TFT-TFD)))"; fi  # TF rows beyond distinct ids
  if [ "$TST" = "$TFT" ] && [ "$TSD" = "$TFD" ]; then V="${V:-MATCH}"
  elif [ "$TSD" != ERR ] && [ "$TFD" != ERR ] && [ "$TSD" -gt "$TFD" ] 2>/dev/null; then V="TF_MISSING(-$((TSD-TFD)))${V:+ $V}"
  else V="${V:-DIFF}"; fi
  printf "%-20s | %12s | %12s | %12s | %10s | %s\n" "$T0" "$TST" "$TFT" "$TSD" "$TFD" "$V"
done
