#!/usr/bin/env bash
set -euo pipefail
: "${PGURL:?TIMEFUSION_PG_URL GitHub Actions secret is required}"
export PGMAXPROTOCOLVERSION=3.0

deadline=$((SECONDS + SOAK_SECS))
probes=0
failures=0
consecutive_failures=0
max_consecutive_failures=0
while (( SECONDS < deadline )); do
  probes=$((probes + 1))
  if timeout 3 psql "$PGURL" -v ON_ERROR_STOP=1 -Atqc \
    "SELECT CASE WHEN EXISTS (SELECT 1 FROM timefusion_stats WHERE component = 'wal' AND key = 'recovery_complete' AND value = 'true') THEN 1 ELSE 0 END" \
    | grep -qx 1; then
    consecutive_failures=0
  else
    failures=$((failures + 1))
    consecutive_failures=$((consecutive_failures + 1))
    if (( consecutive_failures > max_consecutive_failures )); then
      max_consecutive_failures=$consecutive_failures
    fi
    if (( consecutive_failures > MAX_CONSECUTIVE_FAILURES )); then
      echo "::error::production failed ${consecutive_failures} consecutive readiness probes during the post-deploy soak"
      exit 1
    fi
  fi
  sleep "$PROBE_INTERVAL_SECS"
done

echo "post-deploy soak passed: ${probes} probes, ${failures} transient failures, max ${max_consecutive_failures} consecutive"
