#!/usr/bin/env bash
set -euo pipefail
: "${PGURL:?TIMEFUSION_PG_URL GitHub Actions secret is required}"
boot_micros="$(psql "$PGURL" -v ON_ERROR_STOP=1 -Atqc "SELECT value FROM timefusion_stats WHERE component = 'buffered_layer' AND key = 'boot_micros'" || true)"
last_old_response_epoch_ms="$(date +%s%3N)"
echo "boot_micros=${boot_micros}" >> "$GITHUB_OUTPUT"
echo "last_old_response_epoch_ms=${last_old_response_epoch_ms}" >> "$GITHUB_OUTPUT"
# Record total rollout wall time separately from the concurrent SQL
# probe's actual client-visible unready interval.
echo "handoff_started_epoch_ms=$(date +%s%3N)" >> "$GITHUB_OUTPUT"
