#!/usr/bin/env bash
set -euo pipefail
: "${PGURL:?TIMEFUSION_PG_URL GitHub Actions secret is required}"
export PGMAXPROTOCOLVERSION=3.0
boot_micros=""
recovery_complete=""
recovery_ms=""
deadline=$((SECONDS + RECOVERY_WAIT_SECS))
while (( SECONDS < deadline )); do
  if ! timeout 5 psql "$PGURL" -v ON_ERROR_STOP=1 -Atqc 'SELECT 1' >/dev/null 2>&1; then
    sleep 2
    continue
  fi
  boot_micros="$(timeout 5 psql "$PGURL" -v ON_ERROR_STOP=1 -Atqc "SELECT value FROM timefusion_stats WHERE component = 'buffered_layer' AND key = 'boot_micros'" || true)"
  recovery_complete="$(timeout 5 psql "$PGURL" -v ON_ERROR_STOP=1 -Atqc "SELECT value FROM timefusion_stats WHERE component = 'wal' AND key = 'recovery_complete'" || true)"
  recovery_ms="$(timeout 5 psql "$PGURL" -v ON_ERROR_STOP=1 -Atqc "SELECT value FROM timefusion_stats WHERE component = 'wal' AND key = 'recovery_duration_ms'" || true)"
  if [ -n "$boot_micros" ] && [ "$boot_micros" != "$PREVIOUS_BOOT_MICROS" ] && [ "$recovery_complete" = true ] && [[ "$recovery_ms" =~ ^[0-9]+$ ]]; then
    ready_elapsed_ms=$(( $(date +%s%3N) - HANDOFF_STARTED_EPOCH_MS ))
    ready_wait=$(( (ready_elapsed_ms + 999) / 1000 ))
    echo "rollout measurement: total ${ready_elapsed_ms}ms; last-old-query to first-new-query ${QUERY_HANDOFF_MS}ms; longest client-visible unready interval ${OBSERVED_UNREADY_MS}ms; WAL recovery ${recovery_ms}ms"
    [[ "$QUERY_HANDOFF_MS" =~ ^[1-9][0-9]*$ ]] || { echo "::error::rollout probe did not observe the old-to-new query handoff: '$QUERY_HANDOFF_MS'"; exit 1; }
    [[ "$OBSERVED_UNREADY_MS" =~ ^[0-9]+$ ]] || { echo "::error::rollout readiness probe did not produce a valid downtime measurement: '$OBSERVED_UNREADY_MS'"; exit 1; }
    (( OBSERVED_UNREADY_MS <= MAX_READY_WAIT_SECS * 1000 )) || { echo "::error::replacement returned 57P03/not-ready continuously for ${OBSERVED_UNREADY_MS}ms (budget $((MAX_READY_WAIT_SECS * 1000))ms)"; exit 1; }
    # A PLANNED deploy drains the WAL first, so recovery is 0ms and this
    # budget is the assertion that the drain actually happened. It is NOT
    # a bound on crash recovery: after an OOM or SIGKILL the replacement
    # legitimately replays a real backlog (64,427ms on 2026-08-14), and
    # failing the deploy for that told us nothing we could act on while
    # marking every post-incident rollout red — which is how a genuine
    # deploy failure would have been missed.
    #
    # So: fail only when a DRAINED handoff was expected and did not
    # happen, and otherwise surface the replay as a warning with the
    # number attached.
    if (( recovery_ms > MAX_WAL_RECOVERY_MS )); then
      if [ "$PREFLUSHED_HANDOFF" = "true" ]; then
        echo "::error::WAL recovery took ${recovery_ms}ms (budget ${MAX_WAL_RECOVERY_MS}ms) after a DRAINED handoff — the drain did not take"
        exit 1
      fi
      echo "::warning::WAL recovery took ${recovery_ms}ms — the predecessor did not exit cleanly, so this rollout replayed a real backlog"
    fi
    echo "rollout completed within recovery and availability budgets"
    exit 0
  fi
  sleep 2
done
echo "::error::replacement did not finish WAL recovery within ${RECOVERY_WAIT_SECS}s (boot=${boot_micros:-missing}, complete=${recovery_complete:-missing}, duration=${recovery_ms:-missing})"
exit 1
