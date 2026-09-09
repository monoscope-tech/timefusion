#!/usr/bin/env bash
set -euo pipefail
: "${CAPROVER_SERVER:?CAPTAINROVER_SERVER is required}"
: "${CAPROVER_APP:?CAPTAINROVER_APP_NAME is required}"
: "${CAPROVER_TOKEN:?CAPTAINROVER_APP_TOKEN is required}"
: "${PGURL:?TIMEFUSION_PG_URL is required}"
export PGMAXPROTOCOLVERSION=3.0

# Measure client-visible unavailability concurrently with the opaque
# CapRover rollout. Total deploy wall time includes image pull while
# the old task is healthy; it is not database downtime. Persist the
# longest continuous SELECT failure interval for the verification
# step instead.
probe_dir="$(mktemp -d)"
probe_stop="$probe_dir/stop"
probe_result="$probe_dir/max_unready_ms"
handoff_result="$probe_dir/query_handoff_ms"
(
  unready_started_ms=0
  max_unready_ms=0
  # The preceding step already proved this exact old boot answered a
  # query. Seed from that timestamp so a very short handoff remains
  # measurable even when the first concurrent sample hits 57P03.
  last_old_response_ms="$LAST_OLD_RESPONSE_EPOCH_MS"
  query_handoff_ms=0
  while [ ! -e "$probe_stop" ]; do
    attempt_started_ms="$(date +%s%3N)"
    responding_boot="$(timeout 1 psql "$PGURL" -v ON_ERROR_STOP=1 -Atqc "SELECT value FROM timefusion_stats WHERE component = 'buffered_layer' AND key = 'boot_micros'" 2>/dev/null || true)"
    if [ -n "$responding_boot" ]; then
      if (( unready_started_ms > 0 )); then
        interval_ms=$(( $(date +%s%3N) - unready_started_ms ))
        (( interval_ms > max_unready_ms )) && max_unready_ms=$interval_ms
        unready_started_ms=0
      fi
      if [ "$responding_boot" = "$PREVIOUS_BOOT_MICROS" ]; then
        last_old_response_ms="$(date +%s%3N)"
      elif (( query_handoff_ms == 0 )); then
        query_handoff_ms=$(( $(date +%s%3N) - last_old_response_ms ))
      fi
    elif (( unready_started_ms == 0 )); then
      unready_started_ms="$attempt_started_ms"
    fi
    sleep 0.1
  done
  if (( unready_started_ms > 0 )); then
    interval_ms=$(( $(date +%s%3N) - unready_started_ms ))
    (( interval_ms > max_unready_ms )) && max_unready_ms=$interval_ms
  fi
  echo "$max_unready_ms" > "$probe_result"
  echo "$query_handoff_ms" > "$handoff_result"
) &
probe_pid=$!
stop_probe() {
  touch "$probe_stop"
  wait "$probe_pid"
}
trap stop_probe EXIT

# The image is already built, pushed, pulled by the smoke test, and
# the outgoing process is HANDOFF-drained when supported. Submit the
# Swarm update immediately so the leased fence cannot expire. The
# first compatibility rollout may use FLUSH; its shutdown path still
# fences and drains any tail with the full correctness budget.
caprover deploy \
  --caproverUrl "$CAPROVER_SERVER" \
  --appToken "$CAPROVER_TOKEN" \
  --appName "$CAPROVER_APP" \
  -i "$IMAGE_URL" >caprover-deploy.log 2>&1 &
deploy_pid=$!

deploy_status=0
wait "$deploy_pid" || deploy_status=$?

# CapRover can return after it has submitted the Swarm update but
# before the replacement owns PGWire. Keep the concurrent probe alive
# until a query proves that a DIFFERENT boot completed recovery;
# otherwise stopping it here reports a false 0ms outage.
replacement_deadline=$((SECONDS + 720))
while (( deploy_status == 0 && SECONDS < replacement_deadline )); do
  boot_micros="$(timeout 3 psql "$PGURL" -v ON_ERROR_STOP=1 -Atqc "SELECT value FROM timefusion_stats WHERE component = 'buffered_layer' AND key = 'boot_micros'" 2>/dev/null || true)"
  recovery_complete="$(timeout 3 psql "$PGURL" -v ON_ERROR_STOP=1 -Atqc "SELECT value FROM timefusion_stats WHERE component = 'wal' AND key = 'recovery_complete'" 2>/dev/null || true)"
  if [ -n "$boot_micros" ] && [ "$boot_micros" != "$PREVIOUS_BOOT_MICROS" ] && [ "$recovery_complete" = true ]; then
    break
  fi
  sleep 0.5
done
stop_probe
trap - EXIT
observed_unready_ms="$(cat "$probe_result")"
query_handoff_ms="$(cat "$handoff_result")"
echo "observed_unready_ms=$observed_unready_ms" >> "$GITHUB_OUTPUT"
echo "query_handoff_ms=$query_handoff_ms" >> "$GITHUB_OUTPUT"
cat caprover-deploy.log
(( deploy_status == 0 )) || exit "$deploy_status"
