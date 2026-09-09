#!/usr/bin/env bash
set -euo pipefail
: "${PGURL:?TIMEFUSION_PG_URL GitHub Actions secret is required}"
# Best-effort pre-deploy flush. The unflushed tail is WAL-durable and the
# replacement replays it, so this must NOT fail the deploy when prod is
# unreachable — most often it's mid-boot from the *previous* deploy
# (psql: "the database system is starting up"). Failing here deadlocked
# every deploy after a slow boot (2026-07-20). Retry briefly, then proceed.
# timeout: a hung psql (e.g. protocol negotiation with newer psql,
# which needs max_protocol_version=3.0 against TF, or a wedged FLUSH)
# otherwise pins attempt 1 forever and the retry loop never runs
# (2026-07-30: flush step sat 25+ min on a single psql call).
# PGMAXPROTOCOLVERSION: env form (not URI param) — older libpq ignores
# unknown env vars but rejects unknown URI params.
export PGMAXPROTOCOLVERSION=3.0
flush_ok=false
for attempt in $(seq 1 3); do
  if timeout 300 psql "$PGURL" -v ON_ERROR_STOP=1 -c 'FLUSH' 2>&1; then
    echo "online flush ok (attempt $attempt)"
    flush_ok=true
    break
  fi
  echo "flush attempt $attempt failed or timed out; retrying in 15s"
  sleep 15
done
if timeout 300 psql "$PGURL" -v ON_ERROR_STOP=1 -c 'HANDOFF' 2>&1; then
  echo "write-fenced tail drain ready; production remains read-available"
  echo "drained=true" >> "$GITHUB_OUTPUT"
  exit 0
fi
if $flush_ok; then
  echo "HANDOFF unsupported by outgoing binary; using successful FLUSH compatibility path"
  echo "drained=true" >> "$GITHUB_OUTPUT"
  exit 0
fi
echo "drained=false" >> "$GITHUB_OUTPUT"
echo "::warning::neither pre-deploy FLUSH nor HANDOFF succeeded; proceeding (tail is WAL-durable, replay will recover it)"
