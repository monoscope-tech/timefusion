#!/usr/bin/env bash
set -euo pipefail
: "${PGURL:?TIMEFUSION_PG_URL GitHub Actions secret is required}"
# HANDOFF fences admission before it drains the finite write tail. Do not run an
# unfenced FLUSH first: three five-minute attempts on 2026-09-17 let writes keep
# extending the tail, then a failed HANDOFF was followed by a 234-second outage
# while the replacement replayed 57,455 WAL entries. A failed HANDOFF reopens
# admission, so leave the serving process in place and fail the deployment.
#
# Keep the client timeout above HANDOFF's four-minute internal drain limit. The
# extra minute lets the server return its explicit failure and reopen admission.
# PGMAXPROTOCOLVERSION: env form (not URI param) — older libpq ignores
# unknown env vars but rejects unknown URI params.
export PGMAXPROTOCOLVERSION=3.0
if timeout 300 psql "$PGURL" -v ON_ERROR_STOP=1 -c 'HANDOFF' 2>&1; then
  echo "write-fenced tail drain ready; production remains read-available"
  echo "drained=true" >> "$GITHUB_OUTPUT"
  exit 0
fi
echo "::error::HANDOFF did not prove a drained write fence; refusing to replace the serving process"
exit 1
