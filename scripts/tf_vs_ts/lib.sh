#!/usr/bin/env bash
# Shared config for TimescaleDB (source-of-truth) vs TimeFusion parity checks.
# monoscope dual-writes to both, so a *settled* window MUST match exactly.
export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-20}"

MONOSCOPE_ENV="${MONOSCOPE_ENV:-/Users/tonyalaribe/Projects/apitoolkit/monoscope/.env}"
TIMEFUSION_ENV="${TIMEFUSION_ENV:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/.env}"
# Reads use the least-privilege read-only Timescale role (timefusion/.env TS_RO_URL), never monoscope's
# superuser. TF endpoint from monoscope/.env. All overridable via env.
TS_URL="${TS_URL:-$(grep -m1 '^TS_RO_URL=' "$TIMEFUSION_ENV" | cut -d= -f2-)}"
TF_URL="${TF_URL:-$(grep -m1 '^TIMEFUSION_PG_URL=' "$MONOSCOPE_ENV" | cut -d= -f2-)}"
export TS_URL TF_URL

# psql -> clean TSV/value output, no headers, no notices.
ts() { psql "$TS_URL" -At -F'|' -c "$1" 2>/dev/null; }
tf() { psql "$TF_URL" -At -F'|' -c "$1" 2>/dev/null; }
