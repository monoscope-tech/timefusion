#!/usr/bin/env bash
# Read-only production benchmark. Requires ../monoscope/.env with TIMEFUSION_PG_URL.
set -euo pipefail

root=$(git rev-parse --show-toplevel)
url=$(grep -m1 '^TIMEFUSION_PG_URL=' "$root/../monoscope/.env" | cut -d= -f2-)
tenants=(
  98fdd4f3-3544-4087-ad91-1e7ca95aba29
  28f62f01-46a1-400e-8195-da7bc3505b5b
)
ranges=('1 hour' '3 hours' '24 hours')
[[ ${1:-} == --include-3d ]] && ranges+=('3 days')
statement_timeout=${TIMEFUSION_BENCH_STATEMENT_TIMEOUT:-120s}

psql "$url" -X -v ON_ERROR_STOP=1 -P pager=off <<SQL
\\timing on
SELECT component, key, value
FROM timefusion_stats
WHERE component IN ('foyer', 'foyer_metadata', 'parquet', 'scan')
ORDER BY component, key;
SQL

for tenant in "${tenants[@]}"; do
  for range in "${ranges[@]}"; do
    printf '\n-- tenant=%s range=%s\n' "$tenant" "$range"
    psql "$url" -X -v ON_ERROR_STOP=1 -P pager=off \
      -c "SET statement_timeout = '$statement_timeout';" \
      -c "\\timing on" \
      -c "SELECT count(*) FROM otel_logs_and_spans WHERE project_id = '$tenant' AND timestamp >= now() - interval '$range';"
  done
done

psql "$url" -X -v ON_ERROR_STOP=1 -P pager=off <<SQL
SET statement_timeout = '$statement_timeout';
\\timing on
EXPLAIN (ANALYZE, VERBOSE) SELECT count(*)
FROM otel_logs_and_spans
WHERE project_id = '${tenants[0]}'
  AND timestamp >= now() - interval '1 hour';
SELECT component, key, value
FROM timefusion_stats
WHERE component IN ('foyer', 'foyer_metadata', 'parquet', 'scan')
ORDER BY component, key;
SQL
