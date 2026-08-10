#!/usr/bin/env bash
# Footer-repair progress probe.
#
# Reports, per project, the OLDEST absolute timestamp whose window still plans
# as `mode=bounded`. Everything older is footer-poisoned: one file without an
# honest `sorting_columns` voids the scan's ordering for the whole window
# (`derive_common_ordering` is all-or-nothing), which forces DedupExec into its
# unbounded full-set seen-set.
#
# EXPLAIN only — never executes the query. Running the wide query to find the
# boundary is what times out and risks OOM-ing the memory-tight prod box
# (measured 2026-08-08: 147s, 200s, then a timeout, for information the plan
# gives instantly).
#
# ABSOLUTE timestamps, never `now() - interval 'N days'`. A relative offset
# slides with the clock, so the same N covers a DIFFERENT range at a different
# time of day — which is how a stationary boundary was misread as a
# "re-poisoning" on 2026-08-09/10.
set -uo pipefail

PGURL="${TIMEFUSION_PG_URL:-$(grep -m1 '^TIMEFUSION_PG_URL=' "${MONOSCOPE_ENV:-../monoscope/.env}" | cut -d= -f2-)}"
[ -n "$PGURL" ] || { echo "no TIMEFUSION_PG_URL (set it, or MONOSCOPE_ENV to monoscope's .env)" >&2; exit 1; }
export PGMAXPROTOCOLVERSION=3.0
TABLE="${TABLE:-otel_logs_and_spans}"
PROJECTS="${PROJECTS:-87576849-4941-49d3-a15d-680fef88a1a8 6297304f-89c0-48a9-9b5c-20bcac61f54e 28f62f01-46a1-400e-8195-da7bc3505b5b}"

# Absolute probe points, oldest first. Extend as the boundary moves back.
BOUNDS="${BOUNDS:-2026-07-01T00:00:00Z 2026-07-15T00:00:00Z 2026-07-25T00:00:00Z 2026-07-30T00:00:00Z 2026-07-31T00:00:00Z 2026-08-01T00:00:00Z 2026-08-01T06:00:00Z 2026-08-01T12:00:00Z 2026-08-01T18:00:00Z 2026-08-02T00:00:00Z}"

mode_at() { # project, iso-ts -> OK | POISON | ?
  local m
  m=$(timeout 90 psql "$PGURL" -Atqc "EXPLAIN SELECT id FROM $TABLE
        WHERE project_id='$1' AND timestamp > timestamp '$2'
        ORDER BY timestamp DESC LIMIT 20" 2>/dev/null | grep -oE 'mode=[a-z-]+' | sort -u | head -1)
  case "$m" in *bounded*) echo OK;; *full-set*) echo POISON;; *) echo '?';; esac
}

echo "footer boundary @ $(date -u +%Y-%m-%dT%H:%M:%SZ)  table=$TABLE"
for p in $PROJECTS; do
  clean_from=""
  line=""
  for ts in $BOUNDS; do
    r=$(mode_at "$p" "$ts")
    line="$line ${ts:5:11}=$r"
    # Oldest bound that still plans bounded = the clean frontier.
    [ "$r" = "OK" ] && [ -z "$clean_from" ] && clean_from="$ts"
  done
  printf "  %s  clean-from=%s\n" "${p:0:8}" "${clean_from:-none-of-the-probed-bounds}"
  printf "     %s\n" "$line"
done
