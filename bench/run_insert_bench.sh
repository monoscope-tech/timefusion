#!/usr/bin/env bash
# Clean insert-ack measurement: runs insert_tail.sh for one config, saves the
# full concurrent_load report (never truncated) to bench/data/<label>.report,
# and prints just the ingest/latency summary + flush health.
set -euo pipefail
cd "$(dirname "$0")/.."
label="${1:-m}"; writers="${2:-24}"; dur="${3:-60}"; mem="${4:-256}"; flush="${5:-15}"
mkdir -p bench/data
report="bench/data/${label}.report"
ROW_LIMIT="${ROW_LIMIT:-6000}" ./bench/insert_tail.sh "$label" "$writers" "$dur" "$mem" "$flush" > "$report" 2>&1 || true
echo "########## CONFIG: $label  writers=$writers mem=${mem}MB flush=${flush}s ##########"
grep -E 'INSERT ack lat|rows inserted|batches sent|throughput|insert errors|press=' "$report" || true
log="/tmp/tf-itail-${label}.log"
echo "-- flush health --"
echo "force-flush failures: $(grep -c 'force-flush: Delta commit failed' "$log" 2>/dev/null || echo 0)"
echo "backpressure engaged:  $(grep -c 'backpressure engaged' "$log" 2>/dev/null || echo 0)"
echo "backpressure rejects:  $(grep -c 'backpressure exhausted' "$log" 2>/dev/null || echo 0)"
echo "force-flush invocations: $(grep -c 'force-flush' "$log" 2>/dev/null || echo 0)"
