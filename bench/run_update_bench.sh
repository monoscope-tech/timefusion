#!/usr/bin/env bash
# UPDATE-churn A/B bench: starts TF (binary via TF_BIN) against local MinIO,
# runs bench/update_churn.py, prints the latency summary.
#
# Usage: TF_BIN=./target/debug/timefusion ./bench/run_update_bench.sh <label> [duration] [updaters]
set -euo pipefail
cd "$(dirname "$0")/.."

label="${1:-base}"
duration="${2:-60}"
updaters="${3:-2}"
data_dir="./data/uchurn-${label}"
log="/tmp/tf-uchurn-${label}.log"
bin="${TF_BIN:-./target/debug/timefusion}"

pkill -f 'target/(debug|release|release-iter)/timefusion' 2>/dev/null || true
sleep 1
rm -rf "$data_dir"; mkdir -p "$data_dir"

[ -f .env ] && { set -a; source .env; set +a; }
# Pin local MinIO regardless of what .env points at (prod .env targets R2).
export AWS_S3_ENDPOINT=http://127.0.0.1:9000
export AWS_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_S3_BUCKET=timefusion-bench
export TIMEFUSION_DATA_DIR="$data_dir"
export TIMEFUSION_TABLE_PREFIX="uchurn-${label}"
export RUST_LOG="${RUST_LOG_OVERRIDE:-warn,timefusion=info}"
# Short flush so updates race real flush commits, like prod. Flush-immediately
# pushes inserted rows into Delta right away so UPDATEs exercise the merge
# (10-min buckets never complete during a short bench run otherwise).
export TIMEFUSION_BUFFER_FLUSH_INTERVAL_SECS=15
export TIMEFUSION_BUFFER_FLUSH_IMMEDIATELY="${FLUSH_IMMEDIATELY:-true}"
export TIMEFUSION_BUFFER_MAX_MEMORY_MB=2048
export TIMEFUSION_FOYER_METADATA_MEMORY_MB=64
export TIMEFUSION_ALLOW_INSECURE_AUTH=true
export MAX_PG_CONNECTIONS=128
unset OTEL_EXPORTER_OTLP_ENDPOINT
export OTEL_SDK_DISABLED=true

nohup "$bin" >"$log" 2>&1 &
echo $! > /tmp/tf-uchurn.pid
for i in $(seq 1 90); do
  nc -z 127.0.0.1 "${PGWIRE_PORT:-12345}" 2>/dev/null && { echo "TF[$label] up after ${i}s"; break; }
  sleep 1
done
nc -z 127.0.0.1 "${PGWIRE_PORT:-12345}" 2>/dev/null || { echo "TF[$label] failed to start"; tail -30 "$log"; exit 1; }

mkdir -p bench/data
python3 bench/update_churn.py --duration "$duration" --updaters "$updaters" | tee "bench/data/uchurn-${label}.report"
echo "-- dml/flush log tail --"
grep -icE 'conflict|stalled|reject' "$log" || true
kill "$(cat /tmp/tf-uchurn.pid)" 2>/dev/null || true
