#!/usr/bin/env bash
# Insert-ack tail-latency bench: reproduce the dual-write gating symptom.
# Starts TF (release-iter) against local MinIO with a small buffer + many
# projects so the serial Delta flush can't keep up → backpressure → tail.
#
# Usage: ./bench/insert_tail.sh <label> [writers] [duration] [mem_mb] [flush_secs]
set -euo pipefail
cd "$(dirname "$0")/.."

label="${1:-base}"
writers="${2:-24}"
duration="${3:-90}"
mem_mb="${4:-512}"
flush_secs="${5:-30}"
data_dir="./data/itail-${label}"
log="/tmp/tf-itail-${label}.log"
bin="${TF_BIN:-./target/release-iter/timefusion}"

pkill -f 'target/(debug|release|release-iter)/timefusion' 2>/dev/null || true
sleep 1
rm -rf "$data_dir"; mkdir -p "$data_dir"
# WIPE=1 wipes the Delta table for a cold baseline; default reuses the existing
# table (realistic — prod tables always exist; avoids the cold-create race).
if [ "${WIPE:-0}" = "1" ]; then
  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
    aws --endpoint-url http://127.0.0.1:9000 s3 rm s3://timefusion-bench --recursive >/dev/null 2>&1 || true
fi

# Source .env if present (S3 creds etc); under `set -e` a missing file would
# otherwise abort the script with a cryptic error.
[ -f .env ] && { set -a; source .env; set +a; } || echo "note: no .env found; using ambient environment"
export AWS_S3_BUCKET=timefusion-bench
export TIMEFUSION_DATA_DIR="$data_dir"
export TIMEFUSION_TABLE_PREFIX=bench
export RUST_LOG="${RUST_LOG_OVERRIDE:-warn,timefusion=info}"
export TIMEFUSION_BUFFER_FLUSH_INTERVAL_SECS="$flush_secs"
export TIMEFUSION_BUFFER_MAX_MEMORY_MB="$mem_mb"
# Keep cache reservations small so max_memory_mb ≈ effective MemBuffer budget
# (the test .env shrinks foyer memory to 64MB but leaves the 512MB metadata-cache
# reservation, which otherwise eats most of the budget and distorts the bench).
export TIMEFUSION_FOYER_METADATA_MEMORY_MB="${FOYER_META_MB:-64}"
export TIMEFUSION_ALLOW_INSECURE_AUTH=true
export MAX_PG_CONNECTIONS=128
export TIMEFUSION_BATCH_QUEUE_CAPACITY=10000
unset OTEL_EXPORTER_OTLP_ENDPOINT
export OTEL_SDK_DISABLED=true

nohup "$bin" >"$log" 2>&1 &
echo $! > /tmp/tf-itail.pid
for i in $(seq 1 60); do
  nc -z 127.0.0.1 12345 2>/dev/null && { echo "TF[$label] up after ${i}s (pid=$(cat /tmp/tf-itail.pid))"; break; }
  sleep 1
done
nc -z 127.0.0.1 12345 2>/dev/null || { echo "TF[$label] failed to start"; tail -30 "$log"; exit 1; }

echo "=== running concurrent_load: writers=$writers duration=${duration}s mem=${mem_mb}MB flush=${flush_secs}s ==="
python3 bench/concurrent_load.py --writers "$writers" --readers 2 --duration "$duration" --batch 30 --row-limit "${ROW_LIMIT:-6000}" || true
echo "=== TF flush/pressure log tail ==="
grep -iE 'flush|pressure|backpressure|reject|memory limit' "$log" | tail -25 || true
