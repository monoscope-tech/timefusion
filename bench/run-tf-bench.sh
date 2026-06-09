#!/usr/bin/env bash
# Restart TF for a clean benchmark iteration.
# Usage: ./bench/run-tf-bench.sh <label>
set -euo pipefail
label="${1:-bench}"
data_dir="./data/bench-${label}"
log="/tmp/tf-${label}.log"

pkill -f 'target/(debug|release)/timefusion' 2>/dev/null || true
sleep 1
rm -rf "$data_dir"
mkdir -p "$data_dir"
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  aws --endpoint-url http://127.0.0.1:9000 s3 rm s3://timefusion-bench --recursive >/dev/null 2>&1 || true

set -a; source .env; set +a
# Bench-specific overrides
export AWS_S3_BUCKET=timefusion-bench
export TIMEFUSION_DATA_DIR="$data_dir"
export TIMEFUSION_TABLE_PREFIX=bench
export RUST_LOG=warn,timefusion=info
export TIMEFUSION_BUFFER_FLUSH_INTERVAL_SECS=60
export TIMEFUSION_BUFFER_MAX_MEMORY_MB=2048
export TIMEFUSION_ALLOW_INSECURE_AUTH=true
export MAX_PG_CONNECTIONS=64
# Production-shaped batches; this is the prod symptom — many small inserts.
export TIMEFUSION_BATCH_QUEUE_CAPACITY=10000
# Silence prod OTel export — it floods the log with oversized-batch errors
# and skews the bench. Pointing at a sink that doesn't exist:
unset OTEL_EXPORTER_OTLP_ENDPOINT
export OTEL_SDK_DISABLED=true

nohup ./target/debug/timefusion >"$log" 2>&1 &
echo $! > /tmp/tf.pid
for i in $(seq 1 60); do
  if nc -z 127.0.0.1 12345 2>/dev/null; then
    echo "TF[$label] up after ${i}s (pid=$(cat /tmp/tf.pid))"
    exit 0
  fi
  sleep 1
done
echo "TF[$label] failed to start"; tail -20 "$log"; exit 1
