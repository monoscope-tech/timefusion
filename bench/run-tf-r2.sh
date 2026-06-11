#!/usr/bin/env bash
# Start TF against Cloudflare R2 for network-latency-inclusive benchmarks.
# Usage: ./bench/run-tf-r2.sh [label] [debug|release]
set -euo pipefail
label="${1:-r2}"
profile="${2:-release}"
data_dir="./data/bench-r2-${label}"
log="/tmp/tf-r2-${label}.log"

pkill -f 'target/[a-z-]*/timefusion' 2>/dev/null || true
sleep 1
mkdir -p "$data_dir"

set -a; source .env; set +a
# Remote object store for network-latency-inclusive benches. The monoscope
# R2 token is dead (401 on list/put), so we use the OVH S3 creds from
# monoscope/.env.prod — same provider as prod TF's storage. Dedicated
# tf-qlat-bench/ prefix inside the existing rrweb bucket.
export AWS_S3_ENDPOINT="https://s3.de.io.cloud.ovh.net/"
export AWS_S3_BUCKET="rrweb"
export AWS_ACCESS_KEY_ID="$(grep '^S3_ACCESS_KEY' ../monoscope/.env.prod | cut -d= -f2)"
export AWS_SECRET_ACCESS_KEY="$(grep '^S3_SECRET_KEY' ../monoscope/.env.prod | cut -d= -f2)"
export AWS_REGION="de"
export AWS_ALLOW_HTTP="false"
export TIMEFUSION_TABLE_PREFIX="tf-qlat-bench"
export TIMEFUSION_DATA_DIR="$data_dir"
export RUST_LOG="${RUST_LOG_OVERRIDE:-warn,timefusion=info}"
export TIMEFUSION_FLUSH_INTERVAL_SECS=60
export TIMEFUSION_BUFFER_MAX_MEMORY_MB=2048
export TIMEFUSION_ALLOW_INSECURE_AUTH=true
export MAX_PG_CONNECTIONS=64
export TIMEFUSION_BATCH_QUEUE_CAPACITY=10000
unset OTEL_EXPORTER_OTLP_ENDPOINT
export OTEL_SDK_DISABLED=true
# .env carries a 60s foyer TTL (debug leftover) — that expires every cached
# footer/page between bench iterations. Use the 7-day default + roomier disk.
export TIMEFUSION_FOYER_TTL_SECONDS=604800
export TIMEFUSION_FOYER_DISK_GB=4
export TIMEFUSION_FOYER_MEMORY_MB=256
# Warm full file contents (not just footers) for recent partitions, so the
# first data read after boot doesn't pull whole parquet files from S3 inline.
export TIMEFUSION_WARM_FULL_FILES=true

nohup "./target/${profile}/timefusion" >"$log" 2>&1 &
echo $! > /tmp/tf.pid
for i in $(seq 1 90); do
  if nc -z 127.0.0.1 12345 2>/dev/null; then
    echo "TF-R2[$label/$profile] up after ${i}s (pid=$(cat /tmp/tf.pid), log=$log)"
    exit 0
  fi
  sleep 1
done
echo "TF-R2[$label] failed to start"; tail -20 "$log"; exit 1
