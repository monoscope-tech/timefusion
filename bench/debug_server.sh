#!/usr/bin/env bash
# Start TF against local MinIO (same env as run_update_bench.sh) and block.
# Usage: TF_BIN=/tmp/tf-fixed ./bench/debug_server.sh <label>
set -euo pipefail
cd "$(dirname "$0")/.."
label="${1:-dbg}"
bin="${TF_BIN:-./target/debug/timefusion}"
data_dir="./data/uchurn-${label}"
rm -rf "$data_dir"; mkdir -p "$data_dir"
[ -f .env ] && { set -a; source .env; set +a; }
export AWS_S3_ENDPOINT=http://127.0.0.1:9000
export AWS_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_S3_BUCKET=timefusion-bench
export TIMEFUSION_DATA_DIR="$data_dir"
export TIMEFUSION_TABLE_PREFIX="uchurn-${label}"
export RUST_LOG="${RUST_LOG_OVERRIDE:-warn,timefusion=debug}"
export TIMEFUSION_BUFFER_FLUSH_INTERVAL_SECS="${FLUSH_SECS:-600}"
export TIMEFUSION_BUFFER_MAX_MEMORY_MB=2048
export TIMEFUSION_FOYER_METADATA_MEMORY_MB=64
export TIMEFUSION_ALLOW_INSECURE_AUTH=true
unset OTEL_EXPORTER_OTLP_ENDPOINT
export OTEL_SDK_DISABLED=true
exec "$bin"
