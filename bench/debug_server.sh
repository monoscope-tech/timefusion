#!/usr/bin/env bash
# Start TF against local MinIO (same env as run_update_bench.sh) and block.
# Usage: TF_BIN=/tmp/tf-fixed ./bench/debug_server.sh <label>
set -euo pipefail
cd "$(dirname "$0")/.."
label="${1:-dbg}"
bin="${TF_BIN:-./target/debug/timefusion}"
data_dir="./data/uchurn-${label}"
rm -rf "$data_dir"; mkdir -p "$data_dir"
source bench/bench-env.sh
export TIMEFUSION_DATA_DIR="$data_dir"
export TIMEFUSION_TABLE_PREFIX="uchurn-${label}"
export RUST_LOG="${RUST_LOG_OVERRIDE:-warn,timefusion=debug}"
export TIMEFUSION_BUFFER_FLUSH_INTERVAL_SECS="${FLUSH_SECS:-600}"
exec "$bin"
