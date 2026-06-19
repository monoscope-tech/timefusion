#!/usr/bin/env bash
# Clean SELECT-latency bench: fresh TF on a wiped bucket (no cold-create race),
# seed a prod-shaped dataset, then measure the popular log-explorer queries with
# plan-only vs exec breakdown (query_latency.py run + run_ages).
set -euo pipefail
cd "$(dirname "$0")/.."
label="${1:-sel}"
projects="${QLAT_PROJECTS:-1}"; days="${QLAT_DAYS:-3}"; rpd="${QLAT_ROWS_PER_DAY:-20000}"
data_dir="./data/sel-${label}"
log="/tmp/tf-sel-${label}.log"
bin="${TF_BIN:-./target/release-iter/timefusion}"

pkill -f 'target/(debug|release|release-iter)/timefusion' 2>/dev/null || true
sleep 1
rm -rf "$data_dir"; mkdir -p "$data_dir"
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  aws --endpoint-url http://127.0.0.1:9000 s3 rm s3://timefusion-bench --recursive >/dev/null 2>&1 || true

[ -f .env ] && { set -a; source .env; set +a; } || echo "note: no .env found; using ambient environment"
export AWS_S3_BUCKET=timefusion-bench
export TIMEFUSION_DATA_DIR="$data_dir"
export TIMEFUSION_TABLE_PREFIX=bench
export RUST_LOG="${RUST_LOG_OVERRIDE:-warn,timefusion=info}"
export TIMEFUSION_BUFFER_FLUSH_INTERVAL_SECS=15
export TIMEFUSION_BUFFER_MAX_MEMORY_MB="${MEM_MB:-2048}"
export TIMEFUSION_FOYER_METADATA_MEMORY_MB="${FOYER_META_MB:-64}"
export TIMEFUSION_ALLOW_INSECURE_AUTH=true
export MAX_PG_CONNECTIONS=128
unset OTEL_EXPORTER_OTLP_ENDPOINT
export OTEL_SDK_DISABLED=true

nohup "$bin" >"$log" 2>&1 &
for i in $(seq 1 60); do nc -z 127.0.0.1 12345 2>/dev/null && break; sleep 1; done
nc -z 127.0.0.1 12345 2>/dev/null || { echo "TF failed to start"; tail -20 "$log"; exit 1; }
echo "TF[sel-$label] up"

echo "=== seeding ${projects}p x ${days}d x ${rpd}/day ==="
QLAT_PROJECTS=$projects QLAT_DAYS=$days QLAT_ROWS_PER_DAY=$rpd python3 bench/query_latency.py seed_scale 2>&1 | tail -2
# Force a flush so older data is in Delta (queries exercise MemBuffer+Delta union).
sleep 18
echo "=== query_latency run ==="
python3 bench/query_latency.py run 2>&1 | tail -22
echo "=== run_ages (cold/warm partition probe) ==="
python3 bench/query_latency.py run_ages 2>&1 | tail -12 || true
