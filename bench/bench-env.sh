# Shared TF-against-local-MinIO environment, sourced by the bench/debug
# scripts. Sources .env first, then pins MinIO regardless of what .env points
# at (prod .env targets R2).
[ -f .env ] && { set -a; source .env; set +a; }
export AWS_S3_ENDPOINT=http://127.0.0.1:9000
export AWS_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_S3_BUCKET=timefusion-bench
export TIMEFUSION_BUFFER_MAX_MEMORY_MB=2048
export TIMEFUSION_FOYER_METADATA_MEMORY_MB=64
export TIMEFUSION_ALLOW_INSECURE_AUTH=true
unset OTEL_EXPORTER_OTLP_ENDPOINT
export OTEL_SDK_DISABLED=true
