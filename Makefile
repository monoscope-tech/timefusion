.PHONY: test test-ovh test-minio test-prod run-prod build-prod minio-start minio-stop minio-clean

# Default test with MinIO/test environment (uses .env)
test:
	cargo test $${ARGS}

# Explicit test with OVH/S3
test-ovh:
	@echo "Testing with OVH/S3..."
	@export $$(cat .env | grep -v '^#' | xargs) && cargo test $${ARGS}

# Test with MinIO
test-minio:
	@echo "Testing with MinIO..."
	@export $$(cat .env.minio | grep -v '^#' | xargs) && cargo test $${ARGS}

# Test with production config (be careful!)
test-prod:
	@echo "WARNING: Testing with PRODUCTION credentials!"
	@echo "Press Ctrl+C to cancel, or wait 3 seconds to continue..."
	@sleep 3
	@export $$(cat .env.prod | grep -v '^#' | xargs) && cargo test $${ARGS}

# Run with production configuration
run-prod:
	@echo "Running with PRODUCTION configuration..."
	@export $$(cat .env.prod | grep -v '^#' | xargs) && cargo run

# Build release with production configuration
build-prod:
	@echo "Building release with PRODUCTION configuration..."
	@export $$(cat .env.prod | grep -v '^#' | xargs) && cargo build --release

# Start MinIO server
minio-start:
	@mkdir -p /tmp/minio-data
	@pkill -f "minio server" || true
	@MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin nohup minio server /tmp/minio-data --console-address :9001 > /tmp/minio.log 2>&1 &
	@sleep 2
	@export $$(cat .env.test | grep -v '^#' | xargs) && \
		aws s3 mb s3://timefusion-test --endpoint-url=http://127.0.0.1:9000 > /dev/null 2>&1 || true && \
		aws s3 mb s3://timefusion-tests --endpoint-url=http://127.0.0.1:9000 > /dev/null 2>&1 || true
	@echo "MinIO ready on :9000 (API) and :9001 (Console)"

# Stop MinIO server
minio-stop:
	@pkill -f "minio server" || true
	@echo "MinIO stopped"

# Clean MinIO data
minio-clean:
	@rm -rf /tmp/minio-data
	@echo "MinIO data cleaned"