.PHONY: test test-unit test-all test-ovh test-minio test-minio-all test-prod test-integration test-integration-minio test-e2e run-prod run-minio build-prod minio-start minio-stop minio-clean tf-start tf-stop

# Default test (fast, excludes slow integration tests)
test:
	cargo test $${ARGS}

# Fast lib-only iteration: single leaf compile (no redundant `cargo build`),
# skips integration/e2e. Filter one test with ARGS, e.g.
#   make test-unit ARGS=test_recompress_partition_skip_idempotency
test-unit:
	cargo test --lib $${ARGS}

# Run all tests including slow integration tests
test-all:
	@export $$(cat .env | grep -v '^#' | xargs) && cargo test -- --include-ignored $${ARGS}

# Explicit test with OVH/S3
test-ovh:
	@echo "Testing with OVH/S3..."
	@export $$(cat .env | grep -v '^#' | xargs) && cargo test $${ARGS}

# Test with MinIO (fast, excludes slow integration tests)
test-minio:
	@echo "Testing with MinIO..."
	@export $$(cat .env.minio | grep -v '^#' | xargs) && cargo test $${ARGS}

# Test with MinIO including all tests (same as CI)
test-minio-all:
	@echo "Testing all with MinIO (including integration tests)..."
	@export $$(cat .env.minio | grep -v '^#' | xargs) && cargo test -- --include-ignored $${ARGS}

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

# Run with MinIO configuration (local development with prod-like settings)
run-minio:
	@echo "Running with MinIO configuration..."
	@export $$(cat .env.minio.prod | grep -v '^#' | xargs) && cargo run

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

# Run integration tests (postgres wire protocol tests, sqllogictests)
# These are slower tests that start a full PGWire server
test-integration:
	@echo "Running integration tests..."
	@export $$(cat .env | grep -v '^#' | xargs) && cargo test --test integration_test --test sqllogictest -- --ignored $${ARGS}

# Run integration tests with MinIO
test-integration-minio:
	@echo "Running integration tests with MinIO..."
	@export $$(cat .env.minio | grep -v '^#' | xargs) && cargo test --test integration_test --test sqllogictest -- --ignored $${ARGS}

# Background-run TimeFusion against local MinIO. PID + log under /tmp.
# Intended for use by downstream test suites (e.g. monoscope integration tests).
tf-start: minio-start
	@if [ -f /tmp/timefusion.pid ] && kill -0 $$(cat /tmp/timefusion.pid) 2>/dev/null; then \
		echo "timefusion already running (pid $$(cat /tmp/timefusion.pid))"; exit 0; \
	fi
	@rm -f /tmp/timefusion.pid /tmp/timefusion.log
	@export $$(cat .env.minio | grep -v '^#' | xargs) && \
		port="$${PGWIRE_PORT:-12345}" && \
		nohup cargo run --release > /tmp/timefusion.log 2>&1 & \
		echo $$! > /tmp/timefusion.pid && \
		echo "timefusion starting (PGWire: $$port, gRPC: $${GRPC_PORT:-50051}). Logs: /tmp/timefusion.log" && \
		for i in $$(seq 1 900); do \
			nc -z 127.0.0.1 $$port 2>/dev/null && { echo "ready"; exit 0; }; \
			kill -0 $$(cat /tmp/timefusion.pid) 2>/dev/null || { echo "timefusion died; see /tmp/timefusion.log"; tail -50 /tmp/timefusion.log; exit 1; }; \
			sleep 1; \
		done; echo "timeout waiting for PGWire on $$port"; tail -50 /tmp/timefusion.log; exit 1

# E2E tests: dynamic MinIO via testcontainers (requires Docker). Each test
# gets a fresh container + bucket so they parallelize safely.
test-e2e:
	@echo "Running E2E suite (Docker required for MinIO)..."
	cargo test --test e2e --features e2e -- --test-threads=1 --nocapture $${ARGS}

tf-stop:
	@[ -f /tmp/timefusion.pid ] && kill $$(cat /tmp/timefusion.pid) 2>/dev/null || true
	@rm -f /tmp/timefusion.pid
	@echo "timefusion stopped"