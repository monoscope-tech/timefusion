.PHONY: lint lint-fix test test-unit prepush test-all test-ovh test-minio test-minio-all test-prod test-integration test-integration-minio test-e2e run-prod run-minio build-prod minio-start minio-stop minio-clean tf-start tf-stop

# THE inner-loop command: the whole suite, every time you change something.
#
# `cargo nextest` (not `cargo test`) because nextest runs one process per test
# from a single global pool. `cargo test` runs the test *binaries* one after
# another and `#[serial]` re-serializes most tests inside each one, so it uses
# roughly one core no matter how many the box has. Process-per-test also gives
# each test its own copy of the process-global state (`PGWIRE_PORT`, the
# `OnceLock` config) that `#[serial]` exists to protect.
#
# Needs cargo-nextest:
#   curl -LsSf https://get.nexte.st/latest/mac | tar zxf - -C ~/.cargo/bin
#
# Filter with ARGS (substring match on the test name):
#   make test ARGS=dedup_compaction
test:
	cargo nextest run $${ARGS}

# Lib-only: skips the integration binary entirely. Use when iterating on a
# pure-logic change; `make test` is cheap enough to be the default otherwise.
test-unit:
	cargo nextest run --lib $${ARGS}

# Exactly what CI's Clippy step runs — the flags live once, in the `cargo lint`
# alias in `.cargo/config.toml`. A bare `cargo clippy` is NOT equivalent: it
# misses --all-features and doesn't deny warnings, so it passes on code CI
# rejects (2026-08-01).
lint:
	cargo lint

# Apply clippy's autofixes (what the autoformat workflow runs).
lint-fix:
	cargo lint-fix

# Pre-push gate: CI's lint first (fails fast and cheap — a lint error should not
# cost a full test run), then the whole suite. No hand-picked subset any more:
# with nextest the full run is short enough that skipping targets only buys a
# surprise in CI. Set TIMEFUSION_TEST_S3_ENDPOINT to reuse a persistent MinIO.
prepush: lint
	RUST_LOG=off cargo nextest run $${ARGS}

# Everything, including the #[ignore]d tests.
test-all:
	@export $$(cat .env | grep -v '^#' | xargs) && cargo nextest run --run-ignored all $${ARGS}

# Explicit test with OVH/S3
test-ovh:
	@echo "Testing with OVH/S3..."
	@export $$(cat .env | grep -v '^#' | xargs) && cargo nextest run $${ARGS}

# Test with MinIO (fast, excludes slow integration tests)
test-minio:
	@echo "Testing with MinIO..."
	@export $$(cat .env.minio | grep -v '^#' | xargs) && cargo nextest run $${ARGS}

# Test with MinIO including all tests (same as CI)
test-minio-all:
	@echo "Testing all with MinIO (including integration tests)..."
	@export $$(cat .env.minio | grep -v '^#' | xargs) && cargo nextest run --run-ignored all $${ARGS}

# Test with production config (be careful!)
test-prod:
	@echo "WARNING: Testing with PRODUCTION credentials!"
	@echo "Press Ctrl+C to cancel, or wait 3 seconds to continue..."
	@sleep 3
	@export $$(cat .env.prod | grep -v '^#' | xargs) && cargo nextest run $${ARGS}

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
	@export $$(cat .env | grep -v '^#' | xargs) && cargo nextest run --run-ignored all -E 'test(integration) or test(sqllogictest)' $${ARGS}

# Run integration tests with MinIO
test-integration-minio:
	@echo "Running integration tests with MinIO..."
	@export $$(cat .env.minio | grep -v '^#' | xargs) && cargo nextest run --run-ignored all -E 'test(integration) or test(sqllogictest)' $${ARGS}

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
	cargo nextest run --features e2e -E 'binary(e2e)' $${ARGS}

tf-stop:
	@[ -f /tmp/timefusion.pid ] && kill $$(cat /tmp/timefusion.pid) 2>/dev/null || true
	@rm -f /tmp/timefusion.pid
	@echo "timefusion stopped"