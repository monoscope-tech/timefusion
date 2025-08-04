.PHONY: test test-ovh test-minio minio-start minio-stop minio-clean

# Default test with OVH/S3 (uses .env)
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

# Start MinIO server
minio-start:
	@mkdir -p /tmp/minio-data
	@pkill -f "minio server" || true
	@MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin nohup minio server /tmp/minio-data --console-address :9001 > /tmp/minio.log 2>&1 &
	@sleep 2
	@export $$(cat .env.minio | grep -v '^#' | xargs) && \
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