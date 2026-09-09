# Native static emulator for testing the production amd64 image on ARM64.
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends qemu-user && \
    rm -rf /var/lib/apt/lists/*
