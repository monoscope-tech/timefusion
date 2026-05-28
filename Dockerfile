# syntax=docker/dockerfile:1

##############################
#         Builder Stage      #
##############################
FROM rust:1.91-slim-bookworm AS builder
WORKDIR /app

# Install build dependencies. protoc is required by tonic-prost-build (build.rs).
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# Copy Cargo manifests, build.rs, proto files (needed by build.rs at compile
# time), and vendored path-dep crates referenced in Cargo.toml.
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto/ proto/
COPY vendor/ vendor/

# Create dummy bench files (one per [[bench]] in Cargo.toml) and a dummy main
# to allow dependency caching without the full source tree.
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    mkdir benches && \
    echo "fn main() {}" > benches/core_benchmarks.rs && \
    echo "fn main() {}" > benches/tantivy_benchmarks.rs && \
    echo "fn main() {}" > benches/sort_layout_benchmarks.rs

# Build a dummy release binary (to cache dependencies)
RUN cargo build --release

# Copy the full source code
COPY src/ src/
COPY schemas/ schemas/

# Build the real release binary
RUN cargo build --release

# Pre-create app state dirs so they can be copied into the distroless
# runtime (which has no shell to mkdir at runtime).
RUN mkdir -p /app/queue_db /app/data

##############################
#         Runtime Stage      #
##############################
# Distroless/cc ships glibc 2.36 (matches builder), libssl3, and CA roots,
# and runs as the built-in `nonroot` user (uid 65532). Previously this
# stage was ubuntu:20.04 (glibc 2.31) which silently produced binaries
# that crashed at startup with `GLIBC_2.32/2.33/2.34/2.35 not found`.
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

COPY --from=builder --chown=nonroot:nonroot /app/target/release/timefusion /usr/local/bin/timefusion
COPY --from=builder --chown=nonroot:nonroot /app/queue_db /app/queue_db
COPY --from=builder --chown=nonroot:nonroot /app/data     /app/data

EXPOSE 80 5432

ENTRYPOINT ["/usr/local/bin/timefusion"]