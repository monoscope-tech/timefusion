# syntax=docker/dockerfile:1.6

##############################
#         Builder Stage      #
##############################
FROM rust:1.91-slim-bookworm AS builder
WORKDIR /app

# protoc is required by tonic-prost-build (build.rs).
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock build.rs ./
COPY proto/ proto/
COPY vendor/ vendor/
COPY src/ src/
COPY schemas/ schemas/

# BuildKit cache mounts let the cargo registry and target/ dir survive across
# CI runs (combined with `cache-to: type=gha` in the workflow). This replaces
# the previous dummy-main scaffolding, which only cached when Cargo.toml was
# unchanged and never reused target/ between builds.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked && \
    cp target/release/timefusion /timefusion

# App state dirs (distroless runtime has no shell to mkdir at runtime).
RUN mkdir -p /queue_db /data

##############################
#         Runtime Stage      #
##############################
# Distroless/cc ships glibc 2.36 (matches builder), libssl3, and CA roots,
# and runs as the built-in `nonroot` user (uid 65532). Previously this
# stage was ubuntu:20.04 (glibc 2.31) which silently produced binaries
# that crashed at startup with `GLIBC_2.32/2.33/2.34/2.35 not found`.
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

COPY --from=builder --chown=nonroot:nonroot /timefusion /usr/local/bin/timefusion
COPY --from=builder --chown=nonroot:nonroot /queue_db /app/queue_db
COPY --from=builder --chown=nonroot:nonroot /data     /app/data

EXPOSE 80 5432

ENTRYPOINT ["/usr/local/bin/timefusion"]
