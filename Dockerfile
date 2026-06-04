# syntax=docker/dockerfile:1.6

##############################
#         Chef base          #
##############################
FROM rust:1.91-slim-bookworm AS chef
WORKDIR /app
# protoc is required by tonic-prost-build (build.rs).
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --version 0.1.77 --locked

##############################
#         Planner            #
##############################
# Emit recipe.json describing the dep graph. Recipe content only changes when
# Cargo.toml / Cargo.lock / path-dep manifests change, so the cook layer below
# stays cached across most edits.
FROM chef AS planner
# Only inputs cargo chef prepare actually reads: Cargo manifests + path-dep
# manifests (in vendor/). NOT src/ or schemas/ — including them here would
# bust the planner layer on every source edit, transitively invalidating
# the cook layer and defeating cargo-chef's purpose.
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto/ proto/
COPY vendor/ vendor/
RUN cargo chef prepare --recipe-path recipe.json

##############################
#         Builder            #
##############################
FROM chef AS builder
# Cook compiles only dependencies. Docker layer-caches this step; cache-to:
# type=gha,mode=max in deploy.yml persists the layer across CI runs. Layer
# invalidates only when recipe.json changes (i.e. the dep graph changes),
# not on every src/ edit like the previous dummy-main pattern.
COPY --from=planner /app/recipe.json recipe.json
COPY proto/ proto/
COPY vendor/ vendor/
RUN cargo chef cook --release --locked --recipe-path recipe.json

# Now compile the real binary. Deps are already built, so this only rebuilds
# the crate itself when src/ changes.
COPY Cargo.toml Cargo.lock build.rs ./
COPY src/ src/
COPY schemas/ schemas/
RUN cargo build --release --locked

# App state dirs (distroless runtime has no shell to mkdir at runtime).
RUN mkdir -p /queue_db /data

##############################
#         Runtime            #
##############################
# Distroless/cc ships glibc 2.36 (matches builder), libssl3, and CA roots,
# and runs as the built-in `nonroot` user (uid 65532). Previously this
# stage was ubuntu:20.04 (glibc 2.31) which silently produced binaries
# that crashed at startup with `GLIBC_2.32/2.33/2.34/2.35 not found`.
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

COPY --from=builder --chown=nonroot:nonroot /app/target/release/timefusion /usr/local/bin/timefusion
COPY --from=builder --chown=nonroot:nonroot /queue_db /app/queue_db
COPY --from=builder --chown=nonroot:nonroot /data     /app/data

EXPOSE 80 5432

ENTRYPOINT ["/usr/local/bin/timefusion"]
