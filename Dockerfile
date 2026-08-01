# syntax=docker/dockerfile:1.6

##############################
#         Chef base          #
##############################
FROM rust:1.91-slim-bookworm AS chef
WORKDIR /app
# protoc is required by tonic-prost-build (build.rs). make is required by
# tikv-jemalloc-sys (jemalloc compiles from C source under --features
# profiling); the slim image ships cc but not make. libunwind-dev backs the
# heap profiler's unwinder (see JEMALLOC_SYS_PROF_BACKTRACE below).
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev protobuf-compiler make libunwind-dev && \
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
# manifests (in vendor/). NOT src/, schemas/, or proto/ — including them
# here would bust the planner layer on edits unrelated to the dep graph,
# defeating cargo-chef's purpose. vendor/ is copied in full (manifests +
# source) since separating them isn't worth the Dockerfile complexity.
COPY Cargo.toml Cargo.lock build.rs ./
COPY vendor/ vendor/
# Stub auto-discovered targets so `cargo metadata` (run by cargo chef prepare)
# can parse the manifest without the real sources. recipe.json content depends
# only on the dep graph, not on these stubs, so the builder cook layer stays
# cached across src/ edits.
# Derived from the manifest, not hardcoded: a hardcoded list silently breaks the
# image build the first time someone adds a [[bench]] (the manifest then names a
# target whose source is absent, and `cargo chef prepare` fails to parse it).
RUN mkdir -p src benches && \
    echo 'fn main() {}' > src/main.rs && \
    sed -n '/^\[\[bench\]\]/,+2p' Cargo.toml | sed -n 's/^name *= *"\(.*\)"/\1/p' \
      | while read -r bench; do echo 'fn main() {}' > "benches/$bench.rs"; done && \
    ls benches/
RUN cargo chef prepare --recipe-path recipe.json

##############################
#         Builder            #
##############################
FROM chef AS builder
# Cook compiles only dependencies. Docker layer-caches this step; cache-to:
# type=gha,mode=max in deploy.yml persists the layer across CI runs. Layer
# invalidates only when recipe.json changes (i.e. the dep graph changes),
# not on every src/ or proto/ edit. vendor/ is required here for the same
# reason as in the planner stage (path-deps) — the duplication is necessary.
COPY --from=planner /app/recipe.json recipe.json
COPY vendor/ vendor/
# --features profiling: deploy the jemalloc-heap + pprof-CPU profilers to
# attribute the prod OOM (2026-07-04). STRIP=none keeps symbols so jeprof/pprof
# resolve stacks (the release profile strips by default). Set on cook AND build
# so cargo-chef's cached dep layer matches the final build's profile.
ENV CARGO_PROFILE_RELEASE_STRIP=none
# Frame pointers: jemalloc's heap profiler unwinds allocation stacks via frame
# pointers; without them every allocation collapses into one bogus leaf frame
# (2026-07-31: 28GB attributed to a random bz2 symbol — dumps unusable for OOM
# attribution). ~1% perf cost, and it makes every future OOM self-explaining.
ENV RUSTFLAGS="-C force-frame-pointers=yes"
# ...but frame pointers alone were not enough: jemalloc's profiler unwinds with
# whatever method it was *configured* with, and stock tikv-jemalloc-sys leaves it
# on the libgcc unwinder, which returns zero frames here — 100% of an 88GB prod
# dump landed in one anonymous `prof_backtrace_impl` frame (2026-07-31). Our
# vendored -sys patch (vendor/tikv-jemalloc-sys) turns this env var into
# `--enable-prof-libunwind`. Fallback needing no libs: `gcc` (uses the frame
# pointers above). Verify with scripts/verify-jemalloc-prof.sh.
ENV JEMALLOC_SYS_PROF_BACKTRACE=libunwind
RUN cargo chef cook --release --locked --features profiling --recipe-path recipe.json

# Now compile the real binary. Deps are already built, so this only rebuilds
# the crate itself when src/, build.rs, or proto/ change. proto/ must be
# copied *after* cook so .proto edits don't bust the dep-compile layer.
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto/ proto/
COPY src/ src/
COPY schemas/ schemas/
RUN cargo build --release --locked --features profiling

# App state dirs (distroless runtime has no shell to mkdir at runtime).
RUN mkdir -p /queue_db /data
# jemalloc's profiler links libunwind dynamically; distroless ships neither it
# nor its liblzma dep, so stage both (cp -a keeps the soname symlinks) for the
# runtime stage. Arch-agnostic glob: /usr/lib/{x86_64,aarch64}-linux-gnu.
RUN mkdir -p /profdeps && cp -a /usr/lib/*/libunwind.so.8* /usr/lib/*/liblzma.so.5* /profdeps/

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
COPY --from=builder /profdeps/ /usr/local/lib/
ENV LD_LIBRARY_PATH=/usr/local/lib

EXPOSE 80 5432

# Default telemetry destination: the swarm-internal collector. Image ENV
# loses to service-level env, so operators can still override per deploy.
# Spans default off (per-query volume); logs + metrics flow.
ENV OTEL_EXPORTER_OTLP_ENDPOINT=http://srv-captain--otelcol:4317 \
    OTEL_TRACES_EXPORTER=none

# glibc malloc defaults to 8 arenas/core (384 on a 48-core host); the write
# path's inflate→compact churn fragments them so freed memory never returns
# to the OS. Measured 2026-06-11 on prod at identical 2.5-min uptime:
# 20.7GB anon RSS without this, 5.0GB with — the difference was driving the
# 66.6GiB-cgroup OOM crashloop. MMAP_THRESHOLD pins large allocations
# (Arrow batches, parquet buffers) to mmap so frees return to the OS —
# glibc's adaptive threshold (up to 32MB) otherwise strands them on the
# heap: measured steady-state anon creep ~1GB/min without, ~460MB/min
# with. Revisit both if we switch to jemalloc.
ENV MALLOC_ARENA_MAX=2 \
    MALLOC_MMAP_THRESHOLD_=131072

ENTRYPOINT ["/usr/local/bin/timefusion"]
