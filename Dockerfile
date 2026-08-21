# syntax=docker/dockerfile:1.6

##############################
#         Chef base          #
##############################
FROM rust:1.91-slim-bookworm AS chef
WORKDIR /app
# make is required by tikv-jemalloc-sys (jemalloc compiles from C source under --features
# profiling); the slim image ships cc but not make. libunwind-dev backs the
# heap profiler's unwinder (see JEMALLOC_SYS_PROF_BACKTRACE below).
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev make libunwind-dev && \
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
# manifests (in vendor/). NOT src/ or schemas/ — including them
# here would bust the planner layer on edits unrelated to the dep graph,
# defeating cargo-chef's purpose. vendor/ is copied in full (manifests +
# source) since separating them isn't worth the Dockerfile complexity.
COPY Cargo.toml Cargo.lock ./
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
# not on every src/ edit. vendor/ is required here for the same
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
#
# Microarchitecture. Without `-C target-cpu` rustc emits BASELINE x86-64 — SSE2,
# no AVX2, no BMI2, no FMA — which for a columnar engine is most of the machine
# left unused: arrow-rs 58 dropped its explicit `simd` feature and relies on LLVM
# autovectorization, and every Arrow compare/filter/aggregate kernel, the parquet
# bit-packing and RLE decoders, and the null-bitmap popcounts are gated behind
# exactly these target features. Prod is an AMD EPYC 8224P (Zen 4c) advertising
# avx2, avx512f/bw/dq/vl/vnni/vbmi2, bmi1/2, fma, vaes and vpclmulqdq.
#
# `x86-64-v3` (AVX2 + BMI2 + FMA + POPCNT), not `native` and not `v4`:
#   - `native` would target the BUILD RUNNER's CPU, not the deploy host, so the
#     binary can SIGILL on a machine the builder never saw.
#   - `v4` (AVX-512) is supported by this host but pins us to it; v3 is portable
#     to any server CPU since ~2015 and captures the large SSE2 -> AVX2 step.
# Canary v4/znver4 with `--build-arg TARGET_CPU=x86-64-v4`; an arm64 build must
# pass its own value (e.g. `neoverse-n1`), since these names are x86-only.
ARG TARGET_CPU=x86-64-v3
# ONE definition, used by both the cook and build steps below — they must see
# identical RUSTFLAGS or cargo-chef's cached dep layer is silently discarded.
ENV RUSTFLAGS="-C force-frame-pointers=yes -C target-cpu=${TARGET_CPU}"
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
# the crate itself when src/ changes.
COPY Cargo.toml Cargo.lock ./
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

# The probe speaks enough PostgreSQL to distinguish the intentional startup
# 57P03 from other failures. It accepts that responder as live so Swarm can
# advance a start-first update; the external SQL probe still counts 57P03 as
# unavailable. After startup, probe less often and tolerate transient host/load
# stalls so Swarm never recycles an otherwise healthy database task.
#
# timeout 2s->5s, retries 3->5 (prod 2026-08-08). The old budget replaced a
# HEALTHY task: the handshake was measured at 0.896s under ordinary load with no
# deploy in flight, and 3 consecutive misses inside 15s is not evidence that a
# database is dead — it is evidence that it is busy. Each replacement killed an
# in-flight footer repair, discarding a 40-minute rewrite, which is why the
# backlog never drained. 5s x 5 retries = ~25s of continuous failure before
# Swarm acts, still far inside any real outage.
#
# Only interval/timeout/start-interval/retries live here: CapRover overrides
# StartPeriod at the service level (900s), and Swarm merges the two — a
# zero-valued service field inherits the image's. So changing these DOES take
# effect on the deployed service; verify with
# `docker service inspect srv-captain--timefusion` after the deploy.
#
# `--timeout` must stay above 3x `pgwire_ready_at`'s per-operation deadline
# (connect + write + read, 1.5s each in src/main.rs), or Docker kills the probe
# before it can report its own verdict.
HEALTHCHECK --interval=5s --timeout=5s --start-period=10s --start-interval=250ms --retries=5 \
    CMD ["/usr/local/bin/timefusion", "healthcheck"]

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
