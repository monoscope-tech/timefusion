# syntax=docker/dockerfile:1

##############################
#         Builder Stage      #
##############################
FROM rustlang/rust:nightly-bullseye-slim AS builder
WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy Cargo manifests and cache dependencies
COPY Cargo.toml Cargo.lock ./

# Create a dummy main file to allow dependency caching
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build a dummy release binary (to cache dependencies)
RUN cargo build --release

# Copy the full source code, including dashboard.html
COPY src/ src/
COPY schemas/ schemas/

# Build the real release binary
RUN cargo build --release

##############################
#         Runtime Stage      #
##############################
FROM ubuntu:20.04
WORKDIR /app

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y ca-certificates libssl1.1 && \
    rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

# Create and set permissions for directories
RUN mkdir -p /app/queue_db /app/data && \
    chown -R appuser:appgroup /app /app/queue_db /app/data && \
    chmod -R 775 /app /app/queue_db /app/data

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/timefusion /usr/local/bin/timefusion

# Adjust ownership of the binary
RUN chown appuser:appgroup /usr/local/bin/timefusion

# Expose the required ports
EXPOSE 80 5432

# Switch to the non-root user
USER appuser

# Start the application
ENTRYPOINT ["/usr/local/bin/timefusion"]