# --- Stage 1: Build ---
# Use the latest Rust version
FROM rust:1.82 AS builder

WORKDIR /app

# Install cmake (needed for some C dependencies)
RUN apt-get update && apt-get install -y cmake

# Copy source code
COPY . .

# Build for release (using the default Linux target, not musl)
RUN cargo build --release

# --- Stage 2: Final ---
# Use a small Debian image (compatible with the default Rust build)
# instead of 'scratch', because we need some system libraries (like libc)
FROM debian:bookworm-slim

WORKDIR /app

# Copy the binary
COPY --from=builder /app/target/release/ingestion-service /ingestion-service

# Run it
ENTRYPOINT ["/ingestion-service"]