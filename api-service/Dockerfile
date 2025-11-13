# --- Stage 1: Build ---
# Use the official Go image based on Debian (Bookworm)
# This has glibc, which matches the Kafka library
FROM golang:1.25-bookworm AS builder

WORKDIR /app

# Copy go.mod and go.sum first to cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the code
COPY . .

# Build the app with CGO enabled
# We don't need special flags because Debian handles CGO natively
RUN CGO_ENABLED=1 GOOS=linux go build -o /geodispatch-api .

# --- Stage 2: Final ---
# Use a small Debian image for the runtime
# "distroless" is even better, but "debian:bookworm-slim" is easiest to debug
FROM debian:bookworm-slim

# Install CA certificates (needed for HTTPS) and any required shared libs
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from the builder
COPY --from=builder /geodispatch-api /geodispatch-api

# Run it
ENTRYPOINT ["/geodispatch-api"]