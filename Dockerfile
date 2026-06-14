# Build stage: compile the release binary (needs protoc for the gRPC codegen).
FROM rust:1-slim AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY . .
RUN cargo build --release --locked

# Runtime stage: a slim image with just the binary, run as a non-root user.
FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --user-group lwd
COPY --from=builder /build/target/release/lightwalletd-rs /usr/local/bin/lightwalletd-rs
USER lwd
# gRPC (9067) and Prometheus metrics (9100) by default.
EXPOSE 9067 9100
ENTRYPOINT ["/usr/local/bin/lightwalletd-rs"]
